// Package snapshotgc implements garbage collection of contents that are no longer referenced through snapshots.
package snapshotgc

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/contentlog/logparam"
	"github.com/kopia/kopia/internal/contentparam"
	"github.com/kopia/kopia/internal/stats"
	"github.com/kopia/kopia/internal/units"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/maintenance"
	"github.com/kopia/kopia/repo/maintenancestats"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

// User-visible log output.
var userLog = logging.Module("snapshotgc")

const (
	// sortChunkSize is the amount of data to sort in memory at once during external sort.
	// Larger chunks = fewer merge passes but more memory.
	sortChunkSize = 100 << 20 // 100 MB

	// outputChunkSize is the size of each output chunk file for the sorted data.
	// These chunks are loaded on-demand during merge scan to minimize peak memory.
	outputChunkSize = 64 << 20 // 64 MB

	// collectBufferSize is the in-memory buffer size for Phase 1 and Phase 3.
	// Items are accumulated in this buffer, sorted, and deduplicated before writing.
	// This creates pre-sorted runs that drastically reduce external sort complexity.
	collectBufferSize = 50 << 20 // 50 MB buffer

	// verifyWorkers is the number of concurrent goroutines for VerifyObject calls.
	// This parallelizes I/O-bound pack file reads to improve throughput.
	verifyWorkers = 8

	// objectIDSize is the maximum size of an object ID in bytes when appended.
	// Current encoding produces ~66 bytes, but we use 75 to handle future variations:
	// - Up to 5 indirection levels (5 'I' characters) - handles multi-TB objects
	// - Compression marker ('Z' = 1 character)
	// - Maximum hash size (32 bytes = 64 hex characters, per hashing.MaxHashSize)
	// - Prefix (1 character)
	// Total: 5 + 1 + 1 + 64 = 71 bytes, plus 4-byte safety margin = 75 bytes.
	// With 4MB splitter, 5 indirection levels can handle objects beyond 40 petabytes.
	objectIDSize = 75

	// contentIDSize is the maximum size of a content ID in bytes when appended.
	// Current encoding produces ~35 bytes, but we use 70 to handle maximum hash size:
	// - Prefix (1 character)
	// - Maximum hash size (32 bytes = 64 hex characters, per hashing.MaxHashSize)
	// Total: 1 + 64 = 65 bytes, plus 5-byte safety margin = 70 bytes.
	// This accommodates the absolute maximum supported hash size (hashing.MaxHashSize = 32).
	contentIDSize = 70
)

// chunkReader reads sorted content IDs from chunked files on-demand.
type chunkReader struct {
	basePath     string
	chunkIndex   int
	currentChunk []byte
	position     int
	entrySize    int
	ctx          context.Context
}

func newChunkReader(ctx context.Context, basePath string, entrySize int) *chunkReader {
	return &chunkReader{
		basePath:  basePath,
		entrySize: entrySize,
		ctx:       ctx,
	}
}

func (r *chunkReader) next() []byte {
	for {
		if r.position < len(r.currentChunk) {
			// Read next entry from current chunk
			entry := r.currentChunk[r.position : r.position+r.entrySize]
			r.position += r.entrySize
			return entry
		}

		// Current chunk exhausted, release it
		r.currentChunk = nil
		runtime.GC()
		debug.FreeOSMemory()

		// Try to load next chunk
		chunkPath := fmt.Sprintf("%s_chunk_%03d.bin", r.basePath, r.chunkIndex)
		data, err := os.ReadFile(chunkPath)
		if err != nil {
			return nil // No more chunks
		}

		r.currentChunk = data
		r.position = 0
		r.chunkIndex++
	}
}

func (r *chunkReader) close() {
	r.currentChunk = nil
}

// collectObjectsToChunks walks all snapshots and writes object IDs to chunked files.
// Uses an in-memory buffer to accumulate, sort, and deduplicate before writing.
// Each buffer flush creates one sorted chunk file, ready for k-way merge.
func collectObjectsToChunks(ctx context.Context, log *contentlog.Logger, rep repo.Repository, manifests []*snapshot.Manifest, outputBase string) ([]string, int64, error) {
	gcTempDir := filepath.Join(os.TempDir(), "kopia-gc")
	os.MkdirAll(gcTempDir, 0o700) //nolint:errcheck

	// In-memory buffer for sorting
	maxBufferEntries := int(collectBufferSize / objectIDSize)
	buffer := make([][]byte, 0, maxBufferEntries)
	var totalCount int64
	var chunkFiles []string
	var chunkIndex int

	// flushBuffer sorts, deduplicates, and writes the buffer to a chunk file
	flushBuffer := func() error {
		if len(buffer) == 0 {
			return nil
		}

		// Sort buffer
		sort.Slice(buffer, func(i, j int) bool {
			return bytes.Compare(buffer[i], buffer[j]) < 0
		})

		// Deduplicate in-place
		writeIdx := 1
		for readIdx := 1; readIdx < len(buffer); readIdx++ {
			if !bytes.Equal(buffer[readIdx], buffer[writeIdx-1]) {
				if writeIdx != readIdx {
					buffer[writeIdx] = buffer[readIdx]
				}
				writeIdx++
			}
		}
		buffer = buffer[:writeIdx]

		// Write to chunk file
		chunkPath := fmt.Sprintf("%s_chunk_%03d.bin", outputBase, chunkIndex)
		chunkFile, err := os.Create(chunkPath)
		if err != nil {
			return errors.Wrapf(err, "error creating chunk file %s", chunkPath)
		}
		defer chunkFile.Close()

		writer := bufio.NewWriterSize(chunkFile, 1<<20)
		for _, oidBytes := range buffer {
			if _, err := writer.Write(oidBytes); err != nil {
				return errors.Wrap(err, "error writing object ID")
			}
		}

		if err := writer.Flush(); err != nil {
			return errors.Wrap(err, "error flushing chunk")
		}

		chunkFiles = append(chunkFiles, chunkPath)
		chunkIndex++
		totalCount += int64(len(buffer))
		buffer = buffer[:0] // Clear buffer

		runtime.GC()
		debug.FreeOSMemory()

		return nil
	}

	// Tree walker that collects object IDs into buffer
	var walkTree func(entry fs.Entry) error
	walkTree = func(entry fs.Entry) error {
		var oid object.ID
		if h, ok := entry.(object.HasObjectID); ok {
			oid = h.ObjectID()
		} else {
			return nil
		}

		if oid == object.EmptyID {
			return nil
		}

		// Append to buffer
		var buf [128]byte
		oidBytes := oid.Append(buf[:0])
		buffer = append(buffer, append([]byte(nil), oidBytes...))

		// Flush buffer when full
		if len(buffer) >= maxBufferEntries {
			if err := flushBuffer(); err != nil {
				return err
			}
		}

		// Recurse into directories
		if dir, ok := entry.(fs.Directory); ok {
			iter, err := dir.Iterate(ctx)
			if err != nil {
				return errors.Wrap(err, "error iterating directory")
			}
			defer iter.Close()

			for {
				child, err := iter.Next(ctx)
				if err != nil {
					return errors.Wrap(err, "error getting next entry")
				}
				if child == nil {
					break
				}

				if err := walkTree(child); err != nil {
					return err
				}
			}
		}

		return nil
	}

	contentlog.Log(ctx, log, "Collecting objects from snapshots (buffered, sorted chunks)...")

	for _, m := range manifests {
		root, err := snapshotfs.SnapshotRoot(rep, m)
		if err != nil {
			return nil, 0, errors.Wrap(err, "unable to get snapshot root")
		}

		if err := walkTree(root); err != nil {
			return nil, 0, errors.Wrap(err, "error walking tree")
		}
	}

	// Flush remaining buffer
	if err := flushBuffer(); err != nil {
		return nil, 0, err
	}

	contentlog.Log1(ctx, log, "Created sorted chunks", logparam.Int64("uniqueCount", totalCount))

	return chunkFiles, totalCount, nil
}

// kWayMergeToChunks performs k-way merge of sorted chunks with deduplication,
// writing output to multiple chunk files of fixed size.
// Uses buffered I/O to keep peak memory low (only buffers, not entire chunks).
func kWayMergeToChunks(ctx context.Context, log *contentlog.Logger, sortedChunks []string, outputBase string, entrySize int) (int64, error) {
	// Open all input chunks
	type chunkInput struct {
		reader *bufio.Reader
		file   *os.File
		buffer []byte
		eof    bool
	}

	inputs := make([]*chunkInput, len(sortedChunks))
	defer func() {
		for _, inp := range inputs {
			if inp != nil && inp.file != nil {
				inp.file.Close()
			}
		}
	}()

	// Initialize inputs and read first entry from each
	for i, chunkPath := range sortedChunks {
		f, err := os.Open(chunkPath)
		if err != nil {
			return 0, errors.Wrapf(err, "error opening chunk %s", chunkPath)
		}

		inp := &chunkInput{
			file:   f,
			reader: bufio.NewReaderSize(f, 1<<20), // 1 MB buffer per file
			buffer: make([]byte, entrySize),
		}

		// Read first entry
		if n, err := io.ReadFull(inp.reader, inp.buffer); err == nil && n == entrySize {
			inputs[i] = inp
		} else {
			inp.eof = true
			inputs[i] = inp
		}
	}

	// Prepare output
	var (
		outChunkIdx int64
		outFile     *os.File
		outWriter   *bufio.Writer
		outSize     int64
		uniqueCount int64
		prevEntry   []byte
	)

	openNextOutputChunk := func() error {
		if outWriter != nil {
			if err := outWriter.Flush(); err != nil {
				return err
			}
			outFile.Close()
		}

		chunkPath := fmt.Sprintf("%s_chunk_%03d.bin", outputBase, outChunkIdx)
		f, err := os.Create(chunkPath)
		if err != nil {
			return errors.Wrapf(err, "error creating output chunk %s", chunkPath)
		}

		outFile = f
		outWriter = bufio.NewWriterSize(f, 1<<20)
		outSize = 0
		outChunkIdx++

		return nil
	}

	if err := openNextOutputChunk(); err != nil {
		return 0, err
	}

	defer func() {
		if outWriter != nil {
			outWriter.Flush()
			outFile.Close()
		}
	}()

	// K-way merge with deduplication
	for {
		// Find minimum across all inputs
		var (
			minEntry []byte
			minIdx   = -1
		)

		for i, inp := range inputs {
			if inp.eof {
				continue
			}

			if minIdx == -1 || bytes.Compare(inp.buffer, minEntry) < 0 {
				minEntry = inp.buffer
				minIdx = i
			}
		}

		if minIdx == -1 {
			// All inputs exhausted
			break
		}

		// Skip duplicates
		if prevEntry == nil || !bytes.Equal(prevEntry, minEntry) {
			// Write to output
			if _, err := outWriter.Write(minEntry); err != nil {
				return 0, errors.Wrap(err, "error writing to output chunk")
			}

			outSize += int64(entrySize)
			uniqueCount++

			prevEntry = append([]byte(nil), minEntry...)

			// Check if output chunk is full
			if outSize >= outputChunkSize {
				if err := openNextOutputChunk(); err != nil {
					return 0, err
				}
			}
		}

		// Advance the input that provided the minimum
		inp := inputs[minIdx]
		if n, err := io.ReadFull(inp.reader, inp.buffer); err != nil || n != entrySize {
			inp.eof = true
		}
	}

	contentlog.Log1(ctx, log, "Created output chunks", logparam.Int64("chunks", outChunkIdx))

	return uniqueCount, nil
}

func findInUseContentIDs(ctx context.Context, log *contentlog.Logger, rep repo.Repository) (string, error) {
	ids, err := snapshot.ListSnapshotManifests(ctx, rep, nil, nil)
	if err != nil {
		return "", errors.Wrap(err, "unable to list snapshot manifest IDs")
	}

	manifests, err := snapshot.LoadSnapshots(ctx, rep, ids)
	if err != nil {
		return "", errors.Wrap(err, "unable to load manifest IDs")
	}

	gcTempDir := filepath.Join(os.TempDir(), "kopia-gc")

	// Step 1: Collect objects to sorted chunk files (buffered, sorted, deduplicated)
	contentlog.Log(ctx, log, "Step 1: Collecting objects from snapshots...")
	objectsTempPath := filepath.Join(gcTempDir, "objects-temp")
	objectChunkFiles, uniqueObjects, err := collectObjectsToChunks(ctx, log, rep, manifests, objectsTempPath)
	if err != nil {
		return "", err
	}

	contentlog.Log1(ctx, log, "Collected unique objects", logparam.Int64("count", uniqueObjects))

	// Step 2: K-way merge object chunks into final sorted chunks
	contentlog.Log(ctx, log, "Step 2: Merging object chunks...")
	objectsBasePath := filepath.Join(gcTempDir, "objects-sorted")
	uniqueObjects, err = kWayMergeToChunks(ctx, log, objectChunkFiles, objectsBasePath, objectIDSize)
	if err != nil {
		return "", err
	}

	// Delete temp object chunks immediately (Optimization 1!)
	for _, chunkFile := range objectChunkFiles {
		os.Remove(chunkFile)
	}

	contentlog.Log1(ctx, log, "Merged objects", logparam.Int64("uniqueCount", uniqueObjects))

	// Step 3: Process sorted objects to collect used content IDs in sorted chunks
	contentlog.Log(ctx, log, "Step 3: Collecting used content IDs from objects...")
	contentsTempPath := filepath.Join(gcTempDir, "contents-temp")
	contentChunkFiles, uniqueContents, err := collectUsedContentsToChunks(ctx, log, rep, objectsBasePath, contentsTempPath)
	if err != nil {
		// Cleanup object chunks
		cleanupChunks(objectsBasePath)
		return "", err
	}

	// Delete sorted object chunks immediately (Optimization 1!)
	cleanupChunks(objectsBasePath)

	contentlog.Log1(ctx, log, "Collected unique content IDs", logparam.Int64("count", uniqueContents))

	// Step 4: K-way merge content chunks into final sorted chunks
	contentlog.Log(ctx, log, "Step 4: Merging content chunks...")
	usedContentsBasePath := filepath.Join(gcTempDir, "used-contents-sorted")
	uniqueContents, err = kWayMergeToChunks(ctx, log, contentChunkFiles, usedContentsBasePath, contentIDSize)
	if err != nil {
		// Cleanup temp content chunks
		for _, chunkFile := range contentChunkFiles {
			os.Remove(chunkFile)
		}
		return "", err
	}

	// Delete temp content chunks immediately (Optimization 1!)
	for _, chunkFile := range contentChunkFiles {
		os.Remove(chunkFile)
	}

	contentlog.Log1(ctx, log, "Merged content IDs", logparam.Int64("uniqueCount", uniqueContents))

	return usedContentsBasePath, nil
}

// collectUsedContentsToChunks processes sorted objects and collects their backing content IDs.
// Uses double buffering: while one buffer is being flushed (sorted/written), collection continues into the other buffer.
// Uses concurrent VerifyObject calls to overlap I/O wait times.
func collectUsedContentsToChunks(ctx context.Context, log *contentlog.Logger, rep repo.Repository, objectsBasePath string, outputBase string) ([]string, int64, error) {
	maxBufferEntries := int(collectBufferSize / contentIDSize)

	// Double buffers for collection
	bufferA := make([][]byte, 0, maxBufferEntries)
	bufferB := make([][]byte, 0, maxBufferEntries)
	activeBuffer := &bufferA

	var (
		totalWritten int64
		chunkFiles   []string
		chunkIndex   int
		flushMutex   sync.Mutex
		flushWG      sync.WaitGroup
	)

	// flushBuffer sorts, deduplicates, and writes a buffer to a chunk file (runs in background)
	flushBuffer := func(buf [][]byte) error {
		if len(buf) == 0 {
			return nil
		}

		// Sort buffer
		sort.Slice(buf, func(i, j int) bool {
			return bytes.Compare(buf[i], buf[j]) < 0
		})

		// Deduplicate in-place
		writeIdx := 1
		for readIdx := 1; readIdx < len(buf); readIdx++ {
			if !bytes.Equal(buf[readIdx], buf[writeIdx-1]) {
				if writeIdx != readIdx {
					buf[writeIdx] = buf[readIdx]
				}
				writeIdx++
			}
		}
		buf = buf[:writeIdx]

		// Write to chunk file
		flushMutex.Lock()
		myChunkIndex := chunkIndex
		chunkIndex++
		flushMutex.Unlock()

		chunkPath := fmt.Sprintf("%s_chunk_%03d.bin", outputBase, myChunkIndex)
		chunkFile, err := os.Create(chunkPath)
		if err != nil {
			return errors.Wrapf(err, "error creating chunk file %s", chunkPath)
		}
		defer chunkFile.Close()

		writer := bufio.NewWriterSize(chunkFile, 1<<20)
		for _, cidBytes := range buf {
			if _, err := writer.Write(cidBytes); err != nil {
				return errors.Wrap(err, "error writing content ID")
			}
		}

		if err := writer.Flush(); err != nil {
			return errors.Wrap(err, "error flushing chunk")
		}

		flushMutex.Lock()
		chunkFiles = append(chunkFiles, chunkPath)
		totalWritten += int64(len(buf))
		flushMutex.Unlock()

		runtime.GC()
		debug.FreeOSMemory()

		return nil
	}

	// Read sorted objects using chunk reader
	objReader := newChunkReader(ctx, objectsBasePath, objectIDSize)
	defer objReader.close()

	contentlog.Log(ctx, log, "Collecting used content IDs from objects (concurrent, double-buffered)...")

	// Channel for object IDs to process
	objectChan := make(chan object.ID, verifyWorkers*2)

	// Channel for results (content IDs)
	type verifyResult struct {
		contentIDs []content.ID
		err        error
	}
	resultChan := make(chan verifyResult, verifyWorkers*2)

	// Start worker goroutines for concurrent VerifyObject calls
	var workerWG sync.WaitGroup
	for i := 0; i < verifyWorkers; i++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			for oid := range objectChan {
				contentIDs, err := rep.VerifyObject(ctx, oid)
				resultChan <- verifyResult{contentIDs: contentIDs, err: err}
			}
		}()
	}

	// Goroutine to close result channel when all workers are done
	go func() {
		workerWG.Wait()
		close(resultChan)
	}()

	// Goroutine to feed object IDs to workers
	go func() {
		defer close(objectChan)
		for {
			oidBytes := objReader.next()
			if oidBytes == nil {
				break
			}

			oid, err := object.ParseID(string(oidBytes))
			if err != nil {
				continue
			}

			select {
			case objectChan <- oid:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Main loop: collect results and buffer them
	var processedCount int64
	for result := range resultChan {
		if result.err != nil {
			continue
		}

		// Add each content ID to active buffer
		var cidbuf [128]byte
		for _, cid := range result.contentIDs {
			cidBytes := cid.Append(cidbuf[:0])
			*activeBuffer = append(*activeBuffer, append([]byte(nil), cidBytes...))

			// Switch buffers when full
			if len(*activeBuffer) >= maxBufferEntries {
				// Start flushing current buffer in background
				bufferToFlush := *activeBuffer
				flushWG.Add(1)
				go func(buf [][]byte) {
					defer flushWG.Done()
					if err := flushBuffer(buf); err != nil {
						contentlog.Log(ctx, log, "Error flushing buffer")
					}
				}(bufferToFlush)

				// Switch to other buffer
				if activeBuffer == &bufferA {
					bufferB = bufferB[:0]
					activeBuffer = &bufferB
				} else {
					bufferA = bufferA[:0]
					activeBuffer = &bufferA
				}
			}
		}

		processedCount++
		if processedCount%100000 == 0 {
			contentlog.Log1(ctx, log, "Processed objects", logparam.Int64("count", processedCount))
		}
	}

	// Flush remaining active buffer
	if len(*activeBuffer) > 0 {
		if err := flushBuffer(*activeBuffer); err != nil {
			return nil, 0, err
		}
	}

	// Wait for all background flushes to complete
	flushWG.Wait()

	contentlog.Log1(ctx, log, "Created sorted chunks", logparam.Int64("uniqueCount", totalWritten))

	return chunkFiles, totalWritten, nil
}

// cleanupChunks removes all chunk files with the given base path.
func cleanupChunks(basePath string) {
	for i := 0; i < 10000; i++ {
		chunkPath := fmt.Sprintf("%s_chunk_%03d.bin", basePath, i)
		if err := os.Remove(chunkPath); err != nil {
			break // No more chunks
		}
	}
}

// Run performs garbage collection on all the snapshots in the repository.
func Run(ctx context.Context, rep repo.DirectRepositoryWriter, gcDelete bool, safety maintenance.SafetyParameters, maintenanceStartTime time.Time) error {
	err := maintenance.ReportRun(ctx, rep, maintenance.TaskSnapshotGarbageCollection, nil, func() (maintenancestats.Kind, error) {
		return runInternal(ctx, rep, gcDelete, safety, maintenanceStartTime)
	})

	return errors.Wrap(err, "error running snapshot gc")
}

func runInternal(ctx context.Context, rep repo.DirectRepositoryWriter, gcDelete bool, safety maintenance.SafetyParameters, maintenanceStartTime time.Time) (*maintenancestats.SnapshotGCStats, error) {
	ctx = contentlog.WithParams(ctx,
		logparam.String("span:snapshot-gc", contentlog.RandomSpanID()))

	log := rep.LogManager().NewLogger("maintenance-snapshot-gc")

	// Set more aggressive GC to keep heap tighter during GC operation.
	// This reduces peak memory at the cost of slightly more CPU for garbage collection.
	// GOGC=50 means GC triggers when heap grows 50% (vs default 100%).
	oldGOGC := debug.SetGCPercent(50)
	defer debug.SetGCPercent(oldGOGC)

	// Phase 1-4: Collect, sort, and deduplicate used content IDs to chunked files
	// This eliminates the need for bigmap and achieves constant ~100 MB peak memory
	usedContentsBasePath, err := findInUseContentIDs(ctx, log, rep)
	if err != nil {
		return nil, errors.Wrap(err, "unable to find in-use content IDs")
	}
	defer cleanupChunks(usedContentsBasePath) // Cleanup at the end

	// Phase 5: Merge scan to find unreferenced contents
	return findUnreferencedByMergeScan(ctx, log, rep, gcDelete, safety, maintenanceStartTime, usedContentsBasePath)
}

func findUnreferencedByMergeScan(
	ctx context.Context,
	log *contentlog.Logger,
	rep repo.DirectRepositoryWriter,
	gcDelete bool,
	safety maintenance.SafetyParameters,
	maintenanceStartTime time.Time,
	usedContentsBasePath string,
) (*maintenancestats.SnapshotGCStats, error) {
	var unused, inUse, system, tooRecent, undeleted, deleted stats.CountSum

	contentlog.Log(ctx, log, "Phase 5: Finding unreferenced contents via sorted merge scan...")

	// Create chunk reader for sorted used content IDs
	usedReader := newChunkReader(ctx, usedContentsBasePath, contentIDSize)
	defer usedReader.close()

	// Read first used content ID
	nextUsed := usedReader.next()

	// Iterate all contents (sorted) and perform merge scan
	err := rep.ContentReader().IterateContents(ctx, content.IterateOptions{IncludeDeleted: true}, func(ci content.Info) error {
		if manifest.ContentPrefix == ci.ContentID.Prefix() {
			system.Add(int64(ci.PackedLength))
			return nil
		}

		var cidbuf [128]byte
		ciBytes := ci.ContentID.Append(cidbuf[:0])

		// Advance usedReader until we find a match or pass the current content
		isUsed := false
		for nextUsed != nil {
			cmp := bytes.Compare(ciBytes, nextUsed)
			if cmp == 0 {
				// Match found - content is in use
				isUsed = true
				nextUsed = usedReader.next() // Advance for next comparison
				break
			} else if cmp < 0 {
				// Current content < next used ID → content is not in used set
				break
			}
			// Current content > next used ID → advance used reader
			nextUsed = usedReader.next()
		}

		if isUsed {
			if ci.Deleted {
				if err := rep.ContentManager().UndeleteContent(ctx, ci.ContentID); err != nil {
					return errors.Wrapf(err, "Could not undelete referenced content: %v", ci)
				}

				undeleted.Add(int64(ci.PackedLength))
			}

			inUse.Add(int64(ci.PackedLength))
			return nil
		}

		// Content is not in use
		if maintenanceStartTime.Sub(ci.Timestamp()) < safety.MinContentAgeSubjectToGC {
			contentlog.Log3(ctx, log,
				"recent unreferenced content",
				contentparam.ContentID("contentID", ci.ContentID),
				logparam.Int64("bytes", int64(ci.PackedLength)),
				logparam.Time("modified", ci.Timestamp()))
			tooRecent.Add(int64(ci.PackedLength))
			return nil
		}

		contentlog.Log3(ctx, log,
			"unreferenced content",
			contentparam.ContentID("contentID", ci.ContentID),
			logparam.Int64("bytes", int64(ci.PackedLength)),
			logparam.Time("modified", ci.Timestamp()))

		cnt, totalSize := unused.Add(int64(ci.PackedLength))

		if gcDelete {
			if err := rep.ContentManager().DeleteContent(ctx, ci.ContentID); err != nil {
				return errors.Wrap(err, "error deleting content")
			}

			deleted.Add(int64(ci.PackedLength))
		}

		if cnt%100000 == 0 {
			contentlog.Log2(ctx, log,
				"found unused contents so far",
				logparam.UInt32("count", cnt),
				logparam.Int64("bytes", totalSize))

			if gcDelete {
				if err := rep.Flush(ctx); err != nil {
					return errors.Wrap(err, "flush error")
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, errors.Wrap(err, "error iterating contents")
	}

	result := buildGCResult(&unused, &inUse, &system, &tooRecent, &undeleted, &deleted)

	userLog(ctx).Infof("GC found %v unused contents (%v)", result.UnreferencedContentCount, units.BytesString(result.UnreferencedContentSize))
	userLog(ctx).Infof("GC found %v unused contents that are too recent to delete (%v)", result.UnreferencedRecentContentCount, units.BytesString(result.UnreferencedRecentContentSize))
	userLog(ctx).Infof("GC found %v in-use contents (%v)", result.InUseContentCount, units.BytesString(result.InUseContentSize))
	userLog(ctx).Infof("GC found %v in-use system-contents (%v)", result.InUseSystemContentCount, units.BytesString(result.InUseSystemContentSize))
	userLog(ctx).Infof("GC undeleted %v contents (%v)", result.RecoveredContentCount, units.BytesString(result.RecoveredContentSize))

	contentlog.Log1(ctx, log, "Snapshot GC", result)

	if err := rep.Flush(ctx); err != nil {
		return nil, errors.Wrap(err, "flush error")
	}

	if result.UnreferencedContentCount > 0 && !gcDelete {
		return result, errors.New("Not deleting because 'gcDelete' was not set")
	}

	return result, nil
}

func buildGCResult(unused, inUse, system, tooRecent, undeleted, deleted *stats.CountSum) *maintenancestats.SnapshotGCStats {
	result := &maintenancestats.SnapshotGCStats{}

	cnt, size := unused.Approximate()
	result.UnreferencedContentCount = cnt
	result.UnreferencedContentSize = size

	cnt, size = tooRecent.Approximate()
	result.UnreferencedRecentContentCount = cnt
	result.UnreferencedRecentContentSize = size

	cnt, size = inUse.Approximate()
	result.InUseContentCount = cnt
	result.InUseContentSize = size

	cnt, size = system.Approximate()
	result.InUseSystemContentCount = cnt
	result.InUseSystemContentSize = size

	cnt, size = undeleted.Approximate()
	result.RecoveredContentCount = cnt
	result.RecoveredContentSize = size

	cnt, size = deleted.Approximate()
	result.DeletedContentCount = cnt
	result.DeletedContentSize = size

	return result
}
