// Package snapshotgc implements garbage collection of contents that are no longer referenced through snapshots.
package snapshotgc

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/bigmap"
	"github.com/kopia/kopia/internal/contentlog"
	"github.com/kopia/kopia/internal/contentlog/logparam"
	"github.com/kopia/kopia/internal/contentparam"
	"github.com/kopia/kopia/internal/sortedchunk"
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
	cacheSize     = 100 << 20
	chunkSize     = 64 << 20
	objectIDSize  = 75
	contentIDSize = 70
)

func findInUseObjectIDs(ctx context.Context, log *contentlog.Logger, rep repo.Repository, writer sortedchunk.List) error {
	ids, err := snapshot.ListSnapshotManifests(ctx, rep, nil, nil)
	if err != nil {
		return errors.Wrap(err, "unable to list snapshot manifest IDs")
	}

	manifests, err := snapshot.LoadSnapshots(ctx, rep, ids)
	if err != nil {
		return errors.Wrap(err, "unable to load manifest IDs")
	}

	w, twerr := snapshotfs.NewTreeWalker(ctx, snapshotfs.TreeWalkerOptions{
		EntryCallback: func(ctx context.Context, _ fs.Entry, oid object.ID, _ string) error {
			var oidbuf [objectIDSize]byte
			if err := writer.Append(oid.Append(oidbuf[:0])); err != nil {
				return errors.Wrapf(err, "error storing objectID %v", oid)
			}

			return nil
		},
	})
	if twerr != nil {
		return errors.Wrap(twerr, "unable to create tree walker")
	}

	defer w.Close(ctx)

	contentlog.Log(ctx, log, "Looking for active objects...")

	for _, m := range manifests {
		root, err := snapshotfs.SnapshotRoot(rep, m)
		if err != nil {
			return errors.Wrap(err, "unable to get snapshot root")
		}

		if err := w.Process(ctx, root, ""); err != nil {
			return errors.Wrap(err, "error processing snapshot root")
		}
	}

	return nil
}

func findInUseContentIDs(ctx context.Context, log *contentlog.Logger, rep repo.Repository, usedObjects sortedchunk.Iterator, writer sortedchunk.List, parallelism int) error {
	objectChan := make(chan object.ID, parallelism*2)

	type verifyResult struct {
		contentIDs []content.ID
		err        error
	}
	resultChan := make(chan verifyResult, parallelism*2)

	var workerWG sync.WaitGroup
	for i := 0; i < parallelism; i++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			for oid := range objectChan {
				contentIDs, err := rep.VerifyObject(ctx, oid)
				resultChan <- verifyResult{contentIDs: contentIDs, err: err}
			}
		}()
	}

	go func() {
		workerWG.Wait()
		close(resultChan)
	}()

	go func() {
		defer close(objectChan)
		for {
			oidBytes, err := usedObjects.Next()
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

	contentlog.Log(ctx, log, "Looking for active contents...")

	for result := range resultChan {
		if result.err != nil {
			return errors.Wrap(result.err, "error retrieving contentID")
		}

		for _, cid := range result.contentIDs {
			var cidbuf [contentIDSize]byte
			if err := writer.Append(cid.Append(cidbuf[:0])); err != nil {
				return errors.Wrapf(err, "error storing contentID %v", cid)
			}
		}
	}

	return nil
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

	usedObjects := sortedchunk.NewList(ctx, os.TempDir(), cacheSize, chunkSize, objectIDSize)
	defer usedObjects.Close()

	if err := usedObjects.Finalize(); err != nil {
		return nil, errors.Wrap(err, "error finalizing object ID list")
	}

	if err := findInUseObjectIDs(ctx, log, rep, usedObjects); err != nil {
		return nil, errors.Wrap(err, "unable to find in-use object ID")
	}

	usedContents := sortedchunk.NewList(ctx, os.TempDir(), cacheSize, chunkSize, contentIDSize)
	defer usedContents.Close()

	if err := findInUseContentIDs(ctx, log, rep, usedObjects.GetIterator(), usedContents); err != nil {
		return nil, errors.Wrap(err, "unable to find in-use content ID")
	}

	return findUnreferencedAndRepairRereferenced(ctx, log, rep, gcDelete, safety, maintenanceStartTime, used)
}

func findUnreferencedAndRepairRereferenced(
	ctx context.Context,
	log *contentlog.Logger,
	rep repo.DirectRepositoryWriter,
	gcDelete bool,
	safety maintenance.SafetyParameters,
	maintenanceStartTime time.Time,
	used *bigmap.Set,
) (*maintenancestats.SnapshotGCStats, error) {
	var unused, inUse, system, tooRecent, undeleted, deleted stats.CountSum

	contentlog.Log(ctx, log, "Looking for unreferenced contents...")

	// Ensure that the iteration includes deleted contents, so those can be
	// undeleted (recovered).
	err := rep.ContentReader().IterateContents(ctx, content.IterateOptions{IncludeDeleted: true}, func(ci content.Info) error {
		if manifest.ContentPrefix == ci.ContentID.Prefix() {
			system.Add(int64(ci.PackedLength))
			return nil
		}

		var cidbuf [128]byte

		if used.Contains(ci.ContentID.Append(cidbuf[:0])) {
			if ci.Deleted {
				if err := rep.ContentManager().UndeleteContent(ctx, ci.ContentID); err != nil {
					return errors.Wrapf(err, "Could not undelete referenced content: %v", ci)
				}

				undeleted.Add(int64(ci.PackedLength))
			}

			inUse.Add(int64(ci.PackedLength))

			return nil
		}

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

	result := buildGCResult(&unused, &inUse, &system, &tooRecent, &undeleted, &deleted)

	userLog(ctx).Infof("GC found %v unused contents (%v)", result.UnreferencedContentCount, units.BytesString(result.UnreferencedContentSize))
	userLog(ctx).Infof("GC found %v unused contents that are too recent to delete (%v)", result.UnreferencedRecentContentCount, units.BytesString(result.UnreferencedRecentContentSize))
	userLog(ctx).Infof("GC found %v in-use contents (%v)", result.InUseContentCount, units.BytesString(result.InUseContentSize))
	userLog(ctx).Infof("GC found %v in-use system-contents (%v)", result.InUseSystemContentCount, units.BytesString(result.InUseSystemContentSize))
	userLog(ctx).Infof("GC undeleted %v contents (%v)", result.RecoveredContentCount, units.BytesString(result.RecoveredContentSize))

	contentlog.Log1(ctx, log, "Snapshot GC", result)

	if err != nil {
		return nil, errors.Wrap(err, "error iterating contents")
	}

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
