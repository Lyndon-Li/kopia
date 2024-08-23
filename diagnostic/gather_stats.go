package diagnostic

import (
	"context"

	"github.com/kopia/kopia/internal/gather"
)

func DumpStats(ctx context.Context) {
	gather.DumpStats(ctx)
}
