package validator

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator/validator_api"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestProcessBatchResponse_ShortResponseCompletesEveryItem is a regression test
// for the short-response hazard: if the validator server returns fewer
// Errors/Metadata entries than the batch size (a contract violation), every
// batch item must still be completed — with a clear error — rather than
// index-panicking into the sweep (which would release callers with an opaque
// "panic in sendBatchToValidator" error). Mirrors the propagation client guard.
func TestProcessBatchResponse_ShortResponseCompletesEveryItem(t *testing.T) {
	client := &Client{logger: ulogger.TestLogger{}}

	batch := make([]*batchItem, 3)
	group := completion.NewGroup(int32(len(batch)))

	for i := range batch {
		batch[i] = &batchItem{group: group}
	}

	// Server returns only 1 error/metadata entry for a 3-item batch.
	resp := &validator_api.ValidateTransactionBatchResponse{
		Errors:   make([]*errors.TError, 1),
		Metadata: make([][]byte, 1),
	}

	client.processBatchResponse(batch, resp)

	// Bounded wait: if any item were left stranded this would time out and fail.
	require.NoError(t, group.Wait(context.Background(), 2*time.Second))

	for i, item := range batch {
		require.Error(t, item.result.err, "batch item %d must be completed with an error, not stranded", i)
	}
}
