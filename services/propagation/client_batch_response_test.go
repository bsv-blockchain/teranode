package propagation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2/completion"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/propagation/propagation_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestHandleBatchResponse_ShortResponseCompletesEveryItem is a regression test
// for the stranded-caller path: if the propagation server returns fewer results
// than the batch size (a contract violation), every batch item must still be
// completed — with an error — so no single-item caller parks forever on the
// unbounded group.Wait. Before the length guard, ranging over response.Errors
// left the tail items un-completed and their callers stranded.
func TestHandleBatchResponse_ShortResponseCompletesEveryItem(t *testing.T) {
	client := &Client{logger: ulogger.TestLogger{}, settings: &settings.Settings{}}

	batch := make([]*batchItem, 3)
	group := completion.NewGroup(int32(len(batch)))

	for i := range batch {
		batch[i] = &batchItem{ctx: context.Background(), tx: bt.NewTx(), group: group}
	}

	// Server returns only 1 result for a 3-item batch (contract violation).
	response := &propagation_api.ProcessTransactionBatchResponse{
		Errors: make([]*errors.TError, 1),
	}

	client.handleBatchResponse(batch, response)

	// Bounded wait: if any item were left stranded this would time out and fail.
	require.NoError(t, group.Wait(context.Background(), 2*time.Second))

	for i, item := range batch {
		require.Error(t, item.result, "batch item %d must be completed with an error, not stranded", i)
	}
}
