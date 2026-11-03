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

// TestProcessBatchResponse_ItemErrorIsATeranodeError pins the type a batched
// verdict reaches the caller as. The server encodes each item's chain with
// errors.Wrap (Server.go, ValidateTransactionBatch), so the response carries
// *errors.TError. Handing that through unchanged meant every consumer that reads
// a Teranode error with errors.As - the JSON-RPC boundary among them - saw a
// foreign error and fell back to its rendered text, internal codes and all.
func TestProcessBatchResponse_ItemErrorIsATeranodeError(t *testing.T) {
	client := &Client{logger: ulogger.TestLogger{}}

	batch := []*batchItem{{}, {}}
	group := completion.NewGroup(int32(len(batch)))

	for _, item := range batch {
		item.group = group
	}

	verdict := errors.NewProcessingError("[Validate][%s] error validating transaction", "abcd1234",
		errors.NewTxInvalidError(ErrMsgInvalidTx,
			errors.New(errors.ERR_TX_INVALID, "bad-txns-inputs-duplicate")))

	resp := &validator_api.ValidateTransactionBatchResponse{
		Errors:   []*errors.TError{errors.Wrap(verdict), errors.Wrap(nil)},
		Metadata: [][]byte{nil, nil},
	}

	client.processBatchResponse(batch, resp)
	require.NoError(t, group.Wait(context.Background(), 2*time.Second))

	var tErr *errors.Error
	require.ErrorAs(t, batch[0].result.err, &tErr, "a batched verdict must reach the caller as a Teranode error")

	codes := make([]errors.ERR, 0, 3)
	messages := make([]string, 0, 3)

	for cur := tErr; cur != nil; {
		codes = append(codes, cur.Code())
		messages = append(messages, cur.Message())

		next, ok := cur.WrappedErr().(*errors.Error)
		if !ok {
			break
		}

		cur = next
	}

	require.Equal(t, []errors.ERR{errors.ERR_PROCESSING, errors.ERR_TX_INVALID, errors.ERR_TX_INVALID}, codes,
		"every link keeps its code, outermost first")
	require.Equal(t, "bad-txns-inputs-duplicate", messages[2], "the reason survives at the bottom")
	require.ErrorIs(t, batch[0].result.err, errors.ErrTxInvalid)

	require.NoError(t, batch[1].result.err, "a nil item error still means success")
}
