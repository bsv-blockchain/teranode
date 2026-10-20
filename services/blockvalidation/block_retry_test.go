package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func newRetryTestServer(t *testing.T, maxRetries int) *Server {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxRetries = maxRetries

	return &Server{
		logger:             ulogger.TestLogger{},
		settings:           tSettings,
		blockPriorityQueue: NewBlockPriorityQueue(ulogger.TestLogger{}),
		blockRetryAttempts: newBlockRetryAttemptsCache(),
	}
}

// TestBlockRetryDelay_DeadlineExceededIsRetried pins that a block whose subtree check hit
// its deadline goes back on the queue. The subtree client returns the gRPC deadline as a
// plain ERR_ERROR that matches neither the corrupt nor the network branch, so the worker
// used to drop it.
func TestBlockRetryDelay_DeadlineExceededIsRetried(t *testing.T) {
	server := newRetryTestServer(t, 3)
	hash := &chainhash.Hash{1}

	err := errors.NewServiceError("failed block validation BlockFound", errors.New(errors.ERR_ERROR, "context deadline exceeded"))

	delay, ok := server.blockRetryDelay(hash, err)
	require.True(t, ok)
	require.Equal(t, blockDeadlineRetryDelay, delay)
}

func TestBlockRetryDelay_NetworkErrorIsRetried(t *testing.T) {
	server := newRetryTestServer(t, 3)

	delay, ok := server.blockRetryDelay(&chainhash.Hash{1}, errors.NewNetworkError("eof"))
	require.True(t, ok)
	require.Equal(t, blockFetchRetryDelay, delay)
}

func TestBlockRetryDelay_OtherErrorsAreNotRetried(t *testing.T) {
	server := newRetryTestServer(t, 3)

	_, ok := server.blockRetryDelay(&chainhash.Hash{1}, errors.NewBlockInvalidError("bad block"))
	require.False(t, ok)

	_, ok = server.blockRetryDelay(&chainhash.Hash{1}, context.Canceled)
	require.False(t, ok, "shutdown must not requeue")
}

// TestBlockRetryDelay_CappedByMaxRetries pins BlockValidation.MaxRetries as the per-block
// requeue budget. The counter lives outside the queue item because the item is removed
// when a worker takes it.
func TestBlockRetryDelay_CappedByMaxRetries(t *testing.T) {
	server := newRetryTestServer(t, 2)
	hash := &chainhash.Hash{1}
	err := errors.NewNetworkError("eof")

	for i := 0; i < 2; i++ {
		_, ok := server.blockRetryDelay(hash, err)
		require.True(t, ok, "attempt %d", i+1)
	}

	_, ok := server.blockRetryDelay(hash, err)
	require.False(t, ok, "retries beyond MaxRetries must be dropped")

	_, ok = server.blockRetryDelay(&chainhash.Hash{2}, err)
	require.True(t, ok, "the budget is per block hash")

	server.clearBlockRetryAttempts(hash)

	_, ok = server.blockRetryDelay(hash, err)
	require.True(t, ok, "a success resets the budget")
}

// TestRequeueForRetry_KeepsSourcePriorityAndHeight pins that a retried block keeps the peer
// it was announced by, its class and its height. The old retry blanked the source and set
// the "retry" marker, which then looked for alternative sources on a queue item that was
// already gone, so the retry always failed with "no sources available".
func TestRequeueForRetry_KeepsSourcePriorityAndHeight(t *testing.T) {
	initPrometheusMetrics()
	server := newRetryTestServer(t, 3)
	hash := &chainhash.Hash{1}

	server.blockPriorityQueue.Add(processBlockFound{hash: hash, baseURL: "http://peer", peerID: "p1"}, PriorityNearFork, 42)

	taken, status := server.blockPriorityQueue.Get(context.Background(), &mockBlockProcessor{})
	require.Equal(t, GetOK, status)

	server.requeueBlock(taken, 0)

	item, ok := server.blockPriorityQueue.hashIndex[*hash]
	require.True(t, ok)
	require.Equal(t, "http://peer", item.blockFound.baseURL)
	require.Equal(t, "p1", item.blockFound.peerID)
	require.Nil(t, item.blockFound.errCh)
	require.Equal(t, PriorityNearFork, item.priority)
	require.Equal(t, uint32(42), item.height)
}

// TestBlockRetryDelay_DeadlineWinsOverNetworkSubstring pins the check order: IsNetworkError
// matches substrings such as "http", so a deadline error mentioning a URL must still get the
// deadline delay.
func TestBlockRetryDelay_DeadlineWinsOverNetworkSubstring(t *testing.T) {
	server := newRetryTestServer(t, 3)

	err := errors.NewServiceError("validating block from http://peer", errors.New(errors.ERR_ERROR, "context deadline exceeded"))

	delay, ok := server.blockRetryDelay(&chainhash.Hash{1}, err)
	require.True(t, ok)
	require.Equal(t, blockDeadlineRetryDelay, delay)
}
