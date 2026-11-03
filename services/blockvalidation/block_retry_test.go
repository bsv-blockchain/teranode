package blockvalidation

import (
	"bytes"
	"context"
	"io"
	"net"
	"syscall"
	"testing"
	"testing/iotest"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jellydator/ttlcache/v3"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

// TestFinishProcessedBlock_LabelsProcessedMetricWithPriority pins that the processed counter
// carries the block's class, which the queue sets when the block is taken.
func TestFinishProcessedBlock_LabelsProcessedMetricWithPriority(t *testing.T) {
	initPrometheusMetrics()
	server := newRetryTestServer(t, 0)
	server.forkManager = NewForkManager(ulogger.TestLogger{}, server.settings)
	server.processBlockNotify = ttlcache.New[chainhash.Hash, bool]()

	failures := prometheusBlockPriorityQueueProcessed.WithLabelValues("near_fork", "failure")
	successes := prometheusBlockPriorityQueueProcessed.WithLabelValues("near_fork", "success")
	failuresBefore, successesBefore := testutil.ToFloat64(failures), testutil.ToFloat64(successes)

	server.finishProcessedBlock(0, processBlockFound{hash: &chainhash.Hash{1}, priority: PriorityNearFork}, errors.NewProcessingError("boom"))
	server.finishProcessedBlock(0, processBlockFound{hash: &chainhash.Hash{2}, priority: PriorityNearFork}, nil)

	require.Equal(t, failuresBefore+1, testutil.ToFloat64(failures))
	require.Equal(t, successesBefore+1, testutil.ToFloat64(successes))
}

// TestBlockRetryDelay_InvalidVerdictMentioningNetworkNotRetried pins that a final invalid verdict
// is never retried, even when its text matches the network substring classifier. The
// proof-of-work limit rejection says "network proof-of-work limit".
func TestBlockRetryDelay_InvalidVerdictMentioningNetworkNotRetried(t *testing.T) {
	server := newRetryTestServer(t, 3)
	hash := &chainhash.Hash{1}

	limitErr := errors.NewBlockInvalidError("block nBits %s declares target %x, which is easier than the network proof-of-work limit %x", "207fffff", 255, 1)
	err := errors.NewServiceError("failed block validation BlockFound", errors.NewBlockInvalidError("[ValidateBlock][%s] block declares a target easier than the network proof-of-work limit", hash.String(), limitErr))
	require.True(t, errors.IsNetworkError(err), "precondition: the verdict matches the network classifier")

	_, ok := server.blockRetryDelay(hash, err)
	require.False(t, ok)
}

// TestBlockRetryDelay_BrokenDownloadIsRetried pins that a block whose download breaks mid-stream
// is retried. NewBlockFromReader wraps every read failure as ErrBlockInvalid, so the final
// verdict guard must not see a transport failure as a verdict.
func TestBlockRetryDelay_BrokenDownloadIsRetried(t *testing.T) {
	hash := &chainhash.Hash{1}

	tests := []struct {
		name   string
		reader io.Reader
		delay  time.Duration
	}{
		{"truncated body", bytes.NewReader(make([]byte, 10)), blockFetchRetryDelay},
		{"connection reset", iotest.ErrReader(&net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}), blockFetchRetryDelay},
		{"fetch deadline", iotest.ErrReader(context.DeadlineExceeded), blockDeadlineRetryDelay},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := newRetryTestServer(t, 3)

			_, readErr := model.NewBlockFromReader(tt.reader)
			require.True(t, errors.Is(readErr, errors.ErrBlockInvalid), "precondition: the reader wraps the failure as invalid")

			err := errors.NewProcessingError("[catchup:fetchSingleBlock][%s] failed to create block from bytes", hash.String(), readErr)

			delay, ok := server.blockRetryDelay(hash, err)
			require.True(t, ok)
			require.Equal(t, tt.delay, delay)
		})
	}
}
