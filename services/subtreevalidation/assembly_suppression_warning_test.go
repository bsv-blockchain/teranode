package subtreevalidation

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type suppressionWarningLogger struct {
	ulogger.TestLogger
	warnings atomic.Int64
}

func (l *suppressionWarningLogger) Warnf(string, ...interface{}) { l.warnings.Add(1) }

func TestAssemblySuppressionWarningIsRateLimited(t *testing.T) {
	InitPrometheusMetrics()
	logger := &suppressionWarningLogger{}
	server := &Server{logger: logger}
	state := blockchain.FSMStateIDLE
	counter := prometheusAssemblyFeedingSuppressed.WithLabelValues("kafka_subtree", "idle")
	before := testutil.ToFloat64(counter)
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() { defer wg.Done(); server.allowAssemblyForObservedFSM(&state, "kafka_subtree") }()
	}
	wg.Wait()
	require.Equal(t, int64(1), logger.warnings.Load(), "concurrent suppressed entries emit only one warning per minute")
	require.Equal(t, before+20, testutil.ToFloat64(counter), "rate limiting must not suppress the counter")
	server.assemblySuppressionLastWarning.Store(time.Now().Add(-2 * time.Minute).UnixNano())
	require.False(t, server.allowAssemblyForObservedFSM(&state, "kafka_subtree"))
	require.Equal(t, int64(2), logger.warnings.Load())
	state = blockchain.FSMStateRUNNING
	require.True(t, server.allowAssemblyForObservedFSM(&state, "kafka_subtree"))
	require.Equal(t, int64(2), logger.warnings.Load())
}

// Expected catchup must not consume the warning budget for a later IDLE
// observation on another path, which can indicate notification-stream loss.
func TestExpectedCatchupDoesNotHideIdleSuppressionWarning(t *testing.T) {
	InitPrometheusMetrics()
	logger := &suppressionWarningLogger{}
	server := &Server{logger: logger}
	state := blockchain.FSMStateCATCHINGBLOCKS
	counter := prometheusAssemblyFeedingSuppressed.WithLabelValues("check_subtree_legacy", "catchingblocks")
	before := testutil.ToFloat64(counter)
	require.False(t, server.allowAssemblyForObservedFSM(&state, "check_subtree_legacy"))
	require.Equal(t, before+1, testutil.ToFloat64(counter), "admitted catchup validation still records suppressed feeding")
	require.Zero(t, logger.warnings.Load(), "expected catchup is not a subscription-loss warning")

	state = blockchain.FSMStateIDLE
	require.False(t, server.allowAssemblyForObservedFSM(&state, "check_block_subtrees"))
	require.Equal(t, int64(1), logger.warnings.Load(), "catchup must not hide a subsequent IDLE warning")
}
