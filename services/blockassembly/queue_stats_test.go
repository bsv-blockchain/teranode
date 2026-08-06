package blockassembly

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// newQueueStatsTestServer builds a minimal BlockAssembly backed by a mock
// subtree processor so the queue-stats/state handlers can be exercised without a
// running assembler.
func newQueueStatsTestServer(t *testing.T, mockStp *subtreeprocessor.MockSubtreeProcessor) *BlockAssembly {
	t.Helper()

	logger := ulogger.TestLogger{}

	assembler := &BlockAssembler{
		subtreeProcessor: mockStp,
		logger:           logger,
		settings:         &settings.Settings{},
	}
	assembler.setCurrentRunningState(StateRunning)

	// A valid best block header so CurrentBlock() does not return nil.
	var buf bytes.Buffer
	require.NoError(t, chaincfg.RegressionNetParams.GenesisBlock.Serialize(&buf))
	genesisBlock, err := model.NewBlockFromBytes(buf.Bytes())
	require.NoError(t, err)
	assembler.setBestBlockHeader(genesisBlock.Header, 0)

	return &BlockAssembly{
		blockAssembler: assembler,
		logger:         logger,
		stats:          gocore.NewStat("blockassembly_test"),
		settings:       &settings.Settings{},
	}
}

// TestGetBlockAssemblyQueueStats verifies the slim RPC returns the queue depth
// and head-batch age straight from the atomic-backed getters, and crucially that
// it never reaches for GetSubtreeHashes (the path that can block on the
// subtree-processor main loop).
func TestGetBlockAssemblyQueueStats(t *testing.T) {
	initPrometheusMetrics()

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	mockStp.On("QueueLength").Return(int64(7))
	mockStp.On("QueueHeadAge").Return(750 * time.Millisecond)
	// Registered but must NOT be called by the slim RPC. If the handler ever
	// touched it, this would block far longer than the assertion below tolerates.
	mockStp.On("GetSubtreeHashes", mock.Anything).Run(func(mock.Arguments) {
		time.Sleep(5 * time.Second)
	}).Return([]chainhash.Hash{}).Maybe()

	ba := newQueueStatsTestServer(t, mockStp)

	start := time.Now()
	resp, err := ba.GetBlockAssemblyQueueStats(context.Background(), &blockassembly_api.EmptyMessage{})
	elapsed := time.Since(start)

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, int64(7), resp.QueueCount)
	require.Equal(t, int64(750), resp.QueueHeadAgeMillis)

	// Returns immediately: it does not depend on the (deliberately slow) main-loop path.
	require.Less(t, elapsed, time.Second, "queue-stats read must not block on the subtree-processor main loop")
	mockStp.AssertNotCalled(t, "GetSubtreeHashes", mock.Anything)
}

// TestGetBlockAssemblyState_QueueHeadAge verifies the head-batch age is now
// surfaced on the full state RPC alongside the existing queue depth.
func TestGetBlockAssemblyState_QueueHeadAge(t *testing.T) {
	initPrometheusMetrics()

	mockStp := &subtreeprocessor.MockSubtreeProcessor{}
	mockStp.On("QueueLength").Return(int64(3))
	mockStp.On("QueueHeadAge").Return(1234 * time.Millisecond)
	mockStp.On("TxCount").Return(uint64(0))
	mockStp.On("SubtreeCount").Return(0)
	mockStp.On("GetCurrentSubtreeSize").Return(0)
	mockStp.On("GetRemoveMapLength").Return(0)
	mockStp.On("GetCurrentRunningState").Return(subtreeprocessor.StateRunning)
	mockStp.On("GetSubtreeHashes", mock.Anything).Return([]chainhash.Hash{})

	ba := newQueueStatsTestServer(t, mockStp)

	resp, err := ba.GetBlockAssemblyState(context.Background(), &blockassembly_api.EmptyMessage{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, int64(1234), resp.QueueHeadAgeMillis)
	require.Equal(t, int64(3), resp.QueueCount)
}
