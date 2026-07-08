package subtreeprocessor

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestDynamicSizing_Mechanism_NonPowerOfTwoMaximumStillKeepsPowerOfTwoSize(t *testing.T) {
	stp := newDynamicSizingProcessor(t, 2048, 4, 3000) // Intentionally non-power-of-two max.
	stp.currentItemsPerFile.Store(2048)
	setSubtreeNodeSamples(stp, []int{2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000})
	stp.blockIntervals = repeatInterval(50*time.Millisecond, 3)

	stp.adjustSubtreeSize()
	got := stp.currentItemsPerFile.Load()

	require.LessOrEqual(t, int(got), 3000, "mechanism must respect configured maximum")
	require.True(t, got > 0 && got&(got-1) == 0,
		"mechanism must never produce a non-power-of-two subtree size, even when maximum is not a power of two")
}

func TestDynamicSizing_Mechanism_BlockIntervalSampling_IsPerBlockNotCumulative(t *testing.T) {
	stp := newDynamicSizingProcessor(t, 64, 4, 32768)
	stp.settings.BlockAssembly.UseDynamicSubtreeSize = false // keep interval samples intact for inspection

	mockBlockchainClient := &blockchain.Mock{}
	mockBlockchainClient.
		On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).
		Return(nil).
		Twice()
	stp.blockchainClient = mockBlockchainClient

	// Block N: 10 subtrees over ~1s => ~100ms per subtree.
	stp.blockStartTime = time.Now().Add(-1 * time.Second)
	stp.subtreesInBlock = 10
	stp.finalizeBlockProcessing(context.Background(), &model.Block{Header: testBlockHeader(0x41)})
	require.Len(t, stp.blockIntervals, 1)
	first := stp.blockIntervals[0]

	// Block N+1: one additional subtree over a much longer interval.
	time.Sleep(800 * time.Millisecond)
	stp.subtreesInBlock++
	stp.finalizeBlockProcessing(context.Background(), &model.Block{Header: testBlockHeader(0x42)})
	require.Len(t, stp.blockIntervals, 2)
	second := stp.blockIntervals[1]

	require.Greater(t, second, first*4,
		"per-block sampling should reflect the latest block duration/subtree count, not cumulative lifetime averages")
	mockBlockchainClient.AssertExpectations(t)
}
