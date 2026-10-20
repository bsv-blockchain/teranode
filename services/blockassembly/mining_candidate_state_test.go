package blockassembly

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// TestGetMiningCandidate_BlockProcessingStatesBuildOnChainTip covers every state
// in which processNewBlockAnnouncement or a reset is still moving block assembly
// to the chain tip. In each one the Blockchain service already has the new tip
// and block assembly still holds the old one, so a candidate built on the block
// assembly tip would mine on the losing side of a reorg. The candidate must be
// an empty block on the Blockchain service tip instead.
func TestGetMiningCandidate_BlockProcessingStatesBuildOnChainTip(t *testing.T) {
	for _, state := range []State{
		StateBlockchainSubscription,
		StateMovingUp,
		StateReorging,
		StateReconciling,
		StateResetting,
	} {
		t.Run(StateStrings[state], func(t *testing.T) {
			initPrometheusMetrics()

			testItems := setupBlockAssemblyTest(t)
			ba := testItems.blockAssembler
			_, _, genesisBlock := setupBlockchainClient(t, testItems)

			coinbaseTx, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
			require.NoError(t, err)

			require.NoError(t, ba.blockchainClient.AddBlock(t.Context(), &model.Block{
				Header:           blockHeader1,
				CoinbaseTx:       coinbaseTx,
				TransactionCount: 1,
				Subtrees:         []*chainhash.Hash{},
			}, ""))

			chainTip, _, err := ba.blockchainClient.GetBestBlockHeader(t.Context())
			require.NoError(t, err)
			require.Equal(t, blockHeader1.Hash(), chainTip.Hash(), "the Blockchain service must hold the new tip")

			baTip, _ := ba.CurrentBlock()
			require.Equal(t, genesisBlock.Header.Hash(), baTip.Hash(), "block assembly must still hold the old tip")

			ba.setCurrentRunningState(state)

			candidate, subtrees, err := ba.GetMiningCandidate(t.Context())
			require.NoError(t, err)
			require.NotNil(t, candidate)

			require.Equal(t, blockHeader1.Hash().CloneBytes(), candidate.PreviousHash, "the candidate must build on the Blockchain service tip")
			require.Equal(t, uint32(2), candidate.Height)
			require.Empty(t, subtrees, "the candidate must not carry block assembly's pre-tip transactions")
		})
	}
}
