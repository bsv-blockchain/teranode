package blockassembly

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

// TestGetMiningCandidate_StaleTipWhileRunningBuildsOnChainTip covers issue 4991: block assembly
// failed to advance (a ProcessConflicting wedge) and sits idle in StateRunning on an old tip.
// A candidate built on that tip is an orphan, so the candidate must be an empty block on the
// Blockchain service tip.
func TestGetMiningCandidate_StaleTipWhileRunningBuildsOnChainTip(t *testing.T) {
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

	baTip, _ := ba.CurrentBlock()
	require.Equal(t, genesisBlock.Header.Hash(), baTip.Hash(), "block assembly must still hold the old tip")

	ba.setCurrentRunningState(StateRunning)

	before := testutil.ToFloat64(prometheusBlockAssemblyStaleTipCandidates)

	candidate, subtrees, err := ba.GetMiningCandidate(t.Context())
	require.NoError(t, err)
	require.NotNil(t, candidate)

	require.Equal(t, blockHeader1.Hash().CloneBytes(), candidate.PreviousHash, "the candidate must build on the Blockchain service tip")
	require.Equal(t, uint32(2), candidate.Height)
	require.Empty(t, subtrees)
	require.Equal(t, before, testutil.ToFloat64(prometheusBlockAssemblyStaleTipCandidates), "the normal gap before block assembly hears of a block is not a stall")

	// Age the gap past the grace period: the same stale tip is now reported.
	ba.staleTip.mu.Lock()
	ba.staleTip.since = time.Now().Add(-2 * staleTipGrace)
	ba.staleTip.mu.Unlock()

	candidate, _, err = ba.GetMiningCandidate(t.Context())
	require.NoError(t, err)
	require.Equal(t, blockHeader1.Hash().CloneBytes(), candidate.PreviousHash)
	require.Equal(t, before+1, testutil.ToFloat64(prometheusBlockAssemblyStaleTipCandidates), "a sustained gap must be counted")
}

func TestStaleTipTracker(t *testing.T) {
	var tracker staleTipTracker

	tipA, tipB := chainhash.HashH([]byte("a")), chainhash.HashH([]byte("b"))
	start := time.Unix(1_000_000, 0)

	require.False(t, tracker.sustained(tipA, start), "the first sighting is inside the grace period")
	require.False(t, tracker.sustained(tipA, start.Add(staleTipGrace-time.Second)))
	require.True(t, tracker.sustained(tipA, start.Add(staleTipGrace)), "the gap has lasted the grace period")
	require.False(t, tracker.sustained(tipB, start.Add(2*staleTipGrace)), "a new chain tip restarts the clock")

	tracker.clear()
	require.False(t, tracker.sustained(tipB, start.Add(3*staleTipGrace)), "catching up clears the gap")
}

func TestStaleTipTrackerCheck(t *testing.T) {
	var tracker staleTipTracker

	chainTip, baTip := chainhash.HashH([]byte("chain")), chainhash.HashH([]byte("ba"))
	now := time.Unix(1_000_000, 0)

	behind, sustained := tracker.check(&chainTip, 5, &chainTip, 5, now)
	require.False(t, behind, "equal tips are not a gap")
	require.False(t, sustained)

	behind, sustained = tracker.check(&chainTip, 5, &baTip, 4, now)
	require.True(t, behind, "block assembly is one block behind")
	require.False(t, sustained)

	behind, sustained = tracker.check(&chainTip, 5, &baTip, 5, now.Add(2*staleTipGrace))
	require.True(t, behind, "an equal-height fork is a gap")
	require.True(t, sustained)

	behind, sustained = tracker.check(&chainTip, 5, &baTip, 6, now.Add(3*staleTipGrace))
	require.False(t, behind, "block assembly ahead of the chain is not a stall")
	require.False(t, sustained)

	behind, sustained = tracker.check(&chainTip, 5, &baTip, 4, now.Add(4*staleTipGrace))
	require.True(t, behind)
	require.False(t, sustained, "the ahead state restarted the clock")
}
