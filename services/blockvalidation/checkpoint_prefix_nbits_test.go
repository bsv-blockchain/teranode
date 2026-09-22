package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// checkpointPrefixSettings puts the node inside a checkpoint-certified prefix it has not finished
// building: a checkpoint at height 100, with only a height-1 block stored, so
// model.SkipExpectedDifficulty's height predicate is satisfied for every block this test builds.
func checkpointPrefixSettings(t *testing.T) *settings.Settings {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false
	tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{
		{Height: 100, Hash: &chainhash.Hash{0xEE}},
	}

	return tSettings
}

// storeHonestPrefixParent stores a genuine height-1 block mined at the difficulty the chain expects,
// leaving the best height at 1 — below the checkpoint, so the node is still building the prefix.
func storeHonestPrefixParent(ctx context.Context, t *testing.T, client blockchain.ClientI, tSettings *settings.Settings) *model.Block {
	t.Helper()

	timestamp := uint32(time.Now().Unix()) //nolint:gosec

	expected, err := client.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
	require.NoError(t, err)
	require.NotNil(t, expected)

	coinbaseTx := coinbaseAtHeight(t, 1)
	hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(), *expected, timestamp)

	parent, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), 1, 0)
	require.NoError(t, err)

	require.NoError(t, client.AddBlock(ctx, parent, "test",
		blockchainoptions.WithMinedSet(true), blockchainoptions.WithSubtreesSet(true)))

	_, best, err := client.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(1), best.Height, "fixture precondition: the node is still building the checkpoint prefix")
	require.True(t, model.SkipExpectedDifficulty(tSettings.ChainCfgParams.Checkpoints, 2, best.Height),
		"fixture precondition: the height predicate that grants the shortcut is satisfied")

	return parent
}

// difficulty1ChildOfHonestParent builds a child of the honest parent whose declared difficulty bits
// are NOT the ones the chain expects. It is mined only to its own (trivial) target, which is what
// makes it cost difficulty 1.
func difficulty1ChildOfHonestParent(ctx context.Context, t *testing.T, client blockchain.ClientI, parent *model.Block) *model.Block {
	t.Helper()

	timestamp := uint32(time.Now().Unix()) //nolint:gosec

	expected, err := client.GetNextWorkRequired(ctx, parent.Hash(), int64(timestamp))
	require.NoError(t, err)
	require.NotNil(t, expected)

	wrongBits := nBitsFrom(t, "1f7fffff")
	require.NotEqual(t, expected.String(), wrongBits.String(),
		"fixture precondition: the declared bits must differ from the expected bits")

	coinbaseTx := canaryCoinbaseAtHeight(t, 2)
	hdr := minedHeaderWithBits(t, parent.Hash(), coinbaseTx.TxIDChainHash(), wrongBits, timestamp)

	child, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), 2, 0)
	require.NoError(t, err)

	return child
}

// TestValidateBlock_BelowCheckpoint_DirectDeliveryStillEnforcesNBits closes the route that survived
// the first round of this work (bitcoin-sv/teranode#4844).
//
// The checkpoint-prefix shortcut skipped the expected-nBits rule for ANY caller whose block sat
// inside a prefix the node was still building. A directly peer-delivered block qualified, so while a
// node was syncing a peer could extend an honest parent with a difficulty-1 block whose declared
// bits were never checked against the chain it claims to extend. Merkle-bound, that block is
// condemned and PERSISTED as invalid — and its difficulty-1 descendants then reach the
// parent-invalid branch, which deliberately keeps their attacker-chosen unbound bodies. Both writes
// cost difficulty 1, and initial sync is exactly when the node is most exposed.
//
// The shortcut is now catch-up only. This asserts the discriminating behaviour directly: the SAME
// block, in the SAME chain state, is rejected on the direct path and skipped on the catch-up path.
func TestValidateBlock_BelowCheckpoint_DirectDeliveryStillEnforcesNBits(t *testing.T) {
	initPrometheusMetrics()

	t.Run("direct peer delivery is rejected on expected nBits and persists nothing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := checkpointPrefixSettings(t)
		bv, client := newNoPersistHarness(ctx, t, tSettings)

		parent := storeHonestPrefixParent(ctx, t, client, tSettings)
		child := difficulty1ChildOfHonestParent(ctx, t, client, parent)

		// No IsCatchupMode: this is processBlockFound's shape, a block announced by a peer.
		err := bv.ValidateBlockWithOptions(ctx, child, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid))
		require.ErrorContains(t, err, "incorrect difficulty bits",
			"a below-checkpoint direct delivery must still be bound to the chain's difficulty schedule")

		requireNothingPersisted(ctx, t, client, child.Hash())
	})

	t.Run("the same block still takes the shortcut on the catch-up path", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := checkpointPrefixSettings(t)
		bv, client := newNoPersistHarness(ctx, t, tSettings)

		parent := storeHonestPrefixParent(ctx, t, client, tSettings)
		child := difficulty1ChildOfHonestParent(ctx, t, client, parent)

		// Catch-up's own shape. It reaches here only after validateCatchupHeaderDifficulty has
		// recomputed the DAA-required bits over the whole fetched header chain and struck the peer
		// on a mismatch, which is what makes skipping the body-level check safe — and is why this
		// subtest, which drives the body validator directly with no header pipeline in front of it,
		// sees the block accepted. It is the positive control for the gate above: without it, the
		// gate could be closing the shortcut for everyone and every other test would still pass.
		err := bv.ValidateBlockWithOptions(ctx, child, "http://localhost", &ValidateBlockOptions{
			CachedHeaders:           []*model.BlockHeader{parent.Header},
			IsCatchupMode:           true,
			DisableOptimisticMining: true,
		})
		require.NoError(t, err, "catch-up keeps the checkpoint-prefix shortcut")
	})
}
