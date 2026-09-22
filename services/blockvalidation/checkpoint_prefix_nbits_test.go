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
// building: a checkpoint at height 100, with only a height-1 block stored.
//
// The chain parameters are teratestnet with a relaxed proof-of-work limit, not regtest. That matters
// twice. The modern DAA is active from height 0 there, so the expected-nBits rule genuinely applies
// to these fixtures; and regtest sets NoDifficultyAdjustment, which makes catch-up's header precheck
// return nil immediately and would hide the composition this file is about.
func checkpointPrefixSettings(t *testing.T) *settings.Settings {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false

	params := chaincfg.TeraTestNetParams
	params.PowLimit = chaincfg.RegressionNetParams.PowLimit
	params.PowLimitBits = 0x207fffff
	params.Checkpoints = []chaincfg.Checkpoint{{Height: 100, Hash: &chainhash.Hash{0xEE}}}
	tSettings.ChainCfgParams = &params

	require.False(t, tSettings.ChainCfgParams.NoDifficultyAdjustment,
		"fixture precondition: the difficulty rule must actually apply on this chain")

	return tSettings
}

// storeHonestPrefixParent stores a genuine height-1 block mined at the difficulty the chain expects,
// leaving the best height at 1 — below the checkpoint, so the node is still building the prefix and
// the height predicate that used to grant the shortcut is satisfied.
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
		"fixture precondition: the height predicate that used to grant the shortcut is satisfied")

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

// TestValidateBlock_BelowCheckpoint_ExpectedNBitsAlwaysEnforced closes the route that survived two
// rounds of this work (bitcoin-sv/teranode#4844).
//
// The expected-nBits rule could be skipped for a block below the highest CONFIGURED checkpoint while
// the node was still building that prefix. Height below a checkpoint does not establish that a block
// is on the checkpointed chain, so the exemption was never sound — and neither of the two places
// that were supposed to cover it does:
//
//   - the direct peer path has no header-level difficulty pipeline at all;
//   - catch-up has one, but validateHeaderChainDifficulty DEFERS every header whose full 144-block
//     window is not inside the fetched run — the first header and roughly its first 146 successors —
//     naming this very check as the downstream cover for them. Narrowing the exemption to catch-up
//     therefore composed the two skips into a hole rather than closing it.
//
// Both subtests below assert the same thing from the two delivery shapes, and the catch-up one
// asserts the composition directly: the real precheck is run over the fetched header run and shown
// to PASS the bad chain, after which the body validator must be the thing that rejects it.
func TestValidateBlock_BelowCheckpoint_ExpectedNBitsAlwaysEnforced(t *testing.T) {
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

	t.Run("catch-up delivery inside the precheck's skipped window is rejected too", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := checkpointPrefixSettings(t)
		bv, client := newNoPersistHarness(ctx, t, tSettings)

		parent := storeHonestPrefixParent(ctx, t, client, tSettings)
		child := difficulty1ChildOfHonestParent(ctx, t, client, parent)

		_, anchorMeta, err := client.GetBlockHeader(ctx, tSettings.ChainCfgParams.GenesisHash)
		require.NoError(t, err)

		// The composition, asserted against the REAL precheck rather than described. This fetched
		// run is two headers long, so every header in it is closer to the anchor than the 144-block
		// window depth and validateHeaderChainDifficulty skips all of them — it returns nil for a
		// chain whose second header carries the wrong difficulty bits. Catch-up's own doc comment
		// names the body-level check as the cover for exactly these headers.
		require.NoError(t,
			validateHeaderChainDifficulty(tSettings, anchorMeta, []*model.BlockHeader{parent.Header, child.Header}),
			"fixture precondition: the header precheck does NOT cover this block, which is why the body check must")

		// So the body validator has to be what rejects it, even on the catch-up path.
		err = bv.ValidateBlockWithOptions(ctx, child, "http://localhost", &ValidateBlockOptions{
			CachedHeaders:           []*model.BlockHeader{parent.Header},
			IsCatchupMode:           true,
			DisableOptimisticMining: true,
		})
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid))
		require.ErrorContains(t, err, "incorrect difficulty bits",
			"catch-up must not be exempt: its header precheck defers near-anchor headers to this check")

		requireNothingPersisted(ctx, t, client, child.Hash())
	})
}
