package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// gap046Block builds a coinbase-only block whose only transaction has ZERO inputs and one output,
// with a header (PoW against the chain's expected bits, current timestamp) that commits to exactly
// that transaction (merkle root = its txid). The body is therefore bound: the header's author
// committed to a coinbase with no inputs (SV Node: bad-txns-vin-empty).
func gap046Block(ctx context.Context, t *testing.T, tSettings *settings.Settings, bv *BlockValidation, height uint32) *model.Block {
	t.Helper()

	cb := bt.NewTx()
	require.NoError(t, cb.AddP2PKHOutputFromAddress("1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn", 50*100_000_000))
	require.Empty(t, cb.Inputs, "fixture precondition: a present coinbase with no inputs")

	timestamp := uint32(time.Now().Unix()) //nolint:gosec

	expected, err := bv.blockchainClient.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
	require.NoError(t, err)

	hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, cb.TxIDChainHash(), *expected, timestamp)

	block, err := model.NewBlock(hdr, cb, []*chainhash.Hash{}, 1, uint64(cb.Size()), height, 0) //nolint:gosec
	require.NoError(t, err)

	// The header commits to this body: the coinbase-only binding holds.
	require.NoError(t, block.CheckCoinbaseOnlyBodyBound(), "fixture precondition: the body is bound to the header")

	return block
}

// TestGapBV046_PresentCoinbaseWithNoInputsIsInvalidNotIncomplete asserts BV-BODY-005 /
// AC-BV-BODY-005.1 (GAP-BV-046): only a coinbase the peer did not deliver is
// ERR_BLOCK_INCOMPLETE. A coinbase with no inputs that the header commits to is malformed
// (BV-BODY-007 "at least one input"), so on that bound body the verdict is ERR_BLOCK_INVALID, on
// the full path (with and without optimistic mining) and on the quick path alike. See
// coinbaseNotDelivered for why binding, not shape, is what separates the two.
func TestGapBV046_PresentCoinbaseWithNoInputsIsInvalidNotIncomplete(t *testing.T) {
	initPrometheusMetrics()

	const blockHeight = uint32(100)

	t.Run("full path (ValidateBlockWithOptions)", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false

		bv, _ := newNoPersistHarness(ctx, t, tSettings)
		block := gap046Block(ctx, t, tSettings, bv, blockHeight)

		err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrBlockIncomplete),
			"BV-BODY-005: only an absent coinbase is INCOMPLETE, got: %v", err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid),
			"a present, bound, malformed coinbase must be ERR_BLOCK_INVALID, got: %v", err)
	})

	t.Run("full path with optimistic mining", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = true
		tSettings.BlockValidation.OptimisticMiningPeerBlocks = true

		bv, _ := newNoPersistHarness(ctx, t, tSettings)
		block := gap046Block(ctx, t, tSettings, bv, blockHeight)

		err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid),
			"a bound coinbase with no inputs must be condemned, not added optimistically, got: %v", err)
	})

	t.Run("quick path (quickValidateBlock)", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false

		bv, _ := newNoPersistHarness(ctx, t, tSettings)
		block := gap046Block(ctx, t, tSettings, bv, blockHeight)

		err := bv.quickValidateBlock(ctx, block, "peer-serving", "http://localhost")
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrBlockIncomplete),
			"BV-BODY-005: only an absent coinbase is INCOMPLETE, got: %v", err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid),
			"a present, bound, malformed coinbase must be ERR_BLOCK_INVALID, got: %v", err)
	})

	t.Run("quick path, header does not commit to the coinbase", func(t *testing.T) {
		// The shape a peer without the coinbase serves: a stand-in with outputs and no inputs,
		// whose txid the header does not commit to. It must stay INCOMPLETE so the block is
		// fetched again from another peer and the honest hash is never condemned.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false

		bv, _ := newNoPersistHarness(ctx, t, tSettings)
		block := gap046Block(ctx, t, tSettings, bv, blockHeight)

		standIn := bt.NewTx()
		require.NoError(t, standIn.AddP2PKHOutputFromAddress("1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn", 49*100_000_000))
		block.CoinbaseTx = standIn
		require.Error(t, block.CheckCoinbaseOnlyBodyBound(), "fixture precondition: the stand-in is not bound")

		err := bv.quickValidateBlock(ctx, block, "peer-serving", "http://localhost")
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockIncomplete),
			"an unbound coinbase with no inputs is missing data, not invalidity, got: %v", err)
	})
}
