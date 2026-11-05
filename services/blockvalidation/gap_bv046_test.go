package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
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

// gap046StandIn swaps the bound coinbase for the shape a peer without the coinbase serves: a
// stand-in with outputs and no inputs, whose txid the header does not commit to.
func gap046StandIn(t *testing.T, block *model.Block) {
	t.Helper()

	standIn := bt.NewTx()
	require.NoError(t, standIn.AddP2PKHOutputFromAddress("1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn", 49*100_000_000))
	block.CoinbaseTx = standIn
	require.Error(t, block.CheckCoinbaseOnlyBodyBound(), "fixture precondition: the stand-in is not bound")
}

// requireIncompleteNotCondemned asserts the verdict is ERR_BLOCK_INCOMPLETE, neither invalid nor
// corrupt, and that the hash left no trace in the blockchain store, so the block can be fetched
// again from another peer and the serving peer takes no strike.
func requireIncompleteNotCondemned(ctx context.Context, t *testing.T, client blockchain.ClientI, block *model.Block, err error) {
	t.Helper()

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockIncomplete),
		"an undelivered coinbase is missing data, not invalidity, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "must not be ERR_BLOCK_INVALID, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockCorrupt), "must not be ERR_BLOCK_CORRUPT, got: %v", err)
	requireNothingPersisted(ctx, t, client, block.Hash())
}

// requireStoredInvalid asserts the block was condemned on the record: it exists in the blockchain
// store marked invalid, and it is not the best block header.
func requireStoredInvalid(ctx context.Context, t *testing.T, client blockchain.ClientI, block *model.Block) {
	t.Helper()

	_, meta, err := client.GetBlockHeader(ctx, block.Hash())
	require.NoError(t, err, "a bound invalid block must be stored")
	require.True(t, meta.Invalid, "a bound invalid block must be stored invalid")

	best, _, err := client.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.NotEqual(t, block.Hash().String(), best.Hash().String(), "an invalid block must not become the best block")
}

// TestGapBV046_PresentCoinbaseWithNoInputsIsInvalidNotIncomplete asserts BV-BODY-005 /
// AC-BV-BODY-005.1 (GAP-BV-046): only a coinbase the peer did not deliver is
// ERR_BLOCK_INCOMPLETE. A coinbase with no inputs that the header commits to is malformed
// (BV-BODY-007 "at least one input"), so on that bound body the verdict is ERR_BLOCK_INVALID, on
// the full path (with and without optimistic mining) and on the quick paths alike. See
// coinbaseNotDelivered for why binding, not shape, is what separates the two.
func TestGapBV046_PresentCoinbaseWithNoInputsIsInvalidNotIncomplete(t *testing.T) {
	initPrometheusMetrics()

	const blockHeight = uint32(100)

	t.Run("full path (ValidateBlockWithOptions)", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false

		bv, client := newNoPersistHarness(ctx, t, tSettings)
		block := gap046Block(ctx, t, tSettings, bv, blockHeight)

		err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrBlockIncomplete),
			"BV-BODY-005: only an absent coinbase is INCOMPLETE, got: %v", err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid),
			"a present, bound, malformed coinbase must be ERR_BLOCK_INVALID, got: %v", err)
		requireStoredInvalid(ctx, t, client, block)
	})

	t.Run("full path with optimistic mining", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = true
		tSettings.BlockValidation.OptimisticMiningPeerBlocks = true

		bv, client := newNoPersistHarness(ctx, t, tSettings)
		block := gap046Block(ctx, t, tSettings, bv, blockHeight)

		err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid),
			"a bound coinbase with no inputs must be condemned, not added optimistically, got: %v", err)
		requireStoredInvalid(ctx, t, client, block)
	})

	t.Run("full path, header does not commit to the coinbase", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false

		bv, client := newNoPersistHarness(ctx, t, tSettings)
		block := gap046Block(ctx, t, tSettings, bv, blockHeight)
		gap046StandIn(t, block)

		err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		requireIncompleteNotCondemned(ctx, t, client, block, err)
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
		// It must stay INCOMPLETE so the block is fetched again from another peer and the honest
		// hash is never condemned.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false

		bv, client := newNoPersistHarness(ctx, t, tSettings)
		block := gap046Block(ctx, t, tSettings, bv, blockHeight)
		gap046StandIn(t, block)

		err := bv.quickValidateBlock(ctx, block, "peer-serving", "http://localhost")
		requireIncompleteNotCondemned(ctx, t, client, block, err)
	})

	t.Run("quick path, zero-input coinbase on a body with subtrees", func(t *testing.T) {
		// A body with subtrees cannot be bound at the precheck, so a zero-input coinbase on it is
		// missing data. Letting it through would send a stand-in on to subtree binding, where it
		// fails the merkle check as CORRUPT and the serving peer takes a strike.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false

		bv, client := newNoPersistHarness(ctx, t, tSettings)
		block := gap046Block(ctx, t, tSettings, bv, blockHeight)
		block.Subtrees = []*chainhash.Hash{{0x01}}

		err := bv.quickValidateBlock(ctx, block, "peer-serving", "http://localhost")
		requireIncompleteNotCondemned(ctx, t, client, block, err)
	})

	// quickValidateBlockAsync is the catch-up entry point; it has its own copy of the precheck.
	t.Run("async quick path (quickValidateBlockAsync)", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false

		bv, _ := newNoPersistHarness(ctx, t, tSettings)
		block := gap046Block(ctx, t, tSettings, bv, blockHeight)

		// Buffered so the async path never blocks queuing write jobs; a coinbase-only body queues none.
		writeJobsChan := make(chan *SubtreeWriteJob, 16)

		_, _, err := bv.quickValidateBlockAsync(ctx, block, "peer-serving", "http://localhost", writeJobsChan)
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrBlockIncomplete),
			"BV-BODY-005: only an absent coinbase is INCOMPLETE, got: %v", err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid),
			"a present, bound, malformed coinbase must be ERR_BLOCK_INVALID, got: %v", err)
	})

	t.Run("async quick path, header does not commit to the coinbase", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false

		bv, client := newNoPersistHarness(ctx, t, tSettings)
		block := gap046Block(ctx, t, tSettings, bv, blockHeight)
		gap046StandIn(t, block)

		writeJobsChan := make(chan *SubtreeWriteJob, 16)

		_, _, err := bv.quickValidateBlockAsync(ctx, block, "peer-serving", "http://localhost", writeJobsChan)
		requireIncompleteNotCondemned(ctx, t, client, block, err)
	})
}
