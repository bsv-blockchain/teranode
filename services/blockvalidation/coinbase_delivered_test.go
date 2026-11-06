package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
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

	return gap046BlockWithCoinbase(ctx, t, tSettings, bv, height, cb)
}

// gap046BlockWithCoinbase builds a coinbase-only block around cb, with a header that commits to
// exactly cb (merkle root = its txid), so the body is bound.
func gap046BlockWithCoinbase(ctx context.Context, t *testing.T, tSettings *settings.Settings, bv *BlockValidation, height uint32, cb *bt.Tx) *model.Block {
	t.Helper()

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

// gap046SixtyFourByteCoinbase builds a zero-input transaction that serialises to exactly 64
// bytes: version(4) + 00 inputs + 01 output + value(8) + 0x2d + script(45) + locktime(4). Two
// concatenated child hashes under a merkle root can take this shape (a miner can grind the
// coinbase txid for it), so its txid can be an honest block's merkle root.
func gap046SixtyFourByteCoinbase(t *testing.T) *bt.Tx {
	t.Helper()

	script := make([]byte, 45)
	for i := range script {
		script[i] = byte(i + 1)
	}

	cb := bt.NewTx()
	cb.AddOutput(&bt.Output{Satoshis: 50 * 100_000_000, LockingScript: bscript.NewFromBytes(script)})
	require.Empty(t, cb.Inputs, "fixture precondition: no inputs")
	require.Equal(t, 64, cb.Size(), "fixture precondition: a 64-byte transaction")

	return cb
}

// gap046SpendShapedCoinbase builds a one-input transaction whose input spends a non-null
// outpoint, so it has an input but is not coinbase-shaped.
func gap046SpendShapedCoinbase(t *testing.T) *bt.Tx {
	t.Helper()

	in := &bt.Input{
		PreviousTxOutIndex: 0,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x51, 0x51}),
		SequenceNumber:     bt.DefaultSequenceNumber,
	}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{0x01}))

	cb := bt.NewTx()
	cb.Inputs = append(cb.Inputs, in)
	require.NoError(t, cb.AddP2PKHOutputFromAddress("1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn", 50*100_000_000))
	require.False(t, model.IsConsensusCoinbase(cb), "fixture precondition: not coinbase-shaped")

	return cb
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

	t.Run("full path with optimistic mining, one-input coinbase with a non-null prevout", func(t *testing.T) {
		// The optimistic-mining guard keys on coinbase shape, not on a missing input: a committed
		// coinbase that has an input but spends a non-null outpoint must also be validated first.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = true
		tSettings.BlockValidation.OptimisticMiningPeerBlocks = true

		bv, client := newNoPersistHarness(ctx, t, tSettings)
		block := gap046BlockWithCoinbase(ctx, t, tSettings, bv, blockHeight, gap046SpendShapedCoinbase(t))

		err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid),
			"a bound coinbase that spends a non-null outpoint must be condemned, not added optimistically, got: %v", err)
		requireStoredInvalid(ctx, t, client, block)
	})

	t.Run("full path, 64-byte zero-input coinbase bound to the header", func(t *testing.T) {
		// 64 bytes that bind may be the two child hashes under an honest block's merkle root, so
		// the hash must not be condemned on them: the block stays INCOMPLETE and is fetched again.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false

		bv, client := newNoPersistHarness(ctx, t, tSettings)
		block := gap046BlockWithCoinbase(ctx, t, tSettings, bv, blockHeight, gap046SixtyFourByteCoinbase(t))

		err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		requireIncompleteNotCondemned(ctx, t, client, block, err)
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

	t.Run("quick path, 64-byte zero-input coinbase bound to the header", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false

		bv, client := newNoPersistHarness(ctx, t, tSettings)
		block := gap046BlockWithCoinbase(ctx, t, tSettings, bv, blockHeight, gap046SixtyFourByteCoinbase(t))

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
