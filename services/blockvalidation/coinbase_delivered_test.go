package blockvalidation

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/settings"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
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

	block := gap046BlockWithCoinbaseUnchecked(ctx, t, tSettings, bv, height, cb)

	// The header commits to this body: the coinbase-only binding holds.
	require.NoError(t, block.CheckCoinbaseOnlyBodyBound(), "fixture precondition: the body is bound to the header")

	return block
}

// gap046BlockWithCoinbaseUnchecked is gap046BlockWithCoinbase without the binding precondition, for
// a 64-byte coinbase: the header commits to its txid, but the binding refuses 64 bytes.
func gap046BlockWithCoinbaseUnchecked(ctx context.Context, t *testing.T, tSettings *settings.Settings, bv *BlockValidation, height uint32, cb *bt.Tx) *model.Block {
	t.Helper()

	timestamp := uint32(time.Now().Unix()) //nolint:gosec

	expected, err := bv.blockchainClient.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
	require.NoError(t, err)

	hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, cb.TxIDChainHash(), *expected, timestamp)

	block, err := model.NewBlock(hdr, cb, []*chainhash.Hash{}, 1, uint64(cb.Size()), height, 0) //nolint:gosec
	require.NoError(t, err)
	require.True(t, block.Header.HashMerkleRoot.IsEqual(cb.TxIDChainHash()), "fixture precondition: the header commits to cb")

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

// gap046SixtyFourByteSpend builds a one-input transaction spending a non-null outpoint that
// serialises to exactly 64 bytes: version(4) + 01 input + prevout(36) + 0x04 + script(4) +
// sequence(4) + 01 output + value(8) + 00 + locktime(4). Two child hashes can take this shape with
// three constrained bytes (offsets 4, 41 and 59 for the no-output form; a few more for this one),
// so a header committing to its txid may be an honest multi-transaction block.
func gap046SixtyFourByteSpend(t *testing.T) *bt.Tx {
	t.Helper()

	in := &bt.Input{
		PreviousTxOutIndex: 0,
		UnlockingScript:    bscript.NewFromBytes([]byte{0x51, 0x51, 0x51, 0x51}),
		SequenceNumber:     bt.DefaultSequenceNumber,
	}
	require.NoError(t, in.PreviousTxIDAdd(&chainhash.Hash{0x01}))

	tx := bt.NewTx()
	tx.Inputs = append(tx.Inputs, in)
	tx.AddOutput(&bt.Output{Satoshis: 50 * 100_000_000, LockingScript: bscript.NewFromBytes(nil)})
	require.Equal(t, 64, tx.Size(), "fixture precondition: a 64-byte transaction")
	require.False(t, model.IsConsensusCoinbase(tx), "fixture precondition: not coinbase-shaped")

	return tx
}

// requireCorruptNotCondemned asserts the verdict is ERR_BLOCK_CORRUPT, neither invalid nor
// incomplete, and that the hash was not stored, so the block is fetched again and the honest hash is
// never condemned.
func requireCorruptNotCondemned(ctx context.Context, t *testing.T, client blockchain.ClientI, block *model.Block, err error) {
	t.Helper()

	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "an ambiguous body is corrupt, got: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "must not be ERR_BLOCK_INVALID, got: %v", err)
	requireNothingPersisted(ctx, t, client, block.Hash())
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
		block := gap046BlockWithCoinbaseUnchecked(ctx, t, tSettings, bv, blockHeight, gap046SixtyFourByteCoinbase(t))

		err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		requireIncompleteNotCondemned(ctx, t, client, block, err)
	})

	t.Run("full path, 64-byte one-input non-coinbase bound to the header", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false

		bv, client := newNoPersistHarness(ctx, t, tSettings)
		block := gap046BlockWithCoinbaseUnchecked(ctx, t, tSettings, bv, blockHeight, gap046SixtyFourByteSpend(t))

		err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		requireCorruptNotCondemned(ctx, t, client, block, err)
	})

	t.Run("full path with optimistic mining, 64-byte one-input non-coinbase bound to the header", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = true
		tSettings.BlockValidation.OptimisticMiningPeerBlocks = true

		bv, client := newNoPersistHarness(ctx, t, tSettings)
		block := gap046BlockWithCoinbaseUnchecked(ctx, t, tSettings, bv, blockHeight, gap046SixtyFourByteSpend(t))

		err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		requireCorruptNotCondemned(ctx, t, client, block, err)
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
		block := gap046BlockWithCoinbaseUnchecked(ctx, t, tSettings, bv, blockHeight, gap046SixtyFourByteCoinbase(t))

		err := bv.quickValidateBlock(ctx, block, "peer-serving", "http://localhost")
		requireIncompleteNotCondemned(ctx, t, client, block, err)
	})

	t.Run("quick path, 64-byte one-input non-coinbase bound to the header", func(t *testing.T) {
		// The one-input form of the same ambiguity: three bytes across both child hashes. It has an
		// input, so the precheck lets it through; the binding refuses it as CORRUPT, never INVALID.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false

		bv, client := newNoPersistHarness(ctx, t, tSettings)
		block := gap046BlockWithCoinbaseUnchecked(ctx, t, tSettings, bv, blockHeight, gap046SixtyFourByteSpend(t))

		err := bv.quickValidateBlock(ctx, block, "peer-serving", "http://localhost")
		requireCorruptNotCondemned(ctx, t, client, block, err)
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

	t.Run("async quick path, 64-byte zero-input coinbase bound to the header", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false

		bv, client := newNoPersistHarness(ctx, t, tSettings)
		block := gap046BlockWithCoinbaseUnchecked(ctx, t, tSettings, bv, blockHeight, gap046SixtyFourByteCoinbase(t))

		writeJobsChan := make(chan *SubtreeWriteJob, 16)

		_, _, err := bv.quickValidateBlockAsync(ctx, block, "peer-serving", "http://localhost", writeJobsChan)
		requireIncompleteNotCondemned(ctx, t, client, block, err)
	})

	t.Run("async quick path, 64-byte one-input non-coinbase bound to the header", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false

		bv, client := newNoPersistHarness(ctx, t, tSettings)
		block := gap046BlockWithCoinbaseUnchecked(ctx, t, tSettings, bv, blockHeight, gap046SixtyFourByteSpend(t))

		writeJobsChan := make(chan *SubtreeWriteJob, 16)

		_, _, err := bv.quickValidateBlockAsync(ctx, block, "peer-serving", "http://localhost", writeJobsChan)
		requireCorruptNotCondemned(ctx, t, client, block, err)
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

// subtreeBodyHarness wires a BlockValidation over real stores (sqlitememory blockchain store) with
// subtree validation mocked to succeed, and returns a block whose body carries one stored subtree
// and whose header commits to that subtree with committed in the coinbase slot. served is the
// coinbase the peer hands over; when it differs from committed, the body is not bound.
func subtreeBodyHarness(ctx context.Context, t *testing.T, optimistic bool, committed, served *bt.Tx) (*BlockValidation, blockchain.ClientI, *model.Block) {
	t.Helper()

	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
	t.Cleanup(deferFunc)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = optimistic
	tSettings.BlockValidation.OptimisticMiningPeerBlocks = optimistic

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	subtreeVal := &subtreevalidation.MockSubtreeValidation{}
	subtreeVal.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// Coinbase placeholder plus one node. The node need not be a real transaction: the coinbase
	// shape check sits straight behind the merkle binding, ahead of the order and fee checks.
	st, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddNode(chainhash.Hash{0xAB}, 100, 0))

	stBytes, err := st.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, st.RootHash()[:], fileformat.FileTypeSubtree, stBytes))

	committedTree := st.Duplicate()
	committedTree.ReplaceRootNode(committed.TxIDChainHash(), 0, uint64(committed.Size())) //nolint:gosec

	hdr := minedBIP34Header(t, 4, tSettings.ChainCfgParams.GenesisHash, committedTree.RootHash())
	block, err := model.NewBlock(hdr, served, []*chainhash.Hash{st.RootHash()}, uint64(st.Length()), uint64(served.Size()), 100, 0) //nolint:gosec
	require.NoError(t, err)

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, client, subtreeStore, txStore, utxoStore, nil, subtreeVal)

	return bv, client, block
}

// zeroInputCoinbase is a present coinbase with one output and no inputs.
func zeroInputCoinbase(t *testing.T) *bt.Tx {
	t.Helper()

	cb := bt.NewTx()
	require.NoError(t, cb.AddP2PKHOutputFromAddress("1DkmRkb5iQFkDu4NBysog5bugnsyx7kwtn", 50*100_000_000))

	return cb
}

// TestCoinbaseNotDelivered_ZeroInputOnSubtreeBody asserts that a coinbase with no inputs on a body
// that carries subtrees is judged by the merkle binding over those subtrees, not re-fetched
// forever: committed by the header it is ERR_BLOCK_INVALID and the hash is stored invalid, with or
// without optimistic mining; not committed (a stand-in) it is ERR_BLOCK_CORRUPT and nothing is
// stored invalid.
func TestCoinbaseNotDelivered_ZeroInputOnSubtreeBody(t *testing.T) {
	initPrometheusMetrics()

	t.Run("precheck lets it through", func(t *testing.T) {
		block := &model.Block{CoinbaseTx: zeroInputCoinbase(t), Subtrees: []*chainhash.Hash{{0x01}}}
		require.False(t, coinbaseNotDelivered(block), "a subtree body is bound by its merkle check, not by the precheck")
	})

	for _, optimistic := range []bool{false, true} {
		t.Run(fmt.Sprintf("committed by the header, optimistic mining %v", optimistic), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			cb := zeroInputCoinbase(t)
			bv, client, block := subtreeBodyHarness(ctx, t, optimistic, cb, cb)

			err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
			require.Error(t, err)
			require.False(t, errors.Is(err, errors.ErrBlockIncomplete), "a committed coinbase was delivered, got: %v", err)
			require.True(t, errors.Is(err, errors.ErrBlockInvalid), "a committed coinbase with no inputs is invalid, got: %v", err)
			requireStoredInvalid(ctx, t, client, block)
		})
	}

	t.Run("stand-in the header does not commit to", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		committed := coinbaseAtHeight(t, 100)
		bv, client, block := subtreeBodyHarness(ctx, t, false, committed, zeroInputCoinbase(t))

		err := bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err), "an unbound body is corrupt, got: %v", err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid), "an unbound body must never condemn the hash, got: %v", err)

		_, meta, metaErr := client.GetBlockHeader(ctx, block.Hash())
		require.False(t, metaErr == nil && meta != nil && meta.Invalid, "an unbound body must not be stored invalid")
	})

	t.Run("quick route: validateSubtrees condemns it behind the binding", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		cb := zeroInputCoinbase(t)

		st, err := subtreepkg.NewTreeByLeafCount(2)
		require.NoError(t, err)
		require.NoError(t, st.AddCoinbaseNode())
		require.NoError(t, st.AddNode(chainhash.HashH([]byte("tx a")), 1, 0))

		block := testhelpers.CreateTestBlocks(t, 1)[0]
		block.CoinbaseTx = cb
		block.Subtrees = []*chainhash.Hash{st.RootHash()}
		block.SubtreeSlices = []*subtreepkg.Subtree{st}

		root, err := st.RootHashWithReplaceRootNode(cb.TxIDChainHash(), 0, uint64(cb.Size())) //nolint:gosec
		require.NoError(t, err)
		block.Header.HashMerkleRoot = root

		require.False(t, coinbaseNotDelivered(block), "the quick route's precheck must let it through")

		_, err = suite.Server.blockValidation.validateSubtrees(context.Background(), block, 1)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrBlockInvalid), "the body is bound, so this is invalidity, got: %v", err)
	})
}
