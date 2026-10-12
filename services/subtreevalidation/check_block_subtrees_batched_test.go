package subtreevalidation

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

const batchedTestHeight = uint32(20_000)

var opTrue = bscript.NewFromBytes([]byte{0x51})

// countingStore counts SpendAndCreateMulti calls, so a test can tell which path
// CheckBlockSubtrees took.
type countingStore struct {
	utxo.Store
	multiCalls atomic.Int64
}

func (c *countingStore) SpendAndCreateMulti(ctx context.Context, txs []*bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) ([]utxo.SpendAndCreateMultiResult, error) {
	c.multiCalls.Add(1)
	return c.Store.SpendAndCreateMulti(ctx, txs, blockHeight, opts...)
}

// publishSpy is the local validator with PublishTxMeta counted, so a test can
// see the batch path publish txmeta for each created transaction.
type publishSpy struct {
	*validator.Validator
	mu        sync.Mutex
	published map[chainhash.Hash]bool
}

func (p *publishSpy) PublishTxMeta(txMeta *meta.Data, txHash *chainhash.Hash, inBlock bool) {
	p.mu.Lock()
	p.published[*txHash] = inBlock
	p.mu.Unlock()

	p.Validator.PublishTxMeta(txMeta, txHash, inBlock)
}

// spyChecker records which transactions reached the script check.
type spyChecker struct {
	validator.BlockBatchChecker
	mu      sync.Mutex
	checked map[chainhash.Hash]bool
}

func (s *spyChecker) CheckExtendedTransaction(ctx context.Context, tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, opts *validator.Options) error {
	s.mu.Lock()
	s.checked[*tx.TxIDChainHash()] = true
	s.mu.Unlock()

	return s.BlockBatchChecker.CheckExtendedTransaction(ctx, tx, blockHeight, utxoHeights, opts)
}

type batchedFixture struct {
	server    *Server
	store     *countingStore
	txmeta    *kafka.KafkaAsyncProducerMock
	validator *validator.Validator
}

// newBatchedFixture builds a server with the real validator over a sqlitememory
// store, in the given FSM state, on regtest (no checkpoint) with CSV pushed above
// the test height so no median-time lookups are needed.
func newBatchedFixture(t *testing.T, state blockchain.FSMStateType) *batchedFixture {
	t.Helper()
	InitPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings(t)
	params := *tSettings.ChainCfgParams
	params.CSVHeight = 1_000_000
	tSettings.ChainCfgParams = &params
	tSettings.BlockAssembly.Disabled = true

	storeURL, err := url.Parse("sqlitememory:///" + t.Name())
	require.NoError(t, err)

	base, err := sql.New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)
	require.NoError(t, base.SetBlockHeight(batchedTestHeight-1))
	require.NoError(t, base.SetMedianBlockTime(uint32(time.Now().Unix()))) //nolint:gosec

	store := &countingStore{Store: base}

	subtreeStore := blobmemory.New()

	localClient, err := blockchain.NewLocalClient(logger, tSettings, &blockchainstore.MockStore{}, subtreeStore, base)
	require.NoError(t, err)

	blockchainClient := &fsmStateOverrideClient{ClientI: localClient, state: state}

	txmeta := kafka.NewKafkaAsyncProducerMockWithBuffer(10_000)

	v, err := validator.New(ctx, logger, tSettings, base, txmeta, kafka.NewKafkaAsyncProducerMock(), nil, nil, nil)
	require.NoError(t, err)

	nilConsumer := &kafka.KafkaConsumerGroup{}
	spy := &publishSpy{Validator: v.(*validator.Validator), published: map[chainhash.Hash]bool{}}

	server, err := New(ctx, logger, tSettings, subtreeStore, blobmemory.New(), store, spy, blockchainClient, nilConsumer, nilConsumer, nil, nil)
	require.NoError(t, err)

	return &batchedFixture{server: server, store: store, txmeta: txmeta, validator: v.(*validator.Validator)}
}

// opTrueTx spends the given outputs of parents and pays everything but a
// 100-satoshi fee to two OP_TRUE outputs.
func opTrueTx(t *testing.T, lockTime uint32, parents []*bt.Tx, vouts []uint32) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	tx.LockTime = lockTime

	var in uint64

	for n, p := range parents {
		input := &bt.Input{PreviousTxOutIndex: vouts[n], UnlockingScript: bscript.NewFromBytes([]byte{}), SequenceNumber: 0xffffffff}
		require.NoError(t, input.PreviousTxIDAdd(p.TxIDChainHash()))
		tx.Inputs = append(tx.Inputs, input)
		in += p.Outputs[vouts[n]].Satoshis
	}

	half := (in - 100) / 2
	tx.AddOutput(&bt.Output{Satoshis: half, LockingScript: opTrue})
	tx.AddOutput(&bt.Output{Satoshis: in - 100 - half, LockingScript: opTrue})

	return tx
}

// storedRoot creates an outside parent, mined in an earlier block.
func storedRoot(t *testing.T, f *batchedFixture, seed uint32, script *bscript.Script) *bt.Tx {
	t.Helper()

	root := bt.NewTx()
	root.LockTime = seed

	input := &bt.Input{PreviousTxOutIndex: 0, UnlockingScript: bscript.NewFromBytes([]byte{0x00}), SequenceNumber: 0xffffffff}
	require.NoError(t, input.PreviousTxIDAdd(&chainhash.Hash{0xaa, byte(seed)}))
	root.Inputs = append(root.Inputs, input)
	root.AddOutput(&bt.Output{Satoshis: 1_000_000, LockingScript: script})
	root.AddOutput(&bt.Output{Satoshis: 1_000_000, LockingScript: script})

	_, _, err := f.store.SpendAndCreate(context.Background(), root, batchedTestHeight-10, utxo.WithCreateOnly(), utxo.WithSkipExtendedInputs(true),
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 5, BlockHeight: batchedTestHeight - 10}))
	require.NoError(t, err)

	return root
}

// chainedTxs builds `levels` levels of `width` transactions over roots, each
// spending output 0 of the transaction above it, and every third also output 1
// of its neighbour, so there are several parents in the block.
func chainedTxs(t *testing.T, roots []*bt.Tx, levels int) []*bt.Tx {
	t.Helper()

	var txs []*bt.Tx

	prev := roots

	for level := 0; level < levels; level++ {
		row := make([]*bt.Tx, len(prev))

		for i := range prev {
			parents := []*bt.Tx{prev[i]}
			vouts := []uint32{0}

			if i%3 == 0 && i+1 < len(prev) {
				parents = append(parents, prev[i+1])
				vouts = append(vouts, 1)
			}

			row[i] = opTrueTx(t, uint32(level*1000+i+1), parents, vouts) //nolint:gosec // test data
			txs = append(txs, row[i])
		}

		prev = row
	}

	return txs
}

// storeBlock writes one subtree holding a coinbase placeholder and txs, and
// returns a block whose header commits to it with a real coinbase. When
// doctor is set, the subtree the block lists differs from the one the header
// commits to.
func storeBlock(t *testing.T, f *batchedFixture, txs []*bt.Tx, doctor bool) *model.Block {
	t.Helper()

	return storeBlockBody(t, f, txs, doctor, false)
}

// storeBlockBody is storeBlock that, when dupLast is set, stores a
// CVE-2012-2459 duplicate-last mutation of the subtree under the honest
// subtree's hash: the node list and subtree data carry the last transaction
// twice, and the root, so the header's merkle root, is unchanged. That needs
// an odd node count (coinbase placeholder included), so len(txs) must be even.
func storeBlockBody(t *testing.T, f *batchedFixture, txs []*bt.Tx, doctor, dupLast bool) *model.Block {
	t.Helper()

	ctx := context.Background()

	leaves := nextPow2(len(txs) + 1)
	if dupLast {
		require.Zero(t, len(txs)%2, "a duplicate-last mutation keeps the root only over an odd node count")
		leaves = nextPow2(len(txs) + 2)
	}

	st, err := subtreepkg.NewTreeByLeafCount(leaves)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	var data []byte

	for _, tx := range txs {
		require.NoError(t, st.AddNode(*tx.TxIDChainHash(), 100, uint64(tx.Size()))) //nolint:gosec
		data = append(data, tx.Bytes()...)
	}

	subtreeHash := *st.RootHash()

	if dupLast {
		last := txs[len(txs)-1]
		require.NoError(t, st.AddNode(*last.TxIDChainHash(), 100, uint64(last.Size()))) //nolint:gosec
		data = append(data, last.Bytes()...)

		require.Equal(t, subtreeHash, *st.RootHash(), "the mutation must keep the subtree root")
	}

	stBytes, err := st.Serialize()
	require.NoError(t, err)
	for fileType, content := range map[fileformat.FileType][]byte{fileformat.FileTypeSubtreeToCheck: stBytes, fileformat.FileTypeSubtreeData: data} {
		if err := f.server.subtreeStore.Set(ctx, subtreeHash[:], fileType, content); err != nil {
			require.ErrorIs(t, err, errors.ErrBlobAlreadyExists)
		}
	}

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)

	merkleRoot, err := st.RootHashWithReplaceRootNode(coinbase.TxIDChainHash(), 0, uint64(coinbase.Size())) //nolint:gosec
	require.NoError(t, err)

	if doctor {
		merkleRoot = &chainhash.Hash{0xde, 0xad}
	}

	bits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	header := &model.BlockHeader{
		Version:        0x20000000,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: merkleRoot,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *bits,
	}

	block, err := model.NewBlock(header, coinbase, []*chainhash.Hash{&subtreeHash}, uint64(len(txs)+1), 1000, batchedTestHeight, 0) //nolint:gosec
	require.NoError(t, err)

	return block
}

func nextPow2(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}

	return p
}

func checkBlock(t *testing.T, f *batchedFixture, block *model.Block) error {
	t.Helper()

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	_, err = f.server.CheckBlockSubtrees(context.Background(), &subtreevalidation_api.CheckBlockSubtreesRequest{
		Block:   blockBytes,
		BaseUrl: "legacy",
	})

	return err
}

func requireCreatedUnmined(t *testing.T, f *batchedFixture, txs []*bt.Tx) {
	t.Helper()

	for i, tx := range txs {
		md, err := f.store.Get(context.Background(), tx.TxIDChainHash())
		require.NoError(t, err, "tx %d must be in the store", i)
		require.Empty(t, md.BlockIDs, "tx %d is created unmined; the record-mined write adds the block later", i)
		require.False(t, md.Conflicting, "tx %d", i)
		require.False(t, md.Locked, "tx %d", i)
	}
}

func requireAbsent(t *testing.T, f *batchedFixture, txs []*bt.Tx) {
	t.Helper()

	for i, tx := range txs {
		_, err := f.store.Get(context.Background(), tx.TxIDChainHash())
		require.ErrorIs(t, err, errors.ErrTxNotFound, "tx %d must not be written", i)
	}
}

// The batch path: catch-up above the checkpoint writes the block through
// SpendAndCreateMulti, creates every record unmined, publishes txmeta for each
// and hands nothing to block assembly (the validator has no block assembly
// client here, so a hand-off would fail the block).
func TestCheckBlockSubtreesBatched_WritesThroughSpendAndCreateMulti(t *testing.T) {
	f := newBatchedFixture(t, blockchain.FSMStateCATCHINGBLOCKS)

	roots := []*bt.Tx{storedRoot(t, f, 1, opTrue), storedRoot(t, f, 2, opTrue), storedRoot(t, f, 3, opTrue), storedRoot(t, f, 4, opTrue)}
	txs := chainedTxs(t, roots, 6)

	require.NoError(t, checkBlock(t, f, storeBlock(t, f, txs, false)))
	require.Positive(t, f.store.multiCalls.Load(), "catch-up above the checkpoint must take the batch path")

	requireCreatedUnmined(t, f, txs)

	spy := f.server.validatorClient.(*publishSpy)
	for i, tx := range txs {
		inBlock, ok := spy.published[*tx.TxIDChainHash()]
		require.True(t, ok, "txmeta must be published for created tx %d", i)
		require.True(t, inBlock, "published as in-block, as the validator publishes block transactions")
	}
}

// Outside catch-up, or at or below the checkpoint, the level path runs.
func TestCheckBlockSubtreesBatched_Gating(t *testing.T) {
	t.Run("RUNNING keeps the level path", func(t *testing.T) {
		f := newBatchedFixture(t, blockchain.FSMStateRUNNING)
		_, ok := f.server.batchChecker(blockchain.FSMStateRUNNING, batchedTestHeight)
		require.False(t, ok)
	})

	t.Run("at the checkpoint keeps the level path", func(t *testing.T) {
		f := newBatchedFixture(t, blockchain.FSMStateCATCHINGBLOCKS)
		params := *f.server.settings.ChainCfgParams
		params.Checkpoints = append(params.Checkpoints, chaincfg.Checkpoint{Height: int32(batchedTestHeight), Hash: &chainhash.Hash{}})
		f.server.settings.ChainCfgParams = &params

		_, ok := f.server.batchChecker(blockchain.FSMStateCATCHINGBLOCKS, batchedTestHeight)
		require.False(t, ok)

		_, ok = f.server.batchChecker(blockchain.FSMStateCATCHINGBLOCKS, batchedTestHeight+1)
		require.True(t, ok)
	})

	t.Run("a remote validator keeps the level path", func(t *testing.T) {
		f := newBatchedFixture(t, blockchain.FSMStateCATCHINGBLOCKS)
		f.server.validatorClient = &validator.MockValidator{}

		_, ok := f.server.batchChecker(blockchain.FSMStateCATCHINGBLOCKS, batchedTestHeight)
		require.False(t, ok)
	})
}

// Design test 8: a real header with a doctored subtree list writes nothing and
// is corrupt; the honest body then goes through.
func TestCheckBlockSubtreesBatched_DoctoredBodyWritesNothing(t *testing.T) {
	f := newBatchedFixture(t, blockchain.FSMStateCATCHINGBLOCKS)

	roots := []*bt.Tx{storedRoot(t, f, 1, opTrue), storedRoot(t, f, 2, opTrue)}
	txs := chainedTxs(t, roots, 3)

	err := checkBlock(t, f, storeBlock(t, f, txs, true))
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "an unbound body is corrupt, got %v", err)
	require.Zero(t, f.store.multiCalls.Load())
	requireAbsent(t, f, txs)

	require.NoError(t, checkBlock(t, f, storeBlock(t, f, txs, false)))
	requireCreatedUnmined(t, f, txs)
}

// A CVE-2012-2459 duplicate-last mutation keeps the subtree root, so it passes
// the body-binding check. Its repeated transaction must make the block corrupt,
// as ValidateSubtreeInternal's duplicate scan does on the level path, never
// invalid: an invalid verdict would condemn an honest block hash. Nothing is
// written, and the honest body then goes through.
func TestCheckBlockSubtreesBatched_DuplicateLastMutationIsCorrupt(t *testing.T) {
	f := newBatchedFixture(t, blockchain.FSMStateCATCHINGBLOCKS)

	root := storedRoot(t, f, 1, opTrue)
	a := opTrueTx(t, 1, []*bt.Tx{root}, []uint32{0})
	b := opTrueTx(t, 2, []*bt.Tx{a}, []uint32{0})
	txs := []*bt.Tx{a, b}

	mutated := storeBlockBody(t, f, txs, false, true)

	err := checkBlock(t, f, mutated)
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "a duplicate-last mutation is corrupt, got %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a duplicate-last mutation must never mark the block invalid, got %v", err)
	require.Zero(t, f.store.multiCalls.Load())
	requireAbsent(t, f, txs)

	// The mutated files sit under the honest subtree hash; drop them so the
	// honest body can be stored in their place, as a re-download would.
	for _, fileType := range []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData} {
		require.NoError(t, f.server.subtreeStore.Del(context.Background(), mutated.Subtrees[0][:], fileType))
	}

	honest := storeBlock(t, f, txs, false)
	require.Equal(t, mutated.Header.HashMerkleRoot, honest.Header.HashMerkleRoot, "the mutation keeps the header's merkle root")
	require.NoError(t, checkBlock(t, f, honest))
	requireCreatedUnmined(t, f, txs)
}

// Design test 7, first half: a transaction carrying forged extended fields
// (an OP_TRUE script with an inflated value) for a parent whose real script is
// OP_FALSE must fail on the script the store holds.
func TestProcessTransactionsBatched_ForgedExtensionFails(t *testing.T) {
	f := newBatchedFixture(t, blockchain.FSMStateCATCHINGBLOCKS)

	root := storedRoot(t, f, 1, bscript.NewFromBytes([]byte{0x00}))
	forged := opTrueTx(t, 7, []*bt.Tx{root}, []uint32{0})
	forged.Inputs[0].PreviousTxScript = opTrue
	forged.Inputs[0].PreviousTxSatoshis = 21_000_000_00000000

	checker, ok := f.server.batchChecker(blockchain.FSMStateCATCHINGBLOCKS, batchedTestHeight)
	require.True(t, ok)

	err := f.server.processTransactionsBatched(context.Background(), checker, []*bt.Tx{forged}, chainhash.Hash{}, batchedTestHeight, 0, 0, map[uint32]bool{})
	require.ErrorIs(t, err, errors.ErrTxInvalid)
	requireAbsent(t, f, []*bt.Tx{forged})
}

// Design test 7, second half: the coinbase that arrives with the subtree data is
// never a parent. A transaction spending it must not reach the script check with
// the coinbase's script; it goes through the per-transaction path, where the
// spend fails for want of a record.
func TestProcessTransactionsBatched_CoinbaseIsNeverAParent(t *testing.T) {
	f := newBatchedFixture(t, blockchain.FSMStateCATCHINGBLOCKS)

	coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
	require.NoError(t, err)
	coinbase.Outputs = []*bt.Output{{Satoshis: 50_0000_0000, LockingScript: opTrue}}

	spender := opTrueTx(t, 9, []*bt.Tx{coinbase}, []uint32{0})

	inner, ok := f.server.batchChecker(blockchain.FSMStateCATCHINGBLOCKS, batchedTestHeight)
	require.True(t, ok)

	spy := &spyChecker{BlockBatchChecker: inner, checked: map[chainhash.Hash]bool{}}

	_ = f.server.processTransactionsBatched(context.Background(), spy, []*bt.Tx{coinbase, spender}, chainhash.Hash{}, batchedTestHeight, 0, 0, map[uint32]bool{})

	require.False(t, spy.checked[*spender.TxIDChainHash()], "a spend of the subtree data's coinbase must not reach the batch script check")
	require.False(t, spy.checked[*coinbase.TxIDChainHash()])
	requireAbsent(t, f, []*bt.Tx{spender})
}

// Two transactions spending one outpoint, or a spend of a later transaction,
// fail the block before anything is written.
func TestProcessTransactionsBatched_InvalidBodiesFailBeforeWriting(t *testing.T) {
	f := newBatchedFixture(t, blockchain.FSMStateCATCHINGBLOCKS)

	checker, ok := f.server.batchChecker(blockchain.FSMStateCATCHINGBLOCKS, batchedTestHeight)
	require.True(t, ok)

	root := storedRoot(t, f, 1, opTrue)
	a := opTrueTx(t, 1, []*bt.Tx{root}, []uint32{0})
	b := opTrueTx(t, 2, []*bt.Tx{root}, []uint32{0})

	err := f.server.processTransactionsBatched(context.Background(), checker, []*bt.Tx{a, b}, chainhash.Hash{}, batchedTestHeight, 0, 0, map[uint32]bool{})
	require.ErrorIs(t, err, errors.ErrBlockInvalid)

	child := opTrueTx(t, 3, []*bt.Tx{a}, []uint32{0})
	err = f.server.processTransactionsBatched(context.Background(), checker, []*bt.Tx{child, a}, chainhash.Hash{}, batchedTestHeight, 0, 0, map[uint32]bool{})
	require.ErrorIs(t, err, errors.ErrBlockInvalid)

	require.Zero(t, f.store.multiCalls.Load())
	requireAbsent(t, f, []*bt.Tx{a, b, child})
}

// A spend the caller could not foresee (an unmined transaction already spent
// the outside parent) fails in the store; the transaction goes through today's
// path, which creates it as conflicting, and the rest of the block is written.
func TestProcessTransactionsBatched_FailedSpendGoesThroughTodaysPath(t *testing.T) {
	f := newBatchedFixture(t, blockchain.FSMStateCATCHINGBLOCKS)

	checker, ok := f.server.batchChecker(blockchain.FSMStateCATCHINGBLOCKS, batchedTestHeight)
	require.True(t, ok)

	roots := []*bt.Tx{storedRoot(t, f, 1, opTrue), storedRoot(t, f, 2, opTrue)}

	// An unmined transaction spends root 0's output 0 first.
	thief := opTrueTx(t, 99, []*bt.Tx{roots[0]}, []uint32{0})
	_, err := f.validator.Validate(context.Background(), thief, batchedTestHeight)
	require.NoError(t, err)

	loser := opTrueTx(t, 1, []*bt.Tx{roots[0]}, []uint32{0})
	winner := opTrueTx(t, 2, []*bt.Tx{roots[1]}, []uint32{0})
	child := opTrueTx(t, 3, []*bt.Tx{winner}, []uint32{0})

	err = f.server.processTransactionsBatched(context.Background(), checker, []*bt.Tx{loser, winner, child}, chainhash.Hash{}, batchedTestHeight, 0, 0, map[uint32]bool{})
	require.NoError(t, err)
	require.Positive(t, f.store.multiCalls.Load())

	requireCreatedUnmined(t, f, []*bt.Tx{winner, child})

	md, err := f.store.Get(context.Background(), loser.TxIDChainHash())
	require.NoError(t, err)
	require.True(t, md.Conflicting, "the per-transaction path creates a double spend in a block as conflicting")
}

// Lists are cut by the configured size, in block order, and a chain that spans
// lists is still written in order.
func TestProcessTransactionsBatched_CutsLists(t *testing.T) {
	f := newBatchedFixture(t, blockchain.FSMStateCATCHINGBLOCKS)
	f.server.settings.SubtreeValidation.SpendAndCreateMultiMaxTxs = 4

	checker, ok := f.server.batchChecker(blockchain.FSMStateCATCHINGBLOCKS, batchedTestHeight)
	require.True(t, ok)

	roots := []*bt.Tx{storedRoot(t, f, 1, opTrue), storedRoot(t, f, 2, opTrue), storedRoot(t, f, 3, opTrue)}
	txs := chainedTxs(t, roots, 4)

	err := f.server.processTransactionsBatched(context.Background(), checker, txs, chainhash.Hash{}, batchedTestHeight, 0, 0, map[uint32]bool{})
	require.NoError(t, err)
	require.Equal(t, int64(3), f.store.multiCalls.Load(), "12 transactions in lists of 4")
	requireCreatedUnmined(t, f, txs)
}

// levelPathValidator hides BlockBatchChecker, so CheckBlockSubtrees takes the
// level path with the same validator.
type levelPathValidator struct {
	validator.Interface
}

type recordView struct {
	Fee, Size    uint64
	Inpoints     string
	BlockIDs     []uint32
	UnminedSince uint32
	Locked       bool
	Conflicting  bool
	Spends       []string
}

func recordViews(t *testing.T, store utxo.Store, txs []*bt.Tx) []recordView {
	t.Helper()

	ctx := context.Background()
	views := make([]recordView, len(txs))

	for i, tx := range txs {
		md, err := store.Get(ctx, tx.TxIDChainHash())
		require.NoError(t, err)

		views[i] = recordView{
			Fee: md.Fee, Size: md.SizeInBytes, Inpoints: md.TxInpoints.String(), BlockIDs: md.BlockIDs,
			UnminedSince: md.UnminedSince, Locked: md.Locked, Conflicting: md.Conflicting,
		}

		for vout, out := range tx.Outputs {
			h, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), out, uint32(vout)) //nolint:gosec
			require.NoError(t, err)

			resp, err := store.GetSpend(ctx, &utxo.Spend{TxID: tx.TxIDChainHash(), Vout: uint32(vout), UTXOHash: h}) //nolint:gosec
			require.NoError(t, err)

			spender := ""
			if resp.SpendingData != nil {
				spender = resp.SpendingData.TxID.String()
			}

			views[i].Spends = append(views[i].Spends, spender)
		}
	}

	return views
}

// The batch path leaves exactly the records the level path leaves for the same
// block, before and after the record-mined write.
func TestCheckBlockSubtreesBatched_SameRecordsAsLevelPath(t *testing.T) {
	build := func(t *testing.T, f *batchedFixture) []*bt.Tx {
		all := make([]*bt.Tx, 0, 3+3*5)
		all = append(all, storedRoot(t, f, 1, opTrue), storedRoot(t, f, 2, opTrue), storedRoot(t, f, 3, opTrue))
		txs := chainedTxs(t, all[:3], 5)
		require.NoError(t, checkBlock(t, f, storeBlock(t, f, txs, false)))

		return append(all, txs...)
	}

	var batchedViews, levelViews, batchedMined, levelMined []recordView

	t.Run("batch", func(t *testing.T) {
		f := newBatchedFixture(t, blockchain.FSMStateCATCHINGBLOCKS)
		txs := build(t, f)
		require.Positive(t, f.store.multiCalls.Load())

		batchedViews = recordViews(t, f.store, txs)
		markMined(t, f.store, txs)
		batchedMined = recordViews(t, f.store, txs)
	})

	t.Run("level", func(t *testing.T) {
		f := newBatchedFixture(t, blockchain.FSMStateCATCHINGBLOCKS)
		f.server.validatorClient = &levelPathValidator{Interface: f.server.validatorClient}
		txs := build(t, f)
		require.Zero(t, f.store.multiCalls.Load(), "the level path must not use SpendAndCreateMulti")

		levelViews = recordViews(t, f.store, txs)
		markMined(t, f.store, txs)
		levelMined = recordViews(t, f.store, txs)
	})

	require.NotEmpty(t, batchedViews)
	require.Equal(t, levelViews, batchedViews, "records before the record-mined write")
	require.Equal(t, levelMined, batchedMined, "records after the record-mined write")

	for i, v := range batchedViews[3:] {
		require.Equal(t, batchedTestHeight, v.UnminedSince, "tx %d: unmined since the block's height, as today", i)
	}
}

func markMined(t *testing.T, store utxo.Store, txs []*bt.Tx) {
	t.Helper()

	hashes := make([]*chainhash.Hash, 0, len(txs))
	for _, tx := range txs[3:] {
		hashes = append(hashes, tx.TxIDChainHash())
	}

	_, err := store.SetMinedMulti(context.Background(), hashes, utxo.MinedBlockInfo{BlockID: 77, BlockHeight: batchedTestHeight, SubtreeIdx: 0})
	require.NoError(t, err)
}

// faultyParentStore fails every parent-output read.
type faultyParentStore struct {
	*countingStore
}

func (f *faultyParentStore) ParentOutputsForValidation(_ context.Context, outpoints []utxo.Outpoint, _ ...utxo.ParentOutputOption) ([]utxo.ParentOutput, error) {
	answers := make([]utxo.ParentOutput, len(outpoints))
	for i := range answers {
		answers[i] = utxo.ParentOutput{Err: errors.NewStorageError("timeout")}
	}

	return answers, nil
}

// A store fault while reading parents is retryable, never a verdict on the
// block, and nothing is written.
func TestProcessTransactionsBatched_StoreFaultIsRetryable(t *testing.T) {
	f := newBatchedFixture(t, blockchain.FSMStateCATCHINGBLOCKS)

	roots := []*bt.Tx{storedRoot(t, f, 1, opTrue), storedRoot(t, f, 2, opTrue)}
	txs := chainedTxs(t, roots, 3)

	f.server.utxoStore = &faultyParentStore{countingStore: f.store}

	checker, ok := f.server.batchChecker(blockchain.FSMStateCATCHINGBLOCKS, batchedTestHeight)
	require.True(t, ok)

	err := f.server.processTransactionsBatched(context.Background(), checker, txs, chainhash.Hash{}, batchedTestHeight, 0, 0, map[uint32]bool{})
	require.ErrorIs(t, err, errors.ErrProcessing)
	require.NotErrorIs(t, err, errors.ErrTxInvalid)
	require.NotErrorIs(t, err, errors.ErrBlockInvalid)
	require.Zero(t, f.store.multiCalls.Load())
	requireAbsent(t, f, txs)
}

// slowParentStore delays every parent-output read.
type slowParentStore struct {
	*countingStore
	delay time.Duration
}

func (s *slowParentStore) ParentOutputsForValidation(ctx context.Context, outpoints []utxo.Outpoint, opts ...utxo.ParentOutputOption) ([]utxo.ParentOutput, error) {
	time.Sleep(s.delay)

	return s.countingStore.ParentOutputsForValidation(ctx, outpoints, opts...)
}

// slowChecker delays every script check.
type slowChecker struct {
	validator.BlockBatchChecker
	delay time.Duration
}

func (s *slowChecker) CheckExtendedTransaction(ctx context.Context, tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, opts *validator.Options) error {
	time.Sleep(s.delay)

	return s.BlockBatchChecker.CheckExtendedTransaction(ctx, tx, blockHeight, utxoHeights, opts)
}

func batchStepSum(t *testing.T, step string) float64 {
	t.Helper()

	return labeledHistogramSum(t, "teranode_subtreevalidation_batch_step", "step", step)
}

// The resolve and check steps overlap, so check_after_reads, the check time
// left once every parent read is back, is what tells slow reads from slow
// checks: near zero when the reads set the pace, at least the check time when
// the checks do.
func TestProcessTransactionsBatched_StepMetricsSeparateReadsFromChecks(t *testing.T) {
	const delay = 300 * time.Millisecond

	t.Run("slow reads", func(t *testing.T) {
		f := newBatchedFixture(t, blockchain.FSMStateCATCHINGBLOCKS)
		root := storedRoot(t, f, 1, opTrue)
		tx := opTrueTx(t, 1, []*bt.Tx{root}, []uint32{0})

		f.server.utxoStore = &slowParentStore{countingStore: f.store, delay: delay}

		checker, ok := f.server.batchChecker(blockchain.FSMStateCATCHINGBLOCKS, batchedTestHeight)
		require.True(t, ok)

		resolveBefore, tailBefore := batchStepSum(t, "resolve"), batchStepSum(t, "check_after_reads")

		require.NoError(t, f.server.processTransactionsBatched(context.Background(), checker, []*bt.Tx{tx}, chainhash.Hash{}, batchedTestHeight, 0, 0, map[uint32]bool{}))

		require.GreaterOrEqual(t, batchStepSum(t, "resolve")-resolveBefore, delay.Seconds(), "resolve covers the parent reads")
		require.Less(t, batchStepSum(t, "check_after_reads")-tailBefore, delay.Seconds(), "a fast check leaves little after the reads")
	})

	t.Run("slow checks", func(t *testing.T) {
		f := newBatchedFixture(t, blockchain.FSMStateCATCHINGBLOCKS)
		root := storedRoot(t, f, 1, opTrue)
		tx := opTrueTx(t, 1, []*bt.Tx{root}, []uint32{0})

		inner, ok := f.server.batchChecker(blockchain.FSMStateCATCHINGBLOCKS, batchedTestHeight)
		require.True(t, ok)

		tailBefore := batchStepSum(t, "check_after_reads")

		require.NoError(t, f.server.processTransactionsBatched(context.Background(), &slowChecker{BlockBatchChecker: inner, delay: delay}, []*bt.Tx{tx}, chainhash.Hash{}, batchedTestHeight, 0, 0, map[uint32]bool{}))

		require.GreaterOrEqual(t, batchStepSum(t, "check_after_reads")-tailBefore, delay.Seconds(), "a slow check shows after the reads")
	})
}
