package subtreeprocessor

import (
	"context"
	"io"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// scopeMarkRecorder records the hashes of every MarkTransactionsOnLongestChain call, per
// direction, and of every Get and BatchDecorate read, and then applies the call to the real store
// underneath.
type scopeMarkRecorder struct {
	utxostore.Store

	mu        sync.Mutex
	markFalse map[chainhash.Hash]struct{}
	markTrue  map[chainhash.Hash]struct{}
	gets      map[chainhash.Hash]struct{}
}

func newScopeMarkRecorder(store utxostore.Store) *scopeMarkRecorder {
	return &scopeMarkRecorder{
		Store:     store,
		markFalse: make(map[chainhash.Hash]struct{}),
		markTrue:  make(map[chainhash.Hash]struct{}),
		gets:      make(map[chainhash.Hash]struct{}),
	}
}

func (s *scopeMarkRecorder) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*utxostore.UnresolvedMetaData, fields ...fields.FieldName) error {
	s.mu.Lock()
	for _, item := range unresolvedMetaDataSlice {
		s.gets[item.Hash] = struct{}{}
	}
	s.mu.Unlock()

	return s.Store.BatchDecorate(ctx, unresolvedMetaDataSlice, fields...)
}

func (s *scopeMarkRecorder) Get(ctx context.Context, hash *chainhash.Hash, fields ...fields.FieldName) (*meta.Data, error) {
	s.mu.Lock()
	s.gets[*hash] = struct{}{}
	s.mu.Unlock()

	return s.Store.Get(ctx, hash, fields...)
}

func (s *scopeMarkRecorder) gotten(hash chainhash.Hash) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.gets[hash]

	return ok
}

func (s *scopeMarkRecorder) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	s.mu.Lock()
	target := s.markFalse
	if onLongestChain {
		target = s.markTrue
	}

	for _, hash := range txHashes {
		target[hash] = struct{}{}
	}
	s.mu.Unlock()

	return s.Store.MarkTransactionsOnLongestChain(ctx, txHashes, onLongestChain)
}

func (s *scopeMarkRecorder) markedFalse(hash chainhash.Hash) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.markFalse[hash]

	return ok
}

func (s *scopeMarkRecorder) markedTrue(hash chainhash.Hash) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, ok := s.markTrue[hash]

	return ok
}

func (s *scopeMarkRecorder) markFalseCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.markFalse)
}

// createScopeTestTx creates a tx spending coinbaseTx output vout, at height 1. With no
// options it is an unmined mempool tx (unmined_since set).
func createScopeTestTx(t *testing.T, ctx context.Context, store utxostore.Store, vout uint32, opts ...utxostore.CreateOption) chainhash.Hash {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.From(coinbaseTx.TxIDChainHash().String(), vout,
		coinbaseTx.Outputs[vout].LockingScript.String(), coinbaseTx.Outputs[vout].Satoshis))
	require.NoError(t, tx.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 100000000))
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})

	_, _, err := store.SpendAndCreate(ctx, tx, 1, append([]utxostore.CreateOption{utxostore.WithCreateOnly()}, opts...)...)
	require.NoError(t, err)

	return *tx.TxIDChainHash()
}

// storeScopeTestSubtree stores a subtree of [coinbase, hashes...] and returns its key.
func storeScopeTestSubtree(t *testing.T, ctx context.Context, blobStore *blob_memory.Memory, hashes ...chainhash.Hash) *chainhash.Hash {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	for i, hash := range hashes {
		require.NoError(t, st.AddSubtreeNode(subtreepkg.Node{Hash: hash, Fee: uint64(100 + i), SizeInBytes: 250})) //nolint:gosec
	}

	stBytes, err := st.Serialize()
	require.NoError(t, err)

	metaBytes, err := createSubtreeMeta(t, st).Serialize()
	require.NoError(t, err)

	key := st.RootHash()
	require.NoError(t, blobStore.Set(ctx, key[:], fileformat.FileTypeSubtree, stBytes))
	require.NoError(t, blobStore.Set(ctx, key[:], fileformat.FileTypeSubtreeMeta, metaBytes))

	return key
}

// newScopeTestChain returns a blockchain client over an in-memory SQLite chain store that
// holds only genesis.
func newScopeTestChain(t *testing.T, tSettings *settings.Settings) blockchain.ClientI {
	t.Helper()

	chainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, chainStore, nil, nil)
	require.NoError(t, err)

	return client
}

// addScopeTestBlock mines a block on parent, adds it to the chain with its mined_set written,
// and returns it with the id the chain store gave it.
func addScopeTestBlock(t *testing.T, ctx context.Context, client blockchain.ClientI, parent *chainhash.Hash, height uint32,
	coinbase *bt.Tx, subtrees ...*chainhash.Hash) *model.Block {
	t.Helper()

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  parent,
		HashMerkleRoot: coinbase.TxIDChainHash(),           // distinct per block
		Timestamp:      uint32(time.Now().Unix()) + height, //nolint:gosec
		Bits:           *nBits,
	}

	for ok, _, _ := header.HasMetTargetDifficulty(); !ok; ok, _, _ = header.HasMetTargetDifficulty() {
		header.Nonce++
	}

	block, err := model.NewBlock(header, coinbase, subtrees, uint64(1+2*len(subtrees)), 0, height, 0) //nolint:gosec
	require.NoError(t, err)
	require.NoError(t, client.AddBlock(ctx, block, "test", blockchainoptions.WithSubtreesSet(true)))
	// Block validation has finished with it: a reorg waits for blocks whose mined_set is unset.
	require.NoError(t, client.SetBlockMinedSet(ctx, block.Hash()))

	_, meta, err := client.GetBlockHeader(ctx, block.Hash())
	require.NoError(t, err)

	block.ID = meta.ID

	return block
}

// mineScopeTestTxs writes hashes as mined on the longest chain in block.
func mineScopeTestTxs(t *testing.T, ctx context.Context, store utxostore.Store, block *model.Block, hashes ...chainhash.Hash) {
	t.Helper()

	ptrs := make([]*chainhash.Hash, len(hashes))
	for i := range hashes {
		ptrs[i] = &hashes[i]
	}

	_, err := store.SetMinedMulti(ctx, ptrs, utxostore.MinedBlockInfo{BlockID: block.ID, BlockHeight: block.Height, OnLongestChain: true})
	require.NoError(t, err)
}

// TestReorgBlocks_MarkFalseScope pins issue 2051: a reorg marks not-on-longest-chain only
// the txs whose chain state it changes (moved back and not moved forward), never the whole
// block assembly mempool.
//
// Scenario, 1 back / 1 forward:
//
//	moveBack block A:    a1, shared   (mined on the old chain)
//	moveForward block B: shared, b1   (validated as a side chain, so b1 is still unmined)
//	mempool before:      m1, m2       (unmined)
func TestReorgBlocks_MarkFalseScope(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)
	sqlStore, err := sql.New(ctx, ulogger.TestLogger{}, tSettings, utxoStoreURL)
	require.NoError(t, err)

	store := newScopeMarkRecorder(sqlStore)
	blobStore := blob_memory.New()
	chain := newScopeTestChain(t, tSettings)

	a1 := createScopeTestTx(t, ctx, store, 0)
	shared := createScopeTestTx(t, ctx, store, 1)
	b1 := createScopeTestTx(t, ctx, store, 2)
	m1 := createScopeTestTx(t, ctx, store, 3)
	m2 := createScopeTestTx(t, ctx, store, 4)

	mempoolBefore := map[chainhash.Hash]uint32{}
	for _, hash := range []chainhash.Hash{m1, m2} {
		m, err := store.Get(ctx, &hash, fields.UnminedSince)
		require.NoError(t, err)
		require.NotZero(t, m.UnminedSince, "precondition: mempool tx is unmined")
		mempoolBefore[hash] = m.UnminedSince
	}

	_, err = sqlStore.Create(ctx, coinbaseTx2, 2)
	require.NoError(t, err)
	_, err = sqlStore.Create(ctx, coinbaseTx3, 2)
	require.NoError(t, err)

	// A and B compete on genesis. A was the tip and mined a1 and shared; B also mines shared,
	// and b1, which as a side-chain tx is still unmined.
	genesis := tSettings.ChainCfgParams.GenesisHash
	blockA := addScopeTestBlock(t, ctx, chain, genesis, 1, coinbaseTx2, storeScopeTestSubtree(t, ctx, blobStore, a1, shared))
	blockB := addScopeTestBlock(t, ctx, chain, genesis, 1, coinbaseTx3, storeScopeTestSubtree(t, ctx, blobStore, shared, b1))
	mineScopeTestTxs(t, ctx, store, blockA, a1, shared)

	newSubtreeChan := make(chan NewSubtreeRequest, 10)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, blobStore, chain, store, newSubtreeChan)
	require.NoError(t, err)
	stp.Start(ctx)

	stp.InitCurrentBlockHeader(blockA.Header)

	stp.AddBatch([]subtreepkg.Node{{Hash: m1, Fee: 300, SizeInBytes: 250}, {Hash: m2, Fee: 301, SizeInBytes: 250}},
		[]*subtreepkg.TxInpoints{{}, {}})

	require.Eventually(t, func() bool {
		txMap := stp.GetCurrentTxMap()
		return txMap.Exists(m1) && txMap.Exists(m2)
	}, 5*time.Second, 10*time.Millisecond, "mempool txs must be in block assembly before the reorg")

	require.NoError(t, stp.Reorg([]*model.Block{blockA}, []*model.Block{blockB}))
	require.Equal(t, blockB.Header.Hash(), stp.GetCurrentBlockHeader().Hash())

	finalTxMap := stp.GetCurrentTxMap()
	require.True(t, finalTxMap.Exists(a1), "a1 is back in the mempool after its block was moved back")
	require.True(t, finalTxMap.Exists(m1))
	require.True(t, finalTxMap.Exists(m2))
	// B mined shared, so the forward move took it out of block assembly: the tx map alone keeps
	// it out of the mark-false set, and the flag movedBackBlockTxMap holds for it is a second
	// guard behind that. TestReorgBlocks_RequeuedForwardTxIsNotMarkedFalse covers the case where
	// only the flag decides.
	require.False(t, finalTxMap.Exists(shared), "a tx the moveForward block mined is not in block assembly")

	// The mark-false set is exactly what the reorg moved off the chain.
	require.True(t, store.markedFalse(a1), "a moved-back tx not in the new chain must be marked not on the longest chain")
	require.False(t, store.markedFalse(shared), "a tx mined by the moveForward block must not be marked false")
	require.False(t, store.markedFalse(m1), "a pure mempool tx must not be re-marked on a reorg")
	require.False(t, store.markedFalse(m2), "a pure mempool tx must not be re-marked on a reorg")
	require.Equal(t, 1, store.markFalseCount(), "only a1 changed chain state in the not-on-longest-chain direction")

	// End state in the store.
	a1Meta, err := store.Get(ctx, &a1, fields.UnminedSince)
	require.NoError(t, err)
	require.NotZero(t, a1Meta.UnminedSince, "a1 is unmined again")

	b1Meta, err := store.Get(ctx, &b1, fields.UnminedSince)
	require.NoError(t, err)
	require.Zero(t, b1Meta.UnminedSince, "b1 is mined on the new longest chain")

	sharedMeta, err := store.Get(ctx, &shared, fields.UnminedSince)
	require.NoError(t, err)
	require.Zero(t, sharedMeta.UnminedSince, "shared stays mined")

	for hash, before := range mempoolBefore {
		m, err := store.Get(ctx, &hash, fields.UnminedSince)
		require.NoError(t, err)
		require.Equal(t, before, m.UnminedSince, "a pure mempool tx keeps its unmined_since")
	}
}

// TestReorgBlocks_InvalidMoveBackChecksOnlyItsOwnTxs covers the 1 back / 0 forward reorg of
// an invalid block (block 992 on 2026-10-26). The per-tx chain check must run over the
// invalid block's txs only, not every tx in block assembly: on that day it was ~880M
// per-tx store reads, and no block was produced while it ran.
//
//	moveBack block A (invalid): a1 (only in A), dup (also in valid block 5)
//	mempool before:             m1 (unmined)
func TestReorgBlocks_InvalidMoveBackChecksOnlyItsOwnTxs(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	if tSettings.UtxoStore.MaxMinedRoutines < 4 {
		tSettings.UtxoStore.MaxMinedRoutines = 4
	}

	utxoStoreURL, err := url.Parse("sqlitememory:///test")
	require.NoError(t, err)
	sqlStore, err := sql.New(ctx, ulogger.TestLogger{}, tSettings, utxoStoreURL)
	require.NoError(t, err)

	store := newScopeMarkRecorder(sqlStore)
	blobStore := blob_memory.New()
	chain := newScopeTestChain(t, tSettings)

	a1 := createScopeTestTx(t, ctx, store, 0)
	dup := createScopeTestTx(t, ctx, store, 1)
	m1 := createScopeTestTx(t, ctx, store, 2)

	_, err = sqlStore.Create(ctx, coinbaseTx2, 2)
	require.NoError(t, err)

	// genesis -> V -> A. V mined dup; A mined a1 and dup again, and is then invalidated, so
	// V is the tip.
	blockV := addScopeTestBlock(t, ctx, chain, tSettings.ChainCfgParams.GenesisHash, 1, coinbaseTx3)
	blockA := addScopeTestBlock(t, ctx, chain, blockV.Hash(), 2, coinbaseTx2, storeScopeTestSubtree(t, ctx, blobStore, a1, dup))
	mineScopeTestTxs(t, ctx, store, blockV, dup)
	mineScopeTestTxs(t, ctx, store, blockA, a1, dup)

	_, err = chain.InvalidateBlock(ctx, blockA.Hash())
	require.NoError(t, err)
	// Invalidating clears A's mined_set for block validation's unset-mined pass, and the
	// reorg waits for that pass to finish.
	require.NoError(t, chain.SetBlockMinedSet(ctx, blockA.Hash()))

	newSubtreeChan := make(chan NewSubtreeRequest, 10)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, tSettings, blobStore, chain, store, newSubtreeChan)
	require.NoError(t, err)
	stp.Start(ctx)

	stp.InitCurrentBlockHeader(blockA.Header)

	stp.AddBatch([]subtreepkg.Node{{Hash: m1, Fee: 300, SizeInBytes: 250}}, []*subtreepkg.TxInpoints{{}})

	require.Eventually(t, func() bool {
		return stp.GetCurrentTxMap().Exists(m1)
	}, 5*time.Second, 10*time.Millisecond, "mempool tx must be in block assembly before the reorg")

	require.NoError(t, stp.Reorg([]*model.Block{blockA}, []*model.Block{}))

	require.True(t, store.gotten(a1), "the invalid block's txs are checked")
	require.True(t, store.gotten(dup), "the invalid block's txs are checked")
	require.False(t, store.gotten(m1), "a pure mempool tx must not be read by the invalid-block check")

	require.True(t, store.markedFalse(a1), "a tx only in the invalid block is not on the longest chain")
	require.False(t, store.markedFalse(dup), "a tx also in a valid block stays on the longest chain")
	require.False(t, store.markedFalse(m1), "a pure mempool tx must not be re-marked")
}

// existsSet is a tx map that holds the given hashes and counts the probes.
type existsSet struct {
	hashes map[chainhash.Hash]struct{}
	probes *int
}

func (s existsSet) Exists(hash chainhash.Hash) bool {
	*s.probes++
	_, ok := s.hashes[hash]

	return ok
}

// TestMovedBackStillInAssembly: of the moved-back transactions, a reorg marks only those no
// moveForward block mines and block assembly still holds. One block assembly dropped, such as
// a reverse-cascade transaction it evicted, may have no record left to mark.
func TestMovedBackStillInAssembly(t *testing.T) {
	kept := chainhash.HashH([]byte("kept"))
	won := chainhash.HashH([]byte("won"))
	evicted := chainhash.HashH([]byte("evicted"))

	movedBack := map[chainhash.Hash]bool{kept: false, won: true, evicted: false}
	probes := 0
	inAssembly := existsSet{hashes: map[chainhash.Hash]struct{}{kept: {}, won: {}}, probes: &probes}

	require.Equal(t, []chainhash.Hash{kept}, movedBackStillInAssembly(movedBack, inAssembly))
	require.Equal(t, 2, probes, "one probe per moved-back transaction no moveForward block mines")
}

// newReorgMarkFixture returns a processor that records its mark calls, at genesis, with two
// forward blocks ready in chain.moveForward.
func newReorgMarkFixture(t *testing.T) (*SubtreeProcessor, *scopeMarkRecorder, *blob_memory.Memory, *walTestChain) {
	t.Helper()

	stp, base, subtreeStore, chain := newResetWALTestProcessor(t, false)
	store := newScopeMarkRecorder(base)
	stp.utxoStore = store

	return stp, store, subtreeStore, chain
}

// moveToBlockAbove adds a block on genesis, rival to chain.moveForward, and makes it the processor's
// tip, so a reorg can move it back.
func moveToBlockAbove(t *testing.T, stp *SubtreeProcessor, chain *walTestChain, subtrees ...*chainhash.Hash) *model.Block {
	t.Helper()

	if subtrees == nil {
		subtrees = []*chainhash.Hash{}
	}

	block := &model.Block{Height: 1, CoinbaseTx: coinbaseTx2, TransactionCount: 1, Subtrees: subtrees, Header: childHeader(chain.genesis, 901)}
	require.NoError(t, stp.blockchainClient.AddBlock(t.Context(), block, "", blockchainoptions.WithMinedSet(true)))
	stp.InitCurrentBlockHeader(block.Header)

	return block
}

// beforeForwardBlockOn runs fn once, as a reorg is about to move forward the block whose parent is
// the given header. The returned flag says whether it ran, so a test cannot pass without it.
func beforeForwardBlockOn(stp *SubtreeProcessor, parent *model.BlockHeader, fn func()) *bool {
	fired := false

	stp.SetProgressHook(func() {
		if !fired && stp.currentBlockHeader.Load().Hash().IsEqual(parent.Hash()) {
			fired = true

			fn()
		}
	})

	return &fired
}

// A moved-back tx that a forward block mined can come back into block assembly through the queue
// before the last forward block, whose map does not hold it. It is then both in block assembly and
// mined, and only movedBackBlockTxMap's flag keeps it from being marked not on the longest chain.
func TestReorgBlocks_RequeuedForwardTxIsNotMarkedFalse(t *testing.T) {
	stp, store, subtreeStore, chain := newReorgMarkFixture(t)

	sharedTx, _ := createResetWALConflict(t, store, 11)
	goneTx, _ := createResetWALConflict(t, store, 12)
	otherTx, _ := createResetWALConflict(t, store, 13)
	shared, gone := *sharedTx.TxIDChainHash(), *goneTx.TxIDChainHash()

	back := moveToBlockAbove(t, stp, chain, storeTestSubtree(t, subtreeStore, false, sharedTx, goneTx))
	chain.moveForward[0].Subtrees = []*chainhash.Hash{storeTestSubtree(t, subtreeStore, false, sharedTx)}
	chain.moveForward[1].Subtrees = []*chainhash.Hash{storeTestSubtree(t, subtreeStore, false, otherTx)}

	// A drain only takes batches stamped before it starts, so stamp the batch a millisecond earlier
	// than any drain can start. Then the last forward block's drain always takes it.
	enqueueAt := time.Now()
	stp.queue.clock = fixedClock{t: enqueueAt}
	stp.clock = fixedClock{t: enqueueAt.Add(time.Millisecond)}

	fired := beforeForwardBlockOn(stp, chain.moveForward[0].Header, func() {
		inpoints := subtreepkg.NewTxInpoints()
		stp.AddBatch([]subtreepkg.Node{{Hash: shared, Fee: 1, SizeInBytes: 1}}, []*subtreepkg.TxInpoints{&inpoints})
	})

	require.NoError(t, stp.reorgBlocks(t.Context(), []*model.Block{back}, chain.moveForward))
	require.True(t, *fired, "the batch must be queued before the last forward block")

	require.True(t, stp.currentTxMap.Exists(shared), "the mined tx must be back in block assembly, or nothing but the tx map keeps it unmarked")
	require.False(t, store.markedFalse(shared), "a forward block mined this tx, so it stays on the longest chain")
	require.True(t, store.markedFalse(gone), "no forward block mined this moved-back tx")
}

// A forward block's subtree that this node holds as a chained subtree is left out of the block's
// transaction map, so a loser in it counts as mined by no forward block, and stays a loser.
func TestReorgBlocks_LoserInChainedSubtreeStaysLoser(t *testing.T) {
	stp, store, subtreeStore, chain := newReorgMarkFixture(t)

	loserTx, winnerTx := createResetWALConflict(t, store, 7)
	loser := *loserTx.TxIDChainHash()

	back := moveToBlockAbove(t, stp, chain)

	chainedRoot := storeTestSubtree(t, subtreeStore, false, loserTx)
	chained, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, chained.AddCoinbaseNode())
	require.NoError(t, chained.AddNode(loser, 1, 1))
	require.Equal(t, chainedRoot, chained.RootHash(), "the block's subtree must be the one this node holds")

	chain.moveForward[0].Subtrees = []*chainhash.Hash{chainedRoot}
	chain.moveForward[1].Subtrees = []*chainhash.Hash{storeConflictSubtree(t, subtreeStore, winnerTx)}

	fired := beforeForwardBlockOn(stp, chain.genesis, func() {
		stp.chainedSubtrees = []*subtreepkg.Subtree{chained}
		inpoints := subtreepkg.NewTxInpoints()
		stp.currentTxMap.Set(loser, &inpoints)
	})

	require.NoError(t, stp.reorgBlocks(t.Context(), []*model.Block{back}, chain.moveForward))
	require.True(t, *fired, "the subtree must be chained before its block moves forward")

	require.True(t, store.markedFalse(loser), "a loser only in a chained subtree must be marked not on the longest chain")
}

// Only a loser that no forward block mined is marked not on the longest chain, wherever the other
// forward block's conflicting winner sits.
func TestReorgBlocks_LoserMinedByForwardBlockIsNotMarkedFalse(t *testing.T) {
	type subtrees struct{ first, second []*chainhash.Hash }

	tests := []struct {
		name        string
		place       func(loser, winner *chainhash.Hash) subtrees
		markedFalse bool
	}{
		{
			name: "mined in an earlier forward block",
			place: func(loser, winner *chainhash.Hash) subtrees {
				return subtrees{[]*chainhash.Hash{loser}, []*chainhash.Hash{winner}}
			},
		},
		{
			name: "mined in the same forward block",
			place: func(loser, winner *chainhash.Hash) subtrees {
				return subtrees{[]*chainhash.Hash{loser, winner}, []*chainhash.Hash{}}
			},
		},
		{
			name: "mined in a later forward block",
			place: func(loser, winner *chainhash.Hash) subtrees {
				return subtrees{[]*chainhash.Hash{winner}, []*chainhash.Hash{loser}}
			},
		},
		{
			name: "mined in no forward block",
			place: func(_, winner *chainhash.Hash) subtrees {
				return subtrees{[]*chainhash.Hash{winner}, []*chainhash.Hash{}}
			},
			markedFalse: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stp, store, subtreeStore, chain := newReorgMarkFixture(t)

			loserTx, winnerTx := createResetWALConflict(t, store, 21)
			back := moveToBlockAbove(t, stp, chain)

			placed := tt.place(storeTestSubtree(t, subtreeStore, false, loserTx), storeConflictSubtree(t, subtreeStore, winnerTx))
			chain.moveForward[0].Subtrees = placed.first
			chain.moveForward[1].Subtrees = placed.second

			require.NoError(t, stp.reorgBlocks(t.Context(), []*model.Block{back}, chain.moveForward))

			require.Equal(t, tt.markedFalse, store.markedFalse(*loserTx.TxIDChainHash()))
			require.Equal(t, !tt.markedFalse, store.markedTrue(*loserTx.TxIDChainHash()))
			require.True(t, store.markedTrue(*winnerTx.TxIDChainHash()), "the conflicting winner is mined")
			require.Nil(t, stp.reorgForwardTxMapSubtrees, "the reorg must stop recording when it ends")
		})
	}
}

// If the losing filter cannot read a forward block's subtree it cannot tell whether a loser was
// mined, so the reorg fails and rolls back before it writes any mark.
func TestReorgBlocks_LoserFilterSubtreeFailureFailsReorg(t *testing.T) {
	tests := []struct {
		name  string
		spoil func(t *testing.T, subtreeStore *blob_memory.Memory, subtreeHash *chainhash.Hash)
	}{
		{
			name: "subtree file is gone",
			spoil: func(t *testing.T, subtreeStore *blob_memory.Memory, subtreeHash *chainhash.Hash) {
				require.NoError(t, subtreeStore.Del(t.Context(), subtreeHash[:], fileformat.FileTypeSubtree))
			},
		},
		{
			name: "subtree file cannot be deserialized",
			spoil: func(t *testing.T, subtreeStore *blob_memory.Memory, subtreeHash *chainhash.Hash) {
				require.NoError(t, subtreeStore.Del(t.Context(), subtreeHash[:], fileformat.FileTypeSubtree))
				require.NoError(t, subtreeStore.Set(t.Context(), subtreeHash[:], fileformat.FileTypeSubtree, []byte{1, 2, 3}))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stp, store, subtreeStore, chain := newReorgMarkFixture(t)

			loserTx, winnerTx := createResetWALConflict(t, store, 31)
			otherTx, _ := createResetWALConflict(t, store, 32)
			back := moveToBlockAbove(t, stp, chain)

			firstSubtree := storeTestSubtree(t, subtreeStore, false, otherTx)
			chain.moveForward[0].Subtrees = []*chainhash.Hash{firstSubtree}
			chain.moveForward[1].Subtrees = []*chainhash.Hash{storeConflictSubtree(t, subtreeStore, winnerTx)}

			// The first forward block has been moved by now, so only the losing filter reads
			// this subtree again.
			fired := beforeForwardBlockOn(stp, chain.moveForward[0].Header, func() {
				tt.spoil(t, subtreeStore, firstSubtree)
			})

			err := stp.reorgBlocks(t.Context(), []*model.Block{back}, chain.moveForward)
			require.True(t, *fired)
			require.ErrorContains(t, err, "[losersMinedInForwardBlocks]")

			require.Equal(t, back.Header.Hash(), stp.currentBlockHeader.Load().Hash(), "a failed reorg rolls back")
			require.Zero(t, store.markFalseCount(), "the reorg must fail before it writes any mark")
			require.False(t, store.markedTrue(*loserTx.TxIDChainHash()))
			require.False(t, store.markedTrue(*winnerTx.TxIDChainHash()))
		})
	}
}

// readCountingStore counts the subtree reads that reach the blob store.
type readCountingStore struct {
	blob.Store
	reads atomic.Int64
}

func (s *readCountingStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...bloboptions.FileOption) (io.ReadCloser, error) {
	s.reads.Add(1)

	return s.Store.GetIoReader(ctx, key, fileType, opts...)
}

// Most reorgs have no conflicting transactions. Those must not pay for any subtree read.
func TestLosersMinedInForwardBlocks_ReadsSubtreesOnlyForLosers(t *testing.T) {
	stp, store, subtreeStore, _ := newReorgMarkFixture(t)

	minedTx, _ := createResetWALConflict(t, store, 41)
	mined := *minedTx.TxIDChainHash()
	unmined := chainhash.HashH([]byte("unmined"))

	counting := &readCountingStore{Store: subtreeStore}
	stp.subtreeStore = counting
	stp.reorgForwardTxMapSubtrees = []chainhash.Hash{*storeTestSubtree(t, subtreeStore, false, minedTx)}

	found, err := stp.losersMinedInForwardBlocks(t.Context(), nil)
	require.NoError(t, err)
	require.Empty(t, found)
	require.Zero(t, counting.reads.Load(), "no losers, so no subtree reads")

	found, err = stp.losersMinedInForwardBlocks(t.Context(), []chainhash.Hash{mined, unmined})
	require.NoError(t, err)
	require.Equal(t, map[chainhash.Hash]struct{}{mined: {}}, found)
	require.Equal(t, int64(1), counting.reads.Load(), "one read per recorded subtree")
}

// Normal block processing runs for the life of the node, so recording there would grow forever.
func TestMoveForwardBlock_OutsideReorgRecordsNoSubtrees(t *testing.T) {
	stp, store, subtreeStore, chain := newReorgMarkFixture(t)

	minedTx, _ := createResetWALConflict(t, store, 51)
	chain.moveForward[0].Subtrees = []*chainhash.Hash{storeTestSubtree(t, subtreeStore, false, minedTx)}

	transactionMap, _, err := stp.moveForwardBlock(t.Context(), chain.moveForward[0], true, map[chainhash.Hash]struct{}{}, true, true)
	require.NoError(t, err)
	require.True(t, transactionMap.Exists(*minedTx.TxIDChainHash()), "the block's subtree must have gone through the transaction map")

	require.Nil(t, stp.reorgForwardTxMapSubtrees)
}
