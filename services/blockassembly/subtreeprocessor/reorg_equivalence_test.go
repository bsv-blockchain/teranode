package subtreeprocessor

import (
	"context"
	"net/url"
	"testing"
	"time"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtree "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// reorgEquivResult is the observable end-state of a move-forward, captured so the
// two code paths can be compared for BA-REORG-010 equivalence.
type reorgEquivResult struct {
	hasA, hasB, hasX, hasC bool
	txMapLen               int
	txCount                uint64
	tipHash                string
	checkErr               error
}

// runMoveForwardEquivalence assembles an identical starting mempool (txs A, B, X in
// one completed chained subtree; C in the incomplete current subtree) and then moves a
// block forward that mines {A, B, X} and must leave {C}.
//
// The block references its mined subtree in one of two ways, exercising the two
// branches of moveForwardBlock -> processBlockSubtrees (SubtreeProcessor.go:4193):
//
//   - fromStore=false (in-memory / self-produced): block.Subtrees is the assembly's OWN
//     chained-subtree root hash, so blockSubtreesMap ends empty and CreateTransactionMap
//     is skipped (the in-memory shortcut).
//   - fromStore=true (external): block.Subtrees is a DIFFERENT store key holding the same
//     txids, so the txid map is built by reading the subtree from the blob store.
//
// BA-REORG-010 / Open Question #3 require these two paths to leave block assembly in a
// semantically identical state.
func runMoveForwardEquivalence(t *testing.T, fromStore bool, storeName string) reorgEquivResult {
	t.Helper()

	ctx := context.Background()

	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4 // coinbase + 3 txs completes a subtree

	utxoStoreURL, err := url.Parse("sqlitememory:///" + storeName)
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, settings, utxoStoreURL)
	require.NoError(t, err)

	blobStore := blob_memory.New()

	newSubtreeChan := make(chan NewSubtreeRequest, 100)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	mockBC := &blockchain.Mock{}
	mockBC.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, settings, blobStore, mockBC, utxoStore, newSubtreeChan)
	require.NoError(t, err)
	stp.Start(ctx)
	t.Cleanup(func() { stp.Stop(context.Background()) })

	// Distinct, deterministic mempool tx hashes.
	txA := chainhash.HashH([]byte("equiv-tx-A"))
	txB := chainhash.HashH([]byte("equiv-tx-B"))
	txX := chainhash.HashH([]byte("equiv-tx-X"))
	txC := chainhash.HashH([]byte("equiv-tx-C"))

	// A, B, X fill and complete chainedSubtrees[0] = [coinbase, A, B, X].
	stp.AddBatch(
		[]subtree.Node{
			{Hash: txA, Fee: 1, SizeInBytes: 100},
			{Hash: txB, Fee: 1, SizeInBytes: 100},
			{Hash: txX, Fee: 1, SizeInBytes: 100},
		},
		[]*subtree.TxInpoints{{}, {}, {}},
	)
	// C lands in the (incomplete) current subtree.
	stp.AddBatch(
		[]subtree.Node{{Hash: txC, Fee: 1, SizeInBytes: 100}},
		[]*subtree.TxInpoints{{}},
	)

	// Wait until the async queue has settled into exactly one completed chained subtree
	// plus C in the current subtree, so both runs start from an identical committed state.
	require.Eventually(t, func() bool {
		return len(stp.GetChainedSubtrees()) == 1 && stp.GetCurrentLength() == 1
	}, 5*time.Second, 10*time.Millisecond, "assembly must settle to 1 chained subtree + C in current")

	// Precondition: all four txs are in assembly before the move-forward.
	txMap := stp.GetCurrentTxMap()
	require.True(t, txMap.Exists(txA) && txMap.Exists(txB) && txMap.Exists(txX) && txMap.Exists(txC),
		"precondition: A, B, X, C must all be in assembly before move-forward")

	// The block builds directly on the current header.
	stp.InitCurrentBlockHeader(prevBlockHeader)

	// Identical header for both runs, so the resulting tip is directly comparable.
	blockHdr := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  prevBlockHeader.Hash(),
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      1234567890,
		Bits:           model.NBit{},
		Nonce:          4242,
	}

	var subtreeRefs []*chainhash.Hash
	if fromStore {
		// A distinct store key (NOT the in-memory subtree's root hash) holding the same
		// txids -> forces the from-store branch of processBlockSubtrees.
		sKey, err := chainhash.NewHashFromStr("3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c3c")
		require.NoError(t, err)
		require.NotEqual(t, stp.GetChainedSubtrees()[0].RootHash().String(), sKey.String(),
			"store key must differ from the in-memory root hash to exercise the from-store path")

		storeReorgSubtree(t, ctx, blobStore, sKey, []subtree.Node{
			{Hash: txA, Fee: 1, SizeInBytes: 100},
			{Hash: txB, Fee: 1, SizeInBytes: 100},
			{Hash: txX, Fee: 1, SizeInBytes: 100},
		})
		subtreeRefs = []*chainhash.Hash{sKey}
	} else {
		// The assembly's OWN subtree -> forces the in-memory shortcut.
		subtreeRefs = []*chainhash.Hash{stp.GetChainedSubtrees()[0].RootHash()}
	}

	block := &model.Block{
		Header:           blockHdr,
		Height:           1,
		CoinbaseTx:       coinbaseTx2,
		Subtrees:         subtreeRefs,
		TransactionCount: 4,
	}

	require.NoError(t, stp.MoveForwardBlock(block), "move-forward must succeed (fromStore=%v)", fromStore)

	// CheckSubtreeProcessor is channel-synced and recomputes txCount, giving a
	// happens-before edge for the subsequent reads.
	checkErr := stp.CheckSubtreeProcessor()

	txMap = stp.GetCurrentTxMap()

	return reorgEquivResult{
		hasA:     txMap.Exists(txA),
		hasB:     txMap.Exists(txB),
		hasX:     txMap.Exists(txX),
		hasC:     txMap.Exists(txC),
		txMapLen: txMap.Length(),
		txCount:  stp.TxCount(),
		tipHash:  stp.GetCurrentBlockHeader().Hash().String(),
		checkErr: checkErr,
	}
}

// TestReorg_InMemoryVsFromStore_Equivalence verifies BA-REORG-010 (Open Question #3) for
// the no-conflict baseline: moving the same block forward via the in-memory
// chained-subtrees shortcut vs via the from-store tx-map construction must leave block
// assembly in a semantically identical state.
func TestReorg_InMemoryVsFromStore_Equivalence(t *testing.T) {
	inMem := runMoveForwardEquivalence(t, false, "equivInMem")
	fromStore := runMoveForwardEquivalence(t, true, "equivStore")

	// Each path independently must mine {A, B, X} and keep {C}, with invariants intact.
	for _, c := range []struct {
		name string
		r    reorgEquivResult
	}{{"in-memory", inMem}, {"from-store", fromStore}} {
		require.NoError(t, c.r.checkErr,
			"%s: CheckSubtreeProcessor invariants must hold after move-forward", c.name)
		require.True(t, c.r.hasC, "%s: C must remain in assembly", c.name)
		require.False(t, c.r.hasA, "%s: A must be mined out of assembly", c.name)
		require.False(t, c.r.hasB, "%s: B must be mined out of assembly", c.name)
		require.False(t, c.r.hasX, "%s: X must be mined out of assembly", c.name)
		require.Equal(t, 1, c.r.txMapLen, "%s: only C should remain in assembly", c.name)
	}

	// BA-REORG-010: the two paths must be indistinguishable in their end state.
	require.Equal(t, inMem.txMapLen, fromStore.txMapLen, "surviving tx count must match across paths")
	require.Equal(t, inMem.txCount, fromStore.txCount, "TxCount must match across paths")
	require.Equal(t, inMem.tipHash, fromStore.tipHash, "resulting tip must match across paths")
}

// newReorgMempoolTx builds a minimal, unique real transaction (distinct lockTime ->
// distinct txid) suitable for seeding into the UTXO store, mirroring the block
// assembly package's newTx helper.
func newReorgMempoolTx(lockTime uint32) *bt.Tx {
	tx := bt.NewTx()
	tx.LockTime = lockTime
	tx.Inputs = []*bt.Input{{
		PreviousTxOutIndex: 0,
		PreviousTxSatoshis: 0,
		UnlockingScript:    bscript.NewFromBytes([]byte{}),
		SequenceNumber:     0,
	}}
	_ = tx.Inputs[0].PreviousTxIDAdd(&chainhash.Hash{})
	return tx
}

// currentSubtreeContains reports whether hash is a node in the processor's current
// (incomplete) subtree.
func currentSubtreeContains(stp *SubtreeProcessor, hash chainhash.Hash) bool {
	cs := stp.GetCurrentSubtree()
	if cs == nil {
		return false
	}
	for _, n := range cs.Nodes {
		if n.Hash.Equal(hash) {
			return true
		}
	}
	return false
}

// TestReorg_InvalidatesCurrentSubtreeTx verifies behavior during a chain reorg that
// invalidates a transaction the node is holding in its *incomplete current subtree*.
//
// Scenario: the node sits on tip block2 with two mempool txs (minedTx, keepTx) in its
// current subtree (not yet a completed chained subtree). A 1-block reorg orphans block2
// and adopts blockNew, whose subtree mines minedTx. After the reorg:
//   - minedTx must be evicted from the current subtree (now on the longest chain);
//   - keepTx (not on the new chain) must remain available for re-mining;
//   - the subtree processor's internal invariants (CheckSubtreeProcessor) must hold,
//     including a correctly re-asserted coinbase placeholder.
//
// This targets the reorgBlocks moveBack+moveForward path against txs the node holds in
// its current subtree specifically (distinct from the existing tests, which drive the
// competing txs through stored block subtrees on the moveBack side).
func TestReorg_InvalidatesCurrentSubtreeTx(t *testing.T) {
	ctx := context.Background()

	settings := test.CreateBaseTestSettings(t)
	// coinbase + up to 3 txs per subtree; two txs stay in an incomplete current subtree.
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

	utxoStoreURL, err := url.Parse("sqlitememory:///reorgCurrentSubtree")
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, settings, utxoStoreURL)
	require.NoError(t, err)

	// The reorg's mark-not-on-longest-chain writes unmined_since = store block height,
	// so a non-zero height is required for keepTx to be observably marked unmined.
	require.NoError(t, utxoStore.SetBlockHeight(2))

	blobStore := blob_memory.New()

	newSubtreeChan := make(chan NewSubtreeRequest, 100)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	mockBC := &blockchain.Mock{}
	mockBC.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBC.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockBC.On("GetBlockHeader", mock.Anything, mock.Anything).Return(prevBlockHeader, &model.BlockHeaderMeta{}, nil)
	mockBC.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, settings, blobStore, mockBC, utxoStore, newSubtreeChan)
	require.NoError(t, err)
	stp.Start(ctx)
	t.Cleanup(func() { stp.Stop(context.Background()) })

	// Two mempool txs the node holds in its CURRENT (incomplete) subtree. Real txs seeded
	// into the UTXO store (as they would be after validation), so the reorg's
	// mark-on-longest-chain bookkeeping operates on real records.
	minedTxTx := newReorgMempoolTx(101) // mined by the new chain -> invalidated out of assembly
	keepTxTx := newReorgMempoolTx(102)  // not on the new chain -> stays in assembly
	minedTx := *minedTxTx.TxIDChainHash()
	keepTx := *keepTxTx.TxIDChainHash()

	// The moved-forward (new tip) block mines minedTx; store its subtree.
	fwdSubtreeHash, err := chainhash.NewHashFromStr("4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d4d")
	require.NoError(t, err)
	storeReorgSubtree(t, ctx, blobStore, fwdSubtreeHash, []subtree.Node{
		{Hash: minedTx, Fee: 1, SizeInBytes: 100},
	})

	// Old tip (block2) and competing new tip (blockNew), both built on prevBlockHeader.
	block2Header := &model.BlockHeader{Version: 1, HashPrevBlock: prevBlockHeader.Hash(), HashMerkleRoot: &chainhash.Hash{}, Timestamp: 2100000002, Bits: model.NBit{}, Nonce: 9001}
	blockNewHeader := &model.BlockHeader{Version: 1, HashPrevBlock: prevBlockHeader.Hash(), HashMerkleRoot: &chainhash.Hash{}, Timestamp: 2100000003, Bits: model.NBit{}, Nonce: 9002}

	// Both blocks' coinbases must exist in the utxo store (moveBack deletes block2's).
	_, err = utxoStore.Create(ctx, coinbaseTx2, 2)
	require.NoError(t, err)
	_, err = utxoStore.Create(ctx, coinbaseTx3, 2)
	require.NoError(t, err)
	// Seed the mempool txs so the reorg's longest-chain marks hit real records.
	_, err = utxoStore.Create(ctx, minedTxTx, 2)
	require.NoError(t, err)
	_, err = utxoStore.Create(ctx, keepTxTx, 2)
	require.NoError(t, err)

	blockToMoveBack := &model.Block{
		Header:     block2Header,
		Height:     2,
		CoinbaseTx: coinbaseTx2,
		Subtrees:   []*chainhash.Hash{}, // orphaned block carried no mempool txs
	}
	blockToMoveForward := &model.Block{
		Header:           blockNewHeader,
		Height:           2,
		CoinbaseTx:       coinbaseTx3,
		Subtrees:         []*chainhash.Hash{fwdSubtreeHash},
		TransactionCount: 2, // coinbase + minedTx
	}

	// Node is on block2, holding minedTx and keepTx in its current subtree.
	stp.InitCurrentBlockHeader(block2Header)
	stp.AddBatch(
		[]subtree.Node{
			{Hash: minedTx, Fee: 1, SizeInBytes: 100},
			{Hash: keepTx, Fee: 1, SizeInBytes: 100},
		},
		[]*subtree.TxInpoints{{}, {}},
	)

	// Settle: coinbase placeholder + both txs in the current (incomplete) subtree.
	// (GetChainedSubtrees synthesizes an incomplete-subtree copy when no subtree has
	// completed, so GetCurrentLength is the reliable settle signal here.)
	require.Eventually(t, func() bool {
		return stp.GetCurrentLength() == 3
	}, 5*time.Second, 10*time.Millisecond, "both mempool txs must settle into the current subtree")

	// No subtree has completed yet: everything is still in the current subtree.
	require.Equal(t, 1, stp.SubtreeCount(), "no completed chained subtree expected before the reorg")

	// Precondition: both txs live in the current subtree.
	require.True(t, currentSubtreeContains(stp, minedTx), "precondition: minedTx must be in the current subtree")
	require.True(t, currentSubtreeContains(stp, keepTx), "precondition: keepTx must be in the current subtree")

	// Reorg: orphan block2, adopt blockNew (which mines minedTx).
	require.NoError(t, stp.Reorg([]*model.Block{blockToMoveBack}, []*model.Block{blockToMoveForward}),
		"reorg must succeed")

	// Tip advanced to the moved-forward block.
	require.Equal(t, blockNewHeader.Hash().String(), stp.GetCurrentBlockHeader().Hash().String(),
		"tip must advance to the moved-forward block")

	// Invariants must hold after the reorg (also syncs through the processor goroutine).
	require.NoError(t, stp.CheckSubtreeProcessor(),
		"subtree processor invariants must hold after a reorg that invalidates a current-subtree tx")

	// minedTx was invalidated out of block assembly (now on the longest chain);
	// keepTx remains available for re-mining.
	txMap := stp.GetCurrentTxMap()
	require.False(t, txMap.Exists(minedTx),
		"a current-subtree tx mined by the new chain must be evicted from block assembly")
	require.True(t, txMap.Exists(keepTx),
		"a current-subtree tx not on the new chain must remain in block assembly")
	require.False(t, currentSubtreeContains(stp, minedTx),
		"minedTx must no longer be present in the current subtree")
	require.True(t, currentSubtreeContains(stp, keepTx),
		"keepTx must still be present in the current subtree")

	// UTXO-level confirmation of the invalidation. minedTx is now mined on the longest
	// chain (unmined_since cleared to 0); keepTx is marked not-on-longest-chain
	// (unmined_since set) so it remains recoverable for re-mining.
	minedMeta, err := utxoStore.Get(ctx, &minedTx, fields.UnminedSince)
	require.NoError(t, err)
	require.Equal(t, uint32(0), minedMeta.UnminedSince,
		"minedTx must be marked on the longest chain (unmined_since cleared) after the reorg mined it")

	keepMeta, err := utxoStore.Get(ctx, &keepTx, fields.UnminedSince)
	require.NoError(t, err)
	require.NotEqual(t, uint32(0), keepMeta.UnminedSince,
		"keepTx must be marked not on the longest chain (unmined_since set) so it stays available for re-mining")
}

// storeReorgSubtreeWithConflicts stores a real subtree (coinbase + leaves) that also
// carries a ConflictingNodes trailer, so moveForwardBlock's from-store path reads the
// conflicting hashes back via DeserializeHashesFromReaderIntoBuckets and feeds them to
// conflict processing.
func storeReorgSubtreeWithConflicts(t *testing.T, ctx context.Context, blobStore *blob_memory.Memory, key *chainhash.Hash, leaves []subtree.Node, conflicting []chainhash.Hash) {
	t.Helper()

	st, err := subtree.NewTreeByLeafCount(64)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())

	for _, n := range leaves {
		require.NoError(t, st.AddSubtreeNode(n))
	}
	for _, c := range conflicting {
		require.NoError(t, st.AddConflictingNode(c))
	}

	stBytes, err := st.Serialize()
	require.NoError(t, err)
	require.NoError(t, blobStore.Set(ctx, key[:], fileformat.FileTypeSubtree, stBytes))
}

// TestReorg_FromStoreConflictingNodes verifies the store-driven conflict path of
// BA-REORG-011: when moveForwardBlock reads a block subtree whose ConflictingNodes
// trailer flags a winning tx, conflict resolution runs and the displaced mempool loser
// is evicted from block assembly while an unrelated tx is preserved.
//
// The UTXO store is a testify mock (mirroring TestProcessConflictingTransactions): the
// deep ProcessConflicting mechanics are covered there and by the sql store's own tests.
// This test targets the integration wiring that no existing test exercises end-to-end:
// stored ConflictingNodes -> CreateTransactionMap -> processConflictingTransactions ->
// losing-tx eviction from the assembly.
func TestReorg_FromStoreConflictingNodes(t *testing.T) {
	ctx := context.Background()

	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

	blobStore := blob_memory.New()

	newSubtreeChan := make(chan NewSubtreeRequest, 100)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	// Mempool loser (displaced by the conflict) and an unrelated keeper.
	loserTx := chainhash.HashH([]byte("conflict-loser"))
	keepTx := chainhash.HashH([]byte("conflict-keep"))
	// The winning tx the block mines and lists in its subtree's ConflictingNodes.
	winnerTx := chainhash.HashH([]byte("conflict-winner"))

	mockBC := &blockchain.Mock{}
	mockBC.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)
	mockBC.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	// ProcessConflicting free-function call chain (mirrors TestProcessConflictingTransactions):
	// the winner is Conflicting=true and its counter-conflicting set is the loser.
	mockUtxo := &utxo.MockUtxostore{}
	mockUtxo.On("Get", mock.Anything, mock.Anything, mock.Anything).Return(&meta.Data{Conflicting: true}, nil)
	mockUtxo.On("GetCounterConflicting", mock.Anything, mock.Anything).Return([]chainhash.Hash{loserTx}, nil)
	mockUtxo.On("SetConflicting", mock.Anything, mock.Anything, mock.Anything).Return([]*utxo.Spend{}, []chainhash.Hash{}, nil)
	mockUtxo.On("Unspend", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockUtxo.On("Spend", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return([]*utxo.Spend{}, nil)
	mockUtxo.On("SetLocked", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockUtxo.On("Create", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&meta.Data{}, nil)

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, settings, blobStore, mockBC, mockUtxo, newSubtreeChan)
	require.NoError(t, err)
	stp.Start(ctx)
	t.Cleanup(func() { stp.Stop(context.Background()) })

	// Assembly holds the loser and the keeper in the current subtree.
	stp.AddBatch(
		[]subtree.Node{
			{Hash: loserTx, Fee: 1, SizeInBytes: 100},
			{Hash: keepTx, Fee: 1, SizeInBytes: 100},
		},
		[]*subtree.TxInpoints{{}, {}},
	)
	require.Eventually(t, func() bool {
		return stp.GetCurrentLength() == 3
	}, 5*time.Second, 10*time.Millisecond, "loser + keeper must settle into the current subtree")

	// The block's subtree: leaf = winner, ConflictingNodes = [winner].
	fwdSubtreeHash, err := chainhash.NewHashFromStr("5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e")
	require.NoError(t, err)
	storeReorgSubtreeWithConflicts(t, ctx, blobStore, fwdSubtreeHash,
		[]subtree.Node{{Hash: winnerTx, Fee: 1, SizeInBytes: 100}},
		[]chainhash.Hash{winnerTx},
	)

	stp.InitCurrentBlockHeader(prevBlockHeader)
	block := &model.Block{
		Header: &model.BlockHeader{
			Version: 1, HashPrevBlock: prevBlockHeader.Hash(), HashMerkleRoot: &chainhash.Hash{},
			Timestamp: 2200000002, Bits: model.NBit{}, Nonce: 7777,
		},
		Height:           2,
		CoinbaseTx:       coinbaseTx2,
		Subtrees:         []*chainhash.Hash{fwdSubtreeHash},
		TransactionCount: 2,
	}

	require.NoError(t, stp.MoveForwardBlock(block), "move-forward carrying conflicting nodes must succeed")

	require.NoError(t, stp.CheckSubtreeProcessor(), "invariants must hold after a conflict-driven move-forward")

	txMap := stp.GetCurrentTxMap()
	require.False(t, txMap.Exists(loserTx), "the conflict loser must be evicted from block assembly")
	require.True(t, txMap.Exists(keepTx), "an unrelated tx must remain in block assembly")
	require.False(t, currentSubtreeContains(stp, loserTx), "the loser must not be in the current subtree")
	require.True(t, currentSubtreeContains(stp, keepTx), "the keeper must remain in the current subtree")

	// The stored ConflictingNodes drove conflict resolution (winner -> counter-conflicting loser).
	mockUtxo.AssertCalled(t, "GetCounterConflicting", mock.Anything, winnerTx)
}

// TestReorg_DuplicateMoveForward_NoCorruption verifies robustness against a duplicated or
// stale block notification at the subtree-processor layer: moving the same block forward a
// second time must be rejected by the HashPrevBlock guard (moveForwardBlock) and must leave
// the tip and assembly state untouched.
func TestReorg_DuplicateMoveForward_NoCorruption(t *testing.T) {
	ctx := context.Background()

	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

	utxoStoreURL, err := url.Parse("sqlitememory:///reorgDupMoveForward")
	require.NoError(t, err)
	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, settings, utxoStoreURL)
	require.NoError(t, err)

	blobStore := blob_memory.New()

	newSubtreeChan := make(chan NewSubtreeRequest, 100)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	mockBC := &blockchain.Mock{}
	mockBC.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, settings, blobStore, mockBC, utxoStore, newSubtreeChan)
	require.NoError(t, err)
	stp.Start(ctx)
	t.Cleanup(func() { stp.Stop(context.Background()) })

	minedTx := chainhash.HashH([]byte("dup-mined"))
	keepTx := chainhash.HashH([]byte("dup-keep"))

	stp.AddBatch(
		[]subtree.Node{
			{Hash: minedTx, Fee: 1, SizeInBytes: 100},
			{Hash: keepTx, Fee: 1, SizeInBytes: 100},
		},
		[]*subtree.TxInpoints{{}, {}},
	)
	require.Eventually(t, func() bool {
		return stp.GetCurrentLength() == 3
	}, 5*time.Second, 10*time.Millisecond, "both txs must settle into the current subtree")

	fwdSubtreeHash, err := chainhash.NewHashFromStr("6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f6f")
	require.NoError(t, err)
	storeReorgSubtree(t, ctx, blobStore, fwdSubtreeHash, []subtree.Node{
		{Hash: minedTx, Fee: 1, SizeInBytes: 100},
	})

	stp.InitCurrentBlockHeader(prevBlockHeader)
	block := &model.Block{
		Header: &model.BlockHeader{
			Version: 1, HashPrevBlock: prevBlockHeader.Hash(), HashMerkleRoot: &chainhash.Hash{},
			Timestamp: 2300000002, Bits: model.NBit{}, Nonce: 8888,
		},
		Height:           2,
		CoinbaseTx:       coinbaseTx2,
		Subtrees:         []*chainhash.Hash{fwdSubtreeHash},
		TransactionCount: 2,
	}

	// First move-forward succeeds and advances the tip.
	require.NoError(t, stp.MoveForwardBlock(block))
	require.Equal(t, block.Header.Hash().String(), stp.GetCurrentBlockHeader().Hash().String())
	require.NoError(t, stp.CheckSubtreeProcessor())
	require.False(t, stp.GetCurrentTxMap().Exists(minedTx), "minedTx should be gone after the first move-forward")
	require.True(t, stp.GetCurrentTxMap().Exists(keepTx), "keepTx should remain after the first move-forward")

	// Second (duplicate) move-forward of the SAME block: its HashPrevBlock no longer matches
	// the current tip, so it must be rejected without mutating any state.
	err = stp.MoveForwardBlock(block)
	require.Error(t, err, "a duplicate move-forward of the same block must be rejected")
	require.Contains(t, err.Error(), "does not match the current block header")

	// No corruption: tip and assembly unchanged, invariants intact.
	require.Equal(t, block.Header.Hash().String(), stp.GetCurrentBlockHeader().Hash().String(),
		"tip must be unchanged after the rejected duplicate")
	require.NoError(t, stp.CheckSubtreeProcessor(), "invariants must hold after the rejected duplicate")
	require.False(t, stp.GetCurrentTxMap().Exists(minedTx))
	require.True(t, stp.GetCurrentTxMap().Exists(keepTx), "keepTx must still be present after the rejected duplicate")
}

// TestReorg_RollbackOnPartialFailure verifies BA-REORG-007: a partial failure during a
// reorg MUST leave the best-block reference unchanged.
//
// The reorg applies a move-back block first (which genuinely mutates assembly state and
// advances the current header to the common ancestor), then a move-forward block whose
// PreviousHash does NOT chain onto the post-move-back tip. That mismatch trips
// moveForwardBlock's header guard *after* move-back has already run, forcing a mid-reorg
// failure whose deferred rollback in reorgBlocks must restore the pre-reorg state.
func TestReorg_RollbackOnPartialFailure(t *testing.T) {
	ctx := context.Background()

	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

	utxoStoreURL, err := url.Parse("sqlitememory:///reorgRollback")
	require.NoError(t, err)
	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, settings, utxoStoreURL)
	require.NoError(t, err)

	blobStore := blob_memory.New()

	newSubtreeChan := make(chan NewSubtreeRequest, 100)
	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()
	t.Cleanup(func() { close(newSubtreeChan) })

	mockBC := &blockchain.Mock{}
	mockBC.On("GetBlocksMinedNotSet", mock.Anything).Return([]*model.Block{}, nil)
	mockBC.On("SetBlockProcessedAt", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mockBC.On("GetBlockHeader", mock.Anything, mock.Anything).Return(prevBlockHeader, &model.BlockHeaderMeta{}, nil)
	mockBC.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)

	stp, err := NewSubtreeProcessor(ctx, ulogger.TestLogger{}, settings, blobStore, mockBC, utxoStore, newSubtreeChan)
	require.NoError(t, err)
	stp.Start(ctx)
	t.Cleanup(func() { stp.Stop(context.Background()) })

	// Pre-reorg mempool: txA, txB in the current subtree; tip = block2Header.
	txA := chainhash.HashH([]byte("rollback-A"))
	txB := chainhash.HashH([]byte("rollback-B"))
	stp.AddBatch(
		[]subtree.Node{
			{Hash: txA, Fee: 1, SizeInBytes: 100},
			{Hash: txB, Fee: 1, SizeInBytes: 100},
		},
		[]*subtree.TxInpoints{{}, {}},
	)
	require.Eventually(t, func() bool {
		return stp.GetCurrentLength() == 3
	}, 5*time.Second, 10*time.Millisecond, "txA, txB must settle into the current subtree")

	block2Header := &model.BlockHeader{Version: 1, HashPrevBlock: prevBlockHeader.Hash(), HashMerkleRoot: &chainhash.Hash{}, Timestamp: 2400000002, Bits: model.NBit{}, Nonce: 9101}
	stp.InitCurrentBlockHeader(block2Header)

	// Snapshot the pre-reorg tip for the compliance assertion.
	preReorgTip := stp.GetCurrentBlockHeader().Hash().String()
	require.Equal(t, block2Header.Hash().String(), preReorgTip)

	// Move-back block (block2) carries a real subtree with txBack, so move-back genuinely
	// mutates assembly (re-adds txBack) before the forced failure.
	txBack := chainhash.HashH([]byte("rollback-back"))
	backSubtreeHash, err := chainhash.NewHashFromStr("7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a")
	require.NoError(t, err)
	storeReorgSubtree(t, ctx, blobStore, backSubtreeHash, []subtree.Node{{Hash: txBack, Fee: 1, SizeInBytes: 100}})
	_, err = utxoStore.Create(ctx, coinbaseTx2, 2)
	require.NoError(t, err)
	blockToMoveBack := &model.Block{Header: block2Header, Height: 2, CoinbaseTx: coinbaseTx2, Subtrees: []*chainhash.Hash{backSubtreeHash}}

	// Move-forward block whose PreviousHash is block2 (NOT block2's parent). After move-back
	// sets the header to block2's parent (prevBlockHeader), this fails moveForwardBlock's
	// header guard, injecting a mid-reorg failure.
	badHeader := &model.BlockHeader{Version: 1, HashPrevBlock: block2Header.Hash(), HashMerkleRoot: &chainhash.Hash{}, Timestamp: 2400000003, Bits: model.NBit{}, Nonce: 9102}
	blockBad := &model.Block{Header: badHeader, Height: 3, CoinbaseTx: coinbaseTx3, Subtrees: []*chainhash.Hash{}}

	err = stp.Reorg([]*model.Block{blockToMoveBack}, []*model.Block{blockBad})
	require.Error(t, err, "the injected mid-reorg failure must surface as an error")

	// BA-REORG-007: the best-block reference must be unchanged after the partial failure.
	require.Equal(t, preReorgTip, stp.GetCurrentBlockHeader().Hash().String(),
		"best-block reference must roll back to the pre-reorg tip after a partial failure")

	// The pre-reorg mempool transactions must still be in assembly.
	txMap := stp.GetCurrentTxMap()
	require.True(t, txMap.Exists(txA), "pre-reorg tx must survive the rolled-back reorg")
	require.True(t, txMap.Exists(txB), "pre-reorg tx must survive the rolled-back reorg")

	// Strengthen this to a hard requirement: rollback must restore internal
	// subtree-processor consistency, not only the best-block reference.
	require.NoError(t, stp.CheckSubtreeProcessor(),
		"rolled-back reorg must restore subtree processor invariants")
	require.False(t, txMap.Exists(txBack),
		"move-back-only tx must not remain in currentTxMap after rollback")
}
