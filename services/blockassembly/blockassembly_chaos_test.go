// Package blockassembly provides functionality for assembling Bitcoin blocks in Teranode.
package blockassembly

import (
	"context"
	"encoding/binary"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	utxofields "github.com/bsv-blockchain/teranode/stores/utxo/fields"
	utxostoresql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
)

// chaosTestItems extends baTestItems with a real blob store so tests can write
// subtree files directly and exercise the full blob-based conflict pipeline.
type chaosTestItems struct {
	baTestItems
	blobStore *memory.Memory
}

// setupChaosTest is like setupBlockAssemblyTestWithUtxoStore but passes a real
// in-memory blob store to NewBlockAssembler. Required for tests that exercise
// getConflictingNodes → ProcessConflicting → assembly dequeue via subtree blobs.
func setupChaosTest(t *testing.T) *chaosTestItems {
	t.Helper()

	ctx := t.Context()
	logger := ulogger.NewErrorTestLogger(t)

	tSettings := createTestSettings(t)

	// Use file-based SQLite (WAL mode) instead of in-memory SQLite.
	// In-memory SQLite uses shared-cache locking without WAL, so a write transaction
	// in SetConflicting blocks readers (s.Get, s.GetSpend) on separate connections —
	// causing a deadlock when markAsConflicting is called from the test goroutine.
	// File-based SQLite with WAL allows concurrent readers during a write transaction.
	utxoStoreURL, err := url.Parse("sqlite:///utxo")
	require.NoError(t, err)

	utxo, err := utxostoresql.New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	bcStore, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	bcClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, bcStore, nil, nil)
	require.NoError(t, err)

	blobStore := memory.New()
	newSubtreeChan := make(chan subtreeprocessor.NewSubtreeRequest, 100)

	stats := gocore.NewStat("test")

	ba, err := NewBlockAssembler(
		ctx,
		ulogger.TestLogger{},
		tSettings,
		stats,
		utxo,
		blobStore,
		bcClient,
		newSubtreeChan,
	)
	require.NoError(t, err)
	require.NotNil(t, ba.subtreeProcessor)

	t.Cleanup(func() {
		if ba.subtreeProcessor != nil {
			ba.subtreeProcessor.Stop(context.Background())
		}
	})

	ba.subtreeProcessor.Start(t.Context())

	return &chaosTestItems{
		baTestItems: baTestItems{
			utxoStore:        utxo,
			newSubtreeChan:   newSubtreeChan,
			blockchainClient: bcClient,
			blockAssembler:   ba,
		},
		blobStore: blobStore,
	}
}

// buildConflictPair creates a winner tx W and loser tx L in the UTXO store such that
// both spend the same parent output. The UTXO state reflects L as the first spender
// (spending_data points to L) and W marked Conflicting=true (as the Validator would set it).
// GetCounterConflicting(W) will therefore return L as the losing tx.
//
// baseLockTime seeds all three tx lock times so multiple pairs can coexist in the same UTXO
// store without hash collisions (use distinct values, e.g. 77 and 177).
func buildConflictPair(ctx context.Context, t *testing.T, utxo *utxostoresql.Store, baseLockTime uint32) (winner, loser *bt.Tx) {
	t.Helper()

	// Parent tx P: fake input (not validated by Create), one real output.
	txP := bt.NewTx()
	txP.LockTime = baseLockTime
	pIn := &bt.Input{
		PreviousTxOutIndex: 0,
		PreviousTxSatoshis: 300000,
		SequenceNumber:     0xFFFFFFFF,
		UnlockingScript:    bscript.NewFromBytes([]byte{}),
	}
	_ = pIn.PreviousTxIDAdd(&chainhash.Hash{7, 7, 7})
	txP.Inputs = []*bt.Input{pIn}
	txP.Outputs = []*bt.Output{
		{Satoshis: 200000, LockingScript: bscript.NewFromBytes([]byte{0x76, 0xa9, 0x14, 0x00, 0x88, 0xac})},
	}
	require.NoError(t, utxo.SetBlockHeight(1))
	_, err := utxo.Create(ctx, txP, 1)
	require.NoError(t, err)
	pHash := txP.TxIDChainHash()
	require.NoError(t, utxo.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*pHash}, true))

	// Loser L: spends P[0], inserted first so P[0].spending_data = L via raw SQL.
	txL := bt.NewTx()
	txL.LockTime = baseLockTime + 1
	_ = txL.From(pHash.String(), 0, txP.Outputs[0].LockingScript.String(), txP.Outputs[0].Satoshis)
	txL.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})
	txL.Outputs = []*bt.Output{{Satoshis: 180000, LockingScript: bscript.NewFromBytes([]byte{0x52})}}
	_, err = utxo.Create(ctx, txL, 1)
	require.NoError(t, err)
	lHash := txL.TxIDChainHash()

	// Write spending_data[P[0]] = L so GetCounterConflicting(W) finds L.
	// Format: 32 bytes txID + 4 bytes vin index (little-endian).
	sdBytes := make([]byte, 36)
	copy(sdBytes[:32], lHash.CloneBytes())
	binary.LittleEndian.PutUint32(sdBytes[32:], 0)
	_, err = utxo.RawDB().Exec(
		`UPDATE outputs SET spending_data = ? WHERE transaction_id = (SELECT id FROM transactions WHERE hash = ?) AND idx = 0`,
		sdBytes, pHash[:],
	)
	require.NoError(t, err)

	// Winner W: also spends P[0]. Insert into UTXO (Create stores inputs so fields.Tx works).
	txW := bt.NewTx()
	txW.LockTime = baseLockTime + 2
	_ = txW.From(pHash.String(), 0, txP.Outputs[0].LockingScript.String(), txP.Outputs[0].Satoshis)
	txW.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})
	txW.Outputs = []*bt.Output{{Satoshis: 175000, LockingScript: bscript.NewFromBytes([]byte{0x53})}}
	_, err = utxo.Create(ctx, txW, 1)
	require.NoError(t, err)
	wHash := txW.TxIDChainHash()

	// Mark W conflicting=true: the Validator sets this when it detects the double-spend.
	_, err = utxo.RawDB().Exec(`UPDATE transactions SET conflicting = 1 WHERE hash = ?`, wHash[:])
	require.NoError(t, err)

	return txW, txL
}

// writeSubtreeBlobWithConflict creates a minimal subtree that contains winnerHash as both
// a regular node and a conflicting node, serializes it, stores it in the blob store,
// and returns the subtree root hash. The block's Subtrees field should reference this hash.
func writeSubtreeBlobWithConflict(ctx context.Context, t *testing.T, store blob.Store, winnerHash chainhash.Hash) chainhash.Hash {
	t.Helper()

	st, err := subtreepkg.NewTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddSubtreeNode(subtreepkg.Node{Hash: winnerHash, Fee: 1000, SizeInBytes: 250}))
	require.NoError(t, st.AddConflictingNode(winnerHash))

	data, err := st.Serialize()
	require.NoError(t, err)

	stHash := *st.RootHash()
	require.NoError(t, store.Set(ctx, stHash[:], fileformat.FileTypeSubtree, data))

	return stHash
}

// addBlockForChaosTest adds a block to the blockchain store with mined_set=true and
// returns a coinbase tx suitable for use in a model.Block for MoveForwardBlock/Reorg.
func addBlockForChaosTest(ctx context.Context, t *testing.T, items *chaosTestItems, blockHeader *model.BlockHeader) *bt.Tx {
	t.Helper()

	coinbaseTx, err := bt.NewTxFromString("02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff03510101ffffffff0100f2052a01000000232103656065e6886ca1e947de3471c9e723673ab6ba34724476417fa9fcef8bafa604ac00000000")
	require.NoError(t, err)

	err = items.blockchainClient.AddBlock(ctx, &model.Block{
		Header:           blockHeader,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}, "", blockchainoptions.WithMinedSet(true))
	require.NoError(t, err)

	return coinbaseTx
}

// buildUnminedTxInUTXO creates a parent tx P (with a nonexistent grandparent — not validated
// by Create) and a child tx L spending P[0]. Both are inserted into the UTXO store. P is on
// the longest chain; L is unmined. Returns L and its hash.
//
// SetConflicting(L) calls updateParentConflictingChildren which looks up P.id in the
// transactions table. Because P IS in the store (from Create), the lookup succeeds. This
// is the minimal setup needed for markAsConflicting to work on the SQL UTXO store.
func buildUnminedTxInUTXO(ctx context.Context, t *testing.T, utxo *utxostoresql.Store, lockTime uint32) (*bt.Tx, *chainhash.Hash) {
	t.Helper()

	// Parent tx P: fake grandparent input is fine because P itself is not conflicting,
	// so Create never calls updateParentConflictingChildren for P.
	txP := bt.NewTx()
	txP.LockTime = lockTime + 1000
	pIn := &bt.Input{
		PreviousTxOutIndex: 0,
		PreviousTxSatoshis: 100000,
		SequenceNumber:     0xFFFFFFFF,
		UnlockingScript:    bscript.NewFromBytes([]byte{}),
	}
	_ = pIn.PreviousTxIDAdd(&chainhash.Hash{0xaa, 0xbb, 0xcc}) // fake grandparent, not validated
	txP.Inputs = []*bt.Input{pIn}
	txP.Outputs = []*bt.Output{
		{Satoshis: 90000, LockingScript: bscript.NewFromBytes([]byte{0x76, 0xa9, 0x14, 0x00, 0x88, 0xac})},
	}
	require.NoError(t, utxo.SetBlockHeight(1))
	_, err := utxo.Create(ctx, txP, 1)
	require.NoError(t, err)
	pHash := txP.TxIDChainHash()
	require.NoError(t, utxo.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*pHash}, true))

	// Child tx L: spends P[0]. Parent P IS in transactions table, so SetConflicting(L)
	// can successfully call updateParentConflictingChildren.
	txL := bt.NewTx()
	txL.LockTime = lockTime
	_ = txL.From(pHash.String(), 0, txP.Outputs[0].LockingScript.String(), txP.Outputs[0].Satoshis)
	txL.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{})
	txL.Outputs = []*bt.Output{{Satoshis: 80000, LockingScript: bscript.NewFromBytes([]byte{0x52})}}
	_, err = utxo.Create(ctx, txL, 1)
	require.NoError(t, err)
	lHash := txL.TxIDChainHash()

	return txL, lHash
}

// waitForHashAbsent polls GetTransactionHashes (serialized through the subtreeprocessor main loop)
// until hash is no longer present. Safe to use after Remove because both go through the same
// loop — the hash disappears from GetTransactionHashes only after removeTxFromSubtrees runs.
func waitForHashAbsent(t *testing.T, ba *BlockAssembler, hash chainhash.Hash, msg string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return !slices.Contains(ba.subtreeProcessor.GetTransactionHashes(), hash)
	}, 5*time.Second, 10*time.Millisecond, msg)
}

// waitForHashPresent polls GetTransactionHashes until hash is present. Use this to confirm
// an async AddTxBatch has been processed before proceeding with the next test step.
func waitForHashPresent(t *testing.T, ba *BlockAssembler, hash chainhash.Hash, msg string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return slices.Contains(ba.subtreeProcessor.GetTransactionHashes(), hash)
	}, 5*time.Second, 10*time.Millisecond, msg)
}

// TestMarkAsConflicting_EndToEnd_EjectsFromAssembly verifies the Path A conflict pipeline:
// Validator calls markAsConflicting → tx is removed from assembly and marked conflicting
// in the UTXO store. This is the direct external interface between SubtreeValidation and
// BlockAssembly.
func TestMarkAsConflicting_EndToEnd_EjectsFromAssembly(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupChaosTest(t)

	sqlStore, ok := items.utxoStore.(*utxostoresql.Store)
	require.True(t, ok, "test requires SQLite store")

	// Create txL in UTXO so markAsConflicting → SetConflicting → updateParentConflictingChildren works.
	// buildUnminedTxInUTXO ensures the parent tx P is in the transactions table so the FK subselect succeeds.
	_, lHash := buildUnminedTxInUTXO(ctx, t, sqlStore, 42)

	// Add L to assembly.
	items.blockAssembler.AddTxBatch(
		[]subtreepkg.Node{{Hash: *lHash, Fee: 1000, SizeInBytes: 250}},
		[]*subtreepkg.TxInpoints{{}},
	)
	waitForHashPresent(t, items.blockAssembler, *lHash, "txL should be in assembly before markAsConflicting")

	// Invoke the Path A interface: Validator→BA.
	items.blockAssembler.markAsConflicting(ctx, *lHash)

	// Verify UTXO store has txL marked conflicting (markAsConflicting is sync up to this point).
	meta, err := sqlStore.Get(ctx, lHash, utxofields.Conflicting)
	require.NoError(t, err)
	require.True(t, meta.Conflicting, "txL must be marked conflicting in UTXO after markAsConflicting")

	// TxCount is NOT decremented by Remove — use GetTransactionHashes (serialized via main loop)
	// to confirm the tx is actually removed from the subtrees.
	waitForHashAbsent(t, items.blockAssembler, *lHash, "txL should be removed from assembly after markAsConflicting")
}

// TestMarkAsConflicting_ConflictingTxNotReloadedByLightReset verifies that a tx ejected
// by markAsConflicting (conflicting=true in UTXO) is not brought back by a subsequent
// light reset (validateInputs=false). The loadUnminedTransactions query filters by
// WHERE conflicting=false, which naturally excludes the tx.
func TestMarkAsConflicting_ConflictingTxNotReloadedByLightReset(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupChaosTest(t)

	sqlStore, ok := items.utxoStore.(*utxostoresql.Store)
	require.True(t, ok, "test requires SQLite store")

	// Create txL in UTXO with a real parent so SetConflicting → updateParentConflictingChildren works.
	// Create sets unmined_since automatically, so loadUnminedTransactions will see it.
	_, lHash := buildUnminedTxInUTXO(ctx, t, sqlStore, 43)

	// Confirm txL has unmined_since set (prerequisite for loadUnminedTransactions to see it).
	metaBefore, err := sqlStore.Get(ctx, lHash, utxofields.UnminedSince, utxofields.Conflicting)
	require.NoError(t, err)
	require.NotZero(t, metaBefore.UnminedSince, "txL must be in unmined pool before test")
	require.False(t, metaBefore.Conflicting, "txL must not be conflicting before markAsConflicting")

	// Add L to assembly then eject via markAsConflicting.
	items.blockAssembler.AddTxBatch(
		[]subtreepkg.Node{{Hash: *lHash, Fee: 1000, SizeInBytes: 250}},
		[]*subtreepkg.TxInpoints{{}},
	)
	waitForHashPresent(t, items.blockAssembler, *lHash, "txL should be in assembly before markAsConflicting")

	items.blockAssembler.markAsConflicting(ctx, *lHash)
	waitForHashAbsent(t, items.blockAssembler, *lHash, "txL should be ejected by markAsConflicting")

	// Directly call loadUnminedTransactions (same as light reset does) and confirm txL stays out.
	// The WHERE conflicting=false filter must exclude txL.
	items.blockAssembler.settings.BlockAssembly.OnRestartValidateParentChain = false
	err = items.blockAssembler.loadUnminedTransactions(ctx, false)
	require.NoError(t, err)

	// loadUnminedTransactions filters by WHERE conflicting=false, so txL must stay absent.
	waitForHashAbsent(t, items.blockAssembler, *lHash,
		"txL must not be reloaded into assembly: conflicting=true must block loadUnminedTransactions")
}

// TestMoveForward_ConflictPipeline_EjectsLoserViaSubtreeBlob verifies the Path B conflict
// pipeline: getConflictingNodes reads W from the subtree blob, ProcessConflicting resolves
// L as the loser via GetCounterConflicting(W), and L is dequeued from assembly.
//
// This tests the end-to-end wiring that is exercised in production when a mined block
// contains a winner tx that double-spends an assembly tx.
func TestMoveForward_ConflictPipeline_EjectsLoserViaSubtreeBlob(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupChaosTest(t)

	sqlStore, ok := items.utxoStore.(*utxostoresql.Store)
	require.True(t, ok, "test requires SQLite store")

	// Build winner W (Conflicting=true in UTXO) and loser L (spending_data[P[0]] = L).
	txW, txL := buildConflictPair(ctx, t, sqlStore, 77)
	wHash := txW.TxIDChainHash()
	lHash := txL.TxIDChainHash()

	// Write a subtree blob for block2 that lists W as a conflicting node.
	// getConflictingNodes reads this and passes W to ProcessConflicting.
	stHash := writeSubtreeBlobWithConflict(ctx, t, items.blobStore, *wHash)

	// Blockchain store requires blocks in parent-child order: add blockHeader1 before blockHeader2.
	addBlockForChaosTest(ctx, t, items, blockHeader1)
	coinbaseTx := addBlockForChaosTest(ctx, t, items, blockHeader2)

	// Add L to assembly.
	items.blockAssembler.AddTxBatch(
		[]subtreepkg.Node{{Hash: *lHash, Fee: 1000, SizeInBytes: 250}},
		[]*subtreepkg.TxInpoints{{}},
	)
	waitForHashPresent(t, items.blockAssembler, *lHash, "txL should be in assembly before MoveForwardBlock")

	// Advance BA to block1 so it knows block2 is the next block.
	items.blockAssembler.setBestBlockHeader(blockHeader1, 1)
	items.blockAssembler.subtreeProcessor.InitCurrentBlockHeader(blockHeader1)

	// Trigger MoveForwardBlock. The block has W in its subtree's ConflictingNodes.
	// Path B: blob read → getConflictingNodes([W]) → ProcessConflicting([W]) →
	//         GetCounterConflicting(W) → [L] → L ejected from assembly.
	block2 := &model.Block{
		Header:           blockHeader2,
		Height:           2,
		CoinbaseTx:       coinbaseTx,
		TransactionCount: 2,
		Subtrees:         []*chainhash.Hash{&stHash},
	}
	err := items.blockAssembler.subtreeProcessor.MoveForwardBlock(block2)
	require.NoError(t, err, "MoveForwardBlock should succeed with conflict processing")

	waitForHashAbsent(t, items.blockAssembler, *lHash,
		"txL (loser) must be ejected from assembly after MoveForward processes the conflict blob")
}

// TestReset_FastForward_EachIntermediateBlockEjectsItsConflicts verifies that during a
// multi-block reset fast-forward, each intermediate block's conflict markers are processed
// individually. Before the intermediate-block fix, only the last block ran finalizeBlockProcessing;
// this test extends that to verify conflict ejection also happens per-block.
func TestReset_FastForward_EachIntermediateBlockEjectsItsConflicts(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupChaosTest(t)

	sqlStore, ok := items.utxoStore.(*utxostoresql.Store)
	require.True(t, ok, "test requires SQLite store")

	// Build two independent winner/loser conflict pairs:
	//   block2 mines W2, which conflicts with L2 in assembly.
	//   block3 mines W3, which conflicts with L3 in assembly.
	// Use distinct baseLockTime values so the parent tx hashes differ (no TX_EXISTS collision).
	txW2, txL2 := buildConflictPair(ctx, t, sqlStore, 77)
	w2Hash := txW2.TxIDChainHash()
	l2Hash := txL2.TxIDChainHash()

	txW3, txL3 := buildConflictPair(ctx, t, sqlStore, 177)
	w3Hash := txW3.TxIDChainHash()
	l3Hash := txL3.TxIDChainHash()

	// Write subtree blobs: block2 has W2 as conflicting, block3 has W3 as conflicting.
	st2Hash := writeSubtreeBlobWithConflict(ctx, t, items.blobStore, *w2Hash)
	st3Hash := writeSubtreeBlobWithConflict(ctx, t, items.blobStore, *w3Hash)

	// Add blocks to blockchain store with mined_set=true.
	addBlockForChaosTest(ctx, t, items, blockHeader1)
	coinbase2 := addBlockForChaosTest(ctx, t, items, blockHeader2)
	coinbase3 := addBlockForChaosTest(ctx, t, items, blockHeader3)

	// Set BA at block1. Blockchain best is block3. Reset will fast-forward block2, block3.
	items.blockAssembler.setBestBlockHeader(blockHeader1, 1)
	items.blockAssembler.subtreeProcessor.InitCurrentBlockHeader(blockHeader1)

	// Add L2 and L3 to assembly.
	items.blockAssembler.AddTxBatch([]subtreepkg.Node{
		{Hash: *l2Hash, Fee: 1000, SizeInBytes: 250},
		{Hash: *l3Hash, Fee: 1000, SizeInBytes: 250},
	}, []*subtreepkg.TxInpoints{{}, {}})
	waitForHashPresent(t, items.blockAssembler, *l2Hash, "L2 should be in assembly")
	waitForHashPresent(t, items.blockAssembler, *l3Hash, "L3 should be in assembly")

	// Provide the block objects with their subtree references so reset can read the blobs.
	// reset() calls getReorgBlockHeaders which uses blockchain client, then fetches full blocks.
	// The SubtreeProcessor.reset() receives moveForwardBlocks from the blockchain client.
	// We inject the subtree hashes by pre-populating the blockchain store's block subtrees.
	// Since the local blockchain client fetches full blocks from the store, and we added blocks
	// without subtrees, we override by calling Reset directly with pre-built block objects.
	block2 := &model.Block{
		Header:           blockHeader2,
		Height:           2,
		CoinbaseTx:       coinbase2,
		TransactionCount: 2,
		Subtrees:         []*chainhash.Hash{&st2Hash},
	}
	block3 := &model.Block{
		Header:           blockHeader3,
		Height:           3,
		CoinbaseTx:       coinbase3,
		TransactionCount: 2,
		Subtrees:         []*chainhash.Hash{&st3Hash},
	}

	// Call SubtreeProcessor.Reset directly with the pre-built blocks so the conflict blobs
	// are used. This mirrors what BlockAssembler.reset() does after getReorgBlocks.
	resp := items.blockAssembler.subtreeProcessor.Reset(
		blockHeader3,
		nil, // no moveBackBlocks
		[]*model.Block{block2, block3},
		false,
		nil,
	)
	require.NoError(t, resp.Err, "SubtreeProcessor.Reset should succeed with per-block conflict processing")

	// Both losers must be ejected: block2 ejected L2, block3 ejected L3.
	waitForHashAbsent(t, items.blockAssembler, *l2Hash, "L2 must be ejected: block2's conflict set must process its own loser")
	waitForHashAbsent(t, items.blockAssembler, *l3Hash, "L3 must be ejected: block3's conflict set must process its own loser")
}

// TestConflict_DuplicateWinnerInTwoBlocks_NoDoubleProcessingError verifies that when the
// same winner hash W appears in the subtree ConflictingNodes of two consecutive blocks
// (block2 and block3), the processedConflictingHashesMap guard prevents a
// "tx is not conflicting" error on the second block.
//
// Without the map: block2 processes W (sets conflicting=false at the end), block3 tries
// to process W again → W.Conflicting=false and W not in map → ProcessConflicting errors.
// With the map: W is added after block2 processes it, so block3 skips the conflicting check.
func TestConflict_DuplicateWinnerInTwoBlocks_NoDoubleProcessingError(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupChaosTest(t)

	sqlStore, ok := items.utxoStore.(*utxostoresql.Store)
	require.True(t, ok, "test requires SQLite store")

	// One conflict pair: W and L. W will appear in both block2 and block3 subtrees.
	txW, txL := buildConflictPair(ctx, t, sqlStore, 77)
	wHash := txW.TxIDChainHash()
	lHash := txL.TxIDChainHash()

	// Write one subtree blob for winner W. Both block2 and block3 reference the same blob
	// (same content → same root hash), so we write it once and share the hash.
	stSharedHash := writeSubtreeBlobWithConflict(ctx, t, items.blobStore, *wHash)

	addBlockForChaosTest(ctx, t, items, blockHeader1)
	coinbase2 := addBlockForChaosTest(ctx, t, items, blockHeader2)
	coinbase3 := addBlockForChaosTest(ctx, t, items, blockHeader3)

	items.blockAssembler.setBestBlockHeader(blockHeader1, 1)
	items.blockAssembler.subtreeProcessor.InitCurrentBlockHeader(blockHeader1)

	items.blockAssembler.AddTxBatch(
		[]subtreepkg.Node{{Hash: *lHash, Fee: 1000, SizeInBytes: 250}},
		[]*subtreepkg.TxInpoints{{}},
	)
	waitForHashPresent(t, items.blockAssembler, *lHash, "L should be in assembly before Reset")

	block2 := &model.Block{
		Header:           blockHeader2,
		Height:           2,
		CoinbaseTx:       coinbase2,
		TransactionCount: 2,
		Subtrees:         []*chainhash.Hash{&stSharedHash},
	}
	block3 := &model.Block{
		Header:           blockHeader3,
		Height:           3,
		CoinbaseTx:       coinbase3,
		TransactionCount: 2,
		Subtrees:         []*chainhash.Hash{&stSharedHash},
	}

	// Reset with two blocks that both reference the same winner W.
	// The processedConflictingHashesMap must prevent "tx is not conflicting" on block3.
	resp := items.blockAssembler.subtreeProcessor.Reset(
		blockHeader3,
		nil,
		[]*model.Block{block2, block3},
		false,
		nil,
	)
	require.NoError(t, resp.Err,
		"processedConflictingHashesMap must prevent double-processing error when the same winner W appears in two consecutive blocks")

	waitForHashAbsent(t, items.blockAssembler, *lHash, "L must be ejected (processed once, not twice)")
}

// TestReorg_LoserReturnsThenEjectedByWinnerChain verifies the full reorg scenario:
// 1. L is in assembly at block1.
// 2. Block2 is reorged out (moveBack). L's state is preserved in assembly.
// 3. Block2Alt is the winner chain (moveForward). Its subtree has W in ConflictingNodes.
// 4. ProcessConflicting(W) identifies L as the loser and ejects it from assembly.
//
// This is the exact production scenario where a reorg brings a tx back to assembly
// only for the winning chain to immediately eject it as a double-spend.
func TestReorg_LoserReturnsThenEjectedByWinnerChain(t *testing.T) {
	initPrometheusMetrics()

	ctx := t.Context()
	items := setupChaosTest(t)

	sqlStore, ok := items.utxoStore.(*utxostoresql.Store)
	require.True(t, ok, "test requires SQLite store")

	txW, txL := buildConflictPair(ctx, t, sqlStore, 77)
	wHash := txW.TxIDChainHash()
	lHash := txL.TxIDChainHash()

	// block2Alt is the winner chain block; its subtree marks W as conflicting.
	stAltHash := writeSubtreeBlobWithConflict(ctx, t, items.blobStore, *wHash)

	addBlockForChaosTest(ctx, t, items, blockHeader1)
	coinbase2 := addBlockForChaosTest(ctx, t, items, blockHeader2)
	coinbaseAlt := addBlockForChaosTest(ctx, t, items, blockHeader2Alt)

	// Start BA at block2 (the soon-to-be-reorged chain).
	items.blockAssembler.setBestBlockHeader(blockHeader2, 2)
	items.blockAssembler.subtreeProcessor.InitCurrentBlockHeader(blockHeader2)

	// L is in assembly before the reorg.
	items.blockAssembler.AddTxBatch(
		[]subtreepkg.Node{{Hash: *lHash, Fee: 1000, SizeInBytes: 250}},
		[]*subtreepkg.TxInpoints{{}},
	)
	waitForHashPresent(t, items.blockAssembler, *lHash, "L should be in assembly before reorg")

	// Execute reorg: move back block2 (no txs to recover, empty subtrees), move forward
	// block2Alt (has W in ConflictingNodes → ejects L).
	block2Empty := &model.Block{
		Header:           blockHeader2,
		Height:           2,
		CoinbaseTx:       coinbase2,
		TransactionCount: 1,
		Subtrees:         []*chainhash.Hash{},
	}
	block2AltConflicting := &model.Block{
		Header:           blockHeader2Alt,
		Height:           2,
		CoinbaseTx:       coinbaseAlt,
		TransactionCount: 2,
		Subtrees:         []*chainhash.Hash{&stAltHash},
	}

	err := items.blockAssembler.subtreeProcessor.Reorg(
		[]*model.Block{block2Empty},
		[]*model.Block{block2AltConflicting},
	)
	require.NoError(t, err, "Reorg should succeed")

	waitForHashAbsent(t, items.blockAssembler, *lHash,
		"L must be ejected by the winner chain (block2Alt) conflict processing during reorg")
}
