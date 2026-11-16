package blockassembly

import (
	"testing"
	"time"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	utxoStore "github.com/bsv-blockchain/teranode/stores/utxo"
	utxofields "github.com/bsv-blockchain/teranode/stores/utxo/fields"
	utxostoresql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// unminedReloadTimeout bounds a reload that is expected to finish in a few
// seconds. sqlitememory's shared-cache lock wait ignores the context, so a
// wedged reload never returns; the guard turns that into a failure instead of
// a hung test binary. It is sized well above the green run under -race.
const unminedReloadTimeout = 2 * time.Minute

var reloadTestLockingScript = bscript.NewFromBytes([]byte{0x76, 0xa9, 0x14, 0x00, 0x88, 0xac})

// createReloadTestTx stores tx as an unmined transaction at height 5.
func createReloadTestTx(t *testing.T, items *baTestItems, tx *bt.Tx) chainhash.Hash {
	t.Helper()

	_, _, err := items.utxoStore.SpendAndCreate(t.Context(), tx, 5, utxoStore.WithCreateOnly())
	require.NoError(t, err)

	return *tx.TxIDChainHash()
}

// spendingTx builds a one-output transaction spending each of the given outpoints.
func spendingTx(t *testing.T, parent chainhash.Hash, vouts ...uint32) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	for _, vout := range vouts {
		require.NoError(t, tx.From(parent.String(), vout, reloadTestLockingScript.String(), 1000))
	}

	for _, in := range tx.Inputs {
		in.UnlockingScript = bscript.NewFromBytes([]byte{})
	}

	tx.Outputs = []*bt.Output{{Satoshis: 900, LockingScript: reloadTestLockingScript}}

	return tx
}

// setSpendingData points output vout of tx at spender, bypassing Spend so the
// test controls exactly what input validation and SetConflicting will read.
func setSpendingData(t *testing.T, items *baTestItems, tx chainhash.Hash, vout int, spender chainhash.Hash) {
	t.Helper()

	sqlStore, ok := items.utxoStore.(*utxostoresql.Store)
	require.True(t, ok, "test requires the SQL store")

	sd := make([]byte, 36)
	copy(sd[:32], spender.CloneBytes())

	_, err := sqlStore.RawDB().Exec(
		"UPDATE outputs SET spending_data = ? WHERE transaction_id = (SELECT id FROM transactions WHERE hash = ?) AND idx = ?",
		sd, tx[:], vout,
	)
	require.NoError(t, err)
}

// runUnminedReloadWithInputValidation runs the input-validating reload with the
// given block ids as the best chain and fails the test if it does not return in
// time.
func runUnminedReloadWithInputValidation(t *testing.T, items *baTestItems, bestChainBlockIDs ...uint32) {
	t.Helper()

	genesisHeader := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Bits:           model.NBit{},
	}

	mockBC := &blockchain.Mock{}
	mockBC.On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).Return(append([]uint32{}, bestChainBlockIDs...), nil)
	items.blockAssembler.blockchainClient = mockBC
	items.blockAssembler.setBestBlockHeader(genesisHeader, 0)
	items.blockAssembler.subtreeProcessor.InitCurrentBlockHeader(genesisHeader)

	done := make(chan error, 1)

	go func() {
		done <- items.blockAssembler.loadUnminedTransactions(t.Context(), true, true)
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(unminedReloadTimeout):
		t.Fatalf("loadUnminedTransactions with input validation did not finish within %s on sqlitememory: a conflicting mark is waiting on the unmined iterator's open read cursor", unminedReloadTimeout)
	}
}

// TestLoadUnminedTransactions_SQLiteConflictMarkingDoesNotDeadlock reloads an
// unmined set on a real sqlitememory store in which a few transactions' inputs
// are spent by a different transaction, so input validation marks them
// conflicting, while the iterator still has thousands of rows to go.
//
// The SQL unmined iterator keeps a read cursor open on the transactions table
// until it has returned the last row. A SetConflicting UPDATE on that table
// waits for the cursor to close (SQLite's shared cache locks whole tables), and
// the iterator's own next read, the per-row inputs query, then waits behind the
// pending write. The cursor never advances, so the write never runs: one mark
// written from a worker while the cursor is open wedges the reload for good.
//
// The conflicting transactions are created first, so they sit in the first
// iterator batch (1024 rows by id). The rest of that batch and everything after
// it are filler rows that are already on the best chain: the workers set them
// aside without a store read, but the producer still runs its per-row queries
// for each, which keeps the cursor open long after the first mark is attempted.
func TestLoadUnminedTransactions_SQLiteConflictMarkingDoesNotDeadlock(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	require.NotNil(t, items)

	items.blockAssembler.settings.BlockAssembly.OnRestartValidateParentChain = false
	items.blockAssembler.settings.BlockAssembly.UnminedTxDiskSortEnabled = false

	const (
		numConflicting = 40
		numFiller      = 12000
		minedBlockID   = 1
	)

	ctx := t.Context()
	require.NoError(t, items.utxoStore.SetBlockHeight(5))

	parent := bt.NewTx()
	in := &bt.Input{
		PreviousTxOutIndex: 0,
		PreviousTxSatoshis: numConflicting * 2000,
		SequenceNumber:     0xFFFFFFFF,
		UnlockingScript:    bscript.NewFromBytes([]byte{}),
	}
	_ = in.PreviousTxIDAdd(&chainhash.Hash{9, 9, 9})
	parent.Inputs = []*bt.Input{in}

	for i := 0; i < numConflicting; i++ {
		parent.Outputs = append(parent.Outputs, &bt.Output{Satoshis: 1000, LockingScript: reloadTestLockingScript})
	}

	_, _, err := items.utxoStore.SpendAndCreate(ctx, parent, 1, utxoStore.WithCreateOnly())
	require.NoError(t, err)

	parentHash := *parent.TxIDChainHash()
	require.NoError(t, items.utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{parentHash}, true))

	winner := chainhash.HashH([]byte("sqlite-reload-deadlock-winner"))
	conflicting := make([]chainhash.Hash, 0, numConflicting)

	for i := 0; i < numConflicting; i++ {
		conflicting = append(conflicting, createReloadTestTx(t, items, spendingTx(t, parentHash, uint32(i))))

		// Spent by a winner that is none of these transactions, so input
		// validation takes Case 1 and marks each one conflicting.
		setSpendingData(t, items, parentHash, i, winner)
	}

	sqlStore, ok := items.utxoStore.(*utxostoresql.Store)
	require.True(t, ok, "test requires the SQL store")

	var lastConflictingID int64
	require.NoError(t, sqlStore.RawDB().QueryRow("SELECT MAX(id) FROM transactions").Scan(&lastConflictingID))

	for i := 0; i < numFiller; i++ {
		filler := bt.NewTx()
		fin := &bt.Input{
			PreviousTxOutIndex: uint32(i),
			PreviousTxSatoshis: 2000,
			SequenceNumber:     0xFFFFFFFF,
			UnlockingScript:    bscript.NewFromBytes([]byte{}),
		}
		_ = fin.PreviousTxIDAdd(&chainhash.Hash{8, 8, 8})
		filler.Inputs = []*bt.Input{fin}
		filler.Outputs = []*bt.Output{{Satoshis: 1000, LockingScript: reloadTestLockingScript}}

		createReloadTestTx(t, items, filler)
	}

	// The fillers stay unmined but carry a block id on the best chain, so the
	// workers count them as already mined without reading the store.
	_, err = sqlStore.RawDB().Exec(
		"INSERT INTO block_ids (transaction_id, block_id, block_height, subtree_idx) SELECT id, ?, 1, 0 FROM transactions WHERE id > ?",
		minedBlockID, lastConflictingID,
	)
	require.NoError(t, err)

	runUnminedReloadWithInputValidation(t, items, minedBlockID)

	loaded := items.blockAssembler.subtreeProcessor.GetTransactionHashes(ctx)

	for _, h := range conflicting {
		require.False(t, containsHash(loaded, h), "%s spends an output owned by another tx and must not be loaded", h.String())

		meta, getErr := items.utxoStore.Get(ctx, &h, utxofields.Conflicting)
		require.NoError(t, getErr)
		require.True(t, meta.Conflicting, "%s must be marked conflicting by the reload", h.String())
	}
}

// TestLoadUnminedTransactions_DeferredConflictMarkDropsDescendants covers the
// cost of deferring the marks. A descendant of a conflicting transaction passes
// input validation on its own (its parent exists and its spend is recorded), and
// with the marks written after iteration it no longer reads as conflicting while
// it is validated. The reload must still leave it out, because the cascade from
// its parent's mark covers it.
func TestLoadUnminedTransactions_DeferredConflictMarkDropsDescendants(t *testing.T) {
	initPrometheusMetrics()

	items := setupBlockAssemblyTest(t)
	require.NotNil(t, items)

	items.blockAssembler.settings.BlockAssembly.OnRestartValidateParentChain = false
	items.blockAssembler.settings.BlockAssembly.UnminedTxDiskSortEnabled = false

	ctx := t.Context()
	require.NoError(t, items.utxoStore.SetBlockHeight(5))

	grandparent := bt.NewTx()
	in := &bt.Input{
		PreviousTxOutIndex: 0,
		PreviousTxSatoshis: 5000,
		SequenceNumber:     0xFFFFFFFF,
		UnlockingScript:    bscript.NewFromBytes([]byte{}),
	}
	_ = in.PreviousTxIDAdd(&chainhash.Hash{7, 7, 7})
	grandparent.Inputs = []*bt.Input{in}
	grandparent.Outputs = []*bt.Output{{Satoshis: 1000, LockingScript: reloadTestLockingScript}}

	_, _, err := items.utxoStore.SpendAndCreate(ctx, grandparent, 1, utxoStore.WithCreateOnly())
	require.NoError(t, err)

	grandparentHash := *grandparent.TxIDChainHash()
	require.NoError(t, items.utxoStore.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{grandparentHash}, true))

	// root spends grandparent:0, which records a different spender: conflicting.
	root := createReloadTestTx(t, items, spendingTx(t, grandparentHash, 0))
	setSpendingData(t, items, grandparentHash, 0, chainhash.HashH([]byte("deferred-mark-winner")))

	// descendant spends root:0 and root:0 records it as the spender, so its own
	// input check passes; only root's cascade makes it conflicting.
	descendant := createReloadTestTx(t, items, spendingTx(t, root, 0))
	setSpendingData(t, items, root, 0, descendant)

	runUnminedReloadWithInputValidation(t, items)

	loaded := items.blockAssembler.subtreeProcessor.GetTransactionHashes(ctx)

	for _, h := range []chainhash.Hash{root, descendant} {
		require.False(t, containsHash(loaded, h), "%s is conflicting and must not be loaded", h.String())

		meta, getErr := items.utxoStore.Get(ctx, &h, utxofields.Conflicting)
		require.NoError(t, getErr)
		require.True(t, meta.Conflicting, "%s must be marked conflicting by the reload", h.String())
	}

	_, dropped := items.blockAssembler.unminedDropHashes[descendant]
	require.True(t, dropped, "the descendant must be handed to the post-load queue drain")
}
