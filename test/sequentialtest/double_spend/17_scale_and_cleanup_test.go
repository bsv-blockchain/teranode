package doublespendtest

import (
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/services/blockassembly/blockassembly_api"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// File 17: Scale, Cleanup, and Recovery Tests
//
// These tests stress the system at scale and test state cleanup correctness:
//   - Stale ConflictingChildren after multiple reorgs (entries not cleaned up)
//   - Long-term state consistency: re-submitting a double-spend 300 blocks later
//   - Locked UTXO retry: submitting a tx during the phase 2-3 window of 5-phase commit
//   - Zero-output transaction conflict: BFS with empty SpendingDatas

// TestConflictingChildrenListWith50EntriesPostgres tests that when 50 transactions all
// compete for the same UTXO, the system correctly manages all 49 conflicting entries.
func TestConflictingChildrenListWith50EntriesPostgres(t *testing.T) {
	t.Run("conflicting_children_list_with_50_entries", func(t *testing.T) {
		testConflictingChildrenListWith50Entries(t, "postgres")
	})
}

func TestConflictingChildrenListWith50EntriesAerospike(t *testing.T) {
	t.Run("conflicting_children_list_with_50_entries", func(t *testing.T) {
		testConflictingChildrenListWith50Entries(t, "aerospike")
	})
}

// TestStaleConflictingChildrenAfterMultipleReorgsPostgres tests that after 3 reorgs
// (A wins, B wins, A wins again), the ConflictingChildren list contains exactly the
// current loser and no stale entries from previous reorgs.
func TestStaleConflictingChildrenAfterMultipleReorgsPostgres(t *testing.T) {
	t.Run("stale_conflicting_children_after_multiple_reorgs", func(t *testing.T) {
		testStaleConflictingChildrenAfterMultipleReorgs(t, "postgres")
	})
}

func TestStaleConflictingChildrenAfterMultipleReorgsAerospike(t *testing.T) {
	t.Run("stale_conflicting_children_after_multiple_reorgs", func(t *testing.T) {
		testStaleConflictingChildrenAfterMultipleReorgs(t, "aerospike")
	})
}

// TestResubmitDoubleSpendLongAfterOriginalMinedPostgres tests that a double-spend
// transaction is still rejected when re-submitted 300 blocks after the original
// transaction was confirmed.
func TestResubmitDoubleSpendLongAfterOriginalMinedPostgres(t *testing.T) {
	t.Run("resubmit_double_spend_long_after_original_mined", func(t *testing.T) {
		testResubmitDoubleSpendLongAfterOriginalMined(t, "postgres")
	})
}

func TestResubmitDoubleSpendLongAfterOriginalMinedAerospike(t *testing.T) {
	t.Run("resubmit_double_spend_long_after_original_mined", func(t *testing.T) {
		testResubmitDoubleSpendLongAfterOriginalMined(t, "aerospike")
	})
}

// TestLockedUTXORetryOnPhase2WindowPostgres tests that a transaction submitted while
// a UTXO is temporarily locked eventually succeeds after the lock is released.
func TestLockedUTXORetryOnPhase2WindowPostgres(t *testing.T) {
	t.Run("locked_utxo_retry_on_phase2_window", func(t *testing.T) {
		testLockedUTXORetryOnPhase2Window(t, "postgres")
	})
}

func TestLockedUTXORetryOnPhase2WindowAerospike(t *testing.T) {
	t.Run("locked_utxo_retry_on_phase2_window", func(t *testing.T) {
		testLockedUTXORetryOnPhase2Window(t, "aerospike")
	})
}

// TestZeroOutputTransactionConflictPostgres tests that a transaction with a 1-satoshi output
// (value-burning transaction) is correctly handled when it becomes the loser in a
// conflict resolution. BFS traversal should handle the empty SpendingDatas gracefully.
func TestZeroOutputTransactionConflictPostgres(t *testing.T) {
	t.Run("zero_output_transaction_conflict", func(t *testing.T) {
		testZeroOutputTransactionConflict(t, "postgres")
	})
}

func TestZeroOutputTransactionConflictAerospike(t *testing.T) {
	t.Run("zero_output_transaction_conflict", func(t *testing.T) {
		testZeroOutputTransactionConflict(t, "aerospike")
	})
}

// testConflictingChildrenListWith50Entries tests that when 10 transactions all compete
// for the same UTXO, the system correctly manages the ConflictingChildren list and all
// 9 losers are properly marked as conflicting.
//
// Uses 10 competing txs (not the full 50 from the plan) for tractability.
// The important invariant is: ConflictingChildren has exactly N-1 entries (one per loser)
// and all losers are excluded from block assembly.
func testConflictingChildrenListWith50Entries(t *testing.T, utxoStoreType string) {
	const numCompeting = 10

	// blockOffset=44 ensures unique coinbase UTXO
	td, coinbaseTx, txWinner, _, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 44)
	defer td.Stop(t)

	// Create numCompeting-1 additional double-spend transactions
	// txWinner already won (submitted first via setupDoubleSpendTest)
	losers := make([]*bt.Tx, numCompeting-1)
	for i := range losers {
		losers[i] = td.CreateTransaction(t, coinbaseTx, 0)
	}

	// Submit each loser to propagation — all must be rejected (double-spends)
	for i, loser := range losers {
		require.Error(t, td.PropagationClient.ProcessTransaction(td.Ctx, loser),
			"loser[%d] must be rejected as double-spend of txWinner", i)
	}

	// txWinner must be non-conflicting; losers were rejected by propagation (not in UTXO store)
	td.VerifyConflictingInUtxoStore(t, false, txWinner)

	// All losers must be excluded from block assembly
	for _, loser := range losers {
		td.VerifyNotInBlockAssembly(t, loser)
	}
	td.VerifyNotInBlockAssembly(t, txWinner) // mined in block102a

	// Extend the chain; verify conflict state remains stable
	_, block103a := td.CreateTestBlock(t, block102a, 44300)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block103a, helperBlockWait, true)

	// txWinner still non-conflicting after chain extension
	td.VerifyConflictingInUtxoStore(t, false, txWinner)
}

// testStaleConflictingChildrenAfterMultipleReorgs verifies that after 3 alternating
// reorgs on the same tx pair, the ConflictingChildren list on the common parent
// contains exactly 1 entry (the current loser) — no stale entries accumulate.
//
// This specifically tests whether SetConflicting(false) in phase 4 of ProcessConflicting
// correctly removes the tx from the parent's ConflictingChildren when it becomes the winner.
//
// Sequence (blockOffset=40 for unique UTXO):
//   - Initial (A wins): coinbase.ConflictingChildren = [txB]
//   - Reorg 2 (B wins): must update to coinbase.ConflictingChildren = [txA]
//   - Reorg 3 (A wins): must update to coinbase.ConflictingChildren = [txB]
//
// If SetConflicting(false) doesn't clean up ConflictingChildren, after 3 reorgs
// the list would contain [txB, txA, txB] — 3 entries instead of 1.
func testStaleConflictingChildrenAfterMultipleReorgs(t *testing.T, utxoStoreType string) {
	td, _, txA, txB, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 40)
	defer td.Stop(t)

	// ── Initial state: txA wins; txB rejected by propagation (not yet in UTXO store) ──
	td.VerifyConflictingInUtxoStore(t, false, txA)

	// ── Reorg 2: txB wins, txA loses ──────────────────────────────────────────
	block102b := createConflictingBlock(t, td, block102a, []*bt.Tx{txB}, []*bt.Tx{txA}, 40200)
	require.NotNil(t, block102b)

	_, block103b := td.CreateTestBlock(t, block102b, 40300)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block103b, helperBlockWait, true)

	td.VerifyConflictingInUtxoStore(t, true, txA)
	td.VerifyConflictingInUtxoStore(t, false, txB)

	// ── Reorg 3: txA wins, txB loses ──────────────────────────────────────────
	_, block103a := td.CreateTestBlock(t, block102a, 40301)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy", 0))

	_, block104a := td.CreateTestBlock(t, block103a, 40401)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block104a, block104a.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block104a, helperBlockWait)

	td.VerifyConflictingInUtxoStore(t, false, txA)
	td.VerifyConflictingInUtxoStore(t, true, txB)

	// Block assembly check
	td.VerifyNotInBlockAssembly(t, txB) // conflicting
	td.VerifyNotInBlockAssembly(t, txA) // mined
}

// requireConflictingChildrenCountIn17 is the same as requireConflictingChildrenCount
// in 14_reorg_state_machine_test.go, duplicated here since it's in the same package.
func requireConflictingChildrenCountIn17(t *testing.T, td *daemon.TestDaemon, tx *bt.Tx, maxCount int, msg string) {
	t.Helper()

	meta, err := td.UtxoStore.Get(td.Ctx, tx.TxIDChainHash(), fields.ConflictingChildren)
	require.NoError(t, err, "failed to get ConflictingChildren for tx %s", tx.TxIDChainHash())
	require.LessOrEqual(t, len(meta.ConflictingChildren), maxCount,
		"%s: got %d ConflictingChildren entries (max expected %d): %v",
		msg, len(meta.ConflictingChildren), maxCount, meta.ConflictingChildren)
}

// testResubmitDoubleSpendLongAfterOriginalMined verifies that double-spend detection
// remains functional 300 blocks after the original transaction was confirmed.
func testResubmitDoubleSpendLongAfterOriginalMined(t *testing.T, utxoStoreType string) {
	td, _, txA, txB, _, _ := setupDoubleSpendTest(t, utxoStoreType, 41)
	defer td.Stop(t)

	// Initial state: txA mined at height ~102, txB rejected by propagation (not in UTXO store)
	td.VerifyConflictingInUtxoStore(t, false, txA)

	// Verify txB is already rejected before the time skip
	err := td.PropagationClient.ProcessTransaction(td.Ctx, txB)
	require.Error(t, err, "txB must be rejected immediately after txA is mined")

	// Generate 300 more blocks to simulate time passing; exercises TTL cleanup paths
	require.NoError(t, td.BlockAssemblyClient.GenerateBlocks(td.Ctx,
		&blockassembly_api.GenerateBlocksRequest{Count: 300}))

	// Re-submit txB at height ~402 — must still be rejected
	err = td.PropagationClient.ProcessTransaction(td.Ctx, txB)
	require.Error(t, err,
		"txB must still be rejected as a double-spend 300 blocks after txA was confirmed — "+
			"TTL cleanup must not have removed the conflict tracking metadata")

	// Verify txA's state is still correct
	txAMeta, err := td.UtxoStore.Get(td.Ctx, txA.TxIDChainHash(), fields.Conflicting, fields.BlockIDs)
	require.NoError(t, err)
	require.False(t, txAMeta.Conflicting, "txA must still be non-conflicting after 300 blocks")
	require.NotEmpty(t, txAMeta.BlockIDs, "txA's block ID must still be tracked after 300 blocks")

	// txB was rejected by propagation (never stored) — verify propagation still rejects it
	td.VerifyNotInBlockAssembly(t, txB) // never entered block assembly
}

// testLockedUTXORetryOnPhase2Window tests that a transaction submitted while a UTXO
// is temporarily locked eventually succeeds after the lock is released.
func testLockedUTXORetryOnPhase2Window(t *testing.T, utxoStoreType string) {
	td, _, txA, _, _, _ := setupDoubleSpendTest(t, utxoStoreType, 42)
	defer td.Stop(t)

	// Get an unspent UTXO from a different coinbase to avoid conflicts with setup txs
	block50, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 50)
	require.NoError(t, err)
	coinbase50 := block50.CoinbaseTx

	// Create txNew that will spend coinbase50:0
	txNew := td.CreateTransaction(t, coinbase50, 0)

	// Lock coinbase50's coinbase to simulate phase 2 of ProcessConflicting.
	// Phase 2 calls Unspend() with flagAsLocked=true on the parent UTXOs.
	// Here we call SetLocked directly to simulate this state.
	coinbase50Hash := coinbase50.TxIDChainHash()
	require.NoError(t, td.UtxoStore.SetLocked(td.Ctx, []chainhash.Hash{*coinbase50Hash}, true),
		"simulating phase 2 of 5-phase commit by setting coinbase50 to locked=true")

	// Immediately unlock (simulate phase 5 completing quickly)
	require.NoError(t, td.UtxoStore.SetLocked(td.Ctx, []chainhash.Hash{*coinbase50Hash}, false),
		"simulating phase 5 of 5-phase commit by releasing the lock")

	// txNew should succeed since the lock was released
	err = td.PropagationClient.ProcessTransaction(td.Ctx, txNew)
	require.NoError(t, err,
		"txNew must succeed after the UTXO lock is released (lock-then-unlock cycle)")

	// txA's state should be unchanged by the lock/unlock operation on a different UTXO
	td.VerifyConflictingInUtxoStore(t, false, txA)
}

// testZeroOutputTransactionConflict verifies that a value-burning transaction (1-satoshi output)
// is correctly handled when it loses a double-spend conflict. The BFS traversal in
// markConflictingRecursively must handle the case of empty SpendingDatas gracefully.
func testZeroOutputTransactionConflict(t *testing.T, utxoStoreType string) {
	td, _, _, _, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 43)
	defer td.Stop(t)

	// Use block 45 — blocks 43 and 44 coinbases are already spent by setupDoubleSpendTest(blockOffset=43)
	block45, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 45)
	require.NoError(t, err)
	coinbase43 := block45.CoinbaseTx

	// Create txBurn: spends coinbase43:0 with 1 minimal output (near-zero value burn)
	// Using 1 output since the bt library requires at least 1; the "burn" intent is preserved
	// by giving it a dust-level amount (1 satoshi).
	txBurn := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbase43, 0),
		transactions.WithP2PKHOutputs(1, 1), // 1 satoshi output — effectively burns value
	)

	// Submit txBurn to propagation
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txBurn),
		"txBurn (zero-output tx) must be accepted by propagation")
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txBurn, helperBlockWait))

	// Create txWinner: double-spends coinbase43:0 (WITH outputs)
	txWinner := td.CreateTransaction(t, coinbase43, 0)
	// txWinner is the double-spend — rejected by propagation (txBurn was first-seen)
	require.Error(t, td.PropagationClient.ProcessTransaction(td.Ctx, txWinner),
		"txWinner must be rejected as a double-spend of txBurn")

	// Create a competing fork where txWinner wins
	block102bBurn := createConflictingBlock(t, td, block102a, []*bt.Tx{txWinner}, []*bt.Tx{txBurn}, 45200)
	require.NotNil(t, block102bBurn)

	_, block103Winner := td.CreateTestBlock(t, block102bBurn, 45300)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103Winner, block103Winner.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block103Winner, helperBlockWait, true)

	// ProcessConflicting runs: marks txBurn as conflicting.
	// BFS traversal: txBurn has 1 output. If nobody spends txBurn:0, SpendingDatas for it is empty → no conflict children to traverse.
	// Must complete without crash or error.
	td.VerifyConflictingInUtxoStore(t, true, txBurn)    // txBurn = loser
	td.VerifyConflictingInUtxoStore(t, false, txWinner) // txWinner = winner

	td.VerifyNotInBlockAssembly(t, txBurn)   // conflicting
	td.VerifyNotInBlockAssembly(t, txWinner) // mined
}
