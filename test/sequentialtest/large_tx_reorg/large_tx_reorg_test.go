package large_tx_reorg

import (
	"testing"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// TestLargeTxReorgAerospike runs all large transaction reorg tests with Aerospike UTXO store
func TestLargeTxReorgAerospike(t *testing.T) {
	t.Run("1: simple large transaction reorg", func(t *testing.T) {
		testSimpleLargeTransactionReorg(t, "aerospike")
	})

	t.Run("2: partial spend large transaction reorg", func(t *testing.T) {
		testPartialSpendLargeTransactionReorg(t, "aerospike")
	})

	t.Run("3: multiple large transactions reorg", func(t *testing.T) {
		testMultipleLargeTransactionsReorg(t, "aerospike")
	})

	t.Run("4: large transaction chain dependency", func(t *testing.T) {
		testLargeTransactionChainDependency(t, "aerospike")
	})

	t.Run("5: large transaction double spend", func(t *testing.T) {
		testLargeTransactionDoubleSpend(t, "aerospike")
	})

	t.Run("6: multiple reorg cycles stress test", func(t *testing.T) {
		testMultipleReorgCycles(t, "aerospike")
	})
}

// TestLargeTxReorgPostgres runs all large transaction reorg tests with Postgres UTXO store
// Note: Postgres doesn't use spentExtraRecs/totalExtraRecs counters, but we still
// test the reorg behavior to ensure consistency across UTXO store implementations
func TestLargeTxReorgPostgres(t *testing.T) {
	t.Run("1: simple large transaction reorg", func(t *testing.T) {
		testSimpleLargeTransactionReorg(t, "postgres")
	})

	t.Run("2: partial spend large transaction reorg", func(t *testing.T) {
		testPartialSpendLargeTransactionReorg(t, "postgres")
	})

	t.Run("3: multiple large transactions reorg", func(t *testing.T) {
		testMultipleLargeTransactionsReorg(t, "postgres")
	})

	t.Run("4: large transaction chain dependency", func(t *testing.T) {
		testLargeTransactionChainDependency(t, "postgres")
	})

	t.Run("5: large transaction double spend", func(t *testing.T) {
		testLargeTransactionDoubleSpend(t, "postgres")
	})

	t.Run("6: multiple reorg cycles stress test", func(t *testing.T) {
		testMultipleReorgCycles(t, "postgres")
	})
}

// testSimpleLargeTransactionReorg reproduces the exact production bug using CONFLICTING transactions:
// A large transaction with many outputs spanning multiple Aerospike records has its outputs
// spent by two CONFLICTING transactions (double-spend) in competing forks.
//
// Bug scenario:
//  1. Mine largeTx in common block
//  2. Fork A: spendingTxA spends all outputs → spentExtraRecs = totalExtraRecs
//  3. Fork B wins: spendingTxB spends same outputs (conflicts with A) → ProcessConflicting() → Unspend(spendingTxA)
//     Expected: spentExtraRecs = totalExtraRecs (B's spends replace A's spends)
//     BUG: If Lua doesn't signal NOTALLSPENT during unspend, spentExtraRecs stays inflated
//  4. Fork A wins again → ProcessConflicting() → Unspend(spendingTxB) + Spend(spendingTxA)
//     Expected: spentExtraRecs = totalExtraRecs
//     BUG: Without proper signals, spentExtraRecs could exceed totalExtraRecs → PANIC
//
// This tests the actual code path through ProcessConflicting() and Unspend() that the production bug affected.
func testSimpleLargeTransactionReorg(t *testing.T, utxoStoreType string) {
	td, block3 := setupLargeTransactionTest(t, utxoStoreType)
	defer func() {
		td.Stop(t)
	}()

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	// Create large transaction with 10 outputs
	// With utxoBatchSize=2, this creates:
	// - 1 main record (outputs 0-1)
	// - 4 extra records (outputs 2-3, 4-5, 6-7, 8-9)
	// Therefore: totalExtraRecs = 4
	largeTx, err := td.CreateParentTransactionWithNOutputs(t, block1.CoinbaseTx, largeOutputCount)
	require.NoError(t, err)
	require.Equal(t, largeOutputCount, len(largeTx.Outputs), "Transaction should have %d outputs", largeOutputCount)

	expectedTotalExtraRecs := calculateExpectedTotalExtraRecs(largeOutputCount, lowUtxoBatchSize)
	expectedSpent := calculateExpectedSpentExtraRecs(largeOutputCount, lowUtxoBatchSize)
	t.Logf("Created largeTx with %d outputs, expected totalExtraRecs: %d", largeOutputCount, expectedTotalExtraRecs)

	// STEP 1: Mine largeTx in common block (both forks will build on this)
	_, block4 := td.CreateTestBlock(t, block3, 4000, largeTx)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4, "legacy", false), "Failed to process block4")
	td.WaitForBlock(t, block4, blockWait)
	td.WaitForBlockBeingMined(t, block4)

	// Verify largeTx is mined and on longest chain
	td.VerifyNotInBlockAssembly(t, largeTx)
	td.VerifyOnLongestChainInUtxoStore(t, largeTx)

	// Verify initial counters: all records created, none spent yet
	verifySpentExtraRecs(t, td, largeTx, 0, expectedTotalExtraRecs)
	t.Logf("✓ STEP 1: largeTx mined in common ancestor - spentExtraRecs=0, totalExtraRecs=%d", expectedTotalExtraRecs)

	// STEP 2: Create TWO CONFLICTING transactions that spend the SAME outputs
	// spendingTxA: Spends all 10 outputs (Fork A version)
	spendOptionsA := make([]transactions.TxOption, 0, largeOutputCount+1)
	for i := 0; i < largeOutputCount; i++ {
		spendOptionsA = append(spendOptionsA, transactions.WithInput(largeTx, uint32(i)))
	}
	spendOptionsA = append(spendOptionsA, transactions.WithP2PKHOutputs(1, 100000)) // Different output amount

	spendingTxA := td.CreateTransactionWithOptions(t, spendOptionsA...)

	// spendingTxB: Spends the SAME 10 outputs (Fork B version - CONFLICTS with TxA)
	spendOptionsB := make([]transactions.TxOption, 0, largeOutputCount+1)
	for i := 0; i < largeOutputCount; i++ {
		spendOptionsB = append(spendOptionsB, transactions.WithInput(largeTx, uint32(i))) // SAME inputs!
	}
	spendOptionsB = append(spendOptionsB, transactions.WithP2PKHOutputs(1, 150000)) // Different output amount

	spendingTxB := td.CreateTransactionWithOptions(t, spendOptionsB...)

	t.Logf("Created conflicting transactions:")
	t.Logf("  - spendingTxA: %s (output: 100000 sats)", spendingTxA.TxID())
	t.Logf("  - spendingTxB: %s (output: 150000 sats)", spendingTxB.TxID())
	t.Logf("  Both spend ALL outputs of largeTx → DOUBLE-SPEND conflict")

	// STEP 3: Fork A - Mine spendingTxA
	_, block5a := td.CreateTestBlock(t, block4, 5001, spendingTxA)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5a, "legacy", false), "Failed to process block5a")
	td.WaitForBlock(t, block5a, blockWait)
	td.WaitForBlockBeingMined(t, block5a)

	// Verify Fork A state
	td.VerifyNotInBlockAssembly(t, spendingTxA)
	td.VerifyOnLongestChainInUtxoStore(t, spendingTxA)
	bestHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(5), bestHeight, "Fork A should be longest at height 5")

	// All outputs spent by spendingTxA → all extra records fully spent
	verifySpentExtraRecs(t, td, largeTx, expectedSpent, expectedTotalExtraRecs)
	t.Logf("✓ STEP 3: Fork A wins - spendingTxA mined, spentExtraRecs=%d", expectedSpent)

	// STEP 4: Fork B - Mine spendingTxB (CONFLICTS with spendingTxA)
	// Build longer fork to trigger reorg and ProcessConflicting()
	_, block5b := td.CreateTestBlock(t, block4, 5002, spendingTxB)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5b, "legacy", false), "Failed to process block5b")
	td.WaitForBlockBeingMined(t, block5b)

	_, block6b := td.CreateTestBlock(t, block5b, 6002) // Make Fork B longer
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block6b, "legacy", false, false), "Failed to process block6b")
	td.WaitForBlock(t, block6b, blockWait)
	td.WaitForBlockBeingMined(t, block6b)

	// Verify Fork B state
	bestHeight, _, err = td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(6), bestHeight, "Fork B should be longest at height 6")

	// spendingTxA is now conflicting (not in block assembly), spendingTxB mined
	// NOTE: Conflicting transactions are not returned to block assembly - they're marked as conflicting
	td.VerifyNotInBlockAssembly(t, spendingTxB)
	td.VerifyNotOnLongestChainInUtxoStore(t, spendingTxA)
	td.VerifyOnLongestChainInUtxoStore(t, spendingTxB)

	// CRITICAL BUG CHECK: ProcessConflicting() called Unspend(spendingTxA) then Spend(spendingTxB)
	// Expected: spentExtraRecs still = totalExtraRecs (B's spends replace A's spends)
	// BUG: If Lua doesn't signal NOTALLSPENT, counters won't be adjusted properly
	verifySpentExtraRecs(t, td, largeTx, expectedSpent, expectedTotalExtraRecs)
	t.Logf("✓ STEP 4: Fork B wins - ProcessConflicting() called, spentExtraRecs=%d (CRITICAL: Unspend+Spend worked)", expectedSpent)

	// STEP 5: Fork A wins again - Another reorg back to spendingTxA
	_, block6a := td.CreateTestBlock(t, block5a, 6001)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block6a, "legacy", false, false), "Failed to process block6a")
	td.WaitForBlockBeingMined(t, block6a)

	_, block7a := td.CreateTestBlock(t, block6a, 7001) // Make Fork A longer than Fork B
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block7a, "legacy", false, false), "Failed to process block7a")
	td.WaitForBlock(t, block7a, blockWait)
	td.WaitForBlockBeingMined(t, block7a)

	// Verify Fork A state restored
	bestHeight, _, err = td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(7), bestHeight, "Fork A should be longest at height 7")

	// spendingTxA back on longest chain, spendingTxB is now conflicting
	// NOTE: Conflicting transactions are not returned to block assembly
	td.VerifyNotInBlockAssembly(t, spendingTxA)
	td.VerifyOnLongestChainInUtxoStore(t, spendingTxA)
	td.VerifyNotOnLongestChainInUtxoStore(t, spendingTxB)

	// CRITICAL BUG CHECK: ProcessConflicting() called Unspend(spendingTxB) then Spend(spendingTxA)
	// Expected: spentExtraRecs = totalExtraRecs (restored to Fork A state)
	// BUG WITHOUT FIX: spentExtraRecs could exceed totalExtraRecs → PANIC!
	verifySpentExtraRecs(t, td, largeTx, expectedSpent, expectedTotalExtraRecs)
	t.Logf("✓ STEP 5: Fork A wins again - ProcessConflicting() called again, spentExtraRecs=%d", expectedSpent)

	t.Logf("✓✓✓ TEST PASSED: spentExtraRecs correctly maintained through conflicting transaction reorgs")
	t.Logf("    Verified: Unspend() properly decrements counters via NOTALLSPENT signal")
	t.Logf("    Verified: Counter never exceeded totalExtraRecs=%d", expectedTotalExtraRecs)
}

// testPartialSpendLargeTransactionReorg tests PARTIAL spend conflicts where two transactions
// spend OVERLAPPING but different subsets of outputs from the same large transaction.
//
// This tests whether spentExtraRecs correctly handles partial unspend/respend during
// ProcessConflicting() when only some outputs are involved in the conflict.
func testPartialSpendLargeTransactionReorg(t *testing.T, utxoStoreType string) {
	td, block3 := setupLargeTransactionTest(t, utxoStoreType)
	defer func() {
		td.Stop(t)
	}()

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	// Create large transaction with 10 outputs
	// Records: [0-1], [2-3], [4-5], [6-7], [8-9]
	largeTx, err := td.CreateParentTransactionWithNOutputs(t, block1.CoinbaseTx, largeOutputCount)
	require.NoError(t, err)

	expectedTotalExtraRecs := calculateExpectedTotalExtraRecs(largeOutputCount, lowUtxoBatchSize)
	t.Logf("Created largeTx with %d outputs, totalExtraRecs: %d", largeOutputCount, expectedTotalExtraRecs)

	// Mine largeTx in common block
	_, block4 := td.CreateTestBlock(t, block3, 4000, largeTx)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4, "legacy", false), "Failed to process block4")
	td.WaitForBlock(t, block4, blockWait)
	td.WaitForBlockBeingMined(t, block4)

	verifySpentExtraRecs(t, td, largeTx, 0, expectedTotalExtraRecs)
	t.Logf("✓ STEP 1: largeTx mined - spentExtraRecs=0, totalExtraRecs=%d", expectedTotalExtraRecs)

	// Create TWO CONFLICTING transactions with OVERLAPPING partial spends
	// spendingTxA: Spends outputs 0-5 (6 outputs)
	// - Main record [0-1]: 2 outputs
	// - Extra record [2-3]: 2 outputs (fully spent)
	// - Extra record [4-5]: 2 outputs (fully spent)
	// Result: 2 extra records fully spent → spentExtraRecs = 2
	partialOptionsA := make([]transactions.TxOption, 0, 7)
	for i := 0; i < 6; i++ {
		partialOptionsA = append(partialOptionsA, transactions.WithInput(largeTx, uint32(i)))
	}
	partialOptionsA = append(partialOptionsA, transactions.WithP2PKHOutputs(1, 100000))
	spendingTxA := td.CreateTransactionWithOptions(t, partialOptionsA...)

	// spendingTxB: Spends outputs 3-9 (7 outputs) - CONFLICTS on outputs 3,4,5
	// - Main record [0-1]: not spent
	// - Extra record [2-3]: 1 output (partially spent, not counted)
	// - Extra record [4-5]: 2 outputs (fully spent)
	// - Extra record [6-7]: 2 outputs (fully spent)
	// - Extra record [8-9]: 2 outputs (fully spent)
	// Result: 3 extra records fully spent → spentExtraRecs = 3
	partialOptionsB := make([]transactions.TxOption, 0, 8)
	for i := 3; i < 10; i++ {
		partialOptionsB = append(partialOptionsB, transactions.WithInput(largeTx, uint32(i)))
	}
	partialOptionsB = append(partialOptionsB, transactions.WithP2PKHOutputs(1, 150000))
	spendingTxB := td.CreateTransactionWithOptions(t, partialOptionsB...)

	t.Logf("Created PARTIAL OVERLAP conflicting transactions:")
	t.Logf("  - spendingTxA: spends outputs 0-5 → spentExtraRecs=2")
	t.Logf("  - spendingTxB: spends outputs 3-9 → spentExtraRecs=3")
	t.Logf("  Conflict on outputs 3,4,5")

	// Fork A: Mine spendingTxA
	_, block5a := td.CreateTestBlock(t, block4, 5001, spendingTxA)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5a, "legacy", false), "Failed to process block5a")
	td.WaitForBlock(t, block5a, blockWait)
	td.WaitForBlockBeingMined(t, block5a)

	expectedSpentA := calculateExpectedSpentExtraRecs(6, lowUtxoBatchSize)
	verifySpentExtraRecs(t, td, largeTx, expectedSpentA, expectedTotalExtraRecs)
	t.Logf("✓ STEP 2: Fork A wins - spendingTxA mined, spentExtraRecs=%d (outputs 0-5 spent)", expectedSpentA)

	// Fork B: Mine spendingTxB (CONFLICTS with spendingTxA on outputs 3,4,5)
	_, block5b := td.CreateTestBlock(t, block4, 5002, spendingTxB)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5b, "legacy", false), "Failed to process block5b")
	td.WaitForBlockBeingMined(t, block5b)

	_, block6b := td.CreateTestBlock(t, block5b, 6002)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block6b, "legacy", false, false), "Failed to process block6b")
	td.WaitForBlock(t, block6b, blockWait)
	td.WaitForBlockBeingMined(t, block6b)

	// Verify Fork B state: spendingTxB spends 7 outputs (3-9) → spentExtraRecs = 3
	// ProcessConflicting() unspent outputs 0-5 (from TxA) and spent outputs 3-9 (from TxB)
	expectedSpentB := 3 // Extra records [4-5], [6-7], [8-9] fully spent
	verifySpentExtraRecs(t, td, largeTx, expectedSpentB, expectedTotalExtraRecs)
	bestHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(6), bestHeight)
	t.Logf("✓ STEP 3: Fork B wins - ProcessConflicting() called, spentExtraRecs=%d (outputs 3-9 spent)", expectedSpentB)

	// Fork A wins again
	_, block6a := td.CreateTestBlock(t, block5a, 6001)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block6a, "legacy", false, false), "Failed to process block6a")
	td.WaitForBlockBeingMined(t, block6a)

	_, block7a := td.CreateTestBlock(t, block6a, 7001)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block7a, "legacy", false, false), "Failed to process block7a")
	td.WaitForBlock(t, block7a, blockWait)
	td.WaitForBlockBeingMined(t, block7a)

	// Back to Fork A: spentExtraRecs should be 2 (outputs 0-5 spent)
	verifySpentExtraRecs(t, td, largeTx, expectedSpentA, expectedTotalExtraRecs)
	bestHeight, _, err = td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(7), bestHeight)
	t.Logf("✓ STEP 4: Fork A wins again - spentExtraRecs=%d (correctly restored to outputs 0-5 spent)", expectedSpentA)

	t.Logf("✓✓✓ TEST PASSED: Partial spend conflict handled correctly")
	t.Logf("    Fork A: %d records spent, Fork B: %d records spent", expectedSpentA, expectedSpentB)
}

// testMultipleLargeTransactionsReorg tests conflicts involving a large transaction that
// creates multiple spending transactions, where those spenders have conflicts.
//
// This tests that counters for the large parent transaction are correctly maintained
// when its children have conflicting spends in different forks.
func testMultipleLargeTransactionsReorg(t *testing.T, utxoStoreType string) {
	td, block3 := setupLargeTransactionTest(t, utxoStoreType)
	defer func() {
		td.Stop(t)
	}()

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	// Create large parent transaction with 10 outputs
	largeTx, err := td.CreateParentTransactionWithNOutputs(t, block1.CoinbaseTx, largeOutputCount)
	require.NoError(t, err)

	expectedTotalExtraRecs := calculateExpectedTotalExtraRecs(largeOutputCount, lowUtxoBatchSize)
	t.Logf("Created largeTx with %d outputs, totalExtraRecs: %d", largeOutputCount, expectedTotalExtraRecs)

	// Mine largeTx in common block
	_, block4 := td.CreateTestBlock(t, block3, 4000, largeTx)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4, "legacy", false))
	td.WaitForBlock(t, block4, blockWait)
	td.WaitForBlockBeingMined(t, block4)

	verifySpentExtraRecs(t, td, largeTx, 0, expectedTotalExtraRecs)
	t.Logf("✓ STEP 1: largeTx mined - spentExtraRecs=0")

	// Create two child transactions that BOTH spend from largeTx
	// child1: Spends outputs 0-4 (5 outputs) → 2 extra records fully spent
	child1OptionsA := make([]transactions.TxOption, 0, 6)
	for i := 0; i < 5; i++ {
		child1OptionsA = append(child1OptionsA, transactions.WithInput(largeTx, uint32(i)))
	}
	child1OptionsA = append(child1OptionsA, transactions.WithP2PKHOutputs(1, 50000))
	child1A := td.CreateTransactionWithOptions(t, child1OptionsA...)

	// child2: Spends outputs 5-9 (5 outputs) → 2 extra records fully spent
	child2OptionsA := make([]transactions.TxOption, 0, 6)
	for i := 5; i < 10; i++ {
		child2OptionsA = append(child2OptionsA, transactions.WithInput(largeTx, uint32(i)))
	}
	child2OptionsA = append(child2OptionsA, transactions.WithP2PKHOutputs(1, 50000))
	child2A := td.CreateTransactionWithOptions(t, child2OptionsA...)

	// Fork A: Mine both children → all 4 extra records spent
	_, block5a := td.CreateTestBlock(t, block4, 5001, child1A, child2A)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5a, "legacy", false))
	td.WaitForBlock(t, block5a, blockWait)
	td.WaitForBlockBeingMined(t, block5a)

	expectedSpentAll := calculateExpectedSpentExtraRecs(largeOutputCount, lowUtxoBatchSize)
	verifySpentExtraRecs(t, td, largeTx, expectedSpentAll, expectedTotalExtraRecs)
	t.Logf("✓ STEP 2: Fork A - both children mined, spentExtraRecs=%d (all records spent)", expectedSpentAll)

	// Create CONFLICTING children for Fork B
	// child1B: Spends SAME outputs 0-4 as child1A (CONFLICTS!)
	child1OptionsB := make([]transactions.TxOption, 0, 6)
	for i := 0; i < 5; i++ {
		child1OptionsB = append(child1OptionsB, transactions.WithInput(largeTx, uint32(i)))
	}
	child1OptionsB = append(child1OptionsB, transactions.WithP2PKHOutputs(1, 60000)) // Different amount
	child1B := td.CreateTransactionWithOptions(t, child1OptionsB...)

	// child2B: Spends SAME outputs 5-9 as child2A (CONFLICTS!)
	child2OptionsB := make([]transactions.TxOption, 0, 6)
	for i := 5; i < 10; i++ {
		child2OptionsB = append(child2OptionsB, transactions.WithInput(largeTx, uint32(i)))
	}
	child2OptionsB = append(child2OptionsB, transactions.WithP2PKHOutputs(1, 60000)) // Different amount
	child2B := td.CreateTransactionWithOptions(t, child2OptionsB...)

	t.Logf("Created conflicting children:")
	t.Logf("  Fork A: child1A + child2A (spend all outputs)")
	t.Logf("  Fork B: child1B + child2B (spend SAME outputs, different values)")

	// Fork B: Mine conflicting children
	_, block5b := td.CreateTestBlock(t, block4, 5002, child1B, child2B)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5b, "legacy", false))
	td.WaitForBlockBeingMined(t, block5b)

	_, block6b := td.CreateTestBlock(t, block5b, 6002)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block6b, "legacy", false, false))
	td.WaitForBlock(t, block6b, blockWait)
	td.WaitForBlockBeingMined(t, block6b)

	// Fork B wins: ProcessConflicting() for both child pairs
	// child1A conflicts with child1B, child2A conflicts with child2B
	// Result: still all 4 extra records spent (by child1B and child2B)
	verifySpentExtraRecs(t, td, largeTx, expectedSpentAll, expectedTotalExtraRecs)
	bestHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(6), bestHeight)
	t.Logf("✓ STEP 3: Fork B wins - ProcessConflicting() for both children, spentExtraRecs=%d", expectedSpentAll)

	// Fork A wins back
	_, block6a := td.CreateTestBlock(t, block5a, 6001)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block6a, "legacy", false, false))
	td.WaitForBlockBeingMined(t, block6a)

	_, block7a := td.CreateTestBlock(t, block6a, 7001)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block7a, "legacy", false, false))
	td.WaitForBlock(t, block7a, blockWait)
	td.WaitForBlockBeingMined(t, block7a)

	// Verify counters restored to Fork A state
	verifySpentExtraRecs(t, td, largeTx, expectedSpentAll, expectedTotalExtraRecs)
	bestHeight, _, err = td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(7), bestHeight)
	t.Logf("✓ STEP 4: Fork A wins again - spentExtraRecs=%d (all records spent by child1A+child2A)", expectedSpentAll)

	t.Logf("✓✓✓ TEST PASSED: Multiple children with conflicts handled correctly")
	t.Logf("    Both forks spend all outputs, counters maintained through conflicts")
}

// testLargeTransactionChainDependency tests CONFLICTING parent→child chains
// where the parent is a large transaction and children in different forks conflict.
//
// This tests that when child transactions conflict, the parent's spentExtraRecs
// is correctly maintained through ProcessConflicting().
func testLargeTransactionChainDependency(t *testing.T, utxoStoreType string) {
	td, block3 := setupLargeTransactionTest(t, utxoStoreType)
	defer func() {
		td.Stop(t)
	}()

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	// Create large parent transaction
	parentTx, err := td.CreateParentTransactionWithNOutputs(t, block1.CoinbaseTx, largeOutputCount)
	require.NoError(t, err)

	expectedTotalExtraRecs := calculateExpectedTotalExtraRecs(largeOutputCount, lowUtxoBatchSize)
	t.Logf("Created parentTx with %d outputs, totalExtraRecs: %d", largeOutputCount, expectedTotalExtraRecs)

	// Mine parentTx in common block
	_, block4 := td.CreateTestBlock(t, block3, 4000, parentTx)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4, "legacy", false), "Failed to process block4")
	td.WaitForBlock(t, block4, blockWait)
	td.WaitForBlockBeingMined(t, block4)

	verifySpentExtraRecs(t, td, parentTx, 0, expectedTotalExtraRecs)
	t.Logf("✓ STEP 1: parentTx mined - spentExtraRecs=0")

	// Create Fork A chain: parentTx → childA → grandchildA
	// childA spends outputs 0-5 from parentTx → 2 extra records spent
	childAOptions := make([]transactions.TxOption, 0, 7)
	for i := 0; i < 6; i++ {
		childAOptions = append(childAOptions, transactions.WithInput(parentTx, uint32(i)))
	}
	childAOptions = append(childAOptions, transactions.WithP2PKHOutputs(1, 100000))
	childA := td.CreateTransactionWithOptions(t, childAOptions...)

	// grandchildA spends from childA's output
	grandchildA := td.CreateTransaction(t, childA, 0)

	// Mine Fork A chain
	_, block5a := td.CreateTestBlock(t, block4, 5001, childA, grandchildA)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5a, "legacy", false), "Failed to process block5a")
	td.WaitForBlock(t, block5a, blockWait)
	td.WaitForBlockBeingMined(t, block5a)

	expectedSpentA := calculateExpectedSpentExtraRecs(6, lowUtxoBatchSize)
	verifySpentExtraRecs(t, td, parentTx, expectedSpentA, expectedTotalExtraRecs)
	t.Logf("✓ STEP 2: Fork A - childA+grandchildA mined, spentExtraRecs=%d (outputs 0-5 spent)", expectedSpentA)

	// Create Fork B chain: parentTx → childB → grandchildB (CONFLICTS with Fork A)
	// childB spends SAME outputs 0-5 from parentTx (CONFLICTS with childA!)
	childBOptions := make([]transactions.TxOption, 0, 7)
	for i := 0; i < 6; i++ {
		childBOptions = append(childBOptions, transactions.WithInput(parentTx, uint32(i))) // SAME inputs!
	}
	childBOptions = append(childBOptions, transactions.WithP2PKHOutputs(1, 150000)) // Different amount
	childB := td.CreateTransactionWithOptions(t, childBOptions...)

	// grandchildB spends from childB's output (different chain than grandchildA)
	grandchildB := td.CreateTransaction(t, childB, 0)

	t.Logf("Created conflicting chains:")
	t.Logf("  Fork A: parentTx → childA → grandchildA")
	t.Logf("  Fork B: parentTx → childB → grandchildB (childB conflicts with childA)")

	// Mine Fork B chain
	_, block5b := td.CreateTestBlock(t, block4, 5002, childB, grandchildB)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5b, "legacy", false), "Failed to process block5b")
	td.WaitForBlockBeingMined(t, block5b)

	_, block6b := td.CreateTestBlock(t, block5b, 6002)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block6b, "legacy", false, false), "Failed to process block6b")
	td.WaitForBlock(t, block6b, blockWait)
	td.WaitForBlockBeingMined(t, block6b)

	// Fork B wins: childB conflicts with childA
	// ProcessConflicting() unspends childA (and invalidates grandchildA), then spends childB
	// Parent should still have same outputs spent (childB spends same outputs as childA)
	verifySpentExtraRecs(t, td, parentTx, expectedSpentA, expectedTotalExtraRecs)
	bestHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(6), bestHeight)
	t.Logf("✓ STEP 3: Fork B wins - childB replaces childA, spentExtraRecs=%d (same outputs)", expectedSpentA)

	// Fork A wins back
	_, block6a := td.CreateTestBlock(t, block5a, 6001)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block6a, "legacy", false, false), "Failed to process block6a")
	td.WaitForBlockBeingMined(t, block6a)

	_, block7a := td.CreateTestBlock(t, block6a, 7001)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block7a, "legacy", false, false), "Failed to process block7a")
	td.WaitForBlock(t, block7a, blockWait)
	td.WaitForBlockBeingMined(t, block7a)

	// Fork A restored: childA back on longest chain
	// ProcessConflicting() unspends childB, spends childA
	verifySpentExtraRecs(t, td, parentTx, expectedSpentA, expectedTotalExtraRecs)
	bestHeight, _, err = td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(7), bestHeight)
	td.VerifyNotInBlockAssembly(t, childA)
	td.VerifyNotInBlockAssembly(t, grandchildA)
	t.Logf("✓ STEP 4: Fork A wins back - childA restored, spentExtraRecs=%d", expectedSpentA)

	t.Logf("✓✓✓ TEST PASSED: Chain dependency with conflicts handled correctly")
	t.Logf("    Both chains spend same parent outputs, counters maintained")
}

// testLargeTransactionDoubleSpend tests OVERLAPPING conflicting spends where
// two transactions spend DIFFERENT but OVERLAPPING subsets of outputs.
//
// This is an important edge case: when transactions conflict on SOME outputs but
// spend different total sets, ProcessConflicting() must handle the partial overlap correctly.
func testLargeTransactionDoubleSpend(t *testing.T, utxoStoreType string) {
	td, block3 := setupLargeTransactionTest(t, utxoStoreType)
	defer func() {
		td.Stop(t)
	}()

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	// Create large parent transaction
	parentTx, err := td.CreateParentTransactionWithNOutputs(t, block1.CoinbaseTx, largeOutputCount)
	require.NoError(t, err)

	expectedTotalExtraRecs := calculateExpectedTotalExtraRecs(largeOutputCount, lowUtxoBatchSize)
	t.Logf("Created parentTx with %d outputs, totalExtraRecs: %d", largeOutputCount, expectedTotalExtraRecs)

	// Mine parentTx in common block
	_, block4 := td.CreateTestBlock(t, block3, 4000, parentTx)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4, "legacy", false), "Failed to process block4")
	td.WaitForBlock(t, block4, blockWait)
	td.WaitForBlockBeingMined(t, block4)

	verifySpentExtraRecs(t, td, parentTx, 0, expectedTotalExtraRecs)
	t.Logf("✓ STEP 1: parentTx mined - spentExtraRecs=0")

	// Fork A: Spend outputs 0-5 (spends 2 extra records)
	conflictOpts1 := make([]transactions.TxOption, 0, 7)
	for i := 0; i < 6; i++ {
		conflictOpts1 = append(conflictOpts1, transactions.WithInput(parentTx, uint32(i)))
	}
	conflictOpts1 = append(conflictOpts1, transactions.WithP2PKHOutputs(1, 100000))

	conflictTx1 := td.CreateTransactionWithOptions(t, conflictOpts1...)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, conflictTx1))

	_, block5a := td.CreateTestBlock(t, block4, 5001, conflictTx1)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5a, "legacy", false), "Failed to process block5a")
	td.WaitForBlock(t, block5a, blockWait)
	td.WaitForBlockBeingMined(t, block5a)

	expectedSpentForkA := calculateExpectedSpentExtraRecs(6, lowUtxoBatchSize)
	verifySpentExtraRecs(t, td, parentTx, expectedSpentForkA, expectedTotalExtraRecs)
	t.Logf("✓ STEP 2: Fork A - outputs 0-5 spent, spentExtraRecs=%d", expectedSpentForkA)

	// Fork B: Spend outputs 3-9 (7 outputs - CONFLICTS on 3,4,5)
	// This OVERLAPS with Fork A but spends different total set
	// Records: [2-3] partially spent (only output 3), [4-5] fully spent, [6-7] fully spent, [8-9] fully spent
	// Result: 3 extra records fully spent
	conflictOpts2 := make([]transactions.TxOption, 0, 8)
	for i := 3; i < 10; i++ {
		conflictOpts2 = append(conflictOpts2, transactions.WithInput(parentTx, uint32(i)))
	}
	conflictOpts2 = append(conflictOpts2, transactions.WithP2PKHOutputs(1, 150000)) // Different amount

	conflictTx2 := td.CreateTransactionWithOptions(t, conflictOpts2...)

	t.Logf("Created OVERLAPPING conflicts:")
	t.Logf("  Fork A: spends outputs 0-5 → spentExtraRecs=2")
	t.Logf("  Fork B: spends outputs 3-9 → spentExtraRecs=3")
	t.Logf("  Conflict on outputs 3,4,5 (PARTIAL OVERLAP)")

	_, block5b := td.CreateTestBlock(t, block4, 5002, conflictTx2)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block5b, "legacy", false), "Failed to process block5b")
	td.WaitForBlockBeingMined(t, block5b)

	_, block6b := td.CreateTestBlock(t, block5b, 6002)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block6b, "legacy", false, false), "Failed to process block6b")
	td.WaitForBlock(t, block6b, blockWait)
	td.WaitForBlockBeingMined(t, block6b)

	// Fork B wins: ProcessConflicting() unspends 0-5 (Fork A), spends 3-9 (Fork B)
	// Result: 3 extra records fully spent ([4-5], [6-7], [8-9])
	verifySpentExtraRecs(t, td, parentTx, 3, expectedTotalExtraRecs)
	bestHeight, _, err := td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(6), bestHeight)
	t.Logf("✓ STEP 3: Fork B wins - outputs 3-9 spent, spentExtraRecs=3 (OVERLAPPING conflict handled)")

	// Fork A wins back
	_, block6a := td.CreateTestBlock(t, block5a, 6001)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block6a, "legacy", false, false), "Failed to process block6a")
	td.WaitForBlockBeingMined(t, block6a)

	_, block7a := td.CreateTestBlock(t, block6a, 7001)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block7a, "legacy", false, false), "Failed to process block7a")
	td.WaitForBlock(t, block7a, blockWait)
	td.WaitForBlockBeingMined(t, block7a)

	// Fork A restored: ProcessConflicting() unspends 3-9 (Fork B), spends 0-5 (Fork A)
	// Result: 2 extra records fully spent again ([2-3], [4-5])
	verifySpentExtraRecs(t, td, parentTx, expectedSpentForkA, expectedTotalExtraRecs)
	bestHeight, _, err = td.BlockchainClient.GetBestHeightAndTime(td.Ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(7), bestHeight)
	t.Logf("✓ STEP 4: Fork A wins back - outputs 0-5 spent, spentExtraRecs=%d (correctly restored)", expectedSpentForkA)

	t.Logf("✓✓✓ TEST PASSED: Overlapping conflicts handled correctly")
	t.Logf("    Fork A: %d records, Fork B: 3 records, counters maintained", expectedSpentForkA)
}

// testMultipleReorgCycles stress-tests the counter logic with MULTIPLE REORG CYCLES
// involving CONFLICTING transactions to ensure no cumulative drift or counter inflation.
//
// This is the ultimate stress test: A→B→A→B→A→B with conflicts each time to verify
// that ProcessConflicting() and counter management remain correct across many cycles.
func testMultipleReorgCycles(t *testing.T, utxoStoreType string) {
	td, block3 := setupLargeTransactionTest(t, utxoStoreType)
	defer func() {
		td.Stop(t)
	}()

	block1, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 1)
	require.NoError(t, err)

	// Create large transaction with 20 outputs (9 extra records with batchSize=2)
	largeTx, err := td.CreateParentTransactionWithNOutputs(t, block1.CoinbaseTx, 20)
	require.NoError(t, err)

	expectedTotalExtraRecs := calculateExpectedTotalExtraRecs(20, lowUtxoBatchSize)
	expectedSpent := calculateExpectedSpentExtraRecs(20, lowUtxoBatchSize)
	t.Logf("Created largeTx with 20 outputs, totalExtraRecs: %d", expectedTotalExtraRecs)

	// Mine largeTx in common block
	_, block4 := td.CreateTestBlock(t, block3, 4000, largeTx)
	require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, block4, "legacy", false), "Failed to process block4")
	td.WaitForBlock(t, block4, blockWait)
	td.WaitForBlockBeingMined(t, block4)

	verifySpentExtraRecs(t, td, largeTx, 0, expectedTotalExtraRecs)
	t.Logf("✓ Initial state: largeTx mined, spentExtraRecs=0")

	// Create TWO CONFLICTING spending transactions (spend ALL outputs)
	// spendingTxA for Fork A
	spendOptsA := make([]transactions.TxOption, 0, 21)
	for i := 0; i < 20; i++ {
		spendOptsA = append(spendOptsA, transactions.WithInput(largeTx, uint32(i)))
	}
	spendOptsA = append(spendOptsA, transactions.WithP2PKHOutputs(1, 100000))
	spendingTxA := td.CreateTransactionWithOptions(t, spendOptsA...)

	// spendingTxB for Fork B (CONFLICTS with TxA!)
	spendOptsB := make([]transactions.TxOption, 0, 21)
	for i := 0; i < 20; i++ {
		spendOptsB = append(spendOptsB, transactions.WithInput(largeTx, uint32(i))) // SAME inputs!
	}
	spendOptsB = append(spendOptsB, transactions.WithP2PKHOutputs(1, 150000)) // Different amount
	spendingTxB := td.CreateTransactionWithOptions(t, spendOptsB...)

	t.Logf("Created conflicting spenders:")
	t.Logf("  - spendingTxA: %s (100000 sats)", spendingTxA.TxID())
	t.Logf("  - spendingTxB: %s (150000 sats)", spendingTxB.TxID())

	// Perform multiple back-and-forth reorg cycles with CONFLICTS
	// This tests: A wins (TxA) → B wins (TxB conflicts) → A wins (TxA conflicts) → ...
	numCycles := 3

	for cycle := 1; cycle <= numCycles; cycle++ {
		t.Logf("=== REORG CYCLE %d/%d ===", cycle, numCycles)

		// PHASE 1: Fork A wins (includes spendingTxA)
		baseNonce := uint32(5000 + cycle*1000)

		_, blockA1 := td.CreateTestBlock(t, block4, baseNonce, spendingTxA)
		require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, blockA1, "legacy", false), "Failed to process Fork A block1 in cycle %d", cycle)
		td.WaitForBlockBeingMined(t, blockA1)

		// Add more blocks to make Fork A longest
		blocksNeeded := cycle*2 + 1
		var lastBlockA *model.Block = blockA1
		for i := 0; i < blocksNeeded; i++ {
			_, nextBlock := td.CreateTestBlock(t, lastBlockA, baseNonce+uint32(i)+1)
			require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, nextBlock, "legacy", false, false), "Failed to process Fork A block in cycle %d", cycle)
			if i == blocksNeeded-1 {
				td.WaitForBlock(t, nextBlock, blockWait)
			}
			td.WaitForBlockBeingMined(t, nextBlock)
			lastBlockA = nextBlock
		}

		// Verify Fork A: all outputs spent by TxA
		verifySpentExtraRecs(t, td, largeTx, expectedSpent, expectedTotalExtraRecs)
		t.Logf("  ✓ Fork A wins (cycle %d): spendingTxA mined, spentExtraRecs=%d", cycle, expectedSpent)

		// PHASE 2: Fork B wins (includes spendingTxB - CONFLICTS with TxA!)
		baseNonce = uint32(6000 + cycle*1000)

		_, blockB1 := td.CreateTestBlock(t, block4, baseNonce, spendingTxB)
		require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, blockB1, "legacy", false), "Failed to process Fork B block1 in cycle %d", cycle)
		td.WaitForBlockBeingMined(t, blockB1)

		// Make Fork B taller than Fork A
		blocksNeeded = cycle*2 + 2
		var lastBlockB *model.Block = blockB1
		for i := 0; i < blocksNeeded; i++ {
			_, nextBlock := td.CreateTestBlock(t, lastBlockB, baseNonce+uint32(i)+1)
			require.NoError(t, td.BlockValidation.ValidateBlock(td.Ctx, nextBlock, "legacy", false, false), "Failed to process Fork B block in cycle %d", cycle)
			td.WaitForBlockBeingMined(t, nextBlock)
			if i == blocksNeeded-1 {
				td.WaitForBlock(t, nextBlock, blockWait)
			}
			lastBlockB = nextBlock
		}

		// Verify Fork B: ProcessConflicting() unspent TxA, spent TxB
		// All outputs still spent (by TxB now instead of TxA)
		verifySpentExtraRecs(t, td, largeTx, expectedSpent, expectedTotalExtraRecs)
		t.Logf("  ✓ Fork B wins (cycle %d): spendingTxB mined (conflicts with TxA), spentExtraRecs=%d", cycle, expectedSpent)
	}

	// Final verification: counters should still be correct after multiple cycles
	// No cumulative drift or inflation should have occurred
	verifySpentExtraRecs(t, td, largeTx, expectedSpent, expectedTotalExtraRecs)

	t.Logf("✓✓✓ STRESS TEST PASSED: %d full back-and-forth CONFLICTING reorg cycles completed", numCycles)
	t.Logf("    Tested: A(TxA)→B(TxB conflicts)→A(TxA conflicts)→B(TxB conflicts) pattern")
	t.Logf("    ProcessConflicting() called %d times, counters remain correct", numCycles*2)
	t.Logf("    Final state: spentExtraRecs=%d/%d (no cumulative counter inflation)", expectedSpent, expectedTotalExtraRecs)
}
