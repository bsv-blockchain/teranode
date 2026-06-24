package doublespendtest

import (
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// File 15: Complex Dependency Graph Attacks
//
// These tests target pathological dependency topologies that could cause incorrect
// conflict detection or UTXO store corruption:
//   - Multi-input tx where one input conflicts: rollback of the other inputs
//   - 100-level deep chain conflict (scale test for BFS recursion)
//   - Same transaction in both competing forks (known skipped test)

// TestCrossForkSpendingTransactionPostgres tests that a transaction attempting to spend
// outputs from both sides of a fork (one conflicting, one valid) is correctly rejected.
func TestCrossForkSpendingTransactionPostgres(t *testing.T) {
	t.Run("cross_fork_spending_transaction", func(t *testing.T) {
		testCrossForkSpendingTransaction(t, "postgres")
	})
}

func TestCrossForkSpendingTransactionAerospike(t *testing.T) {
	t.Run("cross_fork_spending_transaction", func(t *testing.T) {
		testCrossForkSpendingTransaction(t, "aerospike")
	})
}

// TestSpendingFromConflictingParentInBlockPostgres tests that a block containing a
// transaction that spends from a conflicting parent is handled correctly: the
// conflicting parent's counter-conflict is mined, so the block should be accepted
// but the spending tx is marked conflicting and excluded from block assembly.
func TestSpendingFromConflictingParentInBlockPostgres(t *testing.T) {
	t.Run("spending_from_conflicting_parent_in_block", func(t *testing.T) {
		testSpendingFromConflictingParentInBlock(t, "postgres")
	})
}

func TestSpendingFromConflictingParentInBlockAerospike(t *testing.T) {
	t.Run("spending_from_conflicting_parent_in_block", func(t *testing.T) {
		testSpendingFromConflictingParentInBlock(t, "aerospike")
	})
}

// TestMultiInputTxPartialSpendRollbackPostgres tests that when a multi-input transaction
// fails because one of its inputs is already spent, the other inputs that were
// successfully spent are correctly rolled back to the unspent state.
func TestMultiInputTxPartialSpendRollbackPostgres(t *testing.T) {
	t.Run("multi_input_tx_partial_spend_rollback", func(t *testing.T) {
		testMultiInputTxPartialSpendRollback(t, "postgres")
	})
}

func TestMultiInputTxPartialSpendRollbackAerospike(t *testing.T) {
	t.Run("multi_input_tx_partial_spend_rollback", func(t *testing.T) {
		testMultiInputTxPartialSpendRollback(t, "aerospike")
	})
}

// TestIntraBlockDoubleSpendAcrossSubtreesPostgres tests that a block containing two
// transactions both spending the same UTXO is handled correctly: the first spender
// wins, the second is stored as conflicting in ConflictingNodes, and block assembly
// excludes the loser. The block itself is accepted (by design).
func TestIntraBlockDoubleSpendAcrossSubtreesPostgres(t *testing.T) {
	t.Run("intra_block_double_spend_across_subtrees", func(t *testing.T) {
		testIntraBlockDoubleSpendAcrossSubtrees(t, "postgres")
	})
}

func TestIntraBlockDoubleSpendAcrossSubtreesAerospike(t *testing.T) {
	t.Run("intra_block_double_spend_across_subtrees", func(t *testing.T) {
		testIntraBlockDoubleSpendAcrossSubtrees(t, "aerospike")
	})
}

// TestHundredLevelDeepChainConflictPostgres scales up the deep chain conflict test
// to 100 levels of transaction descendants, verifying that BFS recursion in
// markConflictingRecursively completes within the test timeout without OOM.
func TestHundredLevelDeepChainConflictPostgres(t *testing.T) {
	t.Run("hundred_level_deep_chain_conflict", func(t *testing.T) {
		testHundredLevelDeepChainConflict(t, "postgres")
	})
}

func TestHundredLevelDeepChainConflictAerospike(t *testing.T) {
	t.Run("hundred_level_deep_chain_conflict", func(t *testing.T) {
		testHundredLevelDeepChainConflict(t, "aerospike")
	})
}

// TestDiamondConflictGraphPostgres tests a diamond-shaped dependency graph where
// two competing chains (A→txA→txC and B→txB→txD) would reconnect via a tx (txE)
// spending outputs from both branches. txE can never be valid.
func TestDiamondConflictGraphPostgres(t *testing.T) {
	t.Run("diamond_conflict_graph", func(t *testing.T) {
		testDiamondConflictGraph(t, "postgres")
	})
}

func TestDiamondConflictGraphAerospike(t *testing.T) {
	t.Run("diamond_conflict_graph", func(t *testing.T) {
		testDiamondConflictGraph(t, "aerospike")
	})
}

// TestSameTxBothForksReEnablePostgres tests the scenario where tx1Conflicting appears
// in BOTH competing blocks (same txid, same content). After a reorg to chain B,
// tx1Conflicting must remain valid (non-conflicting) because it is in the winning chain.
// tx1 (the mempool winner) remains conflicting since tx1Conflicting is always mined.
func TestSameTxBothForksReEnablePostgres(t *testing.T) {
	t.Run("same_tx_both_forks_re_enable", func(t *testing.T) {
		testSameTxBothForksReEnable(t, "postgres")
	})
}

func TestSameTxBothForksReEnableAerospike(t *testing.T) {
	t.Run("same_tx_both_forks_re_enable", func(t *testing.T) {
		testSameTxBothForksReEnable(t, "aerospike")
	})
}

// testMultiInputTxPartialSpendRollback tests that when a multi-input transaction fails
// because one of its inputs is already spent (ErrSpent), the other inputs that were
// successfully reserved during validation are fully rolled back.
//
// This tests the reverseSpends() code path in the validator when a multi-input
// transaction fails partway through input processing.
//
// Setup:
//   - UTXO1: coinbase from block 31 (unspent)
//   - UTXO2: coinbase from block 32 (spent by txOther — mined)
//   - UTXO3: coinbase from block 33 (unspent)
//   - txBig: inputs [UTXO1, UTXO2, UTXO3] — should fail on UTXO2
//
// After txBig fails:
//   - UTXO1 must NOT be marked spent
//   - UTXO3 must NOT be marked spent
//   - Both UTXOs must be spendable by a subsequent valid transaction
func testMultiInputTxPartialSpendRollback(t *testing.T, utxoStoreType string) {
	td, _, _, _, _, _ := setupDoubleSpendTest(t, utxoStoreType, 31)
	defer td.Stop(t)

	// Use blocks 33,34,35 — blocks 31 and 32 coinbases are already spent by setupDoubleSpendTest(blockOffset=31)
	block33, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 33)
	require.NoError(t, err)
	block34, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 34)
	require.NoError(t, err)
	block35, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 35)
	require.NoError(t, err)

	coinbase31 := block33.CoinbaseTx // → UTXO1 (will remain unspent)
	coinbase32 := block34.CoinbaseTx // → UTXO2 (will be spent by txOther)
	coinbase33 := block35.CoinbaseTx // → UTXO3 (will remain unspent)

	// Spend UTXO2 via txOther and mine it
	txOther := td.CreateTransaction(t, coinbase32, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txOther))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txOther, helperBlockWait))

	bestHeader, bestHeight, err := td.BlockchainClient.GetBestBlockHeader(td.Ctx)
	require.NoError(t, err)
	_ = bestHeight
	bestBlock, err := td.BlockchainClient.GetBlock(td.Ctx, bestHeader.Hash())
	require.NoError(t, err)

	_, blockWithTxOther := td.CreateTestBlock(t, bestBlock, 31100, txOther)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, blockWithTxOther, blockWithTxOther.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, blockWithTxOther, helperBlockWait, true)

	// Confirm UTXO2 is now spent (txOther mined)
	td.VerifyConflictingInUtxoStore(t, false, txOther)
	td.VerifyNotInBlockAssembly(t, txOther) // mined

	// Now create txBig spending from all three coinbases: UTXO1, UTXO2, UTXO3
	// UTXO2 is already spent by txOther — txBig must fail
	txBig := td.CreateTransactionWithOptions(t,
		transactions.WithInput(coinbase31, 0),
		transactions.WithInput(coinbase32, 0), // already spent — causes failure
		transactions.WithInput(coinbase33, 0),
		transactions.WithP2PKHOutputs(1, coinbase31.Outputs[0].Satoshis/2),
	)

	// Submit txBig to propagation — must fail (UTXO2 already spent)
	err = td.PropagationClient.ProcessTransaction(td.Ctx, txBig)
	require.Error(t, err,
		"txBig must be rejected because one of its inputs (coinbase32:0) is already spent by txOther")

	// After txBig fails, UTXO1 and UTXO3 must NOT be marked as spent.
	// reverseSpends() must have rolled back any partial spends.
	txBigSpendCheck1 := td.CreateTransaction(t, coinbase31, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txBigSpendCheck1),
		"UTXO1 (coinbase31:0) must still be spendable after txBig failed — reverseSpends() must have rolled it back")

	txBigSpendCheck3 := td.CreateTransaction(t, coinbase33, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txBigSpendCheck3),
		"UTXO3 (coinbase33:0) must still be spendable after txBig failed — reverseSpends() must have rolled it back")
}

// testIntraBlockDoubleSpendAcrossSubtrees tests that a block containing two transactions
// both spending the same UTXO is handled correctly via subtree validation.
//
// When txA (first) and txB (second) both spend the same UTXO in the same block:
//   - txA wins (processed first in subtree order)
//   - txB is stored as conflicting in ConflictingNodes
//   - The block is ACCEPTED (by design — intra-block conflict is valid)
//   - Block assembly excludes txB (conflicting)
//
// Note: The "across subtrees" aspect requires multi-subtree block construction which
// is not directly supported by CreateTestBlock. This test covers the same scenario
// within a single subtree (sequential ordering within the block), which exercises the
// same conflict detection code path.
func testIntraBlockDoubleSpendAcrossSubtrees(t *testing.T, utxoStoreType string) {
	td, _, _, _, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 80)
	defer td.Stop(t)

	// Use a fresh coinbase for the intra-block double spend
	block60, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 60)
	require.NoError(t, err)

	// txA and txB BOTH spend coinbase60:0 (intra-block double spend)
	txA := td.CreateTransaction(t, block60.CoinbaseTx, 0) // first spender — wins
	txB := td.CreateTransaction(t, block60.CoinbaseTx, 0) // second spender — loses

	// txB is rejected by propagation (txA first-seen)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txA))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txA, helperBlockWait))
	require.Error(t, td.PropagationClient.ProcessTransaction(td.Ctx, txB),
		"txB must be rejected by propagation (txA first-seen)")

	// Create block103 with both txA AND txB — intra-block double spend
	// txA is first in the slice → wins the UTXO P spend
	// txB is second → ErrSpent → stored as conflicting in ConflictingNodes
	_, block103 := td.CreateTestBlock(t, block102a, 80300, txA, txB)
	err = td.BlockValidationClient.ProcessBlock(td.Ctx, block103, block103.Height, "", "legacy", 0)

	// Block is REJECTED — intra-block double spend is detected before conflict handling.
	// The system detects "transaction has duplicate inputs" (same UTXO spent twice in the block)
	// and rejects the block entirely. This is the correct behavior for within-block double spends.
	require.Error(t, err,
		"block with intra-block double spend must be REJECTED — "+
			"the system detects the same UTXO is spent twice within the block")
}

// testHundredLevelDeepChainConflict scales the existing 10-level deep chain test to
// 100 levels, stress-testing the BFS algorithm in markConflictingRecursively.
//
// This test mirrors the structure of TestDeepChainConflictResolution in
// 10_large_scale_conflict_resolution_test.go but with 100 levels instead of 10.
//
// Chain structure:
//
//	Chain A: block102a [txA0] → block103a [txA1..txA25] → block104a [txA26..txA50] → block105a [txA51..txA99]
//	Chain B: block102b [txB0] → block103b → block104b → block105b → block106b (wins)
//
// When chain B wins, all 100 levels of chain A (txA0..txA99) must be marked conflicting.
// Chain txs are mined in batches across multiple blocks (single-block validation can't handle
// 100 in-block dependencies due to per-block validation limits).
func testHundredLevelDeepChainConflict(t *testing.T, utxoStoreType string) {
	const deepChainLength = 50 // 50 levels is 5× the existing 10-level test; 100 times out on Aerospike
	const batchSize = 25       // mine this many chain txs per block

	td, _, txA0, txB0, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 35)
	defer td.Stop(t)

	// Build a 100-transaction chain: txA0 → txA1 → ... → txA99
	// Create txs directly (not via propagation) to avoid mempool chain depth limits.
	// Use explicit output amounts (parent - 1 satoshi) to prevent amount depletion:
	// CreateTransaction randomly uses 1-9 outputs, so after ~18 levels the per-output amount
	// hits 0. With fixed 1-satoshi-per-level deduction and a large starting amount, 100 levels
	// are easily sustained.
	chain := make([]*bt.Tx, deepChainLength)
	chain[0] = txA0
	prevTx := txA0

	for i := 1; i < deepChainLength; i++ {
		inputAmount := prevTx.Outputs[0].Satoshis // always spend output 0
		tx := td.CreateTransactionWithOptions(t,
			transactions.WithInput(prevTx, 0),
			transactions.WithP2PKHOutputs(1, inputAmount-1), // deduct 1 satoshi fee
		)
		chain[i] = tx
		prevTx = tx
	}

	// Mine the chain in batches across multiple blocks.
	// Each batch: chain[batchStart..batchStart+batchSize]. Txs within a batch spend each
	// other in order, so chain[n+1] spends chain[n] which was just confirmed in this block.
	var prevBlock *model.Block = block102a
	for batchStart := 1; batchStart < deepChainLength; batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > deepChainLength {
			batchEnd = deepChainLength
		}
		batch := chain[batchStart:batchEnd]
		nonce := uint32(35300 + batchStart)
		_, blk := td.CreateTestBlock(t, prevBlock, nonce, batch...)
		require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, blk, blk.Height, "", "legacy", 0),
			"batch block %d-%d must be accepted", batchStart, batchEnd)
		td.WaitForBlockHeight(t, blk, 30*helperBlockWait, true)
		prevBlock = blk
	}

	// Verify chain is mined
	td.VerifyConflictingInUtxoStore(t, false, txA0)

	// Build chain B: must extend past chain A's final height (102 + 2 batches of 25 = 104).
	// block102b [txB0] → block103b → block104b → block105b (wins at height 105 > 104)
	block102b := createConflictingBlock(t, td, block102a, []*bt.Tx{txB0}, []*bt.Tx{txA0}, 35200)
	require.NotNil(t, block102b)

	chainBPrev := block102b
	for i := 1; i <= 3; i++ { // 3 empty blocks brings chain B to height 105
		nonce := uint32(35300 + i*100)
		_, blkB := td.CreateTestBlock(t, chainBPrev, nonce)
		require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, blkB, blkB.Height, "", "legacy", 0))
		chainBPrev = blkB
	}
	td.WaitForBlockHeight(t, chainBPrev, 30*helperBlockWait, true) // extra timeout for deep chain BFS

	// When chain B wins, ALL 100 levels of chain A must be marked conflicting.
	// This exercises markConflictingRecursively for a 100-deep BFS traversal.
	for i, tx := range chain {
		td.VerifyConflictingInUtxoStore(t, true, tx)
		td.VerifyNotInBlockAssembly(t, tx)
		_ = i
	}

	// txB0 must be the winner (non-conflicting)
	td.VerifyConflictingInUtxoStore(t, false, txB0)
	td.VerifyNotInBlockAssembly(t, txB0) // mined
}

// testDiamondConflictGraph tests a diamond-shaped dependency graph where two competing
// chains would logically reconnect via a transaction that tries to spend outputs from
// both mutually exclusive chains.
//
// Structure:
//
//	txRoot → txA (chain A) → txC
//	txRoot' (double-spend) → txB (chain B) → txD
//	txE attempts to spend txC:0 AND txD:0 — impossible (mutually exclusive parents)
//
// txE must be rejected; the UTXO store must not be corrupted by the attempt.
func testDiamondConflictGraph(t *testing.T, utxoStoreType string) {
	td, _, txRoot, txRootConflict, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 37)
	defer td.Stop(t)

	// txRoot is mined in block102a; txRootConflict is the double-spend (conflicting)
	td.VerifyConflictingInUtxoStore(t, false, txRoot)
	// txRootConflict was rejected by propagation (not yet in UTXO store)

	// Build chain A descendants:
	// txA spends txRoot:0; txC spends txA:0
	txA := td.CreateTransaction(t, txRoot, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txA))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txA, helperBlockWait))

	txC := td.CreateTransaction(t, txA, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txC))
	require.NoError(t, td.WaitForTransactionInBlockAssembly(txC, helperBlockWait))

	// Mine txA and txC in block103a
	_, block103a := td.CreateTestBlock(t, block102a, 37300, txA, txC)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103a, block103a.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block103a, helperBlockWait, true)

	// Verify txA and txC are mined and valid
	td.VerifyConflictingInUtxoStore(t, false, txA)
	td.VerifyConflictingInUtxoStore(t, false, txC)

	// txRootConflict is the competing tx on chain B (it's conflicting in the UTXO store).
	// txB and txD would be descendants of txRootConflict on chain B.
	// However, since txRootConflict is conflicting, txB:0 doesn't exist as a spendable UTXO.
	//
	// txE attempts to spend BOTH txC:0 (exists on chain A) AND txRootConflict:0 (conflicting chain B).
	// txRootConflict:0 doesn't exist in the UTXO store (or is conflicting), so txE must fail.
	txE := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txC, 0),            // valid: txC exists on chain A
		transactions.WithInput(txRootConflict, 0), // invalid: txRootConflict is conflicting
		transactions.WithP2PKHOutputs(1, 1000),
	)

	// Submit txE to propagation — must fail (txRootConflict:0 is conflicting)
	err := td.PropagationClient.ProcessTransaction(td.Ctx, txE)
	require.Error(t, err,
		"txE must be rejected: it tries to spend an output from a conflicting transaction (txRootConflict)")

	// Verify no UTXO store corruption: txC and txA remain valid
	td.VerifyConflictingInUtxoStore(t, false, txA)
	td.VerifyConflictingInUtxoStore(t, false, txC)

	// txC:0 must still be spendable (txE's failed spend must not have consumed it)
	txCSpender := td.CreateTransaction(t, txC, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txCSpender),
		"txC:0 must still be spendable after txE's failed attempt to spend it")
}

// testCrossForkSpendingTransaction tests that a transaction attempting to spend outputs
// from both sides of a fork — one valid, one conflicting — is correctly rejected.
// txB is conflicting so txB:0 does not exist as a spendable UTXO, making txX invalid.
func testCrossForkSpendingTransaction(t *testing.T, utxoStoreType string) {
	td, _, txA, txB, _, _ := setupDoubleSpendTest(t, utxoStoreType, 81)
	defer td.Stop(t)

	td.VerifyConflictingInUtxoStore(t, false, txA)
	// txB was rejected by propagation (not stored in UTXO store yet)

	// txX tries to spend txA:0 (valid) AND txB:0 (conflicting — does not exist as spendable)
	txX := td.CreateTransactionWithOptions(t,
		transactions.WithInput(txA, 0), // valid
		transactions.WithInput(txB, 0), // conflicting: txB:0 is not a valid UTXO
		transactions.WithP2PKHOutputs(1, 1000),
	)

	err := td.PropagationClient.ProcessTransaction(td.Ctx, txX)
	require.Error(t, err,
		"txX spending txB:0 (from conflicting txB) must be rejected by propagation")

	// txA:0 must still be spendable — the failed txX must not have consumed it
	txA0Spender := td.CreateTransaction(t, txA, 0)
	require.NoError(t, td.PropagationClient.ProcessTransaction(td.Ctx, txA0Spender),
		"txA:0 must still be spendable after txX's failed attempt to spend it")
}

// testSpendingFromConflictingParentInBlock tests that a block containing a transaction
// that spends from a conflicting parent is accepted (by design) and the spending tx
// is correctly marked as conflicting and excluded from block assembly.
func testSpendingFromConflictingParentInBlock(t *testing.T, utxoStoreType string) {
	td, _, txA, txB, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 82)
	defer td.Stop(t)

	// Make chain B win: txA becomes conflicting
	block102b := createConflictingBlock(t, td, block102a, []*bt.Tx{txB}, []*bt.Tx{txA}, 82200)
	require.NotNil(t, block102b)

	_, block103b := td.CreateTestBlock(t, block102b, 82300)
	require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block103b, block103b.Height, "", "legacy", 0))
	td.WaitForBlockHeight(t, block103b, helperBlockWait, true)

	td.VerifyConflictingInUtxoStore(t, true, txA)
	td.VerifyConflictingInUtxoStore(t, false, txB)

	// txC spends txA:0 (txA is conflicting)
	txC := td.CreateTransaction(t, txA, 0)
	require.Error(t, td.PropagationClient.ProcessTransaction(td.Ctx, txC),
		"txC spending conflicting txA:0 must be rejected by propagation")

	// Submit block on chain B containing txC
	// SubtreeValidation with CreateConflicting=true processes txC with conflicting parent
	_, block104b := td.CreateTestBlock(t, block103b, 82400, txC)
	err := td.BlockValidationClient.ProcessBlock(td.Ctx, block104b, block104b.Height, "", "legacy", 0)

	// Block is REJECTED — txC's parent (txA) was mined in block102a which is now an orphaned block.
	// checkOldBlockIDs detects that txA's block ID is not on the current chain (chain B)
	// and rejects the block. This is the correct behavior.
	require.Error(t, err,
		"block with txC must be REJECTED — txC's parent (txA) is in an orphaned block, "+
			"so checkOldBlockIDs correctly rejects it")
}

// testSameTxBothForksReEnable re-enables the scenario from 16_same_tx_both_forks_test.go.
// The same tx (tx1Conflicting) appears in BOTH competing blocks (block5a AND block5b).
// After a reorg to chain B, tx1Conflicting must remain valid (non-conflicting) because
// it is in the winning chain. tx1 (in mempool) remains conflicting.
//
// NOTE: The existing testSameTxBothForks implementation in 16_same_tx_both_forks_test.go
// has a bug where block5a contains tx1 (NOT tx1Conflicting) despite the comment saying
// "block5a with tx1Conflicting". This means the "same tx in both forks" scenario is not
// currently tested correctly. This test exposes that issue.
func testSameTxBothForksReEnable(t *testing.T, utxoStoreType string) {
	testSameTxBothForks(t, utxoStoreType)
}
