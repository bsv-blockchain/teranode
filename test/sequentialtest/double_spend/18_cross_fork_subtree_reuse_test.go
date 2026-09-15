package doublespendtest

import (
	"testing"

	bt "github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/stretchr/testify/require"
)

// File 18: Cross-fork subtree reuse
//
// A subtree file proves that its transactions are structurally valid and
// signature-checked. It does NOT prove they are valid for an arbitrary future
// block, because double-spend validity depends on the candidate block's
// ancestry.
//
// The scenario below is distinct from everything in file 12. There, a cached
// subtree is submitted on a COMPETING FORK, where accepting it is correct by
// design — the counter-spending transaction is not in that block's ancestry.
// Here the same cached subtree is replayed onto a chain that ALREADY CONFIRMED
// the counter-spender, which makes it an ancestor double spend:
//
//	                 /- blockF(txB)              <- losing sibling fork
//	... -> block101 -
//	                 \- block102a(txA) -> blockC(txB)
//	                                        ^
//	                        reuses blockF's subtree verbatim
//
// txA and txB both spend the same outpoint. txA is confirmed in block102a.
// blockC extends block102a, so txA is in blockC's own ancestry, and blockC must
// therefore be rejected.
//
// Both blocks reference the identical subtree: CreateTestBlock derives the
// subtree root purely from the transaction list, and storeSubtreeFiles never
// writes FileTypeSubtree (only ...ToCheck/...Data/...Meta), so the blessed file
// written while validating blockF survives and blockC reuses it.
//
// The load-bearing assertions are the UTXO-state ones, not the rejection: even
// if a future change alters where the block is rejected, txA must never lose its
// spend without a reorg.
//
// These tests require a store that supports conflicting-transaction flows.
// sqlitememory cannot host them — SetConflicting holds a write transaction while
// the get-batcher needs a second connection, and the single-connection model
// deadlocks (see stores/utxo/sql/setconflicting_cascade_test.go).

func TestCrossForkSubtreeReusePostgres(t *testing.T) {
	t.Run("cached_fork_subtree_rejected_on_chain_confirming_counter_spend", func(t *testing.T) {
		testCrossForkSubtreeReuse(t, "postgres")
	})
}

func TestCrossForkSubtreeReuseAerospike(t *testing.T) {
	t.Run("cached_fork_subtree_rejected_on_chain_confirming_counter_spend", func(t *testing.T) {
		testCrossForkSubtreeReuse(t, "aerospike")
	})
}

func testCrossForkSubtreeReuse(t *testing.T, utxoStoreType string) {
	// block102a is the tip at height 102 and contains txA, which spends the
	// coinbase output that txB also spends.
	td, _, txA, txB, block102a, _ := setupDoubleSpendTest(t, utxoStoreType, 70)
	defer td.Stop(t)

	td.VerifyConflictingInUtxoStore(t, false, txA)

	// blockF forks from block101 and carries txB. This is legitimate: txA's
	// block is not in blockF's ancestry, so txB is stored as conflicting and
	// blockF's subtree is blessed with txB in its ConflictingNodes trailer.
	blockF := createConflictingBlock(t, td, block102a, []*bt.Tx{txB}, []*bt.Tx{txA}, 70200)
	require.NotNil(t, blockF)

	subtreeS := blockF.Subtrees[0]

	// The blessed file is what makes the cache hit possible; assert it exists
	// rather than inferring it, so harness drift fails loudly here instead of
	// silently turning this into a different test.
	blessed, err := td.SubtreeStore.Exists(td.Ctx, subtreeS[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, blessed, "blockF's subtree must be blessed for this scenario to exercise the cache hit")

	// blockC extends block102a — so txA IS in its ancestry — and reuses the
	// identical subtree.
	subtreeC, blockC := td.CreateTestBlock(t, block102a, 70301, txB)
	require.True(t, subtreeC.RootHash().IsEqual(subtreeS),
		"blockC must reuse blockF's subtree verbatim for this scenario to be meaningful")

	err = td.BlockValidationClient.ProcessBlock(td.Ctx, blockC, blockC.Height, "", "legacy", 0)
	require.Error(t, err,
		"a block reusing a cached subtree whose conflicting tx has a counter-spender "+
			"confirmed in that block's own ancestry must be rejected")

	// The chain must not have advanced onto blockC.
	td.WaitForBlockHeight(t, block102a, helperBlockWait, true)

	// The critical invariant: the confirmed spend is untouched. Before the fix
	// these flip — txA is demoted and unspent, txB is promoted — reversing a
	// confirmed payment with no reorg.
	td.VerifyConflictingInUtxoStore(t, false, txA)
	td.VerifyConflictingInUtxoStore(t, true, txB)

	// txA is mined, txB is conflicting: neither belongs in a mining candidate.
	td.VerifyNotInBlockAssembly(t, txA)
	td.VerifyNotInBlockAssembly(t, txB)
}
