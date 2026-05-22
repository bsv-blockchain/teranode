package subtreeprocessor

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProcessConflictingTransactions_ReverseCascadeSurfacing covers the
// filter short-circuit path in processConflictingTransactions that I added
// for the reverse-reorg flow.
//
// When the moveBack loop's ReverseProcessConflicting already promoted a
// counter and demoted the matching ConflictingNodes hash, the moveForward
// pass on a sibling block can hit the same hash in its own ConflictingNodes.
// Re-running ProcessConflicting on that hash double-spends the parent UTXO
// and trips the "tx is not conflicting" precondition. The filter against
// processedConflictingHashesMap (populated by the moveBack loop) drops
// those hashes before they reach ProcessConflicting.
//
// But: silently dropping them leaves any reverse-cascade losers in the
// in-memory chainedSubtrees (rebuilt in processRemainderTxHashes via the
// losingTxHashesMap filter) and any queue-resident losers waiting for the
// next dequeueDuringBlockMovement (which filters via conflictingHashes).
// So when the filter short-circuits, the function returns the reverse-
// cascade set as BOTH a populated losingTxHashesMap and a conflictingSet,
// matching what downstream eviction needs.
//
// These tests assert that behaviour without touching the SQL store
// (sqlitememory deadlocks on SetConflicting + concurrent Get — see comment
// in stores/utxo/tests/tests.go:670-676).
func TestProcessConflictingTransactions_ReverseCascadeSurfacing(t *testing.T) {
	t.Run("all conflicting nodes already processed — surfaces reverse cascade as losers + conflictingSet", func(t *testing.T) {
		stp := newReverseFilterStp(t)

		preProcessedA := chainhash.HashH([]byte("rev-filter-preprocessed-A"))
		preProcessedB := chainhash.HashH([]byte("rev-filter-preprocessed-B"))

		// Reverse cascade includes the demoted hashes plus an unmined
		// descendant the reverse BFS reached.
		cascadeDescendant := chainhash.HashH([]byte("rev-filter-cascade-descendant"))

		stp.reverseCascadedConflictingSet = map[chainhash.Hash]struct{}{
			preProcessedA:     {},
			preProcessedB:     {},
			cascadeDescendant: {},
		}

		conflictingNodes := []chainhash.Hash{preProcessedA, preProcessedB}
		processedMap := map[chainhash.Hash]bool{
			preProcessedA: true,
			preProcessedB: true,
		}

		losing, conflictingSet, err := stp.processConflictingTransactions(
			context.Background(),
			&model.Block{Header: dummyHeader()},
			conflictingNodes,
			processedMap,
		)
		require.NoError(t, err)

		require.NotNil(t, losing, "filter short-circuit must surface reverse cascade as losingTxHashesMap so the chainedSubtrees rebuild drops the losers")
		require.Equal(t, 3, losing.Length(), "losingTxHashesMap must contain every reverse-cascade hash (demoted + descendant)")
		require.True(t, losing.Exists(preProcessedA))
		require.True(t, losing.Exists(preProcessedB))
		require.True(t, losing.Exists(cascadeDescendant))

		require.Len(t, conflictingSet, 3, "conflictingSet must match losingTxHashesMap so the downstream dequeueDuringBlockMovement evicts queue-resident losers")
		require.Contains(t, conflictingSet, preProcessedA)
		require.Contains(t, conflictingSet, preProcessedB)
		require.Contains(t, conflictingSet, cascadeDescendant)
	})

	t.Run("all conflicting nodes already processed, no reverse cascade — both returns empty", func(t *testing.T) {
		stp := newReverseFilterStp(t)

		hashA := chainhash.HashH([]byte("rev-filter-empty-A"))

		// reverseCascadedConflictingSet left nil — simulates a moveBack
		// block whose subtree.ConflictingNodes contained a tx that was
		// already-reversed (Conflicting=true) so ReverseProcessConflicting
		// short-circuited with empty cascade.
		stp.reverseCascadedConflictingSet = nil

		losing, conflictingSet, err := stp.processConflictingTransactions(
			context.Background(),
			&model.Block{Header: dummyHeader()},
			[]chainhash.Hash{hashA},
			map[chainhash.Hash]bool{hashA: true},
		)
		require.NoError(t, err)
		assert.Nil(t, losing, "no cascade → no losers")
		assert.Nil(t, conflictingSet, "no cascade → no conflictingSet")
	})

	t.Run("empty input returns empty without consulting reverse cascade", func(t *testing.T) {
		stp := newReverseFilterStp(t)

		stp.reverseCascadedConflictingSet = map[chainhash.Hash]struct{}{
			chainhash.HashH([]byte("ignored")): {},
		}

		losing, conflictingSet, err := stp.processConflictingTransactions(
			context.Background(),
			&model.Block{Header: dummyHeader()},
			nil,
			map[chainhash.Hash]bool{},
		)
		require.NoError(t, err)
		assert.Nil(t, losing)
		assert.Nil(t, conflictingSet)
	})
}

// dummyHeader is a placeholder header. The filter short-circuit path
// returns before any header-driven work runs, so the only requirement is
// non-nil HashPrevBlock + HashMerkleRoot so .String() doesn't panic.
func dummyHeader() *model.BlockHeader {
	return &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      1,
		Bits:           model.NBit{},
		Nonce:          1,
	}
}

// newReverseFilterStp builds a SubtreeProcessor wired with a MockUtxostore
// expectation that ProcessConflicting is NEVER called — the filter short-
// circuit path under test must skip ProcessConflicting entirely.
func newReverseFilterStp(t *testing.T) *SubtreeProcessor {
	t.Helper()

	mockBlockchainClient := &blockchain.Mock{}
	mockUtxoStore := &utxo.MockUtxostore{}

	// AssertExpectations would fail if ProcessConflicting was called because
	// we set NO expectation for it. We don't need any other utxo method
	// either — the short-circuit returns before Get/Spend/etc. fire.
	t.Cleanup(func() { mockUtxoStore.AssertExpectations(t) })
	t.Cleanup(func() { mockBlockchainClient.AssertExpectations(t) })

	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

	newSubtreeChan := make(chan NewSubtreeRequest, 4)
	t.Cleanup(func() { close(newSubtreeChan) })

	stp, err := NewSubtreeProcessor(
		context.Background(),
		ulogger.TestLogger{},
		settings,
		blob_memory.New(),
		mockBlockchainClient,
		mockUtxoStore,
		newSubtreeChan,
	)
	require.NoError(t, err)

	return stp
}
