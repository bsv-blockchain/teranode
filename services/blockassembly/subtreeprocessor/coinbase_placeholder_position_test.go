package subtreeprocessor

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// TestCheckSubtreeProcessor_CoinbasePlaceholderPosition pins BA-SUBTREE-010 and
// its acceptance criterion AC-BA-SUBTREE-010.1: the service MUST detect and emit
// an error if a coinbase placeholder appears anywhere other than (first subtree,
// first node).
//
// The detection mechanism is checkSubtreeProcessor (reached in production via the
// CheckBlockAssembly gRPC operation / CheckSubtreeProcessor). GetMiningCandidate
// itself does NOT validate placeholder position, so this health-check is the only
// place the contract is enforced — that is the behaviour these tests pin.
//
// Each sub-test seeds exactly one inconsistency (a misplaced placeholder) and
// nothing else, so the returned error is unambiguously the placeholder-position
// error rather than one of checkSubtreeProcessor's other consistency checks.
func TestCheckSubtreeProcessor_CoinbasePlaceholderPosition(t *testing.T) {
	// AC-BA-SUBTREE-010.1 literally: a coinbase placeholder at index 1 (not 0)
	// of the current (incomplete) subtree must be rejected.
	t.Run("placeholder at non-first node of current subtree", func(t *testing.T) {
		stp := setupTestSubtreeProcessor(t)

		// After construction the current subtree already holds the legitimate
		// placeholder at index 0. Append a second placeholder at index 1 — that
		// is the only inconsistency in the processor state.
		cur := stp.currentSubtree.Load()
		require.Equal(t, subtreepkg.CoinbasePlaceholderHashValue, cur.Nodes[0].Hash,
			"precondition: index 0 of the first subtree is the coinbase placeholder")
		// AddNode deliberately refuses coinbase placeholders, so append straight
		// to the exported Nodes slice to forge the corrupted state under test.
		cur.Nodes = append(cur.Nodes, subtreepkg.Node{Hash: subtreepkg.CoinbasePlaceholderHashValue})
		stp.currentSubtree.Store(cur)

		errCh := make(chan error, 1)
		go func() { errCh <- stp.CheckSubtreeProcessor() }()

		select {
		case err := <-errCh:
			require.Error(t, err, "a misplaced coinbase placeholder must be reported")
			require.Contains(t, err.Error(), "coinbase placeholder not in first node",
				"the error must identify the placeholder-position violation")
		case <-time.After(5 * time.Second):
			t.Fatal("CheckSubtreeProcessor hung instead of reporting the misplaced placeholder")
		}
	})

	// The complementary guard: a placeholder living in a non-first chained
	// (completed) subtree must also be rejected (subtree index != 0).
	t.Run("placeholder in non-first chained subtree", func(t *testing.T) {
		stp := setupTestSubtreeProcessor(t)

		// Build two completed subtrees. The first is well-formed: the coinbase
		// placeholder at (0,0) followed by a real tx that is registered in
		// currentTxMap so it passes the membership check. The second subtree
		// carries a stray placeholder at its first node — legal only in the
		// first subtree — which is the single inconsistency under test.
		realTx := chainhash.HashH([]byte("real-tx-in-first-chained-subtree"))
		stp.currentTxMap.Set(realTx, &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{}})

		firstChained, err := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, err)
		require.NoError(t, firstChained.AddCoinbaseNode())
		require.NoError(t, firstChained.AddNode(realTx, 100, 200))

		// AddCoinbaseNode places the placeholder at index 0, which is legal
		// within a subtree — the violation here is that this subtree is NOT the
		// first chained subtree.
		secondChained, err := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, err)
		require.NoError(t, secondChained.AddCoinbaseNode())

		// Writing chainedSubtrees from the test goroutine is safe: the channel
		// send inside CheckSubtreeProcessor establishes a happens-before edge
		// with the dispatcher goroutine that reads it, and nothing else touches
		// chainedSubtrees in this isolated processor.
		stp.chainedSubtrees = []*subtreepkg.Subtree{firstChained, secondChained}

		errCh := make(chan error, 1)
		go func() { errCh <- stp.CheckSubtreeProcessor() }()

		select {
		case err := <-errCh:
			require.Error(t, err, "a placeholder in a non-first chained subtree must be reported")
			require.Contains(t, err.Error(), "coinbase placeholder not in first subtree",
				"the error must identify the placeholder-position violation")
		case <-time.After(5 * time.Second):
			t.Fatal("CheckSubtreeProcessor hung instead of reporting the misplaced placeholder")
		}
	})

	// Control: a correctly placed placeholder at (first subtree, first node) must
	// NOT trigger the placeholder-position error. This guards against a test that
	// would pass simply because checkSubtreeProcessor always errors.
	t.Run("correctly placed placeholder passes the position check", func(t *testing.T) {
		stp := setupTestSubtreeProcessor(t)

		errCh := make(chan error, 1)
		go func() { errCh <- stp.CheckSubtreeProcessor() }()

		select {
		case err := <-errCh:
			if err != nil {
				require.NotContains(t, err.Error(), "coinbase placeholder",
					"a placeholder at (0,0) must not raise a placeholder-position error")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("CheckSubtreeProcessor hung on a well-formed processor state")
		}
	})
}
