package model

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// subtreeWithNodes builds a subtree whose first node is whatever the caller
// says, so a test can distinguish the coinbase placeholder at index 0 from a
// real transaction hash there — the distinction MissingSubtreeDataTxs exists to
// make.
func subtreeWithNodes(t *testing.T, hashes ...chainhash.Hash) *subtreepkg.Subtree {
	t.Helper()

	nodes := make([]subtreepkg.Node, len(hashes))
	for i, h := range hashes {
		nodes[i] = subtreepkg.Node{Hash: h}
	}

	return &subtreepkg.Subtree{Nodes: nodes}
}

func hashOf(t *testing.T, s string) chainhash.Hash {
	t.Helper()

	h := chainhash.HashH([]byte(s))

	return h
}

// TestMissingSubtreeDataTxs is the direct test for the shared completeness
// predicate. It is called from two packages on the consensus path and decides
// whether a subtree_data body may be stored or built on, so each of its
// decisions is pinned here rather than only reached through the regenerator's
// HTTP tests.
func TestMissingSubtreeDataTxs(t *testing.T) {
	txA := hashOf(t, "a")
	txB := hashOf(t, "b")

	t.Run("a body filling every node is complete", func(t *testing.T) {
		subtree := subtreeWithNodes(t, txA, txB)
		data := &subtreepkg.Data{Subtree: subtree, Txs: []*bt.Tx{bt.NewTx(), bt.NewTx()}}

		require.Equal(t, 0, MissingSubtreeDataTxs(subtree, data, false))
	})

	t.Run("an empty body misses every node", func(t *testing.T) {
		subtree := subtreeWithNodes(t, txA, txB)
		data := &subtreepkg.Data{Subtree: subtree, Txs: make([]*bt.Tx, 2)}

		require.Equal(t, 2, MissingSubtreeDataTxs(subtree, data, false))
	})

	t.Run("a truncated tail is counted", func(t *testing.T) {
		subtree := subtreeWithNodes(t, txA, txB)
		data := &subtreepkg.Data{Subtree: subtree, Txs: []*bt.Tx{bt.NewTx(), nil}}

		require.Equal(t, 1, MissingSubtreeDataTxs(subtree, data, false))
	})

	// The coinbase placeholder carries no transaction of its own, so a nil entry
	// under it is the normal shape rather than a short body.
	t.Run("a nil entry under an exempt coinbase placeholder is not missing", func(t *testing.T) {
		subtree := subtreeWithNodes(t, subtreepkg.CoinbasePlaceholderHashValue, txA)
		data := &subtreepkg.Data{Subtree: subtree, Txs: []*bt.Tx{nil, bt.NewTx()}}

		require.Equal(t, 0, MissingSubtreeDataTxs(subtree, data, true))
	})

	// The exemption is the caller's decision, not an inference from the hash. The
	// meta regenerator passes the subtree's real position, because validateSubtree
	// skips node 0 only for sIdx 0 and a meta that leaves it unset condemns a valid
	// block. Inferring here would take that decision away from the caller that has
	// to live with it.
	t.Run("the placeholder does not exempt index 0 when the caller says not to", func(t *testing.T) {
		subtree := subtreeWithNodes(t, subtreepkg.CoinbasePlaceholderHashValue, txA)
		data := &subtreepkg.Data{Subtree: subtree, Txs: []*bt.Tx{nil, bt.NewTx()}}

		require.Equal(t, 1, MissingSubtreeDataTxs(subtree, data, false),
			"a subtree the caller does not exempt gets no index-0 pass, whatever hash it carries there")
	})

	// The decision the predicate's comment spends most of its length defending,
	// and the one a literal copy of Data.Serialize's `i != 0` exemption would get
	// wrong. Only a block's first subtree carries the placeholder, so for every
	// other subtree node 0 is a real transaction. Exempting index 0 regardless
	// would report this body complete and let Serialize walk into a nil *bt.Tx,
	// panicking in an errgroup goroutine that nothing recovers.
	t.Run("a nil entry under a real tx hash at index 0 is missing even when exempt", func(t *testing.T) {
		subtree := subtreeWithNodes(t, txA)
		data := &subtreepkg.Data{Subtree: subtree, Txs: []*bt.Tx{nil}}

		require.Equal(t, 1, MissingSubtreeDataTxs(subtree, data, true),
			"index 0 is only exempt when it genuinely holds the coinbase placeholder")
	})

	// A body carrying fewer entries than the subtree has nodes must count the
	// shortfall. The two lengths always agree today because serializeFromReader
	// allocates Txs at Subtree.Length() before reading, so this pins the
	// predicate against a Data that reached it by some other route rather than
	// against current library behaviour.
	t.Run("a short Txs slice counts the shortfall", func(t *testing.T) {
		subtree := subtreeWithNodes(t, txA, txB)
		data := &subtreepkg.Data{Subtree: subtree, Txs: []*bt.Tx{bt.NewTx()}}

		require.Equal(t, 1, MissingSubtreeDataTxs(subtree, data, false))
	})

	t.Run("nil data misses every node", func(t *testing.T) {
		subtree := subtreeWithNodes(t, txA, txB)

		require.Equal(t, 2, MissingSubtreeDataTxs(subtree, nil, false))
	})

	// Not fail-open: a subtree with no nodes has nothing that could be unfilled.
	t.Run("a nil subtree has no nodes to fill", func(t *testing.T) {
		require.Equal(t, 0, MissingSubtreeDataTxs(nil, nil, false))
	})
}
