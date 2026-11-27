package subtreeprocessor

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
)

// heldChildren holds back the moved-back block transactions that spend a transaction the move-back
// leaves at its pending position (design section 5 item 2c). The move-back builds the block's
// transactions ahead of the pending ones, and a block transaction that is already pending keeps its
// pending position; without this, a child of such a transaction would come out ahead of its parent,
// and a subtree boundary between the two lets a miner be given the child without the parent.
//
// A held-back transaction is emitted right after the last of its parents, so the pending
// transactions keep their relative order. A held-back transaction's own children wait for it, and
// so does a pending transaction that spends one: while anything is held back, the pending
// transactions are checked too (appendPending). The maps hold the kept block transactions and the
// held-back ones, not the pending set.
//
// It relies on a block listing a parent before its child, which block validation enforces.
type heldChildren struct {
	// waiting has an entry for every transaction that is not emitted yet and that a held-back
	// transaction may wait for: the value lists the held-back transactions that wait for it.
	waiting   map[chainhash.Hash][]int
	nodes     []subtreepkg.Node
	remaining []int // per held-back node, the parents not emitted yet

	// unreleased is how many held-back nodes are still waiting. At zero the pending transactions
	// pass straight through, without a tx map lookup.
	unreleased int
}

func newHeldChildren() *heldChildren {
	return &heldChildren{waiting: make(map[chainhash.Hash][]int)}
}

// keep records a block transaction that stays at its pending position.
func (h *heldChildren) keep(hash chainhash.Hash) {
	h.waiting[hash] = nil
}

// hold takes node into the held-back set when it spends a transaction that is not emitted yet, and
// reports whether it did.
func (h *heldChildren) hold(node subtreepkg.Node, parents []chainhash.Hash) bool {
	idx := -1

	for _, parent := range parents {
		children, ok := h.waiting[parent]
		if !ok {
			continue
		}

		if idx < 0 {
			idx = len(h.nodes)
			h.nodes = append(h.nodes, node)
			h.remaining = append(h.remaining, 0)
			h.unreleased++
		}

		if n := len(children); n > 0 && children[n-1] == idx {
			continue // the same parent listed twice
		}

		h.waiting[parent] = append(children, idx)
		h.remaining[idx]++
	}

	if idx < 0 {
		return false
	}

	// A pending node can already have children waiting for it.
	if _, ok := h.waiting[node.Hash]; !ok {
		h.waiting[node.Hash] = nil
	}

	return true
}

// appendPending appends a previous pending node to dst, unless it spends a held-back node, and then
// releases what the node frees. parents gives the node's parents from the tx map.
func (h *heldChildren) appendPending(dst []subtreepkg.Node, node subtreepkg.Node,
	parents func(chainhash.Hash) (*subtreepkg.TxInpoints, bool)) []subtreepkg.Node {
	if h.unreleased > 0 {
		if inpoints, ok := parents(node.Hash); ok && inpoints != nil && h.hold(node, inpoints.ParentTxHashes) {
			return dst
		}
	}

	return h.release(append(dst, node), node.Hash)
}

// release appends to dst every held-back transaction whose last parent is hash, and then whatever
// that frees in turn.
func (h *heldChildren) release(dst []subtreepkg.Node, hash chainhash.Hash) []subtreepkg.Node {
	queue, ok := h.waiting[hash]
	if !ok {
		return dst
	}

	delete(h.waiting, hash)

	// Each queue entry is one parent of that node that has been emitted.
	for i := 0; i < len(queue); i++ {
		idx := queue[i]

		if h.remaining[idx]--; h.remaining[idx] > 0 {
			continue
		}

		dst = append(dst, h.nodes[idx])
		h.unreleased--

		queue = append(queue, h.waiting[h.nodes[idx].Hash]...)
		delete(h.waiting, h.nodes[idx].Hash)
	}

	return dst
}

// stranded returns the held-back transactions that are still waiting, in the block's order. Their
// parent, or an ancestor of it, was never emitted: it is in the tx map but in no subtree, or the
// move-back dropped it. Emitting one would put a transaction in the template without its parent, so
// the caller drops them, and with them everything that waits for them, which is in the list too.
func (h *heldChildren) stranded() []chainhash.Hash {
	var out []chainhash.Hash

	for idx, left := range h.remaining {
		if left > 0 {
			out = append(out, h.nodes[idx].Hash)
		}
	}

	return out
}
