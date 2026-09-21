package model

import (
	"github.com/bsv-blockchain/go-bt/v2"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
)

// MissingSubtreeDataTxs reports how many of the subtree's nodes a subtree_data
// body failed to fill. A non-zero result means the body cannot satisfy the
// subtree and must be rejected rather than built on.
//
// NewSubtreeDataFromReader stops at a clean io.EOF without checking it filled
// every node, so a short or empty body deserializes "successfully" and leaves
// the tail Txs entries nil. An empty body is the issue-1368 signature: a peer's
// proxy cache replaying a failed or aborted on-demand generation as
// "200 + 0 bytes".
//
// This is the shared answer to one question — can this body satisfy this
// subtree — asked by the two places that judge a subtree_data body as a whole:
// blockvalidation's catchup fetcher and the subtree meta regenerator's local
// read. buildMetaFromSubtreeData asks a finer version of it, node by node so it
// can name the one that is missing, and keeps its own loop; the index-0 rule
// below is the part all three have to agree on, so it lives here.
//
// Two distinct disasters follow from trusting an unsatisfying body:
//
//   - A meta derived from it records no parents for the missing tail.
//     GetParentTxHashes then returns nil with no error, validOrderAndBlessed
//     reads that as a transaction it cannot find, and a valid block is
//     permanently invalidated.
//   - Data.Serialize panics on it. Serialize skips index 0 only when Nodes[0]
//     is the coinbase placeholder: it then sets txStartIndex = 1 and never
//     touches Txs[0]. For any other Nodes[0] it sets txStartIndex = 0 while
//     still guarding its own nil check with `i != 0`, so it walks straight into
//     Txs[0].SerializeBytes() on a nil *bt.Tx (IsExtended is nil-safe, so it
//     falls through to Bytes -> toBytesHelper -> Size).
//
// exemptPlaceholderAtZero decides whether a nil entry under a coinbase
// placeholder at index 0 counts as missing, and it is the caller's decision
// rather than something inferred here, because the two callers are protecting
// different things and the right answer differs.
//
// The meta regenerator passes whether this really is the block's first subtree.
// validateSubtree skips node 0 only for sIdx 0, so a later subtree that happens
// to carry the placeholder hash there must still have its node 0 filled: a meta
// with that entry unset makes GetParentTxHashes return nil, which reads
// downstream as "transaction not found" and condemns a valid block. Inferring
// from the hash would exempt it and leave exactly that hole.
//
// The catchup fetcher passes true, because it is not building a meta. It is
// keeping Data.Serialize from walking into a nil *bt.Tx, and Serialize's own
// rule is the hash at index 0 whatever the subtree's position, so rejecting a
// body Serialize would have handled would charge a peer for a response that was
// fine.
//
// What must never happen either way is exempting index 0 unconditionally,
// matching Serialize's literal `i != 0` rather than what it safely tolerates.
// That lets a body through with a count of zero and lands the panic in a
// per-subtree errgroup goroutine that no recover() covers. It is reachable
// without malice: a non-first subtree has no coinbase placeholder, so any block
// whose transaction count is congruent to 1 modulo the subtree size ends with a
// one-node subtree holding a real tx hash at index 0.
//
// The count is driven by the subtree's nodes rather than by len(data.Txs), so a
// body carrying fewer entries than the subtree has nodes counts the shortfall
// instead of reporting the entries it does have as complete. Today those two
// lengths always agree, because serializeFromReader allocates Txs at
// Subtree.Length() before it reads anything — but a Data reaching here by any
// other route would otherwise pass, and the callers' error messages already
// report the denominator as Subtree.Length(). Nil data is the same case with no
// entries at all: every node is unfilled.
//
// A nil subtree has no nodes, so nothing can be unfilled and the answer is
// genuinely zero. That is arithmetic rather than a fail-open default — no body
// can be judged against a subtree that is not there, and neither caller can
// reach it, both holding a subtree they have already deserialized against.
func MissingSubtreeDataTxs(subtree *subtreepkg.Subtree, data *subtreepkg.Data, exemptPlaceholderAtZero bool) int {
	if subtree == nil {
		return 0
	}

	var txs []*bt.Tx
	if data != nil {
		txs = data.Txs
	}

	// One read of the slice header, with both the guard and the index derived
	// from it. Subtree.Length() takes the subtree's own RWMutex and returns
	// len(st.Nodes), but Subtree.ReleaseNodes sets st.Nodes = nil WITHOUT taking
	// that mutex, so length-then-index would be two separate reads of Nodes and a
	// release landing between them turns the index into an out-of-range on a
	// zero-length slice. Both callers reach this with a *Subtree captured out of
	// Block.SubtreeSlices without holding subtreeSlicesMu, which is the window
	// releaseSubtreeNodesLocked runs in, and the panic would land in a
	// per-subtree errgroup goroutine that nothing recovers, killing the node
	// rather than failing the block.
	//
	// This removes the panic. It does not make the read synchronised: a release
	// concurrent with this call is still a data race on st.Nodes, just one that
	// now yields a stale-but-valid slice header rather than a crash. Closing it
	// properly needs the lock to move into go-subtree's ReleaseNodes.
	nodesSlice := subtree.Nodes
	nodes := len(nodesSlice)
	coinbaseAtZero := exemptPlaceholderAtZero && nodes > 0 && nodesSlice[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue)

	missing := 0

	for i := 0; i < nodes; i++ {
		if i < len(txs) && txs[i] != nil {
			continue
		}

		if i == 0 && coinbaseAtZero {
			continue
		}

		missing++
	}

	return missing
}
