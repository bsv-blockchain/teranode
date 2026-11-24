// Package subtreeprocessor provides functionality for processing transaction subtrees in Teranode.
package subtreeprocessor

import (
	"sync/atomic"

	"github.com/bsv-blockchain/go-subtree"
)

// TxBatch represents a batch of transactions as a single queue node.
// This structure enables efficient batch processing in the lock-free queue,
// allowing multiple transactions to be enqueued and dequeued as a single unit.
// This significantly reduces atomic operations and improves throughput at high volumes.
type TxBatch struct {
	nodes      []subtree.Node        // All transaction nodes in this batch
	txInpoints []*subtree.TxInpoints // Corresponding inpoints for each node
	// validatedHeights holds, per node, the block height its transaction was validated
	// at, so the processor can drop one validated under other rules than the next block's.
	// Nil, or 0 for a node, means untagged: the node is admitted as before.
	validatedHeights []uint32
	// checkedSeq is the newest kept block set (recent_mined_sets.go) the batch was checked
	// against when it arrived, so placement checks it only against sets kept after that. 0 for a
	// batch queued without that check.
	checkedSeq uint64
	time       int64                   // Single timestamp for the entire batch
	next       atomic.Pointer[TxBatch] // Pointer to next batch in queue
}
