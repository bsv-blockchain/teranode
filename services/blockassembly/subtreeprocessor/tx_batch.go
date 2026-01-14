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
	nodes      []subtree.Node          // All transaction nodes in this batch
	txInpoints []*subtree.TxInpoints   // Corresponding inpoints for each node
	time       int64                   // Single timestamp for the entire batch
	next       atomic.Pointer[TxBatch] // Pointer to next batch in queue
}
