// Package subtreeprocessor provides functionality for processing transaction subtrees in Teranode.
package subtreeprocessor

import (
	"sync"
	"sync/atomic"

	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

// TxIDAndFee represents a transaction with its associated fee information and linking metadata.
type TxIDAndFee struct {
	node    util.SubtreeNode           // The transaction node containing hash and fee information
	parents []chainhash.Hash           // Slice of parent transaction hashes
	time    int64                      // Timestamp of when the transaction was added
	next    atomic.Pointer[TxIDAndFee] // Pointer to the next transaction in the queue
}

// TxIDAndFeeBatch manages batches of transactions for efficient processing.
type TxIDAndFeeBatch struct {
	txs  []*TxIDAndFee // Slice of transactions in the current batch
	size int           // Maximum size of the batch
	mu   sync.Mutex    // Mutex for thread-safe operations
}

// NewTxIDAndFee creates a new transaction wrapper with the provided node information.
//
// Parameters:
//   - n: The subtree node containing transaction details
//
// Returns:
//   - *TxIDAndFee: A new transaction wrapper
func NewTxIDAndFee(n util.SubtreeNode) *TxIDAndFee {
	return &TxIDAndFee{
		node: n,
	}
}

// NewTxIDAndFeeBatch creates a new batch processor for transactions.
//
// Parameters:
//   - size: The maximum size of the batch
//
// Returns:
//   - *TxIDAndFeeBatch: A new batch processor
func NewTxIDAndFeeBatch(size int) *TxIDAndFeeBatch {
	return &TxIDAndFeeBatch{
		txs:  make([]*TxIDAndFee, 0, size),
		size: size,
	}
}

// Add adds a transaction to the batch, returning the full batch if it reaches capacity.
//
// Parameters:
//   - tx: The transaction to add to the batch
//
// Returns:
//   - *[]*TxIDAndFee: The full batch if capacity is reached, nil otherwise
func (txs *TxIDAndFeeBatch) Add(tx *TxIDAndFee) *[]*TxIDAndFee {
	txs.mu.Lock()
	defer txs.mu.Unlock()

	txs.txs = append(txs.txs, tx)

	if len(txs.txs) >= txs.size {
		TxIDAndFees := txs.txs
		txs.txs = make([]*TxIDAndFee, 0, txs.size)

		return &TxIDAndFees
	}

	return nil
}
