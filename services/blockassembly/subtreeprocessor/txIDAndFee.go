package subtreeprocessor

import (
	"sync"

	"github.com/libsv/go-bt/v2/chainhash"
)

type txIDAndFee struct {
	txID        *chainhash.Hash
	fee         uint64
	sizeInBytes uint64
	waitCh      chan struct{}
}

type txIDAndFeeBatch struct {
	txs  []txIDAndFee
	size int
	mu   sync.Mutex
}

func newTxIDAndFeeBatch(size int) *txIDAndFeeBatch {
	return &txIDAndFeeBatch{
		txs:  make([]txIDAndFee, 0, size),
		size: size,
	}
}

func (txs *txIDAndFeeBatch) add(tx *txIDAndFee) *[]txIDAndFee {
	txs.mu.Lock()
	defer txs.mu.Unlock()

	txs.txs = append(txs.txs, *tx)

	if len(txs.txs) >= txs.size {
		txIDAndFees := txs.txs
		txs.txs = make([]txIDAndFee, 0, txs.size)

		return &txIDAndFees
	}

	return nil
}
