package subtreeprocessor

import (
	"sync"
	"sync/atomic"

	"github.com/bitcoin-sv/ubsv/util"
)

type txIDAndFee struct {
	node   *util.SubtreeNode
	waitCh chan struct{}
	next   atomic.Pointer[txIDAndFee]
}

type txIDAndFeeBatch struct {
	txs  []*txIDAndFee
	size int
	mu   sync.Mutex
}

func newTxIDAndFeeBatch(size int) *txIDAndFeeBatch {
	return &txIDAndFeeBatch{
		txs:  make([]*txIDAndFee, 0, size),
		size: size,
	}
}

func (txs *txIDAndFeeBatch) add(tx *txIDAndFee) *[]*txIDAndFee {
	txs.mu.Lock()
	defer txs.mu.Unlock()

	txs.txs = append(txs.txs, tx)

	if len(txs.txs) >= txs.size {
		txIDAndFees := txs.txs
		txs.txs = make([]*txIDAndFee, 0, txs.size)

		return &txIDAndFees
	}

	return nil
}
