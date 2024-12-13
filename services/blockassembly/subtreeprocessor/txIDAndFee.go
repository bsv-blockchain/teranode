package subtreeprocessor

import (
	"sync"
	"sync/atomic"

	"github.com/bitcoin-sv/ubsv/util"
)

type TxIDAndFee struct {
	node util.SubtreeNode
	time int64
	next atomic.Pointer[TxIDAndFee]
}

type TxIDAndFeeBatch struct {
	txs  []*TxIDAndFee
	size int
	mu   sync.Mutex
}

func NewTxIDAndFee(n util.SubtreeNode) *TxIDAndFee {
	return &TxIDAndFee{
		node: n,
	}
}

func NewTxIDAndFeeBatch(size int) *TxIDAndFeeBatch {
	return &TxIDAndFeeBatch{
		txs:  make([]*TxIDAndFee, 0, size),
		size: size,
	}
}

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
