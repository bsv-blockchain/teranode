package txregistry

import (
	"sync"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

type TxRegistry struct {
	mu  sync.Mutex
	txs map[string]string
}

var txr = &TxRegistry{
	txs: make(map[string]string),
}

func AddTag(tx *bt.Tx, tag string) {
	txr.mu.Lock()
	defer txr.mu.Unlock()

	txr.txs[tx.TxIDChainHash().String()] = tag
}

func GetTag(tx *bt.Tx) string {
	if tx == nil {
		return ""
	}

	return GetTagByHash(tx.TxIDChainHash())
}

func GetTagByHash(hash *chainhash.Hash) string {
	txr.mu.Lock()
	defer txr.mu.Unlock()

	tag, ok := txr.txs[hash.String()]
	if !ok {
		return ""
	}

	return tag
}
