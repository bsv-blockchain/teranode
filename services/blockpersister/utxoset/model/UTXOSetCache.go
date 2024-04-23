package model

import (
	"sync"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
)

type utxoSetCache struct {
	mu sync.RWMutex
	l  ulogger.Logger
	m  map[chainhash.Hash]*UTXOSet
}

var UTXOSetCache = &utxoSetCache{
	l: ulogger.NewZeroLogger("UTXOSetCache"),
	m: make(map[chainhash.Hash]*UTXOSet),
}

func (c *utxoSetCache) Get(hash chainhash.Hash) (*UTXOSet, bool) {
	if hash.String() == "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f" {
		// This is the genesis block, we can return an empty UTXOSet
		return NewUTXOSet(c.l, &hash), true
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	utxoSet, ok := c.m[hash]
	if !ok {
		return nil, false
	}

	return utxoSet, true
}

func (c *utxoSetCache) Put(hash chainhash.Hash, utxoSet *UTXOSet) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.m[hash] = utxoSet
}
