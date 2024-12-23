package model

import (
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils/expiringmap"
)

type utxoSetCache struct {
	mu sync.RWMutex
	l  ulogger.Logger
	m  *expiringmap.ExpiringMap[chainhash.Hash, *UTXOSet]
}

var UTXOSetCache = &utxoSetCache{
	l: ulogger.NewZeroLogger("UTXOSetCache"),
	m: expiringmap.New[chainhash.Hash, *UTXOSet](30 * time.Minute),
}

func (c *utxoSetCache) Get(hash chainhash.Hash) (*UTXOSet, bool) {
	if hash.String() == "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f" || hash.String() == "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206" {
		// This is the genesis block, we can return an empty UTXOSet
		return NewUTXOSet(c.l, &hash), true
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	utxoSet, ok := c.m.Get(hash)
	if !ok {
		return nil, false
	}

	return utxoSet, true
}

func (c *utxoSetCache) Put(hash chainhash.Hash, utxoSet *UTXOSet) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.m.Set(hash, utxoSet)
}
