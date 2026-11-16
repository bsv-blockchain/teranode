package validator

import (
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

const txidLockShards = 64

// txidLocks serialises the store work of one transaction id: the create, the hand-off to block
// assembly and the unwind of a shed. Without it a block's InBlock validation of a transaction can
// read the record a concurrent mempool shed has created and answer idempotent success, and the
// shed's unwind then deletes that record. Locks for different ids never contend, and an entry
// lives only while a holder or a waiter references it. The zero value is ready to use.
type txidLocks struct {
	shards [txidLockShards]txidLockShard
}

type txidLockShard struct {
	mu      sync.Mutex
	entries map[chainhash.Hash]*txidLockEntry
}

type txidLockEntry struct {
	mu   sync.Mutex
	refs int
}

// lock blocks until the caller holds the lock for hash and returns the function that releases it.
func (l *txidLocks) lock(hash *chainhash.Hash) func() {
	shard := &l.shards[hash[0]%txidLockShards]

	shard.mu.Lock()

	if shard.entries == nil {
		shard.entries = make(map[chainhash.Hash]*txidLockEntry)
	}

	entry, ok := shard.entries[*hash]
	if !ok {
		entry = &txidLockEntry{}
		shard.entries[*hash] = entry
	}

	entry.refs++

	shard.mu.Unlock()

	entry.mu.Lock()

	return func() {
		entry.mu.Unlock()

		shard.mu.Lock()

		entry.refs--
		if entry.refs == 0 {
			delete(shard.entries, *hash)
		}

		shard.mu.Unlock()
	}
}
