package subtreeprocessor

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
)

// TxMap defines the interface for transaction map implementations.
// Both ShardedTxMap and the original SyncedMap can satisfy this interface.
type TxMap interface {
	// SetIfNotExists atomically sets the value for a key if it doesn't already exist.
	SetIfNotExists(key chainhash.Hash, value subtreepkg.TxInpoints) (subtreepkg.TxInpoints, bool)

	// Get retrieves the value for a key.
	Get(key chainhash.Hash) (subtreepkg.TxInpoints, bool)

	// Delete removes a key from the map.
	Delete(key chainhash.Hash) bool

	// Exists checks if a key exists in the map.
	Exists(key chainhash.Hash) bool

	// Clear removes all entries from the map.
	Clear() bool

	// Length returns the total number of entries.
	Length() int

	// Keys returns all keys from the map.
	Keys() []chainhash.Hash

	// Iterate calls the provided function for each key-value pair.
	Iterate(f func(key chainhash.Hash, value subtreepkg.TxInpoints) bool)
}

// Verify that both types implement the TxMap interface
var _ TxMap = (*ShardedTxMap)(nil)
var _ TxMap = (*txmap.SyncedMap[chainhash.Hash, subtreepkg.TxInpoints])(nil)

// numShards is the number of shards to use for the ShardedTxMap.
// 1024 provides good distribution while keeping memory overhead reasonable.
const numShards = 1024

// ShardedTxMap is a sharded implementation of a synchronized map for transaction tracking.
// It distributes keys across multiple SyncedMap instances to reduce lock contention.
// This is particularly beneficial for high-throughput scenarios where many goroutines
// are concurrently adding transactions.
type ShardedTxMap struct {
	shards [numShards]*txmap.SyncedMap[chainhash.Hash, subtreepkg.TxInpoints]
}

// NewShardedTxMap creates a new ShardedTxMap with 1024 shards.
func NewShardedTxMap() *ShardedTxMap {
	stm := &ShardedTxMap{}
	for i := range stm.shards {
		stm.shards[i] = txmap.NewSyncedMap[chainhash.Hash, subtreepkg.TxInpoints]()
	}
	return stm
}

// shardIndex returns the shard index for a given key.
// Since chainhash.Hash is already a cryptographic hash (double SHA256),
// the bytes are uniformly distributed. We use the first 2 bytes directly
// as the shard index, masked to numShards-1 (1023).
func (stm *ShardedTxMap) shardIndex(key chainhash.Hash) uint16 {
	return (uint16(key[0])<<8 | uint16(key[1])) & (numShards - 1)
}

// SetIfNotExists atomically sets the value for a key if it doesn't already exist.
// Returns the value (existing or new) and a boolean indicating if the value was set.
func (stm *ShardedTxMap) SetIfNotExists(key chainhash.Hash, value subtreepkg.TxInpoints) (subtreepkg.TxInpoints, bool) {
	return stm.shards[stm.shardIndex(key)].SetIfNotExists(key, value)
}

// Get retrieves the value for a key.
// Returns the value and a boolean indicating if the key was found.
func (stm *ShardedTxMap) Get(key chainhash.Hash) (subtreepkg.TxInpoints, bool) {
	return stm.shards[stm.shardIndex(key)].Get(key)
}

// Delete removes a key from the map.
// Returns true if the key was deleted.
func (stm *ShardedTxMap) Delete(key chainhash.Hash) bool {
	return stm.shards[stm.shardIndex(key)].Delete(key)
}

// Exists checks if a key exists in the map.
func (stm *ShardedTxMap) Exists(key chainhash.Hash) bool {
	return stm.shards[stm.shardIndex(key)].Exists(key)
}

// Clear removes all entries from all shards.
func (stm *ShardedTxMap) Clear() bool {
	for _, shard := range stm.shards {
		shard.Clear()
	}
	return true
}

// Length returns the total number of entries across all shards.
func (stm *ShardedTxMap) Length() int {
	total := 0
	for _, shard := range stm.shards {
		total += shard.Length()
	}
	return total
}

// Keys returns all keys from all shards.
// Note: This operation is expensive and should be used sparingly.
func (stm *ShardedTxMap) Keys() []chainhash.Hash {
	var allKeys []chainhash.Hash
	for _, shard := range stm.shards {
		allKeys = append(allKeys, shard.Keys()...)
	}
	return allKeys
}

// Iterate calls the provided function for each key-value pair across all shards.
// The iteration stops if the function returns false.
// Note: The order of iteration is not guaranteed.
func (stm *ShardedTxMap) Iterate(f func(key chainhash.Hash, value subtreepkg.TxInpoints) bool) {
	for _, shard := range stm.shards {
		shouldContinue := true
		shard.Iterate(func(key chainhash.Hash, value subtreepkg.TxInpoints) bool {
			shouldContinue = f(key, value)
			return shouldContinue
		})
		if !shouldContinue {
			return
		}
	}
}
