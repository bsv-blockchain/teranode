// Package blockvalidation implements block validation for Bitcoin SV nodes in Teranode.
//
// This package provides the core functionality for validating Bitcoin blocks, managing block subtrees,
// and processing transaction metadata. It is designed for high-performance operation at scale,
// supporting features like:
//
// - Concurrent block validation with optimistic mining support
// - Subtree-based block organization and validation
// - Transaction metadata caching and management
// - Automatic chain catchup when falling behind
// - Integration with Kafka for distributed operation
//
// The package exposes gRPC interfaces for block validation operations,
// making it suitable for use in distributed Teranode deployments.
package blockvalidation

import (
	"context"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

// entry represents a cached validation result in the deduplication system.
// It contains the result of a validation operation and synchronization primitives
// to coordinate concurrent access.
type entry struct {
	// err holds any error that occurred during the validation operation
	err error

	// cond is a condition variable for signaling when the entry is ready
	cond *sync.Cond

	// ready indicates whether the validation operation has completed
	ready bool

	// deleteAt specifies the block height at which this entry should be removed from the cache
	deleteAt uint32
}

// DeDuplicator provides concurrent access management for resource-intensive operations.
// It prevents duplicate operations on the same block by caching results and coordinating
// access among multiple goroutines, improving system efficiency and resource utilization.
type DeDuplicator struct {
	// mu protects concurrent access to the cache
	mu sync.Mutex

	// blockHeightRetention defines how many blocks to retain entries for before cleanup
	blockHeightRetention uint32

	// cache maps block hashes to their validation entries
	cache map[chainhash.Hash]*entry
}

// NewDeDuplicator creates a new deduplication manager with the specified retention policy.
// The blockHeightRetention parameter controls how long entries remain in the cache before
// being eligible for cleanup, based on block height.
//
// Parameters:
//   - blockHeightRetention: Number of blocks to keep entries for before cleanup
//
// Returns a configured DeDuplicator instance ready for use.
func NewDeDuplicator(blockHeightRetention uint32) *DeDuplicator {
	return &DeDuplicator{
		mu:                   sync.Mutex{},
		blockHeightRetention: blockHeightRetention,
		cache:                make(map[chainhash.Hash]*entry),
	}
}

// DeDuplicate coordinates concurrent access to resource-intensive operations.
// It ensures that only one goroutine performs the work while others wait for the result,
// providing an efficient concurrency control mechanism for block validation.
//
// The method has three key behaviors:
//   - If the operation is already cached and complete, returns the cached result immediately
//   - If the operation is in progress, waits for completion and returns that result
//   - If the operation is new, executes it and caches the result for others
//
// Parameters:
//   - ctx: Context for the operation
//   - key: Unique identifier (typically a block hash) for deduplication
//   - currentBlockHeight: Current blockchain height for managing cache expiration
//   - fn: The function to execute if the operation isn't already in progress
//
// Returns:
//   - bool: True if the request was handled from cache, false if execution was needed
//   - error: Any error encountered during execution or in cached result
func (u *DeDuplicator) DeDuplicate(ctx context.Context, key chainhash.Hash, currentBlockHeight uint32, fn func() error) (bool, error) {
	start := gocore.CurrentTime()
	stat := gocore.NewStat("DeDuplicator.DeDuplicate")

	u.mu.Lock()

	start = stat.NewStat("1. Get Lock").AddTime(start)

	// Check if resource is in cache and not expired
	if deDupEntry, found := u.cache[key]; found {
		for !deDupEntry.ready {
			deDupEntry.cond.Wait()
		}

		u.mu.Unlock()

		stat.NewStat("2a. Received broadcast").AddTime(start)

		return true, deDupEntry.err
	}

	// Create a new cache entry
	cond := sync.NewCond(&u.mu)

	u.cache[key] = &entry{
		cond:     cond,
		deleteAt: currentBlockHeight + u.blockHeightRetention,
	}

	u.mu.Unlock()

	// Build the resource
	err := fn()

	u.mu.Lock()

	// Update the cache entry
	u.cache[key].err = err
	u.cache[key].ready = true

	cond.Broadcast()

	u.mu.Unlock()

	stat.NewStat("2b. Sent broadcast").AddTime(start)

	return false, err
}
