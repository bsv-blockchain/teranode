package blockvalidation

import (
	"context"
	"sync"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type entry struct {
	err      error
	cond     *sync.Cond
	ready    bool
	deleteAt uint32
}

type DeDuplicator struct {
	mu             sync.Mutex
	blockRetention uint32
	cache          map[chainhash.Hash]*entry
}

func NewDeDuplicator(blockRetention uint32) *DeDuplicator {
	return &DeDuplicator{
		mu:             sync.Mutex{},
		blockRetention: blockRetention,
		cache:          make(map[chainhash.Hash]*entry),
	}
}

// DeDuplicate will execute the function fn if the key is not in the cache. If the key is in the cache, it will wait
// for the function to be executed and return the result.
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
		deleteAt: currentBlockHeight + u.blockRetention,
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
