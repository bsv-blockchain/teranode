package deduplicator

import (
	"context"
	"sync"
	"time"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type entry struct {
	err    error
	cond   *sync.Cond
	ready  bool
	expiry time.Time
}

type DeDuplicator struct {
	mu             sync.Mutex
	expiryDuration time.Duration
	cache          map[chainhash.Hash]*entry
}

func New(expiryDuration time.Duration) *DeDuplicator {
	return &DeDuplicator{
		mu:             sync.Mutex{},
		expiryDuration: expiryDuration,
		cache:          make(map[chainhash.Hash]*entry),
	}
}

// DeDuplicate will execute the function fn if the key is not in the cache. If the key is in the cache, it will wait
// for the function to be executed and return the result.
func (u *DeDuplicator) DeDuplicate(ctx context.Context, key chainhash.Hash, fn func() error) (bool, error) {
	start := gocore.CurrentTime()
	stat := gocore.NewStat("DeDuplicator.DeDuplicate")

	u.mu.Lock()

	start = stat.NewStat("1. Get Lock").AddTime(start)

	// Check if resource is in cache and not expired
	if deDupEntry, found := u.cache[key]; found {
		for !deDupEntry.ready {
			deDupEntry.cond.Wait()
		}

		delete(u.cache, key)

		u.mu.Unlock()

		stat.NewStat("2a. Received broadcast").AddTime(start)

		return true, deDupEntry.err
	}

	// Create a new cache entry
	cond := sync.NewCond(&u.mu)

	u.cache[key] = &entry{
		cond:   cond,
		expiry: time.Now().Add(u.expiryDuration),
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
