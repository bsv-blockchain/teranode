package worker

import (
	"sync"
)

type RollingCache struct {
	items map[string]struct{}
	queue []string
	size  int
	mu    sync.Mutex
}

func NewRollingCache(size int) *RollingCache {
	return &RollingCache{
		items: make(map[string]struct{}),
		queue: make([]string, 0, size),
		size:  size,
	}
}

func (c *RollingCache) Add(txID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// If the txId already exists, no need to do anything
	if _, exists := c.items[txID]; exists {
		return
	}

	// If the cache is full, remove the oldest item
	if len(c.queue) == c.size {
		oldest := c.queue[0]
		c.queue = c.queue[1:]
		delete(c.items, oldest)
	}

	// Add the new item
	c.items[txID] = struct{}{}
	c.queue = append(c.queue, txID)
}

func (c *RollingCache) Contains(txID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, exists := c.items[txID]

	return exists
}
