package txmetacache

import (
	"math/rand/v2"
	"sync/atomic"
)

// stripedCounterStripes is the number of cache lines a stripedCounter spreads
// its adds over.
const stripedCounterStripes = 64

// stripedCounter is a counter for hot paths that many goroutines add to at
// once. A single atomic would make every add move one cache line between
// cores (and between dies on large machines), which at Kafka ingest rates
// costs more than the work being counted. Adds go to a random stripe, each on
// its own cache line; Load sums them.
type stripedCounter struct {
	stripes [stripedCounterStripes]struct {
		n atomic.Uint64
		_ [56]byte
	}
}

// Add adds n to the counter.
func (c *stripedCounter) Add(n uint64) {
	c.stripes[rand.Uint64()%stripedCounterStripes].n.Add(n) //nolint:gosec // stripe choice, not security
}

// Load returns the sum of all adds.
func (c *stripedCounter) Load() uint64 {
	var sum uint64

	for i := range c.stripes {
		sum += c.stripes[i].n.Load()
	}

	return sum
}

// reset sets the counter to zero. It must not race with Add.
func (c *stripedCounter) reset() {
	for i := range c.stripes {
		c.stripes[i].n.Store(0)
	}
}
