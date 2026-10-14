package main

import (
	"sort"
	"sync"
	"time"
)

// latencyHistogram is a simple slice-backed latency recorder. Recording
// is O(1) under a single shared mutex; percentile() sorts on read, so
// it's intended for end-of-run summary, not hot-path sampling.
type latencyHistogram struct {
	mu      sync.Mutex
	samples []time.Duration
}

func newLatencyHistogram() *latencyHistogram {
	return &latencyHistogram{samples: make([]time.Duration, 0, 1<<16)}
}

func (h *latencyHistogram) record(d time.Duration) {
	h.mu.Lock()
	h.samples = append(h.samples, d)
	h.mu.Unlock()
}

func (h *latencyHistogram) count() int64 {
	h.mu.Lock()
	n := int64(len(h.samples))
	h.mu.Unlock()
	return n
}

// percentile returns the nearest-rank p (1..100) latency.
func (h *latencyHistogram) percentile(p int) time.Duration {
	h.mu.Lock()
	n := len(h.samples)
	if n == 0 {
		h.mu.Unlock()
		return 0
	}
	sorted := make([]time.Duration, n)
	copy(sorted, h.samples)
	h.mu.Unlock()
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := (p * n) / 100
	if idx > 0 {
		idx--
	}
	return sorted[idx]
}
