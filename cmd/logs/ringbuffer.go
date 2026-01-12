package logs

import (
	"sync"
)

// RingBuffer is a thread-safe circular buffer for storing log entries
type RingBuffer struct {
	entries  []LogEntry
	capacity int
	head     int // Next write position
	size     int // Current number of entries
	mu       sync.RWMutex
}

// NewRingBuffer creates a new ring buffer with the specified capacity
func NewRingBuffer(capacity int) *RingBuffer {
	if capacity <= 0 {
		capacity = 10000
	}
	return &RingBuffer{
		entries:  make([]LogEntry, capacity),
		capacity: capacity,
	}
}

// Push adds a new entry to the buffer
func (rb *RingBuffer) Push(entry LogEntry) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.entries[rb.head] = entry
	rb.head = (rb.head + 1) % rb.capacity

	if rb.size < rb.capacity {
		rb.size++
	}
}

// PushMany adds multiple entries to the buffer
func (rb *RingBuffer) PushMany(entries []LogEntry) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	for _, entry := range entries {
		rb.entries[rb.head] = entry
		rb.head = (rb.head + 1) % rb.capacity

		if rb.size < rb.capacity {
			rb.size++
		}
	}
}

// Entries returns all entries in chronological order
func (rb *RingBuffer) Entries() []LogEntry {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	result := make([]LogEntry, rb.size)
	if rb.size == 0 {
		return result
	}

	// Calculate the start position (oldest entry)
	start := 0
	if rb.size == rb.capacity {
		start = rb.head // Oldest entry is at head when full
	}

	for i := 0; i < rb.size; i++ {
		idx := (start + i) % rb.capacity
		result[i] = rb.entries[idx]
	}

	return result
}

// Size returns the current number of entries in the buffer
func (rb *RingBuffer) Size() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.size
}

// Capacity returns the maximum capacity of the buffer
func (rb *RingBuffer) Capacity() int {
	return rb.capacity
}

// Clear removes all entries from the buffer
func (rb *RingBuffer) Clear() {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	rb.head = 0
	rb.size = 0
}

// Last returns the most recent n entries in chronological order
func (rb *RingBuffer) Last(n int) []LogEntry {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if n <= 0 || rb.size == 0 {
		return []LogEntry{}
	}

	if n > rb.size {
		n = rb.size
	}

	result := make([]LogEntry, n)

	// Calculate the start position for the last n entries
	start := 0
	if rb.size == rb.capacity {
		start = (rb.head - n + rb.capacity) % rb.capacity
	} else {
		start = rb.size - n
	}

	for i := 0; i < n; i++ {
		idx := (start + i) % rb.capacity
		result[i] = rb.entries[idx]
	}

	return result
}
