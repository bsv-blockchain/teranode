package batcher2

import (
	"time"
)

// Batcher2 is a utility that batches items together and then invokes the provided function
// on that whenever it reaches the specified size or the timeout is reached.

type Option[T any] func(*Batcher2[T])

func WithMaxItems[T any](n int) Option[T] {
	return func(b *Batcher2[T]) {
		b.maxItems = n
	}
}

func WithMaxBytes[T any](n int) Option[T] {
	return func(b *Batcher2[T]) {
		b.maxBytes = n
	}
}

func WithTimeout[T any](timeout time.Duration) Option[T] {
	return func(b *Batcher2[T]) {
		b.timeout = timeout
	}
}

func WithBackground[T any](background bool) Option[T] {
	return func(b *Batcher2[T]) {
		b.background = background
	}
}

type Batcher2[T any] struct {
	fn           func([]*T)
	maxItems     int
	maxBytes     int // Maximum size in bytes before triggering the batch
	currentBytes int // Current size in bytes of the batch
	timeout      time.Duration
	batch        []*T
	ch           chan struct {
		item *T
		size int
	}
	triggerCh  chan struct{}
	background bool
}

// New creates a new Batcher that will invoke the provided function when the batch size is reached.
// The size is the maximum number of items that can be batched before processing the batch.
// The timeout is the duration that will be waited before processing the batch.
func New[T any](fn func(batch []*T), options ...Option[T]) *Batcher2[T] {
	b := &Batcher2[T]{
		fn:        fn,
		triggerCh: make(chan struct{}),
	}

	// Apply each option to configure the Batcher2 instance
	for _, option := range options {
		option(b)
	}

	if b.maxItems == 0 {
		// Default to 1 if not set
		b.maxItems = 1
	}

	b.batch = make([]*T, 0, b.maxItems)

	b.ch = make(chan struct {
		item *T
		size int
	}, b.maxItems*64)

	go b.worker()

	return b
}

// Put adds an item to the batch. If the batch is full, or the timeout is reached
// the batch will be processed.
func (b *Batcher2[T]) Put(item *T, payloadSize ...int) {
	var size int
	if len(payloadSize) > 0 {
		size = payloadSize[0]
	}

	b.ch <- struct {
		item *T
		size int
	}{
		item,
		size,
	}
}

// Trigger will force the batch to be processed immediately.
func (b *Batcher2[T]) Trigger() {
	b.triggerCh <- struct{}{}
}

func (b *Batcher2[T]) worker() {
	var expireTimer *time.Timer

	var expire <-chan time.Time

	for {
		if b.timeout > 0 {
			if expireTimer == nil {
				expireTimer = time.NewTimer(b.timeout)
			} else {
				expireTimer.Reset(b.timeout)
			}

			expire = expireTimer.C
		}

		for {
			select {
			case msg := <-b.ch:
				b.batch = append(b.batch, msg.item)
				b.currentBytes += msg.size

				// Check if either size or maxBytes are exceeded.  Skip maxBytes check if it is 0
				if len(b.batch) == b.maxItems || (b.maxBytes > 0 && b.currentBytes >= b.maxBytes) {
					goto saveBatch
				}

			case <-expire: // Only included if timeout > 0
				goto saveBatch

			case <-b.triggerCh:
				goto saveBatch
			}
		}
	saveBatch:
		if expireTimer != nil {
			expireTimer.Stop()
		}

		if len(b.batch) > 0 {
			// Process the batch
			if b.background {
				// Make a copy of the batch to avoid race conditions if processing is asynchronous
				batch := make([]*T, len(b.batch))
				copy(batch, b.batch)

				go b.fn(batch)
			} else {
				b.fn(b.batch)
			}

			// Clear batch after processing
			b.batch = make([]*T, 0, b.maxItems)
			b.currentBytes = 0 // Reset the current size
		}
	}
}
