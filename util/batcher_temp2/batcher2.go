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
		b.batch = make([]*T, 0, n)
		b.ch = make(chan *ItemWithSize[T], n*64)
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

func WithBatchFunc[T any](fn func(batch []*T)) Option[T] {
	return func(b *Batcher2[T]) {
		b.fn = fn
	}
}

func WithBackground[T any](background bool) Option[T] {
	return func(b *Batcher2[T]) {
		b.background = background
	}
}

type ItemWithSize[T any] struct {
	item *T
	size int
}

type Batcher2[T any] struct {
	fn           func([]*T)
	maxItems     int
	maxBytes     int // Maximum size in bytes before triggering the batch
	currentBytes int // Current size in bytes of the batch
	timeout      time.Duration
	batch        []*T
	ch           chan *ItemWithSize[T]
	triggerCh    chan struct{}
	background   bool
}

// New creates a new Batcher that will invoke the provided function when the batch size is reached.
// The size is the maximum number of items that can be batched before processing the batch.
// The timeout is the duration that will be waited before processing the batch.
func New[T any](options ...Option[T]) *Batcher2[T] {
	b := &Batcher2[T]{
		triggerCh: make(chan struct{}),
	}

	// Apply each option to configure the Batcher2 instance
	for _, option := range options {
		option(b)
	}

	// Ensure channels are initialized with default sizes if not set
	if b.maxItems > 0 && cap(b.ch) == 0 {
		b.ch = make(chan *ItemWithSize[T], b.maxItems*64)
	}

	if len(b.batch) == 0 && b.maxItems > 0 {
		b.batch = make([]*T, 0, b.maxItems)
	}

	if b.timeout == 0 {
		// Set timeout large enough to not trigger
		b.timeout = 24 * time.Hour
	}

	if b.fn == nil {
		b.fn = func(batch []*T) {
			// Do nothing
		}
	}

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

	b.ch <- &ItemWithSize[T]{
		item: item,
		size: size,
	}
}

// Trigger will force the batch to be processed immediately.
func (b *Batcher2[T]) Trigger() {
	b.triggerCh <- struct{}{}
}

func (b *Batcher2[T]) worker() {
	for {
		expire := time.After(b.timeout)

		for {
			select {
			case itemWithSize := <-b.ch:
				b.batch = append(b.batch, itemWithSize.item)
				b.currentBytes += itemWithSize.size

				// Check if either size or maxBytes are exceeded
				if len(b.batch) == b.maxItems || b.currentBytes >= b.maxBytes {
					goto saveBatch
				}

			case <-expire:
				goto saveBatch

			case <-b.triggerCh:
				goto saveBatch
			}
		}
	saveBatch:
		if len(b.batch) > 0 {
			batch := b.batch

			if b.background {
				go b.fn(batch)
			} else {
				b.fn(batch)
			}

			b.batch = make([]*T, 0, b.maxItems)
			b.currentBytes = 0 // Reset the current size
		}
	}
}
