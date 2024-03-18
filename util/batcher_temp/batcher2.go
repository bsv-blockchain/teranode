package batcher

import (
	"time"
)

// Batcher2 is a utility that batches items together and then invokes the provided function
// on that whenever it reaches the specified size or the timeout is reached.
type Batcher2[T any] struct {
	fn         func([]*T)
	size       int
	timeout    time.Duration
	batch      []*T
	ch         chan *T
	background bool
}

// New creates a new Batcher that will invoke the provided function when the batch size is reached.
// The size is the maximum number of items that can be batched before processing the batch.
// The timeout is the duration that will be waited before processing the batch.
func New[T any](size int, timeout time.Duration, fn func(batch []*T), background bool) *Batcher2[T] {
	b := &Batcher2[T]{
		fn:         fn,
		size:       size,
		timeout:    timeout,
		batch:      make([]*T, 0, size),
		ch:         make(chan *T),
		background: background,
	}

	go b.worker()

	return b
}

// Put adds an item to the batch. If the batch is full, or the timeout is reached
// the batch will be processed.
func (b *Batcher2[T]) Put(item *T) {
	b.ch <- item
}

func (b *Batcher2[T]) worker() {
	for {
		expire := time.After(b.timeout)
		for {
			select {
			case item := <-b.ch:
				b.batch = append(b.batch, item)

				if len(b.batch) == b.size {
					goto saveBatch
				}

			case <-expire:
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

			b.batch = make([]*T, 0, b.size)
		}
	}
}
