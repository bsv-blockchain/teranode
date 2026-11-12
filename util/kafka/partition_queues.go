package kafka

import (
	"context"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// partitionQueues hands fetched records to one long-lived worker goroutine per
// partition, through a FIFO queue per partition.
//
// enqueue never blocks and never pauses fetching: the puller keeps calling
// PollFetches at full speed (the txmeta cache consumer must never be held
// back). Within a partition, batches are processed strictly in fetch order, so
// offsets are marked/committed in order. Across partitions processing stays
// parallel.
//
// This replaces a goroutine per partition per fetch serialised by a
// per-partition sync.Mutex. That shape parked one goroutine per fetch, each
// holding its records, whenever a handler slowed down (thousands on a
// collapsing subtree validator), and since sync.Mutex is not FIFO a later fetch
// could be processed before an earlier one.
type partitionQueues struct {
	ctx     context.Context
	wg      *sync.WaitGroup
	process func(records []*kgo.Record, hwm int64)
	closing chan struct{}
	queues  map[int32]*partitionQueue // only touched by the puller goroutine
}

type partitionBatch struct {
	records []*kgo.Record
	hwm     int64
}

type partitionQueue struct {
	mu      sync.Mutex
	batches []partitionBatch
	wake    chan struct{}
}

// newPartitionQueues returns a dispatcher whose workers call process for each
// batch. Each worker is tracked on wg; workers return once ctx is done, or once
// close has been called and their queue is empty.
func newPartitionQueues(ctx context.Context, wg *sync.WaitGroup, process func(records []*kgo.Record, hwm int64)) *partitionQueues {
	return &partitionQueues{
		ctx:     ctx,
		wg:      wg,
		process: process,
		closing: make(chan struct{}),
		queues:  make(map[int32]*partitionQueue),
	}
}

// enqueue appends a fetched batch to its partition's queue. It must only be
// called from the single puller goroutine, and not after close.
func (d *partitionQueues) enqueue(partition int32, records []*kgo.Record, hwm int64) {
	q, ok := d.queues[partition]
	if !ok {
		q = &partitionQueue{wake: make(chan struct{}, 1)}
		d.queues[partition] = q

		d.wg.Add(1)

		go d.work(q)
	}

	q.mu.Lock()
	q.batches = append(q.batches, partitionBatch{records: records, hwm: hwm})
	q.mu.Unlock()

	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// close tells the workers to return once their queue is empty. On consumer
// shutdown the context is already cancelled, so workers stop after the record
// in hand and drop what is still queued; those records were never marked, so
// they stay uncommitted and are re-delivered.
func (d *partitionQueues) close() {
	close(d.closing)
}

func (d *partitionQueues) work(q *partitionQueue) {
	defer d.wg.Done()

	for {
		q.mu.Lock()

		if len(q.batches) == 0 {
			q.mu.Unlock()

			select {
			case <-d.ctx.Done():
				return
			case <-d.closing:
				q.mu.Lock()
				empty := len(q.batches) == 0
				q.mu.Unlock()

				if empty {
					return
				}
			case <-q.wake:
			}

			continue
		}

		b := q.batches[0]
		q.batches[0] = partitionBatch{} // let the records be collected once processed
		q.batches = q.batches[1:]
		q.mu.Unlock()

		if d.ctx.Err() != nil {
			return
		}

		d.process(b.records, b.hwm)
	}
}
