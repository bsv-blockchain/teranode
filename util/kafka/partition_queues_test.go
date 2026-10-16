package kafka

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func recordsFrom(partition int32, from, n int64) []*kgo.Record {
	rs := make([]*kgo.Record, n)
	for i := range rs {
		rs[i] = &kgo.Record{Partition: partition, Offset: from + int64(i)}
	}

	return rs
}

// A slow handler must not turn every fetch into a parked goroutine: on a
// collapsing subtree validator thousands of per-fetch goroutines sat on the
// partition mutex, each holding its fetched records. One worker per partition,
// whatever the number of fetches.
func TestPartitionQueues_OneWorkerPerPartition(t *testing.T) {
	release := make(chan struct{})

	var wg sync.WaitGroup

	q := newPartitionQueues(context.Background(), &wg, func(_ []*kgo.Record, _ int64) {
		<-release
	})

	before := runtime.NumGoroutine()

	const partitions, fetches = 4, 500

	for f := 0; f < fetches; f++ {
		for p := int32(0); p < partitions; p++ {
			q.enqueue(p, recordsFrom(p, int64(f), 1), 0)
		}
	}

	require.LessOrEqual(t, runtime.NumGoroutine()-before, partitions)

	close(release)
	q.close()
	wg.Wait()
}

// Records of one partition are handed over strictly in offset order across
// fetches. sync.Mutex is not FIFO, so the old per-fetch goroutines could take
// a later fetch before an earlier one and commit past unprocessed records.
func TestPartitionQueues_InOrderAcrossFetches(t *testing.T) {
	var (
		mu   sync.Mutex
		seen = map[int32][]int64{}
		wg   sync.WaitGroup
	)

	q := newPartitionQueues(context.Background(), &wg, func(records []*kgo.Record, _ int64) {
		time.Sleep(10 * time.Microsecond) // let fetches queue up behind the worker

		mu.Lock()
		defer mu.Unlock()

		for _, r := range records {
			seen[r.Partition] = append(seen[r.Partition], r.Offset)
		}
	})

	const partitions, fetches, perFetch = 3, 300, 5

	for f := 0; f < fetches; f++ {
		for p := int32(0); p < partitions; p++ {
			q.enqueue(p, recordsFrom(p, int64(f*perFetch), perFetch), 0)
		}
	}

	q.close()
	wg.Wait()

	for p := int32(0); p < partitions; p++ {
		require.Len(t, seen[p], fetches*perFetch)

		for i, off := range seen[p] {
			require.Equal(t, int64(i), off, "partition %d out of order", p)
		}
	}
}

// close lets every worker finish what was already queued, so the consumer's
// final commit covers it; cancellation stops the workers without draining.
func TestPartitionQueues_CloseDrainsCancelStops(t *testing.T) {
	var (
		mu        sync.Mutex
		processed int
		wg        sync.WaitGroup
	)

	q := newPartitionQueues(context.Background(), &wg, func(records []*kgo.Record, _ int64) {
		mu.Lock()
		processed += len(records)
		mu.Unlock()
	})

	for f := 0; f < 100; f++ {
		q.enqueue(0, recordsFrom(0, int64(f), 1), 0)
	}

	q.close()
	wg.Wait()
	require.Equal(t, 100, processed)

	ctx, cancel := context.WithCancel(context.Background())

	block := make(chan struct{})

	var wg2 sync.WaitGroup

	q2 := newPartitionQueues(ctx, &wg2, func(_ []*kgo.Record, _ int64) { <-block })
	q2.enqueue(0, recordsFrom(0, 0, 1), 0)
	q2.enqueue(0, recordsFrom(0, 1, 1), 0)

	cancel()
	close(block)

	done := make(chan struct{})
	go func() { wg2.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("workers did not stop after cancellation")
	}
}
