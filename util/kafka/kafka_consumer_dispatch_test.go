package kafka

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func fetchesOf(parts ...kgo.FetchPartition) kgo.Fetches {
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "t", Partitions: parts}}}}
}

func collectingQueues(t *testing.T) (*partitionQueues, func() map[int32][]int64) {
	t.Helper()

	var (
		mu   sync.Mutex
		seen = map[int32][]int64{}
		wg   sync.WaitGroup
	)

	q := newPartitionQueues(context.Background(), &wg, func(records []*kgo.Record, _ int64) {
		mu.Lock()
		defer mu.Unlock()

		for _, r := range records {
			seen[r.Partition] = append(seen[r.Partition], r.Offset)
		}
	})

	return q, func() map[int32][]int64 {
		q.close()
		wg.Wait()

		return seen
	}
}

// A fetch error on one partition (e.g. not-leader during a broker roll) must
// not discard the records fetched for the others: franz-go has already moved
// past them, so dropping them here loses them for good once later offsets are
// committed.
func TestDispatchFetches_PartitionErrorKeepsOtherPartitions(t *testing.T) {
	k := &KafkaConsumerGroup{Config: KafkaConsumerConfig{Logger: ulogger.TestLogger{}}}
	q, wait := collectingQueues(t)

	shutdown := k.dispatchFetches(fetchesOf(
		kgo.FetchPartition{Partition: 0, Err: errors.NewProcessingError("not leader for partition")},
		kgo.FetchPartition{Partition: 1, Records: recordsFrom(1, 10, 3)},
	), q)

	require.False(t, shutdown)
	require.Equal(t, []int64{10, 11, 12}, wait()[1])
}

// Shutdown errors still stop the loop, without dispatching anything: unmarked
// records stay uncommitted and are re-delivered.
func TestDispatchFetches_ShutdownErrorStops(t *testing.T) {
	k := &KafkaConsumerGroup{Config: KafkaConsumerConfig{Logger: ulogger.TestLogger{}}}

	for _, shutdownErr := range []error{context.Canceled, kgo.ErrClientClosed} {
		q, wait := collectingQueues(t)

		shutdown := k.dispatchFetches(fetchesOf(
			kgo.FetchPartition{Partition: 0, Err: shutdownErr},
			kgo.FetchPartition{Partition: 1, Records: recordsFrom(1, 0, 2)},
		), q)

		require.True(t, shutdown)
		require.Empty(t, wait())
	}
}
