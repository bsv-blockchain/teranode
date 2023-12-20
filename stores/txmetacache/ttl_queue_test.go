package txmetacache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDequeue(t *testing.T) {
	q := NewLockFreeTTLQueue(10)

	require.Nil(t, q.dequeue(0), "should have dequeued nil as the queue is empty")
	require.Equal(t, int64(0), q.length(), "queue should be empty")

	q.enqueue(&ttlQueueItem{})

	require.Equal(t, int64(1), q.length(), "queue should have 1 item")

	notExpired := time.Now().Add(-1 * time.Second).UnixMilli()
	require.Nil(t, q.dequeue(notExpired), "should have dequeued nil as the item is not expired")
	require.Equal(t, int64(1), q.length(), "queue should have 1 item")

	expired := time.Now().Add(time.Second).UnixMilli()
	require.NotNil(t, q.dequeue(expired), "should have dequeued an item as the item is expired")
	require.Equal(t, int64(0), q.length(), "queue should be empty")

	q.enqueue(&ttlQueueItem{})
	q.enqueue(&ttlQueueItem{})
	q.enqueue(&ttlQueueItem{})
	q.enqueue(&ttlQueueItem{})
	q.enqueue(&ttlQueueItem{})
	q.enqueue(&ttlQueueItem{})
	q.enqueue(&ttlQueueItem{})
	q.enqueue(&ttlQueueItem{})
	q.enqueue(&ttlQueueItem{})
	q.enqueue(&ttlQueueItem{})
	q.enqueue(&ttlQueueItem{})
	q.enqueue(&ttlQueueItem{})

	require.Equal(t, int64(12), q.length(), "queue should have 12 items")

	require.NotNil(t, q.dequeue(notExpired), "should have dequeued an item as the queue exceeds max length")
	require.NotNil(t, q.dequeue(expired), "should have dequeued an item as the queue exceeds max length and it has expired")
	require.Nil(t, q.dequeue(notExpired), "should NOT have dequeued an item as the queue is no longer full")

}
