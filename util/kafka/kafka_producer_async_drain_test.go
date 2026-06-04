package kafka

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestAsyncProducer_DrainsBufferedBatchOnStop pins that a graceful Stop() flushes
// whatever is still in the worker's batch buffer instead of silently dropping it.
//
// Before the fix, Stop() set shuttingDown before closing the publish channel, and
// flushBuffered (including the final drain) bailed early on shuttingDown — so a
// buffered batch, up to a full linger window or batch size, was lost on every
// graceful shutdown. This exercises the real batching loop via the produceHook
// seam (no live broker): with linger and batch size set huge, the messages only
// ever leave through the final drain on Stop.
func TestAsyncProducer_DrainsBufferedBatchOnStop(t *testing.T) {
	// The worker reads package-level prometheus collectors that the production
	// startup path initializes lazily; do the same here.
	initProducerMetrics()

	var (
		mu       sync.Mutex
		produced int
	)

	c := &KafkaAsyncProducer{
		Config: KafkaProducerConfig{
			Topic:          "drain-test",
			Logger:         ulogger.TestLogger{},
			FlushFrequency: time.Hour, // never linger-flush during the test
			FlushMessages:  1_000_000, // never size-flush during the test
		},
		produceHook: func(*Message) {
			mu.Lock()
			produced++
			mu.Unlock()
		},
	}

	const n = 25
	ch := make(chan *Message, n)

	c.channelMu.Lock()
	c.publishChannel = ch
	c.channelMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.publishWg.Add(1)
	go func() {
		defer c.publishWg.Done()
		c.runProducerWorker(ctx, ch)
	}()

	for i := 0; i < n; i++ {
		c.Publish(&Message{Value: []byte{byte(i)}})
	}

	// Stop closes the channel; the worker must drain the channel-resident
	// messages into its buffer and produce all of them in the final drain.
	// Stop blocks on publishWg.Wait until the worker has returned, so `produced`
	// is final once Stop returns.
	require.NoError(t, c.Stop())

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, n, produced, "graceful Stop must drain the buffered batch, not drop it")
}
