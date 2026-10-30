package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/require"
)

// slowStopKafkaProducer is a KafkaAsyncProducerI whose Stop takes a fixed time, like a final
// flush to a broker, and records whether it finished.
type slowStopKafkaProducer struct {
	recordingKafkaProducer
	delay time.Duration
}

func (s *slowStopKafkaProducer) Stop() error {
	time.Sleep(s.delay)

	return s.recordingKafkaProducer.Stop()
}

// TestServerStop_HungCommitTailLeavesCloseABudget covers the cost of registering commitBlock with
// backgroundTasks: a commit tail held by a hung store keeps BlockValidation.Wait running to its
// bound. If Wait got Stop's whole deadline, the race against Close that follows it would start with
// ctx already done, and Stop would return before the invalid-block producer's final flush finished.
//
// Here a registered task never ends, the stop deadline is 400ms and the producer's Stop takes 30ms.
// Stop must return with the producer stopped, inside its deadline.
//
// Mutation proof: passing ctx straight to Wait instead of stopWaitContext's ctx makes Stop return
// on ctx.Done with the flush still running, and the stop-count assertion fails.
func TestServerStop_HungCommitTailLeavesCloseABudget(t *testing.T) {
	prod := &slowStopKafkaProducer{delay: 30 * time.Millisecond}

	bv := &BlockValidation{
		logger:                    ulogger.TestLogger{},
		invalidBlockKafkaProducer: prod,
		lastValidatedBlocks:       expiringmap.New[chainhash.Hash, *model.Block](time.Minute),
		blockExistsCache:          expiringmap.New[chainhash.Hash, bool](time.Minute),
		subtreeExistsCache:        expiringmap.New[chainhash.Hash, bool](time.Minute),
	}

	// A commit tail that a hung store never lets finish.
	done, ok := bv.trackBackgroundTask()
	require.True(t, ok)
	t.Cleanup(done)

	consumer := &mockKafkaConsumer{}
	consumer.On("Close").Return(nil)

	server := &Server{
		logger:              ulogger.TestLogger{},
		blockValidation:     bv,
		kafkaConsumerClient: consumer,
	}

	const stopBudget = 400 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), stopBudget)
	defer cancel()

	start := time.Now()
	require.NoError(t, server.Stop(ctx))
	elapsed := time.Since(start)

	require.Equal(t, 1, prod.stopCount(), "Close must finish the producer flush before Stop returns")
	require.Less(t, elapsed, stopBudget+100*time.Millisecond, "Stop must stay inside its deadline")
	consumer.AssertExpectations(t)
}

// TestStopWaitContext pins the reserve: a quarter of the time left, capped at stopCloseReserveMax,
// and no change for a ctx without a deadline.
func TestStopWaitContext(t *testing.T) {
	t.Run("no deadline", func(t *testing.T) {
		waitCtx, cancel := stopWaitContext(context.Background())
		defer cancel()

		_, ok := waitCtx.Deadline()
		require.False(t, ok)
	})

	t.Run("short deadline keeps a quarter", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()

		outer, _ := ctx.Deadline()

		waitCtx, cancelWait := stopWaitContext(ctx)
		defer cancelWait()

		inner, ok := waitCtx.Deadline()
		require.True(t, ok)
		require.InDelta(t, float64(time.Second), float64(outer.Sub(inner)), float64(50*time.Millisecond))
	})

	t.Run("long deadline caps the reserve", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		outer, _ := ctx.Deadline()

		waitCtx, cancelWait := stopWaitContext(ctx)
		defer cancelWait()

		inner, ok := waitCtx.Deadline()
		require.True(t, ok)
		require.Equal(t, stopCloseReserveMax, outer.Sub(inner))
	})
}
