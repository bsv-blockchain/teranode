package aerospike

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSpendQueuePermitAcquisition(t *testing.T) {
	t.Run("AcquireAndReleaseWithoutTimeout", func(t *testing.T) {
		s := &Store{
			spendQueueSem:       make(chan struct{}, 5),
			spendEnqueueTimeout: 0, // No timeout
		}

		ctx := context.Background()

		// Acquire permit
		err := s.acquireSpendPermit(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, len(s.spendQueueSem), "one permit should be in use")

		// Release permit
		s.releaseSpendPermit()
		assert.Equal(t, 0, len(s.spendQueueSem), "permit should be released")
	})

	t.Run("AcquireWithTimeout", func(t *testing.T) {
		s := &Store{
			spendQueueSem:       make(chan struct{}, 2),
			spendEnqueueTimeout: 100 * time.Millisecond,
		}

		ctx := context.Background()

		// Acquire two permits (fill buffer)
		err := s.acquireSpendPermit(ctx)
		require.NoError(t, err)
		err = s.acquireSpendPermit(ctx)
		require.NoError(t, err)

		// Third acquire should timeout
		start := time.Now()
		err = s.acquireSpendPermit(ctx)
		elapsed := time.Since(start)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "timed out")
		assert.GreaterOrEqual(t, elapsed, 100*time.Millisecond, "should wait for timeout")
		assert.Less(t, elapsed, 200*time.Millisecond, "should not wait much longer than timeout")
	})

	t.Run("AcquireWithContextCancellation", func(t *testing.T) {
		s := &Store{
			spendQueueSem:       make(chan struct{}, 1),
			spendEnqueueTimeout: 5 * time.Second, // Long timeout
		}

		// Fill the queue
		_ = s.acquireSpendPermit(context.Background())

		// Create cancellable context
		ctx, cancel := context.WithCancel(context.Background())

		// Try to acquire in goroutine
		errCh := make(chan error, 1)
		go func() {
			errCh <- s.acquireSpendPermit(ctx)
		}()

		// Cancel context after short delay
		time.Sleep(50 * time.Millisecond)
		cancel()

		// Should get context canceled error
		err := <-errCh
		require.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	})

	t.Run("NilSemaphoreIsNoOp", func(t *testing.T) {
		s := &Store{
			spendQueueSem: nil, // Not initialized
		}

		ctx := context.Background()

		// Acquire should succeed immediately
		err := s.acquireSpendPermit(ctx)
		require.NoError(t, err)

		// Release should not panic
		s.releaseSpendPermit()
	})

	t.Run("NilStoreIsNoOp", func(t *testing.T) {
		var s *Store

		ctx := context.Background()

		// Acquire should succeed immediately
		err := s.acquireSpendPermit(ctx)
		require.NoError(t, err)

		// Release should not panic
		s.releaseSpendPermit()
	})
}

func TestSpendQueueConcurrency(t *testing.T) {
	t.Run("ConcurrentAcquireAndRelease", func(t *testing.T) {
		queueSize := 5
		s := &Store{
			spendQueueSem:       make(chan struct{}, queueSize),
			spendEnqueueTimeout: 20 * time.Millisecond, // Short timeout to ensure some fail
		}

		ctx := context.Background()
		goroutines := 50
		iterations := 10

		var wg sync.WaitGroup
		successCount := int32(0)
		timeoutCount := int32(0)

		var successMu sync.Mutex
		var timeoutMu sync.Mutex

		wg.Add(goroutines)
		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < iterations; j++ {
					err := s.acquireSpendPermit(ctx)
					if err == nil {
						successMu.Lock()
						successCount++
						successMu.Unlock()

						// Simulate longer work to create contention
						time.Sleep(10 * time.Millisecond)

						s.releaseSpendPermit()
					} else {
						timeoutMu.Lock()
						timeoutCount++
						timeoutMu.Unlock()
					}
				}
			}()
		}

		wg.Wait()

		// Should have some successes and some timeouts due to queue limit
		assert.Greater(t, successCount, int32(0), "should have successful acquisitions")
		assert.Greater(t, timeoutCount, int32(0), "should have timeouts when queue is full")

		// Queue should be empty at the end (all permits released)
		assert.Equal(t, 0, len(s.spendQueueSem), "all permits should be released")
	})

	t.Run("NoPermitLeakageUnderLoad", func(t *testing.T) {
		queueSize := 5
		s := &Store{
			spendQueueSem:       make(chan struct{}, queueSize),
			spendEnqueueTimeout: 10 * time.Millisecond,
			logger:              ulogger.NewErrorTestLogger(t),
		}

		ctx := context.Background()
		goroutines := 20
		iterations := 100

		var wg sync.WaitGroup
		wg.Add(goroutines)

		for i := 0; i < goroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < iterations; j++ {
					err := s.acquireSpendPermit(ctx)
					if err == nil {
						s.releaseSpendPermit()
					}
				}
			}()
		}

		wg.Wait()

		// Verify no permit leakage - queue should be empty
		assert.Equal(t, 0, len(s.spendQueueSem), "no permits should be leaked")
	})
}

func TestSpendQueueIntegration(t *testing.T) {
	t.Run("QueueLimitEnforcesBackpressure", func(t *testing.T) {
		queueSize := 3
		s := &Store{
			spendQueueSem:       make(chan struct{}, queueSize),
			spendEnqueueTimeout: 50 * time.Millisecond,
			logger:              ulogger.NewErrorTestLogger(t),
		}

		ctx := context.Background()

		// Acquire all permits
		for i := 0; i < queueSize; i++ {
			err := s.acquireSpendPermit(ctx)
			require.NoError(t, err)
		}

		// Queue should be full
		assert.Equal(t, queueSize, len(s.spendQueueSem))

		// Next acquire should timeout
		err := s.acquireSpendPermit(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "timed out")

		// Release one permit
		s.releaseSpendPermit()
		assert.Equal(t, queueSize-1, len(s.spendQueueSem))

		// Should be able to acquire again
		err = s.acquireSpendPermit(ctx)
		require.NoError(t, err)
	})
}

func TestPermitLeakageDetection(t *testing.T) {
	t.Run("DetectsDoubleRelease", func(t *testing.T) {
		s := &Store{
			spendQueueSem:       make(chan struct{}, 2),
			spendEnqueueTimeout: 100 * time.Millisecond,
			logger:              ulogger.NewErrorTestLogger(t),
		}

		ctx := context.Background()

		// Acquire one permit
		err := s.acquireSpendPermit(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, len(s.spendQueueSem), "one permit should be acquired")

		// Release once (normal)
		s.releaseSpendPermit()
		assert.Equal(t, 0, len(s.spendQueueSem), "permit should be released")

		// Release again without acquiring (should trigger default case)
		// This tests the error path where we try to release without holding a permit
		// The semaphore buffer is empty, so the select will hit the default case
		// which logs an error (tested via logger integration)
		s.releaseSpendPermit()
		assert.Equal(t, 0, len(s.spendQueueSem), "semaphore should still be empty")

		// The logger.Errorf call in the default case will be invoked
		// In production this would alert us to a logic bug in permit management
	})
}

func TestSpendQueueEdgeCases(t *testing.T) {
	t.Run("ZeroTimeout", func(t *testing.T) {
		s := &Store{
			spendQueueSem:       make(chan struct{}, 1),
			spendEnqueueTimeout: 0, // Should use blocking mode
		}

		ctx := context.Background()

		// Should be able to acquire
		err := s.acquireSpendPermit(ctx)
		require.NoError(t, err)

		// Second acquire should block indefinitely (we test with context cancellation)
		ctx2, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)

		go func() {
			errCh <- s.acquireSpendPermit(ctx2)
		}()

		time.Sleep(50 * time.Millisecond)
		cancel()

		err = <-errCh
		require.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	})

	t.Run("VeryLargeQueue", func(t *testing.T) {
		s := &Store{
			spendQueueSem:       make(chan struct{}, 10000),
			spendEnqueueTimeout: 100 * time.Millisecond,
		}

		ctx := context.Background()

		// Should be able to acquire many permits
		for i := 0; i < 1000; i++ {
			err := s.acquireSpendPermit(ctx)
			require.NoError(t, err)
		}

		assert.Equal(t, 1000, len(s.spendQueueSem))

		// Release all
		for i := 0; i < 1000; i++ {
			s.releaseSpendPermit()
		}

		assert.Equal(t, 0, len(s.spendQueueSem))
	})
}
