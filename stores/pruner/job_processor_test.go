package pruner

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobStatus(t *testing.T) {
	testCases := []struct {
		status   JobStatus
		expected string
	}{
		{JobStatusPending, "pending"},
		{JobStatusRunning, "running"},
		{JobStatusCompleted, "completed"},
		{JobStatusFailed, "failed"},
		{JobStatusCancelled, "cancelled"},
		{JobStatus(99), "unknown"}, // Invalid status
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.status.String())
		})
	}
}

func TestNewJob(t *testing.T) {
	t.Run("with parent context", func(t *testing.T) {
		ctx := context.Background()
		job := NewJob(123, ctx)

		assert.Equal(t, uint32(123), job.BlockHeight)
		assert.Equal(t, JobStatusPending, job.GetStatus())
		assert.NotNil(t, job.Context())
		assert.NotEqual(t, ctx, job.Context()) // Should be a child context
		assert.Nil(t, job.Error)
		assert.False(t, job.Created.IsZero())
		assert.True(t, job.Started.IsZero())
		assert.True(t, job.Ended.IsZero())
	})

	t.Run("with nil parent context", func(t *testing.T) {
		job := NewJob(123, nil)

		assert.Equal(t, uint32(123), job.BlockHeight)
		assert.Equal(t, JobStatusPending, job.GetStatus())
		assert.NotNil(t, job.Context())
		assert.Nil(t, job.Error)
	})

	t.Run("with done channel", func(t *testing.T) {
		done := make(chan string, 1)
		job := NewJob(123, context.Background(), done)

		assert.Equal(t, done, job.DoneCh)
	})
}

func TestJobStatusOperations(t *testing.T) {
	job := NewJob(123, context.Background())

	// Initial status should be pending
	assert.Equal(t, JobStatusPending, job.GetStatus())

	// Change status to running
	job.SetStatus(JobStatusRunning)
	assert.Equal(t, JobStatusRunning, job.GetStatus())

	// Change status to completed
	job.SetStatus(JobStatusCompleted)
	assert.Equal(t, JobStatusCompleted, job.GetStatus())
}

func TestJobCancellation(t *testing.T) {
	job := NewJob(123, context.Background())

	// Cancel the job
	job.Cancel()

	// Context should be cancelled
	select {
	case <-job.Context().Done():
		// Context was cancelled as expected
	default:
		t.Fatal("Job context was not cancelled")
	}
}

func TestNewJobManager(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)
	processor := func(job *Job, workerID int) {}

	t.Run("valid options", func(t *testing.T) {
		manager, err := NewJobManager(JobManagerOptions{
			Logger:         logger,
			WorkerCount:    2,
			MaxJobsHistory: 5,
			JobProcessor:   processor,
		})

		assert.NoError(t, err)
		assert.NotNil(t, manager)
		assert.Equal(t, 2, manager.workerCount)
		assert.Equal(t, 5, manager.maxJobsHistory)
	})

	t.Run("default worker count", func(t *testing.T) {
		manager, err := NewJobManager(JobManagerOptions{
			Logger:       logger,
			JobProcessor: processor,
		})

		assert.NoError(t, err)
		assert.NotNil(t, manager)
		assert.Equal(t, DefaultWorkerCount, manager.workerCount)
		assert.Equal(t, DefaultMaxJobsHistory, manager.maxJobsHistory)
	})

	t.Run("missing logger", func(t *testing.T) {
		manager, err := NewJobManager(JobManagerOptions{
			JobProcessor: processor,
		})

		assert.Error(t, err)
		assert.Nil(t, manager)
		assert.Contains(t, err.Error(), "logger is required")
	})

	t.Run("missing job processor", func(t *testing.T) {
		manager, err := NewJobManager(JobManagerOptions{
			Logger: logger,
		})

		assert.Error(t, err)
		assert.Nil(t, manager)
		assert.Contains(t, err.Error(), "job processor is required")
	})
}

func TestJobManagerStart(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)
	processor := func(job *Job, workerID int) {}

	t.Run("start with context", func(t *testing.T) {
		manager, err := NewJobManager(JobManagerOptions{
			Logger:       logger,
			JobProcessor: processor,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		manager.Start(ctx)
		defer manager.Stop()

		// Verify workers are started
		manager.mu.RLock()
		assert.True(t, manager.workersStarted)
		assert.NotNil(t, manager.ctx)
		manager.mu.RUnlock()
	})

	t.Run("start with nil context", func(t *testing.T) {
		manager, err := NewJobManager(JobManagerOptions{
			Logger:       logger,
			JobProcessor: processor,
		})
		require.NoError(t, err)

		manager.Start(nil) //nolint:staticcheck
		defer manager.Stop()

		// Verify workers are started with a background context
		manager.mu.RLock()
		assert.True(t, manager.workersStarted)
		assert.NotNil(t, manager.ctx)
		manager.mu.RUnlock()
	})

	t.Run("start multiple times", func(t *testing.T) {
		manager, err := NewJobManager(JobManagerOptions{
			Logger:       logger,
			JobProcessor: processor,
		})
		require.NoError(t, err)

		ctx := context.Background()
		manager.Start(ctx)

		defer manager.Stop()

		// Get the original context
		manager.mu.RLock()
		originalCtx := manager.ctx
		manager.mu.RUnlock()

		// Start again with a different context
		newCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		manager.Start(newCtx)

		// Context should not have changed
		manager.mu.RLock()
		assert.Equal(t, originalCtx, manager.ctx)
		manager.mu.RUnlock()
	})
}

func TestJobManagerUpdateBlockHeight(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)

	t.Run("update block height", func(t *testing.T) {
		var (
			processedJobs []*Job
			processMutex  sync.Mutex
		)

		processor := func(job *Job, workerID int) {
			processMutex.Lock()
			processedJobs = append(processedJobs, job)
			processMutex.Unlock()

			// Mark job as completed
			job.SetStatus(JobStatusCompleted)
		}

		manager, err := NewJobManager(JobManagerOptions{
			Logger:       logger,
			JobProcessor: processor,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		manager.Start(ctx)
		defer manager.Stop()

		// Update block height
		err = manager.UpdateBlockHeight(123)
		require.NoError(t, err)

		// Verify job was created
		jobs := manager.GetJobs()
		require.Len(t, jobs, 1)
		assert.Equal(t, uint32(123), jobs[0].BlockHeight)

		// Wait for job to be processed
		time.Sleep(100 * time.Millisecond)

		// Verify job was processed
		processMutex.Lock()

		assert.GreaterOrEqual(t, len(processedJobs), 1)

		if len(processedJobs) > 0 {
			assert.Equal(t, uint32(123), processedJobs[0].BlockHeight)
		}

		processMutex.Unlock()
	})

	t.Run("update with done channel", func(t *testing.T) {
		processor := func(job *Job, workerID int) {
			// Mark job as completed and signal done channel
			job.SetStatus(JobStatusCompleted)

			if job.DoneCh != nil {
				job.DoneCh <- "completed"
				close(job.DoneCh)
			}
		}

		manager, err := NewJobManager(JobManagerOptions{
			Logger:       logger,
			JobProcessor: processor,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		manager.Start(ctx)
		defer manager.Stop()

		// Create a done channel
		done := make(chan string)

		// Update block height with done channel
		err = manager.UpdateBlockHeight(123, done)
		require.NoError(t, err)

		// Wait for job to complete
		select {
		case result := <-done:
			assert.Equal(t, "completed", result)
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for job to complete")
		}
	})
}

func TestJobManagerTriggerCleanup(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)
	processor := func(job *Job, workerID int) {
		// Mark job as completed
		job.SetStatus(JobStatusCompleted)
	}

	t.Run("trigger cleanup", func(t *testing.T) {
		manager, err := NewJobManager(JobManagerOptions{
			Logger:       logger,
			JobProcessor: processor,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		manager.Start(ctx)
		defer manager.Stop()

		// Trigger cleanup
		err = manager.TriggerPruner(123)
		require.NoError(t, err)

		// Verify job was created
		jobs := manager.GetJobs()
		require.Len(t, jobs, 1)
		assert.Equal(t, uint32(123), jobs[0].BlockHeight)
	})

	t.Run("cancel pending jobs", func(t *testing.T) {
		processorCancelTest := func(job *Job, workerID int) {
			if job.BlockHeight == 123 {
				// Simulate work for the first job to allow cancellation
				select {
				case <-time.After(200 * time.Millisecond):
					// Work completed
				case <-job.Context().Done():
					// Job was cancelled
					job.SetStatus(JobStatusCancelled)
					return
				}
			}
			// Mark job as completed if not cancelled
			job.SetStatus(JobStatusCompleted)
		}

		manager, err := NewJobManager(JobManagerOptions{
			Logger:       logger,
			JobProcessor: processorCancelTest, // Use the new processor for this subtest
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Trigger first cleanup
		err = manager.TriggerPruner(123)
		require.NoError(t, err)

		manager.Start(ctx)
		defer manager.Stop()

		doneCh := make(chan string)

		// Trigger second cleanup while first is maybe running
		err = manager.TriggerPruner(456, doneCh)
		require.NoError(t, err)

		select {
		case <-doneCh:
			// Expected
		case <-time.After(1 * time.Second):
			require.Fail(t, "Second cleanup job should be completed")
		}

		// Verify jobs were created and first job was cancelled
		jobs := manager.GetJobs()

		require.Len(t, jobs, 2)
		assert.Equal(t, uint32(123), jobs[0].BlockHeight)
		assert.Equal(t, JobStatusCancelled, jobs[0].GetStatus())
		assert.Equal(t, uint32(456), jobs[1].BlockHeight)
		assert.Equal(t, JobStatusCompleted, jobs[1].GetStatus())
	})

	t.Run("max jobs history", func(t *testing.T) {
		manager, err := NewJobManager(JobManagerOptions{
			Logger:         logger,
			JobProcessor:   processor,
			MaxJobsHistory: 2,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		manager.Start(ctx)
		defer manager.Stop()

		// Trigger multiple cleanups
		err = manager.TriggerPruner(100)
		require.NoError(t, err)

		err = manager.TriggerPruner(200)
		require.NoError(t, err)

		err = manager.TriggerPruner(300)
		require.NoError(t, err)

		// Verify only the most recent jobs are kept
		jobs := manager.GetJobs()
		require.Len(t, jobs, 2)
		assert.Equal(t, uint32(200), jobs[0].BlockHeight)
		assert.Equal(t, uint32(300), jobs[1].BlockHeight)
	})
}

func TestJobManagerGetJobs(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)
	processor := func(job *Job, workerID int) {}

	manager, err := NewJobManager(JobManagerOptions{
		Logger:       logger,
		JobProcessor: processor,
	})
	require.NoError(t, err)

	// No jobs initially
	jobs := manager.GetJobs()
	assert.Empty(t, jobs)

	// Add a job
	manager.jobsMutex.Lock()
	job := NewJob(123, context.Background())
	manager.jobs = append(manager.jobs, job)
	manager.jobsMutex.Unlock()

	// Get jobs
	jobs = manager.GetJobs()
	require.Len(t, jobs, 1)
	assert.Equal(t, uint32(123), jobs[0].BlockHeight)

	// Modify the returned job (shouldn't affect the original)
	jobs[0].SetStatus(JobStatusCompleted)

	// Original job should be unchanged
	manager.jobsMutex.RLock()
	assert.Equal(t, JobStatusPending, manager.jobs[0].GetStatus())
	manager.jobsMutex.RUnlock()
}

func TestWorkerShutdown(t *testing.T) {
	logger := ulogger.NewVerboseTestLogger(t)

	t.Run("context cancellation", func(t *testing.T) {
		var (
			workerStarted = make(chan struct{})
		)

		processor := func(job *Job, workerID int) {}

		manager, err := NewJobManager(JobManagerOptions{
			Logger:       logger,
			JobProcessor: processor,
			WorkerCount:  1,
		})
		require.NoError(t, err)

		// Create a custom context we can cancel
		ctx, cancel := context.WithCancel(context.Background())

		// Start the manager
		manager.Start(ctx)
		defer manager.Stop()

		// Signal that we're ready to test
		close(workerStarted)

		// Wait a bit for workers to start
		time.Sleep(100 * time.Millisecond)

		// Cancel the context to trigger worker shutdown
		cancel()

		// Wait a bit for workers to exit
		time.Sleep(100 * time.Millisecond)

		// Check that the manager's context is done
		select {
		case <-manager.ctx.Done():
			// Expected
		default:
			t.Fatal("Manager context should be cancelled")
		}
	})
}
