package pruner

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/assert"
)

func TestJob(t *testing.T) {
	t.Run("creation", func(t *testing.T) {
		job := NewJob(42, context.Background())

		assert.Equal(t, uint32(42), job.BlockHeight)
		assert.Equal(t, JobStatusPending, job.GetStatus())
		assert.NotNil(t, job.Context())
		assert.Nil(t, job.Error)
		assert.False(t, job.Created.IsZero())
		assert.True(t, job.Started.IsZero())
		assert.True(t, job.Ended.IsZero())
		assert.Nil(t, job.DoneCh)
	})

	t.Run("with done channel", func(t *testing.T) {
		doneCh := make(chan string, 1)
		job := NewJob(42, context.Background(), doneCh)

		assert.Equal(t, doneCh, job.DoneCh)
	})

	t.Run("status changes", func(t *testing.T) {
		job := NewJob(42, context.Background())

		// Initial status
		assert.Equal(t, JobStatusPending, job.GetStatus())

		// Change to running
		job.SetStatus(JobStatusRunning)
		assert.Equal(t, JobStatusRunning, job.GetStatus())

		// Change to completed
		job.SetStatus(JobStatusCompleted)
		assert.Equal(t, JobStatusCompleted, job.GetStatus())

		// Change to failed
		job.SetStatus(JobStatusFailed)
		assert.Equal(t, JobStatusFailed, job.GetStatus())

		// Change to cancelled
		job.SetStatus(JobStatusCancelled)
		assert.Equal(t, JobStatusCancelled, job.GetStatus())
	})

	t.Run("context and cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		job := NewJob(42, ctx)

		// Context should be a child of the provided context
		assert.NotEqual(t, ctx, job.Context())

		// Context should not be cancelled initially
		select {
		case <-job.Context().Done():
			t.Fatal("Job context should not be cancelled initially")
		default: // Expected
		}

		// Cancel the job
		job.Cancel()

		// Context should be cancelled
		select {
		case <-job.Context().Done():
			// Expected
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Job context should be cancelled after Cancel() is called")
		}

		// Cancelling the parent context should also cancel the job context
		job = NewJob(42, ctx)

		cancel()

		select {
		case <-job.Context().Done():
			// Expected
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Job context should be cancelled when parent context is cancelled")
		}
	})

	t.Run("with nil parent context", func(t *testing.T) {
		job := NewJob(42, nil)

		// Should create a background context
		assert.NotNil(t, job.Context())

		// Context should not be cancelled
		select {
		case <-job.Context().Done():
			t.Fatal("Job context should not be cancelled initially")
		default: // Expected
		}

		// Cancel should work
		job.Cancel()

		select {
		case <-job.Context().Done():
			// Expected
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Job context should be cancelled after Cancel() is called")
		}
	})

	t.Run("error handling", func(t *testing.T) {
		job := NewJob(42, context.Background())

		// Initially no error
		assert.Nil(t, job.Error)

		// Set an error
		testErr := errors.NewProcessingError("test error")
		job.Error = testErr

		assert.Equal(t, testErr, job.Error)
	})

	t.Run("time tracking", func(t *testing.T) {
		job := NewJob(42, context.Background())

		// Created time should be set
		assert.False(t, job.Created.IsZero())

		// Started and Ended should be zero initially
		assert.True(t, job.Started.IsZero())
		assert.True(t, job.Ended.IsZero())

		// Set Started time
		now := time.Now()
		job.Started = now
		assert.Equal(t, now, job.Started)

		// Set Ended time
		later := now.Add(5 * time.Second)
		job.Ended = later
		assert.Equal(t, later, job.Ended)
	})
}

func TestJobStatusString(t *testing.T) {
	tests := []struct {
		status JobStatus
		want   string
	}{
		{JobStatusPending, "pending"},
		{JobStatusRunning, "running"},
		{JobStatusCompleted, "completed"},
		{JobStatusFailed, "failed"},
		{JobStatusCancelled, "cancelled"},
		{JobStatus(999), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.status.String())
		})
	}
}

func TestJobWithDoneChannel(t *testing.T) {
	t.Run("signal completion", func(t *testing.T) {
		doneCh := make(chan string, 1)
		job := NewJob(42, context.Background(), doneCh)

		// Send a message on the done channel
		doneCh <- "completed"

		// Should be able to receive the message
		select {
		case msg := <-job.DoneCh:
			assert.Equal(t, "completed", msg)
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Should receive message from done channel")
		}
	})

	t.Run("nil done channel", func(t *testing.T) {
		job := NewJob(42, context.Background())

		// DoneCh should return nil
		assert.Nil(t, job.DoneCh)
	})
}
