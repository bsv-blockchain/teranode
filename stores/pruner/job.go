package pruner

import (
	"context"
	"sync/atomic"
	"time"
)

// JobStatus represents the status of a cleanup job
type JobStatus int32

// Job statuses
const (
	JobStatusPending JobStatus = iota
	JobStatusRunning
	JobStatusCompleted
	JobStatusFailed
	JobStatusCancelled
)

// String returns the string representation of the job status
func (s JobStatus) String() string {
	switch s {
	case JobStatusPending:
		return "pending"
	case JobStatusRunning:
		return "running"
	case JobStatusCompleted:
		return "completed"
	case JobStatusFailed:
		return "failed"
	case JobStatusCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

// Job represents a pruner job
type Job struct {
	BlockHeight uint32
	status      atomic.Int32 // Using atomic for thread-safe access
	Error       error
	Created     time.Time
	Started     time.Time
	Ended       time.Time
	ctx         context.Context
	cancel      context.CancelFunc
	DoneCh      chan string // Channel to signal when the job is complete (for testing purposes)
}

// GetStatus returns the current status of the job
func (j *Job) GetStatus() JobStatus {
	return JobStatus(j.status.Load())
}

// SetStatus sets the status of the job
func (j *Job) SetStatus(status JobStatus) {
	j.status.Store(int32(status))
}

// NewJob creates a new pruner job for the specified block height
func NewJob(blockHeight uint32, parentCtx context.Context, doneCh ...chan string) *Job {
	// If parentCtx is nil, use background context
	if parentCtx == nil {
		parentCtx = context.Background()
	}

	ctx, cancelFunc := context.WithCancel(parentCtx)

	var ch chan string
	if len(doneCh) > 0 {
		ch = doneCh[0]
	}

	job := &Job{
		BlockHeight: blockHeight,
		Created:     time.Now(),
		ctx:         ctx,
		cancel:      cancelFunc,
		DoneCh:      ch,
	}

	// Initialize status to pending
	job.SetStatus(JobStatusPending)

	return job
}

// Context returns the job's context
func (j *Job) Context() context.Context {
	return j.ctx
}

// Cancel cancels the job
func (j *Job) Cancel() {
	if j.cancel != nil {
		j.cancel()
	}
}
