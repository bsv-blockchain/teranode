// Package pruner provides a job management framework for background cleanup operations
// based on blockchain height. It enables stores to automatically prune old data when
// blocks reach a certain age, coordinating with block persistence to ensure data safety.
//
// The package implements a worker pool pattern with configurable concurrency, job queuing,
// and status tracking. It's designed to be integrated into storage implementations that
// need to clean up historical data as the blockchain progresses.
//
// Key Features:
//   - Configurable worker pool for concurrent job processing
//   - Job status tracking (pending, running, completed, failed, cancelled)
//   - Context-based cancellation support for graceful shutdown
//   - Coordination with block persister to avoid premature deletion
//   - Job history management with configurable retention limits
//   - Thread-safe operations using atomic primitives and mutexes
//
// Architecture:
//   - Job: Represents a single pruning operation for a specific block height
//   - JobManager: Manages worker pool and job queue
//   - Service: Interface for integrating pruner into storage implementations
//   - JobProcessorFunc: Custom function type for implementing pruning logic
//
// Usage Pattern:
//  1. Implement a JobProcessorFunc that performs the actual cleanup
//  2. Create a JobManager with desired configuration
//  3. Start the JobManager with a context
//  4. Trigger pruning operations via UpdateBlockHeight or TriggerPruner
//  5. Monitor job status and history as needed
package pruner

import (
	"context"
	"sync/atomic"
	"time"
)

// JobStatus represents the current state of a pruning job in its lifecycle.
type JobStatus int32

// Job status constants define the possible states of a pruning job.
const (
	JobStatusPending JobStatus = iota
	JobStatusRunning
	JobStatusCompleted
	JobStatusFailed
	JobStatusCancelled
)

// String returns the human-readable string representation of the job status.
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

// Job represents a single pruning operation for a specific block height.
// It tracks the lifecycle from creation through completion, including timing,
// status, and cancellation support.
//
// Thread Safety:
//   - status field uses atomic.Int32 for lock-free status updates
//   - Error, Started, and Ended are set by the worker processing the job
type Job struct {
	BlockHeight uint32
	status      atomic.Int32
	Error       error
	Created     time.Time
	Started     time.Time
	Ended       time.Time
	ctx         context.Context
	cancel      context.CancelFunc
	DoneCh      chan string // Optional channel to signal completion (for testing)
}

// GetStatus returns the current status of the job using atomic operations.
func (j *Job) GetStatus() JobStatus {
	return JobStatus(j.status.Load())
}

// SetStatus updates the job's status using atomic operations.
// Status updates should follow: Pending -> Running -> (Completed|Failed|Cancelled)
func (j *Job) SetStatus(status JobStatus) {
	j.status.Store(int32(status))
}

// NewJob creates a new pruning job for the specified block height.
// The job is initialized in Pending status with a cancellable context.
// Optional doneCh can be provided for completion signaling (testing).
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

// Context returns the job's context for cancellation checking.
// Workers should monitor this context to detect cancellation and stop gracefully.
func (j *Job) Context() context.Context {
	return j.ctx
}

// Cancel requests cancellation of the job by cancelling its context.
// Safe to call multiple times.
func (j *Job) Cancel() {
	if j.cancel != nil {
		j.cancel()
	}
}
