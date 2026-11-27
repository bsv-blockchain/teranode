package lane

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
)

// A subtree's lane bitmaps file is written before its subtree file. The blob store schedules the
// file's deletion (its delete-at height) in the blockchain database before it publishes the file,
// so a write needs that database: when Postgres restarted for 13 seconds on mainnet, five writes
// failed and their subtrees were stored without the file, leaving their blocks without rows.
// BitmapsWriteRetry writes the file again while it fails with an error a retry can pass.

// DefaultBitmapsWriteRetryBound is how long a lane bitmaps file write is retried after its first
// failure. It is bounded by time, not by attempts, so it covers an outage of a known length: the
// 13-second Postgres restart four times over, with room for a slower one, such as a shutdown
// checkpoint on a large database. It is a small part of the ten minutes between blocks.
const DefaultBitmapsWriteRetryBound = time.Minute

// The backoff between attempts doubles from the first to the cap. While a write is still retrying,
// the wait before its next attempt is at most the cap.
const (
	defaultBitmapsWriteFirstBackoff = 250 * time.Millisecond
	defaultBitmapsWriteMaxBackoff   = 4 * time.Second
)

// BitmapsWriteRetry retries failed lane bitmaps file writes for one service, at
// utxostore_laneMigration shadow. The bound is per outage, not per write: it starts at the first
// failure after the store last worked, and once it has run out, a write that fails is not retried,
// as before the retry existed, until a write succeeds again or no write has failed for a whole
// bound. A long outage therefore holds up the service's subtree storage for one bound, not one
// bound per subtree. The zero value is ready to use, with the defaults.
type BitmapsWriteRetry struct {
	// Bound, FirstBackoff and MaxBackoff override the defaults when not zero.
	Bound        time.Duration
	FirstBackoff time.Duration
	MaxBackoff   time.Duration

	mu sync.Mutex
	// failingSince is when the current outage began, zero when the last write succeeded.
	failingSince time.Time
	// lastFailure is when a write last failed with an error a retry can pass.
	lastFailure time.Time
}

func (r *BitmapsWriteRetry) bound() time.Duration {
	if r.Bound > 0 {
		return r.Bound
	}

	return DefaultBitmapsWriteRetryBound
}

func (r *BitmapsWriteRetry) firstBackoff() time.Duration {
	if r.FirstBackoff > 0 {
		return r.FirstBackoff
	}

	return defaultBitmapsWriteFirstBackoff
}

func (r *BitmapsWriteRetry) maxBackoff() time.Duration {
	if r.MaxBackoff > 0 {
		return r.MaxBackoff
	}

	return defaultBitmapsWriteMaxBackoff
}

// nextBackoff is the wait after one of backoff: double it, up to the cap.
func (r *BitmapsWriteRetry) nextBackoff(backoff time.Duration) time.Duration {
	return min(2*backoff, r.maxBackoff())
}

// failed notes a failure at now and returns when the outage it belongs to began: an outage ends
// at a successful write, or when no write has failed for a whole bound.
func (r *BitmapsWriteRetry) failed(now time.Time) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.failingSince.IsZero() || now.Sub(r.lastFailure) > r.bound() {
		r.failingSince = now
	}

	// failures can be noted out of order; the last failure is the latest one
	if now.After(r.lastFailure) {
		r.lastFailure = now
	}

	return r.failingSince
}

// deadline notes a write's first failure at now and returns when its retries end: the bound after
// its outage began, and never later than the bound after now, even when a newer outage was noted first.
func (r *BitmapsWriteRetry) deadline(now time.Time) time.Time {
	since := r.failed(now)
	if since.After(now) {
		since = now
	}

	return since.Add(r.bound())
}

// succeeded ends the current outage with a write that started at started and succeeded, unless a
// write failed after it started: that failure may be newer than the success, so its outage goes on
// until a write that started after it succeeds.
func (r *BitmapsWriteRetry) succeeded(started time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.lastFailure.After(started) {
		return
	}

	r.failingSince = time.Time{}
}

// Do runs write, the write of one lane bitmaps file, and runs it again after a backoff while it
// fails with an error a retry can pass (RetryableBitmapsWriteError) and the outage's bound has not
// run out. The bound starts at the first failure. No retry starts after it, and each retry gets a
// context that ends at it, but an attempt in flight then is waited for wherever its store does not
// honour that context. A write takes its first attempt, at most the bound, and one attempt in
// flight when the bound runs out. A write ctx ends stops at once. It returns nil once write
// succeeds, or the error that ended the retries: write's own when it cannot pass on a retry.
// Writes retried are counted in teranode_lane_bitmaps_write_retries_total, and writes still
// failing when the bound ran out in teranode_lane_bitmaps_write_retries_exhausted_total, both by
// service.
func (r *BitmapsWriteRetry) Do(ctx context.Context, service string, write func(ctx context.Context) error) error {
	started := time.Now()

	err := write(ctx)
	if err == nil {
		r.succeeded(started)

		return nil
	}

	if !RetryableBitmapsWriteError(ctx, err) {
		return err
	}

	deadline := r.deadline(time.Now())
	backoff := r.firstBackoff()

	attemptCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	retried := false

	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			countBitmapsWriteRetriesExhausted(service)

			return errors.NewStorageError("still failing when the lane bitmaps write retry bound of %s ran out", r.bound(), err)
		}

		if !sleepCtx(ctx, min(backoff, remaining)) {
			return errors.NewContextCanceledError("stopped retrying the lane bitmaps write", err)
		}

		// a sleep clamped to the bound, or one that woke late, ends at or past it
		if time.Until(deadline) <= 0 {
			countBitmapsWriteRetriesExhausted(service)

			return errors.NewStorageError("still failing when the lane bitmaps write retry bound of %s ran out", r.bound(), err)
		}

		if !retried {
			retried = true

			countBitmapsWriteRetried(service)
		}

		started = time.Now()

		if err = write(attemptCtx); err == nil {
			r.succeeded(started)

			return nil
		}

		if !RetryableBitmapsWriteError(ctx, err) {
			return err
		}

		r.failed(time.Now())

		backoff = r.nextBackoff(backoff)
	}
}

// sleepCtx waits d, and reports false when ctx ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// RetryableBitmapsWriteError reports whether a lane bitmaps file write that failed with err can
// pass on a retry: the blob store, or the database it schedules the file's deletion in, failed or
// was unavailable, or an attempt timed out while ctx is still live. Nothing is retried once ctx
// has ended, and a cancellation, a file already there, a configuration error or an invalid
// argument fails at once. The file store wraps every failure to schedule a deletion as a storage
// error, including a blockchain service that cannot be reached, whose gRPC error carries no code.
func RetryableBitmapsWriteError(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}

	for _, final := range []error{
		context.Canceled,
		errors.ErrContextCanceled,
		errors.ErrBlobAlreadyExists,
		errors.ErrConfiguration,
		errors.ErrInvalidArgument,
	} {
		if errors.Is(err, final) {
			return false
		}
	}

	for _, transient := range []error{
		context.DeadlineExceeded,
		errors.ErrStorageError,
		errors.ErrStorageUnavailable,
		errors.ErrStorageNotStarted,
		errors.ErrBlobError,
		errors.ErrServiceError,
		errors.ErrServiceUnavailable,
		errors.ErrServiceNotStarted,
		errors.ErrServiceNotReady,
		errors.ErrNetworkError,
		errors.ErrNetworkTimeout,
		errors.ErrNetworkConnectionRefused,
	} {
		if errors.Is(err, transient) {
			return true
		}
	}

	return false
}
