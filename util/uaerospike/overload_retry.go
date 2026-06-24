package uaerospike

import (
	"math/rand/v2"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/retry"
	"github.com/ordishs/gocore"
)

const (
	// defaultOverloadRetryMaxElapsed bounds the total time a single wrapper
	// call may spend retrying overload rejections before the error is
	// returned to the caller. Sized to survive the multi-minute sustained
	// overloads seen during big-output mainnet eras: once the budget is spent
	// the overload error propagates and the failure path this layer exists to
	// avoid (mass spend failures → block-validation retry loops) reappears.
	defaultOverloadRetryMaxElapsed = 2 * time.Minute

	// defaultOverloadRetryBaseBackoff is the first wait between overload
	// retries; it grows exponentially up to defaultOverloadRetryMaxBackoff.
	defaultOverloadRetryBaseBackoff = 50 * time.Millisecond

	// defaultOverloadRetryMaxBackoff caps the exponential backoff growth.
	defaultOverloadRetryMaxBackoff = 5 * time.Second

	// overloadRetryBackoffFactor is the exponential growth factor applied
	// between overload retries.
	overloadRetryBackoffFactor = 2.0

	// overloadRetryJitter is the fraction (±25%) by which each backoff sleep
	// is randomly perturbed. Under overload many operations fail at the same
	// instant and would otherwise retry in lockstep against an already
	// struggling server; jitter decorrelates the herd.
	overloadRetryJitter = 0.25
)

// overloadResultCodes are the result codes treated as "server overloaded":
// DEVICE_OVERLOAD is the server rejecting a write because the storage device
// cannot keep up; MAX_ERROR_RATE is the client's own per-node error-rate
// breaker tripping as a consequence of those rejections. Both are safe to
// re-issue: a DEVICE_OVERLOAD write was rejected before being applied and a
// MAX_ERROR_RATE call was never sent. TIMEOUT is deliberately excluded —
// timed-out writes may have been applied (in-doubt).
var overloadResultCodes = []types.ResultCode{types.DEVICE_OVERLOAD, types.MAX_ERROR_RATE}

// overloadRetryConfig holds the bounded-backoff parameters for overload
// retries. maxElapsed <= 0 disables the retry layer entirely.
type overloadRetryConfig struct {
	maxElapsed  time.Duration
	baseBackoff time.Duration
	maxBackoff  time.Duration
}

func (cfg overloadRetryConfig) enabled() bool {
	return cfg.maxElapsed > 0
}

// WithOverloadRetry configures the bounded retry the Client performs when the
// Aerospike server reports overload (DEVICE_OVERLOAD) or the client's local
// error-rate breaker rejects calls (MAX_ERROR_RATE).
//
//	maxElapsed   total retry budget per wrapper call; <= 0 disables the
//	             retry layer (errors propagate immediately, the prior
//	             behaviour).
//	baseBackoff  first wait between attempts; <= 0 falls back to the default.
//	maxBackoff   cap on the exponentially growing wait; raised to baseBackoff
//	             when smaller.
func WithOverloadRetry(maxElapsed, baseBackoff, maxBackoff time.Duration) ClientOption {
	return func(c *clientConfig) {
		if baseBackoff <= 0 {
			baseBackoff = defaultOverloadRetryBaseBackoff
		}

		if maxBackoff < baseBackoff {
			maxBackoff = baseBackoff
		}

		c.overloadRetry = overloadRetryConfig{
			maxElapsed:  maxElapsed,
			baseBackoff: baseBackoff,
			maxBackoff:  maxBackoff,
		}
	}
}

// WithLogger sets the logger used to report overload retries. When unset the
// retry layer is silent (stats are still recorded).
func WithLogger(logger ulogger.Logger) ClientOption {
	return func(c *clientConfig) {
		c.logger = logger
	}
}

// jitteredBackoff returns d scaled by a uniform random factor in
// [1-overloadRetryJitter, 1+overloadRetryJitter]. Non-positive d is returned
// unchanged. Callers must still clamp the result to any remaining budget.
func jitteredBackoff(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}

	factor := 1 - overloadRetryJitter + rand.Float64()*(2*overloadRetryJitter)

	return time.Duration(float64(d) * factor)
}

// isOverloadError reports whether err (or any error wrapped inside it) is one
// of the overload result codes. nil-safe.
func isOverloadError(err aerospike.Error) bool {
	return err != nil && err.Matches(overloadResultCodes...)
}

// retryOnOverload runs do, retrying with capped exponential backoff while it
// keeps failing with an overload result code, until the configured maxElapsed
// budget is spent. Any other outcome — success or a non-overload error — is
// returned immediately. The connection-semaphore permit is released during
// each backoff sleep and re-acquired before the retry: the server isn't
// seeing the op while we sleep, so holding the slot would only starve other
// callers (see the loop body and reacquirePermit).
func (c *Client) retryOnOverload(do func() aerospike.Error) aerospike.Error {
	err := do()
	if err == nil || !isOverloadError(err) || !c.overloadRetry.enabled() {
		return err
	}

	deadline := time.Now().Add(c.overloadRetry.maxElapsed)
	backoff := c.overloadRetry.baseBackoff

	for attempt := 1; ; attempt++ {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			c.logOverloadGiveUp(attempt, err)
			return err
		}

		// Jitter first to decorrelate retrying callers, then clamp: a wait
		// longer than the time left would push the next attempt beyond
		// maxElapsed, so it must never exceed the remaining budget.
		wait := jitteredBackoff(backoff)
		if wait > remaining {
			wait = remaining
		}

		c.logOverloadRetry(attempt, wait, err)

		start := gocore.CurrentTime()

		// Release the connection permit while we back off: the server isn't
		// seeing this op during the sleep, so holding the slot only starves
		// other callers (turning sustained overload into an ErrTimeout storm).
		// Re-take it (blocking, never timing out) before the retry.
		c.releasePermit()
		time.Sleep(wait)
		c.reacquirePermit()

		backoff = retry.CappedExponentialBackoff(backoff, overloadRetryBackoffFactor, c.overloadRetry.maxBackoff)

		err = do()

		c.stats.overloadRetryStat.AddTime(start)

		if err == nil || !isOverloadError(err) {
			return err
		}
	}
}

// retryBatchOnOverload runs do over records and, while individual records (or
// the whole call) keep failing with overload result codes, resubmits only the
// still-overloaded records with capped exponential backoff until the
// configured maxElapsed budget is spent.
//
// Contract: only overload failures are converted into successes. Non-overload
// per-record errors (KEY_NOT_FOUND_ERROR etc.) are never resubmitted and stay
// on their records exactly as the underlying client set them; non-overload
// top-level errors are returned unchanged.
func (c *Client) retryBatchOnOverload(records []aerospike.BatchRecordIfc, do func([]aerospike.BatchRecordIfc) aerospike.Error) aerospike.Error {
	err := do(records)
	if !c.overloadRetry.enabled() {
		return err
	}

	// Only overload (or BATCH_FAILED, which may be carrying per-record
	// overload failures) outcomes are this layer's concern.
	if err != nil && !isOverloadError(err) && !err.Matches(types.BATCH_FAILED) {
		return err
	}

	// The initial top-level outcome. A BATCH_FAILED here may carry a mix of
	// overload and non-overload per-record failures; only the overload subset
	// is retried, so this is preserved to re-surface the signal if a
	// non-overload failure outlives the retries (see the success exit below).
	firstErr := err

	failed := overloadedRecords(records, isOverloadError(err))
	if len(failed) == 0 {
		return err
	}

	deadline := time.Now().Add(c.overloadRetry.maxElapsed)
	backoff := c.overloadRetry.baseBackoff

	for attempt := 1; ; attempt++ {
		// In the partial-overload path the top-level err is nil while the
		// failed records carry the overload code, so log a representative
		// error rather than <nil>.
		logErr := batchOverloadError(err, failed)

		remaining := time.Until(deadline)
		if remaining <= 0 {
			c.logOverloadGiveUp(attempt, logErr)
			return batchOverloadError(err, failed)
		}

		// Jitter first to decorrelate retrying callers, then clamp: a wait
		// longer than the time left would push the next attempt beyond
		// maxElapsed, so it must never exceed the remaining budget.
		wait := jitteredBackoff(backoff)
		if wait > remaining {
			wait = remaining
		}

		c.logOverloadRetry(attempt, wait, logErr)

		start := gocore.CurrentTime()

		// Release the connection permit while we back off (see retryOnOverload):
		// holding it during the sleep only starves other callers. Re-take it
		// (blocking, never timing out) before the retry.
		c.releasePermit()
		time.Sleep(wait)
		c.reacquirePermit()

		backoff = retry.CappedExponentialBackoff(backoff, overloadRetryBackoffFactor, c.overloadRetry.maxBackoff)

		err = do(failed)

		c.stats.overloadRetryStat.AddTime(start)

		if err != nil && !isOverloadError(err) && !err.Matches(types.BATCH_FAILED) {
			return err
		}

		failed = overloadedRecords(failed, isOverloadError(err))
		if len(failed) == 0 {
			// No overloaded records remain. A residual BATCH_FAILED from the
			// last attempt means a non-overload record failure surfaced
			// inside the retried subset — return it for the caller to handle.
			if err != nil && !isOverloadError(err) {
				return err
			}

			// The retried subset is clean, but if the initial call reported
			// BATCH_FAILED and a non-overload per-record failure still remains
			// — on a record that was never part of the overloaded subset — that
			// top-level signal must be preserved, or callers that check only the
			// top-level error would read the partial success as a full success.
			if firstErr != nil && firstErr.Matches(types.BATCH_FAILED) && hasResidualNonOverloadFailure(records) {
				return firstErr
			}

			return nil
		}
	}
}

// overloadedRecords returns the subset of records that must be resubmitted.
// includeUnfinished covers client-side rejections (e.g. the MAX_ERROR_RATE
// breaker) where the call failed as a whole and unprocessed records still
// carry their initial NO_RESPONSE result code.
func overloadedRecords(records []aerospike.BatchRecordIfc, includeUnfinished bool) []aerospike.BatchRecordIfc {
	var failed []aerospike.BatchRecordIfc

	for _, rec := range records {
		switch rec.BatchRec().ResultCode {
		case types.DEVICE_OVERLOAD, types.MAX_ERROR_RATE:
			failed = append(failed, rec)
		case types.NO_RESPONSE:
			if includeUnfinished {
				failed = append(failed, rec)
			}
		}
	}

	return failed
}

// hasResidualNonOverloadFailure reports whether any record still carries a
// non-overload error. Used at the batch success exit to detect a genuine
// per-record failure that outlived the overload retries (it was never part of
// the retried overloaded subset).
func hasResidualNonOverloadFailure(records []aerospike.BatchRecordIfc) bool {
	for _, rec := range records {
		if recErr := rec.BatchRec().Err; recErr != nil && !isOverloadError(recErr) {
			return true
		}
	}

	return false
}

// batchOverloadError picks the error to return when the retry budget is
// exhausted: the last overload-matching top-level error, else the first
// still-failed record's error, so the result always Matches an overload code
// and callers can still recognise the outcome as overload. Note the spend
// circuit breaker no longer treats DEVICE_OVERLOAD as an infrastructure
// failure (it is owned by this retry layer), so an exhausted-budget overload
// no longer trips the breaker.
func batchOverloadError(lastErr aerospike.Error, failed []aerospike.BatchRecordIfc) aerospike.Error {
	if isOverloadError(lastErr) {
		return lastErr
	}

	for _, rec := range failed {
		if recErr := rec.BatchRec().Err; recErr != nil {
			return recErr
		}
	}

	if lastErr != nil {
		return lastErr
	}

	return &aerospike.AerospikeError{ResultCode: types.DEVICE_OVERLOAD}
}

func (c *Client) logOverloadRetry(attempt int, wait time.Duration, err aerospike.Error) {
	if c.logger == nil {
		return
	}

	// DEBUG for the first attempts to keep brief overload blips quiet, WARN
	// once the condition persists — same convention as util/retry.
	if attempt < 5 {
		c.logger.Debugf("aerospike overloaded (attempt %d): %v, retrying in %.3fs", attempt, err, wait.Seconds())
	} else {
		c.logger.Warnf("aerospike overloaded (attempt %d): %v, retrying in %.3fs", attempt, err, wait.Seconds())
	}
}

func (c *Client) logOverloadGiveUp(attempt int, err aerospike.Error) {
	if c.logger == nil {
		return
	}

	c.logger.Errorf("aerospike still overloaded after %v (attempt %d), giving up: %v", c.overloadRetry.maxElapsed, attempt, err)
}
