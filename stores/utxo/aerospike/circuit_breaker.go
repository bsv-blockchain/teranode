// Package aerospike provides circuit breaker functionality for spend operations.
//
// # Circuit Breaker Purpose
//
// The circuit breaker provides fail-fast behavior to prevent cascading failures when
// Aerospike infrastructure is unhealthy. It tracks infrastructure-level failures and
// temporarily rejects all spend requests during cooldown periods to give the system
// time to recover.
//
// # Circuit Breaker vs Semaphore - Different Purposes
//
// Protection Layer     | Purpose                        | When It Acts
// -------------------- | ------------------------------ | --------------------------------------------
// Semaphore (client.go)| Limits concurrency             | Always - controls how many operations run simultaneously
// Circuit Breaker      | Prevents cascading failures    | Only when Aerospike is failing - stops all requests when system is unhealthy
//
// # When Circuit Breaker Triggers
//
// The circuit breaker only tracks REAL infrastructure failures from Aerospike batch operations,
// not business logic errors. It will NOT trigger on:
//   - UTXO already spent errors
//   - Frozen UTXO errors
//   - Conflicting transaction errors
//   - Other business validation errors
//
// It WILL trigger on:
//   - Aerospike connection failures
//   - Batch operation errors, whether they fail the whole batch or one record
//   - Timeout errors at the database level
//
// Data-state result codes (KEY_NOT_FOUND_ERROR, FILTERED_OUT, etc.) are
// explicitly excluded from the failure counter — see isInfrastructureFailure
// for the exact allow-list. Issue #953.
//
// # Circuit Breaker States
//
// 1. CLOSED: Normal operation, requests flow through
// 2. OPEN: After N consecutive failures, all requests are rejected immediately
// 3. HALF-OPEN: After cooldown period, limited requests are allowed to probe recovery
//
// # Configuration
//
// The circuit breaker is configurable and optional:
//   - SpendCircuitBreakerFailureCount: Number of consecutive failures before opening (0 = disabled)
//   - SpendCircuitBreakerHalfOpenMax: Number of successful probes required to fully recover
//   - SpendCircuitBreakerCooldown: Time to wait before attempting recovery
//
// # When to Use
//
// Keep it if:
//   - You've experienced Aerospike cascading failures in production
//   - You need aggressive fail-fast behavior during infrastructure issues
//   - You want to give the database time to recover without request hammering
//
// Remove it if:
//   - You prefer to let the semaphore + Aerospike's own resilience handle failures
//   - It's causing false positives or being too aggressive
//   - You need the spend system to remain available even during partial Aerospike issues
package aerospike

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/teranode/errors"
)

// infrastructureResultCodes is the allow-list of aerospike ResultCodes that
// count as infrastructure failure for the spend circuit breaker.
//
// All entries here are node- or cluster-level failure modes. Data-state
// codes (KEY_NOT_FOUND_ERROR, FILTERED_OUT, GENERATION_ERROR, etc.) are
// intentionally excluded — they're handled by per-record error paths in
// the calling code (missing parents flow into subtreevalidation's
// sequential revalidation pass; Lua business-rule rejections surface to
// the caller), not by this breaker. Counting them here causes the breaker
// to trip during normal IBD and stall sync (#953).
//
// DEVICE_OVERLOAD is also excluded: it is owned by the uaerospike
// overload-retry layer, which re-issues it under a bounded budget. By the
// time it surfaces here the retry has already given up, so tripping the
// breaker into reject mode would only deepen the stall the retry exists to
// avoid. MAX_ERROR_RATE is retried by that layer too but is kept here: it is
// a client-side per-node error-rate signal that also fires for non-overload
// node failures, so it remains a meaningful infrastructure indicator.
var infrastructureResultCodes = []types.ResultCode{
	types.TIMEOUT,
	types.NETWORK_ERROR,
	types.NO_RESPONSE,
	types.MAX_RETRIES_EXCEEDED,
	types.MAX_ERROR_RATE,
	types.NO_AVAILABLE_CONNECTIONS_TO_NODE,
	types.SERVER_NOT_AVAILABLE,
	types.INVALID_NODE_ERROR,
	types.PARTITION_UNAVAILABLE,
	types.SERVER_MEM_ERROR,
	types.SERVER_ERROR,
	types.BATCH_FAILED,
	types.GRPC_ERROR,
}

// isInfrastructureFailure reports whether err represents an Aerospike
// infrastructure failure that should count toward the spend circuit breaker.
//
// Matches against the aerospike.Error interface (not the *AerospikeError
// concrete type) so that the client's constant sentinels exposed as
// *constAerospikeError — ErrTimeout, ErrNetwork, ErrMaxRetriesExceeded,
// ErrConnectionPoolEmpty, etc. — are classified consistently with the
// equivalent ResultCode-bearing *AerospikeError instances. See
// stores/utxo/aerospike/send_store_batch_test.go for prior evidence that
// the per-record path receives both types.
//
// Non-Aerospike errors are only treated as infrastructure when they are
// stdlib timeout signals (context.DeadlineExceeded or a net.Error with
// Timeout()=true). Anything else defaults to non-infrastructure: the bias
// is against false positives because a false trip stalls sync, while a
// missed signal is recoverable by higher-level timeouts and health checks.
func isInfrastructureFailure(err error) bool {
	if err == nil {
		return false
	}

	var aErr aerospike.Error
	if errors.As(err, &aErr) {
		if aErr.Matches(infrastructureResultCodes...) {
			return true
		}
		return false
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	return false
}

type cbState string

const (
	cbStateClosed   cbState = "closed"
	cbStateOpen     cbState = "open"
	cbStateHalfOpen cbState = "half-open"
)

// circuitBreaker is a lightweight helper to guard Aerospike spend batches from cascading failures.
type circuitBreaker struct {
	mu               sync.Mutex
	state            cbState
	failureThreshold int
	halfOpenMax      int
	cooldown         time.Duration

	consecutiveFailures int
	halfOpenAttempts    int
	consecutiveSuccess  int
	nextAttempt         time.Time

	// halfOpenEpoch advances on every entry to half-open and every trip, so a
	// slot handed out in one half-open epoch cannot be released into another.
	halfOpenEpoch uint64
}

// breakerSlot records whether an admission took a half-open probe slot and the
// half-open epoch it took it in. The zero value took no slot.
type breakerSlot struct {
	epoch uint64
	taken bool
}

func newCircuitBreaker(failureThreshold, halfOpenMax int, cooldown time.Duration) *circuitBreaker {
	if failureThreshold <= 0 {
		return nil
	}
	if halfOpenMax <= 0 {
		halfOpenMax = 1
	}
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}

	return &circuitBreaker{
		state:            cbStateClosed,
		failureThreshold: failureThreshold,
		halfOpenMax:      halfOpenMax,
		cooldown:         cooldown,
	}
}

// Admit reports whether a request may proceed. The slot is taken only when the
// admission consumed a half-open probe slot. Pass it to RecordSuccess or
// RecordFailure when the request reports an outcome, or to Release when it never will.
func (cb *circuitBreaker) Admit() (bool, breakerSlot) {
	if cb == nil {
		return true, breakerSlot{}
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	switch cb.state {
	case cbStateClosed:
		return true, breakerSlot{}
	case cbStateOpen:
		if now.After(cb.nextAttempt) {
			cb.enterHalfOpen(now)
			return true, breakerSlot{epoch: cb.halfOpenEpoch, taken: true}
		}
		return false, breakerSlot{}
	case cbStateHalfOpen:
		if cb.halfOpenAttempts >= cb.halfOpenMax {
			// Backstop: a probe that never reports must not hold half-open
			// shut for ever. One cooldown after entry, probe again.
			if now.After(cb.nextAttempt.Add(cb.cooldown)) {
				cb.enterHalfOpen(now)
				return true, breakerSlot{epoch: cb.halfOpenEpoch, taken: true}
			}
			return false, breakerSlot{}
		}
		cb.halfOpenAttempts++
		return true, breakerSlot{epoch: cb.halfOpenEpoch, taken: true}
	default:
		return true, breakerSlot{}
	}
}

// enterHalfOpen starts a new probe epoch. consecutiveSuccess is kept: one epoch
// admits halfOpenMax inputs but a probe that gets no usable answer is released
// rather than counted, and a probe that never completes holds its slot until the
// backstop, so an epoch can report fewer successes than slots. Only a trip or a
// reset zeroes the count, so the successes of several epochs add up and the
// breaker closes.
func (cb *circuitBreaker) enterHalfOpen(now time.Time) {
	cb.state = cbStateHalfOpen
	cb.halfOpenEpoch++
	// Probes of the previous epoch may still be in flight, so more than halfOpenMax
	// probes can reach an unhealthy backend at once. The overlap is bounded by the
	// request timeout. Accepted: this is a backstop, and the outcomes of those
	// probes no longer apply here.
	cb.halfOpenAttempts = 1
	cb.nextAttempt = now
}

// Release gives back a half-open probe slot taken by Admit for a request that
// will never report an outcome. It does nothing for a slot that was not taken
// or that belongs to an earlier half-open epoch.
func (cb *circuitBreaker) Release(slot breakerSlot) {
	if cb == nil || !slot.taken {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == cbStateHalfOpen && slot.epoch == cb.halfOpenEpoch && cb.halfOpenAttempts > 0 {
		cb.halfOpenAttempts--
	}
}

// RecordSuccess reports a successful outcome of the request admitted with slot.
// In the closed state it resets the failure count. In the half-open state it
// counts toward closing only when slot is a probe slot of the current half-open
// epoch; an outcome of a request admitted while closed, or in an earlier epoch,
// is ignored.
func (cb *circuitBreaker) RecordSuccess(slot breakerSlot) {
	if cb == nil {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case cbStateClosed:
		cb.consecutiveFailures = 0
	case cbStateHalfOpen:
		if !cb.currentProbe(slot) {
			return
		}

		cb.consecutiveSuccess++
		if cb.consecutiveSuccess >= cb.halfOpenMax {
			cb.reset()
		}
	}
}

// RecordFailure reports an infrastructure failure of the request admitted with
// slot. See RecordBatchFailure.
func (cb *circuitBreaker) RecordFailure(slot breakerSlot) {
	cb.RecordBatchFailure([]breakerSlot{slot})
}

// RecordBatchFailure reports one infrastructure failure that covers the requests
// admitted with slots, for example the batch they travelled in. In the closed
// state it counts once toward tripping, whatever the slots. In the half-open
// state it trips the breaker when any slot is a probe slot of the current epoch,
// and is ignored otherwise. The success count is zeroed only when the breaker
// trips or resets, never by an ignored or closed-state failure.
func (cb *circuitBreaker) RecordBatchFailure(slots []breakerSlot) {
	if cb == nil {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case cbStateClosed:
		cb.consecutiveFailures++
		if cb.consecutiveFailures >= cb.failureThreshold {
			cb.trip()
		}
	case cbStateHalfOpen:
		for _, slot := range slots {
			if cb.currentProbe(slot) {
				cb.trip()
				return
			}
		}
	}
}

// currentProbe reports whether slot is a probe slot of the current half-open epoch.
func (cb *circuitBreaker) currentProbe(slot breakerSlot) bool {
	return slot.taken && slot.epoch == cb.halfOpenEpoch
}

func (cb *circuitBreaker) trip() {
	cb.state = cbStateOpen
	cb.halfOpenEpoch++
	cb.nextAttempt = time.Now().Add(cb.cooldown)
	cb.consecutiveFailures = 0
	cb.halfOpenAttempts = 0
	cb.consecutiveSuccess = 0
}

func (cb *circuitBreaker) reset() {
	cb.state = cbStateClosed
	cb.consecutiveFailures = 0
	cb.halfOpenAttempts = 0
	cb.consecutiveSuccess = 0
}

// spendBreakerOutcome coalesces repeated expression outcomes across parts until
// an opposite outcome is recorded. An uninterrupted run of expression timeouts
// counts once per batcher batch, but a success re-arms failure recording so a
// later timeout cannot disappear after the breaker was reset. Outcomes are
// recorded before Lua retries, preserving their processing order.
// Lua records always count, even when a later part reuses the same record key.
// An outcome that carries half-open probe slots is never coalesced, because
// each probe must report.
type spendBreakerOutcome struct {
	success bool
	failure bool
}

func (o *spendBreakerOutcome) recordExpressions(cb *circuitBreaker, success, failure bool, successSlots, failureSlots []breakerSlot) {
	if success && (!o.success || len(successSlots) > 0) {
		o.record(cb, true, successSlots...)
		o.success = true
	}

	if failure && (!o.failure || len(failureSlots) > 0) {
		o.record(cb, false, failureSlots...)
		o.failure = true
	}
}

// record also re-arms the opposite expression outcome: a Lua success resets
// the breaker, so a subsequent expression failure must count again (and vice versa).
// A success reports once per slot, or once without a slot when none is given. A
// failure reports once, with the first slot.
func (o *spendBreakerOutcome) record(cb *circuitBreaker, success bool, slots ...breakerSlot) {
	if success {
		o.failure = false

		if len(slots) == 0 {
			cb.RecordSuccess(breakerSlot{})
		}

		for _, slot := range slots {
			cb.RecordSuccess(slot)
		}

		return
	}

	o.success = false

	cb.RecordBatchFailure(slots)
}

// serverAnswerResultCodes are the ResultCodes that prove the server evaluated
// the request and answered with data state. A half-open probe that gets one of
// them counts as a success. Any other non-infrastructure error is weak evidence
// of health: DEVICE_OVERLOAD is owned by the overload-retry layer, and client
// side errors (PARAMETER_ERROR, SERIALIZE_ERROR, a canceled context) never
// reached a healthy server, so those probes release their slot instead.
var serverAnswerResultCodes = []types.ResultCode{
	types.KEY_NOT_FOUND_ERROR,
	types.KEY_EXISTS_ERROR,
	types.GENERATION_ERROR,
	types.BIN_EXISTS_ERROR,
	types.BIN_TYPE_ERROR,
	types.BIN_NOT_FOUND,
	types.RECORD_TOO_BIG,
	types.OP_NOT_APPLICABLE,
	types.FAIL_ELEMENT_NOT_FOUND,
	types.FAIL_ELEMENT_EXISTS,
	types.FILTERED_OUT,
	types.LOST_CONFLICT,
	types.UDF_BAD_RESPONSE,
}

// isServerAnswer reports whether err is a data-state answer from the server.
func isServerAnswer(err error) bool {
	var aErr aerospike.Error

	return errors.As(err, &aErr) && aErr.Matches(serverAnswerResultCodes...)
}
