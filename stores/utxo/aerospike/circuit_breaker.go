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
//   - Batch operation errors
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
	types.DEVICE_OVERLOAD,
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

func (cb *circuitBreaker) Allow() bool {
	if cb == nil {
		return true
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	switch cb.state {
	case cbStateClosed:
		return true
	case cbStateOpen:
		if now.After(cb.nextAttempt) {
			cb.state = cbStateHalfOpen
			cb.halfOpenAttempts = 1
			cb.consecutiveSuccess = 0
			return true
		}
		return false
	case cbStateHalfOpen:
		if cb.halfOpenAttempts >= cb.halfOpenMax {
			return false
		}
		cb.halfOpenAttempts++
		return true
	default:
		return true
	}
}

func (cb *circuitBreaker) RecordSuccess() {
	if cb == nil {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case cbStateClosed:
		cb.consecutiveFailures = 0
	case cbStateHalfOpen:
		cb.consecutiveSuccess++
		if cb.consecutiveSuccess >= cb.halfOpenMax {
			cb.reset()
		}
	}
}

func (cb *circuitBreaker) RecordFailure() {
	if cb == nil {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.consecutiveSuccess = 0

	switch cb.state {
	case cbStateClosed:
		cb.consecutiveFailures++
		if cb.consecutiveFailures >= cb.failureThreshold {
			cb.trip()
		}
	case cbStateHalfOpen:
		cb.trip()
	}
}

func (cb *circuitBreaker) trip() {
	cb.state = cbStateOpen
	cb.nextAttempt = time.Now().Add(cb.cooldown)
	cb.consecutiveFailures = 0
	cb.halfOpenAttempts = 0
}

func (cb *circuitBreaker) reset() {
	cb.state = cbStateClosed
	cb.consecutiveFailures = 0
	cb.halfOpenAttempts = 0
	cb.consecutiveSuccess = 0
}
