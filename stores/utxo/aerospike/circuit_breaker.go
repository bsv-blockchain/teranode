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
	"sync"
	"time"
)

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
