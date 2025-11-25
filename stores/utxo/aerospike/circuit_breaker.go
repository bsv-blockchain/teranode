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
