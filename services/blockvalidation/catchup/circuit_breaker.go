// Package catchup provides circuit breaker functionality for managing peer connections during blockchain catchup.
package catchup

import (
	"sync"
	"time"
)

// CircuitBreakerState represents the current state of a circuit breaker
type CircuitBreakerState string

const (
	// StateClosed allows requests to pass through
	StateClosed CircuitBreakerState = "closed"
	// StateOpen blocks all requests
	StateOpen CircuitBreakerState = "open"
	// StateHalfOpen allows a limited number of requests to test if the service has recovered
	StateHalfOpen CircuitBreakerState = "half-open"
)

// CircuitBreaker protects against cascading failures by monitoring request failures
// and temporarily blocking requests to failing services.
type CircuitBreaker struct {
	mu                  sync.RWMutex
	state               CircuitBreakerState
	failureCount        int
	successCount        int
	lastFailureTime     time.Time
	lastStateChange     time.Time
	failureThreshold    int
	successThreshold    int
	timeout             time.Duration
	halfOpenRequests    int
	maxHalfOpenRequests int
}

// CircuitBreakerConfig holds configuration for a circuit breaker
type CircuitBreakerConfig struct {
	// FailureThreshold is the number of consecutive failures before opening the circuit
	FailureThreshold int
	// SuccessThreshold is the number of consecutive successes in half-open state before closing
	SuccessThreshold int
	// Timeout is how long to wait before transitioning from open to half-open
	Timeout time.Duration
	// MaxHalfOpenRequests is the maximum number of requests allowed in half-open state
	MaxHalfOpenRequests int
}

// DefaultCircuitBreakerConfig returns a default configuration
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		FailureThreshold:    5,
		SuccessThreshold:    2,
		Timeout:             10 * time.Second,
		MaxHalfOpenRequests: 1,
	}
}

// NewCircuitBreaker creates a new circuit breaker with the given configuration
func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{
		state:               StateClosed,
		failureThreshold:    config.FailureThreshold,
		successThreshold:    config.SuccessThreshold,
		timeout:             config.Timeout,
		maxHalfOpenRequests: config.MaxHalfOpenRequests,
		lastStateChange:     time.Now(),
	}
}

// Call executes the given function if the circuit allows it
func (cb *CircuitBreaker) Call(fn func() error) error {
	if !cb.CanCall() {
		return ErrCircuitOpen
	}

	err := fn()
	if err != nil {
		cb.RecordFailure()
	} else {
		cb.RecordSuccess()
	}
	return err
}

// CanCall checks if a request can be made
func (cb *CircuitBreaker) CanCall() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	switch cb.state {
	case StateClosed:
		return true

	case StateOpen:
		if now.Sub(cb.lastStateChange) > cb.timeout {
			cb.state = StateHalfOpen
			cb.halfOpenRequests = 1
			cb.lastStateChange = now
			return true
		}
		return false

	case StateHalfOpen:
		if cb.halfOpenRequests >= cb.maxHalfOpenRequests {
			return false
		}
		cb.halfOpenRequests++
		return true

	default:
		return false
	}
}

// RecordSuccess records a successful request
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		cb.failureCount = 0

	case StateHalfOpen:
		cb.successCount++
		if cb.successCount >= cb.successThreshold {
			cb.state = StateClosed
			cb.failureCount = 0
			cb.successCount = 0
			cb.lastStateChange = time.Now()
		}
	}
}

// RecordFailure records one or more failed requests
// Optional count parameter allows recording multiple failures at once (defaults to 1)
func (cb *CircuitBreaker) RecordFailure(count ...int) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Default to 1 failure if no count provided
	failureCount := 1
	if len(count) > 0 && count[0] > 0 {
		failureCount = count[0]
	}

	cb.lastFailureTime = time.Now()

	switch cb.state {
	case StateClosed:
		cb.failureCount += failureCount
		if cb.failureCount >= cb.failureThreshold {
			cb.state = StateOpen
			cb.lastStateChange = time.Now()
		}

	case StateHalfOpen:
		cb.state = StateOpen
		cb.failureCount = 0
		cb.successCount = 0
		cb.lastStateChange = time.Now()
	}
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// GetStats returns statistics about the circuit breaker
func (cb *CircuitBreaker) GetStats() (state CircuitBreakerState, failures int, successes int, lastFailure time.Time) {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return cb.state, cb.failureCount, cb.successCount, cb.lastFailureTime
}

// Reset resets the circuit breaker to its initial state
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.state = StateClosed
	cb.failureCount = 0
	cb.successCount = 0
	cb.halfOpenRequests = 0
	cb.lastStateChange = time.Now()
}
