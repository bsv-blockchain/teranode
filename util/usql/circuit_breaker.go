// Package usql provides circuit breaker functionality for SQL database operations.
//
// # Circuit Breaker Purpose
//
// The circuit breaker provides fail-fast behavior to prevent cascading failures when
// the database infrastructure is unhealthy. It tracks infrastructure-level failures and
// temporarily rejects all requests during cooldown periods to give the system
// time to recover.
//
// # Circuit Breaker States
//
// 1. CLOSED (normal): Requests pass through, failures tracked
// 2. OPEN (failing): Fast-fail immediately, no DB calls attempted
// 3. HALF-OPEN (testing): Allow limited probe requests to test recovery
//
// # When Circuit Breaker Triggers
//
// The circuit breaker only tracks REAL infrastructure failures from database operations,
// not business logic errors. It will trigger on:
//   - Connection failures
//   - Timeout errors
//   - Database unavailable errors
//
// # Configuration
//
// The circuit breaker is configurable and optional:
//   - FailureThreshold: Number of consecutive failures before opening (0 = disabled)
//   - HalfOpenMax: Number of successful probes required to fully recover
//   - Cooldown: Time to wait before attempting recovery
//   - FailureWindow: Time window within which failures must occur to trip the circuit
package usql

import (
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
)

// CircuitState represents the current state of the circuit breaker.
type CircuitState int

const (
	// CircuitClosed is the normal operating state where requests pass through.
	CircuitClosed CircuitState = iota
	// CircuitOpen is the failing state where requests are rejected immediately.
	CircuitOpen
	// CircuitHalfOpen is the recovery testing state where limited probes are allowed.
	CircuitHalfOpen
)

// String returns a string representation of the circuit state.
func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// ErrCircuitOpen is returned when the circuit breaker is open and rejecting requests.
var ErrCircuitOpen = errors.NewError("circuit breaker is open")

// CircuitBreakerConfig holds configuration for circuit breaker behavior.
type CircuitBreakerConfig struct {
	// FailureThreshold is the number of consecutive failures before opening the circuit.
	// Set to 0 to disable the circuit breaker.
	FailureThreshold int

	// HalfOpenMax is the number of successful requests required to close the circuit
	// from half-open state.
	HalfOpenMax int

	// Cooldown is the duration the circuit stays open before transitioning to half-open.
	Cooldown time.Duration

	// FailureWindow is the time window within which consecutive failures must occur
	// to trip the circuit. Failures older than this window are not counted.
	FailureWindow time.Duration

	// Enabled controls whether the circuit breaker is active.
	Enabled bool

	// OnStateChange is an optional callback invoked when the circuit state changes.
	// The callback receives the old state, new state, and a reason for the change.
	// The callback runs asynchronously in a separate goroutine and receives all
	// necessary state information as parameters.
	OnStateChange func(from, to CircuitState, reason string)
}

// CircuitBreaker implements the circuit breaker pattern for database operations.
type CircuitBreaker struct {
	mu     sync.Mutex
	config CircuitBreakerConfig

	state               CircuitState
	consecutiveFailures int
	consecutiveSuccess  int
	halfOpenAttempts    int
	lastFailTime        time.Time
	nextAttempt         time.Time
	firstFailTime       time.Time // Track when the failure window started
}

// NewCircuitBreaker creates a new circuit breaker with the given configuration.
// Returns nil if the circuit breaker is disabled or not properly configured.
// All configuration values must be explicitly set - zero values are not defaulted.
func NewCircuitBreaker(config CircuitBreakerConfig) *CircuitBreaker {
	// Circuit breaker is disabled if:
	// - Enabled is false
	// - FailureThreshold is 0 or negative
	// - HalfOpenMax is 0 or negative
	// - Cooldown is 0 or negative
	// - FailureWindow is 0 or negative
	if !config.Enabled ||
		config.FailureThreshold <= 0 ||
		config.HalfOpenMax <= 0 ||
		config.Cooldown <= 0 ||
		config.FailureWindow <= 0 {
		return nil
	}

	return &CircuitBreaker{
		config: config,
		state:  CircuitClosed,
	}
}

// Allow checks if a request should be allowed through the circuit breaker.
// Returns true if the request can proceed, false if it should be rejected.
func (cb *CircuitBreaker) Allow() bool {
	if cb == nil {
		return true
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()

	switch cb.state {
	case CircuitClosed:
		return true

	case CircuitOpen:
		// Check if cooldown has elapsed
		if now.After(cb.nextAttempt) {
			cb.transitionTo(CircuitHalfOpen, "cooldown elapsed, testing recovery")
			cb.halfOpenAttempts = 1
			cb.consecutiveSuccess = 0
			return true
		}
		// Still in cooldown, reject
		prometheusCircuitBreakerFastFailed.Inc()
		return false

	case CircuitHalfOpen:
		// Allow limited probes
		if cb.halfOpenAttempts >= cb.config.HalfOpenMax {
			// Already reached max probes, reject until we get results
			prometheusCircuitBreakerFastFailed.Inc()
			return false
		}
		cb.halfOpenAttempts++
		return true

	default:
		return true
	}
}

// RecordSuccess records a successful operation.
func (cb *CircuitBreaker) RecordSuccess() {
	if cb == nil {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		// Reset failure counters on success
		cb.consecutiveFailures = 0
		cb.firstFailTime = time.Time{}

	case CircuitHalfOpen:
		cb.consecutiveSuccess++
		if cb.consecutiveSuccess >= cb.config.HalfOpenMax {
			cb.reset("recovered after successful probes")
		}
	}
}

// RecordFailure records a failed operation.
// Only infrastructure failures (connection errors, timeouts) should be recorded.
func (cb *CircuitBreaker) RecordFailure() {
	if cb == nil {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	cb.lastFailTime = now
	cb.consecutiveSuccess = 0

	switch cb.state {
	case CircuitClosed:
		// Check if we're within the failure window
		if cb.firstFailTime.IsZero() || now.Sub(cb.firstFailTime) > cb.config.FailureWindow {
			// Start a new failure window
			cb.firstFailTime = now
			cb.consecutiveFailures = 1
		} else {
			cb.consecutiveFailures++
		}

		// Check if threshold exceeded
		if cb.consecutiveFailures >= cb.config.FailureThreshold {
			cb.trip("consecutive failures exceeded threshold")
		}

	case CircuitHalfOpen:
		// Any failure in half-open state trips the circuit again
		cb.trip("probe request failed")
	}
}

// State returns the current state of the circuit breaker.
func (cb *CircuitBreaker) State() CircuitState {
	if cb == nil {
		return CircuitClosed
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	return cb.state
}

// Stats returns statistics about the circuit breaker state.
func (cb *CircuitBreaker) Stats() CircuitBreakerStats {
	if cb == nil {
		return CircuitBreakerStats{State: CircuitClosed}
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	return CircuitBreakerStats{
		State:               cb.state,
		ConsecutiveFailures: cb.consecutiveFailures,
		ConsecutiveSuccess:  cb.consecutiveSuccess,
		LastFailTime:        cb.lastFailTime,
		NextAttemptTime:     cb.nextAttempt,
	}
}

// CircuitBreakerStats holds statistics about the circuit breaker.
type CircuitBreakerStats struct {
	State               CircuitState
	ConsecutiveFailures int
	ConsecutiveSuccess  int
	LastFailTime        time.Time
	NextAttemptTime     time.Time
}

// trip opens the circuit breaker.
func (cb *CircuitBreaker) trip(reason string) {
	cb.transitionTo(CircuitOpen, reason)
	cb.nextAttempt = time.Now().Add(cb.config.Cooldown)
	cb.consecutiveFailures = 0
	cb.halfOpenAttempts = 0
	cb.firstFailTime = time.Time{}

	prometheusCircuitBreakerOpened.Inc()
}

// reset closes the circuit breaker (returns to normal operation).
func (cb *CircuitBreaker) reset(reason string) {
	cb.transitionTo(CircuitClosed, reason)
	cb.consecutiveFailures = 0
	cb.halfOpenAttempts = 0
	cb.consecutiveSuccess = 0
	cb.firstFailTime = time.Time{}
}

// transitionTo changes the circuit breaker state and triggers the callback if set.
func (cb *CircuitBreaker) transitionTo(newState CircuitState, reason string) {
	if cb.state == newState {
		return
	}

	oldState := cb.state
	cb.state = newState

	// Update state gauge
	prometheusCircuitBreakerState.Set(float64(newState))

	// Trigger callback if set
	if cb.config.OnStateChange != nil {
		// Call in goroutine to avoid blocking
		go cb.config.OnStateChange(oldState, newState, reason)
	}
}

// Execute runs a function with circuit breaker protection.
// If the circuit is open, returns ErrCircuitOpen without executing the function.
// Records success or failure based on the function result.
func (cb *CircuitBreaker) Execute(fn func() error) error {
	if cb == nil {
		return fn()
	}

	if !cb.Allow() {
		return ErrCircuitOpen
	}

	err := fn()

	if err != nil && isRetriable(err) {
		// Only record infrastructure failures
		cb.RecordFailure()
	} else if err == nil {
		cb.RecordSuccess()
	}

	return err
}
