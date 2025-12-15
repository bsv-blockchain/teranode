package catchup

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCircuitBreaker_InitialState(t *testing.T) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())

	assert.Equal(t, StateClosed, cb.GetState())
	assert.True(t, cb.CanCall())
}

func TestCircuitBreaker_OpensAfterFailureThreshold(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    3,
		SuccessThreshold:    2,
		Timeout:             100 * time.Millisecond,
		MaxHalfOpenRequests: 1,
	}
	cb := NewCircuitBreaker(config)

	// Should stay closed for first failures
	assert.True(t, cb.CanCall())
	cb.RecordFailure()
	assert.Equal(t, StateClosed, cb.GetState())

	assert.True(t, cb.CanCall())
	cb.RecordFailure()
	assert.Equal(t, StateClosed, cb.GetState())

	// Should open after third failure
	assert.True(t, cb.CanCall())
	cb.RecordFailure()
	assert.Equal(t, StateOpen, cb.GetState())

	// Should not allow requests when open
	assert.False(t, cb.CanCall())
}

func TestCircuitBreaker_TransitionsToHalfOpen(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    1,
		SuccessThreshold:    2,
		Timeout:             50 * time.Millisecond,
		MaxHalfOpenRequests: 3,
	}
	cb := NewCircuitBreaker(config)

	// Open the circuit
	cb.RecordFailure()
	assert.Equal(t, StateOpen, cb.GetState())
	assert.False(t, cb.CanCall())

	// Wait for timeout
	time.Sleep(60 * time.Millisecond)

	// Should transition to half-open and allow request
	assert.True(t, cb.CanCall())
	assert.Equal(t, StateHalfOpen, cb.GetState())

	// Should allow limited requests in half-open
	assert.True(t, cb.CanCall())
	assert.True(t, cb.CanCall())

	// Should not allow more than max
	assert.False(t, cb.CanCall())
}

func TestCircuitBreaker_ClosesAfterSuccessThreshold(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    1,
		SuccessThreshold:    2,
		Timeout:             50 * time.Millisecond,
		MaxHalfOpenRequests: 3,
	}
	cb := NewCircuitBreaker(config)

	// Open the circuit
	cb.RecordFailure()
	assert.Equal(t, StateOpen, cb.GetState())

	// Wait for timeout
	time.Sleep(60 * time.Millisecond)

	// Transition to half-open
	assert.True(t, cb.CanCall())
	assert.Equal(t, StateHalfOpen, cb.GetState())

	// Record successes
	cb.RecordSuccess()
	assert.Equal(t, StateHalfOpen, cb.GetState())

	cb.RecordSuccess()
	assert.Equal(t, StateClosed, cb.GetState())

	// Should allow requests when closed
	assert.True(t, cb.CanCall())
}

func TestCircuitBreaker_ReopensOnHalfOpenFailure(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    1,
		SuccessThreshold:    2,
		Timeout:             50 * time.Millisecond,
		MaxHalfOpenRequests: 3,
	}
	cb := NewCircuitBreaker(config)

	// Open the circuit
	cb.RecordFailure()
	assert.Equal(t, StateOpen, cb.GetState())

	// Wait for timeout
	time.Sleep(60 * time.Millisecond)

	// Transition to half-open
	assert.True(t, cb.CanCall())
	assert.Equal(t, StateHalfOpen, cb.GetState())

	// Any failure reopens
	cb.RecordFailure()
	assert.Equal(t, StateOpen, cb.GetState())
	assert.False(t, cb.CanCall())
}

func TestCircuitBreaker_Reset(t *testing.T) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())

	// Open the circuit
	for i := 0; i < 5; i++ {
		cb.RecordFailure()
	}
	assert.Equal(t, StateOpen, cb.GetState())

	// Reset
	cb.Reset()
	assert.Equal(t, StateClosed, cb.GetState())
	assert.True(t, cb.CanCall())

	state, failures, successes, _ := cb.GetStats()
	assert.Equal(t, StateClosed, state)
	assert.Equal(t, 0, failures)
	assert.Equal(t, 0, successes)
}

func TestPeerCircuitBreakers_MultiplePeers(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    2,
		SuccessThreshold:    1,
		Timeout:             50 * time.Millisecond,
		MaxHalfOpenRequests: 1,
	}
	pcb := NewPeerCircuitBreakers(config)

	peer1 := "peer-001"
	peer2 := "peer-002"

	// Both peers should start closed
	breaker1 := pcb.GetBreaker(peer1)
	breaker2 := pcb.GetBreaker(peer2)
	assert.True(t, breaker1.CanCall())
	assert.True(t, breaker2.CanCall())

	// Open circuit for peer1
	breaker1.RecordFailure()
	breaker1.RecordFailure()
	assert.Equal(t, StateOpen, breaker1.GetState())
	assert.False(t, breaker1.CanCall())

	// Peer2 should still be closed
	assert.Equal(t, StateClosed, breaker2.GetState())
	assert.True(t, breaker2.CanCall())

	// Record success for peer2
	breaker2.RecordSuccess()
	assert.Equal(t, StateClosed, breaker2.GetState())
}

func TestPeerCircuitBreakers_Reset(t *testing.T) {
	pcb := NewPeerCircuitBreakers(DefaultCircuitBreakerConfig())

	peer := "peer-test-001"
	breaker := pcb.GetBreaker(peer)

	// Open circuit
	for i := 0; i < 5; i++ {
		breaker.RecordFailure()
	}
	assert.Equal(t, StateOpen, breaker.GetState())

	// Reset specific peer
	pcb.ResetPeer(peer)
	breaker = pcb.GetBreaker(peer) // Get a fresh breaker after reset
	assert.Equal(t, StateClosed, breaker.GetState())
}

func TestPeerCircuitBreakers_ResetAll(t *testing.T) {
	pcb := NewPeerCircuitBreakers(DefaultCircuitBreakerConfig())

	peer1 := "peer-001"
	peer2 := "peer-002"
	breaker1 := pcb.GetBreaker(peer1)
	breaker2 := pcb.GetBreaker(peer2)

	// Open both circuits
	for i := 0; i < 5; i++ {
		breaker1.RecordFailure()
		breaker2.RecordFailure()
	}
	assert.Equal(t, StateOpen, breaker1.GetState())
	assert.Equal(t, StateOpen, breaker2.GetState())

	// Reset all
	pcb.Reset()
	breaker1 = pcb.GetBreaker(peer1) // Get fresh breakers after reset
	breaker2 = pcb.GetBreaker(peer2)
	assert.Equal(t, StateClosed, breaker1.GetState())
	assert.Equal(t, StateClosed, breaker2.GetState())
}

func TestCircuitBreaker_ConcurrentAccess(t *testing.T) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())
	done := make(chan bool)

	// Concurrent failures
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				if cb.CanCall() {
					cb.RecordFailure()
				}
			}
			done <- true
		}()
	}

	// Concurrent successes
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				if cb.CanCall() {
					cb.RecordSuccess()
				}
			}
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 20; i++ {
		<-done
	}

	// Should not panic and state should be valid
	state := cb.GetState()
	assert.Contains(t, []CircuitBreakerState{StateClosed, StateOpen, StateHalfOpen}, state)
}

func TestPeerCircuitBreakers_GetPeerState_DataRace(t *testing.T) {
	config := CircuitBreakerConfig{
		FailureThreshold:    5,
		SuccessThreshold:    2,
		Timeout:             10 * time.Millisecond,
		MaxHalfOpenRequests: 1,
	}
	pcb := NewPeerCircuitBreakers(config)
	peerID := "test-peer-001"

	// Create the breaker
	breaker := pcb.GetBreaker(peerID)

	// Start goroutines that modify breaker.state
	done := make(chan bool, 3)

	// Goroutine 1: Record failures
	go func() {
		for i := 0; i < 1000; i++ {
			breaker.RecordFailure()
		}
		done <- true
	}()

	// Goroutine 2: Record successes
	go func() {
		for i := 0; i < 1000; i++ {
			breaker.RecordSuccess()
		}
		done <- true
	}()

	// Goroutine 3: Call CanCall (which also modifies state)
	go func() {
		for i := 0; i < 1000; i++ {
			breaker.CanCall()
		}
		done <- true
	}()

	// Main goroutine: Continuously read state via GetPeerState
	// This reads breaker.state via thread-safe GetState() method
	readCount := 0
	for {
		select {
		case <-done:
			readCount++
			if readCount >= 3 {
				return // All writers done
			}
		default:
			// This should be safe now - accessing breaker.state
			// via GetState() which holds breaker.mu
			_ = pcb.GetPeerState(peerID)
		}
	}
}
