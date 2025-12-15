package catchup

import (
	"sync"
)

// PeerCircuitBreakers manages circuit breakers for multiple peers
type PeerCircuitBreakers struct {
	mu       sync.RWMutex
	breakers map[string]*CircuitBreaker // Key is PeerID
	config   CircuitBreakerConfig
}

// NewPeerCircuitBreakers creates a new peer circuit breaker manager
func NewPeerCircuitBreakers(config CircuitBreakerConfig) *PeerCircuitBreakers {
	return &PeerCircuitBreakers{
		breakers: make(map[string]*CircuitBreaker),
		config:   config,
	}
}

// GetBreaker gets or creates a circuit breaker for a peer
func (pcb *PeerCircuitBreakers) GetBreaker(peerID string) *CircuitBreaker {
	pcb.mu.Lock()
	defer pcb.mu.Unlock()

	if breaker, exists := pcb.breakers[peerID]; exists {
		return breaker
	}

	breaker := NewCircuitBreaker(pcb.config)
	pcb.breakers[peerID] = breaker
	return breaker
}

// Reset resets all circuit breakers
func (pcb *PeerCircuitBreakers) Reset() {
	pcb.mu.Lock()
	defer pcb.mu.Unlock()

	for _, breaker := range pcb.breakers {
		breaker.Reset()
	}
}

// ResetPeer resets the circuit breaker for a specific peer
func (pcb *PeerCircuitBreakers) ResetPeer(peerID string) {
	pcb.mu.Lock()
	defer pcb.mu.Unlock()

	if breaker, exists := pcb.breakers[peerID]; exists {
		breaker.Reset()
	}
}

// GetPeerState gets the state of a circuit breaker for a peer
// Returns 0 for closed, 1 for open, 2 for half-open
func (pcb *PeerCircuitBreakers) GetPeerState(peerID string) CircuitBreakerState {
	pcb.mu.RLock()
	defer pcb.mu.RUnlock()

	if breaker, exists := pcb.breakers[peerID]; exists {
		return breaker.GetState()
	}

	// Return StateClosed if breaker doesn't exist yet
	return StateClosed
}
