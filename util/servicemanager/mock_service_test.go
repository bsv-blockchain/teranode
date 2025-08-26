package servicemanager

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
)

// MockService is a configurable mock implementation of the Service interface
// for testing purposes. It allows controlling various behaviors like delays,
// errors, and health status responses.
type MockService struct {
	name string
	mu   sync.Mutex

	// Configuration for different phases
	initDelay    time.Duration
	initError    error
	startDelay   time.Duration
	startError   error
	stopDelay    time.Duration
	stopError    error
	healthStatus int
	healthError  error

	// State tracking
	initCalled  bool
	startCalled bool
	stopCalled  bool
	readyCh     chan<- struct{}

	// Control channels for testing
	blockInit  chan struct{}
	blockStart chan struct{}
	blockStop  chan struct{}
}

// NewMockService creates a new mock service with the given name.
func NewMockService(name string) *MockService {
	return &MockService{
		name:         name,
		healthStatus: http.StatusOK,
		blockInit:    make(chan struct{}),
		blockStart:   make(chan struct{}),
		blockStop:    make(chan struct{}),
	}
}

// SetInitBehavior configures the Init method behavior.
func (m *MockService) SetInitBehavior(delay time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.initDelay = delay
	m.initError = err
}

// SetStartBehavior configures the Start method behavior.
func (m *MockService) SetStartBehavior(delay time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.startDelay = delay
	m.startError = err
}

// SetStopBehavior configures the Stop method behavior.
func (m *MockService) SetStopBehavior(delay time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopDelay = delay
	m.stopError = err
}

// SetHealthBehavior configures the Health method behavior.
func (m *MockService) SetHealthBehavior(status int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.healthStatus = status
	m.healthError = err
}

// BlockInit blocks the Init method until UnblockInit is called.
func (m *MockService) BlockInit() {
	// Don't send to channel, just let Init wait on it
}

// UnblockInit unblocks the Init method.
func (m *MockService) UnblockInit() {
	select {
	case m.blockInit <- struct{}{}:
	default:
	}
}

// BlockStart blocks the Start method until UnblockStart is called.
func (m *MockService) BlockStart() {
	// Don't send to channel, just let Start wait on it
}

// UnblockStart unblocks the Start method.
func (m *MockService) UnblockStart() {
	select {
	case m.blockStart <- struct{}{}:
	default:
	}
}

// BlockStop blocks the Stop method until UnblockStop is called.
func (m *MockService) BlockStop() {
	// Don't send to channel, just let Stop wait on it
}

// UnblockStop unblocks the Stop method.
func (m *MockService) UnblockStop() {
	select {
	case m.blockStop <- struct{}{}:
	default:
	}
}

// Init implements the Service interface.
func (m *MockService) Init(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.initCalled = true

	if m.initDelay > 0 {
		select {
		case <-time.After(m.initDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Check for blocking behavior
	select {
	case <-m.blockInit:
	default:
	}

	return m.initError
}

// Start implements the Service interface.
func (m *MockService) Start(ctx context.Context, ready chan<- struct{}) error {
	m.mu.Lock()
	m.startCalled = true
	m.readyCh = ready
	startDelay := m.startDelay
	startError := m.startError
	m.mu.Unlock()

	if startDelay > 0 {
		select {
		case <-time.After(startDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Signal ready before checking for errors
	if ready != nil {
		select {
		case ready <- struct{}{}:
		default:
		}
	}

	// Check for blocking behavior
	select {
	case <-m.blockStart:
	default:
	}

	if startError != nil {
		return startError
	}

	// Keep running until context is canceled
	<-ctx.Done()
	return ctx.Err()
}

// Stop implements the Service interface.
func (m *MockService) Stop(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.stopCalled = true

	if m.stopDelay > 0 {
		select {
		case <-time.After(m.stopDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Check for blocking behavior
	select {
	case <-m.blockStop:
	default:
	}

	return m.stopError
}

// Health implements the Service interface.
func (m *MockService) Health(context.Context, bool) (int, string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.healthStatus, `"healthy"`, m.healthError
}

// WasCalled returns whether each method was called.
func (m *MockService) WasCalled() (init, start, stop bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.initCalled, m.startCalled, m.stopCalled
}

// Reset clears all call tracking and resets state.
func (m *MockService) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.initCalled = false
	m.startCalled = false
	m.stopCalled = false
	m.readyCh = nil
}

// FailingMockService is a convenience mock that always fails.
type FailingMockService struct {
	*MockService
	phase string
}

// NewFailingMockService creates a mock service that fails during the specified phase.
func NewFailingMockService(name, phase string) *FailingMockService {
	mock := NewMockService(name)
	failErr := errors.NewServiceError("mock service failure")

	switch phase {
	case "init":
		mock.SetInitBehavior(0, failErr)
	case "start":
		mock.SetStartBehavior(0, failErr)
	case "stop":
		mock.SetStopBehavior(0, failErr)
	case "health":
		mock.SetHealthBehavior(http.StatusServiceUnavailable, failErr)
	}

	return &FailingMockService{
		MockService: mock,
		phase:       phase,
	}
}
