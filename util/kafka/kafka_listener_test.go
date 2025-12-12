package kafka

import (
	"context"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartKafkaControlledListenerStartAndStop(t *testing.T) {
	logger := &mockListenerLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kafkaURL, err := url.Parse("memory://localhost/test-topic")
	require.NoError(t, err)

	kafkaControlChan := make(chan bool, 10)
	listenerCalls := &listenerCallTracker{}

	// Start the controlled listener in a goroutine
	go StartKafkaControlledListener(ctx, logger, "test-group", kafkaControlChan, kafkaURL, listenerCalls.listener)

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)

	// Test starting the listener
	kafkaControlChan <- true
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, 1, listenerCalls.getStartCount())

	// Test stopping the listener
	kafkaControlChan <- false
	time.Sleep(50 * time.Millisecond)

	// Test starting again
	kafkaControlChan <- true
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, 2, listenerCalls.getStartCount())

	// Cancel context to stop
	cancel()
	time.Sleep(50 * time.Millisecond)
}

func TestStartKafkaControlledListenerMultipleStartSignals(t *testing.T) {
	logger := &mockListenerLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kafkaURL, err := url.Parse("memory://localhost/test-topic")
	require.NoError(t, err)

	kafkaControlChan := make(chan bool, 10)
	listenerCalls := &listenerCallTracker{}

	// Start the controlled listener in a goroutine
	go StartKafkaControlledListener(ctx, logger, "test-group", kafkaControlChan, kafkaURL, listenerCalls.listener)

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)

	// Send multiple start signals
	kafkaControlChan <- true
	kafkaControlChan <- true
	kafkaControlChan <- true

	time.Sleep(100 * time.Millisecond)

	// Should only start once
	assert.Equal(t, 1, listenerCalls.getStartCount())

	cancel()
}

func TestStartKafkaControlledListenerStopWithoutStart(t *testing.T) {
	logger := &mockListenerLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kafkaURL, err := url.Parse("memory://localhost/test-topic")
	require.NoError(t, err)

	kafkaControlChan := make(chan bool, 10)
	listenerCalls := &listenerCallTracker{}

	// Start the controlled listener in a goroutine
	go StartKafkaControlledListener(ctx, logger, "test-group", kafkaControlChan, kafkaURL, listenerCalls.listener)

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)

	// Send stop signal without starting
	kafkaControlChan <- false
	time.Sleep(50 * time.Millisecond)

	// Should not have started any listener
	assert.Equal(t, 0, listenerCalls.getStartCount())

	cancel()
}

func TestStartKafkaControlledListenerContextCancellation(t *testing.T) {
	logger := &mockListenerLogger{}
	ctx, cancel := context.WithCancel(context.Background())

	kafkaURL, err := url.Parse("memory://localhost/test-topic")
	require.NoError(t, err)

	kafkaControlChan := make(chan bool, 10)
	listenerCalls := &listenerCallTracker{
		cancelDone: make(chan struct{}),
	}

	// Start the controlled listener in a goroutine
	done := make(chan bool)
	go func() {
		StartKafkaControlledListener(ctx, logger, "test-group", kafkaControlChan, kafkaURL, listenerCalls.listener)
		done <- true
	}()

	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)

	// Start a listener
	kafkaControlChan <- true
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, 1, listenerCalls.getStartCount())

	// Cancel context
	cancel()

	// Wait for the listener to finish handling cancellation
	select {
	case <-listenerCalls.cancelDone:
		// Good, listener finished
	case <-time.After(2 * time.Second):
		t.Fatal("Listener did not finish after context cancellation")
	}

	// Wait for StartKafkaControlledListener to return
	select {
	case <-done:
		// Good, it finished
	case <-time.After(2 * time.Second):
		t.Fatal("StartKafkaControlledListener did not finish after context cancellation")
	}

	// Check that the listener received context cancellation
	assert.Equal(t, 1, listenerCalls.getCancelCount())
}

func TestStartKafkaListenerSuccess(t *testing.T) {
	logger := &mockListenerLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kafkaURL, err := url.Parse("memory://localhost/test-topic")
	require.NoError(t, err)

	consumerCallCount := 0
	consumerFn := func(msg *KafkaMessage) error {
		consumerCallCount++
		return nil
	}

	// Start listener in goroutine since it's blocking
	listenerStarted := make(chan bool)
	go func() {
		listenerStarted <- true
		StartKafkaListener(ctx, logger, kafkaURL, "test-group", true, consumerFn, nil)
	}()

	// Wait for listener to start
	<-listenerStarted
	time.Sleep(50 * time.Millisecond)

	// Cancel to stop listener
	cancel()
	time.Sleep(100 * time.Millisecond)

	// Verify logger was used
	assert.Greater(t, logger.getInfoCount(), 0)
}

func TestStartKafkaListenerInvalidURL(t *testing.T) {
	logger := &mockListenerLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumerFn := func(msg *KafkaMessage) error {
		return nil
	}

	// Test with nil URL - this currently panics, which is a limitation of the current implementation
	// The function should check for nil URL but currently doesn't
	assert.Panics(t, func() {
		StartKafkaListener(ctx, logger, nil, "test-group", true, consumerFn, nil)
	})
}

func TestStartKafkaListenerWithKafkaSettings(t *testing.T) {
	logger := &mockListenerLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kafkaURL, err := url.Parse("memory://localhost/test-topic")
	require.NoError(t, err)

	kafkaSettings := &settings.KafkaSettings{
		EnableTLS:     false,
		TLSSkipVerify: false,
	}

	consumerFn := func(msg *KafkaMessage) error {
		return nil
	}

	// Start listener in goroutine since it's blocking
	listenerStarted := make(chan bool)
	go func() {
		listenerStarted <- true
		StartKafkaListener(ctx, logger, kafkaURL, "test-group", false, consumerFn, kafkaSettings)
	}()

	// Wait for listener to start
	<-listenerStarted
	time.Sleep(50 * time.Millisecond)

	// Cancel to stop listener
	cancel()
	time.Sleep(100 * time.Millisecond)
}

func TestStartKafkaListenerContextCancellation(t *testing.T) {
	logger := &mockListenerLogger{}
	ctx, cancel := context.WithCancel(context.Background())

	kafkaURL, err := url.Parse("memory://localhost/test-topic")
	require.NoError(t, err)

	consumerFn := func(msg *KafkaMessage) error {
		return nil
	}

	// Start listener in goroutine
	listenerFinished := make(chan bool)
	listenerStarted := make(chan bool)
	go func() {
		listenerStarted <- true
		StartKafkaListener(ctx, logger, kafkaURL, "test-group", true, consumerFn, nil)
		listenerFinished <- true
	}()

	// Wait for listener to start
	<-listenerStarted
	time.Sleep(50 * time.Millisecond)

	// Cancel context
	cancel()

	// Wait for listener to finish
	select {
	case <-listenerFinished:
		// Good, listener finished
	case <-time.After(2 * time.Second):
		t.Fatal("Listener didn't finish after context cancellation")
	}
}

func TestStartKafkaListenerMultipleSettings(t *testing.T) {
	logger := &mockListenerLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kafkaURL, err := url.Parse("memory://localhost/test-topic")
	require.NoError(t, err)

	kafkaSettings := &settings.KafkaSettings{
		EnableTLS:     false,
		TLSSkipVerify: false,
	}

	consumerFn := func(msg *KafkaMessage) error {
		return nil
	}

	// Test with kafkaSettings
	listenerStarted := make(chan bool)
	go func() {
		listenerStarted <- true
		StartKafkaListener(ctx, logger, kafkaURL, "test-group", true, consumerFn, kafkaSettings)
	}()

	<-listenerStarted
	time.Sleep(50 * time.Millisecond)

	cancel()
	time.Sleep(100 * time.Millisecond)
}

// Mock implementations for testing

type mockListenerLogger struct {
	infoCount  int
	errorCount int
	mu         sync.Mutex
}

func (m *mockListenerLogger) Debug()                        {}
func (m *mockListenerLogger) Debugf(string, ...interface{}) {}
func (m *mockListenerLogger) Info() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.infoCount++
}
func (m *mockListenerLogger) Infof(string, ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.infoCount++
}
func (m *mockListenerLogger) Warn()                        {}
func (m *mockListenerLogger) Warnf(string, ...interface{}) {}
func (m *mockListenerLogger) Error(...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errorCount++
}
func (m *mockListenerLogger) Errorf(string, ...interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errorCount++
}
func (m *mockListenerLogger) Fatal(...interface{})                         {}
func (m *mockListenerLogger) Fatalf(string, ...interface{})                {}
func (m *mockListenerLogger) LogLevel() int                                { return 0 }
func (m *mockListenerLogger) SetLogLevel(string)                           {}
func (m *mockListenerLogger) New(string, ...ulogger.Option) ulogger.Logger { return m }
func (m *mockListenerLogger) Duplicate(...ulogger.Option) ulogger.Logger   { return m }

func (m *mockListenerLogger) getInfoCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.infoCount
}

func (m *mockListenerLogger) getErrorCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.errorCount
}

type listenerCallTracker struct {
	startCount  int
	cancelCount int
	mu          sync.Mutex
	cancelDone  chan struct{}
}

func (l *listenerCallTracker) listener(ctx context.Context, _ *url.URL, _ string) {
	l.mu.Lock()
	l.startCount++
	l.mu.Unlock()

	// Wait for context cancellation
	<-ctx.Done()

	l.mu.Lock()
	l.cancelCount++
	l.mu.Unlock()

	// Signal that cancellation handling is complete
	if l.cancelDone != nil {
		close(l.cancelDone)
	}
}

func (l *listenerCallTracker) getStartCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.startCount
}

func (l *listenerCallTracker) getCancelCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cancelCount
}
