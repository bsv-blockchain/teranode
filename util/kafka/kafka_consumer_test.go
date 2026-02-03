package kafka

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithRetryAndMoveOn(t *testing.T) {
	maxRetries := 5
	backoffMultiplier := 2
	backoffDuration := time.Second

	option := WithRetryAndMoveOn(maxRetries, backoffMultiplier, backoffDuration)

	opts := &consumerOptions{
		withRetryAndMoveOn:  false,
		withRetryAndStop:    false,
		maxRetries:          3,
		backoffMultiplier:   1,
		backoffDurationType: time.Millisecond,
	}

	option(opts)

	assert.True(t, opts.withRetryAndMoveOn)
	assert.False(t, opts.withRetryAndStop)
	assert.Equal(t, maxRetries, opts.maxRetries)
	assert.Equal(t, backoffMultiplier, opts.backoffMultiplier)
	assert.Equal(t, backoffDuration, opts.backoffDurationType)
}

func TestWithRetryAndStop(t *testing.T) {
	maxRetries := 3
	backoffMultiplier := 3
	backoffDuration := 2 * time.Second
	stopFnCalled := false
	stopFn := func() { stopFnCalled = true }

	option := WithRetryAndStop(maxRetries, backoffMultiplier, backoffDuration, stopFn)

	opts := &consumerOptions{
		withRetryAndMoveOn:  true, // Should be set to false by the option
		withRetryAndStop:    false,
		maxRetries:          1,
		backoffMultiplier:   1,
		backoffDurationType: time.Millisecond,
	}

	option(opts)

	assert.False(t, opts.withRetryAndMoveOn)
	assert.True(t, opts.withRetryAndStop)
	assert.Equal(t, maxRetries, opts.maxRetries)
	assert.Equal(t, backoffMultiplier, opts.backoffMultiplier)
	assert.Equal(t, backoffDuration, opts.backoffDurationType)
	assert.NotNil(t, opts.stopFn)

	// Test that stopFn works
	opts.stopFn()
	assert.True(t, stopFnCalled)
}

func TestNewKafkaConsumerGroup(t *testing.T) {
	logger := &mockLogger{}
	kafkaURL, err := url.Parse("memory://localhost/test-topic")
	require.NoError(t, err)
	cfg := KafkaConsumerConfig{
		Logger:            logger,
		URL:               kafkaURL,
		Topic:             "test-topic",
		ConsumerGroupID:   "test-group",
		AutoCommitEnabled: true,
	}

	consumer, err := NewKafkaConsumerGroup(cfg)

	require.NoError(t, err)
	assert.NotNil(t, consumer)
	assert.Equal(t, cfg.Topic, consumer.Config.Topic)
	assert.Equal(t, cfg.ConsumerGroupID, consumer.Config.ConsumerGroupID)
	assert.Equal(t, cfg.AutoCommitEnabled, consumer.Config.AutoCommitEnabled)
}

func TestNewKafkaConsumerGroupNilConsumerFunction(t *testing.T) {
	logger := &mockLogger{}
	kafkaURL, err := url.Parse("memory://localhost/test-topic")
	require.NoError(t, err)
	cfg := KafkaConsumerConfig{
		Logger:            logger,
		URL:               kafkaURL,
		Topic:             "test-topic",
		ConsumerGroupID:   "test-group",
		AutoCommitEnabled: false,
	}

	consumer, err := NewKafkaConsumerGroup(cfg)
	require.NoError(t, err)
	require.NotNil(t, consumer)

	// Start with nil consumerFn is invalid; Start should return without panicking (consumerFn is only used when messages arrive).
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	consumer.Start(ctx, nil)
	cancel()
	_ = consumer.Close()
}

func TestNewKafkaConsumerGroupFromURLInvalidURL(t *testing.T) {
	logger := &mockLogger{}

	consumer, err := NewKafkaConsumerGroupFromURL(logger, nil, "test-group", true, nil)

	assert.Error(t, err)
	assert.Nil(t, consumer)
	assert.Contains(t, err.Error(), "missing kafka url")
}

func TestNewKafkaConsumerGroupFromURLMemoryScheme(t *testing.T) {
	logger := &mockLogger{}
	kafkaURL, err := url.Parse("memory://localhost/test-topic?partitions=4&replay=1")
	require.NoError(t, err)

	consumer, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL, "test-group", true, nil)

	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	assert.Equal(t, "test-topic", consumer.Config.Topic)
	assert.Equal(t, "test-group", consumer.Config.ConsumerGroupID)
	assert.Equal(t, 4, consumer.Config.Partitions)
	assert.True(t, consumer.Config.AutoCommitEnabled)
	assert.True(t, consumer.Config.Replay)
}

func TestNewKafkaConsumerGroupFromURLDefaultValues(t *testing.T) {
	logger := &mockLogger{}
	kafkaURL, err := url.Parse("memory://localhost/test-topic")
	require.NoError(t, err)

	consumer, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL, "test-group", false, nil)

	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	assert.Equal(t, 1, consumer.Config.Partitions) // default partitions
	assert.False(t, consumer.Config.AutoCommitEnabled)
	assert.True(t, consumer.Config.Replay) // default replay=1
}

func TestKafkaConsumerGroupClose(t *testing.T) {
	logger := &mockLogger{}
	kafkaURL, err := url.Parse("memory://localhost/test-topic")
	require.NoError(t, err)

	consumer, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL, "test-group", true, nil)
	require.NoError(t, err)
	require.NotNil(t, consumer)

	err = consumer.Close()
	assert.NoError(t, err)
}

func TestKafkaConsumerGroupBrokersURL(t *testing.T) {
	brokersURL := []string{"broker1:9092", "broker2:9092"}
	consumer := &KafkaConsumerGroup{
		Config: KafkaConsumerConfig{
			BrokersURL: brokersURL,
		},
	}

	result := consumer.BrokersURL()

	assert.Equal(t, brokersURL, result)
}

func TestNewKafkaConsumerGroupValidationErrors(t *testing.T) {
	logger := &mockLogger{}

	tests := []struct {
		name   string
		config KafkaConsumerConfig
		errMsg string
	}{
		{
			name: "Missing URL",
			config: KafkaConsumerConfig{
				Logger:          logger,
				ConsumerGroupID: "test-group",
			},
			errMsg: "kafka URL is not set",
		},
		{
			name: "Missing logger",
			config: KafkaConsumerConfig{
				URL:             &url.URL{Scheme: "memory"},
				ConsumerGroupID: "test-group",
			},
			errMsg: "logger is not set",
		},
		{
			name: "Missing group ID",
			config: KafkaConsumerConfig{
				URL:    &url.URL{Scheme: "memory"},
				Logger: logger,
			},
			errMsg: "group ID is not set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			consumer, err := NewKafkaConsumerGroup(tt.config)

			assert.Error(t, err)
			assert.Nil(t, consumer)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

// Mock implementations for testing

type mockLogger struct {
	warnCount int
}

func (m *mockLogger) Debug()                                          {}
func (m *mockLogger) Debugf(string, ...interface{})                   {}
func (m *mockLogger) Info()                                           {}
func (m *mockLogger) Infof(string, ...interface{})                    {}
func (m *mockLogger) Warn()                                           { m.warnCount++ }
func (m *mockLogger) Warnf(string, ...interface{})                    { m.warnCount++ }
func (m *mockLogger) Error(...interface{})                            {}
func (m *mockLogger) Errorf(string, ...interface{})                   {}
func (m *mockLogger) Fatal(...interface{})                            {}
func (m *mockLogger) Fatalf(string, ...interface{})                   {}
func (m *mockLogger) LogLevel() int                                   { return 0 }
func (m *mockLogger) SetLogLevel(string)                              {}
func (m *mockLogger) New(string, ...ulogger.Option) ulogger.Logger    { return m }
func (m *mockLogger) Duplicate(...ulogger.Option) ulogger.Logger      { return m }
func (m *mockLogger) WithTraceContext(context.Context) ulogger.Logger { return m }

// Watchdog tests

func TestConsumeWatchdogMarkConsumeStarted(t *testing.T) {
	watchdog := &consumeWatchdog{}

	watchdog.markConsumeStarted()

	// Verify that the watchdog is attempting to consume
	assert.True(t, watchdog.isAttemptingConsume.Load())

	// Verify that consume start time was set
	startTime, ok := watchdog.consumeStartTime.Load().(time.Time)
	assert.True(t, ok)
	assert.False(t, startTime.IsZero())

	// Verify that setup called time was reset
	setupTime, ok := watchdog.setupCalledTime.Load().(time.Time)
	assert.True(t, ok)
	assert.True(t, setupTime.IsZero())
}

func TestConsumeWatchdogMarkSetupCalled(t *testing.T) {
	watchdog := &consumeWatchdog{}

	// First mark consume started
	watchdog.markConsumeStarted()
	assert.True(t, watchdog.isAttemptingConsume.Load())

	// Then mark setup called
	watchdog.markSetupCalled()

	// Verify that the watchdog is no longer attempting to consume
	assert.False(t, watchdog.isAttemptingConsume.Load())

	// Verify that setup called time was set
	setupTime, ok := watchdog.setupCalledTime.Load().(time.Time)
	assert.True(t, ok)
	assert.False(t, setupTime.IsZero())
}

func TestConsumeWatchdogMarkConsumeEnded(t *testing.T) {
	watchdog := &consumeWatchdog{}

	// First mark consume started
	watchdog.markConsumeStarted()
	assert.True(t, watchdog.isAttemptingConsume.Load())

	// Then mark consume ended
	watchdog.markConsumeEnded()

	// Verify that the watchdog is no longer attempting to consume
	assert.False(t, watchdog.isAttemptingConsume.Load())
}

func TestConsumeWatchdogIsStuckInRefreshMetadata_NotAttempting(t *testing.T) {
	watchdog := &consumeWatchdog{}

	// Don't mark consume started - should not be stuck
	stuck, duration := watchdog.isStuckInRefreshMetadata(10 * time.Second)

	assert.False(t, stuck)
	assert.Equal(t, time.Duration(0), duration)
}

func TestConsumeWatchdogIsStuckInRefreshMetadata_SetupCalled(t *testing.T) {
	watchdog := &consumeWatchdog{}

	// Mark consume started
	watchdog.markConsumeStarted()

	// Wait a bit then mark setup called
	time.Sleep(10 * time.Millisecond)
	watchdog.markSetupCalled()

	// Should not be stuck because setup was called
	stuck, duration := watchdog.isStuckInRefreshMetadata(5 * time.Millisecond)

	assert.False(t, stuck)
	assert.Equal(t, time.Duration(0), duration)
}

func TestConsumeWatchdogIsStuckInRefreshMetadata_BelowThreshold(t *testing.T) {
	watchdog := &consumeWatchdog{}

	// Mark consume started
	watchdog.markConsumeStarted()

	// Wait less than threshold
	time.Sleep(10 * time.Millisecond)

	// Should not be stuck because duration is below threshold
	stuck, duration := watchdog.isStuckInRefreshMetadata(100 * time.Millisecond)

	assert.False(t, stuck)
	assert.Greater(t, duration, time.Duration(0))
	assert.Less(t, duration, 100*time.Millisecond)
}

func TestConsumeWatchdogIsStuckInRefreshMetadata_AboveThreshold(t *testing.T) {
	watchdog := &consumeWatchdog{}

	// Mark consume started
	watchdog.markConsumeStarted()

	// Wait more than threshold
	time.Sleep(50 * time.Millisecond)

	// Should be stuck because duration exceeds threshold and setup was not called
	stuck, duration := watchdog.isStuckInRefreshMetadata(10 * time.Millisecond)

	assert.True(t, stuck)
	assert.Greater(t, duration, 10*time.Millisecond)
}

func TestConsumeWatchdogIsStuckInRefreshMetadata_ZeroStartTime(t *testing.T) {
	watchdog := &consumeWatchdog{}

	// Set isAttemptingConsume to true but don't set start time
	watchdog.isAttemptingConsume.Store(true)

	// Should not be stuck because start time is not set
	stuck, duration := watchdog.isStuckInRefreshMetadata(10 * time.Millisecond)

	assert.False(t, stuck)
	assert.Equal(t, time.Duration(0), duration)
}

func TestConsumeWatchdogSequence_NormalFlow(t *testing.T) {
	watchdog := &consumeWatchdog{}

	// 1. Consume starts
	watchdog.markConsumeStarted()
	assert.True(t, watchdog.isAttemptingConsume.Load())

	// 2. Some time passes (simulating RefreshMetadata)
	time.Sleep(10 * time.Millisecond)

	// 3. Setup is called successfully
	watchdog.markSetupCalled()
	assert.False(t, watchdog.isAttemptingConsume.Load())

	// 4. Should not be stuck
	stuck, _ := watchdog.isStuckInRefreshMetadata(5 * time.Millisecond)
	assert.False(t, stuck)
}

func TestForceRecovery_WatchdogIntegration(t *testing.T) {
	// Create a watchdog that appears stuck
	watchdog := &consumeWatchdog{}
	watchdog.markConsumeStarted()
	assert.True(t, watchdog.isAttemptingConsume.Load())

	// After simulated recovery, watchdog should be reset
	watchdog.markConsumeEnded()
	assert.False(t, watchdog.isAttemptingConsume.Load())

	// Verify watchdog correctly detects stuck state
	watchdog.markConsumeStarted()
	time.Sleep(10 * time.Millisecond)
	stuck, duration := watchdog.isStuckInRefreshMetadata(5 * time.Millisecond)
	assert.True(t, stuck)
	assert.Greater(t, duration, 5*time.Millisecond)
}

func TestForceRecovery_MutexProtectsConcurrentCalls(t *testing.T) {
	logger := &mockLogger{}
	consumer := &KafkaConsumerGroup{
		Config: KafkaConsumerConfig{
			Logger:          logger,
			Topic:           "test-topic",
			ConsumerGroupID: "test-group",
			BrokersURL:      []string{"localhost:9092"},
		},
		watchdog: &consumeWatchdog{},
	}

	// Launch multiple concurrent force recovery calls; mutex should prevent panics
	const numConcurrent = 5
	done := make(chan bool, numConcurrent)
	for i := 0; i < numConcurrent; i++ {
		go func() {
			_ = consumer.forceRecovery()
			done <- true
		}()
	}
	for i := 0; i < numConcurrent; i++ {
		<-done
	}
}
