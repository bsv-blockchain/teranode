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

	// Verify that lastSuccessfulPollTime is NOT set (never stored on a fresh watchdog)
	pollTime, ok := watchdog.lastSuccessfulPollTime.Load().(time.Time)
	// ok may be false if no value was stored yet — that's expected on first poll
	assert.True(t, !ok || pollTime.IsZero(), "lastSuccessfulPollTime should be zero/unset on first poll (never set)")

	// Simulate a successful poll cycle, then start a new one
	watchdog.markPollSucceeded()
	watchdog.markConsumeStarted()

	// lastSuccessfulPollTime should be preserved from the previous successful poll
	pollTime, ok = watchdog.lastSuccessfulPollTime.Load().(time.Time)
	assert.True(t, ok, "lastSuccessfulPollTime should be a time.Time after markPollSucceeded")
	assert.False(t, pollTime.IsZero(), "lastSuccessfulPollTime should be preserved across polls")
}

func TestConsumeWatchdogMarkPollSucceeded(t *testing.T) {
	watchdog := &consumeWatchdog{}

	// First mark consume started
	watchdog.markConsumeStarted()
	assert.True(t, watchdog.isAttemptingConsume.Load())

	// Then mark poll succeeded
	watchdog.markPollSucceeded()

	// Verify that the watchdog is no longer attempting to consume
	assert.False(t, watchdog.isAttemptingConsume.Load())

	// Verify that lastSuccessfulPollTime was set
	pollTime, ok := watchdog.lastSuccessfulPollTime.Load().(time.Time)
	assert.True(t, ok)
	assert.False(t, pollTime.IsZero())
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

func TestConsumeWatchdogIsStuckInRefreshMetadata_PollSucceeded(t *testing.T) {
	watchdog := &consumeWatchdog{}

	// Mark consume started
	watchdog.markConsumeStarted()

	// Wait a bit then mark poll succeeded
	time.Sleep(10 * time.Millisecond)
	watchdog.markPollSucceeded()

	// Should not be stuck because poll succeeded
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

	// 3. Poll succeeds
	watchdog.markPollSucceeded()
	assert.False(t, watchdog.isAttemptingConsume.Load())

	// 4. Should not be stuck
	stuck, _ := watchdog.isStuckInRefreshMetadata(5 * time.Millisecond)
	assert.False(t, stuck)
}

func TestConsumeWatchdogIdleConsumerNotStuck(t *testing.T) {
	// After a successful first poll, an idle consumer blocking on PollFetches
	// should NOT be detected as stuck — hasPolledOnce prevents false positives.
	watchdog := &consumeWatchdog{}

	// First poll cycle: start -> PollFetches returns -> markPollSucceeded
	watchdog.markConsumeStarted()
	watchdog.markPollSucceeded()
	assert.True(t, watchdog.hasPolledOnce.Load())

	// Second poll cycle: start -> PollFetches blocks (idle, no messages)
	watchdog.markConsumeStarted()
	assert.True(t, watchdog.isAttemptingConsume.Load())

	// Regardless of elapsed time, the consumer should NOT be stuck
	// because hasPolledOnce is true.
	stuck, _ := watchdog.isStuckInRefreshMetadata(10 * time.Millisecond)
	assert.False(t, stuck, "idle consumer after successful poll should not be detected as stuck")
}

func TestConsumeWatchdogHasPolledOnceResetOnRecovery(t *testing.T) {
	// After force recovery resets hasPolledOnce, the watchdog should be able
	// to detect a stuck consumer again if the replacement client also fails.
	watchdog := &consumeWatchdog{}

	// Successful first poll
	watchdog.markConsumeStarted()
	watchdog.markPollSucceeded()
	assert.True(t, watchdog.hasPolledOnce.Load())

	// Simulate forceRecovery clearing the flag
	watchdog.hasPolledOnce.Store(false)
	watchdog.markConsumeEnded()

	// New poll cycle on replacement client that gets stuck.
	// Set consumeStartTime to the past to exceed the threshold deterministically.
	watchdog.markConsumeStarted()
	watchdog.consumeStartTime.Store(time.Now().Add(-20 * time.Millisecond))

	stuck, duration := watchdog.isStuckInRefreshMetadata(10 * time.Millisecond)
	assert.True(t, stuck, "consumer should be detected as stuck after recovery reset")
	assert.Greater(t, duration, 10*time.Millisecond)
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

func TestNewKafkaConsumerGroup_AppliesDefaultTimeouts(t *testing.T) {
	// Verify that zero-value and negative timeouts get default values applied
	// when constructing a consumer group with a non-memory scheme.
	tests := []struct {
		name              string
		maxProcessingTime time.Duration
		sessionTimeout    time.Duration
		heartbeatInterval time.Duration
		rebalanceTimeout  time.Duration
	}{
		{
			name:              "zero values get defaults",
			maxProcessingTime: 0,
			sessionTimeout:    0,
			heartbeatInterval: 0,
			rebalanceTimeout:  0,
		},
		{
			name:              "negative values get defaults",
			maxProcessingTime: -1 * time.Second,
			sessionTimeout:    -1 * time.Second,
			heartbeatInterval: -1 * time.Second,
			rebalanceTimeout:  -1 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use a non-memory Kafka URL so that NewKafkaConsumerGroup
			// exercises the non-memory path and applies default timeouts.
			u, err := url.Parse("kafka://localhost:9092")
			require.NoError(t, err)

			cfg := KafkaConsumerConfig{
				Logger:            &mockLogger{},
				URL:               u,
				BrokersURL:        []string{"localhost:9092"},
				Topic:             "test-topic",
				ConsumerGroupID:   "test-group",
				MaxProcessingTime: tt.maxProcessingTime,
				SessionTimeout:    tt.sessionTimeout,
				HeartbeatInterval: tt.heartbeatInterval,
				RebalanceTimeout:  tt.rebalanceTimeout,
			}

			consumer, err := NewKafkaConsumerGroup(cfg)
			if err != nil {
				// If construction fails (e.g. due to no broker), it should
				// not be due to invalid timeout configuration.
				assert.NotContains(t, err.Error(), "timeout")
				assert.NotContains(t, err.Error(), "session")
				return
			}

			require.NotNil(t, consumer)
			// When there is no construction error, verify that effective
			// timeouts are positive, indicating defaults were applied.
			assert.Greater(t, consumer.Config.MaxProcessingTime, time.Duration(0))
			assert.Greater(t, consumer.Config.SessionTimeout, time.Duration(0))
			assert.Greater(t, consumer.Config.HeartbeatInterval, time.Duration(0))
			assert.Greater(t, consumer.Config.RebalanceTimeout, time.Duration(0))
			_ = consumer.Close()
		})
	}
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
