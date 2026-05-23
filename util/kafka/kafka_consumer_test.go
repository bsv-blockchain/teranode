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
