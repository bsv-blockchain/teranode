package kafka

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/bitcoin-sv/teranode/ulogger"
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

func TestNewKafkaConsumer(t *testing.T) {
	logger := &mockLogger{}
	cfg := KafkaConsumerConfig{
		Logger:            logger,
		Topic:             "test-topic",
		ConsumerGroupID:   "test-group",
		AutoCommitEnabled: true,
	}

	consumerFn := func(message *KafkaMessage) error {
		return nil
	}

	consumer := NewKafkaConsumer(cfg, consumerFn)

	assert.NotNil(t, consumer)
	assert.Equal(t, cfg, consumer.cfg)
	assert.NotNil(t, consumer.consumerClosure)
}

func TestNewKafkaConsumerNilConsumerFunction(t *testing.T) {
	logger := &mockLogger{}
	cfg := KafkaConsumerConfig{
		Logger:            logger,
		Topic:             "test-topic",
		ConsumerGroupID:   "test-group",
		AutoCommitEnabled: false,
	}

	consumer := NewKafkaConsumer(cfg, nil)

	assert.NotNil(t, consumer)
	assert.Equal(t, cfg, consumer.cfg)
	assert.Nil(t, consumer.consumerClosure)
}

func TestKafkaConsumerSetup(t *testing.T) {
	consumer := &KafkaConsumer{
		cfg: KafkaConsumerConfig{
			Topic: "test-topic",
		},
	}

	err := consumer.Setup(&mockConsumerGroupSession{})

	assert.NoError(t, err)
}

func TestKafkaConsumerCleanupAutoCommitEnabled(t *testing.T) {
	consumer := &KafkaConsumer{
		cfg: KafkaConsumerConfig{
			Topic:             "test-topic",
			AutoCommitEnabled: true,
		},
	}

	session := &mockConsumerGroupSession{}
	err := consumer.Cleanup(session)

	assert.NoError(t, err)
	assert.False(t, session.commitCalled) // Should not call commit when auto-commit is enabled
}

func TestKafkaConsumerCleanupManualCommit(t *testing.T) {
	consumer := &KafkaConsumer{
		cfg: KafkaConsumerConfig{
			Topic:             "test-topic",
			AutoCommitEnabled: false,
		},
	}

	session := &mockConsumerGroupSession{}
	err := consumer.Cleanup(session)

	assert.NoError(t, err)
	assert.True(t, session.commitCalled) // Should call commit when auto-commit is disabled
}

func TestNewKafkaConsumerGroupFromURLInvalidURL(t *testing.T) {
	logger := &mockLogger{}

	consumer, err := NewKafkaConsumerGroupFromURL(logger, nil, "test-group", true)

	assert.Error(t, err)
	assert.Nil(t, consumer)
	assert.Contains(t, err.Error(), "missing kafka url")
}

func TestNewKafkaConsumerGroupFromURLMemoryScheme(t *testing.T) {
	logger := &mockLogger{}
	kafkaURL, err := url.Parse("memory://localhost/test-topic?partitions=4&consumer_ratio=2&replay=1")
	require.NoError(t, err)

	consumer, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL, "test-group", true)

	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	assert.Equal(t, "test-topic", consumer.Config.Topic)
	assert.Equal(t, "test-group", consumer.Config.ConsumerGroupID)
	assert.Equal(t, 4, consumer.Config.Partitions)
	assert.Equal(t, 2, consumer.Config.ConsumerRatio)
	assert.Equal(t, 1, consumer.Config.ConsumerCount) // Memory scheme forces consumer count to 1
	assert.True(t, consumer.Config.AutoCommitEnabled)
	assert.True(t, consumer.Config.Replay)
}

func TestNewKafkaConsumerGroupFromURLConsumerRatioValidation(t *testing.T) {
	tests := []struct {
		name             string
		urlParams        string
		expectedRatio    int
		expectedCount    int
		expectedWarnings int
	}{
		{
			name:             "Valid consumer ratio",
			urlParams:        "partitions=6&consumer_ratio=2",
			expectedRatio:    2,
			expectedCount:    1, // Memory scheme forces consumer count to 1
			expectedWarnings: 0,
		},
		{
			name:             "Consumer ratio less than 1",
			urlParams:        "partitions=4&consumer_ratio=0",
			expectedRatio:    1, // Should be corrected to 1
			expectedCount:    1, // Memory scheme forces consumer count to 1
			expectedWarnings: 1,
		},
		{
			name:             "Consumer count less than 1",
			urlParams:        "partitions=1&consumer_ratio=2",
			expectedRatio:    2,
			expectedCount:    1, // Should be corrected to 1
			expectedWarnings: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := &mockLogger{}
			kafkaURL, err := url.Parse("memory://localhost/test-topic?" + tt.urlParams)
			require.NoError(t, err)

			consumer, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL, "test-group", true)

			assert.NoError(t, err)
			assert.NotNil(t, consumer)
			assert.Equal(t, tt.expectedRatio, consumer.Config.ConsumerRatio)
			assert.Equal(t, tt.expectedCount, consumer.Config.ConsumerCount)
			assert.Equal(t, tt.expectedWarnings, logger.warnCount)
		})
	}
}

func TestNewKafkaConsumerGroupFromURLDefaultValues(t *testing.T) {
	logger := &mockLogger{}
	kafkaURL, err := url.Parse("memory://localhost/test-topic")
	require.NoError(t, err)

	consumer, err := NewKafkaConsumerGroupFromURL(logger, kafkaURL, "test-group", false)

	assert.NoError(t, err)
	assert.NotNil(t, consumer)
	assert.Equal(t, 1, consumer.Config.Partitions)    // default partitions
	assert.Equal(t, 1, consumer.Config.ConsumerRatio) // default consumer_ratio
	assert.Equal(t, 1, consumer.Config.ConsumerCount) // 1/1 = 1
	assert.False(t, consumer.Config.AutoCommitEnabled)
	assert.True(t, consumer.Config.Replay) // default replay=1
}

func TestKafkaConsumerGroupClose(t *testing.T) {
	logger := &mockLogger{}
	mockConsumerGroup := &mockSaramaConsumerGroup{}

	consumer := &KafkaConsumerGroup{
		Config: KafkaConsumerConfig{
			Logger:          logger,
			Topic:           "test-topic",
			ConsumerGroupID: "test-group",
		},
		ConsumerGroup: mockConsumerGroup,
	}

	err := consumer.Close()

	assert.NoError(t, err)
	assert.True(t, mockConsumerGroup.closed)
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
				ConsumerCount:   1,
				ConsumerGroupID: "test-group",
			},
			errMsg: "kafka URL is not set",
		},
		{
			name: "Invalid consumer count",
			config: KafkaConsumerConfig{
				URL:             &url.URL{Scheme: "memory"},
				Logger:          logger,
				ConsumerCount:   0,
				ConsumerGroupID: "test-group",
			},
			errMsg: "consumer count must be greater than 0",
		},
		{
			name: "Missing logger",
			config: KafkaConsumerConfig{
				URL:             &url.URL{Scheme: "memory"},
				ConsumerCount:   1,
				ConsumerGroupID: "test-group",
			},
			errMsg: "logger is not set",
		},
		{
			name: "Missing group ID",
			config: KafkaConsumerConfig{
				URL:           &url.URL{Scheme: "memory"},
				Logger:        logger,
				ConsumerCount: 1,
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

func (m *mockLogger) Debug()                                       {}
func (m *mockLogger) Debugf(string, ...interface{})                {}
func (m *mockLogger) Info()                                        {}
func (m *mockLogger) Infof(string, ...interface{})                 {}
func (m *mockLogger) Warn()                                        { m.warnCount++ }
func (m *mockLogger) Warnf(string, ...interface{})                 { m.warnCount++ }
func (m *mockLogger) Error(...interface{})                         {}
func (m *mockLogger) Errorf(string, ...interface{})                {}
func (m *mockLogger) Fatal(...interface{})                         {}
func (m *mockLogger) Fatalf(string, ...interface{})                {}
func (m *mockLogger) LogLevel() int                                { return 0 }
func (m *mockLogger) SetLogLevel(string)                           {}
func (m *mockLogger) New(string, ...ulogger.Option) ulogger.Logger { return m }
func (m *mockLogger) Duplicate(...ulogger.Option) ulogger.Logger   { return m }

type mockConsumerGroupSession struct {
	commitCalled bool
}

func (m *mockConsumerGroupSession) Claims() map[string][]int32 { return nil }
func (m *mockConsumerGroupSession) MemberID() string           { return "test-member" }
func (m *mockConsumerGroupSession) GenerationID() int32        { return 1 }
func (m *mockConsumerGroupSession) MarkOffset(string, int32, int64, string) {
}
func (m *mockConsumerGroupSession) ResetOffset(string, int32, int64, string) {
}
func (m *mockConsumerGroupSession) MarkMessage(*sarama.ConsumerMessage, string) {}
func (m *mockConsumerGroupSession) Context() context.Context                    { return context.Background() }
func (m *mockConsumerGroupSession) Commit() {
	m.commitCalled = true
}

type mockSaramaConsumerGroup struct {
	closed bool
}

// Consume implements sarama.ConsumerGroup interface
func (m *mockSaramaConsumerGroup) Consume(context.Context, []string, sarama.ConsumerGroupHandler) error {
	return nil
}

// Errors implements sarama.ConsumerGroup interface
func (m *mockSaramaConsumerGroup) Errors() <-chan error {
	return make(chan error)
}

// Close implements sarama.ConsumerGroup interface
func (m *mockSaramaConsumerGroup) Close() error {
	m.closed = true
	return nil
}

// Pause and Resume methods are no-ops for the mock
func (m *mockSaramaConsumerGroup) Pause(map[string][]int32) {}

// Resume implements sarama.ConsumerGroup interface
func (m *mockSaramaConsumerGroup) Resume(map[string][]int32) {}

// PauseAll and ResumeAll methods are no-ops for the mock
func (m *mockSaramaConsumerGroup) PauseAll() {}

// ResumeAll implements sarama.ConsumerGroup interface
func (m *mockSaramaConsumerGroup) ResumeAll() {}
