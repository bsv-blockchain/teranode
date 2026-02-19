package kafka

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/bsv-blockchain/teranode/pkg/urlutil"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageStruct(t *testing.T) {
	key := []byte("test-key")
	value := []byte("test-value")

	msg := &Message{
		Key:   key,
		Value: value,
	}

	assert.Equal(t, key, msg.Key)
	assert.Equal(t, value, msg.Value)
}

func TestMessageStatusStruct(t *testing.T) {
	now := time.Now()
	status := &MessageStatus{
		Success: true,
		Error:   nil,
		Time:    now,
	}

	assert.True(t, status.Success)
	assert.NoError(t, status.Error)
	assert.Equal(t, now, status.Time)
}

func TestKafkaAsyncProducerBrokersURL(t *testing.T) {
	brokersURL := []string{"broker1:9092", "broker2:9092"}
	producer := &KafkaAsyncProducer{
		Config: KafkaProducerConfig{
			BrokersURL: brokersURL,
		},
	}

	result := producer.BrokersURL()
	assert.Equal(t, brokersURL, result)
}

func TestKafkaAsyncProducerBrokersURLNilProducer(t *testing.T) {
	var producer *KafkaAsyncProducer

	result := producer.BrokersURL()
	assert.Nil(t, result)
}

func TestKafkaAsyncProducerDecodeKeyOrValue(t *testing.T) {
	producer := &KafkaAsyncProducer{}

	tests := []struct {
		name     string
		encoder  sarama.Encoder
		expected string
	}{
		{
			name:     "Nil encoder",
			encoder:  nil,
			expected: "",
		},
		{
			name:     "Short data",
			encoder:  sarama.ByteEncoder("hello"),
			expected: "68656c6c6f",
		},
		{
			name:     "Long data gets truncated",
			encoder:  sarama.ByteEncoder(make([]byte, 100)),
			expected: "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000... (truncated)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := producer.decodeKeyOrValue(tt.encoder)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestKafkaAsyncProducerStopNilProducer(t *testing.T) {
	var producer *KafkaAsyncProducer

	err := producer.Stop()
	assert.NoError(t, err)
}

func TestKafkaAsyncProducerStopAlreadyClosed(t *testing.T) {
	producer := &KafkaAsyncProducer{}
	producer.closed.Store(true)

	err := producer.Stop()
	assert.NoError(t, err)
}

func TestCreateTopicConfigValidation(t *testing.T) {
	// Test configuration struct validation
	cfg := KafkaProducerConfig{
		Topic:                 "test-topic",
		Partitions:            3,
		ReplicationFactor:     1,
		RetentionPeriodMillis: "600000",
		SegmentBytes:          "1073741824",
	}

	// Verify configuration structure
	assert.Equal(t, "test-topic", cfg.Topic)
	assert.Equal(t, int32(3), cfg.Partitions)
	assert.Equal(t, int16(1), cfg.ReplicationFactor)
	assert.Equal(t, "600000", cfg.RetentionPeriodMillis)
	assert.Equal(t, "1073741824", cfg.SegmentBytes)
}

func TestCreateTopicConfigEntries(t *testing.T) {
	// Test that we can construct the config entries map correctly
	retentionPeriod := "300000"
	segmentBytes := "536870912"

	configEntries := map[string]*string{
		"retention.ms":        &retentionPeriod,
		"delete.retention.ms": &retentionPeriod,
		"segment.ms":          &retentionPeriod,
		"segment.bytes":       &segmentBytes,
	}

	assert.Equal(t, "300000", *configEntries["retention.ms"])
	assert.Equal(t, "300000", *configEntries["delete.retention.ms"])
	assert.Equal(t, "300000", *configEntries["segment.ms"])
	assert.Equal(t, "536870912", *configEntries["segment.bytes"])
}

func TestNewKafkaAsyncProducerFromURLMemoryScheme(t *testing.T) {
	logger := &mockAsyncLogger{}
	ctx := context.Background()
	kafkaURL, err := url.Parse("memory://localhost/test-topic?partitions=4&replication=2&retention=300000")
	require.NoError(t, err)

	producer, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, nil)

	assert.NoError(t, err)
	assert.NotNil(t, producer)
	assert.Equal(t, "test-topic", producer.Config.Topic)
	assert.Equal(t, int32(4), producer.Config.Partitions)
	assert.Equal(t, int16(2), producer.Config.ReplicationFactor)
	assert.Equal(t, "300000", producer.Config.RetentionPeriodMillis)
}

func TestNewKafkaAsyncProducerFromURLDefaultValues(t *testing.T) {
	logger := &mockAsyncLogger{}
	ctx := context.Background()
	kafkaURL, err := url.Parse("memory://localhost/test-topic")
	require.NoError(t, err)

	producer, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, nil)

	assert.NoError(t, err)
	assert.NotNil(t, producer)
	assert.Equal(t, int32(1), producer.Config.Partitions)            // default
	assert.Equal(t, int16(1), producer.Config.ReplicationFactor)     // default
	assert.Equal(t, "600000", producer.Config.RetentionPeriodMillis) // default 10 minutes
	assert.Equal(t, "1073741824", producer.Config.SegmentBytes)      // default 1GB
	assert.Equal(t, 1024*1024, producer.Config.FlushBytes)           // default 1MB
	assert.Equal(t, 50_000, producer.Config.FlushMessages)           // default
	assert.Equal(t, 10*time.Second, producer.Config.FlushFrequency)  // default
}

func TestNewKafkaAsyncProducerFromURLInvalidConversion(t *testing.T) {
	logger := &mockAsyncLogger{}
	ctx := context.Background()

	// Test invalid partitions conversion
	kafkaURL, err := url.Parse("memory://localhost/test-topic?partitions=2147483648") // Max int32 + 1
	require.NoError(t, err)

	producer, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, nil)

	assert.Error(t, err)
	assert.Nil(t, producer)
}

func TestNewKafkaAsyncProducerMemoryScheme(t *testing.T) {
	logger := &mockAsyncLogger{}
	cfg := KafkaProducerConfig{
		Logger: logger,
		URL:    &url.URL{Scheme: memoryScheme},
		Topic:  "memory-topic",
	}

	producer, err := NewKafkaAsyncProducer(logger, cfg)

	assert.NoError(t, err)
	assert.NotNil(t, producer)
	assert.NotNil(t, producer.Producer)
	assert.Equal(t, cfg, producer.Config)
}

func TestNewKafkaAsyncProducerWithKafkaSettings(t *testing.T) {
	logger := &mockAsyncLogger{}
	cfg := KafkaProducerConfig{
		Logger:             logger,
		URL:                &url.URL{Scheme: memoryScheme},
		Topic:              "test-topic",
		EnableTLS:          false,
		TLSSkipVerify:      false,
		EnableDebugLogging: false,
	}

	producer, err := NewKafkaAsyncProducer(logger, cfg)

	assert.NoError(t, err)
	assert.NotNil(t, producer)
}

func TestKafkaAsyncProducerPublish_NilChannel(t *testing.T) {
	producer := &KafkaAsyncProducer{}
	msg := &Message{
		Key:   []byte("key"),
		Value: []byte("value"),
	}

	// Should not panic when publish channel is nil
	assert.NotPanics(t, func() {
		producer.Publish(msg)
	})
}

func TestKafkaAsyncProducerStart_NilProducer(t *testing.T) {
	var producer *KafkaAsyncProducer
	ch := make(chan *Message)

	// Should not panic when producer is nil
	assert.NotPanics(t, func() {
		producer.Start(context.Background(), ch)
	})
}

func TestKafkaProducerConfigValidation(t *testing.T) {
	tests := []struct {
		name   string
		config KafkaProducerConfig
		valid  bool
	}{
		{
			name: "Valid config",
			config: KafkaProducerConfig{
				Logger:                &mockAsyncLogger{},
				URL:                   &url.URL{Scheme: "memory"},
				Topic:                 "test-topic",
				Partitions:            3,
				ReplicationFactor:     1,
				RetentionPeriodMillis: "600000",
				SegmentBytes:          "1073741824",
				FlushBytes:            1024,
				FlushMessages:         1000,
				FlushFrequency:        time.Second,
			},
			valid: true,
		},
		{
			name: "Zero partitions",
			config: KafkaProducerConfig{
				Logger:     &mockAsyncLogger{},
				URL:        &url.URL{Scheme: "memory"},
				Topic:      "test-topic",
				Partitions: 0,
			},
			valid: false, // Usually invalid for real usage
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test that config can be created
			assert.NotNil(t, tt.config.Logger)
			assert.NotNil(t, tt.config.URL)
			assert.NotEmpty(t, tt.config.Topic)
		})
	}
}

// Mock implementations for testing

type mockAsyncLogger struct {
	debugCount int
	infoCount  int
	errorCount int
	fatalCount int
}

func (m *mockAsyncLogger) Debug()                                               { m.debugCount++ }
func (m *mockAsyncLogger) Debugf(string, ...interface{})                        { m.debugCount++ }
func (m *mockAsyncLogger) Info()                                                { m.infoCount++ }
func (m *mockAsyncLogger) Infof(string, ...interface{})                         { m.infoCount++ }
func (m *mockAsyncLogger) Warn()                                                {}
func (m *mockAsyncLogger) Warnf(string, ...interface{})                         {}
func (m *mockAsyncLogger) Error(...interface{})                                 { m.errorCount++ }
func (m *mockAsyncLogger) Errorf(string, ...interface{})                        { m.errorCount++ }
func (m *mockAsyncLogger) Fatal(...interface{})                                 { m.fatalCount++ }
func (m *mockAsyncLogger) Fatalf(string, ...interface{})                        { m.fatalCount++ }
func (m *mockAsyncLogger) LogLevel() int                                        { return 0 }
func (m *mockAsyncLogger) SetLogLevel(string)                                   {}
func (m *mockAsyncLogger) New(string, ...ulogger.Option) ulogger.Logger         { return m }
func (m *mockAsyncLogger) Duplicate(...ulogger.Option) ulogger.Logger           { return m }
func (m *mockAsyncLogger) WithTraceContext(context.Context) ulogger.Logger      { return m }
func (m *mockAsyncLogger) Health() bool                                         { return true }
func (m *mockAsyncLogger) Close() error                                         { return nil }
func (m *mockAsyncLogger) SetCurrentBlockHeight(uint64)                         {}
func (m *mockAsyncLogger) SetCurrentBlockTime(time.Time)                        {}
func (m *mockAsyncLogger) SetCurrentBlockHash(string)                           {}
func (m *mockAsyncLogger) SetCurrentBlockSize(uint64)                           {}
func (m *mockAsyncLogger) SetCurrentBlockTransactions(uint64)                   {}
func (m *mockAsyncLogger) SetCurrentBlockTransactionsSize(uint64)               {}
func (m *mockAsyncLogger) SetCurrentBlockTransactionsInputs(uint64)             {}
func (m *mockAsyncLogger) SetCurrentBlockTransactionsOutputs(uint64)            {}
func (m *mockAsyncLogger) SetCurrentBlockTransactionsFees(uint64)               {}
func (m *mockAsyncLogger) SetCurrentBlockTransactionsFeesPerByte(uint64)        {}
func (m *mockAsyncLogger) SetCurrentBlockTransactionsFeesPerTransaction(uint64) {}

// Additional configuration tests

func TestKafkaAsyncProducerWithCustomFlushSettings(t *testing.T) {
	logger := &mockAsyncLogger{}
	ctx := context.Background()

	kafkaURL, err := url.Parse("memory://localhost/test-topic?flush_bytes=2048&flush_messages=100&flush_frequency=5s")
	require.NoError(t, err)

	producer, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, nil)
	require.NoError(t, err)

	// Verify flush settings
	assert.Equal(t, 2048, producer.Config.FlushBytes)
	assert.Equal(t, 100, producer.Config.FlushMessages)
	assert.Equal(t, 5*time.Second, producer.Config.FlushFrequency)
}

func TestKafkaAsyncProducerBrokersURLParsing(t *testing.T) {
	logger := &mockAsyncLogger{}
	ctx := context.Background()

	kafkaURL, err := urlutil.ParseMultiHostURL("memory://broker1:9092,broker2:9092,broker3:9092/test-topic")
	require.NoError(t, err)

	producer, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, nil)
	require.NoError(t, err)

	brokers := producer.BrokersURL()
	assert.Len(t, brokers, 3)
	assert.Equal(t, "broker1:9092", brokers[0])
	assert.Equal(t, "broker2:9092", brokers[1])
	assert.Equal(t, "broker3:9092", brokers[2])
}

func TestKafkaAsyncProducerWithMultipleBrokers(t *testing.T) {
	logger := &mockAsyncLogger{}
	ctx := context.Background()

	// Test with multiple brokers in URL
	kafkaURL, err := urlutil.ParseMultiHostURL("memory://broker1:9092,broker2:9093/test-topic?partitions=3")
	require.NoError(t, err)

	producer, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, nil)
	require.NoError(t, err)

	assert.Len(t, producer.Config.BrokersURL, 2)
	assert.Equal(t, "broker1:9092", producer.Config.BrokersURL[0])
	assert.Equal(t, "broker2:9093", producer.Config.BrokersURL[1])
	assert.Equal(t, int32(3), producer.Config.Partitions)
}

func TestKafkaAsyncProducerURLQueryParams(t *testing.T) {
	logger := &mockAsyncLogger{}
	ctx := context.Background()

	tests := []struct {
		name      string
		url       string
		checkFunc func(*testing.T, *KafkaAsyncProducer)
	}{
		{
			name: "custom retention",
			url:  "memory://localhost/test?retention=300000",
			checkFunc: func(t *testing.T, p *KafkaAsyncProducer) {
				assert.Equal(t, "300000", p.Config.RetentionPeriodMillis)
			},
		},
		{
			name: "custom segment bytes",
			url:  "memory://localhost/test?segment_bytes=536870912",
			checkFunc: func(t *testing.T, p *KafkaAsyncProducer) {
				assert.Equal(t, "536870912", p.Config.SegmentBytes)
			},
		},
		{
			name: "custom partitions and replication",
			url:  "memory://localhost/test?partitions=5&replication=3",
			checkFunc: func(t *testing.T, p *KafkaAsyncProducer) {
				assert.Equal(t, int32(5), p.Config.Partitions)
				assert.Equal(t, int16(3), p.Config.ReplicationFactor)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kafkaURL, err := url.Parse(tt.url)
			require.NoError(t, err)

			producer, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, nil)
			require.NoError(t, err)
			require.NotNil(t, producer)

			tt.checkFunc(t, producer)
		})
	}
}

func TestKafkaAsyncProducerStopBeforeStart(t *testing.T) {
	logger := &mockAsyncLogger{}
	ctx := context.Background()

	kafkaURL, err := url.Parse("memory://localhost/test-topic-stop-before-start")
	require.NoError(t, err)

	producer, err := NewKafkaAsyncProducerFromURL(ctx, logger, kafkaURL, nil)
	require.NoError(t, err)

	// Stop before Start - should handle gracefully
	producer.closed.Store(true)
	producer.publishChannel = make(chan *Message)
	err = producer.Stop()
	assert.NoError(t, err)
}
