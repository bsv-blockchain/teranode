package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/url"
	"testing"

	"github.com/IBM/sarama"
	"github.com/bitcoin-sv/teranode/settings"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockSyncProducer implements sarama.SyncProducer for testing
type MockSyncProducer struct {
	messages []sarama.ProducerMessage
	closed   bool
	sendErr  error
}

func (m *MockSyncProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	if m.sendErr != nil {
		return 0, 0, m.sendErr
	}
	m.messages = append(m.messages, *msg)
	return msg.Partition, int64(len(m.messages)), nil
}

func (m *MockSyncProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	for _, msg := range msgs {
		_, _, err := m.SendMessage(msg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MockSyncProducer) Close() error {
	m.closed = true
	return nil
}

func (m *MockSyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	return sarama.ProducerTxnFlagReady
}

func (m *MockSyncProducer) IsTransactional() bool {
	return false
}

func (m *MockSyncProducer) BeginTxn() error {
	return nil
}

func (m *MockSyncProducer) CommitTxn() error {
	return nil
}

func (m *MockSyncProducer) AbortTxn() error {
	return nil
}

func (m *MockSyncProducer) AddOffsetsToTxn(map[string][]*sarama.PartitionOffsetMetadata, string) error {
	return nil
}

func (m *MockSyncProducer) AddMessageToTxn(*sarama.ConsumerMessage, string, *string) error {
	return nil
}

func TestSyncKafkaProducerClose(t *testing.T) {
	mockProducer := &MockSyncProducer{}
	producer := &SyncKafkaProducer{
		Producer:   mockProducer,
		Topic:      "test-topic",
		Partitions: 1,
	}

	err := producer.Close()

	assert.NoError(t, err)
	assert.True(t, mockProducer.closed)
}

func TestSyncKafkaProducerGetClient(t *testing.T) {
	mockProducer := &MockSyncProducer{}
	mockClient := &mockConsumerGroup{}
	producer := &SyncKafkaProducer{
		Producer:   mockProducer,
		Topic:      "test-topic",
		Partitions: 1,
		client:     mockClient,
	}

	client := producer.GetClient()

	assert.Equal(t, mockClient, client)
}

func TestSyncKafkaProducerSend(t *testing.T) {
	tests := []struct {
		name          string
		partitions    int32
		key           []byte
		data          []byte
		expectedTopic string
		expectError   bool
	}{
		{
			name:          "Send with valid key and data",
			partitions:    4,
			key:           []byte{0x01, 0x02, 0x03, 0x04},
			data:          []byte("test message"),
			expectedTopic: "test-topic",
			expectError:   false,
		},
		{
			name:          "Send with single partition",
			partitions:    1,
			key:           []byte{0xFF, 0xFF, 0xFF, 0xFF},
			data:          []byte("single partition message"),
			expectedTopic: "test-topic",
			expectError:   false,
		},
		{
			name:          "Send with empty data",
			partitions:    2,
			key:           []byte{0x00, 0x00, 0x00, 0x01},
			data:          []byte{},
			expectedTopic: "test-topic",
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockProducer := &MockSyncProducer{}
			producer := &SyncKafkaProducer{
				Producer:   mockProducer,
				Topic:      tt.expectedTopic,
				Partitions: tt.partitions,
			}

			err := producer.Send(tt.key, tt.data)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				require.Len(t, mockProducer.messages, 1)

				msg := mockProducer.messages[0]
				assert.Equal(t, tt.expectedTopic, msg.Topic)
				assert.Equal(t, sarama.ByteEncoder(tt.key), msg.Key)
				assert.Equal(t, sarama.ByteEncoder(tt.data), msg.Value)

				// Verify partition calculation
				kPartitionsUint32, _ := safeconversion.Int32ToUint32(tt.partitions)
				expectedPartition := binary.LittleEndian.Uint32(tt.key) % kPartitionsUint32
				expectedPartitionInt32, _ := safeconversion.Uint32ToInt32(expectedPartition)
				assert.Equal(t, expectedPartitionInt32, msg.Partition)
			}
		})
	}
}

func TestSyncKafkaProducerSendPartitionCalculation(t *testing.T) {
	mockProducer := &MockSyncProducer{}
	producer := &SyncKafkaProducer{
		Producer:   mockProducer,
		Topic:      "test-topic",
		Partitions: 3,
	}

	testCases := []struct {
		key               []byte
		expectedPartition int32
	}{
		{[]byte{0x00, 0x00, 0x00, 0x00}, 0}, // 0 % 3 = 0
		{[]byte{0x01, 0x00, 0x00, 0x00}, 1}, // 1 % 3 = 1
		{[]byte{0x02, 0x00, 0x00, 0x00}, 2}, // 2 % 3 = 2
		{[]byte{0x03, 0x00, 0x00, 0x00}, 0}, // 3 % 3 = 0
		{[]byte{0x04, 0x00, 0x00, 0x00}, 1}, // 4 % 3 = 1
	}

	for i, tc := range testCases {
		err := producer.Send(tc.key, []byte("test"))
		require.NoError(t, err)

		msg := mockProducer.messages[i]
		assert.Equal(t, tc.expectedPartition, msg.Partition)
	}
}

func TestSyncKafkaProducerSendProducerError(t *testing.T) {
	mockProducer := &MockSyncProducer{
		sendErr: assert.AnError,
	}
	producer := &SyncKafkaProducer{
		Producer:   mockProducer,
		Topic:      "test-topic",
		Partitions: 1,
	}

	err := producer.Send([]byte{0x01, 0x00, 0x00, 0x00}, []byte("test"))

	assert.Error(t, err)
	assert.Equal(t, assert.AnError, err)
}

// Mock consumer group for testing GetClient
type mockConsumerGroup struct{}

func (m *mockConsumerGroup) Consume(context.Context, []string, sarama.ConsumerGroupHandler) error {
	return nil
}

func (m *mockConsumerGroup) Errors() <-chan error {
	return nil
}

func (m *mockConsumerGroup) Close() error {
	return nil
}

func (m *mockConsumerGroup) Pause(map[string][]int32)  {}
func (m *mockConsumerGroup) Resume(map[string][]int32) {}
func (m *mockConsumerGroup) PauseAll()                 {}
func (m *mockConsumerGroup) ResumeAll()                {}

func TestNewKafkaProducer_MemoryScheme(t *testing.T) {
	kafkaURL, err := url.Parse("memory://localhost/test-topic?partitions=2")
	require.NoError(t, err)

	clusterAdmin, producer, err := NewKafkaProducer(kafkaURL, nil)

	assert.NoError(t, err)
	assert.Nil(t, clusterAdmin) // Memory scheme returns nil cluster admin
	assert.NotNil(t, producer)

	syncProducer, ok := producer.(*SyncKafkaProducer)
	require.True(t, ok)
	assert.Equal(t, "test-topic", syncProducer.Topic)

	// Note: Memory scheme producer has 0 partitions, so sending messages would cause divide by zero
	// This is a limitation of the current implementation - the Send method should handle this case
	// For now, just test that the producer was created correctly
	assert.Equal(t, int32(0), syncProducer.Partitions)
}

func TestNewKafkaProducer_InvalidScheme(t *testing.T) {
	kafkaURL, err := url.Parse("kafka://invalid-broker:9092/test-topic")
	require.NoError(t, err)

	clusterAdmin, producer, err := NewKafkaProducer(kafkaURL, nil)

	assert.Error(t, err)
	assert.Nil(t, clusterAdmin)
	assert.Nil(t, producer)
}

func TestNewKafkaProducer_WithKafkaSettings(t *testing.T) {
	kafkaURL, err := url.Parse("memory://localhost/test-topic")
	require.NoError(t, err)

	kafkaSettings := &settings.KafkaSettings{
		EnableTLS:     false,
		TLSSkipVerify: false,
	}

	clusterAdmin, producer, err := NewKafkaProducer(kafkaURL, kafkaSettings)

	assert.NoError(t, err)
	assert.Nil(t, clusterAdmin) // Memory scheme returns nil cluster admin
	assert.NotNil(t, producer)
}

func TestConnectProducer_ConfigurationOptions(t *testing.T) {
	// This test verifies that ConnectProducer would handle various configurations
	// but since it tries to connect to real brokers, we can only test the parameter validation
	// Use a port that's guaranteed to be closed to ensure connection failure
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	require.NoError(t, err)
	l, err := net.ListenTCP("tcp", addr)
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	brokers := []string{fmt.Sprintf("localhost:%d", port)}
	topic := "test-topic"
	partitions := int32(3)

	// Test with default flush bytes
	producer, err := ConnectProducer(brokers, topic, partitions, nil)

	// We expect an error since we're not connecting to a real broker
	assert.Error(t, err)
	assert.Nil(t, producer)

	// Test with custom flush bytes
	producer, err = ConnectProducer(brokers, topic, partitions, nil, 2048)

	// We expect an error since we're not connecting to a real broker
	assert.Error(t, err)
	assert.Nil(t, producer)
}
