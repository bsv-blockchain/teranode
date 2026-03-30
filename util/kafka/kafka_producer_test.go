package kafka

import (
	"fmt"
	"net"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSyncKafkaProducerClose(t *testing.T) {
	kafkaURL, err := url.Parse("memory://localhost/test-topic")
	require.NoError(t, err)
	producer, err := NewKafkaProducer(kafkaURL, nil)
	require.NoError(t, err)
	require.NotNil(t, producer)

	err = producer.Close()
	assert.NoError(t, err)
}

func TestSyncKafkaProducerSend(t *testing.T) {
	kafkaURL, err := url.Parse("memory://localhost/test-topic?partitions=4")
	require.NoError(t, err)
	producer, err := NewKafkaProducer(kafkaURL, nil)
	require.NoError(t, err)
	require.NotNil(t, producer)
	defer producer.Close()

	err = producer.Send([]byte{0x01, 0x02, 0x03, 0x04}, []byte("test message"))
	assert.NoError(t, err)
}

func TestNewKafkaProducer_MemoryScheme(t *testing.T) {
	kafkaURL, err := url.Parse("memory://localhost/test-topic?partitions=2")
	require.NoError(t, err)

	producer, err := NewKafkaProducer(kafkaURL, nil)

	assert.NoError(t, err)
	assert.NotNil(t, producer)

	syncProducer, ok := producer.(*SyncKafkaProducer)
	require.True(t, ok)
	assert.Equal(t, "test-topic", syncProducer.Topic)
}

func TestNewKafkaProducer_InvalidScheme(t *testing.T) {
	kafkaURL, err := url.Parse("kafka://invalid-broker:9092/test-topic")
	require.NoError(t, err)

	producer, err := NewKafkaProducer(kafkaURL, nil)

	assert.Error(t, err)
	assert.Nil(t, producer)
}

func TestNewKafkaProducer_WithKafkaSettings(t *testing.T) {
	kafkaURL, err := url.Parse("memory://localhost/test-topic")
	require.NoError(t, err)

	kafkaSettings := &settings.KafkaSettings{
		EnableTLS:     false,
		TLSSkipVerify: false,
	}

	producer, err := NewKafkaProducer(kafkaURL, kafkaSettings)

	assert.NoError(t, err)
	assert.NotNil(t, producer)
}

func TestConnectProducer_ConfigurationOptions(t *testing.T) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	require.NoError(t, err)
	l, err := net.ListenTCP("tcp", addr)
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	brokers := []string{fmt.Sprintf("localhost:%d", port)}
	topic := "test-topic"
	partitions := int32(3)

	// franz-go may create the client lazily, so we may get either an error or a producer
	producer, err := ConnectProducer(brokers, topic, partitions, nil)
	if err != nil {
		assert.Nil(t, producer)
	} else {
		require.NotNil(t, producer)
		_ = producer.Close()
	}

	producer, err = ConnectProducer(brokers, topic, partitions, nil, 2048)
	if err != nil {
		assert.Nil(t, producer)
	} else {
		require.NotNil(t, producer)
		_ = producer.Close()
	}
}
