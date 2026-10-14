package validator

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/stretchr/testify/require"
)

type partCheckCapture struct {
	mu     sync.Mutex
	errors []string
	warns  []string
}

func (c *partCheckCapture) LogLevel() int                 { return 0 }
func (c *partCheckCapture) SetLogLevel(string)            {}
func (c *partCheckCapture) Debugf(string, ...interface{}) {}
func (c *partCheckCapture) Infof(string, ...interface{})  {}
func (c *partCheckCapture) Warnf(f string, a ...interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.warns = append(c.warns, fmt.Sprintf(f, a...))
}
func (c *partCheckCapture) Errorf(f string, a ...interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errors = append(c.errors, fmt.Sprintf(f, a...))
}
func (c *partCheckCapture) Fatalf(string, ...interface{}) {}
func (c *partCheckCapture) New(string, ...ulogger.Option) ulogger.Logger {
	return c
}
func (c *partCheckCapture) Duplicate(...ulogger.Option) ulogger.Logger { return c }
func (c *partCheckCapture) WithTraceContext(context.Context) ulogger.Logger {
	return c
}

func (c *partCheckCapture) joined() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return strings.Join(append(append([]string{}, c.errors...), c.warns...), "\n")
}

// partCheckProducer is a minimal KafkaAsyncProducerI; asTopicCounter variants
// also satisfy kafka.TopicPartitionCounter.
type partCheckProducer struct {
	count int32
	err   error
}

func (p *partCheckProducer) Start(context.Context, chan *kafka.Message) {}
func (p *partCheckProducer) Stop() error                                { return nil }
func (p *partCheckProducer) BrokersURL() []string                       { return nil }
func (p *partCheckProducer) Publish(*kafka.Message)                     {}
func (p *partCheckProducer) TryPublish(*kafka.Message) bool             { return true }
func (p *partCheckProducer) TopicPartitionCount(context.Context) (int32, error) {
	return p.count, p.err
}

func partCheckValidator(t *testing.T, cap *partCheckCapture, topicURL string, numPartitions int, producer kafka.KafkaAsyncProducerI) *Validator {
	t.Helper()

	u, err := url.Parse(topicURL)
	require.NoError(t, err)

	s := settings.NewSettings()
	s.Validator.TxMetaNumPartitions = numPartitions
	s.Kafka.TxMetaConfig = u

	return &Validator{
		logger:                    cap,
		settings:                  s,
		txmetaKafkaProducerClient: producer,
	}
}

func TestCheckTxMetaPartitionAgreementURLFallback(t *testing.T) {
	cap := &partCheckCapture{}
	// The shipped default-context mismatch: numPartitions 32 vs partitions=8 URL.
	v := partCheckValidator(t, cap, "kafka://localhost:9092/txmeta?partitions=8", 32, nil)

	v.checkTxMetaPartitionAgreement(context.Background())

	require.Contains(t, cap.joined(), "partitions=8")
	require.Contains(t, cap.joined(), "validator_txmeta_numPartitions=32")
}

func TestCheckTxMetaPartitionAgreementMetadataWins(t *testing.T) {
	cap := &partCheckCapture{}
	// URL says 32 (agreeing) but the broker says 8 - metadata is authoritative.
	v := partCheckValidator(t, cap, "kafka://localhost:9092/txmeta?partitions=32", 32,
		&partCheckProducer{count: 8})

	v.checkTxMetaPartitionAgreement(context.Background())

	require.Contains(t, cap.joined(), "has 8 partitions")
}

func TestCheckTxMetaPartitionAgreementAgreementIsQuiet(t *testing.T) {
	cap := &partCheckCapture{}
	// Derived, not hardcoded: BucketsCount is build-tag selected (8 under
	// testtxmetacache, 8192 by default), and numPartitions must divide it to
	// be a legitimate agreement case under every tag.
	np := txmetacache.BucketsCount
	v := partCheckValidator(t, cap, fmt.Sprintf("kafka://localhost:9092/txmeta?partitions=%d", np), np,
		&partCheckProducer{count: int32(np)})

	v.checkTxMetaPartitionAgreement(context.Background())

	require.Empty(t, cap.errors)
	require.Empty(t, cap.warns)
}

func TestCheckTxMetaPartitionAgreementBrokerErrorIsHonest(t *testing.T) {
	cap := &partCheckCapture{}
	v := partCheckValidator(t, cap, "kafka://localhost:9092/txmeta?partitions=8", 32,
		&partCheckProducer{err: errors.NewProcessingError("broker down")})

	v.checkTxMetaPartitionAgreement(context.Background())

	// Must say it could not verify - never assume agreement - and not fall
	// through to the (disagreeing) URL number as if it were verified truth.
	require.Contains(t, cap.joined(), "cannot verify")
}

func TestCheckTxMetaPartitionAgreementBucketDivisibility(t *testing.T) {
	cap := &partCheckCapture{}
	// 8192/80 is not exact; 80 does not divide the default BucketsCount.
	require.NotZero(t, txmetacache.BucketsCount%80)
	v := partCheckValidator(t, cap, "kafka://localhost:9092/txmeta?partitions=80", 80,
		&partCheckProducer{count: 80})

	v.checkTxMetaPartitionAgreement(context.Background())

	require.Contains(t, cap.joined(), "does not divide")
}
