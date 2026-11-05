package testhelpers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blob"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/test/fault"
	"github.com/stretchr/testify/require"
)

// Stack is the real dependency stack a Block Validation test runs against: a sqlitememory
// blockchain store under a LocalClient, a sqlitememory UTXO store, in-memory blob stores and,
// optionally, an in-memory Kafka invalid-blocks topic. Nothing is mocked. With WithFaults every
// store and the client are wrapped in fault decorators sharing one plan.
//
// The fields are the dependencies to hand to NewBlockValidation (or to a test's own harness).
// When faults are enabled they are the decorated values; the Inner* fields hold the real ones,
// for setup and assertions that must not be faulted or logged.
type Stack struct {
	Ctx      context.Context
	Logger   ulogger.Logger
	Settings *settings.Settings

	// Plan is the fault plan shared by every decorator, or nil without WithFaults.
	Plan *fault.Plan

	Blockchain      blockchain.ClientI
	BlockchainStore blockchainstore.Store
	UTXOStore       utxo.Store
	SubtreeStore    blob.Store
	TxStore         blob.Store
	BlockStore      blob.Store

	InnerBlockchain      blockchain.ClientI
	InnerBlockchainStore blockchainstore.Store
	InnerUTXOStore       utxo.Store
	InnerSubtreeStore    blob.Store

	// InvalidBlocks captures the invalid-blocks topic, or is nil without WithInvalidBlocksTopic.
	InvalidBlocks *KafkaCapture
}

type stackConfig struct {
	plan          *fault.Plan
	settings      []func(*settings.Settings)
	invalidBlocks bool
}

// StackOption configures NewStack.
type StackOption func(*stackConfig)

// WithFaults wraps every store and the blockchain client in fault decorators sharing plan.
// The LocalClient is built over the decorated blockchain store, so a store fault also surfaces
// through the client, and the plan's log shows both calls in order.
func WithFaults(plan *fault.Plan) StackOption {
	return func(c *stackConfig) { c.plan = plan }
}

// WithSettings adjusts the test settings before any store is built.
func WithSettings(fn func(*settings.Settings)) StackOption {
	return func(c *stackConfig) { c.settings = append(c.settings, fn) }
}

// WithInvalidBlocksTopic points the invalid-blocks Kafka producer at a per-test in-memory
// topic and captures what is published to it, so a test asserts on the real producer path
// instead of a producer mock.
func WithInvalidBlocksTopic() StackOption {
	return func(c *stackConfig) { c.invalidBlocks = true }
}

var stackSeq atomic.Int64

// uniqueName returns a name unique within the process, derived from the test name, for
// sqlitememory databases and Kafka topics (both are shared process-wide by name).
func uniqueName(t testing.TB) string {
	name := strings.NewReplacer("/", "_", " ", "_", "#", "_").Replace(t.Name())
	return fmt.Sprintf("%s_%d", name, stackSeq.Add(1))
}

// NewStack builds the stack and registers its cleanup on t.
func NewStack(t testing.TB, opts ...StackOption) *Stack {
	t.Helper()

	var cfg stackConfig
	for _, o := range opts {
		o(&cfg)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	name := uniqueName(t)

	var capture *KafkaCapture
	if cfg.invalidBlocks {
		capture = newKafkaCapture(t, ctx, "invalid_blocks_"+name)
		tSettings.Kafka.InvalidBlocks = capture.Topic
		tSettings.Kafka.InvalidBlocksConfig = capture.URL
	}

	for _, fn := range cfg.settings {
		fn(tSettings)
	}

	utxoURL, err := url.Parse("sqlitememory:///" + name)
	require.NoError(t, err)

	innerUTXO, err := utxosql.New(ctx, logger, tSettings, utxoURL)
	require.NoError(t, err)

	t.Cleanup(func() { _ = innerUTXO.Close(context.Background()) })

	innerChainStore, err := blockchainstore.NewStore(logger, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	innerSubtrees := blobmemory.New()

	s := &Stack{
		Ctx:                  ctx,
		Logger:               logger,
		Settings:             tSettings,
		Plan:                 cfg.plan,
		InnerBlockchainStore: innerChainStore,
		InnerUTXOStore:       innerUTXO,
		InnerSubtreeStore:    innerSubtrees,
		BlockchainStore:      innerChainStore,
		UTXOStore:            innerUTXO,
		SubtreeStore:         innerSubtrees,
		TxStore:              blobmemory.New(),
		BlockStore:           blobmemory.New(),
		InvalidBlocks:        capture,
	}

	if cfg.plan != nil {
		s.BlockchainStore = fault.NewBlockchainStore(innerChainStore, cfg.plan)
		s.UTXOStore = fault.NewUTXOStore(innerUTXO, cfg.plan)
		s.SubtreeStore = fault.NewBlobStore(innerSubtrees, cfg.plan)
		s.TxStore = fault.NewBlobStore(s.TxStore, cfg.plan)
		s.BlockStore = fault.NewBlobStore(s.BlockStore, cfg.plan)
	}

	// The real client reads the inner store, so setup through it is never faulted.
	s.InnerBlockchain, err = blockchain.NewLocalClient(logger, tSettings, innerChainStore, nil, nil)
	require.NoError(t, err)

	s.Blockchain = s.InnerBlockchain

	if cfg.plan != nil {
		client, err := blockchain.NewLocalClient(logger, tSettings, s.BlockchainStore, nil, nil)
		require.NoError(t, err)

		s.Blockchain = fault.NewBlockchainClient(client, cfg.plan)
	}

	return s
}

// NewPeer starts an HTTP server standing in for a peer's DataHub and closes it when the test
// ends. Its URL is the base URL Block Validation fetches from.
func NewPeer(t testing.TB, handler http.Handler) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	return srv
}

// KafkaCapture records every message published to one in-memory Kafka topic.
type KafkaCapture struct {
	Topic string
	URL   *url.URL

	mu       sync.Mutex
	messages []*inmemorykafka.Message
	arrived  chan struct{}
}

func newKafkaCapture(t testing.TB, ctx context.Context, topic string) *KafkaCapture {
	t.Helper()

	topicURL, err := url.Parse("memory://localhost/" + topic)
	require.NoError(t, err)

	c := &KafkaCapture{Topic: topic, URL: topicURL, arrived: make(chan struct{}, 1)}

	broker := inmemorykafka.GetSharedBroker()
	group := inmemorykafka.NewInMemoryConsumerGroup(broker, topic, topic+"_capture")

	go func() { _ = group.Consume(ctx, []string{topic}, c) }()

	t.Cleanup(func() {
		_ = group.Close()
		broker.DropTopic(topic)
	})

	// The broker drops messages produced before a consumer registers.
	require.Eventually(t, func() bool { return broker.HasConsumer(topic) }, 5*time.Second, time.Millisecond,
		"capture consumer for %s never registered", topic)

	return c
}

// Messages returns a copy of the messages captured so far.
func (c *KafkaCapture) Messages() []*inmemorykafka.Message {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]*inmemorykafka.Message, len(c.messages))
	copy(out, c.messages)

	return out
}

// WaitFor waits until at least n messages have been captured and returns them, failing the
// test after timeout.
func (c *KafkaCapture) WaitFor(t testing.TB, n int, timeout time.Duration) []*inmemorykafka.Message {
	t.Helper()

	deadline := time.After(timeout)

	for {
		if msgs := c.Messages(); len(msgs) >= n {
			return msgs
		}

		select {
		case <-c.arrived:
		case <-deadline:
			require.FailNow(t, "timed out waiting for Kafka messages", "topic %s: want %d, got %d", c.Topic, n, len(c.Messages()))
		}
	}
}

// Setup implements inmemorykafka.ConsumerGroupHandler.
func (c *KafkaCapture) Setup(inmemorykafka.ConsumerGroupSession) error { return nil }

// Cleanup implements inmemorykafka.ConsumerGroupHandler.
func (c *KafkaCapture) Cleanup(inmemorykafka.ConsumerGroupSession) error { return nil }

// ConsumeClaim implements inmemorykafka.ConsumerGroupHandler.
func (c *KafkaCapture) ConsumeClaim(session inmemorykafka.ConsumerGroupSession, claim inmemorykafka.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			c.mu.Lock()
			c.messages = append(c.messages, msg)
			c.mu.Unlock()

			select {
			case c.arrived <- struct{}{}:
			default:
			}
		case <-session.Context().Done():
			return nil
		}
	}
}
