package subtreevalidation

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/testutil"
	"github.com/ordishs/go-utils/expiringmap"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// setupMemoryKafkaConsumer creates a memory Kafka consumer for testing
func setupMemoryKafkaConsumer(t *testing.T, topic string) kafka.KafkaConsumerGroupI {
	kafkaURL, err := url.Parse("memory://localhost:9092/" + topic)
	require.NoError(t, err)

	cfg := kafka.KafkaConsumerConfig{
		URL:             kafkaURL,
		BrokersURL:      []string{"localhost:9092"},
		Topic:           topic,
		ConsumerGroupID: "test-group",
		Logger:          ulogger.TestLogger{},
	}

	consumer, err := kafka.NewKafkaConsumerGroup(cfg)
	require.NoError(t, err)

	return consumer
}

// setupMemoryKafkaProducer creates a memory Kafka producer for testing
func setupMemoryKafkaProducer(t *testing.T, topic string) kafka.KafkaAsyncProducerI {
	kafkaURL, err := url.Parse("memory://localhost:9092/" + topic)
	require.NoError(t, err)

	cfg := kafka.KafkaProducerConfig{
		URL:            kafkaURL,
		BrokersURL:     []string{"localhost:9092"},
		Topic:          topic,
		FlushBytes:     1,
		FlushMessages:  1,
		FlushFrequency: time.Millisecond,
		Logger:         ulogger.TestLogger{},
	}

	producer, err := kafka.NewKafkaAsyncProducer(ulogger.TestLogger{}, cfg)
	require.NoError(t, err)

	return producer
}

// Mock implementations for testing
type mockValidator struct {
	validator.Interface
	validateFunc func(ctx context.Context, tx *bt.Tx, height uint32, opts ...validator.Option) (*meta.Data, error)
}

func (m *mockValidator) Validate(ctx context.Context, tx *bt.Tx, height uint32, opts ...validator.Option) (*meta.Data, error) {
	if m.validateFunc != nil {
		return m.validateFunc(ctx, tx, height, opts...)
	}
	return &meta.Data{}, nil
}

func (m *mockValidator) ValidateWithOptions(ctx context.Context, tx *bt.Tx, height uint32, opts *validator.Options) (*meta.Data, error) {
	if m.validateFunc != nil {
		return m.validateFunc(ctx, tx, height)
	}
	return &meta.Data{}, nil
}

func (m *mockValidator) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return http.StatusOK, "OK", nil
}

func (m *mockValidator) GetBlockHeight() uint32 {
	return 100
}

func (m *mockValidator) GetMedianBlockTime() uint32 {
	return uint32(time.Now().Unix())
}

// mockKafkaConsumer is used for tests that need to control Kafka behavior
// For most tests, use setupMemoryKafkaConsumer instead
type mockKafkaConsumer struct {
	kafka.KafkaConsumerGroupI
	startFunc      func(ctx context.Context, handler func(*kafka.KafkaMessage) error, opts ...kafka.ConsumerOption)
	closeFunc      func() error
	brokersURLFunc func() []string
}

func (m *mockKafkaConsumer) Start(ctx context.Context, handler func(*kafka.KafkaMessage) error, opts ...kafka.ConsumerOption) {
	if m.startFunc != nil {
		m.startFunc(ctx, handler, opts...)
	}
}

func (m *mockKafkaConsumer) Close() error {
	if m.closeFunc != nil {
		return m.closeFunc()
	}
	return nil
}

func (m *mockKafkaConsumer) BrokersURL() []string {
	if m.brokersURLFunc != nil {
		return m.brokersURLFunc()
	}
	return nil // Return nil to skip Kafka health check
}

// mockKafkaAsyncProducer is used for tests that need to control producer behavior
// For most tests, use setupMemoryKafkaProducer instead
type mockKafkaAsyncProducer struct {
	kafka.KafkaAsyncProducerI
	publishFunc func(msg *kafka.Message)
	startFunc   func(ctx context.Context, ch chan *kafka.Message)
}

func (m *mockKafkaAsyncProducer) Publish(msg *kafka.Message) {
	if m.publishFunc != nil {
		m.publishFunc(msg)
	}
}

func (m *mockKafkaAsyncProducer) Start(ctx context.Context, ch chan *kafka.Message) {
	if m.startFunc != nil {
		m.startFunc(ctx, ch)
	}
}

func (m *mockKafkaAsyncProducer) Input() chan<- interface{} {
	return make(chan interface{})
}

func (m *mockKafkaAsyncProducer) Close() error {
	return nil
}

func (m *mockKafkaAsyncProducer) BrokersURL() []string {
	return []string{"localhost:9092"}
}

// TestServerNew tests the New function
func TestServerNew(t *testing.T) {
	t.Run("successful creation without quorum path", func(t *testing.T) {
		common := testutil.NewCommonTestSetup(t)
		tSettings := common.Settings
		tSettings.SubtreeValidation.QuorumPath = "" // No quorum path

		subtreeStore := testutil.NewMemoryBlobStore()
		txStore := testutil.NewMemoryBlobStore()
		utxoStore := &utxo.MockUtxostore{}
		validatorClient := &mockValidator{}
		blockchainClient := testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t)
		// Use nil consumers for this test as it expects error due to missing quorum path
		subtreeConsumer := &mockKafkaConsumer{}
		txmetaConsumer := &mockKafkaConsumer{}

		server, err := New(common.Ctx, common.Logger, tSettings, subtreeStore, txStore, utxoStore,
			validatorClient, blockchainClient, subtreeConsumer, txmetaConsumer, nil)

		require.Error(t, err)
		require.Nil(t, server)
		require.Contains(t, err.Error(), "No subtree_quorum_path specified")
	})

	t.Run("successful creation with quorum path", func(t *testing.T) {
		common := testutil.NewCommonTestSetup(t)
		tSettings := common.Settings
		tSettings.SubtreeValidation.QuorumPath = "/tmp/test-quorum"
		tSettings.SubtreeValidation.TxMetaCacheEnabled = false
		tSettings.Kafka.InvalidSubtrees = "" // No Kafka producer

		subtreeStore := testutil.NewMemoryBlobStore()
		txStore := testutil.NewMemoryBlobStore()
		utxoStore := &utxo.MockUtxostore{}
		validatorClient := &mockValidator{}

		// Use real blockchain client with memory SQLite
		blockchainClient := testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t)

		// Use memory Kafka consumers
		subtreeConsumer := setupMemoryKafkaConsumer(t, "subtrees-topic")
		txmetaConsumer := setupMemoryKafkaConsumer(t, "txmeta-topic")

		server, err := New(common.Ctx, common.Logger, tSettings, subtreeStore, txStore, utxoStore,
			validatorClient, blockchainClient, subtreeConsumer, txmetaConsumer, nil)

		require.NoError(t, err)
		require.NotNil(t, server)
		require.NotNil(t, server.logger)
		require.NotNil(t, server.settings)
		require.NotNil(t, server.subtreeStore)
		require.NotNil(t, server.txStore)
		require.NotNil(t, server.utxoStore)
		require.NotNil(t, server.validatorClient)
		require.NotNil(t, server.blockchainClient)
		require.NotNil(t, server.stats)
		require.NotNil(t, server.prioritySubtreeCheckActiveMap)
	})

	t.Run("with tx meta cache enabled", func(t *testing.T) {
		common := testutil.NewCommonTestSetup(t)
		tSettings := common.Settings
		tSettings.SubtreeValidation.QuorumPath = "/tmp/test-quorum"
		tSettings.SubtreeValidation.TxMetaCacheEnabled = true
		tSettings.Kafka.InvalidSubtrees = "" // No Kafka producer

		subtreeStore := testutil.NewMemoryBlobStore()
		txStore := testutil.NewMemoryBlobStore()
		utxoStore := &utxo.MockUtxostore{}
		validatorClient := &mockValidator{}

		blockchainClient := testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t)

		// Use memory Kafka consumers
		subtreeConsumer := setupMemoryKafkaConsumer(t, "subtrees-topic-cache")
		txmetaConsumer := setupMemoryKafkaConsumer(t, "txmeta-topic-cache")

		server, err := New(common.Ctx, common.Logger, tSettings, subtreeStore, txStore, utxoStore,
			validatorClient, blockchainClient, subtreeConsumer, txmetaConsumer, nil)

		require.NoError(t, err)
		require.NotNil(t, server)
	})

	t.Run("with invalid subtree kafka producer", func(t *testing.T) {
		common := testutil.NewCommonTestSetup(t)
		tSettings := common.Settings
		tSettings.SubtreeValidation.QuorumPath = "/tmp/test-quorum"
		tSettings.Kafka.InvalidSubtrees = "invalid-subtrees-topic"

		// Set up memory Kafka
		invalidSubtreesURL, _ := url.Parse("memory://localhost:9092/invalid-subtrees")
		tSettings.Kafka.InvalidSubtreesConfig = invalidSubtreesURL

		subtreeStore := testutil.NewMemoryBlobStore()
		txStore := testutil.NewMemoryBlobStore()
		utxoStore := &utxo.MockUtxostore{}
		validatorClient := &mockValidator{}

		blockchainClient := testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t)

		// Use memory Kafka consumers
		subtreeConsumer := setupMemoryKafkaConsumer(t, "subtrees-topic-invalid")
		txmetaConsumer := setupMemoryKafkaConsumer(t, "txmeta-topic-invalid")

		server, err := New(common.Ctx, common.Logger, tSettings, subtreeStore, txStore, utxoStore,
			validatorClient, blockchainClient, subtreeConsumer, txmetaConsumer, nil)

		require.NoError(t, err)
		require.NotNil(t, server)
		require.NotNil(t, server.invalidSubtreeKafkaProducer)
	})
}

// TestServerHealth tests the Health method
func TestServerHealth(t *testing.T) {
	t.Run("liveness check", func(t *testing.T) {
		server := &Server{}

		status, msg, err := server.Health(context.Background(), true)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, "OK", msg)
	})

	t.Run("readiness check with no dependencies", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.SubtreeValidation.GRPCListenAddress = ""

		server := &Server{
			logger:   logger,
			settings: tSettings,
		}

		status, _, err := server.Health(context.Background(), false)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
	})

	t.Run("readiness check with dependencies", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.SubtreeValidation.GRPCListenAddress = ""

		utxoStore := &utxo.MockUtxostore{}
		// Use mock.Anything for the context parameter since it might be wrapped
		utxoStore.On("Health", mock.Anything, false).Return(http.StatusOK, "OK", nil)

		server := &Server{
			logger:               logger,
			settings:             tSettings,
			txmetaConsumerClient: &mockKafkaConsumer{},
			blockchainClient:     testutil.NewMemorySQLiteBlockchainClient(logger, tSettings, t),
			subtreeStore:         memory.New(),
			utxoStore:            utxoStore,
		}

		status, msg, err := server.Health(context.Background(), false)
		if status != http.StatusOK {
			t.Logf("Health check failed with status %d: %s", status, msg)
		}
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
	})

	t.Run("readiness check with gRPC configured", func(t *testing.T) {
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		// Use a port that's unlikely to be in use
		tSettings.SubtreeValidation.GRPCListenAddress = "localhost:19086"

		server := &Server{
			logger:   logger,
			settings: tSettings,
		}

		status, msg, _ := server.Health(context.Background(), false)
		require.NotEqual(t, http.StatusOK, status)
		require.Contains(t, msg, "gRPC Server")
	})
}

// TestServerHealthGRPC tests the HealthGRPC method
func TestServerHealthGRPC(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.SubtreeValidation.GRPCListenAddress = ""

	server := &Server{
		logger:   logger,
		settings: tSettings,
	}

	response, err := server.HealthGRPC(context.Background(), &subtreevalidation_api.EmptyMessage{})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.True(t, response.Ok)
	require.NotNil(t, response.Timestamp)
	// The details field contains JSON-formatted dependency status
	require.Contains(t, response.Details, "status")
}

// TestServerInit tests the Init method
func TestServerInit(t *testing.T) {
	server := &Server{}

	err := server.Init(context.Background())
	require.NoError(t, err)
}

// TestGetSetUutxoStore tests the GetUutxoStore and SetUutxoStore methods
func TestGetSetUutxoStore(t *testing.T) {
	server := &Server{}

	// Test GetUutxoStore with nil
	require.Nil(t, server.GetUutxoStore())

	// Test SetUutxoStore
	utxoStore := &utxo.MockUtxostore{}
	server.SetUutxoStore(utxoStore)

	// Test GetUutxoStore after setting
	require.Equal(t, utxoStore, server.GetUutxoStore())
}

// TestServerStart tests the Start method
func TestServerStart(t *testing.T) {
	t.Run("FSM wait error", func(t *testing.T) {
		// Create mock blockchain client that returns error on FSM wait
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("WaitUntilFSMTransitionFromIdleState", mock.Anything).
			Return(errors.NewServiceError("FSM wait failed"))

		server := &Server{
			logger:           ulogger.TestLogger{},
			blockchainClient: mockBlockchainClient,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		readyCh := make(chan struct{})
		err := server.Start(ctx, readyCh)
		require.Error(t, err)
		require.Contains(t, err.Error(), "FSM wait failed")

		mockBlockchainClient.AssertExpectations(t)
	})

	t.Run("context cancelled during FSM wait", func(t *testing.T) {
		// Create mock blockchain client that blocks until context is cancelled
		mockBlockchainClient := &blockchain.Mock{}
		mockBlockchainClient.On("WaitUntilFSMTransitionFromIdleState", mock.Anything).
			Return(context.Canceled)

		server := &Server{
			logger:           ulogger.TestLogger{},
			blockchainClient: mockBlockchainClient,
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		readyCh := make(chan struct{})
		err := server.Start(ctx, readyCh)
		require.Error(t, err)

		mockBlockchainClient.AssertExpectations(t)
	})
}

// TestServerStop tests the Stop method
func TestServerStop(t *testing.T) {
	t.Run("successful stop", func(t *testing.T) {
		closeCalled := 0
		server := &Server{
			logger: ulogger.TestLogger{},
			subtreeConsumerClient: &mockKafkaConsumer{
				closeFunc: func() error {
					closeCalled++
					return nil
				},
			},
			txmetaConsumerClient: &mockKafkaConsumer{
				closeFunc: func() error {
					closeCalled++
					return nil
				},
			},
		}

		err := server.Stop(context.Background())
		require.NoError(t, err)
		require.Equal(t, 2, closeCalled)
	})

	t.Run("stop with errors", func(t *testing.T) {
		server := &Server{
			logger: ulogger.TestLogger{},
			subtreeConsumerClient: &mockKafkaConsumer{
				closeFunc: func() error {
					return errors.NewStorageError("close error 1")
				},
			},
			txmetaConsumerClient: &mockKafkaConsumer{
				closeFunc: func() error {
					return errors.NewStorageError("close error 2")
				},
			},
		}

		err := server.Stop(context.Background())
		require.NoError(t, err) // Stop doesn't return errors
	})
}

// TestCheckSubtreeFromBlock tests the CheckSubtreeFromBlock method
func TestCheckSubtreeFromBlock(t *testing.T) {
	t.Run("error in checkSubtreeFromBlock", func(t *testing.T) {
		server := &Server{
			logger: ulogger.TestLogger{},
		}

		request := &subtreevalidation_api.CheckSubtreeFromBlockRequest{
			BaseUrl: "",
		}

		response, err := server.CheckSubtreeFromBlock(context.Background(), request)
		require.Error(t, err)
		require.Nil(t, response)
	})

	t.Run("nil hash error", func(t *testing.T) {
		server := &Server{
			logger:                        ulogger.TestLogger{},
			prioritySubtreeCheckActiveMap: make(map[string]bool),
			settings:                      test.CreateBaseTestSettings(t),
		}

		request := &subtreevalidation_api.CheckSubtreeFromBlockRequest{
			BaseUrl:           "http://example.com",
			Hash:              nil, // nil hash causes error
			BlockHash:         make([]byte, 32),
			PreviousBlockHash: make([]byte, 32),
			BlockHeight:       100,
		}

		response, err := server.CheckSubtreeFromBlock(context.Background(), request)
		require.Error(t, err)
		require.Nil(t, response)
	})

	t.Run("with global quorum initialized", func(t *testing.T) {
		t.Skip("Skipping test - requires mock blockchain client")
		// This test needs a mock blockchain client that implements GetBlockHeaderIDs
		// The test causes a panic when blockchainClient is nil
		// TODO: Create a proper mock blockchain client for this test
	})
}

// TestServerProcessOrphans tests the processOrphans method
func TestServerProcessOrphans(t *testing.T) {
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	orphanage, err := NewOrphanage(time.Minute, 100, &logger)
	require.NoError(t, err)
	server := &Server{
		logger:          logger,
		settings:        tSettings,
		orphanage:       orphanage,
		validatorClient: &mockValidator{},
		stats:           gocore.NewStat("test"),
	}

	// Add some orphan transactions
	tx1, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff08044c86041b020602ffffffff0100f2052a010000004341041b0e8c2567c12536aa13357b79a073dc4444acb83c4ec7a0e2f99dd7457516c5817242da796924ca4e99947d087fedf9ce467cb9f7c6287078f801df276fdf84ac00000000")
	server.orphanage.Set(*tx1.TxIDChainHash(), tx1)

	blockHash, _ := chainhash.NewHashFromStr("000000000002d01c1fccc21636b607dfd930d31d01c3a62104612a1719011250")
	blockIds := map[uint32]bool{100: true, 99: true}

	// This should process orphans without error
	server.processOrphans(context.Background(), *blockHash, 100, blockIds)
}

// TestInitialiseInvalidSubtreeKafkaProducer tests the initialiseInvalidSubtreeKafkaProducer function
func TestInitialiseInvalidSubtreeKafkaProducer(t *testing.T) {
	t.Run("successful initialization", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Kafka.InvalidSubtrees = "invalid-subtrees-topic"

		// Set up memory Kafka
		invalidSubtreesURL, _ := url.Parse("memory://localhost:9092/invalid-subtrees")
		tSettings.Kafka.InvalidSubtreesConfig = invalidSubtreesURL

		producer, err := initialiseInvalidSubtreeKafkaProducer(ctx, logger, tSettings)
		require.NoError(t, err)
		require.NotNil(t, producer)
	})

	t.Run("initialization with invalid URL", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.TestLogger{}
		tSettings := test.CreateBaseTestSettings(t)
		tSettings.Kafka.InvalidSubtrees = "invalid-subtrees-topic"

		// Use an invalid URL that will cause an error during producer creation
		invalidURL, _ := url.Parse("invalid://bad-url")
		tSettings.Kafka.InvalidSubtreesConfig = invalidURL

		producer, err := initialiseInvalidSubtreeKafkaProducer(ctx, logger, tSettings)
		require.Error(t, err)
		require.Nil(t, producer)
	})
}

// TestPublishInvalidSubtree tests the publishInvalidSubtree method
func TestPublishInvalidSubtree(t *testing.T) {
	t.Run("no producer configured", func(t *testing.T) {
		server := &Server{
			logger: ulogger.TestLogger{},
		}

		// Should return early without error
		server.publishInvalidSubtree(context.Background(), "hash", "peer", "reason")
	})

	t.Run("FSM state catching blocks", func(t *testing.T) {
		publishCalled := false
		server := &Server{
			logger: ulogger.TestLogger{},
			invalidSubtreeKafkaProducer: &mockKafkaAsyncProducer{
				publishFunc: func(msg *kafka.Message) {
					publishCalled = true
				},
			},
			// blockchainClient is nil, so FSM check is skipped and message is published
			invalidSubtreeDeDuplicateMap: expiringmap.New[string, struct{}](time.Minute),
		}

		server.publishInvalidSubtree(context.Background(), "hash", "peer", "reason")
		require.True(t, publishCalled) // With nil blockchain client, message should be published
	})

	t.Run("successful publish", func(t *testing.T) {
		var publishedMsg *kafka.Message
		server := &Server{
			logger: ulogger.TestLogger{},
			invalidSubtreeKafkaProducer: &mockKafkaAsyncProducer{
				publishFunc: func(msg *kafka.Message) {
					publishedMsg = msg
				},
			},
			// blockchainClient: nil, // Will cause FSM check to return false
			invalidSubtreeDeDuplicateMap: expiringmap.New[string, struct{}](time.Minute),
		}

		server.publishInvalidSubtree(context.Background(), "hash123", "peer456", "test reason")
		require.NotNil(t, publishedMsg)
		require.Equal(t, []byte("hash123"), publishedMsg.Key)

		// Verify the message content
		var msg kafkamessage.KafkaInvalidSubtreeTopicMessage
		err := proto.Unmarshal(publishedMsg.Value, &msg)
		require.NoError(t, err)
		require.Equal(t, "hash123", msg.SubtreeHash)
		require.Equal(t, "peer456", msg.PeerUrl)
		require.Equal(t, "test reason", msg.Reason)
	})

	t.Run("duplicate message", func(t *testing.T) {
		common := testutil.NewCommonTestSetup(t)
		publishCount := 0
		server := &Server{
			logger: common.Logger,
			invalidSubtreeKafkaProducer: &mockKafkaAsyncProducer{
				publishFunc: func(msg *kafka.Message) {
					publishCount++
				},
			},
			blockchainClient:             testutil.NewMemorySQLiteBlockchainClient(common.Logger, common.Settings, t),
			invalidSubtreeDeDuplicateMap: expiringmap.New[string, struct{}](time.Minute),
		}

		// First publish
		server.publishInvalidSubtree(context.Background(), "hash123", "peer", "reason")
		require.Equal(t, 1, publishCount)

		// Second publish with same hash should be deduplicated
		server.publishInvalidSubtree(context.Background(), "hash123", "peer", "reason")
		require.Equal(t, 1, publishCount)
	})
}

// TestCheckSubtreeFromBlockInternal tests the checkSubtreeFromBlock internal method
func TestCheckSubtreeFromBlockInternal(t *testing.T) {
	t.Run("missing base URL", func(t *testing.T) {
		server := &Server{
			logger: ulogger.TestLogger{},
			stats:  gocore.NewStat("test"),
		}

		request := &subtreevalidation_api.CheckSubtreeFromBlockRequest{
			BaseUrl: "",
		}

		ok, err := server.checkSubtreeFromBlock(context.Background(), request)
		require.Error(t, err)
		require.False(t, ok)
		require.Contains(t, err.Error(), "Missing base URL")
	})

	t.Run("invalid hash", func(t *testing.T) {
		server := &Server{
			logger: ulogger.TestLogger{},
			stats:  gocore.NewStat("test"),
		}

		request := &subtreevalidation_api.CheckSubtreeFromBlockRequest{
			BaseUrl:           "http://example.com",
			Hash:              []byte("invalid"),
			BlockHash:         make([]byte, 32),
			PreviousBlockHash: make([]byte, 32),
		}

		ok, err := server.checkSubtreeFromBlock(context.Background(), request)
		require.Error(t, err)
		require.False(t, ok)
		require.Contains(t, err.Error(), "Failed to parse subtree hash")
	})

	t.Run("invalid block hash", func(t *testing.T) {
		server := &Server{
			logger: ulogger.TestLogger{},
			stats:  gocore.NewStat("test"),
		}

		request := &subtreevalidation_api.CheckSubtreeFromBlockRequest{
			BaseUrl:           "http://example.com",
			Hash:              make([]byte, 32),
			BlockHash:         []byte("invalid"),
			PreviousBlockHash: make([]byte, 32),
		}

		ok, err := server.checkSubtreeFromBlock(context.Background(), request)
		require.Error(t, err)
		require.False(t, ok)
		require.Contains(t, err.Error(), "Failed to parse block hash")
	})

	t.Run("invalid previous block hash", func(t *testing.T) {
		server := &Server{
			logger:                        ulogger.TestLogger{},
			stats:                         gocore.NewStat("test"),
			prioritySubtreeCheckActiveMap: make(map[string]bool),
		}

		request := &subtreevalidation_api.CheckSubtreeFromBlockRequest{
			BaseUrl:           "http://example.com",
			Hash:              make([]byte, 32),
			BlockHash:         make([]byte, 32),
			PreviousBlockHash: []byte("invalid"),
		}

		ok, err := server.checkSubtreeFromBlock(context.Background(), request)
		require.Error(t, err)
		require.False(t, ok)
		require.Contains(t, err.Error(), "Failed to parse previous block hash")
	})
}
