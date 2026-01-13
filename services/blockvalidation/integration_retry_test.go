package blockvalidation

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/jellydator/ttlcache/v3"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestIntegrationRetryWithMultipleFailures tests the complete retry flow with multiple failures
func TestIntegrationRetryWithMultipleFailures(t *testing.T) {
	initPrometheusMetrics()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxParallelForks = 4
	// BlockProcessingWorkers setting removed - using default workers

	// Create test blockchain
	blocks := testhelpers.CreateTestBlockChain(t, 10)

	// Create mock blockchain client and stores
	mockBlockchainStore := blockchain_store.NewMockStore()
	mockBlockchainClient, err := blockchain.NewLocalClient(logger, tSettings, mockBlockchainStore, nil, nil)
	require.NoError(t, err)

	// Store initial blocks
	for i := 0; i < 5; i++ {
		err := mockBlockchainClient.AddBlock(ctx, blocks[i], "test-peer")
		require.NoError(t, err)
	}

	// Create mock validator
	mockValidator := &validator.MockValidator{}
	subtreeStore := memory.New()

	// Create block validation
	bv := NewBlockValidation(ctx, logger, tSettings, mockBlockchainClient, subtreeStore, nil, nil, mockValidator, nil)

	// Create server
	mockKafkaConsumer := &mockKafkaConsumer{}
	mockKafkaConsumer.On("Start", mock.Anything, mock.Anything, mock.Anything).Return()
	mockKafkaConsumer.On("Close").Return(nil)
	mockKafkaConsumer.On("BrokersURL").Return([]string{"localhost:9092"})

	server := &Server{
		logger:              logger,
		settings:            tSettings,
		blockchainClient:    mockBlockchainClient,
		blockValidation:     bv,
		subtreeStore:        subtreeStore,
		txStore:             nil,
		utxoStore:           nil,
		validatorClient:     mockValidator,
		blockAssemblyClient: nil,
		blockFoundCh:        make(chan processBlockFound, 20),
		blockPriorityQueue:  NewBlockPriorityQueue(logger),
		blockClassifier:     NewBlockClassifier(logger, 10, mockBlockchainClient),
		forkManager:         NewForkManager(logger, tSettings),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
		catchupCh:           make(chan processBlockCatchup, 10),
		kafkaConsumerClient: mockKafkaConsumer,
		stats:               gocore.NewStat("test"),
	}

	// Initialize server
	err = server.Init(ctx)
	require.NoError(t, err)

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	t.Run("Multiple_Peers_Sequential_Failures", func(t *testing.T) {
		targetBlock := blocks[5]
		targetHash := targetBlock.Hash()

		// Track attempts per peer
		peer1Attempts := int32(0)
		peer2Attempts := int32(0)
		peer3Attempts := int32(0)

		// Peer 1: Fails twice, then succeeds
		httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer1/block/%s", targetHash),
			func(req *http.Request) (*http.Response, error) {
				atomic.AddInt32(&peer1Attempts, 1)
				time.Sleep(100 * time.Millisecond)
				return nil, errors.NewNetworkError("peer3 timeout")
			})

		// Peer 2: Always fails
		httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer2/block/%s", targetHash),
			func(req *http.Request) (*http.Response, error) {
				atomic.AddInt32(&peer2Attempts, 1)
				return nil, errors.NewNetworkError("peer2 permanent failure")
			})

		// Peer 3: Slow response
		httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer3/block/%s", targetHash),
			func(req *http.Request) (*http.Response, error) {
				atomic.AddInt32(&peer3Attempts, 1)
				blockBytes, _ := targetBlock.Bytes()
				return httpmock.NewBytesResponse(200, blockBytes), nil
			})

		// Create Kafka messages from multiple peers
		peers := []struct {
			url    string
			peerID string
		}{
			{"http://peer1", "peer1"},
			{"http://peer2", "peer2"},
			{"http://peer3", "peer3"},
		}

		// Send announcements from all peers
		for _, peer := range peers {
			kafkaMsg := &kafkamessage.KafkaBlockTopicMessage{
				Hash:   targetHash.String(),
				URL:    peer.url,
				PeerId: peer.peerID,
			}

			msgBytes, err := proto.Marshal(kafkaMsg)
			require.NoError(t, err)

			kafkaMessage := &kafka.KafkaMessage{
				ConsumerMessage: sarama.ConsumerMessage{
					Value: msgBytes,
				},
			}

			handler := server.consumerMessageHandler(ctx)
			go func() {
				// We expect this to succeed eventually
				assert.NoError(t, handler(kafkaMessage))
			}()
		}

		// Wait for processing with retries
		var exists bool
		for i := 0; i < 20; i++ {
			exists, err = server.blockValidation.GetBlockExists(ctx, targetHash)
			require.NoError(t, err)
			if exists {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}

		assert.True(t, exists, "Block should eventually be processed")

		// Verify attempts
		assert.GreaterOrEqual(t, int(atomic.LoadInt32(&peer1Attempts)), 1)
		assert.GreaterOrEqual(t, int(atomic.LoadInt32(&peer2Attempts)), 1)
		assert.GreaterOrEqual(t, int(atomic.LoadInt32(&peer3Attempts)), 1)
	})
}

// TestEdgeCasesAndErrorScenarios tests various edge cases
func TestEdgeCasesAndErrorScenarios(t *testing.T) {
	initPrometheusMetrics()
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Create mock blockchain store and client
	mockBlockchainStore := blockchain_store.NewMockStore()
	mockBlockchainClient, err := blockchain.NewLocalClient(logger, tSettings, mockBlockchainStore, nil, nil)
	require.NoError(t, err)

	// Create mock validator
	mockValidator := &validator.MockValidator{}

	// Create memory stores for testing
	subtreeStore := memory.New()
	txStore := memory.New()
	mockUtxoStore := &utxo.MockUtxostore{}

	// Create block validation
	bv := NewBlockValidation(ctx, logger, tSettings, mockBlockchainClient, subtreeStore, txStore, mockUtxoStore, mockValidator, nil)

	server := &Server{
		logger:              logger,
		settings:            tSettings,
		blockchainClient:    mockBlockchainClient,
		blockValidation:     bv,
		blockPriorityQueue:  NewBlockPriorityQueue(logger),
		blockClassifier:     NewBlockClassifier(logger, 10, mockBlockchainClient),
		forkManager:         NewForkManager(logger, tSettings),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
	}

	t.Run("Empty_BaseURL_On_Retry", func(t *testing.T) {
		hash := &chainhash.Hash{0x10}

		// Create retry block with empty baseURL
		retryBlock := processBlockFound{
			hash:    hash,
			baseURL: "",
			peerID:  "",
		}

		// Should handle gracefully
		err := server.processBlockWithPriority(ctx, retryBlock)
		require.Error(t, err)
	})

	t.Run("Duplicate_Retry_Requests", func(t *testing.T) {
		hash := &chainhash.Hash{0x11}

		// Add block
		blockFound := processBlockFound{
			hash:    hash,
			baseURL: "http://test",
			peerID:  "test",
		}

		server.blockPriorityQueue.Add(blockFound, PriorityChainExtending, 100)

		// Multiple retry attempts
		for i := 0; i < 5; i++ {
			server.blockPriorityQueue.RequeueForRetry(blockFound, PriorityDeepFork, 100)
		}

		// Should still have only one entry
		assert.Equal(t, 1, server.blockPriorityQueue.Size())

		// Verify block is still in queue
		assert.True(t, server.blockPriorityQueue.Contains(*hash))
	})

	t.Run("Priority_Queue_Overflow", func(t *testing.T) {
		// Clear queue
		server.blockPriorityQueue.Clear()

		// Add many blocks
		for i := 0; i < 1000; i++ {
			hash := chainhash.Hash{}
			hash[0] = byte(i & 0xFF)
			hash[1] = byte((i >> 8) & 0xFF)

			blockFound := processBlockFound{
				hash:    &hash,
				baseURL: fmt.Sprintf("http://peer%d", i),
				peerID:  fmt.Sprintf("peer%d", i),
			}

			priority := PriorityDeepFork
			if i%10 == 0 {
				priority = PriorityChainExtending
			} else if i%5 == 0 {
				priority = PriorityNearFork
			}

			server.blockPriorityQueue.Add(blockFound, priority, uint32(1000+i))
		}

		assert.Equal(t, 1000, server.blockPriorityQueue.Size())

		// Verify priority order - chain extending should come first
		mockBP := &mockBlockProcessor{}
		firstBlock, status := server.blockPriorityQueue.Get(context.Background(), mockBP)
		require.Equal(t, GetOK, status)
		assert.Equal(t, "http://peer0", firstBlock.baseURL)
	})

	// Note: Test for malicious peer recovery removed as peerMetrics field
	// has been removed from Server struct. Tests should be updated to use
	// mock p2pClient instead for peer metrics functionality.
}
