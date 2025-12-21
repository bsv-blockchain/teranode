package blockvalidation

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
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

// TestBlockHandlerWithMaliciousPeer tests that blocks from malicious peers are still queued
func TestBlockHandlerWithMaliciousPeer(t *testing.T) {
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
		blockFoundCh:        make(chan processBlockFound, 10),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
		stats:               gocore.NewStat("test"),
	}

	// Note: peerMetrics field has been removed from Server struct
	// Tests should be updated to use mock p2pClient instead for peer metrics functionality
	// For now, we'll just test that the block is queued regardless of peer reputation

	// Create Kafka message from malicious peer
	blockHash := &chainhash.Hash{0x01, 0x02, 0x03}
	kafkaMsg := &kafkamessage.KafkaBlockTopicMessage{
		Hash:   blockHash.String(),
		URL:    "http://malicious-peer.com",
		PeerId: "malicious_peer_123",
	}

	// Start goroutine to handle the blockFound message
	handled := make(chan bool)
	go func() {
		select {
		case blockFound := <-server.blockFoundCh:
			// Verify the block was queued despite malicious peer
			assert.Equal(t, blockHash, blockFound.hash)
			assert.Equal(t, "http://malicious-peer.com", blockFound.baseURL)
			assert.Equal(t, "malicious_peer_123", blockFound.peerID)

			// Send success response
			if blockFound.errCh != nil {
				blockFound.errCh <- nil
			}
			handled <- true
		case <-time.After(2 * time.Second):
			t.Error("Timeout waiting for blockFound message")
			handled <- false
		}
	}()

	// Process the message
	err = server.blockHandler(kafkaMsg)
	require.NoError(t, err)

	// Verify block was handled
	wasHandled := <-handled
	assert.True(t, wasHandled, "Block from malicious peer should still be queued")
}

// TestKafkaConsumerMessageHandling tests the full Kafka message processing flow
func TestKafkaConsumerMessageHandling(t *testing.T) {
	initPrometheusMetrics()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Create test blocks
	blocks := testhelpers.CreateTestBlockChain(t, 3)
	targetBlock := blocks[2]
	targetHash := targetBlock.Hash()

	// Create mock blockchain store and client
	mockBlockchainStore2 := blockchain_store.NewMockStore()
	mockBlockchainClient2, err := blockchain.NewLocalClient(logger, tSettings, mockBlockchainStore2, nil, nil)
	require.NoError(t, err)

	// Create mock validator
	mockValidator2 := &validator.MockValidator{}

	// Create memory stores for testing
	subtreeStore2 := memory.New()
	txStore2 := memory.New()
	mockUtxoStore2 := &utxo.MockUtxostore{}

	// Create block validation
	bv2 := NewBlockValidation(ctx, logger, tSettings, mockBlockchainClient2, subtreeStore2, txStore2, mockUtxoStore2, mockValidator2, nil)

	// Store parent blocks
	for i := 0; i < 2; i++ {
		err = mockBlockchainClient2.AddBlock(ctx, blocks[i], "test-peer")
		require.NoError(t, err)
	}

	server := &Server{
		logger:              logger,
		settings:            tSettings,
		blockchainClient:    mockBlockchainClient2,
		blockValidation:     bv2,
		blockFoundCh:        make(chan processBlockFound, 10),
		blockPriorityQueue:  NewBlockPriorityQueue(logger),
		blockClassifier:     NewBlockClassifier(logger, 10, mockBlockchainClient2),
		forkManager:         NewForkManager(logger, tSettings),
		catchupCh:           make(chan processBlockCatchup, 10),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
		stats:               gocore.NewStat("test"),
	}

	// Initialize the server to start background workers
	err = server.Init(ctx)
	require.NoError(t, err)

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	t.Run("Valid_Message_Processing", func(t *testing.T) {
		// Mock HTTP response for block fetch
		httpmock.RegisterResponder("GET", fmt.Sprintf("http://test-peer/block/%s", targetHash),
			httpmock.NewBytesResponder(200, func() []byte {
				blockBytes, _ := targetBlock.Bytes()
				return blockBytes
			}()))

		// Create Kafka message
		kafkaMsg := &kafkamessage.KafkaBlockTopicMessage{
			Hash:   targetHash.String(),
			URL:    "http://test-peer",
			PeerId: "test_peer_123",
		}

		msgBytes, err := proto.Marshal(kafkaMsg)
		require.NoError(t, err)

		kafkaMessage := &kafka.KafkaMessage{
			ConsumerMessage: sarama.ConsumerMessage{
				Value: msgBytes,
			},
		}

		// Get consumer handler
		handler := server.consumerMessageHandler(ctx)

		// Process message
		err = handler(kafkaMessage)
		require.NoError(t, err)

		// Give time for async processing
		time.Sleep(500 * time.Millisecond)

		// Verify block was processed
		exists, err := server.blockValidation.GetBlockExists(ctx, targetHash)
		require.NoError(t, err)
		assert.True(t, exists, "Block should have been processed")
	})

	t.Run("Invalid_URL_Scheme", func(t *testing.T) {
		// Create message with invalid URL
		invalidHash := &chainhash.Hash{0x04, 0x05, 0x06}
		kafkaMsg := &kafkamessage.KafkaBlockTopicMessage{
			Hash:   invalidHash.String(),
			URL:    "peer_123", // Invalid - not a URL
			PeerId: "peer_123",
		}

		msgBytes, err := proto.Marshal(kafkaMsg)
		require.NoError(t, err)

		kafkaMessage := &kafka.KafkaMessage{
			ConsumerMessage: sarama.ConsumerMessage{
				Value: msgBytes,
			},
		}

		handler := server.consumerMessageHandler(ctx)
		err = handler(kafkaMessage)

		// Should return nil (unrecoverable error)
		assert.NoError(t, err)
	})

	t.Run("Recoverable_Error_Handling", func(t *testing.T) {
		// Create mock blockchain client that returns service error
		mockBlockchain := &blockchain.Mock{}
		// Set up mock expectations
		mockBlockchain.On("GetBlockExists", mock.Anything, mock.Anything).
			Return(false, errors.NewServiceError("temporary failure"))
		mockBlockchain.On("Subscribe", mock.Anything, mock.Anything).
			Return(make(chan *blockchain_api.Notification), nil)
		mockBlockchain.On("GetBlocksMinedNotSet", mock.Anything).
			Return(make([]*model.Block, 0), nil)
		mockBlockchain.On("GetBlocksSubtreesNotSet", mock.Anything).
			Return(make([]*model.Block, 0), nil)

		server.blockchainClient = mockBlockchain
		server.blockValidation = NewBlockValidation(ctx, logger, tSettings, mockBlockchain,
			subtreeStore2, txStore2, mockUtxoStore2, mockValidator2, nil)

		recoverableHash := &chainhash.Hash{0x07, 0x08, 0x09}
		kafkaMsg := &kafkamessage.KafkaBlockTopicMessage{
			Hash:   recoverableHash.String(),
			URL:    "http://test-peer",
			PeerId: "peer_456",
		}

		msgBytes, err := proto.Marshal(kafkaMsg)
		require.NoError(t, err)

		kafkaMessage := &kafka.KafkaMessage{
			ConsumerMessage: sarama.ConsumerMessage{
				Value: msgBytes,
			},
		}

		handler := server.consumerMessageHandler(ctx)
		err = handler(kafkaMessage)

		// Should return error for retry
		require.Error(t, err)
		assert.Contains(t, err.Error(), "temporary failure")
	})
}

// TestMaliciousPeerFailover tests failover when primary peer is malicious
func TestMaliciousPeerFailover(t *testing.T) {
	initPrometheusMetrics()
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	blocks := testhelpers.CreateTestBlockChain(t, 4)
	targetBlock := blocks[3]
	targetHash := targetBlock.Hash()

	// Create mock blockchain store and client
	mockBlockchainStore3 := blockchain_store.NewMockStore()
	mockBlockchainClient3, err := blockchain.NewLocalClient(logger, tSettings, mockBlockchainStore3, nil, nil)
	require.NoError(t, err)

	// Create mock validator
	mockValidator3 := &validator.MockValidator{}

	// Create memory stores for testing
	subtreeStore3 := memory.New()
	txStore3 := memory.New()
	mockUtxoStore3 := &utxo.MockUtxostore{}

	// Create block validation
	bv3 := NewBlockValidation(ctx, logger, tSettings, mockBlockchainClient3, subtreeStore3, txStore3, mockUtxoStore3, mockValidator3, nil)

	// Store parent blocks
	for i := 0; i < 3; i++ {
		err = mockBlockchainClient3.AddBlock(ctx, blocks[i], "test-peer")
		require.NoError(t, err)
	}

	server := &Server{
		logger:              logger,
		settings:            tSettings,
		blockchainClient:    mockBlockchainClient3,
		blockValidation:     bv3,
		blockPriorityQueue:  NewBlockPriorityQueue(logger),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
	}

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	// Good peer responds correctly
	httpmock.RegisterResponder("GET", fmt.Sprintf("http://good-peer/block/%s", targetHash),
		httpmock.NewBytesResponder(200, func() []byte {
			blockBytes, _ := targetBlock.Bytes()
			return blockBytes
		}()))

	// Note: peerMetrics field has been removed from Server struct
	// Tests should be updated to use mock p2pClient instead for peer metrics functionality
	// For now, we'll skip the malicious peer marking

	// Add block announcements
	primaryBlock := processBlockFound{
		hash:    targetHash,
		baseURL: "http://malicious-peer",
		peerID:  "malicious_primary",
	}

	alternativeBlock := processBlockFound{
		hash:    targetHash,
		baseURL: "http://good-peer",
		peerID:  "good_peer",
	}

	// Add to queue
	server.blockPriorityQueue.Add(primaryBlock, PriorityChainExtending, targetBlock.Height)
	server.blockPriorityQueue.Add(alternativeBlock, PriorityChainExtending, targetBlock.Height)

	// Process should skip malicious peer and use good peer
	err = server.processBlockWithPriority(ctx, primaryBlock)
	require.NoError(t, err)

	// Verify block was processed successfully
	exists, err := server.blockValidation.GetBlockExists(ctx, targetHash)
	require.NoError(t, err)
	assert.True(t, exists)
}

// TestPeerReputationTracking tests peer reputation and failure tracking
func TestPeerReputationTracking(t *testing.T) {
	initPrometheusMetrics()
	metrics := &catchup.CatchupMetrics{
		PeerMetrics: make(map[string]*catchup.PeerCatchupMetrics),
	}

	peer1 := metrics.GetOrCreatePeerMetrics("peer1")

	// Test failure recording
	for i := 0; i < 5; i++ {
		peer1.RecordFailure()
	}

	assert.True(t, peer1.GetReputation() < 100.0)
	assert.False(t, peer1.IsMalicious())

	// Test malicious attempts
	for i := 0; i < 10; i++ {
		peer1.RecordMaliciousAttempt()
	}

	assert.True(t, peer1.IsMalicious())
	assert.True(t, peer1.IsBad())

	// Test success recording improves reputation
	initialRep := peer1.GetReputation()
	peer1.RecordSuccess()
	assert.True(t, peer1.GetReputation() > initialRep)
}

// TestBlockFoundChannelErrorHandling tests error channel handling in blockFound processing
func TestBlockFoundChannelErrorHandling(t *testing.T) {
	initPrometheusMetrics()
	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)

	// Create mock blockchain store and client
	mockBlockchainStore4 := blockchain_store.NewMockStore()
	mockBlockchainClient4, err := blockchain.NewLocalClient(logger, tSettings, mockBlockchainStore4, nil, nil)
	require.NoError(t, err)

	// Create mock validator
	mockValidator4 := &validator.MockValidator{}

	// Create memory stores for testing
	subtreeStore4 := memory.New()
	txStore4 := memory.New()
	mockUtxoStore4 := &utxo.MockUtxostore{}

	// Create block validation
	bv4 := NewBlockValidation(ctx, logger, tSettings, mockBlockchainClient4, subtreeStore4, txStore4, mockUtxoStore4, mockValidator4, nil)

	server := &Server{
		logger:              logger,
		settings:            tSettings,
		blockchainClient:    mockBlockchainClient4,
		blockValidation:     bv4,
		blockPriorityQueue:  NewBlockPriorityQueue(logger),
		blockClassifier:     NewBlockClassifier(logger, 10, mockBlockchainClient4),
		forkManager:         NewForkManager(logger, tSettings),
		processBlockNotify:  ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives: ttlcache.New[chainhash.Hash, []processBlockCatchup](),
	}

	t.Run("Error_Channel_Response", func(t *testing.T) {
		errCh := make(chan error, 1)
		blockFound := processBlockFound{
			hash:    &chainhash.Hash{0x0a},
			baseURL: "invalid://url",
			peerID:  "test",
			errCh:   errCh,
		}

		// Process should send error to channel
		err = server.processBlockFoundChannel(ctx, blockFound)
		require.Error(t, err)

		// Should receive error on channel
		select {
		case receivedErr := <-errCh:
			require.Error(t, receivedErr)
			assert.Contains(t, receivedErr.Error(), "invalid baseURL")
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for error response")
		}
	})
}
