package p2p

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/kafka"
	kafkamessage "github.com/bitcoin-sv/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestKafkaAsyncProducer is a test implementation of kafka.KafkaAsyncProducerI
type TestKafkaAsyncProducer struct {
	mock.Mock
	publishedMessages []*kafka.Message
	mu                sync.Mutex
}

func (m *TestKafkaAsyncProducer) Publish(msg *kafka.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishedMessages = append(m.publishedMessages, msg)
	m.Called(msg)
}

func (m *TestKafkaAsyncProducer) GetPublishedMessages() []*kafka.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.publishedMessages
}

func (m *TestKafkaAsyncProducer) BrokersURL() []string {
	args := m.Called()
	if args.Get(0) != nil {
		return args.Get(0).([]string)
	}
	return []string{"localhost:9092"}
}

func (m *TestKafkaAsyncProducer) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *TestKafkaAsyncProducer) Start(ctx context.Context, ch chan *kafka.Message) {
	m.Called(ctx, ch)
}

func (m *TestKafkaAsyncProducer) Stop() error {
	args := m.Called()
	return args.Error(0)
}

func TestProcessBufferedAnnouncementsWithSyncPeer(t *testing.T) {
	tSettings := createBaseTestSettings()
	tSettings.ChainCfgParams = &chaincfg.RegressionNetParams

	t.Run("sends_only_sync_peer_best_block_to_kafka", func(t *testing.T) {
		// Setup
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		logger := ulogger.New("test")
		s := &Server{
			logger:       logger,
			blockPeerMap: sync.Map{},
		}

		// Create sync manager
		sm := NewSyncManager(logger, tSettings)
		s.syncManager = sm

		// Create mock Kafka producer
		mockProducer := &TestKafkaAsyncProducer{}
		s.blocksKafkaProducerClient = mockProducer

		// Set up sync peer
		syncPeerID := peer.ID("sync-peer-123")
		sm.SetLocalHeightCallback(func() uint32 { return 100 })
		sm.SetPeerHeightCallback(func(p peer.ID) int32 {
			if p == syncPeerID {
				return 150
			}
			return 90
		})

		// Add sync peer and force selection
		sm.AddPeer(syncPeerID)
		sm.UpdatePeerHeight(syncPeerID, 150)

		// Force sync peer selection by directly setting it (but keep initialSelectionDone false for buffering)
		sm.mu.Lock()
		sm.syncPeer = syncPeerID
		sm.syncPeerState = &syncPeerState{
			lastBlockTime: time.Now(),
		}
		sm.initialSelectionDone = false // Keep false so announcements can be buffered
		sm.mu.Unlock()

		// Create buffered announcements
		announcements := []*BlockAnnouncement{
			// Announcements from sync peer
			{
				Hash:       "sync-block-1",
				Height:     145,
				DataHubURL: "http://sync-peer.com",
				PeerID:     string(syncPeerID),
				From:       "from-sync-1",
				Timestamp:  time.Now(),
			},
			{
				Hash:       "sync-block-best",
				Height:     150, // Best block from sync peer
				DataHubURL: "http://sync-peer.com",
				PeerID:     string(syncPeerID),
				From:       "from-sync-2",
				Timestamp:  time.Now(),
			},
			{
				Hash:       "sync-block-2",
				Height:     148,
				DataHubURL: "http://sync-peer.com",
				PeerID:     string(syncPeerID),
				From:       "from-sync-3",
				Timestamp:  time.Now(),
			},
			// Announcements from other peers (should be discarded)
			{
				Hash:       "other-block-1",
				Height:     140,
				DataHubURL: "http://other-peer.com",
				PeerID:     "other-peer-1",
				From:       "from-other-1",
				Timestamp:  time.Now(),
			},
			{
				Hash:       "other-block-2",
				Height:     155, // Even higher than sync peer, but should be discarded
				DataHubURL: "http://other-peer-2.com",
				PeerID:     "other-peer-2",
				From:       "from-other-2",
				Timestamp:  time.Now(),
			},
		}

		// Buffer the announcements
		for _, ann := range announcements {
			sm.BufferBlockAnnouncement(ann)
		}

		// Now mark initial selection as done so the processing can begin
		sm.mu.Lock()
		sm.initialSelectionDone = true
		sm.mu.Unlock()

		// Set up mock expectations
		mockProducer.On("Publish", mock.Anything).Return().Once()
		mockProducer.On("BrokersURL").Return([]string{"localhost:9092"})
		mockProducer.On("Start", mock.Anything, mock.Anything).Return()
		mockProducer.On("Stop").Return(nil)
		mockProducer.On("Close").Return(nil)

		// Start the processBufferedAnnouncementsWhenReady goroutine
		done := make(chan bool)
		go func() {
			s.processBufferedAnnouncementsWhenReady(ctx)
			done <- true
		}()

		// Wait for processing to complete or timeout
		select {
		case <-done:
			// Processing completed
		case <-time.After(3 * time.Second):
			// Timeout - should have processed by now
		}

		// Verify only one message was published
		mockProducer.AssertNumberOfCalls(t, "Publish", 1)

		// Verify the published message is the sync peer's best block
		publishedMessages := mockProducer.GetPublishedMessages()
		require.Len(t, publishedMessages, 1, "Should publish exactly one message")

		// Unmarshal and verify the message
		var msg kafkamessage.KafkaBlockTopicMessage
		err := proto.Unmarshal(publishedMessages[0].Value, &msg)
		require.NoError(t, err)

		assert.Equal(t, "sync-block-best", msg.Hash, "Should publish sync peer's best block")
		assert.Equal(t, "http://sync-peer.com", msg.URL, "Should have correct URL")

		// Verify block peer map was updated
		entry, exists := s.blockPeerMap.Load("sync-block-best")
		assert.True(t, exists, "Block should be in blockPeerMap")
		if exists {
			peerEntry := entry.(peerMapEntry)
			assert.Equal(t, "from-sync-2", peerEntry.peerID)
		}
	})

	t.Run("processes_all_announcements_when_no_sync_peer", func(t *testing.T) {
		// Setup
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		logger := ulogger.New("test")
		s := &Server{
			logger:       logger,
			blockPeerMap: sync.Map{},
		}

		// Create sync manager
		sm := NewSyncManager(logger, tSettings)
		s.syncManager = sm

		// Create mock Kafka producer
		mockProducer := &TestKafkaAsyncProducer{}
		s.blocksKafkaProducerClient = mockProducer

		// Set up callbacks - we're caught up, no sync peer needed
		sm.SetLocalHeightCallback(func() uint32 { return 100 })
		sm.SetPeerHeightCallback(func(p peer.ID) int32 { return 100 })

		// Keep initial selection false for buffering
		sm.mu.Lock()
		sm.initialSelectionDone = false
		sm.mu.Unlock()

		// Create buffered announcements
		announcements := []*BlockAnnouncement{
			{
				Hash:       "block-1",
				Height:     100,
				DataHubURL: "http://peer1.com",
				PeerID:     "peer-1",
				From:       "from-1",
				Timestamp:  time.Now(),
			},
			{
				Hash:       "block-2",
				Height:     100,
				DataHubURL: "http://peer2.com",
				PeerID:     "peer-2",
				From:       "from-2",
				Timestamp:  time.Now(),
			},
			{
				Hash:       "block-3",
				Height:     100,
				DataHubURL: "http://peer3.com",
				PeerID:     "peer-3",
				From:       "from-3",
				Timestamp:  time.Now(),
			},
		}

		// Buffer the announcements
		for _, ann := range announcements {
			sm.BufferBlockAnnouncement(ann)
		}

		// Now mark initial selection as done so the processing can begin
		sm.mu.Lock()
		sm.initialSelectionDone = true
		sm.mu.Unlock()

		// Set up mock expectations
		mockProducer.On("Publish", mock.Anything).Return().Times(3)
		mockProducer.On("BrokersURL").Return([]string{"localhost:9092"})
		mockProducer.On("Close").Return(nil)

		// Start the processBufferedAnnouncementsWhenReady goroutine
		done := make(chan bool)
		go func() {
			s.processBufferedAnnouncementsWhenReady(ctx)
			done <- true
		}()

		// Wait for processing to complete or timeout
		select {
		case <-done:
			// Processing completed
		case <-time.After(3 * time.Second):
			// Timeout - should have processed by now
		}

		// Verify all messages were published
		mockProducer.AssertNumberOfCalls(t, "Publish", 3)

		publishedMessages := mockProducer.GetPublishedMessages()
		require.Len(t, publishedMessages, 3, "Should publish all 3 messages when no sync peer")

		// Verify all blocks are in the published messages
		publishedHashes := make(map[string]bool)
		for _, msg := range publishedMessages {
			var blockMsg kafkamessage.KafkaBlockTopicMessage
			err := proto.Unmarshal(msg.Value, &blockMsg)
			require.NoError(t, err)
			publishedHashes[blockMsg.Hash] = true
		}

		assert.True(t, publishedHashes["block-1"])
		assert.True(t, publishedHashes["block-2"])
		assert.True(t, publishedHashes["block-3"])
	})

	t.Run("handles_no_announcements_from_sync_peer", func(t *testing.T) {
		// Setup
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		logger := ulogger.New("test")
		s := &Server{
			logger:       logger,
			blockPeerMap: sync.Map{},
		}

		// Create sync manager
		sm := NewSyncManager(logger, tSettings)
		s.syncManager = sm

		// Create mock Kafka producer
		mockProducer := &TestKafkaAsyncProducer{}
		s.blocksKafkaProducerClient = mockProducer

		// Set up sync peer
		syncPeerID := peer.ID("sync-peer-456")
		sm.SetLocalHeightCallback(func() uint32 { return 100 })
		sm.SetPeerHeightCallback(func(p peer.ID) int32 {
			if p == syncPeerID {
				return 150
			}
			return 90
		})

		// Force sync peer selection
		sm.mu.Lock()
		sm.syncPeer = syncPeerID
		sm.syncPeerState = &syncPeerState{
			lastBlockTime: time.Now(),
		}
		sm.initialSelectionDone = false // Keep false for buffering
		sm.mu.Unlock()

		// Create buffered announcements from other peers only
		announcements := []*BlockAnnouncement{
			{
				Hash:       "other-block-1",
				Height:     140,
				DataHubURL: "http://other-peer.com",
				PeerID:     "other-peer-1",
				From:       "from-other-1",
				Timestamp:  time.Now(),
			},
			{
				Hash:       "other-block-2",
				Height:     145,
				DataHubURL: "http://other-peer-2.com",
				PeerID:     "other-peer-2",
				From:       "from-other-2",
				Timestamp:  time.Now(),
			},
		}

		// Buffer the announcements
		for _, ann := range announcements {
			sm.BufferBlockAnnouncement(ann)
		}

		// Now mark initial selection as done so the processing can begin
		sm.mu.Lock()
		sm.initialSelectionDone = true
		sm.mu.Unlock()

		// Set up mock expectations
		mockProducer.On("BrokersURL").Return([]string{"localhost:9092"})
		mockProducer.On("Start", mock.Anything, mock.Anything).Return()
		mockProducer.On("Stop").Return(nil)
		mockProducer.On("Close").Return(nil)

		// Start the processBufferedAnnouncementsWhenReady goroutine
		done := make(chan bool)
		go func() {
			s.processBufferedAnnouncementsWhenReady(ctx)
			done <- true
		}()

		// Wait for processing to complete or timeout
		select {
		case <-done:
			// Processing completed
		case <-time.After(3 * time.Second):
			// Timeout - should have processed by now
		}

		// Verify no messages were published
		mockProducer.AssertNotCalled(t, "Publish", mock.Anything)

		publishedMessages := mockProducer.GetPublishedMessages()
		assert.Len(t, publishedMessages, 0, "Should not publish any messages when no sync peer announcements")
	})

	t.Run("selects_highest_block_from_sync_peer", func(t *testing.T) {
		// Setup
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		logger := ulogger.New("test")
		s := &Server{
			logger:       logger,
			blockPeerMap: sync.Map{},
		}

		// Create sync manager
		sm := NewSyncManager(logger, tSettings)
		s.syncManager = sm

		// Create mock Kafka producer
		mockProducer := &TestKafkaAsyncProducer{}
		s.blocksKafkaProducerClient = mockProducer

		// Set up sync peer
		syncPeerID := peer.ID("sync-peer-789")
		sm.SetLocalHeightCallback(func() uint32 { return 100 })
		sm.SetPeerHeightCallback(func(p peer.ID) int32 {
			if p == syncPeerID {
				return 200
			}
			return 90
		})

		// Force sync peer selection
		sm.mu.Lock()
		sm.syncPeer = syncPeerID
		sm.syncPeerState = &syncPeerState{
			lastBlockTime: time.Now(),
		}
		sm.initialSelectionDone = false // Keep false for buffering
		sm.mu.Unlock()

		// Create multiple announcements from sync peer with different heights
		announcements := []*BlockAnnouncement{
			{
				Hash:       "sync-block-low",
				Height:     110,
				DataHubURL: "http://sync-peer.com",
				PeerID:     string(syncPeerID),
				From:       "from-sync-1",
				Timestamp:  time.Now(),
			},
			{
				Hash:       "sync-block-mid",
				Height:     150,
				DataHubURL: "http://sync-peer.com",
				PeerID:     string(syncPeerID),
				From:       "from-sync-2",
				Timestamp:  time.Now(),
			},
			{
				Hash:       "sync-block-highest",
				Height:     200, // Highest block
				DataHubURL: "http://sync-peer.com",
				PeerID:     string(syncPeerID),
				From:       "from-sync-3",
				Timestamp:  time.Now(),
			},
			{
				Hash:       "sync-block-another-mid",
				Height:     175,
				DataHubURL: "http://sync-peer.com",
				PeerID:     string(syncPeerID),
				From:       "from-sync-4",
				Timestamp:  time.Now(),
			},
		}

		// Buffer the announcements in random order
		for _, ann := range announcements {
			sm.BufferBlockAnnouncement(ann)
		}

		// Now mark initial selection as done so the processing can begin
		sm.mu.Lock()
		sm.initialSelectionDone = true
		sm.mu.Unlock()

		// Set up mock expectations
		mockProducer.On("Publish", mock.Anything).Return().Once()
		mockProducer.On("BrokersURL").Return([]string{"localhost:9092"})
		mockProducer.On("Start", mock.Anything, mock.Anything).Return()
		mockProducer.On("Stop").Return(nil)
		mockProducer.On("Close").Return(nil)

		// Start the processBufferedAnnouncementsWhenReady goroutine
		done := make(chan bool)
		go func() {
			s.processBufferedAnnouncementsWhenReady(ctx)
			done <- true
		}()

		// Wait for processing to complete or timeout
		select {
		case <-done:
			// Processing completed
		case <-time.After(3 * time.Second):
			// Timeout - should have processed by now
		}

		// Verify only one message was published
		mockProducer.AssertNumberOfCalls(t, "Publish", 1)

		// Verify the published message is the highest block
		publishedMessages := mockProducer.GetPublishedMessages()
		require.Len(t, publishedMessages, 1, "Should publish exactly one message")

		var msg kafkamessage.KafkaBlockTopicMessage
		err := proto.Unmarshal(publishedMessages[0].Value, &msg)
		require.NoError(t, err)

		assert.Equal(t, "sync-block-highest", msg.Hash, "Should publish the highest block from sync peer")
		assert.Equal(t, "http://sync-peer.com", msg.URL, "Should have correct URL")
	})
}

// TestProcessBufferedAnnouncementsRegression ensures the fix for sync peer announcements
// is not accidentally reverted in the future
func TestProcessBufferedAnnouncementsRegression(t *testing.T) {
	tSettings := createBaseTestSettings()
	tSettings.ChainCfgParams = &chaincfg.RegressionNetParams

	t.Run("regression_test_sync_peer_best_block_must_be_sent", func(t *testing.T) {
		// This test specifically guards against the regression where sync peer's
		// best block announcement was being discarded instead of sent to Kafka

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		logger := ulogger.New("test")
		s := &Server{
			logger:       logger,
			blockPeerMap: sync.Map{},
		}

		// Create sync manager
		sm := NewSyncManager(logger, tSettings)
		s.syncManager = sm

		// Create mock Kafka producer
		mockProducer := &TestKafkaAsyncProducer{}
		s.blocksKafkaProducerClient = mockProducer

		// Simulate the exact scenario from the bug report:
		// - Local node at height 100
		// - Sync peer at height 3072
		// - Sync peer's best block announcement must be sent to Kafka
		syncPeerID := peer.ID("12D3KooWNTx7RFySipLC6yWwWKmtpVxTV8r6NQWJUyQZQoQ99yu2")
		sm.SetLocalHeightCallback(func() uint32 { return 100 })
		sm.SetPeerHeightCallback(func(p peer.ID) int32 {
			if p == syncPeerID {
				return 3072
			}
			return 90
		})

		// Force sync peer selection
		sm.mu.Lock()
		sm.syncPeer = syncPeerID
		sm.syncPeerState = &syncPeerState{
			lastBlockTime: time.Now(),
		}
		sm.initialSelectionDone = false // Keep false for buffering
		sm.mu.Unlock()

		// Create the exact announcement from the bug report
		announcements := []*BlockAnnouncement{
			{
				Hash:       "00000000fc89f32ff1e810d3c07f38e314a4758b4443941a0d7d0bfacf906107",
				Height:     3072,
				DataHubURL: "http://sync-peer-datahub.com",
				PeerID:     string(syncPeerID),
				From:       "mining_on",
				Timestamp:  time.Now(),
			},
		}

		// Buffer the announcement
		for _, ann := range announcements {
			sm.BufferBlockAnnouncement(ann)
		}

		// Now mark initial selection as done so the processing can begin
		sm.mu.Lock()
		sm.initialSelectionDone = true
		sm.mu.Unlock()

		// Set up mock expectations
		// CRITICAL: The sync peer's announcement MUST be published
		// This is the regression test - if this fails, the bug has been reintroduced
		mockProducer.On("Publish", mock.Anything).Return().Once()
		mockProducer.On("BrokersURL").Return([]string{"localhost:9092"})
		mockProducer.On("Close").Return(nil)

		// Start the processBufferedAnnouncementsWhenReady goroutine
		done := make(chan bool)
		go func() {
			s.processBufferedAnnouncementsWhenReady(ctx)
			done <- true
		}()

		// Wait for processing to complete or timeout
		select {
		case <-done:
			// Processing completed
		case <-time.After(3 * time.Second):
			// Timeout - should have processed by now
		}

		// CRITICAL ASSERTION: Verify the message was published
		mockProducer.AssertNumberOfCalls(t, "Publish", 1)

		publishedMessages := mockProducer.GetPublishedMessages()
		require.Len(t, publishedMessages, 1, "REGRESSION: Sync peer's best block MUST be sent to Kafka!")

		// Verify it's the correct block
		var msg kafkamessage.KafkaBlockTopicMessage
		err := proto.Unmarshal(publishedMessages[0].Value, &msg)
		require.NoError(t, err)

		assert.Equal(t, "00000000fc89f32ff1e810d3c07f38e314a4758b4443941a0d7d0bfacf906107", msg.Hash,
			"REGRESSION: Must send sync peer's exact best block hash")

		// Log success to make it clear this critical test passed
		t.Logf("✓ Regression test passed: Sync peer's best block at height %d was correctly sent to Kafka", 3072)
	})
}
