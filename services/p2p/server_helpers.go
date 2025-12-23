package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"
)

func (s *Server) handleBlockTopic(_ context.Context, m []byte, fromID string) {
	var (
		blockMessage BlockMessage
		hash         *chainhash.Hash
		err          error
	)

	// decode request
	blockMessage = BlockMessage{}

	err = json.Unmarshal(m, &blockMessage)
	if err != nil {
		s.logger.Errorf("[handleBlockTopic] json unmarshal error: %v", err)
		return
	}

	// Check that fromID matches the block peer ID
	if fromID != blockMessage.PeerID {
		// For now, log an error. In the future, we might want to take banning action against peers spoofing other IDs
		s.logger.Errorf("[handleBlockTopic] mismatch between fromID (%s) and blockMessage.PeerID (%s)", fromID, blockMessage.PeerID)
		return
	}
	s.logger.Infof("[handleBlockTopic] received block %s fromID %s", blockMessage.Hash, blockMessage.PeerID)

	select {
	case s.notificationCh <- &notificationMsg{
		Timestamp:  time.Now().UTC().Format(isoFormat),
		Type:       "block",
		Hash:       blockMessage.Hash,
		Height:     blockMessage.Height,
		BaseURL:    blockMessage.DataHubURL,
		PeerID:     blockMessage.PeerID,
		ClientName: blockMessage.ClientName,
	}:
	default:
		s.logger.Warnf("[handleBlockTopic] notification channel full, dropped block notification for %s", blockMessage.Hash)
	}

	// Ignore our own messages
	if s.isOwnMessage(fromID, blockMessage.PeerID) {
		s.logger.Debugf("[handleBlockTopic] ignoring own block message for %s", blockMessage.Hash)
		return
	}

	now := time.Now().UTC()

	// Store the peer ID that sent this block
	s.storePeerMapEntry(&s.blockPeerMap, blockMessage.Hash, fromID, now)

	s.logger.Debugf("[handleBlockTopic] storing peer %s for block %s", fromID, blockMessage.Hash)

	// Store using the originator's peer ID
	if peerID, err := peer.Decode(blockMessage.PeerID); err == nil {
		s.addPeer(peerID, blockMessage.ClientName, blockMessage.Height, hash, blockMessage.DataHubURL)
		s.logger.Debugf("[handleBlockTopic] Stored latest block hash %s for peer %s", blockMessage.Hash, peerID)
	}

	// Update last message time for the sender and originator with client name
	s.updatePeerLastMessageTime(fromID, blockMessage.PeerID)

	// Track bytes received from this message
	s.updateBytesReceived(fromID, blockMessage.PeerID, uint64(len(m)))

	// Skip notifications from banned peers
	if s.shouldSkipBannedPeer(blockMessage.PeerID, "handleBlockTopic") {
		return
	}

	// Skip notifications from unhealthy peers
	if s.shouldSkipUnhealthyPeer(blockMessage.PeerID, "handleBlockTopic") {
		return
	}

	hash, err = s.parseHash(blockMessage.Hash, "handleBlockTopic")
	if err != nil {
		return
	}

	// Always send block to kafka - let block validation service decide what to do based on sync state
	// send block to kafka, if configured
	if s.blocksKafkaProducerClient != nil {
		msg := &kafkamessage.KafkaBlockTopicMessage{
			Hash:   hash.String(),
			URL:    blockMessage.DataHubURL,
			PeerId: blockMessage.PeerID,
		}

		s.logger.Debugf("[handleBlockTopic] Sending block %s to Kafka", hash.String())

		value, err := proto.Marshal(msg)
		if err != nil {
			s.logger.Errorf("[handleBlockTopic] error marshaling KafkaBlockTopicMessage: %v", err)
			return
		}

		s.blocksKafkaProducerClient.Publish(&kafka.Message{
			Key:   []byte(hash.String()),
			Value: value,
		})
	}
}

func (s *Server) handleSubtreeTopic(_ context.Context, m []byte, fromID string) {
	var (
		subtreeMessage SubtreeMessage
		hash           *chainhash.Hash
		err            error
	)

	// decode request
	subtreeMessage = SubtreeMessage{}

	err = json.Unmarshal(m, &subtreeMessage)
	if err != nil {
		s.logger.Errorf("[handleSubtreeTopic] json unmarshal error: %v", err)
		return
	}

	// Check that fromID matches the subtree peer ID
	if fromID != subtreeMessage.PeerID {
		// For now, log an error. In the future, we might want to take banning action against peers spoofing other IDs
		s.logger.Errorf("[handleSubtreeTopic] mismatch between fromID (%s) and subtreeMessage.PeerID (%s)", fromID, subtreeMessage.PeerID)
		return
	}
	s.logger.Debugf("[handleSubtreeTopic] received subtree %s from %s", subtreeMessage.Hash, subtreeMessage.PeerID)

	if s.isBlacklistedBaseURL(subtreeMessage.DataHubURL) {
		s.logger.Errorf("[handleSubtreeTopic] Blocked subtree notification from blacklisted baseURL: %s", subtreeMessage.DataHubURL)
		return
	}

	now := time.Now().UTC()

	select {
	case s.notificationCh <- &notificationMsg{
		Timestamp:  now.Format(isoFormat),
		Type:       "subtree",
		Hash:       subtreeMessage.Hash,
		BaseURL:    subtreeMessage.DataHubURL,
		PeerID:     subtreeMessage.PeerID,
		ClientName: subtreeMessage.ClientName,
	}:
	default:
		s.logger.Warnf("[handleSubtreeTopic] notification channel full, dropped subtree notification for %s", subtreeMessage.Hash)
	}

	// Ignore our own messages
	if s.isOwnMessage(fromID, subtreeMessage.PeerID) {
		s.logger.Debugf("[handleSubtreeTopic] ignoring own subtree message for %s", subtreeMessage.Hash)
		return
	}

	// Update last message time for the sender and originator with client name
	s.updatePeerLastMessageTime(fromID, subtreeMessage.PeerID)

	// Track bytes received from this message
	s.updateBytesReceived(fromID, subtreeMessage.PeerID, uint64(len(m)))

	// Skip notifications from banned peers
	if s.shouldSkipBannedPeer(fromID, "handleSubtreeTopic") {
		s.logger.Debugf("[handleSubtreeTopic] skipping banned peer %s", fromID)
		return
	}

	// Skip notifications from unhealthy peers
	if s.shouldSkipUnhealthyPeer(fromID, "handleSubtreeTopic") {
		return
	}

	hash, err = s.parseHash(subtreeMessage.Hash, "handleSubtreeTopic")
	if err != nil {
		s.logger.Errorf("[handleSubtreeTopic] error parsing hash: %v", err)
		return
	}

	// Store the peer ID that sent this subtree
	s.storePeerMapEntry(&s.subtreePeerMap, subtreeMessage.Hash, fromID, now)
	s.logger.Debugf("[handleSubtreeTopic] storing peer %s for subtree %s", fromID, subtreeMessage.Hash)

	if s.subtreeKafkaProducerClient != nil { // tests may not set this
		msg := &kafkamessage.KafkaSubtreeTopicMessage{
			Hash:   hash.String(),
			URL:    subtreeMessage.DataHubURL,
			PeerId: subtreeMessage.PeerID,
		}

		value, err := proto.Marshal(msg)
		if err != nil {
			s.logger.Errorf("[handleSubtreeTopic] error marshaling KafkaSubtreeTopicMessage: %v", err)
			return
		}

		s.subtreeKafkaProducerClient.Publish(&kafka.Message{
			Key:   []byte(hash.String()),
			Value: value,
		})
	}
}

// isBlacklistedBaseURL checks if the given baseURL matches any entry in the blacklist.
func (s *Server) isBlacklistedBaseURL(baseURL string) bool {
	inputHost := s.extractHost(baseURL)
	if inputHost == "" {
		// Fall back to exact string matching for invalid URLs
		for blocked := range s.settings.SubtreeValidation.BlacklistedBaseURLs {
			if baseURL == blocked {
				return true
			}
		}

		return false
	}

	// Check each blacklisted URL
	for blocked := range s.settings.SubtreeValidation.BlacklistedBaseURLs {
		blockedHost := s.extractHost(blocked)
		if blockedHost == "" {
			// Fall back to exact string matching for invalid blacklisted URLs
			if baseURL == blocked {
				return true
			}

			continue
		}

		if inputHost == blockedHost {
			return true
		}
	}

	return false
}

// extractHost extracts and normalizes the host component from a URL
func (s *Server) extractHost(urlStr string) string {
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return ""
	}

	host := parsedURL.Hostname()
	if host == "" {
		return ""
	}

	return strings.ToLower(host)
}

func (s *Server) handleRejectedTxTopic(_ context.Context, m []byte, fromID string) {
	var (
		rejectedTxMessage RejectedTxMessage
		err               error
	)

	rejectedTxMessage = RejectedTxMessage{}

	err = json.Unmarshal(m, &rejectedTxMessage)
	if err != nil {
		s.logger.Errorf("[handleRejectedTxTopic] json unmarshal error: %v", err)
		return
	}

	// Check that fromID matches the rejected tx peer ID
	if fromID != rejectedTxMessage.PeerID {
		s.logger.Errorf("[handleRejectedTxTopic] peerID does not match fromID: peerID=%s fromID=%s", rejectedTxMessage.PeerID, fromID)
		return
	}

	if s.isOwnMessage(fromID, rejectedTxMessage.PeerID) {
		s.logger.Debugf("[handleRejectedTxTopic] ignoring own rejected tx message for %s", rejectedTxMessage.TxID)
		return
	}
	s.logger.Debugf("[handleRejectedTxTopic] received rejected tx %s fromID %s (reason: %s)",
		rejectedTxMessage.TxID, rejectedTxMessage.PeerID, rejectedTxMessage.Reason)

	// Update last message time with client name
	s.updatePeerLastMessageTime(fromID, rejectedTxMessage.PeerID)

	// Track bytes received from this message
	s.updateBytesReceived(fromID, rejectedTxMessage.PeerID, uint64(len(m)))

	if s.shouldSkipBannedPeer(fromID, "handleRejectedTxTopic") {
		return
	}

	// Skip notifications from unhealthy peers
	if s.shouldSkipUnhealthyPeer(fromID, "handleRejectedTxTopic") {
		return
	}

	// Rejected TX messages from other peers are informational only.
	// They help us understand network state but don't trigger re-broadcasting.
	// If we wanted to take action (e.g., remove from our mempool), we would do it here.
}

// getPeerIDFromDataHubURL finds the peer ID that has the given DataHub URL
func (s *Server) getPeerIDFromDataHubURL(dataHubURL string) string {
	if s.peerRegistry == nil {
		return ""
	}

	peers := s.peerRegistry.GetAll()
	for _, peerInfo := range peers {
		if peerInfo.DataHubURL == dataHubURL {
			return peerInfo.ID.String()
		}
	}
	return ""
}

// contains checks if a slice of strings contains a specific string.
func contains(slice []string, item string) bool {
	for _, s := range slice {
		bootstrapAddr, err := ma.NewMultiaddr(s)
		if err != nil {
			continue
		}

		peerInfo, err := peer.AddrInfoFromP2pAddr(bootstrapAddr)
		if err != nil {
			continue
		}

		if peerInfo.ID.String() == item {
			return true
		}
	}

	return false
}

// startInvalidBlockConsumer initializes and starts the Kafka consumer for invalid blocks
func (s *Server) startInvalidBlockConsumer(ctx context.Context) error {
	var kafkaURL *url.URL

	var brokerURLs []string

	// Use InvalidBlocksConfig URL if available, otherwise construct one
	if s.settings.Kafka.InvalidBlocksConfig != nil {
		s.logger.Infof("Using InvalidBlocksConfig URL: %s", s.settings.Kafka.InvalidBlocksConfig.String())
		kafkaURL = s.settings.Kafka.InvalidBlocksConfig

		// For non-memory schemes, we need to extract broker URLs from the host
		if kafkaURL.Scheme != "memory" {
			brokerURLs = strings.Split(kafkaURL.Host, ",")
		}
	} else {
		// Fall back to the old way of constructing the URL
		host := s.settings.Kafka.Hosts

		s.logger.Infof("Starting invalid block consumer on topic: %s", s.settings.Kafka.InvalidBlocks)
		s.logger.Infof("Raw Kafka host from settings: %s", host)

		// Split the host string in case it contains multiple hosts
		hosts := strings.Split(host, ",")
		brokerURLs = make([]string, 0, len(hosts))

		// Process each host to ensure it has a port
		for _, h := range hosts {
			// Trim any whitespace
			h = strings.TrimSpace(h)

			// Skip empty hosts
			if h == "" {
				continue
			}

			// Check if the host string contains a port
			if !strings.Contains(h, ":") {
				// If no port is specified, use the default Kafka port from settings
				h = h + ":" + strconv.Itoa(s.settings.Kafka.Port)
				s.logger.Infof("Added default port to Kafka host: %s", h)
			}

			brokerURLs = append(brokerURLs, h)
		}

		if len(brokerURLs) == 0 {
			return errors.NewConfigurationError("no valid Kafka hosts found")
		}

		s.logger.Infof("Using Kafka brokers: %v", brokerURLs)

		// Create a valid URL for the Kafka consumer
		kafkaURLString := fmt.Sprintf("kafka://%s/%s?partitions=%d",
			brokerURLs[0], // Use the first broker for the URL
			s.settings.Kafka.InvalidBlocks,
			s.settings.Kafka.Partitions)

		s.logger.Infof("Kafka URL: %s", kafkaURLString)

		var err error

		kafkaURL, err = url.Parse(kafkaURLString)
		if err != nil {
			return errors.NewConfigurationError("invalid Kafka URL: %w", err)
		}
	}

	// Create the Kafka consumer config
	cfg := kafka.KafkaConsumerConfig{
		Logger:            s.logger,
		URL:               kafkaURL,
		BrokersURL:        brokerURLs,
		Topic:             s.settings.Kafka.InvalidBlocks,
		Partitions:        s.settings.Kafka.Partitions,
		ConsumerGroupID:   s.settings.Kafka.InvalidBlocks + "-consumer",
		AutoCommitEnabled: true,
		Replay:            false,
		// TLS/Auth configuration
		EnableTLS:          s.settings.Kafka.EnableTLS,
		TLSSkipVerify:      s.settings.Kafka.TLSSkipVerify,
		TLSCAFile:          s.settings.Kafka.TLSCAFile,
		TLSCertFile:        s.settings.Kafka.TLSCertFile,
		TLSKeyFile:         s.settings.Kafka.TLSKeyFile,
		EnableDebugLogging: s.settings.Kafka.EnableDebugLogging,
	}

	// Create the Kafka consumer group - this will handle the memory scheme correctly
	consumer, err := kafka.NewKafkaConsumerGroup(cfg)
	if err != nil {
		return errors.NewServiceError("failed to create Kafka consumer", err)
	}

	// Store the consumer for cleanup
	s.invalidBlocksKafkaConsumerClient = consumer

	// Start the consumer
	consumer.Start(ctx, s.processInvalidBlockMessage)

	return nil
}

// getLocalHeight returns the current local blockchain height.
func (s *Server) getLocalHeight() uint32 {
	if s.blockchainClient == nil {
		return 0
	}

	_, bhMeta, err := s.blockchainClient.GetBestBlockHeader(s.gCtx)
	if err != nil || bhMeta == nil {
		return 0
	}

	return bhMeta.Height
}

func (s *Server) addPeer(peerID peer.ID, clientName string, height uint32, blockHash *chainhash.Hash, dataHubURL string) {
	if s.peerRegistry != nil {
		s.peerRegistry.Put(peerID, clientName, height, blockHash, dataHubURL)
	}
}

// addConnectedPeer adds a peer and marks it as directly connected
func (s *Server) addConnectedPeer(peerID peer.ID, clientName string, height uint32, blockHash *chainhash.Hash, dataHubURL string) {
	if s.peerRegistry != nil {
		s.peerRegistry.Put(peerID, clientName, height, blockHash, dataHubURL)
		s.peerRegistry.UpdateConnectionState(peerID, true)
	}
}

// InjectPeerForTesting directly injects a peer into the registry for testing purposes.
// This method allows deterministic peer setup without requiring actual P2P network connections.
// After injecting, it triggers the sync coordinator to consider syncing from the new peer.
func (s *Server) InjectPeerForTesting(peerID peer.ID, clientName, dataHubURL string, height uint32, blockHash *chainhash.Hash) {
	if s.peerRegistry == nil {
		return
	}

	s.peerRegistry.Put(peerID, clientName, height, blockHash, dataHubURL)
	s.peerRegistry.UpdateStorage(peerID, "full")

	// Trigger sync coordinator to consider the new peer
	if s.syncCoordinator != nil {
		_ = s.syncCoordinator.TriggerSync()
	}
}

func (s *Server) removePeer(peerID peer.ID) {
	if s.peerRegistry != nil {
		// Mark as disconnected before removing
		s.peerRegistry.UpdateConnectionState(peerID, false)
		s.peerRegistry.Remove(peerID)
	}
	if s.syncCoordinator != nil {
		s.syncCoordinator.HandlePeerDisconnected(peerID)
	}
}

// getPeer gets peer information from the registry
func (s *Server) getPeer(peerID peer.ID) (*PeerInfo, bool) {
	if s.peerRegistry != nil {
		return s.peerRegistry.Get(peerID)
	}
	return nil, false
}

func (s *Server) getSyncPeer() peer.ID {
	if s.syncCoordinator != nil {
		return s.syncCoordinator.GetCurrentSyncPeer()
	}
	return ""
}

// updateStorage updates peer storage mode in the registry
func (s *Server) updateStorage(peerID peer.ID, mode string) {
	if s.peerRegistry != nil && mode != "" {
		s.peerRegistry.UpdateStorage(peerID, mode)
	}
}

func (s *Server) processInvalidBlockMessage(message *kafka.KafkaMessage) error {
	ctx := context.Background()

	var invalidBlockMsg kafkamessage.KafkaInvalidBlockTopicMessage
	if err := proto.Unmarshal(message.Value, &invalidBlockMsg); err != nil {
		s.logger.Errorf("failed to unmarshal invalid block message: %v", err)
		return err
	}

	blockHash := invalidBlockMsg.GetBlockHash()
	reason := invalidBlockMsg.GetReason()

	s.logger.Infof("[handleInvalidBlockMessage] processing invalid block %s: %s", blockHash, reason)

	// Look up the peer ID that sent this block
	peerID, err := s.getPeerFromMap(&s.blockPeerMap, blockHash, "block")
	if err != nil {
		s.logger.Warnf("[handleInvalidBlockMessage] %v", err)
		return nil // Not an error, just no peer to ban
	}

	// Add ban score to the peer
	s.logger.Infof("[handleInvalidBlockMessage] adding ban score to peer %s for invalid block %s: %s",
		peerID, blockHash, reason)

	req := &p2p_api.AddBanScoreRequest{
		PeerId: peerID,
		Reason: "invalid_block",
	}

	if _, err := s.AddBanScore(ctx, req); err != nil {
		s.logger.Errorf("[handleInvalidBlockMessage] error adding ban score to peer %s: %v", peerID, err)
		return err
	}

	// Remove the block from the map to avoid memory leaks
	s.blockPeerMap.Delete(blockHash)

	return nil
}

func (s *Server) isBlockchainSyncingOrCatchingUp(ctx context.Context) (bool, error) {
	if s.blockchainClient == nil {
		return false, nil
	}
	var (
		state *blockchain.FSMStateType
		err   error
	)

	// Retry for up to 15 seconds if we get an error getting FSM state
	// This handles the case where blockchain service isn't ready yet
	retryCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	retryCount := 0
	for {
		state, err = s.blockchainClient.GetFSMCurrentState(retryCtx)
		if err == nil {
			// Successfully got state
			if retryCount > 0 {
				s.logger.Infof("[isBlockchainSyncingOrCatchingUp] successfully got FSM state after %d retries", retryCount)
			}
			break
		}

		retryCount++

		// Check if context is done (timeout or cancellation)
		select {
		case <-retryCtx.Done():
			s.logger.Errorf("[isBlockchainSyncingOrCatchingUp] timeout after 15s getting blockchain FSM state (tried %d times): %v", retryCount, err)
			// On timeout, allow sync to proceed rather than blocking
			return false, nil
		case <-time.After(1 * time.Second):
			// Retry after short delay
			if retryCount == 1 || retryCount%10 == 0 {
				s.logger.Infof("[isBlockchainSyncingOrCatchingUp] retrying FSM state check (attempt %d) after error: %v", retryCount, err)
			}
		}
	}

	if *state == blockchain_api.FSMStateType_CATCHINGBLOCKS || *state == blockchain_api.FSMStateType_LEGACYSYNCING {
		// ignore notifications while syncing or catching up
		return true, nil
	}

	return false, nil
}

// cleanupPeerMaps performs periodic cleanup of blockPeerMap and subtreePeerMap
// It removes entries older than TTL and enforces size limits using LRU eviction
func (s *Server) cleanupPeerMaps() {
	now := time.Now()

	// Collect entries to delete
	var blockKeysToDelete []string
	var subtreeKeysToDelete []string
	blockCount := 0
	subtreeCount := 0

	// First pass: count entries and collect expired ones
	s.blockPeerMap.Range(func(key, value interface{}) bool {
		blockCount++
		if entry, ok := value.(peerMapEntry); ok {
			if now.Sub(entry.timestamp) > s.peerMapTTL {
				blockKeysToDelete = append(blockKeysToDelete, key.(string))
			}
		}
		return true
	})

	s.subtreePeerMap.Range(func(key, value interface{}) bool {
		subtreeCount++
		if entry, ok := value.(peerMapEntry); ok {
			if now.Sub(entry.timestamp) > s.peerMapTTL {
				subtreeKeysToDelete = append(subtreeKeysToDelete, key.(string))
			}
		}
		return true
	})

	// Delete expired entries
	for _, key := range blockKeysToDelete {
		s.blockPeerMap.Delete(key)
	}
	for _, key := range subtreeKeysToDelete {
		s.subtreePeerMap.Delete(key)
	}

	// Log cleanup stats
	if len(blockKeysToDelete) > 0 || len(subtreeKeysToDelete) > 0 {
		s.logger.Infof("[cleanupPeerMaps] removed %d expired block entries and %d expired subtree entries",
			len(blockKeysToDelete), len(subtreeKeysToDelete))
	}

	// Second pass: enforce size limits if needed
	remainingBlockCount := blockCount - len(blockKeysToDelete)
	remainingSubtreeCount := subtreeCount - len(subtreeKeysToDelete)

	if remainingBlockCount > s.peerMapMaxSize {
		s.enforceMapSizeLimit(&s.blockPeerMap, s.peerMapMaxSize, "block")
	}

	if remainingSubtreeCount > s.peerMapMaxSize {
		s.enforceMapSizeLimit(&s.subtreePeerMap, s.peerMapMaxSize, "subtree")
	}

	// Log current sizes
	s.logger.Infof("[cleanupPeerMaps] current map sizes - blocks: %d, subtrees: %d",
		remainingBlockCount, remainingSubtreeCount)
}

// enforceMapSizeLimit removes oldest entries from a map to enforce size limit
func (s *Server) enforceMapSizeLimit(m *sync.Map, maxSize int, mapType string) {
	type entryWithKey struct {
		key       string
		timestamp time.Time
	}

	var entries []entryWithKey

	// Collect all entries with their timestamps
	m.Range(func(key, value interface{}) bool {
		if entry, ok := value.(peerMapEntry); ok {
			entries = append(entries, entryWithKey{
				key:       key.(string),
				timestamp: entry.timestamp,
			})
		}
		return true
	})

	// Sort by timestamp (oldest first)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].timestamp.Before(entries[j].timestamp)
	})

	// Remove oldest entries to get under the limit
	toRemove := len(entries) - maxSize
	if toRemove > 0 {
		for i := 0; i < toRemove; i++ {
			m.Delete(entries[i].key)
		}
		s.logger.Warnf("[enforceMapSizeLimit] removed %d oldest %s entries to enforce size limit of %d",
			toRemove, mapType, maxSize)
	}
}

// startPeerMapCleanup starts the periodic cleanup goroutine
// Helper methods to reduce redundancy

// isOwnMessage checks if a message is from this node
func (s *Server) isOwnMessage(from string, peerID string) bool {
	return from == s.P2PClient.GetID() || peerID == s.P2PClient.GetID()
}

// shouldSkipBannedPeer checks if we should skip a message from a banned peer
func (s *Server) shouldSkipBannedPeer(from string, messageType string) bool {
	if s.banManager.IsBanned(from) {
		s.logger.Debugf("[%s] ignoring notification from banned peer %s", messageType, from)
		return true
	}
	return false
}

// shouldSkipUnhealthyPeer checks if we should skip a message from an unhealthy peer
// Only checks health for directly connected peers (not gossiped peers)
func (s *Server) shouldSkipUnhealthyPeer(from string, messageType string) bool {
	// If no peer registry, allow all messages
	if s.peerRegistry == nil {
		return false
	}

	peerID, err := peer.Decode(from)
	if err != nil {
		// If we can't decode the peer ID (e.g., from is a hostname/identifier in gossiped messages),
		// we can't check health status, so allow the message through.
		// This is normal for gossiped messages where 'from' is the relay peer's identifier, not a valid peer ID.
		return false
	}

	peerInfo, exists := s.peerRegistry.Get(peerID)
	if !exists {
		// Peer not in registry - allow message (peer might be new)
		return false
	}

	// Filter peers with very low reputation scores
	if peerInfo.ReputationScore < 20.0 {
		s.logger.Debugf("[%s] ignoring notification from low reputation peer %s (score: %.2f)", messageType, from, peerInfo.ReputationScore)
		return true
	}

	return false
}

// storePeerMapEntry stores a peer entry in the specified map
func (s *Server) storePeerMapEntry(peerMap *sync.Map, hash string, from string, timestamp time.Time) {
	entry := peerMapEntry{
		peerID:    from,
		timestamp: timestamp,
	}
	peerMap.Store(hash, entry)
}

// getPeerFromMap retrieves and validates a peer entry from a map
func (s *Server) getPeerFromMap(peerMap *sync.Map, hash string, mapType string) (string, error) {
	peerIDVal, ok := peerMap.Load(hash)
	if !ok {
		s.logger.Warnf("[getPeerFromMap] no peer found for %s %s", mapType, hash)
		return "", errors.NewNotFoundError("no peer found for %s %s", mapType, hash)
	}

	entry, ok := peerIDVal.(peerMapEntry)
	if !ok {
		s.logger.Errorf("[getPeerFromMap] peer entry for %s %s is not a peerMapEntry: %v", mapType, hash, peerIDVal)
		return "", errors.NewInvalidArgumentError("peer entry for %s %s is not a peerMapEntry", mapType, hash)
	}
	return entry.peerID, nil
}

// parseHash converts a string hash to chainhash
func (s *Server) parseHash(hashStr string, context string) (*chainhash.Hash, error) {
	hash, err := chainhash.NewHashFromStr(hashStr)
	if err != nil {
		s.logger.Errorf("[%s] error getting chainhash from string %s: %v", context, hashStr, err)
		return nil, err
	}

	return hash, nil
}

// shouldSkipDuringSync checks if we should skip processing during sync
func (s *Server) shouldSkipDuringSync(from string, originatorPeerID string, messageHeight uint32, messageType string) bool {
	syncPeer := s.getSyncPeer()
	if syncPeer == "" {
		return false
	}

	syncing, err := s.isBlockchainSyncingOrCatchingUp(s.gCtx)
	if err != nil || !syncing {
		return false
	}

	// Get sync peer's height from registry
	syncPeerHeight := int32(0)
	if peerInfo, exists := s.getPeer(syncPeer); exists {
		syncPeerHeight = int32(peerInfo.Height)
	}

	// Discard announcements from peers that are behind our sync peer
	if messageHeight < uint32(syncPeerHeight) {
		s.logger.Debugf("[%s] Discarding announcement at height %d from %s (below sync peer height %d)",
			messageType, messageHeight, from, syncPeerHeight)
		return true
	}

	// Skip if it's not from our sync peer
	peerID, err := peer.Decode(originatorPeerID)
	if err != nil || peerID != syncPeer {
		s.logger.Debugf("[%s] Skipping announcement during sync (not from sync peer)", messageType)
		return true
	}

	return false
}

func (s *Server) startPeerMapCleanup(ctx context.Context) {
	// Use configured interval or default
	cleanupInterval := defaultPeerMapCleanupInterval
	if s.settings.P2P.PeerMapCleanupInterval > 0 {
		cleanupInterval = s.settings.P2P.PeerMapCleanupInterval
	}

	s.peerMapCleanupTicker = time.NewTicker(cleanupInterval)

	go func() {
		for {
			select {
			case <-ctx.Done():
				s.logger.Infof("[startPeerMapCleanup] stopping peer map cleanup")
				return
			case <-s.peerMapCleanupTicker.C:
				s.cleanupPeerMaps()
			}
		}
	}()

	s.logger.Infof("[startPeerMapCleanup] started peer map cleanup with interval %v", cleanupInterval)
}

// startPeerRegistryCacheSave starts periodic saving of peer registry cache
func (s *Server) startPeerRegistryCacheSave(ctx context.Context) {
	// Save every 5 minutes
	saveInterval := 5 * time.Minute

	s.registryCacheSaveTicker = time.NewTicker(saveInterval)

	go func() {
		for {
			select {
			case <-ctx.Done():
				// Save one final time before shutdown
				if s.peerRegistry != nil {
					if err := s.peerRegistry.SavePeerRegistryCache(s.settings.P2P.PeerCacheDir); err != nil {
						s.logger.Errorf("[startPeerRegistryCacheSave] failed to save peer registry cache on shutdown: %v", err)
					} else {
						s.logger.Infof("[startPeerRegistryCacheSave] saved peer registry cache on shutdown")
					}
				}
				s.logger.Infof("[startPeerRegistryCacheSave] stopping peer registry cache save")
				return
			case <-s.registryCacheSaveTicker.C:
				if s.peerRegistry != nil {
					if err := s.peerRegistry.SavePeerRegistryCache(s.settings.P2P.PeerCacheDir); err != nil {
						s.logger.Errorf("[startPeerRegistryCacheSave] failed to save peer registry cache: %v", err)
					} else {
						peerCount := s.peerRegistry.PeerCount()
						s.logger.Debugf("[startPeerRegistryCacheSave] saved peer registry cache with %d peers", peerCount)
					}
				}
			}
		}
	}()

	s.logger.Infof("[startPeerRegistryCacheSave] started peer registry cache save with interval %v", saveInterval)
}

func (s *Server) listenForBanEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-s.banChan:
			s.handleBanEvent(ctx, event)
		}
	}
}

func (s *Server) handleBanEvent(ctx context.Context, event BanEvent) {
	if event.Action != banActionAdd {
		return // we only care about new bans
	}

	// Only handle PeerID-based banning
	if event.PeerID == "" {
		s.logger.Warnf("[handleBanEvent] Ban event received without PeerID, ignoring (PeerID-only banning enabled)")
		return
	}

	s.logger.Infof("[handleBanEvent] Received ban event for PeerID: %s (reason: %s)", event.PeerID, event.Reason)

	// Parse the PeerID
	peerID, err := peer.Decode(event.PeerID)
	if err != nil {
		s.logger.Errorf("[handleBanEvent] Invalid PeerID in ban event: %s, error: %v", event.PeerID, err)
		return
	}

	// Disconnect by PeerID
	s.disconnectBannedPeerByID(ctx, peerID, event.Reason)
}

// disconnectBannedPeerByID disconnects a specific peer by their PeerID
func (s *Server) disconnectBannedPeerByID(_ context.Context, peerID peer.ID, reason string) {
	// Check if we're connected to this peer
	peers := s.P2PClient.GetPeers()

	for _, p := range peers {
		if p.ID == peerID.String() {
			s.logger.Infof("[disconnectBannedPeerByID] Disconnecting banned peer: %s (reason: %s)", peerID, reason)

			// Remove peer from SyncCoordinator before disconnecting
			// Remove peer from registry
			s.removePeer(peerID)

			return
		}
	}

	s.logger.Debugf("[disconnectBannedPeerByID] Peer %s not found in connected peers", peerID)
}
