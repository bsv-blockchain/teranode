package p2p

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
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

const reputationCacheTTL = 5 * time.Second

type reputationCacheEntry struct {
	score     float64
	expiresAt time.Time
}

func (s *Server) handleBlockTopic(_ context.Context, m []byte, fromID string) {
	var (
		blockMessage BlockMessage
		hash         *chainhash.Hash
		err          error
	)

	// Check message size before parsing to prevent memory exhaustion
	if len(m) > maxBlockMessageSize {
		s.logger.Errorf("[handleBlockTopic] message size %d exceeds max %d from peer %s", len(m), maxBlockMessageSize, fromID)
		return
	}

	// decode request
	blockMessage = BlockMessage{}

	if err = json.Unmarshal(m, &blockMessage); err != nil {
		s.logger.Errorf("[handleBlockTopic] json unmarshal error: %v", err)
		return
	}

	// Check that fromID matches the block peer ID
	if fromID != blockMessage.PeerID {
		s.logger.Errorf("[handleBlockTopic] peer ID spoofing detected: from=%s claimed=%s", fromID, blockMessage.PeerID)
		s.addProtocolViolation(fromID)
		return
	}

	// Drop messages from banned peers before any registration, WebSocket
	// forwarding, or further processing. Own messages skip the registry
	// round-trip; they return at the isOwnMessage check below.
	if !s.isOwnMessage(fromID, blockMessage.PeerID) && s.shouldSkipBannedPeer(fromID, "handleBlockTopic") {
		return
	}

	// Validate DataHubURL to prevent SSRF attacks
	if err = s.validateDataHubURL(blockMessage.DataHubURL); err != nil {
		s.logger.Errorf("[handleBlockTopic] invalid DataHubURL from peer %s: %v", fromID, err)
		s.addProtocolViolation(fromID)
		return
	}

	s.logger.Infof("[handleBlockTopic] received block %s fromID %s", blockMessage.Hash, blockMessage.PeerID)

	isSelf := s.isOwnMessage(fromID, blockMessage.PeerID)
	advertisedHeight := blockMessage.Height
	if isSelf {
		hash, err = s.parseHash(blockMessage.Hash, "handleBlockTopic")
		if err != nil {
			return
		}
	} else {
		var ok bool
		advertisedHeight, hash, ok = s.sanitizeAdvertisedTip(blockMessage.PeerID, blockMessage.Height, blockMessage.Hash, s.getLocalHeight())
		if !ok {
			return
		}
	}

	select {
	case s.notificationCh <- &notificationMsg{
		Timestamp:  time.Now().UTC().Format(isoFormat),
		Type:       "block",
		Hash:       hash.String(),
		Height:     advertisedHeight,
		BaseURL:    blockMessage.DataHubURL,
		PeerID:     blockMessage.PeerID,
		ClientName: blockMessage.ClientName,
	}:
	default:
		s.logger.Warnf("[handleBlockTopic] notification channel full, dropped block notification for %s", blockMessage.Hash)
	}

	// Ignore our own messages
	if isSelf {
		s.logger.Debugf("[handleBlockTopic] ignoring own block message for %s", blockMessage.Hash)
		return
	}

	now := time.Now().UTC()

	// Store the peer ID that sent this block
	s.storePeerMapEntry(&s.blockPeerMap, blockMessage.Hash, fromID, now)

	s.logger.Debugf("[handleBlockTopic] storing peer %s for block %s", fromID, blockMessage.Hash)

	// Store using the originator's peer ID
	if peerID, err := peer.Decode(blockMessage.PeerID); err == nil {
		s.addPeer(peerID, blockMessage.ClientName, advertisedHeight, hash, blockMessage.DataHubURL)
		s.logger.Debugf("[handleBlockTopic] Stored latest block hash %s for peer %s", blockMessage.Hash, peerID)
	}

	// Update last message time for the sender and originator with client name
	s.updatePeerLastMessageTime(fromID, blockMessage.PeerID)

	// Track bytes received from this message
	s.updateBytesReceived(fromID, blockMessage.PeerID, uint64(len(m)))

	// Note: we intentionally do NOT filter blocks from unhealthy peers here,
	// unlike handleSubtreeTopic. Block announcements are what *trigger* catchup,
	// so dropping them from low-reputation peers stops a node that is behind from
	// ever starting catchup when its only available peers have low reputation.
	// Block validation handles bad blocks safely, and catchup fetches blocks and
	// their subtrees directly over HTTP (not via the gossip subtree handler), so
	// the reputation filter retained in handleSubtreeTopic does not affect catchup.

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

	// Check message size before parsing to prevent memory exhaustion
	if len(m) > maxSubtreeMessageSize {
		s.logger.Errorf("[handleSubtreeTopic] message size %d exceeds max %d from peer %s", len(m), maxSubtreeMessageSize, fromID)
		return
	}

	// decode request
	subtreeMessage = SubtreeMessage{}

	if err = json.Unmarshal(m, &subtreeMessage); err != nil {
		s.logger.Errorf("[handleSubtreeTopic] json unmarshal error: %v", err)
		return
	}

	// Check that fromID matches the subtree peer ID
	if fromID != subtreeMessage.PeerID {
		s.logger.Errorf("[handleSubtreeTopic] peer ID spoofing detected: from=%s claimed=%s", fromID, subtreeMessage.PeerID)
		s.addProtocolViolation(fromID)
		return
	}

	// Drop messages from banned peers before any registration, WebSocket
	// forwarding, or further processing. Own messages skip the registry
	// round-trip; they return at the isOwnMessage check below.
	if !s.isOwnMessage(fromID, subtreeMessage.PeerID) && s.shouldSkipBannedPeer(fromID, "handleSubtreeTopic") {
		return
	}

	// Validate DataHubURL to prevent SSRF attacks
	if err = s.validateDataHubURL(subtreeMessage.DataHubURL); err != nil {
		s.logger.Errorf("[handleSubtreeTopic] invalid DataHubURL from peer %s: %v", fromID, err)
		s.addProtocolViolation(fromID)
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

// addProtocolViolation records a protocol violation against a peer.
func (s *Server) addProtocolViolation(peerID string) {
	s.applyBanScore(peerID, ReasonProtocolViolation)
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

// isUnsafeIP checks if an IP address points to an internal/unsafe network.
// Returns a non-empty reason string if unsafe, empty string if safe.
func isUnsafeIP(ip net.IP) string {
	switch {
	case ip.IsLoopback():
		return "loopback address"
	case ip.IsPrivate():
		return "private address"
	case ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast():
		return "link-local address"
	case ip.IsUnspecified():
		return "unspecified address"
	default:
		return ""
	}
}

// isLocalhostHostname checks if a hostname refers to localhost.
func isLocalhostHostname(hostname string) bool {
	return hostname == "localhost" || strings.HasSuffix(hostname, ".localhost")
}

// validateDataHubURL validates that a DataHubURL is safe to use (not pointing to internal resources).
// This prevents SSRF attacks by rejecting URLs with:
// - Invalid schemes (only http/https allowed)
// - Loopback addresses (127.x.x.x, ::1)
// - Private network addresses (10.x.x.x, 172.16-31.x.x, 192.168.x.x, fc00::/7)
// - Link-local addresses (169.254.x.x, fe80::/10)
func (s *Server) validateDataHubURL(urlStr string) error {
	if urlStr == "" {
		return errors.NewInvalidArgumentError("DataHubURL is empty")
	}

	parsed, err := url.Parse(urlStr)
	if err != nil {
		return errors.NewInvalidArgumentError("invalid DataHubURL: %v", err)
	}

	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.NewInvalidArgumentError("DataHubURL has invalid scheme: %s (only http/https allowed)", parsed.Scheme)
	}

	hostname := parsed.Hostname()
	if hostname == "" {
		return errors.NewInvalidArgumentError("DataHubURL has no hostname")
	}

	// Skip SSRF checks when private/localhost IPs are allowed (e.g. local dev with host networking)
	if s != nil && s.settings != nil && s.settings.P2P.AllowPrivateIPs {
		return nil
	}

	if ip := net.ParseIP(hostname); ip != nil {
		if reason := isUnsafeIP(ip); reason != "" {
			return errors.NewInvalidArgumentError("DataHubURL points to %s", reason)
		}
	} else if isLocalhostHostname(hostname) {
		return errors.NewInvalidArgumentError("DataHubURL points to localhost")
	}

	return nil
}

func (s *Server) handleRejectedTxTopic(_ context.Context, m []byte, fromID string) {
	var (
		rejectedTxMessage RejectedTxMessage
		err               error
	)

	// Check message size before parsing to prevent memory exhaustion
	if len(m) > maxRejectedTxMessageSize {
		s.logger.Errorf("[handleRejectedTxTopic] message size %d exceeds max %d from peer %s", len(m), maxRejectedTxMessageSize, fromID)
		return
	}

	rejectedTxMessage = RejectedTxMessage{}

	err = json.Unmarshal(m, &rejectedTxMessage)
	if err != nil {
		s.logger.Errorf("[handleRejectedTxTopic] json unmarshal error: %v", err)
		return
	}

	// Check that fromID matches the rejected tx peer ID
	if fromID != rejectedTxMessage.PeerID {
		s.logger.Errorf("[handleRejectedTxTopic] peer ID spoofing detected: from=%s claimed=%s", fromID, rejectedTxMessage.PeerID)
		s.addProtocolViolation(fromID)
		return
	}

	if s.isOwnMessage(fromID, rejectedTxMessage.PeerID) {
		s.logger.Debugf("[handleRejectedTxTopic] ignoring own rejected tx message for %s", rejectedTxMessage.TxID)
		return
	}

	// Drop messages from banned peers before any registration or further processing.
	if s.shouldSkipBannedPeer(fromID, "handleRejectedTxTopic") {
		return
	}
	s.logger.Debugf("[handleRejectedTxTopic] received rejected tx %s fromID %s (reason: %s)",
		rejectedTxMessage.TxID, rejectedTxMessage.PeerID, rejectedTxMessage.Reason)

	// Update last message time with client name
	s.updatePeerLastMessageTime(fromID, rejectedTxMessage.PeerID)

	// Track bytes received from this message
	s.updateBytesReceived(fromID, rejectedTxMessage.PeerID, uint64(len(m)))

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

	peers, err := s.peerRegistry.ListPeers(s.gCtx, nil, 0, 0, false, false)
	if err != nil {
		s.logger.Warnf("[getPeerIDFromDataHubURL] ListPeers failed: %v", err)
		return ""
	}
	for _, peerInfo := range peers {
		if peerInfo.DataHubURL == dataHubURL {
			return peerInfo.ID
		}
	}
	return ""
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

func (s *Server) sanitizeAdvertisedTip(peerID string, advertisedHeight uint32, advertisedHash string, localHeight uint32) (uint32, *chainhash.Hash, bool) {
	hash, err := chainhash.NewHashFromStr(advertisedHash)
	if err != nil {
		s.logger.Warnf("[sanitizeAdvertisedTip] rejecting advertised tip from peer %s: invalid block hash %q: %v", peerID, advertisedHash, err)
		return 0, nil, false
	}

	maxLead := uint32(0)
	if s.settings != nil {
		maxLead = s.settings.P2P.MaxUnvalidatedAdvertisedHeightLead
	}

	maxAcceptedHeight := uint64(localHeight) + uint64(maxLead)
	if maxAcceptedHeight > uint64(^uint32(0)) {
		maxAcceptedHeight = uint64(^uint32(0))
	}

	if uint64(advertisedHeight) > maxAcceptedHeight {
		cappedHeight := uint32(maxAcceptedHeight)
		s.logger.Warnf("[sanitizeAdvertisedTip] capped advertised height for peer %s from %d to %d (local height %d, max lead %d)", peerID, advertisedHeight, cappedHeight, localHeight, maxLead)
		return cappedHeight, hash, true
	}

	return advertisedHeight, hash, true
}

// registerPeer is a fire-and-forget shorthand for the centralized registry's
// RegisterPeer RPC, building the PeerInfo struct from libp2p-source data.
// Used by addPeer / addConnectedPeer / InjectPeerForTesting which previously
// shared a single Put helper on the local registry.
func (s *Server) registerPeer(peerID peer.ID, clientName string, height uint32, blockHash *chainhash.Hash, dataHubURL string) {
	if s.peerRegistry == nil {
		return
	}
	info := &blockchain.PeerInfo{
		ID:               peerID.String(),
		TransportType:    blockchain_api.TransportType_TRANSPORT_HTTP,
		TransportTypeSet: true,
		ClientName:       clientName,
		Height:           height,
		BlockHash:        blockHash,
		DataHubURL:       dataHubURL,
	}
	if err := s.peerRegistry.RegisterPeer(s.gCtx, info); err != nil {
		s.logger.Warnf("[registerPeer] RegisterPeer %s failed: %v", info.ID, err)
	}
}

func (s *Server) addPeer(peerID peer.ID, clientName string, height uint32, blockHash *chainhash.Hash, dataHubURL string) {
	s.registerPeer(peerID, clientName, height, blockHash, dataHubURL)
}

// addConnectedPeer adds a peer and marks it as directly connected
func (s *Server) addConnectedPeer(peerID peer.ID, clientName string, height uint32, blockHash *chainhash.Hash, dataHubURL string) {
	s.registerPeer(peerID, clientName, height, blockHash, dataHubURL)
	if s.peerRegistry == nil {
		return
	}
	if err := s.peerRegistry.UpdateConnectionState(s.gCtx, peerID.String(), true); err != nil {
		s.logger.Warnf("[addConnectedPeer] UpdateConnectionState %s failed: %v", peerID, err)
	}
}

// InjectPeerForTesting directly injects a peer into the registry for testing purposes.
// This method allows deterministic peer setup without requiring actual P2P network connections.
// After injecting, it triggers the sync coordinator to consider syncing from the new peer.
func (s *Server) InjectPeerForTesting(peerID peer.ID, clientName, dataHubURL string, height uint32, blockHash *chainhash.Hash) {
	if s.peerRegistry == nil {
		return
	}

	s.registerPeer(peerID, clientName, height, blockHash, dataHubURL)
	if err := s.peerRegistry.UpdateStorage(s.gCtx, peerID.String(), "full"); err != nil {
		s.logger.Warnf("[InjectPeerForTesting] UpdateStorage %s failed: %v", peerID, err)
	}

	// Trigger sync coordinator to consider the new peer
	if s.syncCoordinator != nil {
		_ = s.syncCoordinator.TriggerSync()
	}
}

func (s *Server) removePeer(peerID peer.ID) {
	if s.peerRegistry != nil {
		idStr := peerID.String()
		if err := s.peerRegistry.UpdateConnectionState(s.gCtx, idStr, false); err != nil {
			s.logger.Warnf("[removePeer] UpdateConnectionState %s failed: %v", peerID, err)
		}
		if err := s.peerRegistry.RemovePeer(s.gCtx, idStr); err != nil {
			s.logger.Warnf("[removePeer] RemovePeer %s failed: %v", peerID, err)
		}
	}
	if s.syncCoordinator != nil {
		s.syncCoordinator.HandlePeerDisconnected(peerID)
	}
}

// getPeer gets peer information from the centralized registry. Returns nil and
// false on error or when the peer is unknown.
func (s *Server) getPeer(peerID peer.ID) (*blockchain.PeerInfo, bool) {
	if s.peerRegistry == nil {
		return nil, false
	}
	info, found, err := s.peerRegistry.GetPeer(s.gCtx, peerID.String())
	if err != nil {
		s.logger.Warnf("[getPeer] GetPeer %s failed: %v", peerID, err)
		return nil, false
	}
	return info, found
}

// getSyncPeer returns the current sync peer as a libp2p peer.ID. Empty when no
// sync peer is selected or the configured ID can't be decoded.
func (s *Server) getSyncPeer() peer.ID {
	if s.syncCoordinator == nil {
		return ""
	}
	idStr := s.syncCoordinator.GetCurrentSyncPeer()
	if idStr == "" {
		return ""
	}
	pid, err := peer.Decode(idStr)
	if err != nil {
		return ""
	}
	return pid
}

// updateStorage updates peer storage mode in the centralized registry.
func (s *Server) updateStorage(peerID peer.ID, mode string) {
	if s.peerRegistry != nil && mode != "" {
		if err := s.peerRegistry.UpdateStorage(s.gCtx, peerID.String(), mode); err != nil {
			s.logger.Warnf("[updateStorage] UpdateStorage %s failed: %v", peerID, err)
		}
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

	if *state == blockchain_api.FSMStateType_CATCHINGBLOCKS {
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

	// Evict expired reputationCache entries. shouldSkipUnhealthyPeer only ever
	// inserts; without this sweep the map would grow once per unique peer ID
	// the node has ever processed gossip from.
	var reputationKeysToDelete []string
	s.reputationCache.Range(func(key, value interface{}) bool {
		if entry, ok := value.(reputationCacheEntry); ok {
			if now.After(entry.expiresAt) {
				reputationKeysToDelete = append(reputationKeysToDelete, key.(string))
			}
		}
		return true
	})
	for _, key := range reputationKeysToDelete {
		s.reputationCache.Delete(key)
	}

	// Evict expired ipBanCache entries for the same reason: isPeerIPBanned
	// only ever inserts, one entry per unique peer ID gossip is seen from.
	var ipBanKeysToDelete []string
	s.ipBanCache.Range(func(key, value interface{}) bool {
		if entry, ok := value.(ipBanCacheEntry); ok {
			if now.After(entry.expiresAt) {
				ipBanKeysToDelete = append(ipBanKeysToDelete, key.(string))
			}
		}
		return true
	})
	for _, key := range ipBanKeysToDelete {
		s.ipBanCache.Delete(key)
	}

	// Log cleanup stats
	if len(blockKeysToDelete) > 0 || len(subtreeKeysToDelete) > 0 || len(reputationKeysToDelete) > 0 || len(ipBanKeysToDelete) > 0 {
		s.logger.Infof("[cleanupPeerMaps] removed %d expired block entries, %d expired subtree entries, %d expired reputation entries, %d expired IP-ban entries",
			len(blockKeysToDelete), len(subtreeKeysToDelete), len(reputationKeysToDelete), len(ipBanKeysToDelete))
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

// shouldSkipBannedPeer checks if we should skip a message from a banned peer:
// score-based bans live in the centralized peer registry, operator IP/subnet
// bans in the local ban list. Registry failures are tolerated (return false)
// so a transient registry blip doesn't drop traffic silently.
func (s *Server) shouldSkipBannedPeer(from string, messageType string) bool {
	if s.peerRegistry != nil {
		banned, err := s.peerRegistry.IsPeerBanned(s.gCtx, from)
		if err != nil {
			s.logger.Warnf("[%s] IsPeerBanned %s failed: %v", messageType, from, err)
		} else if banned {
			s.logger.Debugf("[%s] ignoring notification from banned peer %s", messageType, from)
			return true
		}
	}

	if s.isPeerIPBanned(from) {
		s.logger.Debugf("[%s] ignoring notification from IP-banned peer %s", messageType, from)
		return true
	}

	return false
}

type ipBanCacheEntry struct {
	banned    bool
	expiresAt time.Time
}

// isPeerIPBanned reports whether any of the peer's connected addresses match
// an entry in the IP/subnet ban list. This is what keeps operator IP bans
// effective for gossip: the current P2P client cannot sever the libp2p
// connection, so a banned peer stays connected and must be filtered here.
// Results are cached briefly to avoid a GetPeers scan per gossip message.
func (s *Server) isPeerIPBanned(peerID string) bool {
	if s.banList == nil || s.P2PClient == nil {
		return false
	}

	now := time.Now()
	if v, ok := s.ipBanCache.Load(peerID); ok {
		entry := v.(ipBanCacheEntry)
		if now.Before(entry.expiresAt) {
			return entry.banned
		}
	}

	banned := false
	for _, p := range s.P2PClient.GetPeers() {
		if p.ID != peerID {
			continue
		}
		for _, addr := range p.Addrs {
			if ip := extractIPFromMultiaddr(addr); ip != "" && s.banList.IsBanned(ip) {
				banned = true
				break
			}
		}
		break
	}

	s.ipBanCache.Store(peerID, ipBanCacheEntry{banned: banned, expiresAt: now.Add(reputationCacheTTL)})

	return banned
}

// shouldSkipUnhealthyPeer checks if we should skip a message from an unhealthy
// peer. Reputation scores are cached for reputationCacheTTL to avoid a gRPC
// round-trip on every pubsub message. Only applies to directly connected peers
// whose ID can be decoded as a libp2p peer.ID; gossiped relay IDs are allowed
// through unconditionally.
func (s *Server) shouldSkipUnhealthyPeer(from string, messageType string) bool {
	if s.peerRegistry == nil {
		return false
	}

	peerID, err := peer.Decode(from)
	if err != nil {
		return false
	}
	idStr := peerID.String()

	// Check cache first.
	now := time.Now()
	if v, ok := s.reputationCache.Load(idStr); ok {
		entry := v.(reputationCacheEntry)
		if now.Before(entry.expiresAt) {
			if entry.score < 20.0 {
				s.logger.Debugf("[%s] ignoring notification from low reputation peer %s (cached score: %.2f)", messageType, from, entry.score)
				return true
			}
			return false
		}
	}

	// Cache miss or expired — fetch from registry.
	peerInfo, exists, err := s.peerRegistry.GetPeer(s.gCtx, idStr)
	if err != nil {
		s.logger.Warnf("[shouldSkipUnhealthyPeer] GetPeer %s failed: %v", peerID, err)
		return false
	}
	if !exists {
		return false
	}

	s.reputationCache.Store(idStr, reputationCacheEntry{
		score:     peerInfo.ReputationScore,
		expiresAt: now.Add(reputationCacheTTL),
	})

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

// startPeerRegistryCleanup and startPeerRegistryCacheSave have been removed.
// TTL/LRU eviction and persistence both belong to the centralized peer registry
// in the blockchain service now (the periodic cleanup-driver goroutine over there
// is intentionally deferred to a follow-up PR; the Cleanup method itself ships in PR1).

func (s *Server) listenForBanEvents(ctx context.Context) {
	// Without a libp2p connection gater we cannot refuse dials from banned
	// addresses, so periodically re-check connected peers against the ban list
	// to catch peers that reconnected after the initial ban event.
	sweep := time.NewTicker(banSweepInterval)
	defer sweep.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-s.banChan:
			s.handleBanEvent(ctx, event)
		case <-sweep.C:
			s.disconnectPeersOnBanList(ctx, "address on ban list")
		}
	}
}

func (s *Server) handleBanEvent(ctx context.Context, event BanEvent) {
	if event.Action != banActionAdd {
		return // we only care about new bans
	}

	if event.PeerID != "" {
		s.logger.Infof("[handleBanEvent] Received ban event for PeerID: %s (reason: %s)", event.PeerID, event.Reason)

		peerID, err := peer.Decode(event.PeerID)
		if err != nil {
			s.logger.Errorf("[handleBanEvent] Invalid PeerID in ban event: %s, error: %v", event.PeerID, err)
			return
		}

		s.disconnectBannedPeerByID(ctx, peerID, event.Reason)

		return
	}

	// IP/subnet ban with no PeerID (operator BanPeer RPC, startup replay). The
	// ban list has already been updated, so disconnect every connected peer
	// whose address matches an entry.
	s.disconnectPeersOnBanList(ctx, event.Reason)
}

// extractIPFromMultiaddr returns the literal IP component of a libp2p
// multiaddr string such as "/ip4/1.2.3.4/tcp/9905/p2p/12D3KooW...". It returns
// "" for addresses without a literal IP (DNS names, relay circuits) and for
// strings that don't parse as a multiaddr.
func extractIPFromMultiaddr(addrStr string) string {
	maddr, err := ma.NewMultiaddr(addrStr)
	if err != nil {
		return ""
	}

	if ip, err := maddr.ValueForProtocol(ma.P_IP4); err == nil {
		return ip
	}

	if ip, err := maddr.ValueForProtocol(ma.P_IP6); err == nil {
		return ip
	}

	return ""
}

// checkMultiaddrBanned reports whether the dial target of a multiaddr is on
// the IP/subnet ban list. Literal IPs are checked directly; DNS components
// (/dns, /dns4, /dns6, /dnsaddr) are resolved and every returned address is
// checked. A parse or resolution failure returns an error so callers can fail
// closed. Multiaddrs with neither an IP nor a DNS name (e.g. relay circuits)
// have nothing to check and pass; peer-ID bans cover those.
func (s *Server) checkMultiaddrBanned(ctx context.Context, addrStr string) (bool, error) {
	if s.banList == nil {
		return false, nil
	}

	if ip := extractIPFromMultiaddr(addrStr); ip != "" {
		return s.banList.IsBanned(ip), nil
	}

	maddr, err := ma.NewMultiaddr(addrStr)
	if err != nil {
		return false, errors.NewInvalidArgumentError("invalid multiaddr %s: %v", addrStr, err)
	}

	host := ""
	for _, proto := range []int{ma.P_DNS, ma.P_DNS4, ma.P_DNS6, ma.P_DNSADDR} {
		if v, verr := maddr.ValueForProtocol(proto); verr == nil {
			host = v
			break
		}
	}
	if host == "" {
		return false, nil
	}

	ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
	if err != nil {
		return false, errors.NewServiceError("cannot resolve %s for ban check: %v", host, err)
	}

	for _, ip := range ips {
		if s.banList.IsBanned(ip.String()) {
			return true, nil
		}
	}

	return false, nil
}

// disconnectPeersOnBanList disconnects every connected peer whose multiaddr IP
// matches an entry (IP or subnet) in the IP-based ban list.
func (s *Server) disconnectPeersOnBanList(ctx context.Context, reason string) {
	if s.P2PClient == nil || s.banList == nil {
		return
	}

	// Without network severance a banned peer stays connected at the libp2p
	// layer forever, so the periodic sweep would re-match it on every pass.
	// Only act when there is something to do: the peer re-entered the registry
	// or the client can actually close the connection.
	_, canSever := s.P2PClient.(networkDisconnector)

	for _, p := range s.P2PClient.GetPeers() {
		for _, addr := range p.Addrs {
			ip := extractIPFromMultiaddr(addr)
			if ip == "" || !s.banList.IsBanned(ip) {
				continue
			}

			pid, err := peer.Decode(p.ID)
			if err != nil {
				s.logger.Errorf("[disconnectPeersOnBanList] failed to decode peer ID %s: %v", p.ID, err)
				break
			}

			if _, inRegistry := s.getPeer(pid); inRegistry || canSever {
				s.disconnectBannedPeerByID(ctx, pid, reason)
			}

			break
		}
	}
}

// networkDisconnector is implemented by P2P clients that can sever the
// underlying libp2p connection. go-p2p-message-bus does not implement it as of
// v0.1.17 (nothing up to v0.1.21 exposes the host, a gater, or a disconnect);
// once the library gains this capability, network-layer ban enforcement starts
// working here without further changes.
type networkDisconnector interface {
	DisconnectPeer(ctx context.Context, peerID peer.ID) error
}

// disconnectBannedPeerByID drops a peer at the application layer (registry +
// sync coordinator) and, when the P2P client supports it, closes the libp2p
// connection as well.
func (s *Server) disconnectBannedPeerByID(ctx context.Context, peerID peer.ID, reason string) {
	s.logger.Infof("[disconnectBannedPeerByID] Disconnecting banned peer %s (reason: %s)", peerID, reason)

	if nd, ok := s.P2PClient.(networkDisconnector); ok {
		if err := nd.DisconnectPeer(ctx, peerID); err != nil {
			s.logger.Errorf("[disconnectBannedPeerByID] network disconnect of %s failed: %v", peerID, err)
		}
	}

	s.removePeer(peerID)
}
