package p2p

import (
	"context"
	"encoding/json"
	"net"
	"net/url"
	"strings"
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

func (s *Server) handleBlockTopic(ctx context.Context, m []byte, fromID string) {
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

	// Drop messages from banned peers before any registration, WebSocket
	// forwarding, or further processing (and before field validation and the
	// spoof check, so a banned peer cannot keep triggering uncached
	// AddBanScore RPCs). Own messages are identified by the real sender, not
	// the claimed PeerID, so a banned peer spoofing our ID cannot dodge the
	// skip; genuine own messages return at the self check below.
	if fromID != s.P2PClient.GetID() && s.shouldSkipBannedPeer(fromID, "handleBlockTopic") {
		return
	}

	// Bound every peer-controlled string field before any side effect (logging
	// — including the spoof log below — registry write, WebSocket fan-out).
	// Display-only free text is sanitized in place; a malformed
	// protocol-format field is a protocol violation. The self check is on
	// fromID alone so a peer claiming our ID cannot dodge the score.
	blockMessage.sanitizeFields()

	if err = blockMessage.validateFields(); err != nil {
		s.logger.Errorf("[handleBlockTopic] invalid block message field from peer %s: %v", fromID, err)
		if fromID != s.P2PClient.GetID() {
			s.addProtocolViolation(fromID)
		}
		return
	}

	// Check that fromID matches the block peer ID
	if fromID != blockMessage.PeerID {
		s.logger.Errorf("[handleBlockTopic] peer ID spoofing detected: from=%s claimed=%s", fromID, blockMessage.PeerID)
		s.addProtocolViolation(fromID)
		return
	}

	// Validate DataHubURL (SSRF + operator blacklist)
	if !s.checkDataHubURL(blockMessage.DataHubURL, fromID, "handleBlockTopic") {
		return
	}

	s.logger.Infof("[handleBlockTopic] received block %s fromID %s", blockMessage.Hash, blockMessage.PeerID)

	// The spoof check above proved fromID == blockMessage.PeerID, so the
	// sender comparison alone decides self.
	isSelf := fromID == s.P2PClient.GetID()
	advertisedHeight := blockMessage.Height
	if isSelf {
		hash, err = s.parseHash(blockMessage.Hash, "handleBlockTopic")
		if err != nil {
			return
		}
	} else {
		var ok bool
		advertisedHeight, hash, ok = s.sanitizeAdvertisedTip(blockMessage.PeerID, blockMessage.Height, blockMessage.Hash, s.getLocalHeight(ctx))
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
		notificationDropped("block")
		s.logger.Warnf("[handleBlockTopic] notification channel full, dropped block notification for %s", blockMessage.Hash)
	}

	// Ignore our own messages
	if isSelf {
		s.logger.Debugf("[handleBlockTopic] ignoring own block message for %s", blockMessage.Hash)
		return
	}

	now := time.Now().UTC()

	// Store the peer ID that sent this block, keyed by the canonical hash
	// string. Ban lookups (ReportInvalidBlock, processInvalidBlockMessage) use
	// hash.String() from block validation, so keying by the raw message string
	// would let a peer evade the invalid-block ban by announcing a
	// non-canonical hex form (uppercase, truncated).
	s.storePeerMapEntry(&s.blockPeerMap, hash.String(), fromID, now)

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

	// Drop messages from banned peers before any registration, WebSocket
	// forwarding, or further processing (and before field validation and the
	// spoof check, so a banned peer cannot keep triggering uncached
	// AddBanScore RPCs). Own messages are identified by the real sender, not
	// the claimed PeerID, so a banned peer spoofing our ID cannot dodge the
	// skip; genuine own messages return at the self check below.
	if fromID != s.P2PClient.GetID() && s.shouldSkipBannedPeer(fromID, "handleSubtreeTopic") {
		return
	}

	// Bound every peer-controlled string field before any side effect:
	// display-only free text is sanitized in place, a malformed
	// protocol-format field drops the message and scores the sender.
	subtreeMessage.sanitizeFields()

	if err = subtreeMessage.validateFields(); err != nil {
		s.logger.Errorf("[handleSubtreeTopic] invalid subtree message field from peer %s: %v", fromID, err)
		if fromID != s.P2PClient.GetID() {
			s.addProtocolViolation(fromID)
		}
		return
	}

	// Check that fromID matches the subtree peer ID
	if fromID != subtreeMessage.PeerID {
		s.logger.Errorf("[handleSubtreeTopic] peer ID spoofing detected: from=%s claimed=%s", fromID, subtreeMessage.PeerID)
		s.addProtocolViolation(fromID)
		return
	}

	// Validate DataHubURL (SSRF + operator blacklist)
	if !s.checkDataHubURL(subtreeMessage.DataHubURL, fromID, "handleSubtreeTopic") {
		return
	}

	s.logger.Debugf("[handleSubtreeTopic] received subtree %s from %s", subtreeMessage.Hash, subtreeMessage.PeerID)

	// Parse the hash before any use, mirroring handleBlockTopic: a malformed
	// hash must not reach WebSocket subscribers or count as peer activity.
	hash, err = s.parseHash(subtreeMessage.Hash, "handleSubtreeTopic")
	if err != nil {
		return
	}

	now := time.Now().UTC()

	select {
	case s.notificationCh <- &notificationMsg{
		Timestamp:  now.Format(isoFormat),
		Type:       "subtree",
		Hash:       hash.String(),
		BaseURL:    subtreeMessage.DataHubURL,
		PeerID:     subtreeMessage.PeerID,
		ClientName: subtreeMessage.ClientName,
	}:
	default:
		notificationDropped("subtree")
		s.logger.Warnf("[handleSubtreeTopic] notification channel full, dropped subtree notification for %s", subtreeMessage.Hash)
	}

	// Ignore our own messages. The spoof check above proved fromID equals the
	// claimed PeerID, so the sender comparison alone decides self.
	if fromID == s.P2PClient.GetID() {
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

	// Store the peer ID that sent this subtree, keyed by the canonical hash
	// string so the ReportInvalidSubtree lookup (which uses hash.String() from
	// subtree validation) matches even when the announcer sent a non-canonical
	// hex form.
	s.storePeerMapEntry(&s.subtreePeerMap, hash.String(), fromID, now)
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

// isBlacklistedBaseURL checks the given baseURL against the operator-configured
// blacklist (settings.SubtreeValidation.BlacklistedBaseURLs).
func (s *Server) isBlacklistedBaseURL(baseURL string) bool {
	if s.settings == nil {
		return false
	}

	return isBaseURLBlacklisted(baseURL, s.settings.SubtreeValidation.BlacklistedBaseURLs)
}

// isBaseURLBlacklisted checks if the given baseURL matches any entry in the
// blacklist. Package-level so both the gossip handlers (via the Server wrapper
// above) and the sync PeerSelector can enforce the same blacklist.
func isBaseURLBlacklisted(baseURL string, blacklist map[string]struct{}) bool {
	inputHost := extractHost(baseURL)
	if inputHost == "" {
		// Fall back to exact string matching for invalid URLs
		for blocked := range blacklist {
			if baseURL == blocked {
				return true
			}
		}

		return false
	}

	// Check each blacklisted URL
	for blocked := range blacklist {
		blockedHost := blacklistEntryHost(blocked)
		if blockedHost == "" {
			// Fall back to exact string matching for unparseable blacklisted entries
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

// blacklistEntryHost extracts the normalized host of a blacklist entry.
// Operators commonly configure bare hosts ("evil.example"), which url.Parse
// reads as a path (empty host), so scheme-less entries are retried in
// protocol-relative form. Returns "" only for entries with no parseable host.
func blacklistEntryHost(blocked string) string {
	if host := extractHost(blocked); host != "" {
		return host
	}

	return extractHost("//" + blocked)
}

// extractHost extracts and normalizes the host component from a URL
func extractHost(urlStr string) string {
	parsedURL, err := url.Parse(urlStr)
	if err != nil {
		return ""
	}

	// Strip trailing dots of a rooted FQDN so "evil.example." (or the
	// non-resolvable "evil.example..") matches a blacklist entry for
	// "evil.example".
	host := strings.TrimRight(parsedURL.Hostname(), ".")
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
//
// This is a cheap static pre-filter only: it deliberately performs no DNS resolution, since
// that would let a peer trigger a blocking lookup per announcement. A hostname that resolves
// to an internal address therefore passes here and is stopped at connection time instead -
// by the peer health check probe client and by the shared
// block/subtree fetch client (util.NewSSRFSafeDialContext).
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

	// Canonicalize before checking: strip trailing dots of a rooted FQDN and
	// lowercase, so "localhost.", "LOCALHOST" or "127.0.0.1." cannot slip past
	// the checks below (DNS resolution is case-insensitive).
	hostname := strings.ToLower(strings.TrimRight(parsed.Hostname(), "."))
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

// checkDataHubURL runs the trust checks shared by the block and subtree
// announcement handlers: SSRF validation (a failure counts as a protocol
// violation) and the operator-configured blacklist (a match drops the message
// without penalising the peer). Returns false when the message must be dropped.
// handleNodeStatusTopic runs the same two checks inline because a blacklist
// match there only strips the BaseURL instead of dropping the telemetry.
func (s *Server) checkDataHubURL(dataHubURL, fromID, handlerName string) bool {
	if err := s.validateDataHubURL(dataHubURL); err != nil {
		s.logger.Errorf("[%s] invalid DataHubURL from peer %s: %v", handlerName, fromID, err)
		s.addProtocolViolation(fromID)
		return false
	}

	if s.isBlacklistedBaseURL(dataHubURL) {
		s.logger.Warnf("[%s] blocked notification from blacklisted DataHubURL %s (peer %s)", handlerName, dataHubURL, fromID)
		return false
	}

	return true
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

	// Drop messages from banned peers before any registration or further
	// processing (and before field validation and the spoof check, so a
	// banned peer cannot keep triggering uncached AddBanScore RPCs). Own
	// messages are identified by the real sender, not the claimed PeerID, so
	// a banned peer spoofing our ID cannot dodge the skip.
	if fromID != s.P2PClient.GetID() && s.shouldSkipBannedPeer(fromID, "handleRejectedTxTopic") {
		return
	}

	// Bound every peer-controlled string field before any side effect:
	// display-only free text is sanitized in place, a malformed
	// protocol-format field drops the message and scores the sender.
	rejectedTxMessage.sanitizeFields()

	if err = rejectedTxMessage.validateFields(); err != nil {
		s.logger.Errorf("[handleRejectedTxTopic] invalid rejected tx message field from peer %s: %v", fromID, err)
		if fromID != s.P2PClient.GetID() {
			s.addProtocolViolation(fromID)
		}
		return
	}

	// Check that fromID matches the rejected tx peer ID
	if fromID != rejectedTxMessage.PeerID {
		s.logger.Errorf("[handleRejectedTxTopic] peer ID spoofing detected: from=%s claimed=%s", fromID, rejectedTxMessage.PeerID)
		s.addProtocolViolation(fromID)
		return
	}

	// The spoof check above proved fromID equals the claimed PeerID, so the
	// sender comparison alone decides self.
	if fromID == s.P2PClient.GetID() {
		s.logger.Debugf("[handleRejectedTxTopic] ignoring own rejected tx message for %s", rejectedTxMessage.TxID)
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

// localHeightCacheTTL bounds how stale the cached local height may be. The
// height only feeds advertised-tip sanitization caps and periodic sync
// evaluation, both tolerant of a second of staleness. Failed reads are cached
// with the shorter error TTL: long enough to shed the per-message RPC storm
// during a blockchain outage, short enough that recovery is picked up quickly
// (while an error entry is served, advertised tips are capped as if the local
// height were 0).
const (
	localHeightCacheTTL      = time.Second
	localHeightErrorCacheTTL = 200 * time.Millisecond
)

type localHeightCacheEntry struct {
	height    uint32
	ok        bool
	fetchedAt time.Time
}

// getLocalHeight returns the current local blockchain height. The result is
// cached briefly: gossip handlers call this per message (via
// sanitizeAdvertisedTip) and must not issue a blockchain gRPC round-trip each
// time. Failures are cached too (with a shorter TTL), so a blockchain outage
// does not turn back into a per-message RPC storm. Cache misses issue one RPC
// bounded by defaultRPCTimeout derived from the caller's ctx so a hung
// blockchain service cannot stall the caller (the sync coordinator's monitor
// loops reach this on every tick via its local-height callback).
func (s *Server) getLocalHeight(ctx context.Context) uint32 {
	if s.blockchainClient == nil {
		return 0
	}

	if e := s.localHeightCache.Load(); e != nil {
		ttl := localHeightCacheTTL
		if !e.ok {
			ttl = localHeightErrorCacheTTL
		}
		if time.Since(e.fetchedAt) < ttl {
			return e.height
		}
	}

	rpcCtx, cancel := context.WithTimeout(ctx, defaultRPCTimeout)
	defer cancel()

	height, ok := uint32(0), false
	if _, bhMeta, err := s.blockchainClient.GetBestBlockHeader(rpcCtx); err == nil && bhMeta != nil {
		height, ok = bhMeta.Height, true
	}

	s.localHeightCache.Store(&localHeightCacheEntry{height: height, ok: ok, fetchedAt: time.Now()})

	return height
}

func (s *Server) sanitizeAdvertisedTip(peerID string, advertisedHeight uint32, advertisedHash string, localHeight uint32) (uint32, *chainhash.Hash, bool) {
	hash, err := chainhash.NewHashFromStr(advertisedHash)
	if err != nil {
		// Log the length, never the value: node_status deliberately feeds the
		// raw advertised hash through here (sanitizeAdvertisedTip must see what
		// the peer actually sent), so this is the one place a peer-controlled
		// string of near-message-cap size could otherwise reach the logs.
		s.logger.Warnf("[sanitizeAdvertisedTip] rejecting advertised tip from peer %s: invalid block hash (len %d): %v", peerID, len(advertisedHash), err)
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
	if s.registryBatcher != nil {
		s.registryBatcher.enqueueRegister(peerID.String(), clientName, height, blockHash, dataHubURL, false)
		return
	}
	s.registerPeer(peerID, clientName, height, blockHash, dataHubURL)
}

// addConnectedPeer adds a peer and marks it as directly connected
func (s *Server) addConnectedPeer(peerID peer.ID, clientName string, height uint32, blockHash *chainhash.Hash, dataHubURL string) {
	if s.registryBatcher != nil {
		s.registryBatcher.enqueueRegister(peerID.String(), clientName, height, blockHash, dataHubURL, true)
		return
	}
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
	// Clear batcher state first: pending updates for a removed peer are stale,
	// and its next message must re-register it rather than being skipped as
	// recently asserted.
	if s.registryBatcher != nil {
		s.registryBatcher.forget(peerID.String())
	}
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
	if mode == "" {
		return
	}
	if s.registryBatcher != nil {
		s.registryBatcher.enqueueStorage(peerID.String(), mode)
		return
	}
	if s.peerRegistry != nil {
		if err := s.peerRegistry.UpdateStorage(s.gCtx, peerID.String(), mode); err != nil {
			s.logger.Warnf("[updateStorage] UpdateStorage %s failed: %v", peerID, err)
		}
	}
}

// startInvalidBlocksConsumer starts the injected invalid-blocks Kafka consumer
// with processInvalidBlockMessage. The consumer field is never reassigned after
// this, so Stop() closes the consumer that is actually running.
func (s *Server) startInvalidBlocksConsumer(ctx context.Context) {
	if s.invalidBlocksKafkaConsumerClient == nil {
		s.logger.Errorf("[startInvalidBlocksConsumer] invalid-blocks Kafka consumer not configured (kafka_invalidBlocksConfig unset), peers will not be banned for invalid blocks")
		return
	}

	s.logger.Infof("[startInvalidBlocksConsumer] starting invalid blocks Kafka consumer on topic: %s", s.settings.Kafka.InvalidBlocks)
	// Transient handler failures (e.g. peer registry unavailable) get two
	// retries with backoff (three attempts total) before the offset is
	// committed; after that the message is skipped so it cannot stall the
	// partition.
	s.invalidBlocksKafkaConsumerClient.Start(ctx, s.processInvalidBlockMessage, kafka.WithRetryAndMoveOn(2, 2, time.Second))
}

func (s *Server) processInvalidBlockMessage(message *kafka.KafkaMessage) error {
	// Use the server context so an in-flight AddBanScore is cancelled at shutdown.
	ctx := s.gCtx

	var invalidBlockMsg kafkamessage.KafkaInvalidBlockTopicMessage
	if err := proto.Unmarshal(message.Value, &invalidBlockMsg); err != nil {
		// A malformed message can never succeed on retry: log and skip it.
		s.logger.Errorf("[processInvalidBlockMessage] failed to unmarshal invalid block message: %v", err)
		return nil
	}

	blockHash := invalidBlockMsg.GetBlockHash()
	reason := invalidBlockMsg.GetReason()

	s.logger.Infof("[processInvalidBlockMessage] processing invalid block %s: %s", blockHash, reason)

	// Look up the peer ID that sent this block
	peerID, err := s.getPeerFromMap(&s.blockPeerMap, blockHash, "block")
	if err != nil {
		s.logger.Warnf("[processInvalidBlockMessage] %v", err)
		return nil // Not an error, just no peer to ban
	}

	// Add ban score to the peer
	s.logger.Infof("[processInvalidBlockMessage] adding ban score to peer %s for invalid block %s: %s",
		peerID, blockHash, reason)

	req := &p2p_api.AddBanScoreRequest{
		PeerId: peerID,
		Reason: ReasonInvalidBlock,
	}

	if _, err := s.AddBanScore(ctx, req); err != nil {
		s.logger.Errorf("[processInvalidBlockMessage] error adding ban score to peer %s: %v", peerID, err)
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
// and of the reputation, IP-ban and ban-status caches. It removes entries older
// than their TTL; size is enforced at insert instead (issue 1409), so there is
// no size pass here.
func (s *Server) cleanupPeerMaps() {
	now := time.Now()

	// Expire the attribution maps in a single locked pass each. Size is not
	// enforced here: the maps are bounded at insert (issue 1409), so by the
	// time this runs they are already within the cap.
	//
	// Resolve the TTL rather than trusting the field: a Server that never
	// reached applyPeerMapLimits leaves it at zero, and a zero TTL puts the
	// cutoff at now, expiring every entry on every tick and switching off ban
	// attribution entirely. The cap fails closed, so this does too.
	ttlCutoff := now.Add(-s.peerMapTTLOrDefault())
	blockExpired := s.blockPeerMap.DeleteExpired(ttlCutoff)
	subtreeExpired := s.subtreePeerMap.DeleteExpired(ttlCutoff)

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

	// Evict expired liveConnCache entries: the liveness checks insert one
	// entry per unique peer ID gossip is seen from.
	var liveConnKeysToDelete []string
	s.liveConnCache.Range(func(key, value interface{}) bool {
		if entry, ok := value.(liveConnCacheEntry); ok {
			if now.After(entry.expiresAt) {
				liveConnKeysToDelete = append(liveConnKeysToDelete, key.(string))
			}
		}
		return true
	})
	for _, key := range liveConnKeysToDelete {
		s.liveConnCache.Delete(key)
	}

	// Evict expired banStatusCache entries: shouldSkipBannedPeer inserts one
	// entry per unique peer ID gossip is seen from.
	var banStatusKeysToDelete []string
	s.banStatusCache.Range(func(key, value interface{}) bool {
		if entry, ok := value.(banStatusCacheEntry); ok {
			if now.After(entry.expiresAt) {
				banStatusKeysToDelete = append(banStatusKeysToDelete, key.(string))
			}
		}
		return true
	})
	for _, key := range banStatusKeysToDelete {
		s.banStatusCache.Delete(key)
	}

	// Log cleanup stats
	if blockExpired > 0 || subtreeExpired > 0 || len(reputationKeysToDelete) > 0 || len(ipBanKeysToDelete) > 0 || len(banStatusKeysToDelete) > 0 {
		s.logger.Infof("[cleanupPeerMaps] removed %d expired block entries, %d expired subtree entries, %d expired reputation entries, %d expired IP-ban entries, %d expired ban-status entries",
			blockExpired, subtreeExpired, len(reputationKeysToDelete), len(ipBanKeysToDelete), len(banStatusKeysToDelete))
	}

	// Surface how many entries the inline cap evicted since the last sweep
	// (issue 1409) — flood visibility without a per-insert log line. Sustained
	// eviction means announcements are arriving faster than the cap can hold,
	// so attribution for the oldest of them is being aged out early. The
	// attribution matters as much as the count: pressure spread across peers
	// is throughput and a larger cap helps, whereas one dominant contributor
	// is a flood, where a larger cap just hands the attacker more memory and a
	// longer sweep — ban the peer instead (issue 1503).
	if blockEvictions, subtreeEvictions := s.blockPeerMap.EvictionsSinceLastRead(), s.subtreePeerMap.EvictionsSinceLastRead(); blockEvictions.total > 0 || subtreeEvictions.total > 0 {
		s.logger.Warnf("[cleanupPeerMaps] peer maps at capacity since the last sweep: evicted %d oldest block entries (%s) and %d oldest subtree entries (%s)",
			blockEvictions.total, blockEvictions, subtreeEvictions.total, subtreeEvictions)
	}

	// Log current sizes
	s.logger.Infof("[cleanupPeerMaps] current map sizes - blocks: %d, subtrees: %d",
		s.blockPeerMap.Len(), s.subtreePeerMap.Len())
}

// startPeerMapCleanup starts the periodic cleanup goroutine
// Helper methods to reduce redundancy

// shouldSkipBannedPeer checks if we should skip a message from a banned peer:
// score-based bans live in the centralized peer registry, operator IP/subnet
// bans in the local ban list. Registry failures are tolerated (return false)
// so a transient registry blip doesn't drop traffic silently. Registry lookups
// are cached for reputationCacheTTL to avoid a gRPC round-trip per gossip
// message; local ban transitions (onPeerBanned) overwrite the cache entry
// immediately.
func (s *Server) shouldSkipBannedPeer(from string, messageType string) bool {
	if s.peerRegistry != nil {
		if banned, ok := s.cachedBanStatus(from); ok {
			if banned {
				s.logger.Debugf("[%s] ignoring notification from banned peer %s", messageType, from)
				return true
			}
		} else {
			banned, err := s.peerRegistry.IsPeerBanned(s.gCtx, from)
			if err != nil {
				// Error breaker: cache the failure as not-banned (fail open,
				// same staleness contract as the reputation cache) so a
				// degraded registry is hit — and logged — at most once per
				// TTL per peer instead of once per gossip message.
				s.banStatusCache.Store(from, banStatusCacheEntry{banned: false, expiresAt: time.Now().Add(reputationCacheTTL)})
				s.logger.Warnf("[%s] IsPeerBanned %s failed (treating as not banned for %s): %v", messageType, from, reputationCacheTTL, err)
			} else {
				s.banStatusCache.Store(from, banStatusCacheEntry{banned: banned, expiresAt: time.Now().Add(reputationCacheTTL)})
				if banned {
					s.logger.Debugf("[%s] ignoring notification from banned peer %s", messageType, from)
					return true
				}
			}
		}
	}

	if s.isPeerIPBanned(from) {
		s.logger.Debugf("[%s] ignoring notification from IP-banned peer %s", messageType, from)
		return true
	}

	return false
}

type banStatusCacheEntry struct {
	banned    bool
	expiresAt time.Time
}

// cachedBanStatus returns the cached registry ban status for the peer and
// whether a fresh cache entry existed.
func (s *Server) cachedBanStatus(peerID string) (bool, bool) {
	if v, ok := s.banStatusCache.Load(peerID); ok {
		entry := v.(banStatusCacheEntry)
		if time.Now().Before(entry.expiresAt) {
			return entry.banned, true
		}
	}
	return false, false
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
	live := false
	for _, p := range s.P2PClient.GetPeers() {
		if p.ID != peerID {
			continue
		}
		live = len(p.Addrs) > 0
		for _, addr := range p.Addrs {
			if ip := extractIPFromMultiaddr(addr); ip != "" && s.banList.IsBanned(ip) {
				banned = true
				break
			}
		}
		break
	}

	s.ipBanCache.Store(peerID, ipBanCacheEntry{banned: banned, expiresAt: now.Add(reputationCacheTTL)})
	// This walk just observed the peer's connectedness; record it so the
	// hasLiveConnection check that follows on the same gossip path does not
	// have to repeat the walk.
	s.cacheLiveConn(peerID, live, now)

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

// storePeerMapEntry stores a peer entry in the specified map. The map is
// bounded inline (issue 1409): at capacity the OLDEST entry is evicted, so a
// distinct-hash flood cannot grow memory without bound between sweeps, and
// cannot pre-emptively suppress attribution for the announcement arriving
// next. A flood after an honest announcement can still age it out before
// validation reports on it (issue 1503), and a peer can age out its own
// attribution the same way to escape the invalid-block ban path (issue 1433).
func (s *Server) storePeerMapEntry(peerMap *cappedPeerMap, hash string, from string, timestamp time.Time) {
	peerMap.Store(hash, peerMapEntry{
		peerID:    from,
		timestamp: timestamp,
	})
}

// getPeerFromMap retrieves a peer entry from a map
func (s *Server) getPeerFromMap(peerMap *cappedPeerMap, hash string, mapType string) (string, error) {
	entry, ok := peerMap.Load(hash)
	if !ok {
		s.logger.Warnf("[getPeerFromMap] no peer found for %s %s", mapType, hash)
		return "", errors.NewNotFoundError("no peer found for %s %s", mapType, hash)
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
				// Reconcile on its own goroutine so a slow registry can never
				// delay the cache sweep above (the only expirer of the
				// reputation/ban caches); skip the tick if the previous pass
				// is still running.
				if s.reconcileInFlight.CompareAndSwap(false, true) {
					go func() {
						defer s.reconcileInFlight.Store(false)
						s.reconcileConnectionStates(ctx)
					}()
				}
			}
		}
	}()

	s.logger.Infof("[startPeerMapCleanup] started peer map cleanup with interval %v", cleanupInterval)
}

// reconcileTimeout bounds one reconcileConnectionStates pass so a wedged or
// flooded registry cannot pin the reconcile goroutine (and its RPCs) forever.
const reconcileTimeout = 30 * time.Second

// snapshotLiveConnIDs returns the set of peer IDs that currently have an
// open libp2p connection. Liveness comes from P2PClient.GetPeers(): verified
// against go-p2p-message-bus v0.1.17 (client.go GetPeers), Addrs is built
// from host.Network().ConnsToPeer — open connections only, not the peerstore
// — while the peer list itself is every peer that ever authored a message on
// a subscribed topic (gossip-only publishers included, never pruned). So the
// Addrs filter is what separates live neighbours from gossip-only authors;
// it is not redundant with the listing. This differs from the p2p service's
// own GetPeers RPC, which is filtered to IsConnected registry entries.
func (s *Server) snapshotLiveConnIDs() map[string]struct{} {
	live := make(map[string]struct{})
	if s.P2PClient != nil {
		for _, p := range s.P2PClient.GetPeers() {
			if len(p.Addrs) > 0 {
				live[p.ID] = struct{}{}
			}
		}
	}
	return live
}

type liveConnCacheEntry struct {
	live      bool
	expiresAt time.Time
}

// cacheLiveConn records whether the peer had an open connection when a
// GetPeers walk last saw it, valid for reputationCacheTTL.
func (s *Server) cacheLiveConn(peerID string, live bool, now time.Time) {
	s.liveConnCache.Store(peerID, liveConnCacheEntry{live: live, expiresAt: now.Add(reputationCacheTTL)})
}

// hasLiveConnection reports whether the peer has an open libp2p connection,
// answered from liveConnCache (populated by isPeerIPBanned's walk, which the
// gossip handlers run for the same peer immediately before this) or, on a
// miss, by a targeted GetPeers walk cached for reputationCacheTTL. A freshly
// connected neighbour is therefore visible on its first message, while a
// gossip-relayed publisher walks once per TTL and stays unflagged. The answer
// can be up to reputationCacheTTL stale after a disconnect; the reconcile
// sweep corrects that within one cleanup interval.
func (s *Server) hasLiveConnection(peerID string) bool {
	now := time.Now()
	if v, ok := s.liveConnCache.Load(peerID); ok {
		entry := v.(liveConnCacheEntry)
		if now.Before(entry.expiresAt) {
			return entry.live
		}
	}

	if s.P2PClient == nil {
		return false
	}

	live := false
	for _, p := range s.P2PClient.GetPeers() {
		if p.ID != peerID {
			continue
		}
		live = len(p.Addrs) > 0
		break
	}

	s.cacheLiveConn(peerID, live, now)

	return live
}

// reconcileConnectionStates synchronizes the registry's IsConnected flags with
// actual libp2p connectedness, in both directions: it clears the flag on
// entries with no live connection and sets it on live peers the hot path
// missed (their messages arrived before the liveness snapshot included them).
// Nothing else ever clears the flag: go-p2p-message-bus exposes no disconnect
// callback (see networkDisconnector) and the only other clearing site is the
// ban path (removePeer), so without this sweep every flagged peer would stay
// cleanup-exempt in the registry for the life of the process.
func (s *Server) reconcileConnectionStates(ctx context.Context) {
	if s.P2PClient == nil || s.peerRegistry == nil {
		return
	}

	live := s.snapshotLiveConnIDs()

	// Seed the per-peer liveness cache from the snapshot just built: nearly
	// free (bounded by open connections), and it tightens the hot path right
	// after this pass — a peer this pass is about to clear must not be
	// re-flagged off a stale cached "live" for the next few seconds.
	now := time.Now()
	for id := range live {
		s.cacheLiveConn(id, true, now)
	}

	ctx, cancel := context.WithTimeout(ctx, reconcileTimeout)
	defer cancel()

	peers, err := s.peerRegistry.ListPeers(ctx, nil, 0, 0, false, false)
	if err != nil {
		s.logger.Warnf("[reconcileConnectionStates] ListPeers failed: %v", err)
		return
	}

	cleared, flagged := 0, 0
	for _, info := range peers {
		if ctx.Err() != nil {
			s.logger.Warnf("[reconcileConnectionStates] pass cut short (%v) after clearing %d and flagging %d peers", ctx.Err(), cleared, flagged)
			return
		}

		_, isLive := live[info.ID]
		switch {
		case info.IsConnected && !isLive:
			if err := s.peerRegistry.UpdateConnectionState(ctx, info.ID, false); err != nil {
				s.logger.Warnf("[reconcileConnectionStates] UpdateConnectionState %s false failed: %v", info.ID, err)
				continue
			}
			// Overwrite any stale cached "live" so the hot path cannot
			// re-flag this peer off a pre-disconnect cache entry, and drop
			// the batcher's reassert memory so a peer that reconnects gets
			// re-marked connected on its next message instead of being
			// skipped as recently asserted.
			s.cacheLiveConn(info.ID, false, time.Now())
			if s.registryBatcher != nil {
				s.registryBatcher.forgetAssertState(info.ID)
			}
			cleared++
		case !info.IsConnected && isLive:
			// Deliberately no batcher poke here, unlike the clear branch: the
			// batcher's assert memory is either absent (next message asserts
			// anyway) or stale past the reassert TTL, and at worst the peer's
			// next message re-sends one redundant, idempotent
			// UpdateConnectionState(true). Not worth coupling for.
			if err := s.peerRegistry.UpdateConnectionState(ctx, info.ID, true); err != nil {
				s.logger.Warnf("[reconcileConnectionStates] UpdateConnectionState %s true failed: %v", info.ID, err)
				continue
			}
			flagged++
		}
	}

	if cleared > 0 || flagged > 0 {
		s.logger.Infof("[reconcileConnectionStates] reconciled connection flags: %d cleared, %d flagged live", cleared, flagged)
	}
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
// "" for addresses without a literal IP (DNS names), for relay circuits, and
// for strings that don't parse as a multiaddr. Relay circuits must be
// rejected: for a relayed connection the transport address is the RELAY's
// (e.g. "/ip4/<relay-ip>/tcp/9905/p2p/<relayID>/p2p-circuit"), so treating it
// as the peer's IP would ban or match the relay and every peer behind it.
// Peer-ID bans cover peers reached over a circuit.
func extractIPFromMultiaddr(addrStr string) string {
	maddr, err := ma.NewMultiaddr(addrStr)
	if err != nil {
		return ""
	}

	return extractIPFromParsedMultiaddr(maddr)
}

// extractIPFromParsedMultiaddr is extractIPFromMultiaddr for an
// already-parsed multiaddr; it applies the same relay-circuit rejection.
func extractIPFromParsedMultiaddr(maddr ma.Multiaddr) string {
	if isRelayCircuit(maddr) {
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

// isRelayCircuit reports whether the multiaddr contains a libp2p relay
// circuit component (/p2p-circuit). Every IP/DNS component of such an address
// names the RELAY, not the peer behind it, so ban logic must never attribute
// those components to the peer.
func isRelayCircuit(maddr ma.Multiaddr) bool {
	_, err := maddr.ValueForProtocol(ma.P_CIRCUIT)
	return err == nil
}

// checkMultiaddrBanned reports whether the dial target of a multiaddr is on
// the IP/subnet ban list. Literal IPs are checked directly; DNS components
// (/dns, /dns4, /dns6, /dnsaddr) are resolved and every returned address is
// checked. A parse or resolution failure returns an error so callers can fail
// closed. Relay circuits pass without any check: their IP/DNS components name
// the relay, not the dial target, so checking them would refuse to dial every
// peer behind a banned relay; peer-ID bans cover circuit peers.
func (s *Server) checkMultiaddrBanned(ctx context.Context, addrStr string) (bool, error) {
	if s.banList == nil {
		return false, nil
	}

	maddr, err := ma.NewMultiaddr(addrStr)
	if err != nil {
		return false, errors.NewInvalidArgumentError("invalid multiaddr %s: %v", addrStr, err)
	}

	if isRelayCircuit(maddr) {
		return false, nil
	}

	if ip := extractIPFromParsedMultiaddr(maddr); ip != "" {
		return s.banList.IsBanned(ip), nil
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
// v0.1.17 (nothing up to v0.1.23 exposes the host, a gater, or a disconnect);
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
