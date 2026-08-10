package p2p

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/bsv-blockchain/teranode/errors"
)

// Per-field length bounds for peer-controlled gossip strings. The per-topic
// message caps in Server.go bound the whole payload; these bound each string
// field against its realistic maximum so a message inside the cap cannot smuggle
// a padded field into the peer registry, the logs, or the WebSocket broadcast
// (where one inbound message fans out to every connected /p2p-ws client).
//
// Fields come in two classes, handled differently on ingress:
//
//   - Protocol-format fields (peer IDs, hashes, headers, chainwork, URLs) have
//     a well-defined shape every honest node produces. A violation is a
//     protocol violation: the message is dropped and the sender scored, same
//     as a spoofed peer ID (validateFields).
//   - Display-only free text (client name, miner name, version, rejection
//     reason, ...) is telemetry that may embed third-party or operator input —
//     miner names come from coinbase tags, reasons from validator error
//     chains. Rejecting those would let a miner's coinbase or a long error get
//     honest relaying nodes banned network-wide, and would suppress block
//     announcements (the catchup trigger) over a cosmetic field. They are
//     sanitized in place instead: control/non-printable runes stripped and the
//     value truncated to its bound (sanitizeFields).
const (
	maxGossipPeerIDLen     = 128  // libp2p peer IDs are ~52 chars base58
	maxGossipClientNameLen = 128  // operator-configured client name
	maxGossipMinerNameLen  = 256  // extracted coinbase miner tag (third-party data, may be non-ASCII UTF-8)
	maxGossipVersionLen    = 64   // release/git-describe version string
	maxGossipCommitHashLen = 64   // git commit hash (40 hex) with headroom for suffixes like "-dirty"
	maxGossipTypeLen       = 32   // message type discriminator ("node_status")
	maxGossipFSMStateLen   = 64   // blockchain FSM state enum name
	maxGossipListenModeLen = 32   // "full" / "listen_only" / "silent"
	maxGossipStorageLen    = 32   // "full" / "pruned" / ""
	maxGossipHashLen       = 64   // hex-encoded 32-byte hash
	maxGossipHeaderLen     = 160  // hex-encoded 80-byte block header
	maxGossipChainWorkLen  = 64   // hex-encoded chainwork (256-bit big-endian)
	maxGossipURLLen        = 2048 // DataHub / propagation URLs
	maxGossipReasonLen     = 1024 // rejected-tx reason (validator error text)
)

// checkGossipString rejects a peer-supplied string that exceeds maxLen bytes,
// is not valid UTF-8, or contains non-printable runes (guards log injection
// and bidi/line-separator spoofing tricks such as U+202E and U+2028; matches
// the unicode.IsPrint filter extractMiner applies to coinbase tags). Printable
// non-ASCII UTF-8 is allowed. The offending value is never echoed into the
// returned error so an oversized field cannot inflate logs. Note this bounds
// the value, it does not HTML-escape it — renderers are still responsible for
// escaping.
func checkGossipString(field, value string, maxLen int) error {
	if len(value) > maxLen {
		return errors.NewInvalidArgumentError("%s length %d exceeds max %d", field, len(value), maxLen)
	}

	if !utf8.ValidString(value) {
		return errors.NewInvalidArgumentError("%s is not valid UTF-8", field)
	}

	for _, r := range value {
		if !unicode.IsPrint(r) {
			return errors.NewInvalidArgumentError("%s contains a non-printable character", field)
		}
	}

	return nil
}

// checkGossipHex rejects a peer-supplied string that exceeds maxLen bytes or
// contains a non-hexadecimal character. Empty values pass: hash and chainwork
// fields are optional in gossip messages.
func checkGossipHex(field, value string, maxLen int) error {
	if len(value) > maxLen {
		return errors.NewInvalidArgumentError("%s length %d exceeds max %d", field, len(value), maxLen)
	}

	for i := 0; i < len(value); i++ {
		c := value[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return errors.NewInvalidArgumentError("%s contains a non-hex character", field)
		}
	}

	return nil
}

// gossipFieldCheck describes one protocol-format field to validate: free-text
// fields use checkGossipString, hex fields (hashes, chainwork, headers) use
// checkGossipHex.
type gossipFieldCheck struct {
	field  string
	value  string
	maxLen int
	hex    bool
}

func checkGossipFields(checks []gossipFieldCheck) error {
	for _, c := range checks {
		var err error
		if c.hex {
			err = checkGossipHex(c.field, c.value, c.maxLen)
		} else {
			err = checkGossipString(c.field, c.value, c.maxLen)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

// validateFields checks the protocol-format fields of a node_status message.
// Handlers call it (after sanitizeFields) before the message reaches the
// notification channel, the peer registry, or the logs.
func (m *NodeStatusMessage) validateFields() error {
	return checkGossipFields([]gossipFieldCheck{
		{"peer_id", m.PeerID, maxGossipPeerIDLen, false},
		{"base_url", m.BaseURL, maxGossipURLLen, false},
		{"propagation_url", m.PropagationURL, maxGossipURLLen, false},
		{"best_block_hash", m.BestBlockHash, maxGossipHashLen, true},
		{"chain_work", m.ChainWork, maxGossipChainWorkLen, true},
		{"sync_peer_id", m.SyncPeerID, maxGossipPeerIDLen, false},
		{"sync_peer_block_hash", m.SyncPeerBlockHash, maxGossipHashLen, true},
	})
}

// sanitizeFields bounds the display-only free-text fields of a node_status
// message in place.
func (m *NodeStatusMessage) sanitizeFields() {
	m.ClientName = sanitizeGossipString(m.ClientName, maxGossipClientNameLen)
	m.MinerName = sanitizeGossipString(m.MinerName, maxGossipMinerNameLen)
	m.Version = sanitizeGossipString(m.Version, maxGossipVersionLen)
	m.CommitHash = sanitizeGossipString(m.CommitHash, maxGossipCommitHashLen)
	m.Type = sanitizeGossipString(m.Type, maxGossipTypeLen)
	m.FSMState = sanitizeGossipString(m.FSMState, maxGossipFSMStateLen)
	m.ListenMode = sanitizeGossipString(m.ListenMode, maxGossipListenModeLen)
	m.Storage = sanitizeGossipString(m.Storage, maxGossipStorageLen)
}

// validateFields checks the protocol-format fields of a block announcement.
// Coinbase is charset-checked only: Teranode does not populate it today and
// nothing consumes it, so its length is bounded by maxBlockMessageSize (which
// keeps headroom for it).
func (m *BlockMessage) validateFields() error {
	return checkGossipFields([]gossipFieldCheck{
		{"peer_id", m.PeerID, maxGossipPeerIDLen, false},
		{"data_hub_url", m.DataHubURL, maxGossipURLLen, false},
		{"hash", m.Hash, maxGossipHashLen, true},
		{"header", m.Header, maxGossipHeaderLen, true},
		{"coinbase", m.Coinbase, maxBlockMessageSize, true},
	})
}

// sanitizeFields bounds the display-only free-text fields of a block
// announcement in place.
func (m *BlockMessage) sanitizeFields() {
	m.ClientName = sanitizeGossipString(m.ClientName, maxGossipClientNameLen)
}

// validateFields checks the protocol-format fields of a subtree announcement.
func (m *SubtreeMessage) validateFields() error {
	return checkGossipFields([]gossipFieldCheck{
		{"peer_id", m.PeerID, maxGossipPeerIDLen, false},
		{"data_hub_url", m.DataHubURL, maxGossipURLLen, false},
		{"hash", m.Hash, maxGossipHashLen, true},
	})
}

// sanitizeFields bounds the display-only free-text fields of a subtree
// announcement in place.
func (m *SubtreeMessage) sanitizeFields() {
	m.ClientName = sanitizeGossipString(m.ClientName, maxGossipClientNameLen)
}

// validateFields checks the protocol-format fields of a rejected-tx message.
func (m *RejectedTxMessage) validateFields() error {
	return checkGossipFields([]gossipFieldCheck{
		{"peer_id", m.PeerID, maxGossipPeerIDLen, false},
		{"tx_id", m.TxID, maxGossipHashLen, true},
	})
}

// sanitizeFields bounds the display-only free-text fields of a rejected-tx
// message in place. The reason is validator error text and may legitimately be
// long on un-upgraded peers, so it is truncated rather than treated as a
// violation.
func (m *RejectedTxMessage) sanitizeFields() {
	m.ClientName = sanitizeGossipString(m.ClientName, maxGossipClientNameLen)
	m.Reason = sanitizeGossipString(m.Reason, maxGossipReasonLen)
}

// sanitizeGossipString strips non-printable runes (and the U+FFFD replacement
// runes strings.Map substitutes for invalid UTF-8 bytes) and truncates the
// value to maxLen bytes on a rune boundary. Used on ingress for display-only
// fields, and on egress for locally sourced free text — operator-configured
// client names, coinbase-derived miner names, validator rejection reasons — so
// our own published values always satisfy checkGossipString on remote peers.
func sanitizeGossipString(value string, maxLen int) string {
	value = strings.Map(func(r rune) rune {
		if !unicode.IsPrint(r) || r == utf8.RuneError {
			return -1
		}
		return r
	}, value)

	if len(value) <= maxLen {
		return value
	}

	cut := maxLen
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}

	return value[:cut]
}
