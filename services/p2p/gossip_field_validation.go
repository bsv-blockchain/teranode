package p2p

import (
	"unicode"
	"unicode/utf8"

	"github.com/bsv-blockchain/teranode/errors"
)

// Per-field bounds for peer-controlled gossip strings. The per-topic message
// caps in Server.go bound the whole payload; sanitize.go bounds display-only
// free text (filter, never reject — telemetry keeps flowing); this file bounds
// the protocol-format fields, where every honest node produces a well-defined
// shape, so a violation drops the message and scores the sender
// (applyBanScore / addProtocolViolation), same as a spoofed peer ID. The one
// exception below is maxGossipReasonLen, which is display-only.
const (
	maxGossipPeerIDLen = 128  // libp2p peer IDs are ~52 chars base58
	maxGossipHashLen   = 64   // hex-encoded 32-byte hash
	maxGossipHeaderLen = 160  // hex-encoded 80-byte block header; exact size on purpose — headers are fixed-width, so headroom would only admit junk
	maxGossipURLLen    = 2048 // DataHub / propagation URLs

	// maxGossipReasonLen bounds the rejected-tx reason (validator error text).
	// Display-only: used by RejectedTxMessage.sanitizeFields (truncates), never
	// by validateFields — a breach is not scored. It lives here rather than
	// next to maxPeerDisplayStringLen because the reason legitimately runs
	// longer than any other display string (wrapped error chains).
	maxGossipReasonLen = 1024
)

// checkGossipString rejects a peer-supplied string that exceeds maxLen bytes,
// is not valid UTF-8, or contains non-printable runes (guards log injection
// and bidi/line-separator spoofing tricks such as U+202E and U+2028). The
// offending value is never echoed into the returned error so an oversized
// field cannot inflate logs.
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
// contains a non-hexadecimal character. Empty values pass: hash fields are
// optional in gossip messages.
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
// fields use checkGossipString, hex fields (hashes, headers) use checkGossipHex.
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

// validateFields checks the protocol-format fields of a node_status message:
// the claimed peer ID (logged by the spoof check) and the URLs, which
// otherwise have no length bound. Display strings, hex telemetry, and enums
// are handled by sanitizeNodeStatusMessage (sanitize.go), which filters
// rather than rejects. Handlers call it after sanitizing, before the message
// reaches the notification channel, the peer registry, or the logs.
func (m *NodeStatusMessage) validateFields() error {
	return checkGossipFields([]gossipFieldCheck{
		{"peer_id", m.PeerID, maxGossipPeerIDLen, false},
		{"base_url", m.BaseURL, maxGossipURLLen, false},
		{"propagation_url", m.PropagationURL, maxGossipURLLen, false},
	})
}

// validateFields checks the protocol-format fields of a block announcement.
// Coinbase is deliberately NOT validated: no Teranode version has ever
// populated it, nothing consumes it, and scoring it would ban another
// implementation that encodes it differently (e.g. base64). Its size is
// bounded by maxBlockMessageSize, which keeps headroom for it.
func (m *BlockMessage) validateFields() error {
	return checkGossipFields([]gossipFieldCheck{
		{"peer_id", m.PeerID, maxGossipPeerIDLen, false},
		{"data_hub_url", m.DataHubURL, maxGossipURLLen, false},
		{"hash", m.Hash, maxGossipHashLen, true},
		{"header", m.Header, maxGossipHeaderLen, true},
	})
}

// sanitizeFields bounds the display-only free-text fields of a block
// announcement in place, using the shared sanitizer from sanitize.go.
func (m *BlockMessage) sanitizeFields() {
	m.ClientName = sanitizePeerDisplayString(m.ClientName, maxPeerDisplayStringLen)
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
	m.ClientName = sanitizePeerDisplayString(m.ClientName, maxPeerDisplayStringLen)
}

// validateFields checks the protocol-format fields of a rejected-tx message.
func (m *RejectedTxMessage) validateFields() error {
	return checkGossipFields([]gossipFieldCheck{
		{"peer_id", m.PeerID, maxGossipPeerIDLen, false},
		{"tx_id", m.TxID, maxGossipHashLen, true},
	})
}

// sanitizeFields bounds the display-only free-text fields of a rejected-tx
// message in place. The reason is validator error text and may legitimately
// be long, so it gets a larger bound than other display strings and is
// truncated rather than treated as a violation.
func (m *RejectedTxMessage) sanitizeFields() {
	m.ClientName = sanitizePeerDisplayString(m.ClientName, maxPeerDisplayStringLen)
	m.Reason = sanitizePeerDisplayString(m.Reason, maxGossipReasonLen)
}
