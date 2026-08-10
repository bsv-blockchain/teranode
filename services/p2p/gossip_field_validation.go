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
// is not valid UTF-8, or contains control characters (guards log injection and
// makes the value safe to relay). Non-ASCII printable UTF-8 is allowed: miner
// names extracted from coinbase tags legitimately contain it, and banning the
// relaying peer for third-party coinbase content would let a miner get honest
// nodes banned network-wide. The offending value is never echoed into the
// returned error so an oversized field cannot inflate logs.
func checkGossipString(field, value string, maxLen int) error {
	if len(value) > maxLen {
		return errors.NewInvalidArgumentError("%s length %d exceeds max %d", field, len(value), maxLen)
	}

	if !utf8.ValidString(value) {
		return errors.NewInvalidArgumentError("%s is not valid UTF-8", field)
	}

	for _, r := range value {
		if unicode.IsControl(r) {
			return errors.NewInvalidArgumentError("%s contains a control character", field)
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

// gossipFieldCheck describes one string field to validate: free-text fields use
// checkGossipString, hex fields (hashes, chainwork, headers) use checkGossipHex.
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

// validateFields bounds every peer-controlled string field of a node_status
// message. Handlers call it straight after the peer-ID spoof check, before the
// message reaches the notification channel, the peer registry, or the logs.
func (m *NodeStatusMessage) validateFields() error {
	return checkGossipFields([]gossipFieldCheck{
		{"peer_id", m.PeerID, maxGossipPeerIDLen, false},
		{"client_name", m.ClientName, maxGossipClientNameLen, false},
		{"type", m.Type, maxGossipTypeLen, false},
		{"base_url", m.BaseURL, maxGossipURLLen, false},
		{"propagation_url", m.PropagationURL, maxGossipURLLen, false},
		{"version", m.Version, maxGossipVersionLen, false},
		{"commit_hash", m.CommitHash, maxGossipCommitHashLen, false},
		{"best_block_hash", m.BestBlockHash, maxGossipHashLen, true},
		{"fsm_state", m.FSMState, maxGossipFSMStateLen, false},
		{"miner_name", m.MinerName, maxGossipMinerNameLen, false},
		{"listen_mode", m.ListenMode, maxGossipListenModeLen, false},
		{"chain_work", m.ChainWork, maxGossipChainWorkLen, true},
		{"sync_peer_id", m.SyncPeerID, maxGossipPeerIDLen, false},
		{"sync_peer_block_hash", m.SyncPeerBlockHash, maxGossipHashLen, true},
		{"storage", m.Storage, maxGossipStorageLen, false},
	})
}

// validateFields bounds every peer-controlled string field of a block
// announcement. Coinbase is charset-checked only: Teranode never populates it
// and nothing consumes it, so its length is bounded by maxBlockMessageSize.
func (m *BlockMessage) validateFields() error {
	return checkGossipFields([]gossipFieldCheck{
		{"peer_id", m.PeerID, maxGossipPeerIDLen, false},
		{"client_name", m.ClientName, maxGossipClientNameLen, false},
		{"data_hub_url", m.DataHubURL, maxGossipURLLen, false},
		{"hash", m.Hash, maxGossipHashLen, true},
		{"header", m.Header, maxGossipHeaderLen, true},
		{"coinbase", m.Coinbase, maxBlockMessageSize, true},
	})
}

// validateFields bounds every peer-controlled string field of a subtree
// announcement.
func (m *SubtreeMessage) validateFields() error {
	return checkGossipFields([]gossipFieldCheck{
		{"peer_id", m.PeerID, maxGossipPeerIDLen, false},
		{"client_name", m.ClientName, maxGossipClientNameLen, false},
		{"data_hub_url", m.DataHubURL, maxGossipURLLen, false},
		{"hash", m.Hash, maxGossipHashLen, true},
	})
}

// validateFields bounds every peer-controlled string field of a rejected-tx
// message.
func (m *RejectedTxMessage) validateFields() error {
	return checkGossipFields([]gossipFieldCheck{
		{"peer_id", m.PeerID, maxGossipPeerIDLen, false},
		{"client_name", m.ClientName, maxGossipClientNameLen, false},
		{"tx_id", m.TxID, maxGossipHashLen, true},
		{"reason", m.Reason, maxGossipReasonLen, false},
	})
}

// sanitizeGossipString strips control characters and truncates the value to
// maxLen bytes on a rune boundary. Applied on egress (our own published
// messages) so locally sourced free text — operator-configured client names,
// coinbase-derived miner names, validator rejection reasons — always passes the
// ingress validation of remote peers running the same bounds.
func sanitizeGossipString(value string, maxLen int) string {
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
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
