package p2p

import (
	"slices"
	"strings"

	"github.com/bsv-blockchain/teranode/settings"
)

// Gossip messages carry free-form strings chosen by whoever sent them. Those
// strings are relayed verbatim to every /p2p-ws subscriber and stored in the
// peer registry, which the operator dashboard renders. Bounding them at ingest
// keeps a hostile peer from pushing unbounded or markup-bearing text into an
// operator's browser, independently of how carefully any one consumer escapes.
const (
	// maxPeerDisplayStringLen bounds free-form peer-supplied display strings.
	// Matches the peer registry's own cap for client names
	// (blockchain.sanitizeClientName) so the same value is not bounded to two
	// different lengths depending on which path it took. Real values (a semver,
	// a short commit, "client/1.0", a coinbase miner tag - the genesis one is
	// 69 characters) fit inside it.
	maxPeerDisplayStringLen = 128

	// maxPeerHexStringLen bounds peer-supplied hex fields. A chain work value
	// is a 256-bit big-endian integer, so 64 hex digits is the natural maximum.
	maxPeerHexStringLen = 64

	// The storage modes a peer may advertise, as produced by
	// util.DetermineStorageMode. Anything else is treated as unknown.
	storageModeFull   = "full"
	storageModePruned = "pruned"
)

// sanitizePeerDisplayString bounds a peer-supplied string to maxLen characters
// and drops everything outside a conservative charset: printable ASCII minus
// the characters that carry meaning in HTML. Disallowed characters are removed
// rather than substituted, so the result never grows.
//
// Filtering rather than rejecting is deliberate: node_status is telemetry, and
// dropping a whole message because one peer picked an odd client name would
// lose monitoring data for that peer.
func sanitizePeerDisplayString(value string, maxLen int) string {
	if value == "" {
		return ""
	}

	var b strings.Builder

	for _, r := range value {
		if b.Len() >= maxLen {
			break
		}

		// Printable ASCII only: no control characters, no newlines (log
		// injection), no multi-byte runes that could render deceptively.
		if r < 0x20 || r > 0x7E {
			continue
		}

		// Characters that let text become markup, break out of an attribute, or
		// act as an escape once the value is embedded somewhere else.
		switch r {
		case '<', '>', '&', '"', '\'', '`', '\\':
			continue
		}

		b.WriteRune(r)
	}

	return b.String()
}

// sanitizePeerHexString keeps a peer-supplied value only if it is plausible
// hex, returning "" otherwise. Unlike the display strings this is all-or-
// nothing: a partially stripped hex number would silently mean something
// different from what the peer sent.
func sanitizePeerHexString(value string, maxLen int) string {
	if value == "" || len(value) > maxLen {
		return ""
	}

	for _, r := range value {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		case r >= 'A' && r <= 'F':
		default:
			return ""
		}
	}

	return value
}

// sanitizePeerEnum keeps a peer-supplied value only if it is one of the values
// the protocol defines, returning "" (the "unknown / old peer" encoding that
// consumers already handle) otherwise.
func sanitizePeerEnum(value string, allowed ...string) string {
	if slices.Contains(allowed, value) {
		return value
	}

	return ""
}

// sanitizeNodeStatusMessage bounds every peer-controlled string in a received
// node_status in place. It must run before the message is forwarded to
// WebSocket clients or written to the peer registry.
//
// PeerID and BaseURL are left alone: PeerID is already pinned to the sender's
// libp2p identity by the anti-spoofing check, and BaseURL has its own SSRF and
// blacklist validation. Type and PropagationURL are never read off a received
// message. BestBlockHash is handled by the caller instead: sanitizeAdvertisedTip
// must see the value the peer actually sent so it can still reject a malformed
// tip outright, so handleNodeStatusTopic bounds it where it seeds the
// notification. TestSanitizeNodeStatusMessage_CoversEveryStringField pins that
// list so a newly added field cannot be forgotten here.
func sanitizeNodeStatusMessage(msg *NodeStatusMessage) {
	msg.Version = sanitizePeerDisplayString(msg.Version, maxPeerDisplayStringLen)
	msg.CommitHash = sanitizePeerDisplayString(msg.CommitHash, maxPeerDisplayStringLen)
	msg.ClientName = sanitizePeerDisplayString(msg.ClientName, maxPeerDisplayStringLen)
	msg.MinerName = sanitizePeerDisplayString(msg.MinerName, maxPeerDisplayStringLen)
	msg.FSMState = sanitizePeerDisplayString(msg.FSMState, maxPeerDisplayStringLen)
	msg.SyncPeerID = sanitizePeerDisplayString(msg.SyncPeerID, maxPeerDisplayStringLen)

	msg.ChainWork = sanitizePeerHexString(msg.ChainWork, maxPeerHexStringLen)
	msg.SyncPeerBlockHash = sanitizePeerHexString(msg.SyncPeerBlockHash, maxPeerHexStringLen)

	msg.ListenMode = sanitizePeerEnum(msg.ListenMode,
		settings.ListenModeFull, settings.ListenModeListenOnly, settings.ListenModeSilent)
	msg.Storage = sanitizePeerEnum(msg.Storage, storageModeFull, storageModePruned)
}
