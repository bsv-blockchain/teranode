package p2p

import p2pconstants "github.com/bsv-blockchain/teranode/interfaces/p2p"

// Ban reason strings used by P2P-internal callsites when reporting peer
// misbehaviour to the centralized peer registry's AddBanScore RPC. The
// blockchain-side BanConfig assigns concrete penalty points to each reason;
// callers pass 0 points and rely on the config lookup.
const (
	ReasonProtocolViolation = "protocol_violation"
	ReasonInvalidSubtree    = "invalid_subtree"
	ReasonInvalidBlock      = "invalid_block"
	// ReasonCorruptBlockBody re-exports the single source of truth in interfaces/p2p so
	// dependency-free callers can reference the corrupt-body strike reason without importing this
	// server package (bitcoin-sv/teranode#4692). Same value: "corrupt_block_body".
	ReasonCorruptBlockBody = string(p2pconstants.ReasonCorruptBlockBody)
	ReasonSpam             = "spam"
	ReasonUnknown          = "unknown"
)
