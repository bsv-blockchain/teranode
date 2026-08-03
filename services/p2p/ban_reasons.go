package p2p

// Ban reason strings used by P2P-internal callsites when reporting peer
// misbehaviour to the centralized peer registry's AddBanScore RPC. The
// blockchain-side BanConfig assigns concrete penalty points to each reason;
// callers pass 0 points and rely on the config lookup.
const (
	ReasonProtocolViolation = "protocol_violation"
	ReasonInvalidSubtree    = "invalid_subtree"
	ReasonInvalidBlock      = "invalid_block"
	// ReasonCorruptBlockBody scores a peer that served a corrupt block body for a valid
	// block hash (bitcoin-sv/teranode#4692). Unlike ReasonInvalidBlock the block is NOT persisted
	// invalid; the peer is struck and the body re-downloaded, mirroring svnode's
	// CorruptionOrDoS stance of punishing the connection without failing the header.
	ReasonCorruptBlockBody = "corrupt_block_body"
	ReasonSpam             = "spam"
	ReasonUnknown          = "unknown"
)
