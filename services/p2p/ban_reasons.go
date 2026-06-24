package p2p

// Ban reason strings used by P2P-internal callsites when reporting peer
// misbehaviour to the centralized peer registry's AddBanScore RPC. The
// blockchain-side BanConfig assigns concrete penalty points to each reason;
// callers pass 0 points and rely on the config lookup.
const (
	ReasonProtocolViolation = "protocol_violation"
	ReasonInvalidSubtree    = "invalid_subtree"
	ReasonInvalidBlock      = "invalid_block"
	ReasonSpam              = "spam"
	ReasonUnknown           = "unknown"
)
