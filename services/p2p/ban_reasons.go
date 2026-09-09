package p2p

import (
	p2pconstants "github.com/bsv-blockchain/teranode/interfaces/p2p"
)

// Ban reason strings used by P2P-internal callsites when reporting peer
// misbehaviour to the centralized peer registry's AddBanScore RPC. The
// blockchain-side BanConfig assigns concrete penalty points to each reason;
// callers pass 0 points and rely on the config lookup.
const (
	ReasonProtocolViolation = "protocol_violation"
	ReasonInvalidSubtree    = "invalid_subtree"
	ReasonInvalidBlock      = "invalid_block"
	ReasonSpam              = "spam"
	ReasonCatchupMalicious  = "catchup_malicious"
	ReasonUnknown           = "unknown"

	// ReasonCorruptBlockBody is derived from the shared interfaces/p2p constant rather
	// than re-typed, so a rename there cannot leave this side scoring an unknown reason.
	// The same constant keys the blockchain peer registry's penalty row.
	ReasonCorruptBlockBody = string(p2pconstants.ReasonCorruptBlockBody)
)
