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

	// ReasonOperatorBan labels the prometheusP2PBanEvents metric for bans
	// applied directly by the BanPeer RPC, which bypasses AddBanScore (and
	// therefore onPeerBanned) entirely. It is a metric-only label, never
	// passed to peerRegistry.AddBanScore.
	ReasonOperatorBan = "operator_ban"
)

// knownBanReasons is the bounded set of label values prometheusP2PBanEvents
// may carry. reason on AddBanScore's wire request originates from a gRPC
// caller, so it must not be used as a metric label unchecked: an
// unrecognised value would mint an unbounded cardinality of
// teranode_p2p_ban_events_total series.
var knownBanReasons = map[string]struct{}{
	ReasonProtocolViolation: {},
	ReasonInvalidSubtree:    {},
	ReasonInvalidBlock:      {},
	ReasonSpam:              {},
	ReasonUnknown:           {},
	ReasonOperatorBan:       {},
}

// normalizeBanReasonLabel bounds a ban reason for use as a Prometheus label,
// collapsing anything outside the known set to ReasonUnknown. It must only be
// applied at the metric callsite: the registry has per-reason ban-score
// weights, so the value passed to peerRegistry.AddBanScore must stay
// untouched.
func normalizeBanReasonLabel(reason string) string {
	if _, ok := knownBanReasons[reason]; ok {
		return reason
	}

	return ReasonUnknown
}
