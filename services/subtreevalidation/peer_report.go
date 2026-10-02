package subtreevalidation

import "strings"

// LegacyPeerIDPrefix is the namespace legacy netsync puts on a serving peer's id,
// "legacy:<address>". It must equal blockvalidation.LegacyPeerIDPrefix, which is the one netsync
// builds ids with; this package cannot import blockvalidation, which imports it, so it holds a copy
// and TestLegacyPeerIDPrefixMatchesBlockValidation keeps the two identical.
const LegacyPeerIDPrefix = "legacy:"

// reportsValidSubtreeTo reports whether a validated subtree should credit peerID's reputation
// with the P2P service.
//
// Not for a legacy peer. Reputation is a P2P concept, and the P2P service cannot act on a
// "legacy:" id (the same reason block validation keeps such ids away from ban scores; see
// blockvalidation's isLegacyPeerID). The call is not free either: it is made on the validation
// path, and with P2P switched off, as it is on a node syncing over the legacy protocol, it waited
// about ten seconds for a connection before failing, once per subtree, holding up the block.
func reportsValidSubtreeTo(peerID string) bool {
	return peerID != "" && !strings.HasPrefix(peerID, LegacyPeerIDPrefix)
}
