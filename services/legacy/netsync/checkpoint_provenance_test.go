package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/stretchr/testify/require"
)

// peerAdvertised is the provenance handleInvMsg records: we are fetching the
// block only because a peer said it exists.
var peerAdvertised = blockRequestOrigin{}

// headerProven is the provenance fetchHeaderBlocks records: the hash came from a
// header run verified to terminate at a pinned checkpoint hash.
var headerProven = blockRequestOrigin{headerProven: true}

// provenanceSyncManager builds the minimal SyncManager needed to exercise
// quickValidationAllowed. The gate consults nothing but the chain params and the
// provenance it is handed — no blockchain client, UTXO store, settings or request
// map — which is the point: it cannot be fooled by, or made to depend on, the
// state of anything else.
func provenanceSyncManager(t *testing.T) *SyncManager {
	t.Helper()

	return &SyncManager{chainParams: &chaincfg.MainNetParams}
}

// TestQuickValidationAllowed_DeniesPeerAdvertisedBlock is the regression test
// for GHSA-gggq-8f59-4jm9. The reported attack is exactly this shape: a peer
// advertises an unknown block by inv, we request it (which is the only
// unsolicited-block check on the path), and it claims a height below the highest
// hardcoded checkpoint. Before the fix the gate was a pure height test, so the
// block was granted checkpoint trust: script validation was skipped and its
// transactions spent real UTXOs.
//
// A peer-advertised request carries no proof of anything. The gate must deny it.
func TestQuickValidationAllowed_DeniesPeerAdvertisedBlock(t *testing.T) {
	sm := provenanceSyncManager(t)

	require.False(t, sm.quickValidationAllowed(peerAdvertised, 944_999),
		"a peer-advertised below-checkpoint block must never be granted checkpoint trust")
}

// TestQuickValidationAllowed_AllowsHeaderProvenBlock is the other half of the
// contract: initial block download must keep the fast path, or mainnet sync
// regresses to full script validation over the whole certified prefix.
//
// fetchHeaderBlocks only ever requests hashes from a header run that
// handleHeadersMsg verified links back to a block we already trust and forward
// to a pinned checkpoint hash. That run IS the proof of checkpoint ancestry.
func TestQuickValidationAllowed_AllowsHeaderProvenBlock(t *testing.T) {
	sm := provenanceSyncManager(t)

	require.True(t, sm.quickValidationAllowed(headerProven, 944_999),
		"a block requested from a checkpoint-terminated header run must keep the fast path")
}

// TestQuickValidationAllowed_DeniesEveryHeightInCertifiedPrefix — the pure
// height test was true for EVERY height in 1..945000, which is what made the
// prefix forgeable. Sample across it to pin that provenance, not height, decides.
func TestQuickValidationAllowed_DeniesEveryHeightInCertifiedPrefix(t *testing.T) {
	sm := provenanceSyncManager(t)

	for _, h := range []uint32{1, 11_111, 500_000, 944_998, 944_999, 945_000} {
		require.False(t, sm.quickValidationAllowed(peerAdvertised, h),
			"height %d is inside the certified prefix but carries no ancestry proof", h)
		require.True(t, sm.quickValidationAllowed(headerProven, h),
			"height %d is inside the certified prefix and is header-proven", h)
	}
}

// TestQuickValidationAllowed_DeniesAboveCheckpoint — above the highest
// checkpoint there is no certified prefix to appeal to, so provenance is
// irrelevant and the answer is always no.
func TestQuickValidationAllowed_DeniesAboveCheckpoint(t *testing.T) {
	sm := provenanceSyncManager(t)

	require.False(t, sm.quickValidationAllowed(headerProven, 945_001),
		"height above the highest checkpoint must never fast-path, however it was requested")
	require.False(t, sm.quickValidationAllowed(headerProven, 0),
		"genesis must never fast-path")
}

// TestQuickValidationAllowed_FailsClosedWithoutChainParams — with no chain params
// there are no checkpoints to appeal to, so the gate must deny rather than panic.
// The cost of a false negative is slower but fully correct validation; the cost of
// a false positive is an unauthenticated peer writing forged spends.
func TestQuickValidationAllowed_FailsClosedWithoutChainParams(t *testing.T) {
	noParams := &SyncManager{}
	require.False(t, noParams.quickValidationAllowed(headerProven, 944_999),
		"no chain params means nothing is certified")
}

// TestQuickValidationAllowed_DeniesOnNetworkWithoutCheckpoints — regtest defines
// no checkpoints, so nothing is certified and the fast path cannot apply.
func TestQuickValidationAllowed_DeniesOnNetworkWithoutCheckpoints(t *testing.T) {
	sm := &SyncManager{chainParams: &chaincfg.RegressionNetParams}

	require.False(t, sm.quickValidationAllowed(headerProven, 1),
		"a network with no checkpoints has no certified prefix")
}

// TestBlockRequestOrigin_ZeroValueIsUntrusted pins the safety property that
// makes the request map safe to extend: the zero value must mean "no
// provenance". Any new call site that records a request without thinking about
// provenance therefore gets the safe answer by default.
func TestBlockRequestOrigin_ZeroValueIsUntrusted(t *testing.T) {
	var origin blockRequestOrigin

	require.False(t, origin.headerProven,
		"the zero value of blockRequestOrigin must be untrusted, so new call sites fail closed")
}
