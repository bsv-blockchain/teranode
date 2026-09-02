package errors

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPublicCauseAllowlistIsClosed pins the exact membership of publicCauseCodes.
//
// Every code on this list has its message surfaced verbatim to external HTTP and
// gRPC clients by DeepestPublicCause, so adding one widens what a node tells an
// unauthenticated submitter. The bar is stated on publicCauseCodes itself: the
// code must be a verdict about the submitted transaction, carry no node-internal
// state, and be actionable by the submitter.
//
// This test does not check that bar — no unit test can, since the message text is
// chosen by whichever package constructs the error. It exists so that widening the
// list cannot happen silently: an addition fails here and the author has to come
// and read the paragraph above. Pair it with the per-formatter checks that pin
// what the messages actually contain, e.g.
// TestPublicVerdictMessagesOmitInternalBatchID in stores/utxo/aerospike.
func TestPublicCauseAllowlistIsClosed(t *testing.T) {
	want := []ERR{
		ERR_UTXO_NON_FINAL,
		ERR_TX_LOCK_TIME,
		ERR_TX_POLICY,
		ERR_TX_INVALID,
		ERR_TX_INVALID_DOUBLE_SPEND,
		ERR_TX_CONFLICTING,
		ERR_UTXO_SPENT,
		ERR_TX_LOCKED,
		ERR_TX_CREATING,
		ERR_UTXO_FROZEN,
		ERR_TX_MISSING_PARENT,
	}

	got := make([]ERR, 0, len(publicCauseCodes))
	for code := range publicCauseCodes {
		got = append(got, code)
	}

	require.ElementsMatch(t, want, got,
		"publicCauseCodes changed: every code here has its message surfaced to external clients, "+
			"so confirm the addition meets the bar documented on publicCauseCodes before updating this list")

	for _, code := range want {
		require.True(t, isPublicCause(code), "%s should be an allowlisted public cause", code)
	}
}
