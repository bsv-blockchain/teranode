package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jellydator/ttlcache/v3"
	"github.com/stretchr/testify/require"
)

// TestBlockAttemptCaps_EmptyPeerIDFailsOpenAndBucketsPerPeer pins the per-(hash, peerID) attempt
// caps (bitcoin-sv/teranode#4692): an empty peerID is NEVER capped (an unidentified delivery must
// never gate an honest tip), and two distinct peer identities occupy distinct buckets so exhausting
// one leaves the other's delivery ungated. This is why the legacy route must plumb a real per-peer
// identity (peer.Addr()) rather than a shared "" — see the netsync ProcessBlock plumbing test.
func TestBlockAttemptCaps_EmptyPeerIDFailsOpenAndBucketsPerPeer(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.MaxCorruptAttemptsPerBlock = 3

	u := &Server{
		settings:                   tSettings,
		blockCorruptAttempts:       ttlcache.New[blockAttemptKey, int](),
		blockPolicyDeclineAttempts: ttlcache.New[blockAttemptKey, int](),
	}

	hash := chainhash.Hash{0x01}

	t.Run("corrupt cap: empty peerID never exhausts, distinct peers bucket separately", func(t *testing.T) {
		// Hammer the empty-peerID key well past the cap: it must stay unexhausted (fail-open).
		for i := 0; i < 10; i++ {
			u.recordCorruptAttempt(&hash, "")
		}
		require.False(t, u.corruptAttemptsExhausted(&hash, ""),
			"an empty peerID must never be capped — an unidentified delivery cannot wedge an honest tip")

		// Drive peer A to the cap; A is exhausted, B (a distinct identity) is untouched.
		for i := 0; i < 3; i++ {
			u.recordCorruptAttempt(&hash, "peer-A")
		}
		require.True(t, u.corruptAttemptsExhausted(&hash, "peer-A"),
			"peer A reached the corrupt cap")
		require.False(t, u.corruptAttemptsExhausted(&hash, "peer-B"),
			"capping peer A must leave peer B's delivery ungated (distinct per-peer bucket)")
	})

	t.Run("policy-decline cap: empty peerID never exhausts, distinct peers bucket separately", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			u.recordPolicyDeclineAttempt(&hash, "")
		}
		require.False(t, u.policyDeclineAttemptsExhausted(&hash, ""),
			"an empty peerID must never be capped on the policy-decline budget either")

		for i := 0; i < 3; i++ {
			u.recordPolicyDeclineAttempt(&hash, "peer-A")
		}
		require.True(t, u.policyDeclineAttemptsExhausted(&hash, "peer-A"))
		require.False(t, u.policyDeclineAttemptsExhausted(&hash, "peer-B"),
			"capping peer A must leave peer B ungated on the policy-decline budget")
	})
}
