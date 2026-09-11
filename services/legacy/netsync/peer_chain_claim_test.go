package netsync

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// TestPeerChainClaim covers the record that replaces an unfalsifiable integer.
//
// The old record was seeded from the height a peer advertised about itself at
// handshake and only ever rose, so every peer permanently claimed every block and
// the scheduler's canServe test could never refuse anybody. The grading here is
// what makes refusal possible: a peer's own word cannot produce proof.
func TestPeerChainClaim(t *testing.T) {
	h1 := chainhash.Hash{0x01}
	h2 := chainhash.Hash{0x02}

	t.Run("a fresh peer has proven nothing", func(t *testing.T) {
		s := &peerSyncState{}

		require.Equal(t, proofNone, s.Claim().proof)
		require.False(t, s.HasProvenTo(1),
			"a peer that has said nothing must not answer yes to any height, which is the bug this replaces")
	})

	t.Run("proof carries the height we established", func(t *testing.T) {
		s := &peerSyncState{}
		s.noteProvenClaim(h1, 721000)

		require.True(t, s.HasProvenTo(721000))
		require.True(t, s.HasProvenTo(700000))
		require.False(t, s.HasProvenTo(721001),
			"a peer proven to 721000 has said nothing about 721001")
	})

	t.Run("a proven height never lowers", func(t *testing.T) {
		s := &peerSyncState{}
		s.noteProvenClaim(h1, 721000)
		s.noteProvenClaim(h2, 700000)

		require.Equal(t, int32(721000), s.Claim().height,
			"a peer that had block N a minute ago still has it, so the record is monotone like the one it replaces")
	})

	t.Run("a pending claim never displaces proof", func(t *testing.T) {
		s := &peerSyncState{}
		s.noteProvenClaim(h1, 721000)
		s.notePendingClaim(h2)

		c := s.Claim()
		require.Equal(t, proofProven, c.proof,
			"an unresolvable hash is strictly less informative than a height we worked out")
		require.Equal(t, int32(721000), c.height)
	})

	t.Run("a pending claim carries no height at all", func(t *testing.T) {
		// This is why HasProvenTo is safe even if someone loosens its grade
		// test: a pending claim has nothing to compare. The grade and the empty
		// height are two independent reasons an unresolvable announcement cannot
		// grant permission, and this pins the second so the first is not the
		// only thing standing between a peer's word and a scheduling decision.
		s := &peerSyncState{}
		s.notePendingClaim(h2)

		c := s.Claim()
		require.Equal(t, proofPending, c.proof)
		require.Zero(t, c.height,
			"an unresolvable hash has no height by construction; recording one would be inventing it")
		require.False(t, s.HasProvenTo(1))
	})

	t.Run("proof displaces a pending claim", func(t *testing.T) {
		s := &peerSyncState{}
		s.notePendingClaim(h2)
		require.Equal(t, proofPending, s.Claim().proof)
		require.False(t, s.HasProvenTo(1), "pending is not permission")

		s.noteProvenClaim(h1, 500)
		require.True(t, s.HasProvenTo(500))
	})

	t.Run("a non-positive height proves nothing", func(t *testing.T) {
		s := &peerSyncState{}
		s.noteProvenClaim(h1, 0)
		s.noteProvenClaim(h1, -5)

		require.Equal(t, proofNone, s.Claim().proof,
			"height zero is the value every unknown carries, so accepting it would prove everything")
	})

	t.Run("a nil state answers rather than panicking", func(t *testing.T) {
		var s *peerSyncState

		require.NotPanics(t, func() {
			s.noteProvenClaim(h1, 1)
			s.notePendingClaim(h1)
			require.Equal(t, proofNone, s.Claim().proof)
			require.False(t, s.HasProvenTo(1))
		}, "tests in this package build managers as struct literals, so every accessor takes a nil receiver")
	})

	t.Run("a proven claim survives every header list rebuild", func(t *testing.T) {
		// The design decision this pins: the claim stores the height it proved
		// rather than resolving one on read. "Peer P handed us the header at
		// height N" is a fact about P and the chain, not about our current walk,
		// so wiping the walk must not falsify it. A version that resolved height
		// against headerIndex would collapse to zero at the final checkpoint,
		// when leaveHeadersFirstMode empties that index, and would then refuse
		// every peer for good.
		sm := newRaceManager(t)
		s := &peerSyncState{}
		s.noteProvenClaim(h1, 721000)
		sm.peerStates.Set(nil, s)

		sm.headerMu.Lock()
		sm.resetHeaderStateLocked(&h2, 721000)
		sm.headerMu.Unlock()

		sm.leaveHeadersFirstMode()

		require.True(t, s.HasProvenTo(721000),
			"the walk was thrown away twice and the peer still has the chain it handed us")
	})
}
