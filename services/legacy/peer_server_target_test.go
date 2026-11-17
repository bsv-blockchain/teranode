package legacy

import (
	"testing"

	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/services/legacy/addrmgr"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestAutomaticOutboundTarget pins that MaxPeers bounds the automatic tier and
// nothing else.
//
// Permanent peers are budgeted separately by MaxAddnodePeers, so they must not
// shrink this target — that is what makes the addnode budget additive rather
// than a share of MaxPeers, and it is how svnode arranges the same two tiers:
// its addnode semaphore is sized independently of nMaxConnections, and its
// inbound arithmetic (nMaxConnections minus outbound and feeler) never
// mentions addnode at all.
func TestAutomaticOutboundTarget(t *testing.T) {
	tests := []struct {
		name       string
		configured uint32
		maxPeers   int
		want       uint32
	}{
		{name: "ample headroom leaves the target alone", configured: 8, maxPeers: 125, want: 8},
		{name: "cap below the target binds", configured: 8, maxPeers: 5, want: 5},
		{name: "cap equal to the target", configured: 8, maxPeers: 8, want: 8},
		{name: "zero configured target stays zero", configured: 0, maxPeers: 125, want: 0},
		{name: "zero cap yields no automatic peers", configured: 8, maxPeers: 0, want: 0},
		{name: "negative cap is treated as none", configured: 8, maxPeers: -1, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, automaticOutboundTarget(tt.configured, tt.maxPeers))
		})
	}
}

// TestAddnodePeers pins the addnode budget itself.
//
// svnode enforces it with a semaphore of MaxAddnodePeers permits, so a longer
// -addnode list waits rather than growing the node without bound. Teranode's
// list is fixed at startup, so the equivalent is to dial the first budget-many
// and report the rest as ignored — silently truncating would leave an operator
// believing peers were connected that never were.
func TestAddnodePeers(t *testing.T) {
	four := []string{"a", "b", "c", "d"}

	tests := []struct {
		name        string
		configured  []string
		budget      int
		wantDial    []string
		wantDropped int
	}{
		{name: "within budget dials all", configured: four, budget: 8, wantDial: four},
		{name: "exactly at budget dials all", configured: four, budget: 4, wantDial: four},
		{name: "over budget dials the first few", configured: four, budget: 2, wantDial: []string{"a", "b"}, wantDropped: 2},
		{name: "zero budget dials none", configured: four, budget: 0, wantDial: []string{}, wantDropped: 4},
		{name: "negative budget dials none", configured: four, budget: -1, wantDial: []string{}, wantDropped: 4},
		{name: "none configured", configured: nil, budget: 8, wantDial: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dial, dropped := addnodePeers(tt.configured, tt.budget)
			require.Equal(t, tt.wantDial, dial)
			require.Equal(t, tt.wantDropped, dropped)
			require.LessOrEqual(t, len(dial), max(tt.budget, 0), "must never dial more named peers than the budget")
			require.Equal(t, len(tt.configured), len(dial)+dropped, "every configured peer is either dialed or reported dropped")
		})
	}
}

// TestCountExcludingPermanentIsAdditive pins the other half of the addnode
// budget: named peers must not eat the capacity that MaxPeers governs.
//
// The peer cap is enforced at the door in handleAddPeerMsg, so if permanent
// peers counted there, giving a node eight named peers would silently cost it
// eight inbound slots — the separate budget granted at startup and taken back
// on the first inbound connection. svnode avoids this by deriving inbound
// capacity from nMaxConnections minus the outbound and feeler budgets only,
// leaving addnode out of the sum entirely.
func TestCountExcludingPermanentIsAdditive(t *testing.T) {
	state := &peerState{
		inboundPeers:    txmap.NewSyncedMap[int32, *serverPeer](),
		outboundPeers:   txmap.NewSyncedMap[int32, *serverPeer](),
		persistentPeers: txmap.NewSyncedMap[int32, *serverPeer](),
	}

	for i := int32(0); i < 3; i++ {
		state.inboundPeers.Set(i, &serverPeer{})
	}

	for i := int32(0); i < 2; i++ {
		state.outboundPeers.Set(i, &serverPeer{})
	}

	require.Equal(t, 5, state.CountExcludingPermanent())
	require.Equal(t, 5, state.Count())

	// Named peers are additive: they raise the total the node holds without
	// drawing down the budget MaxPeers governs.
	for i := int32(0); i < 4; i++ {
		state.persistentPeers.Set(i, &serverPeer{})
	}

	require.Equal(t, 5, state.CountExcludingPermanent(),
		"permanent peers must not consume the capacity MaxPeers bounds")
	require.Equal(t, 9, state.Count(),
		"the node still holds them, and Count still reports the true total")
}

// TestOutboundGroupsCountsOnlyTheAutomaticTier pins the netgroup rule now that
// it is derived rather than maintained.
//
// The set exists so newAddressFunc will not spend several of the node's limited
// automatic slots on one network segment. Only peers holding such a slot belong
// in it: inbound peers do not hold one, and named peers have their own budget,
// so counting either would cost the node an independently chosen address for a
// slot that was never taken. svnode excludes both from its setConnected for the
// same reason.
//
// Deriving it from the peer lists is what makes that true without anyone having
// to remember it. The three lists ARE the rule, so there is no claim to skip and
// no release to forget — which is exactly how the two bugs this replaced arose.
// The peers below are all built as outbound so each has a usable address; which
// list they are filed under is the thing under test.
func TestOutboundGroupsCountsOnlyTheAutomaticTier(t *testing.T) {
	tSettings := settings.NewSettings()

	newPeer := func(t *testing.T, addr string) *serverPeer {
		t.Helper()

		p, err := peer.NewOutboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{}, addr)
		require.NoError(t, err)

		return &serverPeer{Peer: p}
	}

	state := &peerState{
		inboundPeers:    txmap.NewSyncedMap[int32, *serverPeer](),
		outboundPeers:   txmap.NewSyncedMap[int32, *serverPeer](),
		persistentPeers: txmap.NewSyncedMap[int32, *serverPeer](),
	}

	// Routable addresses in distinct /16s. Documentation ranges all collapse to
	// the same "unroutable" group key, which would make this test agree with
	// itself for the wrong reason.
	automatic := newPeer(t, "8.8.8.8:8333")
	named := newPeer(t, "1.1.1.1:8333")
	inbound := newPeer(t, "9.9.9.9:8333")

	state.outboundPeers.Set(1, automatic)
	state.persistentPeers.Set(2, named)
	state.inboundPeers.Set(3, inbound)

	groups := state.outboundGroups()

	require.Contains(t, groups, addrmgr.GroupKey(automatic.NA()),
		"an automatic outbound peer holds its netgroup")
	require.NotContains(t, groups, addrmgr.GroupKey(named.NA()),
		"a named peer has its own budget and must not cost the automatic tier a netgroup")
	require.NotContains(t, groups, addrmgr.GroupKey(inbound.NA()),
		"an inbound peer occupies no automatic slot, so it claims no netgroup")
	require.Len(t, groups, 1)
}

// TestNetgroupFreedWhenPeerDropsBeforeHandshake covers the case that used to
// leak, driven through the real handleDonePeerMsg.
//
// The old maintained tally released a peer's netgroup only if VersionKnown was
// also true, so a peer that dropped before completing its version exchange kept
// its group for the life of the process, barring that whole segment from
// automatic outbound selection. It leaked worst when peers were flakiest, which
// is when reach matters most.
//
// Deriving the set retires the question: the group is occupied for exactly as
// long as the peer is in the list, so removing the peer frees it with no release
// step to get wrong. The peer here is deliberately never handshaked, which is
// the condition the old guard tripped over.
func TestNetgroupFreedWhenPeerDropsBeforeHandshake(t *testing.T) {
	tSettings := settings.NewSettings()

	p, err := peer.NewOutboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{}, "8.8.4.4:8333")
	require.NoError(t, err)
	require.False(t, p.VersionKnown(), "the peer must be pre-handshake for this test to mean anything")

	srv := &server{logger: ulogger.TestLogger{}}
	sp := &serverPeer{Peer: p, server: srv}

	state := &peerState{
		inboundPeers:    txmap.NewSyncedMap[int32, *serverPeer](),
		outboundPeers:   txmap.NewSyncedMap[int32, *serverPeer](),
		persistentPeers: txmap.NewSyncedMap[int32, *serverPeer](),
	}

	state.outboundPeers.Set(sp.ID(), sp)

	key := addrmgr.GroupKey(sp.NA())
	require.Contains(t, state.outboundGroups(), key, "the peer should hold its netgroup while connected")

	srv.handleDonePeerMsg(state, sp)

	require.NotContains(t, state.outboundGroups(), key,
		"a peer that dropped before its handshake kept its netgroup, barring that segment from automatic outbound")

	_, stillTracked := state.outboundPeers.Get(sp.ID())
	require.False(t, stillTracked, "the peer should have been removed from the outbound list")
}

// TestPermanentPeerListSparesConnectOnly pins where the addnode budget applies.
//
// The budget is for -addnode: peers added on top of a node that is already
// dialling the network for itself, so dropping the surplus costs it nothing it
// cannot replace from the address book. Connect-only mode is the opposite
// case. cfg.ConnectPeers is the node's entire connectivity — there is no
// address source at all, and MaxPeers is set to the length of that same list —
// so capping it at eight would strand every entry past the eighth with nothing
// to fall back on, and leave the node permanently below the capacity it had
// just sized itself for.
//
// svnode draws the line in the same place: semAddnode gates
// ThreadOpenAddedConnections, while the -connect loop in ThreadOpenConnections
// dials every entry with no semaphore at all.
func TestPermanentPeerListSparesConnectOnly(t *testing.T) {
	twelve := make([]string, 12)
	for i := range twelve {
		twelve[i] = string(rune('a' + i))
	}

	t.Run("connect-only is never truncated", func(t *testing.T) {
		dial, dropped := permanentPeerList(twelve, nil, 8)

		require.Equal(t, twelve, dial,
			"connect-only peers are the node's only connectivity and must all be dialed")
		require.Zero(t, dropped)
	})

	t.Run("connect-only wins over addnode and is still not truncated", func(t *testing.T) {
		dial, dropped := permanentPeerList(twelve, []string{"x", "y"}, 8)

		require.Equal(t, twelve, dial)
		require.Zero(t, dropped)
	})

	t.Run("addnode is budgeted", func(t *testing.T) {
		dial, dropped := permanentPeerList(nil, twelve, 8)

		require.Len(t, dial, 8, "the addnode list is capped at its own budget")
		require.Equal(t, 4, dropped)
	})

	t.Run("neither configured", func(t *testing.T) {
		dial, dropped := permanentPeerList(nil, nil, 8)

		require.Empty(t, dial)
		require.Zero(t, dropped)
	})
}

// TestConnectNodeAdmitted pins that the runtime addnode path spends the same
// two budgets as the startup list and the admission door.
//
// The budgets are only additive if every path that spends them agrees. This one
// used to compare every request against MaxPeers, which broke it in both
// directions: a node holding its automatic quota plus a few named peers could
// never gain another named peer however generous MaxAddnodePeers was, and
// nothing enforced MaxAddnodePeers here at all, so an operator could walk
// straight past the startup budget by adding peers at runtime.
func TestConnectNodeAdmitted(t *testing.T) {
	const (
		maxAddnode = 8
		maxPeers   = 125
	)

	tests := []struct {
		name            string
		permanent       bool
		persistentCount int
		automaticCount  int
		want            bool
	}{
		{name: "named peer with room in its own tier", permanent: true, persistentCount: 3, want: true},
		{name: "named peer at its budget", permanent: true, persistentCount: maxAddnode},
		{name: "named peer past its budget", permanent: true, persistentCount: maxAddnode + 4},
		{
			name:            "named peer admitted even when the automatic tier is full",
			permanent:       true,
			persistentCount: 1,
			automaticCount:  maxPeers,
			want:            true,
		},
		{name: "one-shot with room", automaticCount: 10, want: true},
		{name: "one-shot at the peer cap", automaticCount: maxPeers},
		{
			name:            "one-shot admitted even when the named tier is full",
			persistentCount: maxAddnode,
			automaticCount:  10,
			want:            true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := connectNodeAdmitted(tt.permanent, tt.persistentCount, tt.automaticCount, maxAddnode, maxPeers)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestCountIPSurvivesTheDedupPath covers the ratchet that the maintained per-IP
// tally used to suffer.
//
// handleAddPeerMsg's already-connected dedup disconnects the displaced peer and
// removes it from its list on the spot. When that peer's disconnect later
// arrived, handleDonePeerMsg no longer found it in the list and took the
// fall-through branch, which decrements nothing — so the old counter went up
// and never came back down. After MaxPeersPerIP (default 5) such events the
// node refused every peer from that host for the life of the process, which is
// the same permanent, invisible shrinking of reach as the leaks elsewhere in
// this PR.
//
// Counting the peers themselves cannot ratchet, because there is nothing to
// decrement. This drives the real handleDonePeerMsg down that fall-through
// branch — the peer is deliberately absent from every list, exactly as the
// dedup leaves it — and asserts the count reflects reality either way.
func TestCountIPSurvivesTheDedupPath(t *testing.T) {
	tSettings := settings.NewSettings()

	newPeer := func(t *testing.T, addr string) *serverPeer {
		t.Helper()

		p, err := peer.NewOutboundPeer(ulogger.TestLogger{}, tSettings, &peer.Config{}, addr)
		require.NoError(t, err)

		return &serverPeer{Peer: p, server: &server{logger: ulogger.TestLogger{}}}
	}

	const host = "8.8.8.8"

	state := &peerState{
		inboundPeers:    txmap.NewSyncedMap[int32, *serverPeer](),
		outboundPeers:   txmap.NewSyncedMap[int32, *serverPeer](),
		persistentPeers: txmap.NewSyncedMap[int32, *serverPeer](),
	}

	require.Equal(t, 0, state.CountIP(host), "no peers, no connections from the host")

	displaced := newPeer(t, host+":8333")
	state.outboundPeers.Set(displaced.ID(), displaced)

	require.Equal(t, 1, state.CountIP(host))

	// What the dedup does: disconnect the old peer and drop it from the list
	// immediately, rather than letting handleDonePeerMsg unwind it.
	state.outboundPeers.Delete(displaced.ID())

	require.Equal(t, 0, state.CountIP(host),
		"a peer removed by the dedup must stop counting the moment it leaves the list")

	// Its disconnect still arrives afterwards, and finds it in no list at all.
	// Under the old maintained tally this path decremented nothing, so the
	// count stayed high for ever.
	srv := &server{logger: ulogger.TestLogger{}}
	srv.handleDonePeerMsg(state, displaced)

	require.Equal(t, 0, state.CountIP(host),
		"the late disconnect of an already-removed peer must not leave the host counted")

	// And the host is usable again, which is the behaviour that was lost.
	replacement := newPeer(t, host+":8334")
	state.outboundPeers.Set(replacement.ID(), replacement)

	require.Equal(t, 1, state.CountIP(host), "the host must still be connectable after a dedup")
}
