package p2p

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/stretchr/testify/require"
)

// A ban applied locally while a registry listing was in flight must survive
// the listing being installed without it.
func TestBannedPeerMirror_LocalBanSurvivesStraddlingRefresh(t *testing.T) {
	var m bannedPeerMirror
	now := time.Now()
	pid := mustNewPeerID(t).String()

	m.add(pid, now)
	_, gen := m.state(now)
	require.True(t, m.replace(nil, now.Add(time.Millisecond), gen))
	require.True(t, m.contains(pid), "a fresh local ban must survive a refresh that predates it")

	require.True(t, m.replace(nil, now.Add(bannedPeerRefreshInterval), gen))
	require.False(t, m.contains(pid), "once the registry has had a full interval to report it, the registry's view wins")
}

// A listing that was in flight when the mirror was invalidated predates the
// request for a fresh one, so it must be discarded and leave the mirror due a
// re-list. The view in force before the invalidation keeps enforcing until
// that re-list lands.
func TestBannedPeerMirror_InvalidateDiscardsStraddlingListing(t *testing.T) {
	var m bannedPeerMirror
	now := time.Now()
	pid := mustNewPeerID(t).String()

	_, gen := m.state(now)
	require.True(t, m.replace([]string{pid}, now, gen))
	require.True(t, m.contains(pid))

	// A refresh starts (captures gen), the mirror is invalidated, then the
	// refresh's listing arrives.
	_, inFlight := m.state(now)
	m.invalidate()
	require.False(t, m.replace(nil, now.Add(time.Millisecond), inFlight), "a listing that straddled an invalidation must be discarded")
	require.True(t, m.contains(pid), "the previous view stays in force until a re-list lands")

	stale, next := m.state(now.Add(time.Millisecond))
	require.True(t, stale, "the discarded listing must leave the mirror due a re-list")

	// The re-list, started after the invalidation, is installed normally.
	require.True(t, m.replace(nil, now.Add(2*time.Millisecond), next))
	require.False(t, m.contains(pid))
}

// The gossip path never waits on a refresh another worker is running: while
// one lookup holds the round-trip, concurrent lookups answer from the current
// view at once, even before the first load has landed.
func TestIsRegistryBanned_NeverWaitsOnInFlightRefresh(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)

	// Simulate a refresh stuck in its registry round-trip.
	s.bannedPeers.refreshMu.Lock()
	defer s.bannedPeers.refreshMu.Unlock()

	done := make(chan bool, 1)
	go func() { done <- s.isRegistryBanned(mustNewPeerID(t).String()) }()

	select {
	case banned := <-done:
		require.False(t, banned, "an unknown author fails open while the first load is in flight")
	case <-time.After(2 * time.Second):
		t.Fatal("isRegistryBanned blocked behind another worker's refresh")
	}
}

// Start primes the mirror so a peer banned before the process started is
// dropped from its first message, without the hot path having to wait.
func TestPrimeBannedPeers_LoadsPreexistingBans(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	banPeerInRegistry(t, reg, pid)

	counting := newCountingRegistryClient(s.peerRegistry)
	s.peerRegistry = counting

	s.primeBannedPeers()

	require.Equal(t, 1, counting.callCount("ListBannedPeers"))
	require.True(t, s.shouldSkipBannedPeer(pid.String(), "test"), "a pre-existing ban is enforced from the first message")
	require.Equal(t, 1, counting.callCount("ListBannedPeers"), "the primed mirror serves the first lookup without another round-trip")
}

// A failed refresh keeps the last good banned set in force and is not retried
// until the next interval: one registry fault must neither unban every peer
// nor turn each lookup into a round-trip.
func TestRefreshBannedPeers_FailureKeepsLastGoodSet(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	banPeerInRegistry(t, reg, pid)

	counting := newCountingRegistryClient(s.peerRegistry)
	s.peerRegistry = counting

	s.primeBannedPeers()
	require.True(t, s.isRegistryBanned(pid.String()), "precondition: the primed mirror holds the ban")
	require.Equal(t, 1, counting.callCount("ListBannedPeers"))

	counting.failListBannedPeers = errors.NewServiceError("registry unavailable")
	s.bannedPeers.invalidate()

	require.True(t, s.isRegistryBanned(pid.String()), "a failed refresh must keep the last good banned set")
	require.Equal(t, 2, counting.callCount("ListBannedPeers"), "the stale mirror attempted one refresh")

	require.True(t, s.isRegistryBanned(pid.String()))
	require.Equal(t, 2, counting.callCount("ListBannedPeers"), "a failed refresh is not retried until the next interval")
}

// A refresh that fails while a reset happens must not stamp the mirror fresh:
// the reset asked for a re-list on the next lookup, and the failed listing
// predates it.
func TestRefreshBannedPeers_FailureAcrossResetStillReLists(t *testing.T) {
	s, reg := newServerWithLocalRegistry(t)
	pid := mustNewPeerID(t)
	banPeerInRegistry(t, reg, pid)

	counting := newCountingRegistryClient(s.peerRegistry)
	s.peerRegistry = counting
	s.primeBannedPeers()
	require.Equal(t, 1, counting.callCount("ListBannedPeers"))

	entered := make(chan struct{})
	release := make(chan struct{})
	counting.onListBannedPeers = func() {
		close(entered)
		<-release
	}
	counting.failListBannedPeers = errors.NewServiceError("registry unavailable")
	s.bannedPeers.invalidate()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.isRegistryBanned(pid.String())
	}()

	<-entered

	_, err := s.ResetReputation(context.Background(), &p2p_api.ResetReputationRequest{PeerId: pid.String()})
	require.NoError(t, err)

	close(release)
	<-done

	counting.onListBannedPeers = nil
	counting.failListBannedPeers = nil

	stale, _ := s.bannedPeers.state(time.Now())
	require.True(t, stale, "a refresh that failed across a reset must not stamp the mirror fresh")

	require.True(t, s.isRegistryBanned(pid.String()), "the re-list after the reset reads the registry, which still bans the peer")
	require.Equal(t, 3, counting.callCount("ListBannedPeers"), "the lookup after the reset re-lists")
}
