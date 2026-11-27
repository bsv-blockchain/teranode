package p2p

import (
	"context"
	"sync"
	"time"
)

const (
	// bannedPeerRefreshInterval is how long the banned-peer mirror serves
	// lookups before it re-lists the registry. It is the same window the
	// per-peer IsPeerBanned cache it replaces allowed a registry-side ban to
	// go unnoticed, so the staleness contract is unchanged; bans applied in
	// this process (onPeerBanned) are visible immediately regardless.
	bannedPeerRefreshInterval = reputationCacheTTL

	// bannedPeerRefreshTimeout bounds one ListBannedPeers round-trip so a
	// wedged registry cannot pin the gossip worker that happened to trigger
	// the refresh.
	bannedPeerRefreshTimeout = reputationCacheTTL
)

// bannedPeerMirror is a local copy of the registry's banned peer IDs, the set
// the gossip gate consults for every message. It replaces a per-author cache
// of IsPeerBanned lookups: that cache needed one registry round-trip and one
// map entry per distinct author, and the author of a gossip message is an
// identity a remote party mints offline for free, so a rotating-identity flood
// missed it on every message. The banned set, by contrast, is listed once per
// bannedPeerRefreshInterval for one round-trip however many distinct authors
// arrive in between, and a lookup for an unknown author allocates nothing.
// The set is not bounded against an attacker: each fresh identity that earns
// a ban adds an entry for the ban's duration, and every refresh copies the
// whole set.
//
// Refreshes are single-flight: the worker that finds the mirror stale
// re-lists inline, waiting up to bannedPeerRefreshTimeout, while concurrent
// workers read the previous view without waiting. Server.Start primes the
// mirror before subscribing to any topic so a peer banned before this process
// started is dropped from its very first message; should that priming fail
// or be skipped (tests that construct Server directly), lookups fail open on
// the empty view until the first refresh lands. A failed refresh keeps the last good view and is not retried
// until the next interval (fail open for bans it has not yet seen, the same
// breaker the per-peer cache applied).
//
// A ban applied in this process is added at once and remembered for one
// interval, so a refresh whose round-trip straddled it cannot drop it. Nothing
// unbans locally: a reputation reset does not lift registry bans, so it only
// invalidates the mirror, and a ban lifts when the registry stops listing it.
type bannedPeerMirror struct {
	mu          sync.RWMutex
	ids         map[string]struct{}
	recentLocal map[string]time.Time // local additions, kept across a refresh for one interval
	refreshedAt time.Time
	loaded      bool
	// gen counts invalidations. A refresh captures it before the registry
	// round-trip; replace and markAttempt do nothing if it moved, since the
	// result predates the request for a fresh listing.
	gen uint64

	// refreshMu serialises refreshes. It is held across the registry
	// round-trip, so it must never be taken while holding mu.
	refreshMu sync.Mutex
}

// contains reports whether peerID is in the mirrored banned set.
func (m *bannedPeerMirror) contains(peerID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.ids[peerID]

	return ok
}

// bannedIDs returns a copy of the mirrored banned set, for the admission
// sync (syncAdmissionBans).
func (m *bannedPeerMirror) bannedIDs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]string, 0, len(m.ids))
	for id := range m.ids {
		ids = append(ids, id)
	}

	return ids
}

// state returns whether the mirror is due a refresh at now, and the
// invalidation generation a refresh started now must hand back to replace
// and markAttempt.
func (m *bannedPeerMirror) state(now time.Time) (stale bool, gen uint64) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return !m.loaded || now.Sub(m.refreshedAt) >= bannedPeerRefreshInterval, m.gen
}

// add marks peerID banned immediately (a local ban transition).
func (m *bannedPeerMirror) add(peerID string, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.ids == nil {
		m.ids = make(map[string]struct{})
	}

	if m.recentLocal == nil {
		m.recentLocal = make(map[string]time.Time)
	}

	m.ids[peerID] = struct{}{}
	m.recentLocal[peerID] = now
}

// invalidate makes the next lookup re-list the registry and discards any
// listing already in flight, which predates the request. The current view
// stays in force until the re-list lands, so no ban goes unenforced while it
// is in flight or if it fails.
func (m *bannedPeerMirror) invalidate() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.refreshedAt = time.Time{}
	m.gen++
}

// replace installs the registry's banned set as of now, provided the mirror
// was not invalidated since gen was captured (otherwise the listing predates
// the invalidation and is discarded; refreshedAt stays cleared so the next
// lookup re-lists). Local additions younger than one refresh interval are kept: a
// ban applied here while the listing round-trip was in flight may be missing
// from the reply, and dropping it would re-open a window the immediate add
// exists to close. Returns whether the listing was installed.
func (m *bannedPeerMirror) replace(banned []string, now time.Time, gen uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.gen != gen {
		return false
	}

	ids := make(map[string]struct{}, len(banned)+len(m.recentLocal))
	for _, id := range banned {
		ids[id] = struct{}{}
	}

	for id, addedAt := range m.recentLocal {
		if now.Sub(addedAt) < bannedPeerRefreshInterval {
			ids[id] = struct{}{}
		} else {
			delete(m.recentLocal, id)
		}
	}

	m.ids = ids
	m.refreshedAt = now
	m.loaded = true

	return true
}

// markAttempt records a failed refresh so the registry is not retried until
// the next interval. The current view (empty on a first-load failure) stays
// in force. Like replace, it does nothing if gen has moved since the refresh
// captured it: an invalidation during the failed round-trip asked for a
// re-list on the next lookup, and stamping the mirror fresh would swallow it.
func (m *bannedPeerMirror) markAttempt(now time.Time, gen uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.gen != gen {
		return
	}

	m.refreshedAt = now
	m.loaded = true
}

// isRegistryBanned reports whether the registry currently bans peerID,
// answered from the banned-peer mirror. A lookup that finds the mirror stale
// (or not yet loaded) re-lists the registry inline if no other worker is
// already doing so, and otherwise answers from the current view; it never
// waits on another worker's round-trip.
func (s *Server) isRegistryBanned(peerID string) bool {
	if s.peerRegistry == nil {
		return false
	}

	if stale, _ := s.bannedPeers.state(time.Now()); stale {
		s.refreshBannedPeers(false)
	}

	return s.bannedPeers.contains(peerID)
}

// refreshBannedPeersIfStale re-lists the registry if the mirror is due a
// refresh and no other refresh is in flight, without waiting for one. The ban
// sweep calls it so a registry ban that lapses reaches the admission rules
// even while no gossip arrives to refresh the mirror.
func (s *Server) refreshBannedPeersIfStale() {
	if s.peerRegistry == nil {
		return
	}

	if stale, _ := s.bannedPeers.state(time.Now()); stale {
		s.refreshBannedPeers(false)
	}
}

// primeBannedPeers loads the banned-peer mirror once, waiting for the
// round-trip, so the gossip gate enforces pre-existing registry bans from the
// first message it sees and the admission sync that follows can block them.
// Called from Start before the topic subscriptions (startGossipIntake).
func (s *Server) primeBannedPeers() {
	if s.peerRegistry == nil {
		return
	}

	s.refreshBannedPeers(true)
}

// refreshBannedPeers re-lists the registry's banned peers into the mirror.
// With wait set the caller blocks until a refresh has happened (startup
// priming); otherwise it returns at once when another refresh is in flight,
// which is the only mode the gossip path uses.
func (s *Server) refreshBannedPeers(wait bool) {
	if wait {
		s.bannedPeers.refreshMu.Lock()
	} else if !s.bannedPeers.refreshMu.TryLock() {
		return
	}
	defer s.bannedPeers.refreshMu.Unlock()

	// Whoever held the lock before us may have just done the work.
	stale, gen := s.bannedPeers.state(time.Now())
	if !stale {
		return
	}

	parent := s.gCtx
	if parent == nil {
		parent = context.Background()
	}

	ctx, cancel := context.WithTimeout(parent, bannedPeerRefreshTimeout)
	defer cancel()

	banned, err := s.peerRegistry.ListBannedPeers(ctx)
	if err != nil {
		s.bannedPeers.markAttempt(time.Now(), gen)
		s.logger.Warnf("[refreshBannedPeers] ListBannedPeers failed (serving the previous banned set for %s): %v", bannedPeerRefreshInterval, err)

		return
	}

	if !s.bannedPeers.replace(banned, time.Now(), gen) {
		s.logger.Debugf("[refreshBannedPeers] discarded a banned-peer listing that straddled an invalidation; the next lookup re-lists")
	}
}
