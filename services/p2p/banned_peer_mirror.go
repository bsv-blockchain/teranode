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
// missed it on every message. The banned set, by contrast, is small, bounded
// by the registry rather than by the attacker, and cheap to list: refreshing
// it once per bannedPeerRefreshInterval costs one round-trip however many
// distinct authors arrive in between, and a lookup for an unknown author
// allocates nothing.
//
// Refreshes are single-flight and, after the first load, never block a
// lookup: the worker that finds the mirror stale re-lists inline while
// concurrent workers read the previous view. The first load is awaited by
// everyone so a peer banned before this process started is dropped from its
// very first message. A failed refresh keeps the last good view and is not
// retried until the next interval (fail open for bans it has not yet seen,
// the same breaker the per-peer cache applied).
//
// Local ban transitions are added directly and remembered for one interval so
// a refresh whose round-trip straddled the transition cannot drop them.
type bannedPeerMirror struct {
	mu          sync.RWMutex
	ids         map[string]struct{}
	recentLocal map[string]time.Time // local additions, kept across a refresh for one interval
	refreshedAt time.Time
	loaded      bool

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

// state returns whether the mirror has ever been loaded and whether it is due
// a refresh at now.
func (m *bannedPeerMirror) state(now time.Time) (loaded, stale bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.loaded, !m.loaded || now.Sub(m.refreshedAt) >= bannedPeerRefreshInterval
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

// remove drops peerID from the mirror (an unban) and forces a refresh on the
// next lookup so the registry's view is re-read rather than trusted stale.
func (m *bannedPeerMirror) remove(peerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.ids, peerID)
	delete(m.recentLocal, peerID)
	m.refreshedAt = time.Time{}
}

// clear empties the mirror (all bans reset) and forces a refresh on the next
// lookup.
func (m *bannedPeerMirror) clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ids = nil
	m.recentLocal = nil
	m.refreshedAt = time.Time{}
}

// invalidate makes the next lookup refresh the mirror from the registry.
func (m *bannedPeerMirror) invalidate() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.refreshedAt = time.Time{}
}

// replace installs the registry's banned set as of now, keeping any local
// additions younger than one refresh interval: a ban applied here while the
// listing round-trip was in flight may be missing from the reply, and dropping
// it would re-open a window the immediate add exists to close.
func (m *bannedPeerMirror) replace(banned []string, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

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
}

// markAttempt records a failed refresh so the registry is not retried until
// the next interval. The current view (empty on a first-load failure) stays
// in force.
func (m *bannedPeerMirror) markAttempt(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.refreshedAt = now
	m.loaded = true
}

// isRegistryBanned reports whether the registry currently bans peerID,
// answered from the banned-peer mirror. The first lookup loads the mirror
// synchronously; afterwards a lookup that finds it older than
// bannedPeerRefreshInterval re-lists the registry inline if no other worker is
// already doing so, and otherwise answers from the current view.
func (s *Server) isRegistryBanned(peerID string) bool {
	if s.peerRegistry == nil {
		return false
	}

	now := time.Now()

	if loaded, stale := s.bannedPeers.state(now); stale {
		s.refreshBannedPeers(!loaded)
	}

	return s.bannedPeers.contains(peerID)
}

// refreshBannedPeers re-lists the registry's banned peers into the mirror.
// With wait set the caller blocks until a refresh has happened (first load);
// otherwise it returns at once when another refresh is in flight.
func (s *Server) refreshBannedPeers(wait bool) {
	if wait {
		s.bannedPeers.refreshMu.Lock()
	} else if !s.bannedPeers.refreshMu.TryLock() {
		return
	}
	defer s.bannedPeers.refreshMu.Unlock()

	// Whoever held the lock before us may have just done the work.
	if _, stale := s.bannedPeers.state(time.Now()); !stale {
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
		s.bannedPeers.markAttempt(time.Now())
		s.logger.Warnf("[refreshBannedPeers] ListBannedPeers failed (serving the previous banned set for %s): %v", bannedPeerRefreshInterval, err)

		return
	}

	s.bannedPeers.replace(banned, time.Now())
}
