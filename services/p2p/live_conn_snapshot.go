package p2p

import (
	"sync"
	"time"
)

const (
	// liveConnSnapshotTTL is how long a snapshot of the open libp2p connections
	// serves a hit before it is rebuilt. It is the same staleness the per-peer
	// liveness cache it replaces allowed: a peer that disconnected can look
	// live for up to this long, and the reconcile sweep corrects the registry
	// within one cleanup interval.
	liveConnSnapshotTTL = reputationCacheTTL

	// liveConnMissRefreshInterval bounds how often a lookup MISS may rebuild
	// the snapshot. A miss is the common case for honest gossip — most authors
	// are relayed publishers we never dialled — and the only case an attacker
	// can produce at will by rotating identities, so it must not cost a
	// GetPeers walk each time: the walk is linear in every author the message
	// bus has ever tracked, not just in open connections. Rebuilding at most
	// once per interval bounds that work to one walk per interval however many
	// novel authors arrive, while a neighbour that has just connected is still
	// seen live on its first message in the quiet case and within one interval
	// under load.
	liveConnMissRefreshInterval = time.Second
)

// liveConnSnapshot is the gossip hot path's view of which peers have an open
// libp2p connection, and at which addresses: peer ID -> connected multiaddrs,
// live peers only. It replaces three per-author caches (liveness, IP-ban
// verdict, and the GetPeers walk each of them ran on a miss) with one
// structure whose size is bounded by the node's open connections rather than
// by how many distinct authors it has heard from, so a message from a
// never-before-seen identity allocates nothing here.
//
// The zero value is an untaken snapshot; the first lookup builds it.
type liveConnSnapshot struct {
	mu      sync.Mutex
	addrs   map[string][]string
	takenAt time.Time
	taken   bool
}

// invalidate forces the next lookup to rebuild the snapshot.
func (l *liveConnSnapshot) invalidate() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.taken = false
}

// liveConnAddrs reports whether peerID has an open libp2p connection and, if
// so, the multiaddrs of those connections. Liveness comes from
// P2PClient.GetPeers(): Addrs is built from the host's open connections while
// the listing itself covers every author the message bus has tracked, so a
// non-empty Addrs is what separates a live neighbour from a gossip-only
// publisher (see snapshotLiveConnIDs).
//
// A hit is served from the current snapshot while it is younger than
// liveConnSnapshotTTL. A miss rebuilds the snapshot first — so a neighbour that
// connected after the last build is found — unless one was built within
// liveConnMissRefreshInterval, which is what keeps a flood of unknown authors
// from turning every message into a walk. The mutex is held across the walk:
// concurrent lookups wait for the one rebuild rather than each running their
// own.
func (s *Server) liveConnAddrs(peerID string) ([]string, bool) {
	if s.P2PClient == nil {
		return nil, false
	}

	now := time.Now()

	s.liveConns.mu.Lock()
	defer s.liveConns.mu.Unlock()

	if s.liveConns.taken {
		age := now.Sub(s.liveConns.takenAt)

		if age < liveConnSnapshotTTL {
			if addrs, ok := s.liveConns.addrs[peerID]; ok {
				return addrs, true
			}

			if age < liveConnMissRefreshInterval {
				return nil, false
			}
		}
	}

	s.rebuildLiveConnsLocked(now)

	addrs, ok := s.liveConns.addrs[peerID]

	return addrs, ok
}

// rebuildLiveConnsLocked walks P2PClient.GetPeers() once and stores the peers
// with open connections. Callers must hold liveConns.mu.
func (s *Server) rebuildLiveConnsLocked(now time.Time) {
	peers := s.P2PClient.GetPeers()
	addrs := make(map[string][]string, len(peers))

	for _, p := range peers {
		if len(p.Addrs) > 0 {
			addrs[p.ID] = p.Addrs
		}
	}

	s.liveConns.addrs = addrs
	s.liveConns.takenAt = now
	s.liveConns.taken = true
}

// hasLiveConnection reports whether the peer has an open libp2p connection,
// answered from the live-connection snapshot (see liveConnAddrs for its
// freshness contract). updatePeerLastMessageTime uses it so gossip-relayed
// publishers are never marked IsConnected while a freshly connected neighbour
// is flagged on its first message.
func (s *Server) hasLiveConnection(peerID string) bool {
	_, live := s.liveConnAddrs(peerID)

	return live
}

// snapshotLiveConnIDs rebuilds the live-connection snapshot and returns the
// set of peer IDs with an open libp2p connection. Liveness comes from
// P2PClient.GetPeers(): verified against go-p2p-message-bus v0.1.23
// (client.go GetPeers), Addrs is built from host.Network().ConnsToPeer — open
// connections only, not the peerstore — while the peer list itself is every
// peer that ever authored a message on a subscribed topic (gossip-only
// publishers included, never pruned). So the Addrs filter is what separates
// live neighbours from gossip-only authors; it is not redundant with the
// listing. This differs from the p2p service's own GetPeers RPC, which is
// filtered to IsConnected registry entries.
//
// The rebuild is unconditional so the reconcile pass and the hot path share
// one fresh view: a peer the pass is about to clear cannot be re-flagged off a
// stale snapshot in the seconds that follow.
func (s *Server) snapshotLiveConnIDs() map[string]struct{} {
	live := make(map[string]struct{})
	if s.P2PClient == nil {
		return live
	}

	s.liveConns.mu.Lock()
	defer s.liveConns.mu.Unlock()

	s.rebuildLiveConnsLocked(time.Now())

	for id := range s.liveConns.addrs {
		live[id] = struct{}{}
	}

	return live
}
