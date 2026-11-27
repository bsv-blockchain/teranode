package p2p

// liveConnAddrs reports whether peerID has an open libp2p connection and, if
// so, the remote multiaddrs of those connections. It asks the message bus's
// connection table directly (P2PClient.PeerAddrs), in time proportional to
// that peer's connections, so it answers for a neighbour from the moment it
// connects and never walks the bus's author tracker. A gossip-relayed author
// has no connection: its lookup is an ID decode and one table miss, and
// leaves nothing behind, so a flood of fresh identities costs no more than
// one known neighbour.
func (s *Server) liveConnAddrs(peerID string) ([]string, bool) {
	if s.P2PClient == nil {
		return nil, false
	}

	addrs := s.P2PClient.PeerAddrs(peerID)

	return addrs, len(addrs) > 0
}

// hasLiveConnection reports whether the peer has an open libp2p connection
// (see liveConnAddrs). updatePeerLastMessageTime uses it so gossip-relayed
// publishers are never marked IsConnected while a neighbour is flagged on its
// first message.
func (s *Server) hasLiveConnection(peerID string) bool {
	_, live := s.liveConnAddrs(peerID)

	return live
}

// liveConnIDs returns the set of peer IDs with an open libp2p connection, for
// the reconcile pass, which needs every live peer rather than one. It walks
// P2PClient.GetPeers() once: the listing is every peer that authored a
// message on a subscribed topic and is connected or was heard from within the
// last hour (at most 10,000 authors are tracked, connected ones always kept),
// and Addrs is built from the host's open connections, so a non-empty Addrs
// is what separates a live neighbour from a gossip-only author. This differs
// from the p2p service's own GetPeers RPC, which is filtered to IsConnected
// registry entries.
func (s *Server) liveConnIDs() map[string]struct{} {
	live := make(map[string]struct{})
	if s.P2PClient == nil {
		return live
	}

	for _, p := range s.P2PClient.GetPeers() {
		if len(p.Addrs) > 0 {
			live[p.ID] = struct{}{}
		}
	}

	return live
}
