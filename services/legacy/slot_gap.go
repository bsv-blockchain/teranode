package legacy

// slotGapWatch reports when the connection manager counts more automatic outbound connections
// than there are outbound peers, two checks running. A gap that lasts is a leaked slot: the
// manager believes it is full and stops dialling, and the node syncs on fewer peers than it
// should. On 2026-09-24 four leaked slots kept mainnet on four peers for seven hours.
type slotGapWatch struct {
	lastGap int
}

// observe records one check and returns the gap when it has now been seen twice running, or zero.
func (w *slotGapWatch) observe(counted, connected int) int {
	gap := max(0, counted-connected)
	persisted := gap > 0 && w.lastGap > 0

	w.lastGap = gap

	if persisted {
		return gap
	}

	return 0
}

// automaticOutboundConnected counts the connected peers that hold an automatic outbound slot:
// outbound and not persistent. It is the connected side of the comparison slotGapWatch makes, so
// it has to count the same set the connection manager's AutomaticOutboundCount does, which leaves
// out permanent (addnode) connections. A persistent peer is one built from a permanent connection
// request (newServerPeer in outboundPeerConnected), so the two exclusions are the same flag.
// Counting every outbound peer instead lets each addnode peer hide one leaked slot.
func automaticOutboundConnected(peers []*serverPeer) int {
	n := 0

	for _, sp := range peers {
		if sp == nil || sp.Peer == nil || sp.Inbound() || sp.persistent {
			continue
		}

		n++
	}

	return n
}
