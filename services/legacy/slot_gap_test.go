package legacy

import (
	"fmt"
	"testing"

	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// The connection manager counting more outbound slots than there are outbound peers is a leak. On
// 2026-09-24 it counted 8 against 4 peers for seven hours and nothing said so. A gap seen in two
// consecutive checks, a minute apart, is reported; one alone may be a peer connecting or leaving.
func TestASlotGapIsReportedWhenItPersists(t *testing.T) {
	var w slotGapWatch

	require.Zero(t, w.observe(8, 8), "no gap")
	require.Zero(t, w.observe(8, 4), "a gap seen once may be a peer in transit")
	require.Equal(t, 4, w.observe(8, 4), "seen twice running, it is reported")
	require.Zero(t, w.observe(8, 8), "and clears when it closes")
	require.Zero(t, w.observe(8, 5), "a new gap starts over")
}

// The connected side must leave out addnode peers, as AutomaticOutboundCount does. With 8 slots
// counted and 4 automatic plus 4 addnode peers connected, counting every outbound peer finds no gap
// and the 4 leaked slots go unreported.
func TestAutomaticOutboundConnectedLeavesOutAddnodeAndInboundPeers(t *testing.T) {
	peers := make([]*serverPeer, 0, 10)

	for i := range 4 {
		peers = append(peers, newTestOutboundPeer(t, nil, fmt.Sprintf("10.0.0.%d:8333", i+1)))

		permanent := newTestOutboundPeer(t, nil, fmt.Sprintf("10.0.1.%d:8333", i+1))
		permanent.persistent = true
		peers = append(peers, permanent)
	}

	inbound := peer.NewInboundPeer(ulogger.TestLogger{}, settings.NewSettings(), &peer.Config{})
	peers = append(peers, &serverPeer{Peer: inbound}, nil)

	require.Equal(t, 4, automaticOutboundConnected(peers), "only the four automatic outbound peers hold slots")

	var w slotGapWatch

	require.Zero(t, w.observe(8, automaticOutboundConnected(peers)))
	require.Equal(t, 4, w.observe(8, automaticOutboundConnected(peers)), "the four leaked slots are reported")
}
