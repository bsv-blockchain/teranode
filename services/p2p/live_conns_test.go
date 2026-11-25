package p2p

import (
	"testing"

	p2pMessageBus "github.com/bsv-blockchain/go-p2p-message-bus"
	"github.com/stretchr/testify/require"
)

// Liveness is asked of the bus's connection table per peer, so neither a
// flood of unknown authors nor a stream of messages from a neighbour costs a
// GetPeers walk, and a neighbour that connects later is seen at once.
func TestLiveConnAddrs_AsksTheConnectionTableWithoutWalking(t *testing.T) {
	s, _ := newServerWithLocalRegistry(t)
	neighbour := mustNewPeerID(t)
	client := &countingP2PClient{}
	client.peers = []p2pMessageBus.PeerInfo{{ID: neighbour.String(), Addrs: []string{"/ip4/10.0.0.1/tcp/9905"}}}
	s.P2PClient = client

	for i := 0; i < 100; i++ {
		require.False(t, s.hasLiveConnection(mustNewPeerID(t).String()))
		require.True(t, s.hasLiveConnection(neighbour.String()))
	}

	late := mustNewPeerID(t)
	client.peers = append(client.peers, p2pMessageBus.PeerInfo{ID: late.String(), Addrs: []string{"/ip4/10.0.0.2/tcp/9905"}})

	addrs, live := s.liveConnAddrs(late.String())
	require.True(t, live, "a neighbour that connected after the last lookup is seen on its first lookup")
	require.Equal(t, []string{"/ip4/10.0.0.2/tcp/9905"}, addrs)
	require.Zero(t, client.walks.Load(), "per-peer liveness must never walk GetPeers")
}
