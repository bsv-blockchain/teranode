package legacy

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// setban sends the legacy service an IP or a subnet. The ban has to reach the ban list the server
// admits peers by, whether or not a matching peer is connected, as in bitcoin-sv.
func TestServerBanPeer_RecordsAnIPOrSubnetWithoutAConnectedPeer(t *testing.T) {
	// a peer handler with no peers connected, which is what a lookup of the peer list reaches
	srv := &server{logger: ulogger.TestLogger{}, banList: emptyWritableBanList(t), query: make(chan interface{}, 1)}
	state := newTestPeerState()

	go func() {
		for msg := range srv.query {
			srv.handleQuery(state, msg)
		}
	}()
	t.Cleanup(func() { close(srv.query) })

	s := &Server{logger: ulogger.TestLogger{}, server: srv}
	until := time.Now().Add(time.Hour).Unix()

	require.NoError(t, s.banPeer(t.Context(), "10.1.0.0/16", until))
	require.True(t, s.server.banList.IsBanned("10.1.2.3:8333"), "a subnet ban covers a host inside it")
	require.False(t, s.server.banList.IsBanned("10.2.0.1"))

	require.NoError(t, s.banPeer(t.Context(), "8.8.8.8", until))
	require.True(t, s.server.banList.IsBanned("8.8.8.8:51000"))

	require.NoError(t, s.banPeer(t.Context(), "9.9.9.9:8333", until), "an IP:port is accepted as before")
	require.True(t, s.server.banList.IsBanned("9.9.9.9"))

	require.Error(t, s.banPeer(t.Context(), "not-an-address", until))

	require.NoError(t, s.banPeer(t.Context(), "2001:db8::/32", until))
	require.True(t, s.server.banList.IsBanned("[2001:db8::7]:8333"), "an IPv6 peer address with a port is inside the prefix")
	require.False(t, s.server.banList.IsBanned("[2001:db9::1]:8333"))
}
