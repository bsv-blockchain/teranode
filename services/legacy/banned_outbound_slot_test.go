package legacy

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/services/legacy/connmgr"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// A dial that reaches a banned address gives its slot back. On 2026-09-24 mainnet dialled four
// addresses it had banned as "not a BSV node" right after a restart. Each was rejected with the
// socket closed but its connection request left in the connection manager as established, so the
// manager counted 8 of its 8 outbound slots in use with only 4 peers connected, never dialled
// again, and the node synced all night on 4 peers.
func TestABannedOutboundPeerGivesItsSlotBack(t *testing.T) {
	var (
		mtx     sync.Mutex
		closers []net.Conn
	)

	srv := &server{logger: ulogger.TestLogger{}, banList: bannedTestBanList(t, "10.0.0.1")}

	cmgr, err := connmgr.New(ulogger.TestLogger{}, &connmgr.Config{
		TargetOutbound: 8,
		Dial: func(to net.Addr) (net.Conn, error) {
			ours, theirs := net.Pipe()

			mtx.Lock()
			closers = append(closers, ours, theirs)
			mtx.Unlock()

			return addressedConn{Conn: ours, remote: to}, nil
		},
		OnConnection: srv.outboundPeerConnected,
	})
	require.NoError(t, err)

	srv.connManager = cmgr

	cmgr.Start()

	t.Cleanup(func() {
		cmgr.Stop()
		cmgr.Wait()

		mtx.Lock()
		defer mtx.Unlock()

		for _, c := range closers {
			_ = c.Close()
		}
	})

	addr, err := net.ResolveTCPAddr("tcp", "10.0.0.1:8333")
	require.NoError(t, err)

	req := &connmgr.ConnReq{}
	req.SetAddr(addr)

	go cmgr.Connect(req)

	// The manager records the connection as established a moment before handing it over, so the
	// slot is held briefly by design. What matters is that the request is removed, gives its slot
	// back, and is not dialled again: the address is banned.
	require.Eventually(t, func() bool { return req.State() == connmgr.ConnDisconnected }, 5*time.Second, 10*time.Millisecond,
		"the banned peer's request is removed")
	require.Zero(t, cmgr.AutomaticOutboundCount(), "a rejected banned peer must give its outbound slot back")
	require.Never(t, func() bool { return cmgr.AutomaticOutboundCount() > 0 }, 500*time.Millisecond, 10*time.Millisecond,
		"and the banned address is not dialled again")
}

// addressedConn is an in-memory connection that reports the address it was dialled at, as a real
// one does. net.Pipe reports "pipe", which no ban list matches.
type addressedConn struct {
	net.Conn
	remote net.Addr
}

func (c addressedConn) RemoteAddr() net.Addr { return c.remote }
