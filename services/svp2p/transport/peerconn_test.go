package transport

import "testing"

// Conn must satisfy PeerConn: every method protocol.Peer calls on its
// connection (peer.go, getdata.go) is on this interface and nothing else.
func TestConnIsAPeerConn(t *testing.T) {
	var _ PeerConn = (*Conn)(nil)
}
