package svnode

import (
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

// With FreePorts, each start attempt runs the node on two distinct ports that were free when
// picked, and the RPC URL, P2P host and command line follow them.
func TestDockerSVNode_UseFreePorts(t *testing.T) {
	d := NewDockerSVNode(Options{FreePorts: true})

	require.NoError(t, d.useFreePorts())

	rpc, p2p := d.opts.RPCPort, d.opts.P2PPort
	require.Positive(t, rpc)
	require.Positive(t, p2p)
	require.NotEqual(t, rpc, p2p)

	require.Equal(t, fmt.Sprintf("http://localhost:%d", rpc), d.RPCURL())
	require.Equal(t, fmt.Sprintf("localhost:%d", p2p), d.P2PHost())
	require.Contains(t, d.buildCmd(), fmt.Sprintf("-rpcport=%d", rpc))
	require.Contains(t, d.buildCmd(), fmt.Sprintf("-port=%d", p2p))

	// The ports are free: something can bind them.
	for _, port := range []int{rpc, p2p} {
		l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
		require.NoError(t, err, "port %d", port)
		require.NoError(t, l.Close())
	}
}

// Without FreePorts, the configured ports are kept.
func TestDockerSVNode_FixedPortsAreKept(t *testing.T) {
	d := NewDockerSVNode(DefaultOptions())

	require.Equal(t, fmt.Sprintf("http://localhost:%d", DefaultRPCPort), d.RPCURL())
	require.Equal(t, fmt.Sprintf("localhost:%d", DefaultP2PPort), d.P2PHost())
}
