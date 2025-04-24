package p2p

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildAdvertiseMultiAddrs(t *testing.T) {
	defaultPort := 5000

	t.Run("empty list", func(t *testing.T) {
		addrs := buildAdvertiseMultiAddrs(nil, defaultPort)
		require.Empty(t, addrs)
	})

	t.Run("ipv4 default port", func(t *testing.T) {
		addrs := buildAdvertiseMultiAddrs([]string{"192.168.1.10"}, defaultPort)
		require.Len(t, addrs, 1)
		require.Equal(t, "/ip4/192.168.1.10/tcp/5000", addrs[0].String())
	})

	t.Run("dns default port", func(t *testing.T) {
		addrs := buildAdvertiseMultiAddrs([]string{"example.com"}, defaultPort)
		require.Len(t, addrs, 1)
		require.Equal(t, "/dns4/example.com/tcp/5000", addrs[0].String())
	})

	t.Run("ipv4 override port", func(t *testing.T) {
		addrs := buildAdvertiseMultiAddrs([]string{"10.0.0.5:6001"}, defaultPort)
		require.Len(t, addrs, 1)
		require.Equal(t, "/ip4/10.0.0.5/tcp/6001", addrs[0].String())
	})

	t.Run("dns override port", func(t *testing.T) {
		addrs := buildAdvertiseMultiAddrs([]string{"node.local:7002"}, defaultPort)
		require.Len(t, addrs, 1)
		require.Equal(t, "/dns4/node.local/tcp/7002", addrs[0].String())
	})

	t.Run("invalid port skipped", func(t *testing.T) {
		// invalid port should be skipped
		addrs := buildAdvertiseMultiAddrs([]string{"127.0.0.1:abc"}, defaultPort)
		require.Empty(t, addrs)
	})
}
