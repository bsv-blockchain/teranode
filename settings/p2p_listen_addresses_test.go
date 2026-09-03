package settings

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestValidateP2PListenAddresses pins the contract that p2p_listen_addresses may
// only describe the wildcard bind go-p2p-message-bus performs. A narrower value
// used to be accepted and silently ignored, leaving operators believing inbound
// exposure was restricted when libp2p still listened on every interface.
func TestValidateP2PListenAddresses(t *testing.T) {
	const port = 9905

	accepted := [][]string{
		{"0.0.0.0"},
		{"::"},
		{"/ip4/0.0.0.0"},
		{"/ip6/::"},
		{"/ip4/0.0.0.0/tcp/9905"},
		{"/ip6/::/tcp/9905"},
		{"/ip4/0.0.0.0/tcp/9905", "/ip6/::/tcp/9905"},
		{" 0.0.0.0 "},
	}
	for _, addrs := range accepted {
		require.NoError(t, ValidateP2PListenAddresses(addrs, port), "%v", addrs)
	}

	rejected := []struct {
		name  string
		addrs []string
		want  string
	}{
		{"nil", nil, "not set"},
		{"empty slice", []string{}, "not set"},
		{"empty entry", []string{""}, "empty entry"},
		{"loopback", []string{"127.0.0.1"}, "specific interface"},
		{"private interface", []string{"10.0.1.5"}, "specific interface"},
		{"loopback multiaddr", []string{"/ip4/127.0.0.1/tcp/9905"}, "specific interface"},
		{"ipv6 interface", []string{"/ip6/fe80::1/tcp/9905"}, "specific interface"},
		{"port mismatch", []string{"/ip4/0.0.0.0/tcp/9906"}, "does not match p2p_port"},
		{"dns multiaddr", []string{"/dns4/node.example/tcp/9905"}, "unsupported multiaddr component"},
		{"udp", []string{"/ip4/0.0.0.0/udp/9905"}, "unsupported multiaddr component"},
		{"garbage", []string{"not-an-address"}, "not an IP address or multiaddr"},
		{"bad multiaddr", []string{"/ip4/999.0.0.1"}, "invalid multiaddr"},
		{"one bad among good", []string{"0.0.0.0", "192.168.1.10"}, "192.168.1.10"},
	}
	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateP2PListenAddresses(tc.addrs, port)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

// TestCommittedP2PListenAddressesAreValid guards the committed settings.conf: the
// value it ships for the current SETTINGS_CONTEXT must pass the startup check.
func TestCommittedP2PListenAddressesAreValid(t *testing.T) {
	s := NewSettings()
	require.NoError(t, ValidateP2PListenAddresses(s.P2P.ListenAddresses, s.P2P.Port))
}
