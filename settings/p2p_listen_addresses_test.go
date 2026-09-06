package settings

import (
	"path/filepath"
	"strconv"
	"strings"
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
		{"legacy host:port form", []string{"0.0.0.0:9905"}, "write 0.0.0.0 or /ip4/0.0.0.0/tcp/9905"},
		{"bad multiaddr", []string{"/ip4/999.0.0.1"}, "invalid multiaddr"},
		{"one bad among good", []string{"0.0.0.0", "192.168.1.10"}, "192.168.1.10"},
		{"second ip hidden behind wildcard", []string{"/ip4/0.0.0.0/tcp/9905/ip4/127.0.0.1/tcp/9905"}, "specific interface"},
		{"two wildcard ips", []string{"/ip4/0.0.0.0/ip6/::/tcp/9905"}, "more than one IP"},
		{"second tcp hidden behind good one", []string{"/ip4/0.0.0.0/tcp/9905/tcp/9906"}, "does not match p2p_port"},
		{"duplicate tcp", []string{"/ip4/0.0.0.0/tcp/9905/tcp/9905"}, "more than one /tcp"},
		{"peer id suffix", []string{"/ip4/0.0.0.0/tcp/9905/p2p/12D3KooWAFXWuxgdJoRsaA4J4RRRr8yu6WCrAPf8FaS7UfZg3ceG"}, "unsupported multiaddr component"},
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
// value it ships for every shipped context must pass the startup check. It reads
// the file directly rather than through NewSettings so a developer's gitignored
// settings_local.conf can neither fail it nor mask a regression (see
// TestP2PGRPCBindMatchesClientAddress). A narrowed override such as the former
// p2p_listen_addresses.dev = 127.0.0.1 would fail here.
//
// compose/settings_test.conf is committed too and is mounted over settings.conf
// as settings_local.conf by the compose stacks; its docker.host contexts are the
// only committed values that carry an explicit port, so they are checked with
// that file layered on top rather than through settings.conf alone.
func TestCommittedP2PListenAddressesAreValid(t *testing.T) {
	base := readCommittedSettingsConf(t)

	check := func(t *testing.T, conf settingsConf, settingsContext string) {
		t.Helper()

		portStr, ok := conf.resolve("p2p_port", settingsContext)
		require.True(t, ok, "p2p_port must be set")
		portStr = conf.expandPlaceholders(portStr, settingsContext)

		port, err := strconv.Atoi(portStr)
		require.NoError(t, err, "p2p_port %q must resolve to a number", portStr)

		listen, ok := conf.resolve("p2p_listen_addresses", settingsContext)
		require.True(t, ok, "p2p_listen_addresses must be set")
		listen = conf.expandPlaceholders(listen, settingsContext)

		require.NoError(t, ValidateP2PListenAddresses(strings.Split(listen, "|"), port))
	}

	for _, settingsContext := range shippedP2PContexts {
		name := settingsContext
		if name == "" {
			name = "default"
		}

		t.Run(name, func(t *testing.T) { check(t, base, settingsContext) })
	}

	compose := base.layered(readSettingsFile(t, filepath.Join("..", "compose", "settings_test.conf")))

	for _, settingsContext := range []string{"docker.host.teranode1", "docker.host.teranode2", "docker.host.teranode3"} {
		t.Run("compose/"+settingsContext, func(t *testing.T) {
			// Pin that the layered value really is the port-bearing one, so this
			// subtest cannot go vacuous if the compose override is ever dropped.
			listen, ok := compose.resolve("p2p_listen_addresses", settingsContext)
			require.True(t, ok)
			require.Contains(t, listen, "/tcp/", "compose docker.host listen address should carry an explicit port")

			check(t, compose, settingsContext)
		})
	}
}
