package settings

import (
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// shippedP2PContexts are the settings contexts committed settings.conf carries
// p2p overrides for, plus the plain defaults. Deployment-specific contexts that
// live in generated or gitignored config are out of scope here.
var shippedP2PContexts = []string{
	"",
	"dev",
	"test",
	"docker.m",
	"docker.ss.teranode1",
	"docker.host.teranode1",
	"docker.teranode1.test",
	"operator",
	"operator.teratestnet",
}

// TestP2PGRPCBindMatchesClientAddress pins the invariant that the p2p gRPC
// listener and the address its clients dial have to agree, in both directions:
//
//   - A client address that is not loopback means the service is reached from
//     another container or pod, so a loopback-only bind makes every caller fail
//     with connection-refused. GetPeersForCatchup and the catchup/validation
//     reporters go over this connection, so the visible symptom is a node that
//     finds no catchup peers while its port healthcheck still reports healthy.
//   - A bind on all interfaces while clients only ever dial loopback is the
//     mirror-image bug: nothing can use the wider bind, but the unauthenticated
//     read-only peer registry (peer IDs, DataHub URLs, heights, reputation, ban
//     state) is offered to the whole network for a reach that never happens.
//
// Both halves have been shipped broken before, in different contexts, so the
// invariant is checked rather than left to review.
//
// This reads committed settings.conf directly instead of going through
// NewSettings, which layers a developer's gitignored settings_local.conf on top.
// Otherwise a local override of either key - entirely reasonable when pointing a
// local node at a containerised p2p - would fail this test for a reason nobody
// else can reproduce, and the mirror case would let a local override mask a real
// regression in the committed file.
func TestP2PGRPCBindMatchesClientAddress(t *testing.T) {
	conf := readCommittedSettingsConf(t)

	for _, settingsContext := range shippedP2PContexts {
		name := settingsContext
		if name == "" {
			name = "default"
		}

		t.Run(name, func(t *testing.T) {
			clientAddr, ok := conf.resolve("p2p_grpcAddress", settingsContext)
			require.True(t, ok, "p2p_grpcAddress must be set in settings.conf")

			listenAddr, ok := conf.resolve("p2p_grpcListenAddress", settingsContext)
			require.True(t, ok, "p2p_grpcListenAddress must be set in settings.conf")

			require.Equal(t, addressIsLoopback(clientAddr), addressIsLoopback(listenAddr),
				"p2p_grpcAddress (%s) and p2p_grpcListenAddress (%s) disagree on whether p2p is reached across the network: "+
					"either give the client a routable address or keep the bind on loopback",
				clientAddr, listenAddr)
		})
	}
}

// settingsConf is the committed settings.conf parsed as key -> value, keys
// retaining their ".context" suffix exactly as written.
type settingsConf map[string]string

// resolve mimics the context-layered lookup: try the fully qualified key, then
// drop one trailing context segment at a time, then the bare key.
func (c settingsConf) resolve(key, settingsContext string) (string, bool) {
	for ctx := settingsContext; ctx != ""; {
		if v, ok := c[key+"."+ctx]; ok {
			return v, true
		}

		idx := strings.LastIndex(ctx, ".")
		if idx < 0 {
			break
		}

		ctx = ctx[:idx]
	}

	v, ok := c[key]

	return v, ok
}

var settingsLineRE = regexp.MustCompile(`^([A-Za-z0-9_.]+)\s*=\s*(.*)$`)

// readCommittedSettingsConf parses the repository's settings.conf. Values keep
// their ${VAR} placeholders: only the host half of each address is inspected,
// and every port in these keys is a placeholder.
func readCommittedSettingsConf(t *testing.T) settingsConf {
	t.Helper()

	path := filepath.Join("..", "settings.conf")

	data, err := os.ReadFile(path)
	require.NoError(t, err, "committed settings.conf must be readable at %s", path)

	conf := make(settingsConf)

	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		m := settingsLineRE.FindStringSubmatch(line)
		if m == nil {
			continue
		}

		// Strip a trailing comment, then surrounding quotes.
		value := m[2]
		if idx := strings.Index(value, " #"); idx >= 0 {
			value = value[:idx]
		}

		conf[m[1]] = strings.Trim(strings.TrimSpace(value), `"`)
	}

	require.NotEmpty(t, conf, "parsed no settings from settings.conf")

	return conf
}

// addressIsLoopback reports whether an address targets only the local host. A
// scheme-qualified address (the k8s:/// resolver form) is never loopback.
func addressIsLoopback(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		// No host:port shape at all; treat as routable rather than loopback.
		return false
	}

	switch host {
	case "localhost":
		return true
	case "", "0.0.0.0", "::":
		return false
	}

	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}

	// A DNS name that is not "localhost" is a routable service address.
	return false
}
