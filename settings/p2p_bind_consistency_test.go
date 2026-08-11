package settings

import (
	"net"
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
func TestP2PGRPCBindMatchesClientAddress(t *testing.T) {
	for _, settingsContext := range shippedP2PContexts {
		name := settingsContext
		if name == "" {
			name = "default"
		}

		t.Run(name, func(t *testing.T) {
			var tSettings *Settings
			if settingsContext == "" {
				tSettings = NewSettings()
			} else {
				tSettings = NewSettings(settingsContext)
			}

			clientAddr := tSettings.P2P.GRPCAddress
			listenAddr := tSettings.P2P.GRPCListenAddress

			require.NotEmpty(t, listenAddr, "p2p_grpcListenAddress must be set")
			require.NotEmpty(t, clientAddr, "p2p_grpcAddress must be set")

			clientIsLocal := addressIsLoopback(t, clientAddr)
			listenIsLocal := addressIsLoopback(t, listenAddr)

			require.Equal(t, clientIsLocal, listenIsLocal,
				"p2p_grpcAddress (%s) and p2p_grpcListenAddress (%s) disagree on whether p2p is reached across the network: "+
					"either give the client a routable address or keep the bind on loopback",
				clientAddr, listenAddr)
		})
	}
}

// addressIsLoopback reports whether an address targets only the local host. A
// scheme-qualified address (the k8s:/// resolver form) is never loopback.
func addressIsLoopback(t *testing.T, address string) bool {
	t.Helper()

	host, _, err := net.SplitHostPort(address)
	if err != nil {
		// No host:port shape at all (e.g. "k8s:///peer.ns.svc:9906" splits on
		// the last colon and still yields a host, so this is a genuine oddity).
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
