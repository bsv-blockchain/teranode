package legacy

import (
	"testing"

	"github.com/bsv-blockchain/teranode/services/legacy/peer_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/util/test/mocklogger"
	"github.com/stretchr/testify/require"
)

// TestProtectedMethodsCoverAllRPCs forces every PeerService RPC to be
// classified as either admin-protected or explicitly public, so a new
// mutating RPC cannot ship unauthenticated by omission. Mirrors
// services/p2p/server_auth_test.go's TestAdminProtectedMethodsCoverAllRPCs.
//
// This is also the test that forced ClearBanned into protectedMethods: before
// it existed, ClearBanned sat in neither map and was reachable without the
// admin key even though it wipes the entire ban list - a strict superset of
// UnbanPeer sitting right next to it, protected.
func TestProtectedMethodsCoverAllRPCs(t *testing.T) {
	registered := make(map[string]bool, len(peer_api.PeerService_ServiceDesc.Methods))

	for _, m := range peer_api.PeerService_ServiceDesc.Methods {
		fullMethod := "/" + peer_api.PeerService_ServiceDesc.ServiceName + "/" + m.MethodName
		registered[fullMethod] = true

		isProtected := protectedMethods[fullMethod]
		isPublic := publicPeerServiceMethods[fullMethod]

		require.False(t, isProtected && isPublic, "%s is both protected and public", fullMethod)
		require.True(t, isProtected || isPublic,
			"%s is not classified: add it to protectedMethods (state-mutating RPC) or publicPeerServiceMethods (read-only)", fullMethod)
	}

	for method := range protectedMethods {
		require.True(t, registered[method], "protected method %s is not a registered PeerService RPC", method)
	}

	for method := range publicPeerServiceMethods {
		require.True(t, registered[method], "public method %s is not a registered PeerService RPC", method)
	}

	// The auth interceptor is unary-only (util.StartGRPCServer installs no
	// stream auth interceptor); PeerService has no streaming RPCs today, but
	// pin that so a future stream doesn't silently bypass auth.
	require.Empty(t, peer_api.PeerService_ServiceDesc.Streams,
		"PeerService has streaming RPCs but the auth interceptor only covers unary methods; add stream auth before registering streams")
}

// TestResolveAdminAPIKey_Configured verifies that a configured, strong admin
// API key on a loopback listener is returned verbatim and no warning is
// logged.
func TestResolveAdminAPIKey_Configured(t *testing.T) {
	logger := mocklogger.NewTestLogger()
	s := &Server{
		logger: logger,
		settings: &settings.Settings{
			GRPCAdminAPIKey: "a-strong-random-admin-secret-value",
			Legacy:          settings.LegacySettings{GRPCListenAddress: "127.0.0.1:8087"},
		},
	}

	apiKey, err := s.resolveAdminAPIKey()

	require.NoError(t, err)
	require.Equal(t, "a-strong-random-admin-secret-value", apiKey)
	logger.AssertNumberOfCalls(t, "Warnf", 0)
}

// TestResolveAdminAPIKey_ConfiguredWeakOrExposed verifies that a configured
// key still warns (but is not rejected) when it is short, or when the
// listener is not loopback-bound without verified TLS - non-posture
// hardening carried over from the fail-closed design.
func TestResolveAdminAPIKey_ConfiguredWeakOrExposed(t *testing.T) {
	logger := mocklogger.NewTestLogger()
	s := &Server{
		logger: logger,
		settings: &settings.Settings{
			GRPCAdminAPIKey: "short-key",
			Legacy:          settings.LegacySettings{GRPCListenAddress: "0.0.0.0:8087"},
		},
	}

	apiKey, err := s.resolveAdminAPIKey()

	require.NoError(t, err)
	require.Equal(t, "short-key", apiKey)
	logger.AssertNumberOfCalls(t, "Warnf", 2) // one length warning and one cleartext-exposure warning
}

// TestResolveAdminAPIKey_Empty verifies that an unset admin API key is
// returned as-is (no key is fabricated - a generated key no client could
// ever learn just masked the exposure) and a single warning is logged
// naming the exposure.
func TestResolveAdminAPIKey_Empty(t *testing.T) {
	logger := mocklogger.NewTestLogger()
	s := &Server{
		logger:   logger,
		settings: &settings.Settings{GRPCAdminAPIKey: ""},
	}

	apiKey, err := s.resolveAdminAPIKey()

	require.NoError(t, err)
	require.Empty(t, apiKey)
	logger.AssertNumberOfCalls(t, "Warnf", 1)
}
