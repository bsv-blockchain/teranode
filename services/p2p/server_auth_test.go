package p2p

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// publicPeerServiceMethods are the PeerService RPCs deliberately reachable
// without the admin API key: read-only queries and internal data-plane
// reporting calls made by other services (block/subtree validation) that do
// not carry admin credentials. Adding an RPC to neither this list nor
// adminProtectedMethods fails TestAdminProtectedMethodsCoverAllRPCs.
var publicPeerServiceMethods = map[string]bool{
	// Read-only queries
	"/p2p_api.PeerService/GetPeers":           true,
	"/p2p_api.PeerService/IsBanned":           true,
	"/p2p_api.PeerService/ListBanned":         true,
	"/p2p_api.PeerService/GetPeersForCatchup": true,
	"/p2p_api.PeerService/IsPeerMalicious":    true,
	"/p2p_api.PeerService/IsPeerUnhealthy":    true,
	"/p2p_api.PeerService/GetPeerRegistry":    true,
	"/p2p_api.PeerService/GetPeer":            true,
	// Internal data-plane reporting
	"/p2p_api.PeerService/RecordCatchupAttempt":         true,
	"/p2p_api.PeerService/RecordCatchupSuccess":         true,
	"/p2p_api.PeerService/RecordCatchupFailure":         true,
	"/p2p_api.PeerService/RecordCatchupMalicious":       true,
	"/p2p_api.PeerService/UpdateCatchupReputation":      true,
	"/p2p_api.PeerService/UpdateCatchupError":           true,
	"/p2p_api.PeerService/ReportValidSubtree":           true,
	"/p2p_api.PeerService/ReportValidBlock":             true,
	"/p2p_api.PeerService/ReportValidBlockHeaders":      true,
	"/p2p_api.PeerService/ReportValidatedChainProgress": true,
	"/p2p_api.PeerService/RecordBytesDownloaded":        true,
}

// TestAdminProtectedMethodsCoverAllRPCs forces every PeerService RPC to be
// classified as either admin-protected or explicitly public, so new mutating
// RPCs cannot ship unauthenticated by omission.
func TestAdminProtectedMethodsCoverAllRPCs(t *testing.T) {
	protected := adminProtectedMethods()

	for _, m := range p2p_api.PeerService_ServiceDesc.Methods {
		fullMethod := "/" + p2p_api.PeerService_ServiceDesc.ServiceName + "/" + m.MethodName

		isProtected := protected[fullMethod]
		isPublic := publicPeerServiceMethods[fullMethod]

		require.False(t, isProtected && isPublic, "%s is both protected and public", fullMethod)
		require.True(t, isProtected || isPublic,
			"%s is not classified: add it to adminProtectedMethods (state-mutating admin RPC) or publicPeerServiceMethods (read-only or internal data-plane)", fullMethod)
	}

	// Every protected/public entry must correspond to a real RPC (catches typos).
	registered := make(map[string]bool)
	for _, m := range p2p_api.PeerService_ServiceDesc.Methods {
		registered["/"+p2p_api.PeerService_ServiceDesc.ServiceName+"/"+m.MethodName] = true
	}

	for method := range protected {
		require.True(t, registered[method], "protected method %s is not a registered PeerService RPC", method)
	}

	for method := range publicPeerServiceMethods {
		require.True(t, registered[method], "public method %s is not a registered PeerService RPC", method)
	}

	// The auth interceptor is unary-only (util.StartGRPCServer installs no
	// stream auth interceptor), so a streaming RPC would bypass authentication
	// entirely. Adding one requires wiring stream auth first.
	require.Empty(t, p2p_api.PeerService_ServiceDesc.Streams,
		"PeerService has streaming RPCs but the auth interceptor only covers unary methods; add stream auth before registering streams")
}

// TestAuthInterceptorProtectsAdminMethods exercises the auth interceptor with
// the p2p protected-method set: admin RPCs must be rejected without a valid
// API key while public RPCs pass through untouched.
func TestAuthInterceptorProtectsAdminMethods(t *testing.T) {
	const apiKey = "test-admin-key"

	interceptor := util.CreateAuthInterceptor(apiKey, adminProtectedMethods())

	handlerCalled := false
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return "ok", nil
	}

	call := func(ctx context.Context, fullMethod string) error {
		handlerCalled = false
		_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: fullMethod}, handler)

		return err
	}

	for method := range adminProtectedMethods() {
		// No metadata at all
		err := call(context.Background(), method)
		require.Equal(t, codes.Unauthenticated, status.Code(err), "%s without metadata must be rejected", method)
		require.False(t, handlerCalled, "%s handler must not run without a key", method)

		// Wrong key
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", "wrong-key"))
		err = call(ctx, method)
		require.Equal(t, codes.Unauthenticated, status.Code(err), "%s with a wrong key must be rejected", method)
		require.False(t, handlerCalled, "%s handler must not run with a wrong key", method)

		// Correct key
		ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", apiKey))
		err = call(ctx, method)
		require.NoError(t, err, "%s with the correct key must succeed", method)
		require.True(t, handlerCalled, "%s handler must run with the correct key", method)
	}

	// Public methods pass through without any key.
	for method := range publicPeerServiceMethods {
		err := call(context.Background(), method)
		require.NoError(t, err, "public method %s must not require a key", method)
		require.True(t, handlerCalled, "public method %s handler must run", method)
	}
}
