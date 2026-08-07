package p2p

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/p2p/p2p_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// publicPeerServiceMethods are the PeerService RPCs deliberately reachable
// without the API key. Every entry must be read-only: the handler may query the
// peer registry and ban list but must never write to them, which
// TestPublicRPCsDoNotMutateRegistry proves from the handler source. Adding an
// RPC to neither this list nor authProtectedMethods fails
// TestAuthProtectedMethodsCoverAllRPCs.
var publicPeerServiceMethods = map[string]bool{
	"/p2p_api.PeerService/GetPeers":           true,
	"/p2p_api.PeerService/IsBanned":           true,
	"/p2p_api.PeerService/ListBanned":         true,
	"/p2p_api.PeerService/GetPeersForCatchup": true,
	"/p2p_api.PeerService/IsPeerMalicious":    true,
	"/p2p_api.PeerService/IsPeerUnhealthy":    true,
	"/p2p_api.PeerService/GetPeerRegistry":    true,
	"/p2p_api.PeerService/GetPeer":            true,
}

// TestAuthProtectedMethodsCoverAllRPCs forces every PeerService RPC to be
// classified as either protected or explicitly public, so new mutating RPCs
// cannot ship unauthenticated by omission.
func TestAuthProtectedMethodsCoverAllRPCs(t *testing.T) {
	protected := authProtectedMethods()

	for _, m := range p2p_api.PeerService_ServiceDesc.Methods {
		fullMethod := "/" + p2p_api.PeerService_ServiceDesc.ServiceName + "/" + m.MethodName

		isProtected := protected[fullMethod]
		isPublic := publicPeerServiceMethods[fullMethod]

		require.False(t, isProtected && isPublic, "%s is both protected and public", fullMethod)
		require.True(t, isProtected || isPublic,
			"%s is not classified: add it to authProtectedMethods (any state-mutating RPC) or publicPeerServiceMethods (read-only only)", fullMethod)
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

// readOnlyMethodsByField lists, per guarded Server field, the methods a public
// RPC handler may call on it. Anything absent counts as a write, so a new
// registry or ban-list method is treated as mutating until it is deliberately
// listed here.
var readOnlyMethodsByField = map[string]map[string]bool{
	"peerRegistry": {
		"GetPeer":         true,
		"ListPeers":       true,
		"IsPeerBanned":    true,
		"ListBannedPeers": true,
	},
	"banList": {
		"IsBanned":   true,
		"ListBanned": true,
	},
}

// TestPublicRPCsDoNotMutateRegistry proves from the handler source that every
// RPC left out of authProtectedMethods is genuinely read-only. Without this,
// "internal data-plane reporting" stays a comment: the classification drifts the
// moment somebody adds a write to an existing public handler, and an
// unauthenticated caller inherits it.
func TestPublicRPCsDoNotMutateRegistry(t *testing.T) {
	fns := parsePackageFuncs(t)

	for method := range publicPeerServiceMethods {
		name := method[strings.LastIndex(method, "/")+1:]

		decl, ok := fns["Server."+name]
		require.True(t, ok, "no (*Server).%s handler found for public RPC %s", name, method)

		writes := findGuardedWrites(fns, decl, map[string]bool{})
		require.Empty(t, writes,
			"public RPC %s mutates guarded state (%s); move it into authProtectedMethods or drop the write",
			method, strings.Join(writes, ", "))
	}
}

// protectedWithoutGuardedWrites are protected RPCs that mutate state the source
// guard cannot see - libp2p connection state rather than the peer registry or
// ban list - so they are exempt from TestProtectedRPCsAreTheMutatingOnes.
var protectedWithoutGuardedWrites = map[string]bool{
	"/p2p_api.PeerService/ConnectPeer":    true,
	"/p2p_api.PeerService/DisconnectPeer": true,
}

// TestProtectedRPCsAreTheMutatingOnes is the other direction: an RPC that no
// longer writes anything should not stay behind the API key by inertia, since
// needless authentication on a read path invites operators to hand the key out
// more widely than necessary.
func TestProtectedRPCsAreTheMutatingOnes(t *testing.T) {
	fns := parsePackageFuncs(t)

	for method := range authProtectedMethods() {
		if protectedWithoutGuardedWrites[method] {
			continue
		}

		name := method[strings.LastIndex(method, "/")+1:]

		decl, ok := fns["Server."+name]
		require.True(t, ok, "no (*Server).%s handler found for protected RPC %s", name, method)

		writes := findGuardedWrites(fns, decl, map[string]bool{})
		require.NotEmpty(t, writes,
			"protected RPC %s no longer writes guarded state; reclassify it as public, or add it to protectedWithoutGuardedWrites if it mutates state the guard cannot see", method)
	}

	for method := range protectedWithoutGuardedWrites {
		require.True(t, authProtectedMethods()[method],
			"%s is exempted from the mutation check but is not protected", method)
	}
}

// parsePackageFuncs indexes every non-test function in this package by
// "Server.<name>" for methods on *Server, or "<name>" for plain functions.
func parsePackageFuncs(t *testing.T) map[string]*ast.FuncDecl {
	t.Helper()

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()
	fns := make(map[string]*ast.FuncDecl)

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}

		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)

		for _, d := range file.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}

			if fn.Recv == nil {
				fns[fn.Name.Name] = fn
				continue
			}

			if serverReceiverName(fn) != "" {
				fns["Server."+fn.Name.Name] = fn
			}
		}
	}

	return fns
}

// serverReceiverName returns the receiver identifier of a (*Server) method, or
// "" if the function is not a *Server method or the receiver is unnamed.
func serverReceiverName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) != 1 {
		return ""
	}

	star, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
	if !ok {
		return ""
	}

	if ident, ok := star.X.(*ast.Ident); !ok || ident.Name != "Server" {
		return ""
	}

	if len(fn.Recv.List[0].Names) != 1 {
		return ""
	}

	return fn.Recv.List[0].Names[0].Name
}

// findGuardedWrites walks fn and every same-package function it calls, and
// reports each way it could write a guarded field. Anything it cannot prove
// read-only - the field escaping into a call argument or a local variable - is
// reported too, so the guard fails closed.
func findGuardedWrites(fns map[string]*ast.FuncDecl, fn *ast.FuncDecl, seen map[string]bool) []string {
	recv := serverReceiverName(fn)

	// isGuarded reports whether expr is `<recv>.<guardedField>`.
	isGuarded := func(expr ast.Expr) (string, bool) {
		sel, ok := expr.(*ast.SelectorExpr)
		if !ok {
			return "", false
		}

		ident, ok := sel.X.(*ast.Ident)
		if !ok || recv == "" || ident.Name != recv {
			return "", false
		}

		if _, guarded := readOnlyMethodsByField[sel.Sel.Name]; !guarded {
			return "", false
		}

		return sel.Sel.Name, true
	}

	var writes []string

	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.CallExpr:
			sel, ok := node.Fun.(*ast.SelectorExpr)
			if ok {
				if field, guarded := isGuarded(sel.X); guarded && !readOnlyMethodsByField[field][sel.Sel.Name] {
					writes = append(writes, fn.Name.Name+" -> "+field+"."+sel.Sel.Name)
				}

				// A call on the receiver itself (s.helper(...)) may write on our behalf.
				if ident, ok := sel.X.(*ast.Ident); ok && recv != "" && ident.Name == recv {
					writes = append(writes, callee(fns, "Server."+sel.Sel.Name, seen)...)
				}
			}

			if ident, ok := node.Fun.(*ast.Ident); ok {
				writes = append(writes, callee(fns, ident.Name, seen)...)
			}

			// Handing a guarded field to another function loses track of it.
			for _, arg := range node.Args {
				if field, guarded := isGuarded(arg); guarded {
					writes = append(writes, fn.Name.Name+" -> passes "+field+" to a call")
				}
			}

		case *ast.AssignStmt:
			for _, rhs := range node.Rhs {
				if field, guarded := isGuarded(rhs); guarded {
					writes = append(writes, fn.Name.Name+" -> aliases "+field+" into a local")
				}
			}
		}

		return true
	})

	return writes
}

// callee recurses into a same-package function, guarding against cycles.
func callee(fns map[string]*ast.FuncDecl, key string, seen map[string]bool) []string {
	if seen[key] {
		return nil
	}

	target, ok := fns[key]
	if !ok {
		return nil
	}

	seen[key] = true

	return findGuardedWrites(fns, target, seen)
}

// TestAuthInterceptorProtectsMutatingMethods exercises the auth interceptor with
// the p2p protected-method set: protected RPCs must be rejected without a valid
// API key while public RPCs pass through untouched.
func TestAuthInterceptorProtectsMutatingMethods(t *testing.T) {
	const apiKey = "test-admin-key"

	interceptor := util.CreateAuthInterceptor(apiKey, authProtectedMethods())

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

	for method := range authProtectedMethods() {
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

func TestBindsAllInterfaces(t *testing.T) {
	for addr, want := range map[string]bool{
		":9906":            true,
		"0.0.0.0:9906":     true,
		"[::]:9906":        true,
		"localhost:9906":   false,
		"127.0.0.1:9906":   false,
		"[::1]:9906":       false,
		"10.0.1.50:9906":   false,
		"peer.internal:99": false,
		"not-an-address":   false,
		"":                 false,
	} {
		require.Equal(t, want, bindsAllInterfaces(addr), "bindsAllInterfaces(%q)", addr)
	}
}

const testDataPlaneAPIKey = "data-plane-test-key"

// startAuthedPeerService serves the real PeerService over bufconn behind the
// same auth interceptor the daemon installs, and returns clients with and
// without the API key. This exercises the whole path an attacker would use -
// dial the gRPC port, call the RPC - rather than the interceptor in isolation.
func startAuthedPeerService(t *testing.T, s *Server) (authed, anon p2p_api.PeerServiceClient) {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(
		util.CreateAuthInterceptor(testDataPlaneAPIKey, authProtectedMethods()),
	))
	p2p_api.RegisterPeerServiceServer(srv, s)

	go func() { _ = srv.Serve(lis) }()

	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})

	dial := func(withKey bool) p2p_api.PeerServiceClient {
		opts := []grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}

		if withKey {
			opts = append(opts, grpc.WithUnaryInterceptor(
				func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn,
					invoker grpc.UnaryInvoker, callOpts ...grpc.CallOption) error {
					ctx = metadata.AppendToOutgoingContext(ctx, "x-api-key", testDataPlaneAPIKey)
					return invoker(ctx, method, req, reply, cc, callOpts...)
				}))
		}

		conn, err := grpc.NewClient("passthrough:///bufnet", opts...)
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })

		return p2p_api.NewPeerServiceClient(conn)
	}

	return dial(true), dial(false)
}

// TestReportValidatedChainProgressRequiresAuth covers the trust anchor that
// sync-peer selection reads: an unauthenticated caller must not be able to write
// validated chain progress for a peer ID it minted.
func TestReportValidatedChainProgressRequiresAuth(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	authed, anon := startAuthedPeerService(t, s)

	req := &p2p_api.ReportValidatedChainProgressRequest{
		PeerId:    pid.String(),
		Height:    900_000,
		BlockHash: "0000000000000000000000000000000000000000000000000000000000000001",
		ChainWork: []byte{0xff, 0xff, 0xff, 0xff},
	}

	_, err := anon.ReportValidatedChainProgress(context.Background(), req)
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	info, found := reg.Get(pid.String())
	require.True(t, found)
	require.Zero(t, info.ValidatedHeight, "rejected call must not record validated height")
	require.Empty(t, info.ValidatedChainWork, "rejected call must not record validated chainwork")

	// The same call with the key still works, so the callers inside the
	// deployment are unaffected.
	resp, err := authed.ReportValidatedChainProgress(context.Background(), req)
	require.NoError(t, err)
	require.True(t, resp.Success)

	info, found = reg.Get(pid.String())
	require.True(t, found)
	require.Equal(t, uint32(900_000), info.ValidatedHeight)
	require.Equal(t, []byte{0xff, 0xff, 0xff, 0xff}, info.ValidatedChainWork)
}

// TestRecordCatchupMaliciousRequiresAuth covers the griefing half of the same
// attack: an unauthenticated caller must not be able to flag an honest peer
// malicious and thereby remove it from sync-peer selection.
func TestRecordCatchupMaliciousRequiresAuth(t *testing.T) {
	s, reg, pid := freshTestServer(t)

	validatedHash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	local := []byte{0x00, 0x10}
	reg.Register(&blockchain.PeerInfo{
		ID:                 pid.String(),
		DataHubURL:         "https://peer.example/api/v1",
		ValidatedHeight:    900_000,
		ValidatedBlockHash: validatedHash,
		ValidatedChainWork: []byte{0x00, 0x20},
	})

	// The DataHub URL is a placeholder, so keep selection off the network.
	tSettings := settings.NewSettings()
	tSettings.P2P.HealthCheckEnabled = false

	selector := NewPeerSelector(ulogger.TestLogger{}, tSettings)
	criteria := SelectionCriteria{LocalChainWork: local}

	info, found := reg.Get(pid.String())
	require.True(t, found)
	require.True(t, selector.isEligible(info, criteria), "peer must start out eligible for the test to mean anything")

	authed, anon := startAuthedPeerService(t, s)
	req := &p2p_api.RecordCatchupMaliciousRequest{PeerId: pid.String()}

	_, err = anon.RecordCatchupMalicious(context.Background(), req)
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	info, found = reg.Get(pid.String())
	require.True(t, found)
	require.Zero(t, info.MaliciousCount, "rejected call must not flag the peer")
	require.True(t, selector.isEligible(info, criteria), "rejected call must not change sync eligibility")

	// Prove the attack would have worked: the same call with the key does
	// evict the peer from selection.
	_, err = authed.RecordCatchupMalicious(context.Background(), req)
	require.NoError(t, err)

	info, found = reg.Get(pid.String())
	require.True(t, found)
	require.Equal(t, int64(1), info.MaliciousCount)
	require.False(t, selector.isEligible(info, criteria), "a malicious flag must remove the peer from selection")
}
