package p2p

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
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
	"google.golang.org/protobuf/types/known/emptypb"
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
// listed here. Fields that reach peer state only through writes (the batcher,
// the sync coordinator, the gossip caches, the ban channel) get an empty
// allow-list: touching them at all from a public handler is a finding.
//
// The guard is a source-level approximation, not a proof. It follows calls on
// the receiver within this package only; mutation reached through an interface
// value, a function that takes *Server as a parameter, or another package is
// invisible to it. Treat a pass as "no obvious write", and keep reviewing new
// public handlers by hand.
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
	// blockchainClient is guarded because its interface exposes ReportPeerFailure,
	// a peer-state write that feeds peer selection. No public handler touches it
	// today; listing it keeps a future one from shipping unauthenticated.
	"blockchainClient": {},
	// P2PClient reaches libp2p directly. ConnectPeer/DisconnectPeer are exempted
	// from the mutation check for exactly this reason, so the field has to be
	// guarded here or a public RPC could dial, drop or gossip unauthenticated.
	"P2PClient": {
		"GetID":    true,
		"GetPeers": true,
	},
	"registryBatcher":  {},
	"syncCoordinator":  {},
	"peerSelector":     {},
	"banChan":          {},
	"banStatusCache":   {},
	"reputationCache":  {},
	"ipBanCache":       {},
	"blockPeerMap":     {},
	"subtreePeerMap":   {},
	"localHeightCache": {},
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

			// Index Server methods whatever the receiver is called, and whether
			// it is a pointer or value receiver, so recursion never silently
			// stops at an unconventionally declared helper.
			if isServerMethod(fn) {
				fns["Server."+fn.Name.Name] = fn
			}
		}
	}

	return fns
}

// isServerType reports whether an AST type expression is Server or *Server.
func isServerType(expr ast.Expr) bool {
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}

	ident, ok := expr.(*ast.Ident)

	return ok && ident.Name == "Server"
}

// isServerMethod reports whether fn is a method on Server or *Server.
func isServerMethod(fn *ast.FuncDecl) bool {
	return fn.Recv != nil && len(fn.Recv.List) == 1 && isServerType(fn.Recv.List[0].Type)
}

// serverIdents returns every identifier inside fn that names a Server: the
// receiver and any parameter of type Server or *Server. Parameters matter
// because a plain function taking *Server can write the registry just as easily
// as a method can.
func serverIdents(fn *ast.FuncDecl) map[string]bool {
	names := make(map[string]bool)

	collect := func(fields *ast.FieldList) {
		if fields == nil {
			return
		}

		for _, field := range fields.List {
			if !isServerType(field.Type) {
				continue
			}

			for _, name := range field.Names {
				if name.Name != "_" {
					names[name.Name] = true
				}
			}
		}
	}

	collect(fn.Recv)
	collect(fn.Type.Params)

	return names
}

// findGuardedWrites walks fn and every same-package function it calls, and
// reports each way it could write a guarded field. Anything it cannot prove
// read-only - the field escaping into a call argument or a local variable - is
// reported too, so the guard fails closed.
func findGuardedWrites(fns map[string]*ast.FuncDecl, fn *ast.FuncDecl, seen map[string]bool) []string {
	servers := serverIdents(fn)

	// guardedField reports whether expr is `<server>.<guardedField>`.
	guardedField := func(expr ast.Expr) (string, bool) {
		sel, ok := expr.(*ast.SelectorExpr)
		if !ok {
			return "", false
		}

		ident, ok := sel.X.(*ast.Ident)
		if !ok || !servers[ident.Name] {
			return "", false
		}

		if _, guarded := readOnlyMethodsByField[sel.Sel.Name]; !guarded {
			return "", false
		}

		return sel.Sel.Name, true
	}

	// unwrap strips &x / *x so `&s.peerRegistry` is still recognised.
	unwrap := func(expr ast.Expr) ast.Expr {
		for {
			switch e := expr.(type) {
			case *ast.UnaryExpr:
				expr = e.X
			case *ast.StarExpr:
				expr = e.X
			case *ast.ParenExpr:
				expr = e.X
			default:
				return expr
			}
		}
	}

	var writes []string

	ast.Inspect(fn.Body, func(n ast.Node) bool {
		switch node := n.(type) {
		// Any selection off a guarded field that is not on its read-only list:
		// a call, a method value, or a nested field. Checking the selector
		// rather than the call covers `f := s.peerRegistry.Ban` too.
		case *ast.SelectorExpr:
			if field, guarded := guardedField(node.X); guarded && !readOnlyMethodsByField[field][node.Sel.Name] {
				writes = append(writes, fn.Name.Name+" -> "+field+"."+node.Sel.Name)
			}

		case *ast.CallExpr:
			// A call on a Server value (s.helper(...)) may write on our behalf.
			if sel, ok := node.Fun.(*ast.SelectorExpr); ok {
				if ident, ok := sel.X.(*ast.Ident); ok && servers[ident.Name] {
					writes = append(writes, callee(fns, "Server."+sel.Sel.Name, seen)...)
				}
			}

			if ident, ok := node.Fun.(*ast.Ident); ok {
				writes = append(writes, callee(fns, ident.Name, seen)...)
			}

			// Handing a guarded field to another function loses track of it.
			for _, arg := range node.Args {
				if field, guarded := guardedField(unwrap(arg)); guarded {
					writes = append(writes, fn.Name.Name+" -> passes "+field+" to a call")
				}
			}

		case *ast.AssignStmt:
			for _, lhs := range node.Lhs {
				if field, guarded := guardedField(unwrap(lhs)); guarded {
					writes = append(writes, fn.Name.Name+" -> replaces "+field)
				}
			}

			for _, rhs := range node.Rhs {
				if field, guarded := guardedField(unwrap(rhs)); guarded {
					writes = append(writes, fn.Name.Name+" -> aliases "+field+" into a local")
				}
			}

		case *ast.SendStmt:
			if field, guarded := guardedField(unwrap(node.Chan)); guarded {
				writes = append(writes, fn.Name.Name+" -> sends on "+field)
			}
		}

		return true
	})

	return writes
}

// TestFindGuardedWritesDetectsEscapes keeps the source guard honest. A walker
// that silently stopped matching would make TestPublicRPCsDoNotMutateRegistry
// pass vacuously, so each way a handler can reach guarded state gets a synthetic
// case here.
func TestFindGuardedWritesDetectsEscapes(t *testing.T) {
	const src = `package p2p

func (s *Server) readOnlyDirect()      { s.peerRegistry.GetPeer(nil, "id") }
func (s *Server) readOnlyBanList()     { s.banList.IsBanned("ip") }
func (s *Server) unrelatedField()      { s.logger.Debugf("hi") }

func (s *Server) writeDirect()         { s.peerRegistry.RemovePeer(nil, "id") }
func (s *Server) writeViaHelper()      { s.helper() }
func (s *Server) helper()              { s.peerRegistry.RemovePeer(nil, "id") }
func (s *Server) writeViaFreeFunc()    { freeWriter(s) }
func freeWriter(s *Server)             { s.peerRegistry.RemovePeer(nil, "id") }
func (s *Server) writeViaBatcher()     { s.registryBatcher.enqueue("id") }
func (s *Server) writeViaCache()       { s.banStatusCache.Store("id", true) }
func (s *Server) writeViaChannel()     { s.banChan <- BanEvent{} }
func (s *Server) writeInGoroutine()    { go func() { s.peerRegistry.RemovePeer(nil, "id") }() }
func (s *Server) writeViaMethodValue() { f := s.peerRegistry.RemovePeer; _ = f }
func (s *Server) replacesField()       { s.banList = nil }
func (s *Server) escapesAsArgument()   { sink(s.peerRegistry) }
func (s *Server) escapesViaPointer()   { sink(&s.banStatusCache) }
func (s *Server) aliasesIntoLocal()    { reg := s.peerRegistry; _ = reg }
func (v Server) valueReceiverWrite()   { v.peerRegistry.RemovePeer(nil, "id") }
func sink(any interface{})             {}
`

	file, err := parser.ParseFile(token.NewFileSet(), "synthetic.go", src, 0)
	require.NoError(t, err)

	fns := make(map[string]*ast.FuncDecl)

	for _, d := range file.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}

		if fn.Recv == nil {
			fns[fn.Name.Name] = fn
		} else if isServerMethod(fn) {
			fns["Server."+fn.Name.Name] = fn
		}
	}

	clean := []string{"readOnlyDirect", "readOnlyBanList", "unrelatedField"}
	dirty := []string{
		"writeDirect", "writeViaHelper", "writeViaFreeFunc", "writeViaBatcher",
		"writeViaCache", "writeViaChannel", "writeInGoroutine", "writeViaMethodValue",
		"replacesField", "escapesAsArgument", "escapesViaPointer", "aliasesIntoLocal",
		"valueReceiverWrite",
	}

	for _, name := range clean {
		fn, ok := fns["Server."+name]
		require.True(t, ok, "%s not indexed", name)
		require.Empty(t, findGuardedWrites(fns, fn, map[string]bool{}), "%s must be read-only", name)
	}

	for _, name := range dirty {
		fn, ok := fns["Server."+name]
		require.True(t, ok, "%s not indexed", name)
		require.NotEmpty(t, findGuardedWrites(fns, fn, map[string]bool{}), "%s must be flagged as a write", name)
	}
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

// TestGRPCAuthOptionsProtectEveryMutatingRPC pins the wiring Start hands to
// util.StartGRPCServer. Without it, dropping ProtectedMethods from the auth
// options would leave every other test in this file green while shipping the
// vulnerability again.
func TestGRPCAuthOptionsProtectEveryMutatingRPC(t *testing.T) {
	t.Run("configured key is used", func(t *testing.T) {
		tSettings := settings.NewSettings()
		tSettings.GRPCAdminAPIKey = "a-configured-key"

		s := &Server{settings: tSettings, logger: ulogger.TestLogger{}}

		opts, err := s.grpcAuthOptions()
		require.NoError(t, err)
		require.Equal(t, "a-configured-key", opts.APIKey)
		require.Equal(t, authProtectedMethods(), opts.ProtectedMethods)
	})

	t.Run("missing key falls closed, not open", func(t *testing.T) {
		tSettings := settings.NewSettings()
		tSettings.GRPCAdminAPIKey = ""

		s := &Server{settings: tSettings, logger: ulogger.TestLogger{}}

		opts, err := s.grpcAuthOptions()
		require.NoError(t, err)

		// A non-empty key matters twice over: util.StartGRPCServer installs no
		// auth interceptor at all for an empty key, and the generated one is
		// unguessable, so protected RPCs reject rather than admit everyone.
		require.NotEmpty(t, opts.APIKey)
		require.Equal(t, authProtectedMethods(), opts.ProtectedMethods)
	})
}

// warnRecorder captures Warnf output so the exposure warnings can be asserted
// rather than merely executed. Everything else falls through to TestLogger.
type warnRecorder struct {
	ulogger.TestLogger
	warnings []string
	errors   []string
}

func (l *warnRecorder) Warnf(format string, args ...interface{}) {
	l.warnings = append(l.warnings, fmt.Sprintf(format, args...))
}

func (l *warnRecorder) Errorf(format string, args ...interface{}) {
	l.errors = append(l.errors, fmt.Sprintf(format, args...))
}

// TestRejectWeakAdminAPIKey covers the gap the merge to the shared helper left:
// util.ValidateAdminAPIKey only warns about a short key, but a short key is
// accepted as genuine, so on a listener reachable beyond loopback it can be
// brute-forced into exactly the capability this authentication exists to deny.
func TestRejectWeakAdminAPIKey(t *testing.T) {
	newServer := func(networkName string) *Server {
		tSettings := settings.NewSettings()

		switch networkName {
		case chaincfg.RegressionNetParams.Name:
			tSettings.ChainCfgParams = &chaincfg.RegressionNetParams
		case chaincfg.TestNetParams.Name:
			tSettings.ChainCfgParams = &chaincfg.TestNetParams
		default:
			tSettings.ChainCfgParams = &chaincfg.MainNetParams
		}

		return &Server{logger: &warnRecorder{}, settings: tSettings}
	}

	t.Run("short key on a wide bind is fatal on public networks", func(t *testing.T) {
		for _, network := range []string{chaincfg.MainNetParams.Name, chaincfg.TestNetParams.Name} {
			err := newServer(network).rejectWeakAdminAPIKey(":9904", "abc")
			require.Error(t, err, "%s must refuse to start", network)
			require.Contains(t, err.Error(), network)
			require.NotContains(t, err.Error(), "abc", "the error must not echo the key")
		}
	})

	t.Run("short key on a loopback bind is allowed", func(t *testing.T) {
		require.NoError(t, newServer(chaincfg.MainNetParams.Name).rejectWeakAdminAPIKey("localhost:9904", "abc"),
			"loopback keeps the port unreachable, so a short key is not brute-forceable from outside")
	})

	t.Run("short key on regtest only warns", func(t *testing.T) {
		s := newServer(chaincfg.RegressionNetParams.Name)
		require.NoError(t, s.rejectWeakAdminAPIKey(":9904", "abc"))
		require.Len(t, s.logger.(*warnRecorder).warnings, 1)
	})

	t.Run("strong key is allowed everywhere", func(t *testing.T) {
		require.NoError(t, newServer(chaincfg.MainNetParams.Name).rejectWeakAdminAPIKey(":9904", "not-a-real-key-just-long-enough"))
	})

	t.Run("unknown network is not exempt", func(t *testing.T) {
		// A security guard that reads "network unknown" as "probably development"
		// fails open, so an unset ChainCfgParams must reject like any public net.
		tSettings := settings.NewSettings()
		tSettings.ChainCfgParams = nil

		s := &Server{logger: &warnRecorder{}, settings: tSettings}

		err := s.rejectWeakAdminAPIKey(":9904", "abc")
		require.Error(t, err, "an unidentified network must not inherit the regtest exemption")
		require.Contains(t, err.Error(), "unknown network")
	})

	t.Run("placeholder and empty keys are left to the shared helper", func(t *testing.T) {
		// Both fail closed elsewhere (ignored / random key), so this guard must
		// not turn them into a startup failure.
		require.NoError(t, newServer(chaincfg.MainNetParams.Name).rejectWeakAdminAPIKey(":9904", "testkey"))
		require.NoError(t, newServer(chaincfg.MainNetParams.Name).rejectWeakAdminAPIKey(":9904", ""))
	})
}

// TestWarnIfUnreachableBind covers the mirror-image misconfiguration the loopback
// default can introduce for a settings context this repository does not ship: a
// routable client address meeting a loopback bind, which silently stops catchup
// because every caller gets connection-refused and nothing else surfaces it.
func TestWarnIfUnreachableBind(t *testing.T) {
	tests := []struct {
		name       string
		listenAddr string
		clientAddr string
		wantError  bool
	}{
		{"routable client, loopback bind", "localhost:9904", "p2p-1:9904", true},
		{"routable IP client, loopback bind", "127.0.0.1:9904", "10.0.0.5:9904", true},
		{"routable client, wide bind", ":9904", "p2p-1:9904", false},
		{"loopback client, loopback bind", "localhost:9904", "localhost:9904", false},
		{"loopback client, wide bind", ":9904", "localhost:9904", false},
		{"no client address configured", "localhost:9904", "", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger := &warnRecorder{}
			s := &Server{logger: logger}

			s.warnIfUnreachableBind(tc.listenAddr, tc.clientAddr)

			if tc.wantError {
				require.Len(t, logger.errors, 1)
				require.Contains(t, logger.errors[0], tc.clientAddr)
				require.Contains(t, logger.errors[0], tc.listenAddr)
			} else {
				require.Empty(t, logger.errors)
			}
		})
	}
}

// TestNewClientWarnsWhenAPIKeyMissing covers the caller side of the same
// failure: with no key configured, every mutating report this client sends is
// rejected, and the warning is where an operator finds out. grpc.NewClient is
// lazy, so no server is needed.
func TestNewClientWarnsWhenAPIKeyMissing(t *testing.T) {
	t.Run("missing key warns", func(t *testing.T) {
		tSettings := settings.NewSettings()
		tSettings.GRPCAdminAPIKey = ""

		logger := &warnRecorder{}

		client, err := NewClientWithAddress(context.Background(), logger, "localhost:19906", tSettings)
		require.NoError(t, err)
		t.Cleanup(func() { _ = client.(*Client).Close() })

		require.Len(t, logger.warnings, 1)
		require.Contains(t, logger.warnings[0], "grpc_admin_api_key is unset or a well-known placeholder")
	})

	t.Run("placeholder key warns", func(t *testing.T) {
		// The server ignores placeholders and uses a random key, so a client
		// that sent one would be rejected: the client must not log a reassuring
		// "using API key" line that contradicts the server.
		tSettings := settings.NewSettings()
		tSettings.GRPCAdminAPIKey = "testkey"

		logger := &warnRecorder{}

		client, err := NewClientWithAddress(context.Background(), logger, "localhost:19906", tSettings)
		require.NoError(t, err)
		t.Cleanup(func() { _ = client.(*Client).Close() })

		require.Len(t, logger.warnings, 1)
		require.Contains(t, logger.warnings[0], "well-known placeholder")
	})

	t.Run("configured key does not warn", func(t *testing.T) {
		tSettings := settings.NewSettings()
		tSettings.GRPCAdminAPIKey = "a-configured-key"

		logger := &warnRecorder{}

		client, err := NewClientWithAddress(context.Background(), logger, "localhost:19906", tSettings)
		require.NoError(t, err)
		t.Cleanup(func() { _ = client.(*Client).Close() })

		require.Empty(t, logger.warnings)
	})
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

	// isEligibleBasic holds the per-peer merit checks - ban state, DataHub URL,
	// blacklist, reputation threshold, validated-work progress, sync cooldown.
	// That is the level a forged malicious flag acts on: it pins reputation to
	// 5.0, below the 20.0 threshold. The availability probe now runs as a
	// separate pre-pass, so this stays off the network without extra setup.
	selector := NewPeerSelector(ulogger.TestLogger{}, settings.NewSettings())
	criteria := SelectionCriteria{LocalChainWork: local}

	info, found := reg.Get(pid.String())
	require.True(t, found)
	require.True(t, selector.isEligibleBasic(info, criteria), "peer must start out eligible for the test to mean anything")

	authed, anon := startAuthedPeerService(t, s)
	req := &p2p_api.RecordCatchupMaliciousRequest{PeerId: pid.String()}

	_, err = anon.RecordCatchupMalicious(context.Background(), req)
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	info, found = reg.Get(pid.String())
	require.True(t, found)
	require.Zero(t, info.MaliciousCount, "rejected call must not flag the peer")
	require.True(t, selector.isEligibleBasic(info, criteria), "rejected call must not change sync eligibility")

	// Prove the attack would have worked: the same call with the key does
	// evict the peer from selection.
	_, err = authed.RecordCatchupMalicious(context.Background(), req)
	require.NoError(t, err)

	info, found = reg.Get(pid.String())
	require.True(t, found)
	require.Equal(t, int64(1), info.MaliciousCount)
	require.False(t, selector.isEligibleBasic(info, criteria), "a malicious flag must remove the peer from selection")
}

// TestPublicRPCsStayAnonymousOverTheWire checks the other half of the contract
// over a real connection: closing the mutators must not have closed the
// read-only queries that operators and dashboards depend on.
func TestPublicRPCsStayAnonymousOverTheWire(t *testing.T) {
	s, reg, pid := freshTestServer(t)
	reg.Register(&blockchain.PeerInfo{ID: pid.String()})

	_, anon := startAuthedPeerService(t, s)

	registry, err := anon.GetPeerRegistry(context.Background(), &emptypb.Empty{})
	require.NoError(t, err)
	require.Len(t, registry.Peers, 1)

	banned, err := anon.IsPeerMalicious(context.Background(), &p2p_api.IsPeerMaliciousRequest{PeerId: pid.String()})
	require.NoError(t, err)
	require.False(t, banned.IsMalicious)
}
