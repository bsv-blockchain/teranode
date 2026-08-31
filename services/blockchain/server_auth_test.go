package blockchain

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	sqlstore "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/test/mocklogger"
	"github.com/labstack/echo/v4"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// TestProtectedMethodsCoverAllRPCs forces every unary RPC on both
// BlockchainAPI and PeerRegistryService (served on the same listener, see
// [Blockchain][Start]) to be classified as either admin-protected or
// explicitly public, so new mutating RPCs cannot ship unauthenticated by
// omission. Mirrors services/p2p/server_auth_test.go's
// TestAdminProtectedMethodsCoverAllRPCs.
func TestProtectedMethodsCoverAllRPCs(t *testing.T) {
	protected := protectedMethods

	checkCoverage := func(t *testing.T, serviceName string, methods []grpc.MethodDesc, public map[string]bool) {
		t.Helper()

		registered := make(map[string]bool, len(methods))

		for _, m := range methods {
			fullMethod := "/" + serviceName + "/" + m.MethodName
			registered[fullMethod] = true

			isProtected := protected[fullMethod]
			isPublic := public[fullMethod]

			require.False(t, isProtected && isPublic, "%s is both protected and public", fullMethod)
			require.True(t, isProtected || isPublic,
				"%s is not classified: add it to protectedMethods (state-mutating RPC) or the public list in this test (read-only)", fullMethod)
		}

		for method := range public {
			require.True(t, registered[method], "public method %s is not a registered %s RPC", method, serviceName)
		}
	}

	checkCoverage(t, blockchain_api.BlockchainAPI_ServiceDesc.ServiceName, blockchain_api.BlockchainAPI_ServiceDesc.Methods, publicBlockchainAPIMethods)
	checkCoverage(t, blockchain_api.PeerRegistryService_ServiceDesc.ServiceName, blockchain_api.PeerRegistryService_ServiceDesc.Methods, publicPeerRegistryServiceMethods)

	// Every protected entry must correspond to a real, registered RPC on one
	// of the two services (catches typos).
	allRegistered := make(map[string]bool)

	for _, m := range blockchain_api.BlockchainAPI_ServiceDesc.Methods {
		allRegistered["/"+blockchain_api.BlockchainAPI_ServiceDesc.ServiceName+"/"+m.MethodName] = true
	}

	for _, m := range blockchain_api.PeerRegistryService_ServiceDesc.Methods {
		allRegistered["/"+blockchain_api.PeerRegistryService_ServiceDesc.ServiceName+"/"+m.MethodName] = true
	}

	for method := range protected {
		require.True(t, allRegistered[method], "protected method %s is not a registered RPC", method)
	}

	// BlockchainAPI declares one streaming RPC, Subscribe. The auth
	// interceptor installed in util.StartGRPCServer is unary-only (see
	// util/grpc.go), so Subscribe bypasses authentication entirely and
	// cannot be added to protectedMethods. It is deliberately left
	// unauthenticated rather than silently exempted: it only pushes
	// server-initiated block/subtree/FSM notifications to the caller, so an
	// unauthenticated subscriber can observe but not mutate chain state.
	// Caller-supplied state does reach the server - req.Source is logged, is
	// returned by GetSubscribers and identifies the subscriber - so it is
	// sanitized on entry (sanitizeSubscriberSource) and never used as a
	// Prometheus label value directly (metricSourceLabel). This assertion pins
	// that decision -
	// if BlockchainAPI ever gains a second stream, or Subscribe starts
	// accepting mutating input, this must be revisited alongside stream auth.
	require.Len(t, blockchain_api.BlockchainAPI_ServiceDesc.Streams, 1,
		"BlockchainAPI stream count changed; re-evaluate stream auth before adding more streams")
	require.Equal(t, "Subscribe", blockchain_api.BlockchainAPI_ServiceDesc.Streams[0].StreamName,
		"BlockchainAPI's only stream is expected to be Subscribe (deliberately left unauthenticated, see comment above)")
	require.Empty(t, blockchain_api.PeerRegistryService_ServiceDesc.Streams,
		"PeerRegistryService has streaming RPCs but the auth interceptor only covers unary methods; add stream auth before registering streams")
}

// TestAuthInterceptorProtectsSendNotification exercises the auth interceptor
// with the production blockchain protected-method set: SendNotification must
// be rejected without a valid API key, while an unrelated RPC on the same
// service passes through untouched.
func TestAuthInterceptorProtectsSendNotification(t *testing.T) {
	const apiKey = "test-admin-key"

	interceptor := util.CreateAuthInterceptor(apiKey, protectedMethods)

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

	const method = "/blockchain_api.BlockchainAPI/SendNotification"

	// No metadata at all.
	err := call(context.Background(), method)
	require.Equal(t, codes.Unauthenticated, status.Code(err), "SendNotification without metadata must be rejected")
	require.False(t, handlerCalled, "handler must not run without a key")

	// Wrong key.
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", "wrong-key"))
	err = call(ctx, method)
	require.Equal(t, codes.Unauthenticated, status.Code(err), "SendNotification with a wrong key must be rejected")
	require.False(t, handlerCalled, "handler must not run with a wrong key")

	// Correct key.
	ctx = metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", apiKey))
	err = call(ctx, method)
	require.NoError(t, err, "SendNotification with the correct key must succeed")
	require.True(t, handlerCalled, "handler must run with the correct key")

	// An unrelated, public RPC on the same service must not require a key.
	const unrelatedMethod = "/blockchain_api.BlockchainAPI/GetBestBlockHeader"
	err = call(context.Background(), unrelatedMethod)
	require.NoError(t, err, "unrelated RPC must not require a key")
	require.True(t, handlerCalled, "unrelated RPC handler must run")
}

// TestAuthInterceptorProtectsReportPeerFailureAndSetBlockSubtreesSet exercises
// the two RPCs that, unprotected, let an attacker reach the same
// b.notifications flood vector as SendNotification without ever calling
// SendNotification directly (see [Blockchain][ReportPeerFailure] and
// [Blockchain][SetBlockSubtreesSet], both of which call b.SendNotification as
// a plain Go method call that bypasses the gRPC interceptor boundary of
// SendNotification itself).
func TestAuthInterceptorProtectsReportPeerFailureAndSetBlockSubtreesSet(t *testing.T) {
	const apiKey = "test-admin-key"

	interceptor := util.CreateAuthInterceptor(apiKey, protectedMethods)

	handlerCalled := false
	handler := func(ctx context.Context, req any) (any, error) {
		handlerCalled = true
		return "ok", nil
	}

	for _, method := range []string{
		"/blockchain_api.BlockchainAPI/ReportPeerFailure",
		"/blockchain_api.BlockchainAPI/SetBlockSubtreesSet",
	} {
		handlerCalled = false
		_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: method}, handler)
		require.Equal(t, codes.Unauthenticated, status.Code(err), "%s without metadata must be rejected", method)
		require.False(t, handlerCalled, "%s handler must not run without a key", method)

		handlerCalled = false
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-api-key", apiKey))
		_, err = interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: method}, handler)
		require.NoError(t, err, "%s with the correct key must succeed", method)
		require.True(t, handlerCalled, "%s handler must run with the correct key", method)
	}
}

// TestResolveAdminAPIKey_Configured verifies that a configured, strong admin
// API key on a loopback listener is returned verbatim and no warning is
// logged.
func TestResolveAdminAPIKey_Configured(t *testing.T) {
	logger := mocklogger.NewTestLogger()
	b := &Blockchain{
		logger: logger,
		settings: &settings.Settings{
			GRPCAdminAPIKey: "a-strong-random-admin-secret-value",
			BlockChain:      settings.BlockChainSettings{GRPCListenAddress: "127.0.0.1:8087"},
		},
	}

	apiKey, err := b.resolveAdminAPIKey()

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
	b := &Blockchain{
		logger: logger,
		settings: &settings.Settings{
			GRPCAdminAPIKey: "short-key",
			BlockChain:      settings.BlockChainSettings{GRPCListenAddress: "0.0.0.0:8087"},
		},
	}

	apiKey, err := b.resolveAdminAPIKey()

	require.NoError(t, err)
	require.Equal(t, "short-key", apiKey)
	logger.AssertNumberOfCalls(t, "Warnf", 2) // one length warning and one cleartext-exposure warning
}

// TestResolveAdminAPIKey_Placeholder verifies that a known placeholder key is
// refused outright rather than accepted as a credential. A non-empty key
// installs the auth interceptor, so accepting "testkey" would make the service
// claim its state-mutating RPCs are protected while the credential is public.
func TestResolveAdminAPIKey_Placeholder(t *testing.T) {
	for _, key := range []string{"testkey", "TestKey", " testkey ", "changeme", "admin"} {
		b := &Blockchain{
			logger:   mocklogger.NewTestLogger(),
			settings: &settings.Settings{GRPCAdminAPIKey: key},
		}

		apiKey, err := b.resolveAdminAPIKey()

		require.Error(t, err, "placeholder key %q must be refused", key)
		require.Empty(t, apiKey)
	}
}

// TestResolveAdminAPIKey_Empty verifies that an unset admin API key is
// returned as-is (no key is fabricated - a generated key no client could
// ever learn just masked the exposure) and a single warning is logged
// naming the exposure.
func TestResolveAdminAPIKey_Empty(t *testing.T) {
	logger := mocklogger.NewTestLogger()
	b := &Blockchain{
		logger:   logger,
		settings: &settings.Settings{GRPCAdminAPIKey: ""},
	}

	apiKey, err := b.resolveAdminAPIKey()

	require.NoError(t, err)
	require.Empty(t, apiKey)
	logger.AssertNumberOfCalls(t, "Warnf", 1)
}

// newTestBlockchainForNotifications builds a minimal Blockchain sufficient to
// exercise SendNotification's validation and channel-send logic directly,
// without a running gRPC server or store.
func newTestBlockchainForNotifications(t *testing.T, bufferSize int) *Blockchain {
	t.Helper()

	initPrometheusMetrics()

	return &Blockchain{
		logger:        ulogger.TestLogger{},
		notifications: make(chan *blockchain_api.Notification, bufferSize),
	}
}

func TestSendNotification_Validation(t *testing.T) {
	ctx := context.Background()
	validHash := make([]byte, chainhash.HashSize)

	t.Run("nil request is rejected", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		_, err := b.SendNotification(ctx, nil)
		require.Error(t, err)
	})

	t.Run("unrecognized type is rejected", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		_, err := b.SendNotification(ctx, &blockchain_api.Notification{
			Type: model.NotificationType(999),
			Hash: validHash,
		})
		require.Error(t, err)
	})

	t.Run("PING with a hash is rejected", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		_, err := b.SendNotification(ctx, &blockchain_api.Notification{
			Type: model.NotificationType_PING,
			Hash: validHash,
		})
		require.Error(t, err)
	})

	t.Run("non-PING with an empty hash is rejected", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		_, err := b.SendNotification(ctx, &blockchain_api.Notification{
			Type: model.NotificationType_Block,
		})
		require.Error(t, err)
	})

	t.Run("non-PING with a short hash is rejected", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		_, err := b.SendNotification(ctx, &blockchain_api.Notification{
			Type: model.NotificationType_Block,
			Hash: []byte{1, 2, 3},
		})
		require.Error(t, err)
	})

	t.Run("valid PING notification is accepted", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		_, err := b.SendNotification(ctx, &blockchain_api.Notification{
			Type: model.NotificationType_PING,
		})
		require.NoError(t, err)
	})

	t.Run("valid Block notification is accepted", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		_, err := b.SendNotification(ctx, &blockchain_api.Notification{
			Type: model.NotificationType_Block,
			Hash: validHash,
		})
		require.NoError(t, err)
	})
}

// TestSendNotification_NonBlockingWhenChannelFull verifies that a flood of
// calls degrades gracefully (dropping notifications once the channel buffer
// is full) rather than blocking the RPC handler goroutine.
func TestSendNotification_NonBlockingWhenChannelFull(t *testing.T) {
	ctx := context.Background()
	validHash := make([]byte, chainhash.HashSize)

	b := newTestBlockchainForNotifications(t, 1)

	notification := &blockchain_api.Notification{
		Type: model.NotificationType_Block,
		Hash: validHash,
	}

	// Fill the buffer.
	_, err := b.SendNotification(ctx, notification)
	require.NoError(t, err)

	// The channel is now full; without the non-blocking select this call
	// would hang forever since nothing is draining the channel.
	done := make(chan struct{})
	go func() {
		_, err := b.SendNotification(ctx, notification)
		require.NoError(t, err)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("SendNotification blocked on a full channel instead of dropping the notification")
	}
}

func TestSendNotification_PayloadBounds(t *testing.T) {
	ctx := context.Background()
	validHash := make([]byte, chainhash.HashSize)

	t.Run("oversized base_URL is rejected", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		_, err := b.SendNotification(ctx, &blockchain_api.Notification{
			Type:     model.NotificationType_Block,
			Hash:     validHash,
			Base_URL: strings.Repeat("a", maxNotificationBaseURLLen+1),
		})
		require.Error(t, err)
	})

	t.Run("too many metadata entries are rejected", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		md := make(map[string]string, maxNotificationMetadataEntries+1)
		for i := 0; i <= maxNotificationMetadataEntries; i++ {
			md[strconv.Itoa(i)] = "v"
		}

		_, err := b.SendNotification(ctx, &blockchain_api.Notification{
			Type:     model.NotificationType_Block,
			Hash:     validHash,
			Metadata: &blockchain_api.NotificationMetadata{Metadata: md},
		})
		require.Error(t, err)
	})

	t.Run("oversized metadata value is rejected", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		_, err := b.SendNotification(ctx, &blockchain_api.Notification{
			Type: model.NotificationType_Block,
			Hash: validHash,
			Metadata: &blockchain_api.NotificationMetadata{
				Metadata: map[string]string{"k": strings.Repeat("v", maxNotificationMetadataFieldLen+1)},
			},
		})
		require.Error(t, err)
	})

	t.Run("bounded base_URL and metadata are accepted", func(t *testing.T) {
		b := newTestBlockchainForNotifications(t, 1)

		_, err := b.SendNotification(ctx, &blockchain_api.Notification{
			Type:     model.NotificationType_Block,
			Hash:     validHash,
			Base_URL: "http://example.com",
			Metadata: &blockchain_api.NotificationMetadata{Metadata: map[string]string{"k": "v"}},
		})
		require.NoError(t, err)
	})
}

// TestSanitizeSubscriberSource pins that a caller-supplied Subscribe source
// cannot forge log lines with embedded newlines, cannot grow without bound, and
// cannot become an unbounded Prometheus label value.
func TestSanitizeSubscriberSource(t *testing.T) {
	require.Equal(t, "unknown", sanitizeSubscriberSource(""))
	require.Equal(t, "unknown", sanitizeSubscriberSource("\n\r\t"))
	require.Equal(t, "p2pServer", sanitizeSubscriberSource("p2pServer"))
	require.Equal(t, "evilINFO fake log line", sanitizeSubscriberSource("evil\nINFO fake log line"))
	require.Len(t, sanitizeSubscriberSource(strings.Repeat("x", 1024)), 64)

	// A multi-byte rune sitting exactly on the 64-byte truncation boundary
	// must not be split in half: the result must stay valid UTF-8, or
	// protobuf refuses to marshal it into GetSubscribersResponse.sources.
	multiByte := strings.Repeat("a", 63) + "é" // 'é' is 2 bytes, so byte 64 lands mid-rune
	sanitized := sanitizeSubscriberSource(multiByte)
	require.True(t, utf8.ValidString(sanitized), "sanitized source must be valid UTF-8, got %q", sanitized)
	_, err := proto.Marshal(&blockchain_api.GetSubscribersResponse{Sources: []string{sanitized}})
	require.NoError(t, err, "sanitized source must marshal into the public GetSubscribers response")

	require.Equal(t, SubscriberP2P, metricSourceLabel(SubscriberP2P))
	require.Equal(t, "other", metricSourceLabel(strings.Repeat("x", 64)))
	require.Equal(t, "other", metricSourceLabel("unknown"))
}

// TestGRPCAuthIsWiredIntoStart is the regression test the classification map
// itself cannot provide: it starts the real blockchain gRPC server through
// Start() -> util.StartGRPCServer with a key configured, dials it, and asserts
// a protected RPC is rejected without the header and accepted with it.
// Reverting the authOptions argument in Start() to nil fails here, which the
// map-only tests do not catch.
func TestGRPCAuthIsWiredIntoStart(t *testing.T) {
	const apiKey = "wiring-test-admin-key"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.NewErrorTestLogger(t)

	tSettings := test.CreateBaseTestSettings(t)
	// A unique settings context keeps util.GetListener's listener cache from
	// colliding with other tests in this package.
	tSettings.Context = "auth-wiring-test"
	tSettings.GRPCAdminAPIKey = apiKey
	tSettings.BlockChain.GRPCListenAddress = "127.0.0.1:0"
	tSettings.BlockChain.HTTPListenAddress = "127.0.0.1:0"

	storeURL, err := url.Parse("sqlitememory:///blockchain_auth_wiring")
	require.NoError(t, err)

	blockchainStore, err := sqlstore.New(logger, storeURL, tSettings)
	require.NoError(t, err)

	server, err := New(ctx, logger, tSettings, blockchainStore, kafka.NewKafkaAsyncProducerMock())
	require.NoError(t, err)
	require.NoError(t, server.Init(ctx))

	readyCh := make(chan struct{})

	go func() {
		_ = server.Start(ctx, readyCh)
	}()

	select {
	case <-readyCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for blockchain server to start")
	}

	defer util.RemoveListener(tSettings.Context, "blockchain", "")
	defer util.RemoveListener(tSettings.Context, "blockchain", "http://")

	// The listener is cached by util.GetListener, so asking for it again
	// returns the one the server is serving on, along with its real address.
	_, address, _, err := util.GetListener(tSettings.Context, "blockchain", "", tSettings.BlockChain.GRPCListenAddress)
	require.NoError(t, err)

	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	defer func() {
		_ = conn.Close()
	}()

	client := blockchain_api.NewBlockchainAPIClient(conn)

	notification := &blockchain_api.Notification{
		Type: model.NotificationType_Block,
		Hash: make([]byte, chainhash.HashSize),
	}

	callCtx, callCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer callCancel()

	_, err = client.SendNotification(callCtx, notification)
	require.Equal(t, codes.Unauthenticated, status.Code(err), "SendNotification must be rejected without the API key - protectedMethods is not wired into Start()")

	authedCtx := metadata.AppendToOutgoingContext(callCtx, "x-api-key", apiKey)
	_, err = client.SendNotification(authedCtx, notification)
	require.NoError(t, err, "SendNotification must succeed with the configured API key")

	// A public RPC on the same listener must stay reachable without a key.
	_, err = client.GetFSMCurrentState(callCtx, &emptypb.Empty{})
	require.NotEqual(t, codes.Unauthenticated, status.Code(err), "a public RPC must not require the API key")
}

// TestHTTPAdminRoutesRequireAPIKey covers the second door onto
// InvalidateBlock/RevalidateBlock: the blockchain service's own HTTP listener.
// The gRPC auth interceptor never runs on that path, so the routes carry their
// own check. They are also POST-only, so a bare <img src="..."> on a page an
// operator visits cannot fire them.
func TestHTTPAdminRoutesRequireAPIKey(t *testing.T) {
	const apiKey = "http-admin-key"

	newServer := func(t *testing.T, key string) (*Blockchain, *echo.Echo) {
		t.Helper()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.GRPCAdminAPIKey = key

		b := &Blockchain{logger: ulogger.TestLogger{}, settings: tSettings}

		e := echo.New()
		e.POST("/invalidate/:hash", func(c echo.Context) error {
			return c.String(http.StatusOK, "reached handler")
		}, b.requireAdminAPIKey)

		return b, e
	}

	hash := chainhash.Hash{}.String()

	t.Run("no header is rejected", func(t *testing.T) {
		_, e := newServer(t, apiKey)

		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/invalidate/"+hash, nil))
		require.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("wrong key is rejected", func(t *testing.T) {
		_, e := newServer(t, apiKey)

		req := httptest.NewRequest(http.MethodPost, "/invalidate/"+hash, nil)
		req.Header.Set("x-api-key", "wrong")

		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		require.Equal(t, http.StatusUnauthorized, rec.Code)
	})

	t.Run("correct key reaches the handler", func(t *testing.T) {
		_, e := newServer(t, apiKey)

		req := httptest.NewRequest(http.MethodPost, "/invalidate/"+hash, nil)
		req.Header.Set("x-api-key", apiKey)

		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("unset key closes the route rather than opening it", func(t *testing.T) {
		_, e := newServer(t, "")

		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/invalidate/"+hash, nil))
		require.Equal(t, http.StatusForbidden, rec.Code)
	})

	t.Run("GET is not routed", func(t *testing.T) {
		b, _ := newServer(t, apiKey)

		// Build the real route table so the GET/POST decision is the
		// production one rather than the test's.
		e := echo.New()
		e.POST("/invalidate/:hash", b.invalidateHandler, b.requireAdminAPIKey)
		e.POST("/revalidate/:hash", b.revalidateHandler, b.requireAdminAPIKey)

		for _, path := range []string{"/invalidate/" + hash, "/revalidate/" + hash} {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			req.Header.Set("x-api-key", apiKey)

			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)
			require.Equal(t, http.StatusMethodNotAllowed, rec.Code, "%s must not be reachable by GET", path)
		}
	})
}

// TestGRPCPanicRecoveryKeepsProcessAlive proves the recovery interceptor
// installed by util.StartGRPCServer turns a panicking handler into an Internal
// error. Without it, grpc-go lets the panic unwind and the process dies.
func TestGRPCPanicRecoveryKeepsProcessAlive(t *testing.T) {
	interceptor := util.CreatePanicRecoveryUnaryInterceptor(ulogger.TestLogger{}, "blockchain")

	_, err := interceptor(context.Background(), nil,
		&grpc.UnaryServerInfo{FullMethod: "/blockchain_api.BlockchainAPI/GetBlockIsMined"},
		func(ctx context.Context, req any) (any, error) {
			var short []byte
			_ = chainhash.Hash(short) //nolint:staticcheck // deliberately panics

			return nil, nil
		})

	require.Equal(t, codes.Internal, status.Code(err))
}

// TestHashValidationRejectsShortHashes covers every handler that converts a
// caller-supplied slice into a chainhash.Hash. Before validation these panicked
// on any slice shorter than 32 bytes, killing the process - and three of them
// (GetBlockIsMined, GetSuitableBlock, GetHashOfAncestorBlock) are public, so no
// API key was needed to do it.
func TestHashValidationRejectsShortHashes(t *testing.T) {
	ctx := context.Background()

	b := &Blockchain{logger: ulogger.TestLogger{}, stats: gocore.NewStat("test")}

	for _, short := range [][]byte{nil, {}, {1, 2, 3}, make([]byte, chainhash.HashSize-1)} {
		require.NotPanics(t, func() {
			_, err := b.GetBlockIsMined(ctx, &blockchain_api.GetBlockIsMinedRequest{BlockHash: short})
			require.Error(t, err)

			_, err = b.SetBlockMinedSet(ctx, &blockchain_api.SetBlockMinedSetRequest{BlockHash: short})
			require.Error(t, err)

			_, err = b.ClearBlockMinedSet(ctx, &blockchain_api.ClearBlockMinedSetRequest{BlockHash: short})
			require.Error(t, err)

			_, err = b.SetBlockPersistedAt(ctx, &blockchain_api.SetBlockPersistedAtRequest{BlockHash: short})
			require.Error(t, err)

			_, err = b.SetBlockSubtreesSet(ctx, &blockchain_api.SetBlockSubtreesSetRequest{BlockHash: short})
			require.Error(t, err)

			_, err = b.GetSuitableBlock(ctx, &blockchain_api.GetSuitableBlockRequest{Hash: short})
			require.Error(t, err)

			_, err = b.GetHashOfAncestorBlock(ctx, &blockchain_api.GetHashOfAncestorBlockRequest{Hash: short, Depth: 1})
			require.Error(t, err)
		}, "short hash of length %d must be rejected, not panic", len(short))
	}
}

// TestHeightRangeBoundsRejectUnboundedRequests covers the unauthenticated
// range-query DoS surface: a reversed range used to underflow uint32
// subtraction into a ~4 billion element preallocation.
func TestHeightRangeBoundsRejectUnboundedRequests(t *testing.T) {
	ctx := context.Background()

	b := &Blockchain{logger: ulogger.TestLogger{}, stats: gocore.NewStat("test")}

	_, err := b.GetBlocksByHeight(ctx, &blockchain_api.GetBlocksByHeightRequest{StartHeight: 2, EndHeight: 0})
	require.Error(t, err, "reversed range must be rejected before it reaches the store")

	_, err = b.GetBlocksByHeight(ctx, &blockchain_api.GetBlocksByHeightRequest{StartHeight: 0, EndHeight: 4294967294})
	require.Error(t, err, "an oversized range must be rejected")

	// The reversed header range keeps its long-standing empty-result contract
	// (callers compute these bounds), but must never reach the store's capacity
	// calculation, where it used to underflow into a ~4 billion preallocation.
	resp, err := b.GetBlockHeadersByHeight(ctx, &blockchain_api.GetBlockHeadersByHeightRequest{StartHeight: 2, EndHeight: 0})
	require.NoError(t, err)
	require.Empty(t, resp.BlockHeaders)
	// The store-side preallocation bound for the same range is covered by
	// TestPreallocBounds in stores/blockchain/sql.

	_, err = b.GetMedianTimePastByHeights(ctx, &blockchain_api.GetMedianTimePastByHeightsRequest{
		Heights: make([]uint32, defaultMaxMedianTimePastHeights+1),
	})
	require.Error(t, err, "an oversized heights list must be rejected")

	_, err = b.GetBlockHeaders(ctx, &blockchain_api.GetBlockHeadersRequest{
		StartHash:       make([]byte, chainhash.HashSize),
		NumberOfHeaders: maxBlockHeadersPerRequest + 1,
	})
	require.Error(t, err, "an oversized numberOfHeaders must be rejected before it reaches the store LIMIT")

	_, err = b.GetBlockHeadersFromOldestRequest(ctx, &blockchain_api.GetBlockHeadersFromOldestRequest{
		ChainTipHash:    make([]byte, chainhash.HashSize),
		TargetHash:      make([]byte, chainhash.HashSize),
		NumberOfHeaders: maxBlockHeadersPerRequest + 1,
	})
	require.Error(t, err, "an oversized numberOfHeaders must be rejected before GetBlockHeadersFromOldest reaches the store")

	_, err = b.GetBlockHeaderIDs(ctx, &blockchain_api.GetBlockHeadersRequest{
		StartHash:       make([]byte, chainhash.HashSize),
		NumberOfHeaders: maxBlockHeadersPerRequest + 1,
	})
	require.Error(t, err, "an oversized numberOfHeaders must be rejected before GetBlockHeaderIDs reaches the store")

	_, err = b.LocateBlockHeaders(ctx, &blockchain_api.LocateBlockHeadersRequest{
		HashStop:  make([]byte, chainhash.HashSize),
		MaxHashes: maxBlockHeadersPerRequest + 1,
	})
	require.Error(t, err, "an oversized maxHashes must be rejected before LocateBlockHeaders reaches the store")
}

// TestGetMedianTimePastByHeightsRejectsOversizedSpan covers the span this PR's
// count cap does not: len(req.Heights) can be tiny while minHeight/maxHeight
// still drive a whole-chain header read and dense cache write.
func TestGetMedianTimePastByHeightsRejectsOversizedSpan(t *testing.T) {
	ctx := context.Background()

	b := &Blockchain{logger: ulogger.TestLogger{}, stats: gocore.NewStat("test")}

	_, err := b.GetMedianTimePastByHeights(ctx, &blockchain_api.GetMedianTimePastByHeightsRequest{
		Heights: []uint32{0, 920000},
	})
	require.Error(t, err, "a two-element request spanning the whole chain must be rejected, not just a request with many heights")
}
