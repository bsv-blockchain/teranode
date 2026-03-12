package banlist

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcpeer "google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func newTestBanList(t *testing.T) *BanList {
	t.Helper()

	storeURL, err := url.Parse("sqlitememory://")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)

	store, err := blockchain.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	bl := New(store.GetDB(), util.SqliteMemory, ulogger.TestLogger{})

	err = bl.Init(context.Background())
	require.NoError(t, err)

	return bl
}

func TestBanList_AddAndIsBanned(t *testing.T) {
	bl := newTestBanList(t)

	// Ban an IP
	err := bl.Add(context.Background(), "192.168.1.1", time.Now().Add(time.Hour))
	require.NoError(t, err)

	require.True(t, bl.IsBanned("192.168.1.1"))
	require.False(t, bl.IsBanned("192.168.1.2"))

	// Ban a subnet
	err = bl.Add(context.Background(), "10.0.0.0/24", time.Now().Add(time.Hour))
	require.NoError(t, err)

	require.True(t, bl.IsBanned("10.0.0.5"))
	require.False(t, bl.IsBanned("10.0.1.1"))
}

func TestBanList_IsBannedWithPort(t *testing.T) {
	bl := newTestBanList(t)

	err := bl.Add(context.Background(), "192.168.1.1", time.Now().Add(time.Hour))
	require.NoError(t, err)

	require.True(t, bl.IsBanned("192.168.1.1:8333"))
	require.False(t, bl.IsBanned("192.168.1.2:8333"))
}

func TestBanList_IsBannedEmpty(t *testing.T) {
	bl := newTestBanList(t)
	require.False(t, bl.IsBanned(""))
}

func TestBanList_Remove(t *testing.T) {
	bl := newTestBanList(t)

	err := bl.Add(context.Background(), "192.168.1.1", time.Now().Add(time.Hour))
	require.NoError(t, err)
	require.True(t, bl.IsBanned("192.168.1.1"))

	err = bl.Remove(context.Background(), "192.168.1.1")
	require.NoError(t, err)
	require.False(t, bl.IsBanned("192.168.1.1"))
}

func TestBanList_ListBanned(t *testing.T) {
	bl := newTestBanList(t)

	err := bl.Add(context.Background(), "1.2.3.4", time.Now().Add(time.Hour))
	require.NoError(t, err)
	err = bl.Add(context.Background(), "10.0.0.0/24", time.Now().Add(time.Hour))
	require.NoError(t, err)

	banned := bl.ListBanned()
	require.Len(t, banned, 2)
}

func TestBanList_Clear(t *testing.T) {
	bl := newTestBanList(t)

	err := bl.Add(context.Background(), "1.2.3.4", time.Now().Add(time.Hour))
	require.NoError(t, err)

	bl.Clear()
	require.Empty(t, bl.ListBanned())
	require.False(t, bl.IsBanned("1.2.3.4"))
}

func TestBanList_LoadFromDatabase(t *testing.T) {
	bl := newTestBanList(t)

	err := bl.Add(context.Background(), "192.168.1.1", time.Now().Add(time.Hour))
	require.NoError(t, err)
	err = bl.Add(context.Background(), "10.0.0.0/24", time.Now().Add(time.Hour))
	require.NoError(t, err)

	// Reload from database (simulates cross-service sync)
	err = bl.LoadFromDatabase(context.Background())
	require.NoError(t, err)

	require.Len(t, bl.BannedPeers(), 2)
}

func TestBanList_ReloadFromDatabase(t *testing.T) {
	bl := newTestBanList(t)

	err := bl.Add(context.Background(), "192.168.1.1", time.Now().Add(time.Hour))
	require.NoError(t, err)

	// Simulate external change by directly inserting into DB
	ctx := context.Background()
	subnet, _ := parseAddress("10.0.0.0/24")
	_, err = bl.db.ExecContext(ctx, `
		INSERT INTO bans (key, expiration_time, subnet)
		VALUES ($1, $2, $3)
	`, "10.0.0.0/24", time.Now().Add(time.Hour).Format(time.RFC3339), subnet.String())
	require.NoError(t, err)

	// Reload picks up the new entry
	err = bl.reloadFromDatabase()
	require.NoError(t, err)

	require.True(t, bl.IsBanned("10.0.0.5"))
	require.Len(t, bl.BannedPeers(), 2)
}

func TestBanList_PeriodicReload(t *testing.T) {
	bl := newTestBanList(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bl.StartPeriodicReload(ctx, 50*time.Millisecond)
	defer bl.Stop()

	// Insert directly into DB
	subnet, _ := parseAddress("10.0.0.0/24")
	_, err := bl.db.ExecContext(ctx, `
		INSERT INTO bans (key, expiration_time, subnet)
		VALUES ($1, $2, $3)
	`, "10.0.0.0/24", time.Now().Add(time.Hour).Format(time.RFC3339), subnet.String())
	require.NoError(t, err)

	// Wait for a reload cycle
	time.Sleep(150 * time.Millisecond)

	require.True(t, bl.IsBanned("10.0.0.5"))
}

func TestBanList_Subscribe(t *testing.T) {
	bl := newTestBanList(t)

	ch := bl.Subscribe()
	defer bl.Unsubscribe(ch)

	err := bl.Add(context.Background(), "1.2.3.4", time.Now().Add(time.Hour))
	require.NoError(t, err)

	select {
	case event := <-ch:
		require.Equal(t, "add", event.Action)
		require.Equal(t, "1.2.3.4", event.IP)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestBanList_IPv6(t *testing.T) {
	bl := newTestBanList(t)

	err := bl.Add(context.Background(), "2406:da18:1f7:353a:b079:da22:c7d5:e166", time.Now().Add(time.Hour))
	require.NoError(t, err)

	require.True(t, bl.IsBanned("2406:da18:1f7:353a:b079:da22:c7d5:e166"))
	require.True(t, bl.IsBanned("[2406:da18:1f7:353a:b079:da22:c7d5:e166]:8333"))
	require.False(t, bl.IsBanned("2406:da18:1f7:353a:b079:da22:c7d5:e167"))
}

func TestBanList_ExpiredBan(t *testing.T) {
	bl := newTestBanList(t)

	// Add a ban that expires immediately
	err := bl.Add(context.Background(), "1.2.3.4", time.Now().Add(-time.Second))
	require.NoError(t, err)

	require.False(t, bl.IsBanned("1.2.3.4"))
}

// --- Middleware tests ---

func TestEchoMiddleware_BannedIP(t *testing.T) {
	bl := newTestBanList(t)

	err := bl.Add(context.Background(), "1.2.3.4", time.Now().Add(time.Hour))
	require.NoError(t, err)

	e := echo.New()
	e.Use(CreateEchoMiddleware(bl))
	e.GET("/test", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.RemoteAddr = "1.2.3.4:12345"
	rec := httptest.NewRecorder()

	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestEchoMiddleware_AllowedIP(t *testing.T) {
	bl := newTestBanList(t)

	e := echo.New()
	e.Use(CreateEchoMiddleware(bl))
	e.GET("/test", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.RemoteAddr = "5.6.7.8:12345"
	rec := httptest.NewRecorder()

	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

// --- gRPC interceptor tests ---

func TestGRPCInterceptor_BannedIP(t *testing.T) {
	bl := newTestBanList(t)

	err := bl.Add(context.Background(), "1.2.3.4", time.Now().Add(time.Hour))
	require.NoError(t, err)

	interceptor := CreateGRPCUnaryInterceptor(bl)

	// Create a context with peer info
	addr, _ := net.ResolveTCPAddr("tcp", "1.2.3.4:12345")
	ctx := grpcpeer.NewContext(context.Background(), &grpcpeer.Peer{Addr: addr})

	_, err = interceptor(ctx, nil, nil, func(ctx context.Context, req any) (any, error) {
		t.Fatal("handler should not be called for banned IP")
		return nil, nil
	})

	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.PermissionDenied, st.Code())
}

func TestGRPCInterceptor_AllowedIP(t *testing.T) {
	bl := newTestBanList(t)

	interceptor := CreateGRPCUnaryInterceptor(bl)

	addr, _ := net.ResolveTCPAddr("tcp", "5.6.7.8:12345")
	ctx := grpcpeer.NewContext(context.Background(), &grpcpeer.Peer{Addr: addr})

	called := false
	_, err := interceptor(ctx, nil, nil, func(ctx context.Context, req any) (any, error) {
		called = true
		return "ok", nil
	})

	require.NoError(t, err)
	require.True(t, called)
}

// --- Error path tests ---

func TestBanList_AddInvalidIP(t *testing.T) {
	bl := newTestBanList(t)

	err := bl.Add(context.Background(), "not-an-ip", time.Now().Add(time.Hour))
	require.Error(t, err)
}

func TestBanList_AddInvalidSubnet(t *testing.T) {
	bl := newTestBanList(t)

	err := bl.Add(context.Background(), "10.0.0.0/33", time.Now().Add(time.Hour))
	require.Error(t, err)
}

func TestBanList_RemoveNonExistent(t *testing.T) {
	bl := newTestBanList(t)

	// Should be a no-op, not an error
	err := bl.Remove(context.Background(), "1.2.3.4")
	require.NoError(t, err)
}

func TestBanList_AddUpdatesExpiration(t *testing.T) {
	bl := newTestBanList(t)

	// Add with short expiration
	err := bl.Add(context.Background(), "1.2.3.4", time.Now().Add(time.Second))
	require.NoError(t, err)
	require.True(t, bl.IsBanned("1.2.3.4"))

	// Re-add with longer expiration - should update, not duplicate
	err = bl.Add(context.Background(), "1.2.3.4", time.Now().Add(time.Hour))
	require.NoError(t, err)

	banned := bl.ListBanned()
	require.Len(t, banned, 1)
	require.True(t, bl.IsBanned("1.2.3.4"))
}

func TestBanList_IsBannedInvalidIP(t *testing.T) {
	bl := newTestBanList(t)

	// Invalid IP should return false, not panic
	require.False(t, bl.IsBanned("not-an-ip"))
}

// --- Additional middleware/interceptor tests ---

func TestEchoMiddleware_SubnetBan(t *testing.T) {
	bl := newTestBanList(t)

	err := bl.Add(context.Background(), "10.0.0.0/24", time.Now().Add(time.Hour))
	require.NoError(t, err)

	e := echo.New()
	e.Use(CreateEchoMiddleware(bl))
	e.GET("/test", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	// IP within banned subnet
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.RemoteAddr = "10.0.0.42:12345"
	rec := httptest.NewRecorder()

	e.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)

	// IP outside banned subnet
	req2 := httptest.NewRequest(http.MethodGet, "/test", nil)
	req2.RemoteAddr = "10.0.1.1:12345"
	rec2 := httptest.NewRecorder()

	e.ServeHTTP(rec2, req2)
	require.Equal(t, http.StatusOK, rec2.Code)
}

func TestGRPCInterceptor_NoPeerContext(t *testing.T) {
	bl := newTestBanList(t)

	err := bl.Add(context.Background(), "1.2.3.4", time.Now().Add(time.Hour))
	require.NoError(t, err)

	interceptor := CreateGRPCUnaryInterceptor(bl)

	// No peer info in context - should pass through
	called := false
	_, err = interceptor(context.Background(), nil, nil, func(ctx context.Context, req any) (any, error) {
		called = true
		return "ok", nil
	})

	require.NoError(t, err)
	require.True(t, called)
}

// --- Subscription lifecycle tests ---

func TestBanList_SubscribeReceivesRemoveEvent(t *testing.T) {
	bl := newTestBanList(t)

	ch := bl.Subscribe()
	defer bl.Unsubscribe(ch)

	err := bl.Add(context.Background(), "1.2.3.4", time.Now().Add(time.Hour))
	require.NoError(t, err)

	// Drain the add event
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for add event")
	}

	err = bl.Remove(context.Background(), "1.2.3.4")
	require.NoError(t, err)

	select {
	case event := <-ch:
		require.Equal(t, "remove", event.Action)
		require.Equal(t, "1.2.3.4", event.IP)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for remove event")
	}
}

func TestBanList_UnsubscribeStopsDelivery(t *testing.T) {
	bl := newTestBanList(t)

	ch := bl.Subscribe()
	bl.Unsubscribe(ch)

	err := bl.Add(context.Background(), "1.2.3.4", time.Now().Add(time.Hour))
	require.NoError(t, err)

	// Give async notification time to (not) arrive
	time.Sleep(100 * time.Millisecond)

	select {
	case <-ch:
		t.Fatal("should not receive events after unsubscribe")
	default:
		// expected - channel is empty
	}
}

func TestBanList_StopHaltsReload(t *testing.T) {
	bl := newTestBanList(t)

	ctx := context.Background()
	bl.StartPeriodicReload(ctx, 50*time.Millisecond)
	bl.Stop()

	// Insert directly into DB after stop
	subnet, _ := parseAddress("10.0.0.0/24")
	_, err := bl.db.ExecContext(ctx, `
		INSERT INTO bans (key, expiration_time, subnet)
		VALUES ($1, $2, $3)
	`, "10.0.0.0/24", time.Now().Add(time.Hour).Format(time.RFC3339), subnet.String())
	require.NoError(t, err)

	// Wait longer than reload interval
	time.Sleep(150 * time.Millisecond)

	// Should NOT have been picked up since reload was stopped
	require.False(t, bl.IsBanned("10.0.0.5"))
}

// --- Race condition test ---

func TestBanList_ConcurrentAccess(t *testing.T) {
	bl := newTestBanList(t)

	const numIterations = 500
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < numIterations; i++ {
			ip := fmt.Sprintf("192.168.1.%d", i%255)
			_ = bl.Add(context.Background(), ip, time.Now().Add(time.Hour))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < numIterations; i++ {
			ip := fmt.Sprintf("192.168.1.%d", i%255)
			_ = bl.IsBanned(ip)
		}
	}()

	wg.Wait()
}
