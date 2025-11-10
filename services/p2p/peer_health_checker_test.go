package p2p

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeerHealthChecker_NewPeerHealthChecker(t *testing.T) {
	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()

	hc := NewPeerHealthChecker(logger, registry, settings)

	assert.NotNil(t, hc)
	assert.Equal(t, logger, hc.logger)
	assert.Equal(t, registry, hc.registry)
	assert.Equal(t, settings, hc.settings)
	assert.NotNil(t, hc.httpClient)
	assert.Equal(t, 30*time.Second, hc.checkInterval)
}

func TestPeerHealthChecker_StartAndStop(t *testing.T) {
	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()

	hc := NewPeerHealthChecker(logger, registry, settings)
	hc.checkInterval = 100 * time.Millisecond // Speed up for testing

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start the health checker
	hc.Start(ctx)

	// Let it run for a bit
	time.Sleep(50 * time.Millisecond)

	// Stop it
	hc.Stop()

	// Should not panic and should stop cleanly
	assert.True(t, true, "Health checker stopped cleanly")
}

func TestPeerHealthChecker_isDataHubReachable_Success(t *testing.T) {
	// Create test HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}))
	defer server.Close()

	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()

	hc := NewPeerHealthChecker(logger, registry, settings)

	// Test successful connection
	duration, reachable := hc.isDataHubReachable(server.URL)
	assert.True(t, reachable, "DataHub should be reachable")
	assert.Greater(t, duration, time.Duration(0), "Duration should be greater than zero")
}

func TestPeerHealthChecker_isDataHubReachable_Failure(t *testing.T) {
	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()

	hc := NewPeerHealthChecker(logger, registry, settings)

	// Test unreachable URL
	duration, reachable := hc.isDataHubReachable("http://localhost:99999")
	assert.False(t, reachable, "DataHub should not be reachable")
	assert.Equal(t, time.Duration(0), duration, "Duration should be zero on failure")
}

func TestPeerHealthChecker_isDataHubReachable_EmptyURL(t *testing.T) {
	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()

	hc := NewPeerHealthChecker(logger, registry, settings)

	// Empty URL should be considered healthy
	duration, reachable := hc.isDataHubReachable("")
	assert.True(t, reachable, "Empty URL should be considered healthy")
	assert.Equal(t, time.Duration(0), duration, "Duration should be zero for empty URL")
}

func TestPeerHealthChecker_isDataHubReachable_404Offline(t *testing.T) {
	// Create test HTTP server that returns offline indication
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("Tunnel offline"))
	}))
	defer server.Close()

	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()

	hc := NewPeerHealthChecker(logger, registry, settings)

	// Should detect offline status
	duration, reachable := hc.isDataHubReachable(server.URL)
	assert.False(t, reachable, "DataHub should be detected as offline")
	assert.Equal(t, duration, time.Duration(0), "Duration should be zero for offline")
}

func TestPeerHealthChecker_isDataHubReachable_404Normal(t *testing.T) {
	// Create test HTTP server that returns normal 404
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("Not Found"))
	}))
	defer server.Close()

	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()

	hc := NewPeerHealthChecker(logger, registry, settings)

	// Normal 404 should still be considered reachable
	duration, reachable := hc.isDataHubReachable(server.URL)
	assert.True(t, reachable, "Normal 404 should still be considered reachable")
	assert.Greater(t, duration, time.Duration(0), "Duration should be greater than zero")
}

func TestPeerHealthChecker_isDataHubReachable_500Error(t *testing.T) {
	// Create test HTTP server that returns 500 error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("Internal Server Error"))
	}))
	defer server.Close()

	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()

	hc := NewPeerHealthChecker(logger, registry, settings)

	// 500 errors should be considered unreachable
	duration, reachable := hc.isDataHubReachable(server.URL)
	assert.False(t, reachable, "500 errors should be considered unreachable")
	assert.Greater(t, duration, time.Duration(0), "Duration should be greater than zero")
}

func TestPeerHealthChecker_checkPeerHealth(t *testing.T) {
	// Create test HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()

	hc := NewPeerHealthChecker(logger, registry, settings)

	// Add peer to registry
	peerID := peer.ID("test-peer")
	registry.AddPeer(peerID)
	registry.UpdateDataHubURL(peerID, server.URL)

	// Get peer info
	peerInfo, exists := registry.GetPeer(peerID)
	require.True(t, exists)

	// Check peer health
	hc.checkPeerHealth(peerInfo)

	// Verify health was updated
	updatedInfo, _ := registry.GetPeer(peerID)
	assert.True(t, updatedInfo.IsHealthy, "Peer should be marked healthy")
	assert.NotZero(t, updatedInfo.LastHealthCheck)
}

func TestPeerHealthChecker_CheckPeerNow(t *testing.T) {
	// Create test HTTP server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()

	hc := NewPeerHealthChecker(logger, registry, settings)

	// Add peer to registry
	peerID := peer.ID("test-peer")
	registry.AddPeer(peerID)
	registry.UpdateDataHubURL(peerID, server.URL)

	// Check specific peer immediately
	hc.CheckPeerNow(peerID)

	// Verify health was updated
	info, _ := registry.GetPeer(peerID)
	assert.True(t, info.IsHealthy, "Peer should be marked healthy")

	// Check non-existent peer (should not panic)
	hc.CheckPeerNow(peer.ID("non-existent"))
}

func TestPeerHealthChecker_checkAllPeers(t *testing.T) {
	// Create test HTTP servers
	successServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer successServer.Close()

	failServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failServer.Close()

	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()
	settings.P2P.PeerHealthCheckInterval = 1 * time.Second
	settings.P2P.PeerHealthHTTPTimeout = 1 * time.Second
	settings.P2P.PeerHealthRemoveAfterFailures = 1

	hc := NewPeerHealthChecker(logger, registry, settings)

	// Add multiple peers with different DataHub URLs
	peerA := peer.ID("peer-a")
	peerB := peer.ID("peer-b")
	peerC := peer.ID("peer-c")
	peerD := peer.ID("peer-d")

	registry.AddPeer(peerA)
	registry.UpdateConnectionState(peerA, true)
	registry.UpdateDataHubURL(peerA, successServer.URL)

	registry.AddPeer(peerB)
	registry.UpdateConnectionState(peerB, true)
	registry.UpdateDataHubURL(peerB, failServer.URL)

	registry.AddPeer(peerC)
	registry.UpdateConnectionState(peerC, true)
	registry.UpdateDataHubURL(peerC, "http://localhost:99999") // Unreachable

	registry.AddPeer(peerD) // No DataHub URL
	registry.UpdateConnectionState(peerD, true)

	// Check all peers
	hc.checkAllPeers()

	// Verify health statuses
	infoA, _ := registry.GetPeer(peerA)
	assert.True(t, infoA.IsHealthy, "Peer A should be healthy")

	infoB, _ := registry.GetPeer(peerB)
	assert.False(t, infoB.IsHealthy, "Peer B should be unhealthy (500 error)")

	infoC, _ := registry.GetPeer(peerC)
	assert.False(t, infoC.IsHealthy, "Peer C should be unhealthy (unreachable)")

	infoD, _ := registry.GetPeer(peerD)
	assert.True(t, infoD.IsHealthy, "Peer D should remain healthy (no DataHub)")
}

func TestPeerHealthChecker_ConcurrentHealthChecks(t *testing.T) {
	// Create test HTTP server with delay to test concurrency
	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		time.Sleep(10 * time.Millisecond) // Small delay to simulate processing
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()

	hc := NewPeerHealthChecker(logger, registry, settings)

	// Add many peers
	for i := 0; i < 20; i++ {
		peerID := peer.ID(string(rune('A' + i)))
		registry.AddPeer(peerID)
		registry.UpdateConnectionState(peerID, true)
		registry.UpdateDataHubURL(peerID, server.URL)
	}

	// Check all peers concurrently
	hc.checkAllPeers()

	// All peers should be checked
	peers := registry.GetAllPeers()
	for _, p := range peers {
		assert.True(t, p.IsHealthy, "All peers should be marked healthy")
		assert.NotZero(t, p.LastHealthCheck, "Health check timestamp should be set")
	}
}

func TestPeerHealthChecker_HealthCheckLoop(t *testing.T) {
	// Create test HTTP server
	var checkCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&checkCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()

	hc := NewPeerHealthChecker(logger, registry, settings)
	hc.checkInterval = 50 * time.Millisecond // Speed up for testing

	// Add peer
	peerID := peer.ID("test-peer")
	registry.AddPeer(peerID)
	registry.UpdateConnectionState(peerID, true)
	registry.UpdateDataHubURL(peerID, server.URL)

	// Start health checker
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	hc.Start(ctx)

	// Wait for multiple check intervals (allow for race detector overhead)
	time.Sleep(200 * time.Millisecond)

	// Stop health checker
	hc.Stop()

	// Should have performed multiple health checks
	// Initial check + at least 1-2 interval checks (relaxed for race detector overhead)
	assert.GreaterOrEqual(t, int(atomic.LoadInt32(&checkCount)), 2, "Should have performed multiple health checks")
}

func TestPeerHealthChecker_DoesNotRemoveAfterConsecutiveFailures(t *testing.T) {
	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()
	// Set low threshold to speed test
	settings.P2P.PeerHealthRemoveAfterFailures = 2

	hc := NewPeerHealthChecker(logger, registry, settings)

	// Add a peer with unreachable URL
	pid := peer.ID("peer-remove")
	registry.AddPeer(pid)
	registry.UpdateDataHubURL(pid, "http://127.0.0.1:65535") // unreachable port

	// First failure -> should not remove yet
	if p1, ok := registry.GetPeer(pid); ok {
		hc.checkPeerHealth(p1)
	} else {
		t.Fatalf("peer not found")
	}
	if _, ok := registry.GetPeer(pid); !ok {
		t.Fatalf("peer should still exist after first failure")
	}

	// Second consecutive failure -> should NOT remove; peer remains but unhealthy
	if p2, ok := registry.GetPeer(pid); ok {
		hc.checkPeerHealth(p2)
	} else {
		t.Fatalf("peer not found on second check")
	}

	if info, ok := registry.GetPeer(pid); ok {
		assert.False(t, info.IsHealthy, "peer should remain in registry and be marked unhealthy after failures")
	} else {
		t.Fatalf("peer should not be removed after failures")
	}
}

func TestPeerHealthChecker_FailureCountResetsOnSuccess(t *testing.T) {
	// Success after a failure resets the counter so removal requires full threshold again
	// Prepare servers: one failing (500), then success (200), then failing again
	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failSrv.Close()

	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer okSrv.Close()

	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()
	settings.P2P.PeerHealthRemoveAfterFailures = 2
	hc := NewPeerHealthChecker(logger, registry, settings)

	pid := peer.ID("peer-reset")
	registry.AddPeer(pid)

	// First failure
	registry.UpdateDataHubURL(pid, failSrv.URL)
	if p, ok := registry.GetPeer(pid); ok {
		hc.checkPeerHealth(p)
	}
	if _, ok := registry.GetPeer(pid); !ok {
		t.Fatalf("peer should not be removed after first failure")
	}

	// Success, which should reset counter
	registry.UpdateDataHubURL(pid, okSrv.URL)
	if p, ok := registry.GetPeer(pid); ok {
		hc.checkPeerHealth(p)
	}

	// Failure again should be counted as first failure after reset; peer must still exist
	registry.UpdateDataHubURL(pid, failSrv.URL)
	if p, ok := registry.GetPeer(pid); ok {
		hc.checkPeerHealth(p)
	}
	if _, ok := registry.GetPeer(pid); !ok {
		t.Fatalf("peer should still exist; failure counter should have reset after success")
	}
}

func TestPeerHealthChecker_SettingsOverrides(t *testing.T) {
	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()
	// Override values
	settings.P2P.PeerHealthCheckInterval = 123 * time.Millisecond
	settings.P2P.PeerHealthHTTPTimeout = 456 * time.Millisecond
	settings.P2P.PeerHealthRemoveAfterFailures = 7

	hc := NewPeerHealthChecker(logger, registry, settings)

	assert.Equal(t, 123*time.Millisecond, hc.checkInterval)
	// http.Client timeout should match
	assert.Equal(t, 456*time.Millisecond, hc.httpClient.Timeout)
	assert.Equal(t, 7, hc.removeAfterFailures)
}

func TestPeerHealthChecker_HTTPTimeout(t *testing.T) {
	// Server sleeps longer than configured timeout -> unreachable
	sleep := 100 * time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(sleep)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()
	settings.P2P.PeerHealthHTTPTimeout = 20 * time.Millisecond

	hc := NewPeerHealthChecker(logger, registry, settings)

	duration, reachable := hc.isDataHubReachable(srv.URL)
	assert.False(t, reachable, "request should time out and be considered unreachable")
	assert.Equal(t, time.Duration(0), duration, "duration should be zero on timeout")
}

func TestPeerHealthChecker_RedirectHandling(t *testing.T) {
	// Single redirect should be followed and considered reachable
	okSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer okSrv.Close()

	redirectSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, okSrv.URL+r.URL.Path, http.StatusFound)
	}))
	defer redirectSrv.Close()

	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()
	hc := NewPeerHealthChecker(logger, registry, settings)

	duration, healthy := hc.isDataHubReachable(redirectSrv.URL)
	assert.True(t, healthy, "single redirect should be reachable")
	assert.Greater(t, duration, time.Duration(0), "duration should be greater than zero")

	// Too many redirects (loop) should be considered unreachable
	// Create two servers that redirect to each other to form a loop
	var srvA, srvB *httptest.Server
	srvA = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, srvB.URL+r.URL.Path, http.StatusFound)
	}))
	defer srvA.Close()
	srvB = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, srvA.URL+r.URL.Path, http.StatusFound)
	}))
	defer srvB.Close()

	// Our http.Client stops after 3 redirects and returns the last 3xx response.
	// Since isDataHubReachable treats <500 as reachable, this should be true.
	duration, healthy = hc.isDataHubReachable(srvA.URL)
	assert.True(t, healthy, "redirect loop should still be considered reachable (3xx)")
	assert.Greater(t, duration, time.Duration(0), "duration should be greater than zero")
}

func TestPeerHealthChecker_ListenOnlyPeersNotChecked(t *testing.T) {
	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()

	hc := NewPeerHealthChecker(logger, registry, settings)

	// Add a listen-only peer (no DataHub URL)
	listenOnlyPeerID := peer.ID("listen-only-peer")
	registry.AddPeer(listenOnlyPeerID)
	registry.UpdateConnectionState(listenOnlyPeerID, true)
	// Do not set DataHubURL - it remains empty for listen-only peers

	// Get initial state
	initialInfo, exists := registry.GetPeer(listenOnlyPeerID)
	require.True(t, exists)
	initialHealthCheck := initialInfo.LastHealthCheck

	// Call CheckPeerNow - should skip the health check
	hc.CheckPeerNow(listenOnlyPeerID)

	// Verify the health check was NOT performed
	updatedInfo, exists := registry.GetPeer(listenOnlyPeerID)
	require.True(t, exists)
	assert.Equal(t, initialHealthCheck, updatedInfo.LastHealthCheck,
		"LastHealthCheck should not be updated for listen-only peers")
	assert.True(t, updatedInfo.IsHealthy,
		"Listen-only peers should remain in their initial healthy state")
}

func TestPeerHealthChecker_CheckAllPeersSkipsListenOnly(t *testing.T) {
	// Create test HTTP server
	var checkCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&checkCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()

	hc := NewPeerHealthChecker(logger, registry, settings)

	// Add a regular peer with DataHub URL
	regularPeerID := peer.ID("regular-peer")
	registry.AddPeer(regularPeerID)
	registry.UpdateConnectionState(regularPeerID, true)
	registry.UpdateDataHubURL(regularPeerID, server.URL)

	// Add a listen-only peer (no DataHub URL)
	listenOnlyPeerID := peer.ID("listen-only-peer")
	registry.AddPeer(listenOnlyPeerID)
	registry.UpdateConnectionState(listenOnlyPeerID, true)
	// Do not set DataHubURL

	// Run check on all peers
	hc.checkAllPeers()

	// Verify only the regular peer was checked (one HTTP request)
	assert.Equal(t, int32(1), atomic.LoadInt32(&checkCount),
		"Only regular peer should be health-checked, not listen-only peer")

	// Verify regular peer was marked healthy
	regularInfo, _ := registry.GetPeer(regularPeerID)
	assert.True(t, regularInfo.IsHealthy, "Regular peer should be marked healthy")
	assert.NotZero(t, regularInfo.LastHealthCheck, "Regular peer should have health check timestamp")

	// Verify listen-only peer health status was not updated
	listenOnlyInfo, _ := registry.GetPeer(listenOnlyPeerID)
	assert.True(t, listenOnlyInfo.IsHealthy, "Listen-only peer should remain healthy")
	assert.Zero(t, listenOnlyInfo.LastHealthCheck,
		"Listen-only peer should not have health check timestamp")
}
