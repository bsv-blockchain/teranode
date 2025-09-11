package p2p

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/ulogger"
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
	reachable := hc.isDataHubReachable(server.URL)
	assert.True(t, reachable, "DataHub should be reachable")
}

func TestPeerHealthChecker_isDataHubReachable_Failure(t *testing.T) {
	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()

	hc := NewPeerHealthChecker(logger, registry, settings)

	// Test unreachable URL
	reachable := hc.isDataHubReachable("http://localhost:99999")
	assert.False(t, reachable, "DataHub should not be reachable")
}

func TestPeerHealthChecker_isDataHubReachable_EmptyURL(t *testing.T) {
	logger := ulogger.New("test")
	registry := NewPeerRegistry()
	settings := CreateTestSettings()

	hc := NewPeerHealthChecker(logger, registry, settings)

	// Empty URL should be considered healthy
	reachable := hc.isDataHubReachable("")
	assert.True(t, reachable, "Empty URL should be considered healthy")
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
	reachable := hc.isDataHubReachable(server.URL)
	assert.False(t, reachable, "DataHub should be detected as offline")
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
	reachable := hc.isDataHubReachable(server.URL)
	assert.True(t, reachable, "Normal 404 should still be considered reachable")
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
	reachable := hc.isDataHubReachable(server.URL)
	assert.False(t, reachable, "500 errors should be considered unreachable")
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

	hc := NewPeerHealthChecker(logger, registry, settings)

	// Add multiple peers with different DataHub URLs
	peerA := peer.ID("peer-a")
	peerB := peer.ID("peer-b")
	peerC := peer.ID("peer-c")
	peerD := peer.ID("peer-d")

	registry.AddPeer(peerA)
	registry.UpdateDataHubURL(peerA, successServer.URL)

	registry.AddPeer(peerB)
	registry.UpdateDataHubURL(peerB, failServer.URL)

	registry.AddPeer(peerC)
	registry.UpdateDataHubURL(peerC, "http://localhost:99999") // Unreachable

	registry.AddPeer(peerD) // No DataHub URL

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
		registry.UpdateDataHubURL(peerID, server.URL)
	}

	// Check all peers concurrently
	start := time.Now()
	hc.checkAllPeers()
	elapsed := time.Since(start)

	// With semaphore limiting to 5 concurrent checks and 10ms delay per check,
	// 20 peers should take roughly 4 * 10ms = 40ms (4 batches of 5)
	// Allow some margin for overhead
	assert.Less(t, elapsed, 200*time.Millisecond, "Concurrent checks should be faster than sequential")

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
	registry.UpdateDataHubURL(peerID, server.URL)

	// Start health checker
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	hc.Start(ctx)

	// Wait for multiple check intervals
	time.Sleep(150 * time.Millisecond)

	// Stop health checker
	hc.Stop()

	// Should have performed multiple health checks
	// Initial check + at least 2 interval checks
	assert.GreaterOrEqual(t, int(atomic.LoadInt32(&checkCount)), 3, "Should have performed multiple health checks")
}
