package p2p

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/libp2p/go-libp2p/core/peer"
)

// PeerHealthChecker monitors peer health asynchronously
// This runs in the background and updates the PeerRegistry
type PeerHealthChecker struct {
	logger        ulogger.Logger
	registry      *PeerRegistry
	settings      *settings.Settings
	httpClient    *http.Client
	checkInterval time.Duration
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

// NewPeerHealthChecker creates a new health checker
func NewPeerHealthChecker(logger ulogger.Logger, registry *PeerRegistry, settings *settings.Settings) *PeerHealthChecker {
	return &PeerHealthChecker{
		logger:        logger,
		registry:      registry,
		settings:      settings,
		checkInterval: 30 * time.Second, // Check every 30 seconds
		httpClient: &http.Client{
			Timeout: 5 * time.Second, // Reasonable timeout for health checks
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				// Allow redirects but limit to 3 to prevent loops
				if len(via) >= 3 {
					return http.ErrUseLastResponse
				}
				return nil
			},
			Transport: &http.Transport{
				// Disable automatic HTTP->HTTPS upgrades
				DisableKeepAlives: true,
				MaxIdleConns:      10,
				IdleConnTimeout:   30 * time.Second,
			},
		},
		stopCh: make(chan struct{}),
	}
}

// Start begins health checking in the background
func (hc *PeerHealthChecker) Start(ctx context.Context) {
	hc.wg.Add(1)
	go hc.healthCheckLoop(ctx)
}

// Stop stops the health checker
func (hc *PeerHealthChecker) Stop() {
	close(hc.stopCh)
	hc.wg.Wait()
}

// healthCheckLoop runs periodic health checks
func (hc *PeerHealthChecker) healthCheckLoop(ctx context.Context) {
	defer hc.wg.Done()

	ticker := time.NewTicker(hc.checkInterval)
	defer ticker.Stop()

	// Initial check
	hc.checkAllPeers()

	for {
		select {
		case <-ctx.Done():
			hc.logger.Infof("[HealthChecker] Stopping health checker")
			return
		case <-hc.stopCh:
			hc.logger.Infof("[HealthChecker] Stop requested")
			return
		case <-ticker.C:
			hc.checkAllPeers()
		}
	}
}

// checkAllPeers checks health of all peers
func (hc *PeerHealthChecker) checkAllPeers() {
	peers := hc.registry.GetAllPeers()

	hc.logger.Debugf("[HealthChecker] Checking health of %d peers", len(peers))

	// Check peers concurrently but limit concurrency
	semaphore := make(chan struct{}, 5) // Max 5 concurrent checks
	var wg sync.WaitGroup

	for _, p := range peers {
		if p.DataHubURL == "" {
			continue // Skip peers without DataHub URLs
		}

		wg.Add(1)
		go func(peer *PeerInfo) {
			defer wg.Done()

			semaphore <- struct{}{}        // Acquire
			defer func() { <-semaphore }() // Release

			hc.checkPeerHealth(peer)
		}(p)
	}

	wg.Wait()
}

// checkPeerHealth checks a single peer's health
func (hc *PeerHealthChecker) checkPeerHealth(p *PeerInfo) {
	healthy := hc.isDataHubReachable(p.DataHubURL)

	hc.registry.UpdateHealth(p.ID, healthy)

	if healthy {
		hc.logger.Debugf("[HealthChecker] Peer %s is healthy (DataHub: %s)", p.ID, p.DataHubURL)
	} else {
		hc.logger.Warnf("[HealthChecker] Peer %s is unhealthy (DataHub: %s unreachable)", p.ID, p.DataHubURL)
	}
}

// isDataHubReachable checks if a DataHub URL is reachable
func (hc *PeerHealthChecker) isDataHubReachable(dataHubURL string) bool {
	if dataHubURL == "" {
		return true // No DataHub is considered "healthy"
	}

	// Get genesis hash for the check
	var genesisHash string
	if hc.settings != nil && hc.settings.ChainCfgParams != nil && hc.settings.ChainCfgParams.GenesisHash != nil {
		genesisHash = hc.settings.ChainCfgParams.GenesisHash.String()
	} else {
		// Default to regtest genesis
		genesisHash = "18e7664a7abf9bb0e96b889eaa3cb723a89a15b610cc40538e5ebe3e9222e8d2"
	}

	blockURL := dataHubURL + "/block/" + genesisHash

	// Create request with timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", blockURL, nil)
	if err != nil {
		hc.logger.Debugf("[HealthChecker] Failed to create request for %s: %v", dataHubURL, err)
		return false
	}

	resp, err := hc.httpClient.Do(req)
	if err != nil {
		hc.logger.Debugf("[HealthChecker] DataHub %s not reachable: %v", dataHubURL, err)
		return false
	}
	defer resp.Body.Close()

	// Check for offline indicators in 404 responses
	if resp.StatusCode == 404 {
		body, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if err == nil {
			bodyStr := strings.ToLower(string(body))
			if strings.Contains(bodyStr, "offline") ||
				strings.Contains(bodyStr, "tunnel not found") {
				hc.logger.Debugf("[HealthChecker] DataHub %s is offline", dataHubURL)
				return false
			}
		}
	}

	// Consider 2xx, 3xx, and 4xx (except offline 404s) as reachable
	return resp.StatusCode < 500
}

// CheckPeerNow performs an immediate health check for a specific peer
func (hc *PeerHealthChecker) CheckPeerNow(peerID peer.ID) {
	if p, exists := hc.registry.GetPeer(peerID); exists {
		hc.checkPeerHealth(p)
	}
}
