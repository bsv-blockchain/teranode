// Package httpimpl provides HTTP REST API endpoints for blockchain data access.
//
//go:generate swagger generate spec -m -o swagger.json -w .
package httpimpl

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/internal/banlist"
	"github.com/bsv-blockchain/teranode/services/asset/repository"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ui/dashboard"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/servicemanager"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/ordishs/gocore"
)

var AssetStat = gocore.NewStat("Asset")

// HTTP handles blockchain data API endpoints using the Echo framework.
// Provides RESTful access to blocks, transactions, subtrees, and UTXO data with
// support for JSON and binary formats, request signing, CORS, and health checking.
//
// Thread-safe: Echo framework and repository handle concurrent requests safely.
type HTTP struct {
	logger              ulogger.Logger
	settings            *settings.Settings
	repository          repository.Interface
	blockAssemblyClient blockassembly.ClientI
	e                   *echo.Echo
	startTime           time.Time
	privKey             crypto.PrivKey
	peerAuth            *peerAuthVerifier
	rateLimiters        []*tieredRateLimiter
}

// New creates and configures a new HTTP server instance with all routes and middleware.
//
// Parameters:
//   - logger: Logger instance for server operations
//   - repo: Repository instance for blockchain data access
//
// Returns:
//   - *HTTP: Configured HTTP server instance
//   - error: Any error encountered during setup
//
// API Endpoints:
//
//	Health and Status:
//	- GET /alive: Service liveness check
//	- GET /health: Service health check with dependency status
//
//	Transaction Related:
//	- GET /api/v1/tx/{hash}: Get transaction (binary/hex/json)
//	- POST /api/v1/txs: Batch transaction retrieval
//	- GET /api/v1/txmeta/{hash}/json: Get transaction metadata
//	- GET /api/v1/txmeta_raw/{hash}: Get raw transaction metadata
//
//	Block Related:
//	- GET /api/v1/block/{hash}: Get block by hash
//	- GET /api/v1/blocks: Get paginated block list
//	- GET /api/v1/block/{hash}/forks: Get block fork information
//	- GET /api/v1/bestblockheader: Get latest block header
//	- GET /api/v1/blockstats: Get blockchain statistics
//	- GET /api/v1/blockgraphdata/{period}: Get time-series block data
//
//	UTXO Related:
//	- GET /api/v1/utxo/{hash}: Get UTXO information
//	- GET /api/v1/utxos/{hash}/json: Get UTXOs by transaction
//	- GET /api/v1/balance: Get UTXO set balance
//
//	Search and Discovery:
//	- GET /api/v1/search: Search for blockchain entities
//
//	Legacy Compatibility:
//	- GET /rest/block/{hash}.bin: Get block in legacy format
//	- GET /api/v1/block_legacy/{hash}: Get block in legacy format
//
//	Network and P2P:
//	- GET /api/v1/catchup/status: Get blockchain catchup status
//	- GET /api/v1/peers: Get peer registry data
//
// Configuration:
//   - ECHO_DEBUG: Enable debug logging
//   - http_sign_response: Enable response signing
//   - p2p_private_key: Private key for response signing
//   - securityLevelHTTP: 0 for HTTP, non-zero for HTTPS
//   - server_certFile: TLS certificate file (HTTPS only)
//   - server_keyFile: TLS key file (HTTPS only)
//
// Security Features:
//   - Optional HTTPS support
//   - Response signing capability
//   - CORS configuration
//   - Gzip compression
//
// Monitoring:
//   - Custom request logging in debug mode
//   - Prometheus metrics
//   - Statistical tracking with reset capability
func New(logger ulogger.Logger, tSettings *settings.Settings, repo *repository.Repository, banList banlist.Interface, blockAssemblyClient ...blockassembly.ClientI) (*HTTP, error) {
	initPrometheusMetrics()

	// TODO: change logger name
	// logger := gocore.Log("b_http")

	e := echo.New()
	// Check if the ECHO_DEBUG environment variable is set to "true"

	if tSettings.Asset.EchoDebug {
		e.Debug = true
	}

	e.HideBanner = true
	e.HidePort = true

	// Configure real IP extraction for reverse proxy deployments. When
	// asset_trustedProxyCIDRs is non-empty but no valid CIDRs are parsed,
	// fail loudly rather than silently falling back to "trust all private
	// ranges" — operator typos must not weaken the trust boundary.
	if tSettings.Asset.TrustedProxyCIDRs != "" {
		var trustOpts []echo.TrustOption
		var parseErrors []string
		for _, cidrStr := range strings.Split(tSettings.Asset.TrustedProxyCIDRs, "|") {
			cidrStr = strings.TrimSpace(cidrStr)
			if cidrStr == "" {
				continue
			}
			_, ipNet, err := net.ParseCIDR(cidrStr)
			if err != nil {
				parseErrors = append(parseErrors, fmt.Sprintf("%q (%v)", cidrStr, err))
				continue
			}
			trustOpts = append(trustOpts, echo.TrustIPRange(ipNet))
		}
		if len(trustOpts) == 0 {
			return nil, errors.NewConfigurationError(
				"[Asset] asset_trustedProxyCIDRs is set but no valid CIDRs were parsed: %s",
				strings.Join(parseErrors, ", "),
			)
		}
		if len(parseErrors) > 0 {
			// Some valid, some invalid: still fail. Mixed input is almost
			// always a typo and silently using only the valid subset masks it.
			return nil, errors.NewConfigurationError(
				"[Asset] asset_trustedProxyCIDRs contains invalid entries: %s",
				strings.Join(parseErrors, ", "),
			)
		}
		e.IPExtractor = echo.ExtractIPFromXFFHeader(trustOpts...)
	} else {
		e.IPExtractor = echo.ExtractIPFromXFFHeader()
	}

	e.HTTPErrorHandler = customHTTPErrorHandler(logger)

	e.Use(middleware.Recover())

	// Ban list middleware - reject requests from banned IPs early
	if banList != nil {
		e.Use(banlist.CreateEchoMiddleware(banList))
	}

	// Default CORS config for non-dashboard endpoints
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		// Use AllowOriginFunc instead of AllowOrigins to dynamically approve origins
		AllowOriginFunc: func(origin string) (bool, error) {
			// Allow any origin to access the dashboard
			return true, nil
		},
		AllowMethods:     []string{echo.GET, echo.HEAD, echo.PUT, echo.PATCH, echo.POST, echo.DELETE, echo.OPTIONS},
		AllowHeaders:     []string{echo.HeaderOrigin, echo.HeaderContentType, echo.HeaderAccept, echo.HeaderAuthorization, echo.HeaderXRequestedWith},
		ExposeHeaders:    []string{echo.HeaderContentLength, echo.HeaderContentType},
		AllowCredentials: true,
		MaxAge:           86400,
	}))

	e.Use(middleware.GzipWithConfig(middleware.GzipConfig{
		Skipper: shouldSkipGzipForLargeBinaryAssetResponse,
	}))

	e.Use(securityHeadersMiddleware())

	// Body size limit runs BEFORE peer-auth so the auth middleware (which reads
	// the body to verify the SHA-256 digest header) cannot be turned into a
	// DoS surface by an oversized body.
	if tSettings.Asset.HTTPBodyLimit != "" {
		e.Use(middleware.BodyLimit(tSettings.Asset.HTTPBodyLimit))
	}

	// Peer authentication — verifies Ed25519 signed requests and sets peer_tier in context.
	// The verifier owns the tier cache and the replay cache; both are started in
	// Start() when a context is available.
	//
	// Tier elevation requires explicit operator opt-in via asset_peerAuthAllowlist.
	// An empty allowlist (the default) means signatures are still verified
	// (replay cache + body digest + freshness window all apply) but every
	// authenticated peer is treated as tierUnverified for rate-limit purposes.
	var peerAuth *peerAuthVerifier
	p2pClient := repo.GetP2PClient()
	if p2pClient != nil {
		peerCache := newPeerTierCache(logger, p2pClient, tSettings.Asset.PeerMinerReputationThreshold)
		allowlist := parsePeerAuthAllowlist(logger, tSettings.Asset.PeerAuthAllowlist)
		peerAuth = newPeerAuthVerifier(logger, peerCache, allowlist)
		e.Use(peerAuth.Middleware())
	}

	// Always-on access logging with Prometheus metrics.
	e.Use(accessLogMiddleware(logger))

	// Global tiered rate limiting. Unverified clients are IP-keyed (IPv6 to
	// /64) in a bounded LRU; authenticated peers are peer-ID-keyed.
	// Rate limiters are created here; cleanup goroutines are started in Start() with a context.
	var rateLimiters []*tieredRateLimiter
	if tSettings.Asset.HTTPRateLimit > 0 {
		globalRL := newTieredRateLimiter(
			tSettings.Asset.HTTPRateLimit,
			tSettings.Asset.HTTPPeerRateMultiplier,
			tSettings.Asset.HTTPMinerRateLimit,
			"global",
		)
		e.Use(globalRL.Middleware())
		rateLimiters = append(rateLimiters, globalRL)
	}

	// Heavy-endpoint rate limiter (applied per-route below).
	var heavyRateLimiter echo.MiddlewareFunc
	if tSettings.Asset.HTTPHeavyRateLimit > 0 {
		heavyRL := newTieredRateLimiter(
			tSettings.Asset.HTTPHeavyRateLimit,
			tSettings.Asset.HTTPPeerRateMultiplier,
			tSettings.Asset.HTTPMinerRateLimit,
			"heavy",
		)
		heavyRateLimiter = heavyRL.Middleware()
		rateLimiters = append(rateLimiters, heavyRL)
	}
	heavyMW := func() []echo.MiddlewareFunc {
		if heavyRateLimiter != nil {
			return []echo.MiddlewareFunc{heavyRateLimiter}
		}
		return nil
	}

	h := &HTTP{
		logger:       logger,
		settings:     tSettings,
		repository:   repo,
		e:            e,
		startTime:    time.Now(),
		peerAuth:     peerAuth,
		rateLimiters: rateLimiters,
	}

	if len(blockAssemblyClient) > 0 && blockAssemblyClient[0] != nil {
		h.blockAssemblyClient = blockAssemblyClient[0]
	}

	// add the private key for signing responses
	if tSettings.Asset.SignHTTPResponses {
		privateKey := tSettings.P2P.PrivateKey
		if privateKey != "" {
			privKeyBytes, err := hex.DecodeString(privateKey)
			if err != nil {
				logger.Errorf("failed to decode private key: %s", err.Error())
			} else {
				privKey, err := crypto.UnmarshalEd25519PrivateKey(privKeyBytes)
				if err != nil {
					logger.Errorf("failed to unmarshal private key: %s", err.Error())
				} else {
					h.privKey = privKey
				}
			}
		}
	}

	e.GET("/alive", func(c echo.Context) error {
		return c.String(http.StatusOK, fmt.Sprintf("Asset service is alive. Uptime: %s\n", time.Since(h.startTime)))
	})

	e.GET("/health", func(c echo.Context) error {
		logger.Debugf("[Asset_http] Health check")

		_, details, err := repo.Health(c.Request().Context(), false)
		if err != nil {
			return c.String(http.StatusInternalServerError, details)
		}

		return c.String(http.StatusOK, details)
	})

	apiRestGroup := e.Group("/rest")
	apiRestGroup.GET("/block/:hash.bin", h.GetRestLegacyBlock(), heavyMW()...) // BINARY_STREAM

	apiPrefix := tSettings.Asset.APIPrefix
	apiGroup := e.Group(apiPrefix)

	apiGroup.GET("/tx/:hash", h.GetTransaction(BINARY_STREAM))
	apiGroup.GET("/tx/:hash/hex", h.GetTransaction(HEX))
	apiGroup.GET("/tx/:hash/json", h.GetTransaction(JSON))

	if tSettings.Asset.PropagationProxyEnabled && tSettings.Asset.PropagationProxyAddress != "" {
		proxyTarget, err := url.Parse(tSettings.Asset.PropagationProxyAddress)
		if err != nil {
			logger.Errorf("[Asset] failed to parse propagation proxy address %q: %v", tSettings.Asset.PropagationProxyAddress, err)
		} else {
			apiGroup.POST("/tx", h.ProxyPropagationTx(proxyTarget, "/tx"))
			apiGroup.POST("/txs", h.ProxyPropagationTx(proxyTarget, "/txs"))
		}
	}

	apiGroup.GET("/txmeta/:hash/json", h.GetTransactionMeta(JSON))

	apiGroup.GET("/txmeta_raw/:hash", h.GetTxMetaByTxID(BINARY_STREAM))
	apiGroup.GET("/txmeta_raw/:hash/hex", h.GetTxMetaByTxID(HEX))
	apiGroup.GET("/txmeta_raw/:hash/json", h.GetTxMetaByTxID(JSON))

	apiGroup.GET("/subtree/:hash", h.GetSubtree(BINARY_STREAM), heavyMW()...)
	apiGroup.GET("/subtree/:hash/hex", h.GetSubtree(HEX), heavyMW()...)
	apiGroup.GET("/subtree/:hash/json", h.GetSubtree(JSON), heavyMW()...)
	apiGroup.GET("/subtree_data/:hash", h.GetSubtreeData(), heavyMW()...)
	apiGroup.POST("/subtree/:hash/txs", h.GetTransactions(), heavyMW()...) // BINARY_STREAM only

	apiGroup.GET("/subtree/:hash/txs/json", h.GetSubtreeTxs(JSON))

	apiGroup.GET("/headers/:hash", h.GetBlockHeaders(BINARY_STREAM))
	apiGroup.GET("/headers/:hash/hex", h.GetBlockHeaders(HEX))
	apiGroup.GET("/headers/:hash/json", h.GetBlockHeaders(JSON))

	// this needs to be removed in the future, after all clients have migrated to the new endpoint
	apiGroup.GET("/headers_to_common_ancestor/:hash", h.GetBlockHeadersToCommonAncestor(BINARY_STREAM))
	apiGroup.GET("/headers_to_common_ancestor/:hash/hex", h.GetBlockHeadersToCommonAncestor(HEX))
	apiGroup.GET("/headers_to_common_ancestor/:hash/json", h.GetBlockHeadersToCommonAncestor(JSON))

	apiGroup.GET("/headers_from_common_ancestor/:hash", h.GetBlockHeadersFromCommonAncestor(BINARY_STREAM))
	apiGroup.GET("/headers_from_common_ancestor/:hash/hex", h.GetBlockHeadersFromCommonAncestor(HEX))
	apiGroup.GET("/headers_from_common_ancestor/:hash/json", h.GetBlockHeadersFromCommonAncestor(JSON))

	apiGroup.GET("/header/:hash", h.GetBlockHeader(BINARY_STREAM))
	apiGroup.GET("/header/:hash/hex", h.GetBlockHeader(HEX))
	apiGroup.GET("/header/:hash/json", h.GetBlockHeader(JSON))

	apiGroup.GET("/blocks", h.GetBlocks)
	apiGroup.GET("/block_locator", h.GetBlockLocator)

	apiGroup.GET("/blocks/:hash", h.GetNBlocks(BINARY_STREAM), heavyMW()...)
	apiGroup.GET("/blocks/:hash/hex", h.GetNBlocks(HEX), heavyMW()...)
	apiGroup.GET("/blocks/:hash/json", h.GetNBlocks(JSON), heavyMW()...)

	apiGroup.GET("/block_legacy/:hash", h.GetLegacyBlock(), heavyMW()...) // BINARY_STREAM (also supports ?type=miningcandidate)

	apiGroup.GET("/block/:hash", h.GetBlockByHash(BINARY_STREAM), heavyMW()...)
	apiGroup.GET("/block/:hash/hex", h.GetBlockByHash(HEX), heavyMW()...)
	apiGroup.GET("/block/:hash/json", h.GetBlockByHash(JSON), heavyMW()...)
	apiGroup.GET("/block/:hash/forks", h.GetBlockForks)
	apiGroup.GET("/block/:hash/nearestforks", h.GetNearestForkHeights)

	apiGroup.GET("/block/:hash/subtrees/json", h.GetBlockSubtrees(JSON))

	apiGroup.GET("/search", h.Search)
	apiGroup.GET("/blockstats", h.GetBlockStats)
	apiGroup.GET("/blockgraphdata/:period", h.GetBlockGraphData)
	apiGroup.GET("/chainparams", h.GetChainParams)

	// ARC-compatible policy endpoint (https://bitcoin-sv.github.io/arc/api.html)
	e.GET("/v1/policy", h.GetPolicy)

	apiGroup.GET("/lastblocks", h.GetLastNBlocks)

	apiGroup.GET("/utxo/:hash", h.GetUTXO(BINARY_STREAM))
	apiGroup.GET("/utxo/:hash/hex", h.GetUTXO(HEX))
	apiGroup.GET("/utxo/:hash/json", h.GetUTXO(JSON))

	apiGroup.GET("/utxos/:hash/json", h.GetUTXOsByTxID(JSON))

	// Bulk UTXO spend-status lookup. All three modes accept the same 36-byte
	// binary request body; only the response format differs. Routed through
	// heavyMW() so a single request (up to 1024-way Aerospike fan-out) is
	// gated by the same tiered rate limiter that protects /subtree/:hash/txs
	// and other heavy endpoints. Body size is capped by the global
	// asset_httpBodyLimit middleware.
	apiGroup.POST("/utxos", h.GetUTXOs(BINARY_STREAM), heavyMW()...)
	apiGroup.POST("/utxos/hex", h.GetUTXOs(HEX), heavyMW()...)
	apiGroup.POST("/utxos/json", h.GetUTXOs(JSON), heavyMW()...)

	apiGroup.GET("/bestblockheader", h.GetBestBlockHeader(BINARY_STREAM))
	apiGroup.GET("/bestblockheader/hex", h.GetBestBlockHeader(HEX))
	apiGroup.GET("/bestblockheader/json", h.GetBestBlockHeader(JSON))

	apiGroup.GET("/merkle_proof/:hash", h.GetMerkleProof(BINARY_STREAM))
	apiGroup.GET("/merkle_proof/:hash/hex", h.GetMerkleProof(HEX))
	apiGroup.GET("/merkle_proof/:hash/json", h.GetMerkleProof(JSON))

	if h.settings.StatsPrefix != "" {
		e.GET(h.settings.StatsPrefix+"stats", AdaptStdHandler(gocore.HandleStats))
		e.GET(h.settings.StatsPrefix+"reset", AdaptStdHandler(gocore.ResetStats))
		e.GET(h.settings.StatsPrefix+"*", AdaptStdHandler(gocore.HandleOther))
	}

	// Create auth handler for protecting admin endpoints (used regardless of dashboard state)
	authHandler := dashboard.NewAuthHandler(h.logger, h.settings)

	if h.settings.Dashboard.Enabled {
		// Initialize dashboard with settings
		dashboard.InitDashboard(h.settings)

		// Apply authentication middleware for all POST endpoints
		apiGroup.Use(authHandler.PostAuthMiddleware)

		// Register dashboard-compatible API routes that need auth protection
		// The dashboard's SvelteKit +server.ts endpoints don't work in production (adapter-static)
		// so we need to provide the same endpoints directly in the Go backend
		apiP2PGroup := e.Group("/api/p2p")
		apiP2PGroup.Use(authHandler.PostAuthMiddleware) // Protect POST endpoints
		apiP2PGroup.GET("/peers", h.GetPeers)
		apiP2PGroup.POST("/reset-reputation", h.ResetReputation)

		apiCatchupGroup := e.Group("/api/catchup")
		apiCatchupGroup.GET("/status", h.GetCatchupStatus)

		dashboardConfig := middleware.CORSConfig{
			// Use AllowOriginFunc instead of AllowOrigins to dynamically approve origins
			AllowOriginFunc: func(origin string) (bool, error) {
				// Allow any origin to access the dashboard
				return true, nil
			},
			AllowMethods:     []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodPatch, http.MethodPost, http.MethodDelete, http.MethodOptions},
			AllowHeaders:     []string{echo.HeaderOrigin, echo.HeaderContentType, echo.HeaderAccept, echo.HeaderAuthorization, echo.HeaderXRequestedWith, "X-CSRF-Token"},
			ExposeHeaders:    []string{echo.HeaderContentLength, echo.HeaderContentType},
			AllowCredentials: true,
			MaxAge:           86400,
		}
		// Apply CORS middleware to the entire Echo instance
		e.Use(middleware.CORSWithConfig(dashboardConfig))

		// Register handlers for all HTTP methods to support API endpoints
		e.GET("*", dashboard.AppHandler)
		e.POST("*", dashboard.AppHandler)
		e.PUT("*", dashboard.AppHandler)
		e.DELETE("*", dashboard.AppHandler)
		e.OPTIONS("*", dashboard.AppHandler) // Important for CORS preflight requests
	} else {
		e.GET("*", func(c echo.Context) error {
			return echo.NewHTTPError(http.StatusNotFound, "Not Found")
		})
	}

	fsmHandler := NewFSMHandler(repo.BlockchainClient, logger)

	const (
		pathFsmState  = "/fsm/state"
		pathFsmEvents = "/fsm/events"
		pathFsmStates = "/fsm/states"
	)

	// Register FSM read-only endpoints (no auth required)
	apiGroup.GET(pathFsmState, fsmHandler.GetFSMState)
	apiGroup.GET(pathFsmEvents, fsmHandler.GetFSMEvents)
	apiGroup.GET(pathFsmStates, fsmHandler.GetFSMStates)

	// Register FSM write endpoint with auth (requires authentication regardless of dashboard state)
	apiAdminGroup := e.Group(apiPrefix)
	apiAdminGroup.Use(authHandler.RequireAuthMiddleware)
	apiAdminGroup.POST(pathFsmState, fsmHandler.SendFSMEvent)

	// Add OPTIONS handlers for CORS preflight requests
	apiGroup.OPTIONS(pathFsmState, func(c echo.Context) error {
		return c.NoContent(http.StatusOK)
	})
	apiGroup.OPTIONS(pathFsmEvents, func(c echo.Context) error {
		return c.NoContent(http.StatusOK)
	})
	apiGroup.OPTIONS(pathFsmStates, func(c echo.Context) error {
		return c.NoContent(http.StatusOK)
	})

	// Create and register block handler for block operations
	blockHandler := NewBlockHandler(repo.BlockchainClient, repo.BlockvalidationClient, logger)

	// Register block invalidation/revalidation endpoints under authenticated group
	// These are admin operations that could disrupt the blockchain state
	apiBlockAdminGroup := e.Group(apiPrefix + "/block")
	apiBlockAdminGroup.Use(authHandler.RequireAuthMiddleware)
	apiBlockAdminGroup.POST("/invalidate", blockHandler.InvalidateBlock)
	apiBlockAdminGroup.POST("/revalidate", blockHandler.RevalidateBlock)

	// Read-only endpoint for invalid blocks doesn't require auth
	apiGroup.GET("/blocks/invalid", blockHandler.GetLastNInvalidBlocks)

	// Register catchup status endpoint
	apiGroup.GET("/catchup/status", h.GetCatchupStatus)

	// Register service heights endpoint
	apiGroup.GET("/service/heights", h.GetServiceHeights)

	// Register peers endpoint
	apiGroup.GET("/peers", h.GetPeers)

	// Register settings handler for settings portal (always requires authentication)
	settingsHandler := NewSettingsHandler(tSettings, logger)
	apiSettingsGroup := e.Group(apiPrefix + "/settings")
	apiSettingsGroup.Use(authHandler.RequireAuthMiddleware)
	apiSettingsGroup.GET("", settingsHandler.GetSettings)
	apiSettingsGroup.GET("/categories", settingsHandler.GetSettingsCategories)

	// Add OPTIONS handlers for block operations (CORS preflight doesn't require auth)
	apiBlockAdminGroup.OPTIONS("/invalidate", func(c echo.Context) error {
		return c.NoContent(http.StatusOK)
	})
	apiBlockAdminGroup.OPTIONS("/revalidate", func(c echo.Context) error {
		return c.NoContent(http.StatusOK)
	})
	apiGroup.OPTIONS("/blocks/invalid", func(c echo.Context) error {
		return c.NoContent(http.StatusOK)
	})

	return h, nil
}

func shouldSkipGzipForLargeBinaryAssetResponse(c echo.Context) bool {
	path := c.Path()
	if path == "" && c.Request() != nil && c.Request().URL != nil {
		path = c.Request().URL.Path
	}

	parts := strings.Split(strings.Trim(path, "/"), "/")
	for i, part := range parts {
		switch part {
		case "subtree_data":
			return true
		case "subtree":
			tail := parts[i+1:]
			if len(tail) == 1 {
				return true
			}
			if len(tail) == 2 && tail[1] == "txs" {
				return true
			}
		}
	}

	return false
}

func AdaptStdHandler(handler func(w http.ResponseWriter, r *http.Request)) echo.HandlerFunc {
	return func(c echo.Context) error {
		handler(c.Response().Writer, c.Request())
		return nil
	}
}

func (h *HTTP) Init(_ context.Context) error {
	return nil
}

func (h *HTTP) Start(ctx context.Context, addr string) error {
	// Start background goroutines (all stop when ctx is cancelled).
	if h.peerAuth != nil {
		h.peerAuth.Start(ctx)
	}
	for _, rl := range h.rateLimiters {
		rl.StartCleanup(ctx)
	}

	mode := "HTTPS"
	if level := h.settings.SecurityLevelHTTP; level == 0 {
		mode = "HTTP"
	}

	// Get listener using util.GetListener
	listener, address, _, err := util.GetListener(h.settings.Context, "asset", "http://", addr)
	if err != nil {
		return errors.NewServiceError("[Asset] failed to get listener", err)
	}

	defer util.RemoveListener(h.settings.Context, "asset", "http://")

	go func() {
		<-ctx.Done()

		h.logger.Infof("[Asset] %s (impl) service shutting down", mode)

		err := h.e.Shutdown(context.Background())
		if err != nil {
			h.logger.Errorf("[Asset] %s (impl) service shutdown error: %s", mode, err)
		}
	}()

	// Set the listener on the Echo server
	h.e.Listener = listener

	// Defense-in-depth timeouts (reverse proxy also enforces limits)
	h.e.Server.ReadTimeout = 30 * time.Second
	h.e.Server.WriteTimeout = 120 * time.Second // generous for large block responses
	h.e.Server.IdleTimeout = 120 * time.Second
	h.e.Server.ReadHeaderTimeout = 10 * time.Second

	if mode == "HTTP" {
		servicemanager.AddListenerInfo(fmt.Sprintf("Asset HTTP listening on %s", address))
		err = h.e.Start(address)
	} else {
		certFile := h.settings.ServerCertFile
		if certFile == "" {
			return errors.NewConfigurationError("server_certFile is required for HTTPS")
		}

		keyFile := h.settings.ServerKeyFile
		if keyFile == "" {
			return errors.NewConfigurationError("server_keyFile is required for HTTPS")
		}

		servicemanager.AddListenerInfo(fmt.Sprintf("Asset HTTPS listening on %s", address))
		err = h.e.StartTLS(address, certFile, keyFile)
	}

	if !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

func (h *HTTP) Stop(ctx context.Context) error {
	return h.e.Shutdown(ctx)
}

func (h *HTTP) AddHTTPHandler(pattern string, handler http.Handler) error {
	h.e.GET(pattern, echo.WrapHandler(handler))
	return nil
}

func (h *HTTP) Sign(resp *echo.Response, hash []byte) error {
	// sign the response
	if h.privKey != nil {
		// sign the response
		signature, err := h.privKey.Sign(hash)
		if err != nil {
			return err
		}

		// add the signature to the response
		resp.Header().Set("X-Signature", hex.EncodeToString(signature))
	}

	return nil
}

// customHTTPErrorHandler creates a custom error handler that logs all errors before returning them to the client
func customHTTPErrorHandler(logger ulogger.Logger) echo.HTTPErrorHandler {
	return func(err error, c echo.Context) {
		var (
			message = ""
			code    = http.StatusInternalServerError
		)

		// Extract error details if it's an Echo HTTP error
		var he *echo.HTTPError
		if errors.As(err, &he) {
			code = he.Code
			if msg, ok := he.Message.(string); ok {
				message = msg
			} else {
				message = fmt.Sprintf("%v", he.Message)
			}
		}

		// Log the error with the route pattern (c.Path()) rather than the
		// raw RequestURI. Error paths are precisely where query-string values
		// (tokens, search terms, etc.) should not leak into logs.
		logger.Errorf("[Asset HTTP] Error handling request [%s %s]: status=%d, error=%v", c.Request().Method, c.Path(), code, err)

		// Send JSON response if not already sent
		if !c.Response().Committed {
			if c.Request().Method == http.MethodHead {
				err = c.NoContent(code)
			} else {
				err = c.JSON(code, map[string]interface{}{
					"message": message,
				})
			}
			if err != nil {
				logger.Errorf("[Asset HTTP] Failed to send error response: %v", err)
			}
		}
	}
}

// accessLogMiddleware logs every HTTP request with real client IP, duration, status,
// response size, and peer tier. It also records Prometheus histogram metrics.
func accessLogMiddleware(logger ulogger.Logger) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			prometheusAssetHTTPInFlight.Inc()
			defer prometheusAssetHTTPInFlight.Dec()

			start := time.Now()

			err := next(c)
			if err != nil {
				// Invoke the error handler so the response status/size are finalized
				// before we read them for metrics and logging. Return nil afterward
				// to prevent Echo from invoking the error handler a second time.
				c.Error(err)
			}

			duration := time.Since(start)
			status := c.Response().Status
			size := c.Response().Size
			method := c.Request().Method
			path := c.Path() // route pattern, not full URI — keeps Prometheus cardinality bounded
			ip := c.RealIP()
			statusStr := strconv.Itoa(status)

			tier, _ := c.Get("peer_tier").(peerTier)

			prometheusAssetHTTPRequestDuration.WithLabelValues(method, path, statusStr).Observe(duration.Seconds())
			prometheusAssetHTTPResponseSize.WithLabelValues(method, path, statusStr).Observe(float64(size))

			// Log only the route pattern (already bounded for Prometheus). The raw
			// RequestURI is intentionally omitted to keep query-string values out
			// of the access log; any future endpoint adding sensitive query
			// parameters won't accidentally leak them here.
			logger.Infof("[Asset_http] %s %s client_ip=%s status=%d duration=%v size=%d tier=%s",
				method, path, ip, status, duration, size, tier)

			return nil
		}
	}
}

// securityHeadersMiddleware adds security headers to all HTTP responses.
func securityHeadersMiddleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			c.Response().Header().Set("X-Content-Type-Options", "nosniff")
			c.Response().Header().Set("X-Frame-Options", "DENY")
			c.Response().Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
			return next(c)
		}
	}
}
