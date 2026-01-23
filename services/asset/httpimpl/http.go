// Package httpimpl provides HTTP REST API endpoints for blockchain data access.
package httpimpl

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/asset/repository"
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
	logger     ulogger.Logger
	settings   *settings.Settings
	repository repository.Interface
	e          *echo.Echo
	startTime  time.Time
	privKey    crypto.PrivKey
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
func New(logger ulogger.Logger, tSettings *settings.Settings, repo *repository.Repository) (*HTTP, error) {
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

	e.Use(middleware.Recover())

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

	e.Use(middleware.Gzip())

	if e.Debug {
		e.Use(customLoggerMiddleware(logger))
	}

	h := &HTTP{
		logger:     logger,
		settings:   tSettings,
		repository: repo,
		e:          e,
		startTime:  time.Now(),
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
	apiRestGroup.GET("/block/:hash.bin", h.GetRestLegacyBlock()) // BINARY_STREAM

	apiPrefix := tSettings.Asset.APIPrefix
	apiGroup := e.Group(apiPrefix)

	apiGroup.GET("/tx/:hash", h.GetTransaction(BINARY_STREAM))
	apiGroup.GET("/tx/:hash/hex", h.GetTransaction(HEX))
	apiGroup.GET("/tx/:hash/json", h.GetTransaction(JSON))

	// backwards compatibility for legacy endpoints - remove in future
	apiGroup.POST("/txs", h.GetTransactions())       // BINARY_STREAM only
	apiGroup.POST("/:hash/txs", h.GetTransactions()) // BINARY_STREAM only

	apiGroup.GET("/txmeta/:hash/json", h.GetTransactionMeta(JSON))

	apiGroup.GET("/txmeta_raw/:hash", h.GetTxMetaByTxID(BINARY_STREAM))
	apiGroup.GET("/txmeta_raw/:hash/hex", h.GetTxMetaByTxID(HEX))
	apiGroup.GET("/txmeta_raw/:hash/json", h.GetTxMetaByTxID(JSON))

	apiGroup.GET("/subtree/:hash", h.GetSubtree(BINARY_STREAM))
	apiGroup.GET("/subtree/:hash/hex", h.GetSubtree(HEX))
	apiGroup.GET("/subtree/:hash/json", h.GetSubtree(JSON))
	apiGroup.GET("/subtree_data/:hash", h.GetSubtreeData())
	apiGroup.POST("/subtree/:hash/txs", h.GetTransactions()) // BINARY_STREAM only

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

	apiGroup.GET("/blocks/:hash", h.GetNBlocks(BINARY_STREAM))
	apiGroup.GET("/blocks/:hash/hex", h.GetNBlocks(HEX))
	apiGroup.GET("/blocks/:hash/json", h.GetNBlocks(JSON))

	apiGroup.GET("/block_legacy/:hash", h.GetLegacyBlock()) // BINARY_STREAM

	apiGroup.GET("/block/:hash", h.GetBlockByHash(BINARY_STREAM))
	apiGroup.GET("/block/:hash/hex", h.GetBlockByHash(HEX))
	apiGroup.GET("/block/:hash/json", h.GetBlockByHash(JSON))
	apiGroup.GET("/block/:hash/forks", h.GetBlockForks)

	apiGroup.GET("/block/:hash/subtrees/json", h.GetBlockSubtrees(JSON))

	apiGroup.GET("/search", h.Search)
	apiGroup.GET("/blockstats", h.GetBlockStats)
	apiGroup.GET("/blockgraphdata/:period", h.GetBlockGraphData)

	apiGroup.GET("/lastblocks", h.GetLastNBlocks)

	apiGroup.GET("/utxo/:hash", h.GetUTXO(BINARY_STREAM))
	apiGroup.GET("/utxo/:hash/hex", h.GetUTXO(HEX))
	apiGroup.GET("/utxo/:hash/json", h.GetUTXO(JSON))

	apiGroup.GET("/utxos/:hash/json", h.GetUTXOsByTxID(JSON))

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

	if h.settings.Dashboard.Enabled {
		// Initialize dashboard with settings
		dashboard.InitDashboard(h.settings)

		// Apply authentication middleware for all POST endpoints
		authHandler := dashboard.NewAuthHandler(h.logger, h.settings)
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

	// Register FSM API endpoints
	apiGroup.GET(pathFsmState, fsmHandler.GetFSMState)
	apiGroup.POST(pathFsmState, fsmHandler.SendFSMEvent)
	apiGroup.GET(pathFsmEvents, fsmHandler.GetFSMEvents)
	apiGroup.GET(pathFsmStates, fsmHandler.GetFSMStates)

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

	// Register block invalidation/revalidation endpoints
	apiGroup.POST("/block/invalidate", blockHandler.InvalidateBlock)
	apiGroup.POST("/block/revalidate", blockHandler.RevalidateBlock)
	apiGroup.GET("/blocks/invalid", blockHandler.GetLastNInvalidBlocks)

	// Register catchup status endpoint
	apiGroup.GET("/catchup/status", h.GetCatchupStatus)

	// Register service heights endpoint
	apiGroup.GET("/service/heights", h.GetServiceHeights)

	// Register peers endpoint
	apiGroup.GET("/peers", h.GetPeers)

	// Register settings handler for settings portal (always requires authentication)
	settingsHandler := NewSettingsHandler(tSettings, logger)
	authHandler := dashboard.NewAuthHandler(logger, tSettings)
	apiSettingsGroup := e.Group(apiPrefix + "/settings")
	apiSettingsGroup.Use(authHandler.RequireAuthMiddleware)
	apiSettingsGroup.GET("", settingsHandler.GetSettings)
	apiSettingsGroup.GET("/categories", settingsHandler.GetSettingsCategories)

	// Add OPTIONS handlers for block operations
	apiGroup.OPTIONS("/block/invalidate", func(c echo.Context) error {
		return c.NoContent(http.StatusOK)
	})
	apiGroup.OPTIONS("/block/revalidate", func(c echo.Context) error {
		return c.NoContent(http.StatusOK)
	})
	apiGroup.OPTIONS("/blocks/invalid", func(c echo.Context) error {
		return c.NoContent(http.StatusOK)
	})

	return h, nil
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

// Middleware to log HTTP requests using the custom logger
func customLoggerMiddleware(logger ulogger.Logger) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			start := time.Now()

			// Process the request
			err := next(c)

			// Log response status and duration
			status := c.Response().Status
			duration := time.Since(start)

			if err != nil {
				c.Error(err) // Ensure Echo's default error handling
			}

			logger.Infof("http request: Method=%s, URI=%s, RemoteAddr=%s Status=%d, Duration=%v, err=%v", c.Request().Method, c.Request().RequestURI, c.Request().RemoteAddr, status, duration, err)

			return err
		}
	}
}
