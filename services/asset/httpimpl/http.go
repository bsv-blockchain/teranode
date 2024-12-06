// Package httpimpl provides the HTTP server implementation for the Bitcoin SV blockchain service.
// It implements a comprehensive REST API for accessing blockchain data with support for multiple
// response formats.
package httpimpl

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/asset/repository"
	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/ui/dashboard"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/servicemanager"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/ordishs/gocore"
)

var AssetStat = gocore.NewStat("Asset")

// HTTP represents the main HTTP server structure for the blockchain service.
// It handles routing, middleware, and request processing for all blockchain data endpoints.
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

	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{echo.GET},
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

	apiGroup.POST("/txs", h.GetTransactions()) // BINARY_STREAM

	apiGroup.GET("/txmeta/:hash/json", h.GetTransactionMeta(JSON))

	apiGroup.GET("/txmeta_raw/:hash", h.GetTxMetaByTxID(BINARY_STREAM))
	apiGroup.GET("/txmeta_raw/:hash/hex", h.GetTxMetaByTxID(HEX))
	apiGroup.GET("/txmeta_raw/:hash/json", h.GetTxMetaByTxID(JSON))

	apiGroup.GET("/subtree/:hash", h.GetSubtree(BINARY_STREAM))
	apiGroup.GET("/subtree/:hash/hex", h.GetSubtree(HEX))
	apiGroup.GET("/subtree/:hash/json", h.GetSubtree(JSON))

	apiGroup.GET("/subtree/:hash/txs/json", h.GetSubtreeTxs(JSON))

	apiGroup.GET("/headers/:hash", h.GetBlockHeaders(BINARY_STREAM))
	apiGroup.GET("/headers/:hash/hex", h.GetBlockHeaders(HEX))
	apiGroup.GET("/headers/:hash/json", h.GetBlockHeaders(JSON))

	apiGroup.GET("/header/:hash", h.GetBlockHeader(BINARY_STREAM))
	apiGroup.GET("/header/:hash/hex", h.GetBlockHeader(HEX))
	apiGroup.GET("/header/:hash/json", h.GetBlockHeader(JSON))

	apiGroup.GET("/blocks", h.GetBlocks)

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

	if h.settings.StatsPrefix != "" {
		e.GET(h.settings.StatsPrefix+"stats", AdaptStdHandler(gocore.HandleStats))
		e.GET(h.settings.StatsPrefix+"reset", AdaptStdHandler(gocore.ResetStats))
		e.GET(h.settings.StatsPrefix+"*", AdaptStdHandler(gocore.HandleOther))
	}

	if h.settings.Dashboard.Enabled {
		e.GET("*", func(c echo.Context) error { // nolint
			return dashboard.AppHandler(c)
		})
	} else {
		e.GET("*", func(c echo.Context) error {
			return echo.NewHTTPError(http.StatusNotFound, "Not Found")
		})
	}

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

	h.logger.Infof("Asset %s service listening on %s", mode, addr)

	go func() {
		<-ctx.Done()

		h.logger.Infof("[Asset] %s (impl) service shutting down", mode)

		err := h.e.Shutdown(context.Background())
		if err != nil {
			h.logger.Errorf("[Asset] %s (impl) service shutdown error: %s", mode, err)
		}
	}()

	// err := h.e.Start(addr)
	// if err != nil && !errors.Is(err, http.ErrServerClosed) {
	// 	return err
	// }

	var err error

	if mode == "HTTP" {
		servicemanager.AddListenerInfo(fmt.Sprintf("Asset HTTP listening on %s", addr))
		err = h.e.Start(addr)
	} else {
		certFile := h.settings.ServerCertFile
		if certFile == "" {
			return errors.NewConfigurationError("server_certFile is required for HTTPS")
		}

		keyFile := h.settings.ServerKeyFile
		if keyFile == "" {
			return errors.NewConfigurationError("server_keyFile is required for HTTPS")
		}

		servicemanager.AddListenerInfo(fmt.Sprintf("Asset HTTPS listening on %s", addr))
		err = h.e.StartTLS(addr, certFile, keyFile)
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
