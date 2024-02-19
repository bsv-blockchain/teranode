package http_impl

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/bitcoin-sv/ubsv/services/asset/asset_api"
	"github.com/bitcoin-sv/ubsv/services/asset/repository"
	"github.com/bitcoin-sv/ubsv/ui/dashboard"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/servicemanager"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/ordishs/gocore"
)

var AssetStat = gocore.NewStat("Asset")

type HTTP struct {
	logger         ulogger.Logger
	repository     *repository.Repository
	e              *echo.Echo
	notificationCh chan *asset_api.Notification
	startTime      time.Time
}

func New(logger ulogger.Logger, repo *repository.Repository, notificationCh chan *asset_api.Notification) (*HTTP, error) {
	initPrometheusMetrics()

	// TODO: change logger name
	// logger := gocore.Log("b_http")

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.Use(middleware.Recover())

	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{echo.GET},
	}))

	h := &HTTP{
		logger:         logger,
		repository:     repo,
		e:              e,
		notificationCh: notificationCh,
		startTime:      time.Now(),
	}

	e.GET("/alive", func(c echo.Context) error {
		return c.String(http.StatusOK, fmt.Sprintf("Asset service is alive. Uptime: %s\n", time.Since(h.startTime)))
	})

	e.GET("/health", func(c echo.Context) error {
		_, details, err := repo.Health(c.Request().Context())
		logger.Debugf("[Asset_http] Health check")

		if err != nil {
			return c.String(http.StatusInternalServerError, details)
		}

		return c.String(http.StatusOK, details)
	})

	apiPrefix, _ := gocore.Config().Get("asset_apiPrefix", "/api/v1")
	apiGroup := e.Group(apiPrefix)

	apiGroup.GET("/tx/:hash", h.GetTransaction(BINARY_STREAM))
	apiGroup.GET("/tx/:hash/hex", h.GetTransaction(HEX))
	apiGroup.GET("/tx/:hash/json", h.GetTransaction(JSON))

	apiGroup.POST("/txs", h.GetTransactions()) // BINARY_STREAM

	apiGroup.GET("/txmeta/:hash/json", h.GetTransactionMeta(JSON))

	apiGroup.GET("/subtree/:hash", h.GetSubtree(BINARY_STREAM))
	apiGroup.GET("/subtree/:hash/hex", h.GetSubtree(HEX))
	apiGroup.GET("/subtree/:hash/json", h.GetSubtree(JSON))

	apiGroup.GET("/subtree/:hash/extended/json", h.GetSubtreeExtended(JSON))

	apiGroup.GET("/headers/:hash", h.GetBlockHeaders(BINARY_STREAM))
	apiGroup.GET("/headers/:hash/hex", h.GetBlockHeaders(HEX))
	apiGroup.GET("/headers/:hash/json", h.GetBlockHeaders(JSON))

	apiGroup.GET("/header/:hash", h.GetBlockHeader(BINARY_STREAM))
	apiGroup.GET("/header/:hash/hex", h.GetBlockHeader(HEX))
	apiGroup.GET("/header/:hash/json", h.GetBlockHeader(JSON))

	apiGroup.GET("/block/:hash", h.GetBlockByHash(BINARY_STREAM))
	apiGroup.GET("/block/:hash/hex", h.GetBlockByHash(HEX))
	apiGroup.GET("/block/:hash/json", h.GetBlockByHash(JSON))

	apiGroup.GET("/block/:hash/subtrees/json", h.GetBlockSubtrees(JSON))

	apiGroup.GET("/search", h.Search)
	apiGroup.GET("/blockstats", h.GetBlockStats)
	apiGroup.GET("/blockgraphdata", h.GetBlockGraphData)

	apiGroup.GET("/lastblocks", h.GetLastNBlocks)

	apiGroup.GET("/utxo/:hash", h.GetUTXO(BINARY_STREAM))
	apiGroup.GET("/utxo/:hash/hex", h.GetUTXO(HEX))
	apiGroup.GET("/utxo/:hash/json", h.GetUTXO(JSON))

	apiGroup.GET("/utxos/:hash/json", h.GetUTXOsByTXID(JSON))

	apiGroup.GET("/balance", h.GetBalance)

	apiGroup.GET("/bestblockheader", h.GetBestBlockHeader(BINARY_STREAM))
	apiGroup.GET("/bestblockheader/hex", h.GetBestBlockHeader(HEX))
	apiGroup.GET("/bestblockheader/json", h.GetBestBlockHeader(JSON))

	apiGroup.GET("/asset-ws", h.HandleWebSocket(h.notificationCh))

	// TODO make configurable, whether stats are enabled
	prefix := gocore.GetStatPrefix()
	e.GET(prefix+"stats", AdaptStdHandler(gocore.HandleStats))
	e.GET(prefix+"reset", AdaptStdHandler(gocore.ResetStats))
	e.GET(prefix+"*", AdaptStdHandler(gocore.HandleOther))

	e.GET("*", func(c echo.Context) error {
		return dashboard.AppHandler(c)
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
	if level, _ := gocore.Config().GetInt("securityLevelHTTP", 0); level == 0 {
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

		certFile, found := gocore.Config().Get("server_certFile")
		if !found {
			return errors.New("server_certFile is required for HTTPS")
		}
		keyFile, found := gocore.Config().Get("server_keyFile")
		if !found {
			return errors.New("server_keyFile is required for HTTPS")
		}

		servicemanager.AddListenerInfo(fmt.Sprintf("Asset HTTPS listening on %s", addr))
		err = h.e.StartTLS(addr, certFile, keyFile)
	}

	if err != http.ErrServerClosed {
		return err
	}

	return nil
}

func (h *HTTP) Stop(ctx context.Context) error {
	return h.e.Shutdown(ctx)
}
