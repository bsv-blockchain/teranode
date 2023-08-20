package http_impl

import (
	"context"
	"errors"
	"net/http"

	"github.com/bitcoin-sv/ubsv/services/blobserver/repository"
	"github.com/bitcoin-sv/ubsv/ui/dashboard"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/ordishs/go-utils"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusBlobServerHttpGetTransaction     *prometheus.CounterVec
	prometheusBlobServerHttpGetSubtree         *prometheus.CounterVec
	prometheusBlobServerHttpGetBlockHeader     *prometheus.CounterVec
	prometheusBlobServerHttpGetBestBlockHeader *prometheus.CounterVec
	prometheusBlobServerHttpGetBlock           *prometheus.CounterVec
	prometheusBlobServerHttpGetUTXO            *prometheus.CounterVec
)

type HTTP struct {
	logger     utils.Logger
	repository *repository.Repository
	e          *echo.Echo
}

func New(logger utils.Logger, repo *repository.Repository) (*HTTP, error) {
	// TODO: change logger name
	// logger := gocore.Log("b_http")

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{echo.GET},
	}))

	h := &HTTP{
		logger:     logger,
		repository: repo,
		e:          e,
	}

	e.GET("/health", func(c echo.Context) error {
		logger.Debugf("[BlobServer_http] Health check")
		return c.String(http.StatusOK, "OK")
	})

	e.GET("/tx/:hash", h.GetTransaction(BINARY_STREAM))
	e.GET("/tx/:hash/hex", h.GetTransaction(HEX))
	e.GET("/tx/:hash/json", h.GetTransaction(JSON))

	e.GET("/subtree/:hash", h.GetSubtree(BINARY_STREAM))
	e.GET("/subtree/:hash/hex", h.GetSubtree(HEX))
	e.GET("/subtree/:hash/json", h.GetSubtree(JSON))

	e.GET("/header/:height/height", h.GetBlockHeaderByHeight(BINARY_STREAM))
	e.GET("/header/:height/height/hex", h.GetBlockHeaderByHeight(HEX))
	e.GET("/header/:height/height/json", h.GetBlockHeaderByHeight(JSON))

	e.GET("/header/:hash/hash", h.GetBlockHeaderByHash(BINARY_STREAM))
	e.GET("/header/:hash/hash/hex", h.GetBlockHeaderByHash(HEX))
	e.GET("/header/:hash/hash/json", h.GetBlockHeaderByHash(JSON))

	e.GET("/block/:height/height", h.GetBlockByHeight(BINARY_STREAM))
	e.GET("/block/:height/height/hex", h.GetBlockByHeight(HEX))
	e.GET("/block/:height/height/json", h.GetBlockByHeight(JSON))

	e.GET("/block/:hash/hash", h.GetBlockByHash(BINARY_STREAM))
	e.GET("/block/:hash/hash/hex", h.GetBlockByHash(HEX))
	e.GET("/block/:hash/hash/json", h.GetBlockByHash(JSON))

	e.GET("/utxo/:hash", h.GetUTXO(BINARY_STREAM))
	e.GET("/utxo/:hash/hex", h.GetUTXO(HEX))
	e.GET("/utxo/:hash/json", h.GetUTXO(JSON))

	e.GET("/bestblockheader", h.GetBestBlockHeader(BINARY_STREAM))
	e.GET("/bestblockheader/hex", h.GetBestBlockHeader(HEX))
	e.GET("/bestblockheader/json", h.GetBestBlockHeader(JSON))

	e.GET("*", func(c echo.Context) error {
		return dashboard.AppHandler(c)
	})

	return h, nil
}

func (h *HTTP) Init(_ context.Context) error {
	return nil
}

func (h *HTTP) Start(ctx context.Context, addr string) error {
	h.logger.Infof("BlobServer HTTP service listening on %s", addr)

	go func() {
		<-ctx.Done()
		h.logger.Infof("[BlobServer] HTTP (impl) service shutting down")
		err := h.e.Shutdown(ctx)
		if err != nil {
			h.logger.Errorf("[BlobServer] HTTP (impl) service shutdown error: %s", err)
		}
	}()

	err := h.e.Start(addr)
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}

func (h *HTTP) Stop(ctx context.Context) error {
	return h.e.Shutdown(ctx)
}

func init() {
	prometheusBlobServerHttpGetTransaction = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blobserver_http_get_transaction",
			Help: "Number of Get transactions ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusBlobServerHttpGetSubtree = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blobserver_http_get_subtree",
			Help: "Number of Get subtree ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusBlobServerHttpGetBlockHeader = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blobserver_http_get_block_header",
			Help: "Number of Get block header ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusBlobServerHttpGetBestBlockHeader = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blobserver_http_get_best_block_header",
			Help: "Number of Get best block header ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusBlobServerHttpGetBlock = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blobserver_http_get_block",
			Help: "Number of Get block ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)

	prometheusBlobServerHttpGetUTXO = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blobserver_http_get_utxo",
			Help: "Number of Get UTXO ops",
		},
		[]string{
			"function",  //function tracking the operation
			"operation", // type of operation achieved
		},
	)
}
