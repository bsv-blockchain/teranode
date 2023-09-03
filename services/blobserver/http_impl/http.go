package http_impl

import (
	"context"
	"errors"
	"net/http"

	"github.com/bitcoin-sv/ubsv/services/blobserver/blobserver_api"
	"github.com/bitcoin-sv/ubsv/services/blobserver/repository"
	"github.com/bitcoin-sv/ubsv/ui/dashboard"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusBlobServerHttpGetTransaction     *prometheus.CounterVec
	prometheusBlobServerHttpGetSubtree         *prometheus.CounterVec
	prometheusBlobServerHttpGetBlockHeader     *prometheus.CounterVec
	prometheusBlobServerHttpGetBestBlockHeader *prometheus.CounterVec
	prometheusBlobServerHttpGetBlock           *prometheus.CounterVec
	prometheusBlobServerHttpGetLastNBlocks     *prometheus.CounterVec
	prometheusBlobServerHttpGetUTXO            *prometheus.CounterVec
)

type HTTP struct {
	logger         utils.Logger
	repository     *repository.Repository
	e              *echo.Echo
	notificationCh chan *blobserver_api.Notification
}

func New(logger utils.Logger, repo *repository.Repository, notificationCh chan *blobserver_api.Notification) (*HTTP, error) {
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
		logger:         logger,
		repository:     repo,
		e:              e,
		notificationCh: notificationCh,
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

	e.GET("/headers/:hash", h.GetBlockHeaders(BINARY_STREAM))
	e.GET("/headers/:hash/hex", h.GetBlockHeaders(HEX))
	e.GET("/headers/:hash/json", h.GetBlockHeaders(JSON))

	e.GET("/header/:hash", h.GetBlockHeader(BINARY_STREAM))
	e.GET("/header/:hash/hex", h.GetBlockHeader(HEX))
	e.GET("/header/:hash/json", h.GetBlockHeader(JSON))

	e.GET("/block/:hash", h.GetBlockByHash(BINARY_STREAM))
	e.GET("/block/:hash/hex", h.GetBlockByHash(HEX))
	e.GET("/block/:hash/json", h.GetBlockByHash(JSON))

	e.GET("/lastblocks", h.GetLastNBlocks)

	e.GET("/utxo/:hash", h.GetUTXO(BINARY_STREAM))
	e.GET("/utxo/:hash/hex", h.GetUTXO(HEX))
	e.GET("/utxo/:hash/json", h.GetUTXO(JSON))

	e.GET("/bestblockheader", h.GetBestBlockHeader(BINARY_STREAM))
	e.GET("/bestblockheader/hex", h.GetBestBlockHeader(HEX))
	e.GET("/bestblockheader/json", h.GetBestBlockHeader(JSON))

	e.GET("/ws", h.HandleWebSocket(h.notificationCh))

	e.GET("*", func(c echo.Context) error {
		return dashboard.AppHandler(c)
	})

	return h, nil
}

func (h *HTTP) Init(_ context.Context) error {
	return nil
}

func (h *HTTP) Start(ctx context.Context, addr string) error {
	h.logger.Infof("BlobServer HTTPS service listening on %s", addr)

	go func() {
		<-ctx.Done()
		h.logger.Infof("[BlobServer] HTTPS (impl) service shutting down")
		err := h.e.Shutdown(ctx)
		if err != nil {
			h.logger.Errorf("[BlobServer] HTTPS (impl) service shutdown error: %s", err)
		}
	}()

	// err := h.e.Start(addr)
	// if err != nil && !errors.Is(err, http.ErrServerClosed) {
	// 	return err
	// }

	certFile, found := gocore.Config().Get("server_certFile")
	if !found {
		return errors.New("server_certFile is required for HTTPS")
	}
	keyFile, found := gocore.Config().Get("server_keyFile")
	if !found {
		return errors.New("server_keyFile is required for HTTPS")
	}

	err := h.e.StartTLS(addr, certFile, keyFile)
	if err != http.ErrServerClosed {
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

	prometheusBlobServerHttpGetLastNBlocks = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "blobserver_http_get_last_n_blocks",
			Help: "Number of Get last N blocks ops",
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
