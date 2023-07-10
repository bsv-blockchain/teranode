package http_impl

import (
	"context"
	"net/http"

	"github.com/TAAL-GmbH/ubsv/services/blobserver/repository"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	prometheusBlobServerHttpGetTransaction *prometheus.CounterVec
	prometheusBlobServerHttpGetSubtree     *prometheus.CounterVec
	prometheusBlobServerHttpGetBlockHeader *prometheus.CounterVec
	prometheusBlobServerHttpGetBlock       *prometheus.CounterVec
	prometheusBlobServerHttpGetUTXO        *prometheus.CounterVec
)

type HTTP struct {
	logger     utils.Logger
	repository *repository.Repository
	e          *echo.Echo
}

func New(repository *repository.Repository) (*HTTP, error) {
	logger := gocore.Log("b_http")

	e := echo.New()
	e.HideBanner = true

	e.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "OK")
	})

	e.GET("/tx/:hash", func(c echo.Context) error {
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		b, err := repository.GetTransaction(c.Request().Context(), hash)
		if err != nil {
			return err
		}

		prometheusBlobServerHttpGetTransaction.WithLabelValues("OK", "200").Inc()

		return c.Blob(200, echo.MIMEOctetStream, b)
	})

	e.GET("/subtree/:hash", func(c echo.Context) error {
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		b, err := repository.GetSubtree(c.Request().Context(), hash)
		if err != nil {
			return err
		}

		prometheusBlobServerHttpGetSubtree.WithLabelValues("OK", "200").Inc()

		return c.Blob(200, echo.MIMEOctetStream, b)
	})

	e.GET("/header/:hash", func(c echo.Context) error {
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		b, err := repository.GetBlockHeaderByHash(c.Request().Context(), hash)
		if err != nil {
			return err
		}

		prometheusBlobServerHttpGetBlockHeader.WithLabelValues("OK", "200").Inc()

		return c.Blob(200, echo.MIMEOctetStream, b)
	})

	e.GET("/block/:hash", func(c echo.Context) error {
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		b, err := repository.GetBlockByHash(c.Request().Context(), hash)
		if err != nil {
			return err
		}

		prometheusBlobServerHttpGetBlock.WithLabelValues("OK", "200").Inc()

		return c.Blob(200, echo.MIMEOctetStream, b)
	})

	e.GET("/utxo/:hash", func(c echo.Context) error {
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		b, err := repository.GetUtxo(c.Request().Context(), hash)
		if err != nil {
			return err
		}

		prometheusBlobServerHttpGetUTXO.WithLabelValues("OK", "200").Inc()

		return c.Blob(200, echo.MIMEOctetStream, b)
	})

	return &HTTP{
		logger:     logger,
		repository: repository,
		e:          e,
	}, nil
}

func (h *HTTP) Start(addr string) error {
	return h.e.Start(addr)
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
