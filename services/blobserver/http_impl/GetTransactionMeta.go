package http_impl

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func (h *HTTP) GetTransactionMeta(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		start := gocore.CurrentTime()
		defer func() {
			blobServerStat.NewStat("GetTransactionMeta_http").AddTime(start)
		}()

		h.logger.Debugf("[BlobServer_http] GetTransactionMeta in %s for %s: %s", mode, c.Request().RemoteAddr, c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		meta, err := h.repository.TxMetaStore.Get(c.Request().Context(), hash)
		if err != nil {
			if strings.HasSuffix(err.Error(), " not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		prometheusBlobServerHttpGetTransaction.WithLabelValues("OK", "200").Inc()

		switch mode {
		case JSON:
			return c.JSONPretty(200, meta, "  ")
		default:
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}
