package http_impl

import (
	"net/http"
	"strings"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func (h *HTTP) GetUTXO(mode ReadMode) func(c echo.Context) error {

	return func(c echo.Context) error {
		start := gocore.CurrentTime()
		defer func() {
			blobServerStat.NewStat("GetUTXO_http").AddTime(start)
		}()

		h.logger.Debugf("[BlobServer_http] GetUTXO in %s for %s: %s", mode, c.Request().RemoteAddr, c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		utxo, err := h.repository.GetUtxo(c.Request().Context(), hash)
		if err != nil {
			if strings.HasSuffix(err.Error(), " not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		if utxo == nil || utxo.Status == int(utxostore_api.Status_NOT_FOUND) {
			return echo.NewHTTPError(http.StatusNotFound, "UTXO not found")
		}

		prometheusBlobServerHttpGetUTXO.WithLabelValues("OK", "200").Inc()

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, utxo.SpendingTxID.CloneBytes())
		case HEX:
			return c.String(200, utxo.SpendingTxID.String())
		case JSON:
			return c.JSONPretty(200, utxo, "  ")
		default:
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}
