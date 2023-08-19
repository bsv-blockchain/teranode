package http_impl

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (h *HTTP) GetUTXO(mode ReadMode) func(c echo.Context) error {

	return func(c echo.Context) error {
		h.logger.Debugf("[BlobServer_http] GetUTXO: %s", c.Param("hash"))
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
