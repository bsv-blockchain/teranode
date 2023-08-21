package http_impl

import (
	"encoding/hex"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (h *HTTP) GetTransaction(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		h.logger.Debugf("[BlobServer_http] GetTransaction in %s: %s", mode, c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		b, err := h.repository.GetTransaction(c.Request().Context(), hash)
		if err != nil {
			if strings.HasSuffix(err.Error(), " not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		prometheusBlobServerHttpGetTransaction.WithLabelValues("OK", "200").Inc()

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, b)

		case HEX:
			return c.String(200, hex.EncodeToString(b))

		case JSON:
			tx, err := bt.NewTxFromBytes(b)
			if err != nil {
				return err
			}

			return c.JSONPretty(200, tx, "  ")

		default:
			err = errors.New("bad read mode")
			return sendError(c, http.StatusInternalServerError, 52, err)
		}
	}
}
