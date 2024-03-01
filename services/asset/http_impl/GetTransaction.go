package http_impl

import (
	"encoding/hex"
	"errors"
	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func (h *HTTP) GetTransaction(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		start := gocore.CurrentTime()
		defer func() {
			AssetStat.NewStat("GetTransaction_http").AddTime(start)
		}()

		h.logger.Debugf("[Asset_http] GetTransaction in %s for %s: %s", mode, c.Request().RemoteAddr, c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		b, err := h.repository.GetTransaction(c.Request().Context(), hash)
		if err != nil {
			if errors.Is(err, ubsverrors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		prometheusAssetHttpGetTransaction.WithLabelValues("OK", "200").Inc()

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
