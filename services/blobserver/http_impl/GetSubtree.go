package http_impl

import (
	"encoding/hex"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (h *HTTP) GetSubtree(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		h.logger.Debugf("[BlobServer_http] GetSubtree: %s", c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		subtree, err := h.repository.GetSubtree(c.Request().Context(), hash)
		if err != nil {
			if strings.HasSuffix(err.Error(), " not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		prometheusBlobServerHttpGetSubtree.WithLabelValues("OK", "200").Inc()

		if mode == JSON {
			return c.JSONPretty(200, subtree, "  ")
		}

		b, err := subtree.SerializeNodes()
		if err != nil {
			return err
		}

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, b)

		case HEX:
			return c.String(200, hex.EncodeToString(b))

		default:
			err = errors.New("bad read mode")
			return sendError(c, http.StatusInternalServerError, 52, err)
		}
	}
}
