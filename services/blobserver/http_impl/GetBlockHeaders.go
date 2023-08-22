package http_impl

import (
	"encoding/hex"
	"net/http"
	"strings"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (h *HTTP) GetBlockHeaders(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		hashOrHeight := c.Param("hash")
		h.logger.Debugf("[BlobServer_http] GetBlockHeaders in %s for %s: %s", mode, c.Request().RemoteAddr, hashOrHeight)

		var headers []*model.BlockHeader
		hash, err := chainhash.NewHashFromStr(hashOrHeight)
		if err != nil {
			return err
		}

		headers, err = h.repository.GetBlockHeaders(c.Request().Context(), hash)
		if err != nil {
			if strings.HasSuffix(err.Error(), " not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		prometheusBlobServerHttpGetBlockHeader.WithLabelValues("OK", "200").Inc()

		if mode == JSON {
			return c.JSONPretty(200, headers, "  ")
		}

		bytes := make([]byte, 0, len(headers)*model.BlockHeaderSize)
		for _, header := range headers {
			bytes = append(bytes, header.Bytes()...)
		}

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, bytes)
		case HEX:
			return c.String(200, hex.EncodeToString(bytes))
		default:
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}
