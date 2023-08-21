package http_impl

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (h *HTTP) GetBlockHeader(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		hashOrHeight := c.Param("hash")
		h.logger.Debugf("[BlobServer_http] GetBlockHeader in %s: %s", mode, hashOrHeight)

		var hash *chainhash.Hash
		var header *model.BlockHeader
		height, err := strconv.ParseUint(hashOrHeight, 10, 64)
		if err == nil {
			header, err = h.repository.GetBlockHeaderByHeight(c.Request().Context(), uint32(height))
			if err != nil {
				if strings.HasSuffix(err.Error(), " not found") {
					return echo.NewHTTPError(http.StatusNotFound, err.Error())
				} else {
					return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
				}
			}
		} else {
			hash, err = chainhash.NewHashFromStr(hashOrHeight)
			if err != nil {
				return err
			}

			header, err = h.repository.GetBlockHeaderByHash(c.Request().Context(), hash)
			if err != nil {
				if strings.HasSuffix(err.Error(), " not found") {
					return echo.NewHTTPError(http.StatusNotFound, err.Error())
				} else {
					return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
				}
			}
		}

		prometheusBlobServerHttpGetBlockHeader.WithLabelValues("OK", "200").Inc()

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, header.Bytes())
		case HEX:
			return c.String(200, hex.EncodeToString(header.Bytes()))
		case JSON:
			return c.JSONPretty(200, header, "  ")
		default:
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}
