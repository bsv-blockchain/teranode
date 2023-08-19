package http_impl

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (h *HTTP) GetBlockHeaderByHeight(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		h.logger.Debugf("[BlobServer_http] GetBlockHeaderByHeight: %s", c.Param("height"))
		height, err := strconv.ParseUint(c.Param("height"), 10, 64)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		block, err := h.repository.GetBlockHeaderByHeight(c.Request().Context(), uint32(height))
		if err != nil {
			if strings.HasSuffix(err.Error(), " not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}
		prometheusBlobServerHttpGetBlockHeader.WithLabelValues("OK", "200").Inc()

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, block.Bytes())
		case HEX:
			return c.String(200, hex.EncodeToString(block.Bytes()))
		case JSON:
			return c.JSONPretty(200, block, "  ")
		default:
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}

func (h *HTTP) GetBlockHeaderByHash(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		h.logger.Debugf("[BlobServer_http] GetBlockHeaderByHash: %s", c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		header, err := h.repository.GetBlockHeaderByHash(c.Request().Context(), hash)
		if err != nil {
			if strings.HasSuffix(err.Error(), " not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
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
