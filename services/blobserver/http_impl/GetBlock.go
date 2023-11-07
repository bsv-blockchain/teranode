package http_impl

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"

	"github.com/bitcoin-sv/ubsv/services/blobserver"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func (h *HTTP) GetBlockByHeight(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		start := gocore.CurrentNanos()
		defer func() {
			blobserver.BlobServerStat.NewStat("GetBlock_http").AddTime(start)
		}()

		h.logger.Debugf("[BlobServer_http] GetBlockByHeight in %s for %s: %s", mode, c.Request().RemoteAddr, c.Param("height"))
		height, err := strconv.ParseUint(c.Param("height"), 10, 64)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		block, err := h.repository.GetBlockByHeight(c.Request().Context(), uint32(height))
		if err != nil {
			if strings.HasSuffix(err.Error(), " not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}
		prometheusBlobServerHttpGetBlock.WithLabelValues("OK", "200").Inc()

		if mode == JSON {
			return c.JSONPretty(200, block, "  ")
		}

		b, err := block.Bytes()
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, b)
		case HEX:
			return c.String(200, hex.EncodeToString(b))
		default:
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}

func (h *HTTP) GetBlockByHash(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		h.logger.Debugf("[BlobServer_http] GetBlockByHash in %s for %s: %s", mode, c.Request().RemoteAddr, c.Param("hash"))
		hash, err := chainhash.NewHashFromStr(c.Param("hash"))
		if err != nil {
			return err
		}

		block, err := h.repository.GetBlockByHash(c.Request().Context(), hash)
		if err != nil {
			if strings.HasSuffix(err.Error(), " not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		prometheusBlobServerHttpGetBlock.WithLabelValues("OK", "200").Inc()

		if mode == JSON {
			return c.JSONPretty(200, block, "  ")
		}

		b, err := block.Bytes()
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, b)
		case HEX:
			return c.String(200, hex.EncodeToString(b))
		default:
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}
