package http_impl

import (
	"encoding/hex"
	"net/http"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func (h *HTTP) GetFullBlock(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		start := gocore.CurrentTime()
		defer func() {
			AssetStat.NewStat("GetBlock_http").AddTime(start)
			h.logger.Infof("[Asset_http] GetFullBlock in %s for %s: %s DONE in %s", mode, c.Request().RemoteAddr, c.Param("hash"), time.Since(start))
		}()

		hashParam := c.Param("hash")
		h.logger.Debugf("[Asset_http] GetBlockHeader in %s for %s: %s", mode, c.Request().RemoteAddr, hashParam)
		defer h.logger.Debugf("[Asset_http] GetBlockHeader completed in %s for %s: %s", mode, c.Request().RemoteAddr, hashParam)

		var hash *chainhash.Hash
		var err error

		hash, err = chainhash.NewHashFromStr(hashParam)
		if err != nil {
			return err
		}
		b, err := h.repository.GetFullBlockBytes(c.Request().Context(), *hash)
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		// sign the response, if the private key is set, ignore error
		// do this before any output is sent to the client, this adds a signature to the response header
		_ = h.Sign(c.Response(), hash.CloneBytes())

		prometheusAssetHttpGetBlock.WithLabelValues("OK", "200").Inc()

		if mode == JSON {
			return echo.NewHTTPError(http.StatusNotImplemented, "Bad read mode")
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
