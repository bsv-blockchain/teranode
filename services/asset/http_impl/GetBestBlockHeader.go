package http_impl

import (
	"encoding/hex"
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/ordishs/gocore"
)

func (h *HTTP) GetBestBlockHeader(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		start := gocore.CurrentTime()
		defer func() {
			AssetStat.NewStat("GetBestBlockHeader_http").AddTime(start)
		}()

		h.logger.Debugf("[Asset_http] GetBestBlockHeader in %s for %s", mode, c.Request().RemoteAddr)
		defer h.logger.Debugf("[Asset_http] GetBestBlockHeader completed in %s for %s", mode, c.Request().RemoteAddr)

		blockHeader, meta, err := h.repository.GetBestBlockHeader(c.Request().Context())
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
		prometheusAssetHttpGetBestBlockHeader.WithLabelValues("OK", "200").Inc()

		r := &blockHeaderResponse{
			BlockHeader: blockHeader,
			Hash:        blockHeader.String(),
			Height:      meta.Height,
			TxCount:     meta.TxCount,
			SizeInBytes: meta.SizeInBytes,
			Miner:       meta.Miner,
		}

		switch mode {
		case JSON:
			return c.JSONPretty(200, r, "  ")
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, blockHeader.Bytes())
		case HEX:
			return c.String(200, hex.EncodeToString(blockHeader.Bytes()))
		default:
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}
