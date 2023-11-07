package http_impl

import (
	"encoding/hex"
	"net/http"

	"github.com/bitcoin-sv/ubsv/services/blobserver"
	"github.com/labstack/echo/v4"
	"github.com/ordishs/gocore"
)

func (h *HTTP) GetBestBlockHeader(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		start := gocore.CurrentNanos()
		defer func() {
			blobserver.BlobServerStat.NewStat("GetBestBlockHeader_http").AddTime(start)
		}()

		h.logger.Debugf("[BlobServer_http] GetBestBlockHeader in %s for %s", mode, c.Request().RemoteAddr)

		blockHeader, meta, err := h.repository.GetBestBlockHeader(c.Request().Context())
		if err != nil {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
		prometheusBlobServerHttpGetBestBlockHeader.WithLabelValues("OK", "200").Inc()

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
