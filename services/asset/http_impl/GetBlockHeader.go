package http_impl

import (
	"encoding/hex"
	"errors"
	"net/http"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func (h *HTTP) GetBlockHeader(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		start := gocore.CurrentTime()
		defer func() {
			AssetStat.NewStat("GetBlockHeader_http").AddTime(start)
		}()

		hashParam := c.Param("hash")
		h.logger.Debugf("[Asset_http] GetBlockHeader in %s for %s: %s", mode, c.Request().RemoteAddr, hashParam)

		var hash *chainhash.Hash
		var err error

		hash, err = chainhash.NewHashFromStr(hashParam)
		if err != nil {
			return err
		}

		var header *model.BlockHeader
		var meta *model.BlockHeaderMeta

		header, meta, err = h.repository.GetBlockHeader(c.Request().Context(), hash)
		if err != nil {
			if errors.Is(err, ubsverrors.ErrNotFound) {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		prometheusAssetHttpGetBlockHeader.WithLabelValues("OK", "200").Inc()

		switch mode {
		case BINARY_STREAM:
			return c.Blob(200, echo.MIMEOctetStream, header.Bytes())
		case HEX:
			return c.String(200, hex.EncodeToString(header.Bytes()))
		case JSON:
			headerResponse := &blockHeaderResponse{
				BlockHeader: header,
				Hash:        header.String(),
				Height:      meta.Height,
				TxCount:     meta.TxCount,
				SizeInBytes: meta.SizeInBytes,
				Miner:       meta.Miner,
			}
			return c.JSONPretty(200, headerResponse, "  ")
		default:
			return echo.NewHTTPError(http.StatusInternalServerError, "Bad read mode")
		}
	}
}
