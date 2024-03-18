package http_impl

import (
	"encoding/hex"
	"errors"
	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"net/http"
	"strconv"
	"strings"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func (h *HTTP) GetBlockHeaders(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		start := gocore.CurrentTime()
		defer func() {
			AssetStat.NewStat("GetBlockHeaders_http").AddTime(start)
		}()

		hashStr := c.Param("hash")
		nStr := c.QueryParam("n")

		numberOfHeaders := 100
		if nStr != "" {
			numberOfHeaders, _ = strconv.Atoi(nStr)
			if numberOfHeaders == 0 {
				numberOfHeaders = 100
			}
			if numberOfHeaders > 1000 {
				numberOfHeaders = 1000
			}
		}

		h.logger.Debugf("[Asset_http] Get %s Block Headers in %s for %s", mode, c.Request().RemoteAddr, hashStr)

		var headers []*model.BlockHeader
		var heights []uint32
		hash, err := chainhash.NewHashFromStr(hashStr)
		if err != nil {
			return err
		}

		headers, heights, err = h.repository.GetBlockHeaders(c.Request().Context(), hash, uint64(numberOfHeaders))
		if err != nil {
			if errors.Is(err, ubsverrors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		prometheusAssetHttpGetBlockHeader.WithLabelValues("OK", "200").Inc()

		if mode == JSON {
			headerResponses := make([]*blockHeaderResponse, 0, len(headers))
			for idx, header := range headers {
				headerResponses = append(headerResponses, &blockHeaderResponse{
					BlockHeader: header,
					Height:      heights[idx],
					Hash:        header.String(),
				})
			}
			return c.JSONPretty(200, headerResponses, "  ")
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
