package http_impl

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/labstack/echo/v4"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

func (h *HTTP) GetNBlocks(mode ReadMode) func(c echo.Context) error {
	return func(c echo.Context) error {
		start := gocore.CurrentTime()
		defer func() {
			AssetStat.NewStat("GetNBlocks_http").AddTime(start)
		}()

		hashStr := c.Param("hash")
		nStr := c.QueryParam("n")

		numberOfBlocks := 100
		if nStr != "" {
			numberOfBlocks, _ = strconv.Atoi(nStr)
			if numberOfBlocks == 0 {
				numberOfBlocks = 100
			}
			if numberOfBlocks > 1000 {
				numberOfBlocks = 1000
			}
		}

		h.logger.Debugf("[Asset_http] Get %s Blocks in %s for %s", mode, c.Request().RemoteAddr, hashStr)

		hash, err := chainhash.NewHashFromStr(hashStr)
		if err != nil {
			return err
		}

		// get all the blocks from the repository
		blocks, err := h.repository.GetBlocks(c.Request().Context(), hash, uint32(numberOfBlocks))
		if err != nil {
			if errors.Is(err, errors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
				return echo.NewHTTPError(http.StatusNotFound, err.Error())
			} else {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
		}

		prometheusAssetHttpGetBlockHeader.WithLabelValues("OK", "200").Inc()

		if mode == JSON {
			return c.JSONPretty(200, blocks, "  ")
		}

		bytes := make([]byte, 0, len(blocks)*32*1024)
		for _, block := range blocks {
			blockBytes, err := block.Bytes()
			if err != nil {
				return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
			}
			bytes = append(bytes, blockBytes...)
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
