package http_impl

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/ordishs/gocore"
)

func (h *HTTP) GetBlocks(c echo.Context) error {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("GetBlocks_http").AddTime(start)
	}()

	offset, limit, err := h.getLimitOffset(c)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	includeOrphans := false
	if c.QueryParam("includeOrphans") == "true" {
		includeOrphans = true
	}

	// First we find the latest block height
	_, blockMeta, err := h.repository.GetBestBlockHeader(c.Request().Context())
	if err != nil {
		if strings.HasSuffix(err.Error(), " not found") {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
	}

	latestBlockHeight := blockMeta.Height
	fromHeight := latestBlockHeight - uint32(offset)

	h.logger.Debugf("[Asset_http] GetBlockChain for %s with offset = %d, limit = %d and fromHeight = %d", c.Request().RemoteAddr, offset, limit, fromHeight)

	blocks, err := h.repository.GetLastNBlocks(c.Request().Context(), int64(limit), includeOrphans, uint32(fromHeight))
	if err != nil {
		if strings.HasSuffix(err.Error(), " not found") {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
	}
	prometheusAssetHttpGetLastNBlocks.WithLabelValues("OK", "200").Inc()

	response := ExtendedResponse{
		Data: blocks,
		Pagination: Pagination{
			Offset:       offset,
			Limit:        limit,
			TotalRecords: int(latestBlockHeight) + 1,
		},
	}

	h.logger.Infof("[GetBlocks][%d][%d] sending to client in json (%d nodes)", offset, limit, len(blocks))
	return c.JSONPretty(200, response, "  ")
}
