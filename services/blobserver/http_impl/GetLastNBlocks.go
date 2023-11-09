package http_impl

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/ordishs/gocore"
)

func (h *HTTP) GetLastNBlocks(c echo.Context) error {
	start := gocore.CurrentTime()
	defer func() {
		blobServerStat.NewStat("GetLastNBlocks_http").AddTime(start)
	}()

	n := int64(10)
	if c.QueryParam("n") != "" {
		var err error

		n, err = strconv.ParseInt(c.QueryParam("n"), 10, 64)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
	}

	includeOrphans := false
	if c.QueryParam("includeOrphans") == "true" {
		includeOrphans = true
	}

	h.logger.Debugf("[BlobServer_http] GetBlockChain for %s for last %d blocks", c.Request().RemoteAddr, n)

	blocks, err := h.repository.GetLastNBlocks(c.Request().Context(), n, includeOrphans)
	if err != nil {
		if strings.HasSuffix(err.Error(), " not found") {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
	}
	prometheusBlobServerHttpGetLastNBlocks.WithLabelValues("OK", "200").Inc()

	return c.JSONPretty(200, blocks, "  ")
}
