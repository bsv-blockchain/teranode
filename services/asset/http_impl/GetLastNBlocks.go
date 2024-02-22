package http_impl

import (
	"errors"
	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/ordishs/gocore"
)

func (h *HTTP) GetLastNBlocks(c echo.Context) error {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("GetLastNBlocks_http").AddTime(start)
	}()

	n := int64(10)
	if c.QueryParam("n") != "" {
		var err error

		n, err = strconv.ParseInt(c.QueryParam("n"), 10, 64)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
	}

	fromHeight := uint64(0)
	if c.QueryParam("fromHeight") != "" {
		var err error

		fromHeight, err = strconv.ParseUint(c.QueryParam("fromHeight"), 10, 32)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
	}

	includeOrphans := false
	if c.QueryParam("includeOrphans") == "true" {
		includeOrphans = true
	}

	h.logger.Debugf("[Asset_http] GetBlockChain for %s for last %d blocks", c.Request().RemoteAddr, n)

	blocks, err := h.repository.GetLastNBlocks(c.Request().Context(), n, includeOrphans, uint32(fromHeight))
	if err != nil {
		if errors.Is(err, ubsverrors.ErrNotFound) || strings.Contains(err.Error(), "not found") {
			return echo.NewHTTPError(http.StatusNotFound, err.Error())
		} else {
			return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
		}
	}
	prometheusAssetHttpGetLastNBlocks.WithLabelValues("OK", "200").Inc()

	return c.JSONPretty(200, blocks, "  ")
}
