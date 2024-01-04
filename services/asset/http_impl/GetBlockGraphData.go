package http_impl

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/ordishs/gocore"
)

func (h *HTTP) GetBlockGraphData(c echo.Context) error {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("GetBlockGraphData_http").AddTime(start)
	}()

	periodMillisStr := c.QueryParam("periodMillis")

	if periodMillisStr == "" {
		return echo.NewHTTPError(http.StatusBadRequest, errors.New("periodMillis is required"))
	}

	periodMillis, err := strconv.ParseInt(periodMillisStr, 10, 64)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	dataPoints, err := h.repository.GetBlockGraphData(c.Request().Context(), uint64(periodMillis))
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSONPretty(200, dataPoints, "  ")
}
