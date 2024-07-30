package http_impl

import (
	"github.com/bitcoin-sv/ubsv/errors"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/ordishs/gocore"
)

func (h *HTTP) GetBlockGraphData(c echo.Context) error {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("GetBlockGraphData_http").AddTime(start)
	}()

	periodMillis := int64(0)
	switch c.Param("period") {
	case "2h":
		periodMillis = time.Now().Add(-2*time.Hour).UnixNano() / int64(time.Millisecond)
	case "6h":
		periodMillis = time.Now().Add(-6*time.Hour).UnixNano() / int64(time.Millisecond)
	case "12h":
		periodMillis = time.Now().Add(-12*time.Hour).UnixNano() / int64(time.Millisecond)
	case "24h":
		periodMillis = time.Now().Add(-24*time.Hour).UnixNano() / int64(time.Millisecond)
	case "1w":
		periodMillis = time.Now().Add(-7*24*time.Hour).UnixNano() / int64(time.Millisecond)
	case "1m":
		periodMillis = time.Now().Add(-30*24*time.Hour).UnixNano() / int64(time.Millisecond)
	case "3m":
		periodMillis = time.Now().Add(-90*24*time.Hour).UnixNano() / int64(time.Millisecond)
	default:
		return echo.NewHTTPError(http.StatusBadRequest, errors.NewInvalidArgumentError("period is required"))
	}

	dataPoints, err := h.repository.GetBlockGraphData(c.Request().Context(), uint64(periodMillis))
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSONPretty(200, dataPoints, "  ")
}
