package http_impl

import (
	"net/http"

	"github.com/labstack/echo/v4"
	"github.com/ordishs/gocore"
)

func (h *HTTP) GetBalance(c echo.Context) error {
	start := gocore.CurrentTime()
	defer func() {
		AssetStat.NewStat("GetBalance_http").AddTime(start)
	}()

	numberOfUtxos, totalSatoshis, err := h.repository.GetBalance(c.Request().Context())
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	type balance struct {
		NumberOfUtxos uint64 `json:"numberOfUtxos"`
		TotalSatoshis uint64 `json:"totalSatoshis"`
	}

	return c.JSONPretty(200, balance{
		NumberOfUtxos: numberOfUtxos,
		TotalSatoshis: totalSatoshis,
	}, "  ")

}
