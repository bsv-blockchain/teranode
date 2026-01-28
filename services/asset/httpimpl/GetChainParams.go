package httpimpl

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

type ChainParamsResponse struct {
	LegacyPubKeyHashAddrID uint8  `json:"legacyPubKeyHashAddrID"`
	LegacyScriptHashAddrID uint8  `json:"legacyScriptHashAddrID"`
	Name                   string `json:"name"`
}

func (h *HTTP) GetChainParams(c echo.Context) error {
	params := h.settings.ChainCfgParams
	if params == nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "chain params not configured")
	}

	return c.JSON(http.StatusOK, &ChainParamsResponse{
		LegacyPubKeyHashAddrID: params.LegacyPubKeyHashAddrID,
		LegacyScriptHashAddrID: params.LegacyScriptHashAddrID,
		Name:                   params.Name,
	})
}
