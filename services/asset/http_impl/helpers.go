package http_impl

import (
	"github.com/labstack/echo/v4"
	"net/http"
	"strconv"
)

type Pagination struct {
	Offset       int `json:"offset"`
	Limit        int `json:"limit"`
	TotalRecords int `json:"totalRecords"`
}

type ExtendedResponse struct {
	Data       interface{} `json:"data"`
	Pagination Pagination  `json:"pagination"`
}

func (h *HTTP) getLimitOffset(c echo.Context) (int, int, error) {
	var err error

	// get subtree information
	offset := 0
	offsetStr := c.QueryParam("offset")
	if offsetStr != "" {
		offset, err = strconv.Atoi(offsetStr)
		if err != nil {
			return 0, 0, echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
	}

	limit := 20
	limitStr := c.QueryParam("limit")
	if limitStr != "" {
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			return 0, 0, echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
	}
	if limit > 100 {
		limit = 100
	}

	return offset, limit, nil
}
