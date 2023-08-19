package http_impl

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
)

// swagger:model errorResponse
type errorResponse struct {
	Status int32  `json:"status"`
	Code   int32  `json:"code"`
	Err    string `json:"error"`
}

func sendError(c echo.Context, status int, code int32, err error) error {

	if status == http.StatusInternalServerError && strings.Contains(err.Error(), "rpc error: code = InvalidArgument desc") {
		status = http.StatusBadRequest
	}

	e := &errorResponse{
		Status: int32(status),
		Code:   code,
		Err:    err.Error(),
	}

	return c.JSON(status, e)
}
