package handlers

import (
	"fmt"

	"github.com/labstack/echo/v4"
	"github.com/ordishs/gocore"
)

var logger = gocore.Log("bins")

func sendError(c echo.Context, status int, msg interface{}) error {
	logger.Errorf("%s %s - %d: %v", c.Request().Method, c.Request().URL, status, msg)

	var message string

	if err, ok := msg.(error); ok {
		// If msg is of error type, convert it to a string
		message = err.Error()
		// If msg is of string type, use it directly
	} else if err, ok := msg.(string); ok {
		message = err
	} else {
		// Otherwise, try to convert it to a string as it is
		message = fmt.Sprintf("%v", msg)
	}

	return c.String(status, message)
}
