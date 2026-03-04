package banlist

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// CreateEchoMiddleware returns an Echo middleware that rejects requests
// from banned IPs with HTTP 403 Forbidden.
func CreateEchoMiddleware(bl Interface) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if bl.IsBanned(c.RealIP()) {
				return c.String(http.StatusForbidden, "Forbidden")
			}

			return next(c)
		}
	}
}
