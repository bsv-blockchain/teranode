package probe

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

const bearerPrefix = "Bearer "

func (s *Server) newRouter() *echo.Echo {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.Use(middleware.Recover())
	e.Use(s.requireToken)

	e.GET("/health", s.handleHealth)

	return e
}

func (s *Server) requireToken(next echo.HandlerFunc) echo.HandlerFunc {
	expected := []byte(s.settings.Probe.AuthToken)

	return func(c echo.Context) error {
		header := c.Request().Header.Get(echo.HeaderAuthorization)
		if !strings.HasPrefix(header, bearerPrefix) {
			return c.NoContent(http.StatusUnauthorized)
		}

		presented := []byte(strings.TrimPrefix(header, bearerPrefix))
		if subtle.ConstantTimeCompare(presented, expected) != 1 {
			return c.NoContent(http.StatusUnauthorized)
		}

		return next(c)
	}
}

func (s *Server) handleHealth(c echo.Context) error {
	status, message, _ := s.Health(c.Request().Context(), c.QueryParam("liveness") == "true")

	return c.String(status, message)
}
