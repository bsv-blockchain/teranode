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

	e.GET("/health", s.handleHealth, s.requireToken)
	e.GET("/api/snapshot", s.handleSnapshot, s.requireToken)
	e.GET("/api/alarms", s.handleAlarms, s.requireToken)
	e.GET("/api/history", s.handleHistory, s.requireToken)
	e.POST("/api/snooze", s.handleSnooze, s.requireToken)
	e.POST("/api/unsnooze", s.handleUnsnooze, s.requireToken)
	e.GET("/ws", s.handleWS)

	return e
}

func (s *Server) requireToken(next echo.HandlerFunc) echo.HandlerFunc {
	expected := []byte(s.settings.Probe.AuthToken)

	return func(c echo.Context) error {
		if len(expected) == 0 {
			return c.NoContent(http.StatusUnauthorized)
		}

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
