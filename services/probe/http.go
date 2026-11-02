package probe

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/bsv-blockchain/teranode/services/probe/ui"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

const (
	bearerPrefix = "Bearer "
	uiIndex      = "index.html"
	uiCSP        = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
)

var uiContentTypes = map[string]string{
	uiIndex:     "text/html; charset=utf-8",
	"style.css": "text/css; charset=utf-8",
	"view.js":   "text/javascript; charset=utf-8",
	"app.js":    "text/javascript; charset=utf-8",
}

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
	e.GET("/", handleUI)
	e.GET("/:file", handleUI)

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

func handleUI(c echo.Context) error {
	h := c.Response().Header()
	h.Set("Content-Security-Policy", uiCSP)
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Referrer-Policy", "no-referrer")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Cache-Control", "no-store")

	name := c.Param("file")
	if c.Request().URL.Path == "/" {
		name = uiIndex
	}

	contentType, ok := uiContentTypes[name]
	if !ok {
		return c.NoContent(http.StatusNotFound)
	}

	body, err := ui.Files.ReadFile(name)
	if err != nil {
		return c.NoContent(http.StatusNotFound)
	}

	return c.Blob(http.StatusOK, contentType, body)
}
