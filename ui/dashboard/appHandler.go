package dashboard

import (
	"embed"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/labstack/echo/v4"
	"github.com/ordishs/gocore"
)

const (
	authPathLogin       = "/api/auth/login"
	authPathLogout      = "/api/auth/logout"
	authPathCheck       = "/api/auth/check"
	configPathWebSocket = "/api/config/websocket"
	configPathNode      = "/api/config/node"
)

var (
	//go:embed all:build/*

	res embed.FS

	logger      ulogger.Logger
	authHandler *AuthHandler
)

func init() {
	var logLevelStr, _ = gocore.Config().Get("logLevel", "INFO")
	logger = ulogger.New("appHandler", ulogger.WithLevel(logLevelStr))
}

// InitDashboard initializes the dashboard with settings
func InitDashboard(settings *settings.Settings) {
	authHandler = NewAuthHandler(logger, settings)
}

func AppHandler(c echo.Context) error {
	var resource string

	path := c.Request().URL.Path

	// Check if this is an auth API request
	switch {
	case path == authPathLogin:
		return authHandler.LoginHandler(c)
	case path == authPathLogout:
		return authHandler.LogoutHandler(c)
	case path == authPathCheck:
		return authHandler.CheckAuthHandler(c)
	case path == configPathWebSocket:
		return authHandler.WebSocketConfigHandler(c)
	case path == configPathNode:
		return authHandler.NodeConfigHandler(c)
	}

	// Check authentication for admin paths
	if strings.HasPrefix(path, "/admin") && !authHandler.CheckAuth(c.Request()) {
		// Redirect to login page
		return c.Redirect(http.StatusFound, "/login?redirect="+path)
	}

	if path == "/" {
		resource = "build/index.html"
	} else {
		resource = fmt.Sprintf("build%s", path)
	}

	// Remove trailing slash if present
	resource = strings.TrimSuffix(resource, "/")

	b, err := res.ReadFile(resource)
	if err != nil {
		// Just in case we're missing the /index.html, add it and try again...
		resource += "/index.html"
		b, err = res.ReadFile(resource)

		if err != nil {
			// Use SvelteKit's 200.html fallback for SPA routing
			resource = "build/200.html"
			b, err = res.ReadFile(resource)

			if err != nil {
				logger.Debugf("Resource %q - NOT FOUND", resource)
				return c.String(http.StatusNotFound, "Not found")
			}
		}
	}

	var mimeType string

	extension := filepath.Ext(resource)
	switch extension {
	case ".css":
		mimeType = "text/css"
	case ".woff2":
		mimeType = "font/woff2"
	case ".js":
		mimeType = "text/javascript"
	case ".png":
		mimeType = "image/png"
	case ".map":
		mimeType = "application/json"
	default:
		mimeType = "text/html"
	}

	logger.Debugf("Resource %q [%s] - OK", resource, mimeType)

	return c.Blob(http.StatusOK, mimeType, b)
}
