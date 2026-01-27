package dashboard

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/labstack/echo/v4"
)

const cookieName = "auth"

// AuthHandler handles authentication for the dashboard UI
type AuthHandler struct {
	logger   ulogger.Logger
	settings *settings.Settings
	authsha  [sha256.Size]byte
}

// NewAuthHandler creates a new authentication handler
func NewAuthHandler(logger ulogger.Logger, settings *settings.Settings) *AuthHandler {
	var authsha [sha256.Size]byte

	rpcUser := settings.RPC.RPCUser
	rpcPass := settings.RPC.RPCPass

	if rpcUser != "" && rpcPass != "" {
		login := rpcUser + ":" + rpcPass
		auth := "Basic " + base64.StdEncoding.EncodeToString([]byte(login))
		authsha = sha256.Sum256([]byte(auth))

		logger.Debugf("Auth initialized with user: %s", rpcUser)
	} else {
		logger.Warnf("RPC authentication not configured properly. User: %s, Pass: %s",
			rpcUser != "", rpcPass != "")
	}

	return &AuthHandler{
		logger:   logger,
		settings: settings,
		authsha:  authsha,
	}
}

// CheckAuth checks if the request has valid authentication credentials
func (h *AuthHandler) CheckAuth(r *http.Request) bool {
	// If no auth is configured, allow all requests
	if h.settings.RPC.RPCUser == "" || h.settings.RPC.RPCPass == "" {
		h.logger.Infof("No auth configured, allowing request")
		return true
	}

	// Get the Authorization header
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		// Check for auth in cookie
		cookie, err := r.Cookie(cookieName)
		if err != nil || cookie.Value == "" {
			h.logger.Debugf("No auth header or cookie found")
			return false
		}

		authHeader = cookie.Value
	}

	// Validate the auth header
	if !strings.HasPrefix(authHeader, "Basic ") {
		h.logger.Debugf("Auth header doesn't have Basic prefix")
		return false
	}

	// For direct comparison with credentials
	rpcUser := h.settings.RPC.RPCUser
	rpcPass := h.settings.RPC.RPCPass

	// Try to decode the auth header
	encodedCreds := strings.TrimPrefix(authHeader, "Basic ")

	decodedCreds, err := base64.StdEncoding.DecodeString(encodedCreds)
	if err != nil {
		h.logger.Debugf("Failed to decode auth header: %v", err)
		return false
	}

	// Split username and password
	parts := strings.SplitN(string(decodedCreds), ":", 2)
	if len(parts) != 2 {
		h.logger.Debugf("Invalid auth format")
		return false
	}

	username := parts[0]
	password := parts[1]

	// Direct credential comparison
	if username == rpcUser && password == rpcPass {
		h.logger.Debugf("Direct credential match successful")
		return true
	}

	// If direct comparison fails, try the hash comparison as a fallback
	authsha := sha256.Sum256([]byte(authHeader))
	isValid := authsha == h.authsha

	// Add debug logging
	if !isValid {
		expectedAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", rpcUser, rpcPass)))
		expectedHash := sha256.Sum256([]byte(expectedAuth))

		h.logger.Debugf("Auth validation failed. Got: %x, Expected: %x", authsha, expectedHash)
		h.logger.Debugf("Username comparison: '%s' vs '%s'", username, rpcUser)
		h.logger.Debugf("Password comparison: '%s' vs '%s'", password, rpcPass)
	}

	return isValid
}

// LoginHandler handles login requests
func (h *AuthHandler) LoginHandler(c echo.Context) error {
	h.logger.Infof("Login attempt received")

	// Check if this is a form submission
	username := c.FormValue("username")
	password := c.FormValue("password")

	if username != "" && password != "" {
		// Form-based authentication
		h.logger.Debugf("Form-based login attempt for user: %s", username)

		// Check if credentials match
		if username == h.settings.RPC.RPCUser && password == h.settings.RPC.RPCPass {
			// Create auth header for cookie
			authHeader := "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))

			// Set auth cookie
			cookie := new(http.Cookie)
			cookie.Name = cookieName
			cookie.Value = authHeader
			cookie.Path = "/"
			cookie.HttpOnly = true
			cookie.SameSite = http.SameSiteStrictMode
			cookie.Domain = ""    // Use the domain from the request
			cookie.MaxAge = 86400 // 24 hours
			c.SetCookie(cookie)

			h.logger.Infof("Form-based login successful, cookie set: %s", cookie.Name)

			return c.JSON(http.StatusOK, map[string]interface{}{
				"success": true,
			})
		}

		h.logger.Infof("Form-based login failed")

		return c.JSON(http.StatusUnauthorized, map[string]interface{}{
			"success": false,
			"error":   "Invalid credentials",
		})
	}

	// Traditional header-based authentication
	if h.CheckAuth(c.Request()) {
		// Set auth cookie
		authHeader := c.Request().Header.Get("Authorization")
		cookie := new(http.Cookie)
		cookie.Name = cookieName
		cookie.Value = authHeader
		cookie.Path = "/"
		cookie.HttpOnly = true
		cookie.SameSite = http.SameSiteStrictMode
		cookie.Domain = ""    // Use the domain from the request
		cookie.MaxAge = 86400 // 24 hours
		c.SetCookie(cookie)

		h.logger.Infof("Login successful, cookie set: %s", cookie.Name)

		return c.JSON(http.StatusOK, map[string]interface{}{
			"success": true,
		})
	}

	h.logger.Infof("Login failed")

	return c.JSON(http.StatusUnauthorized, map[string]interface{}{
		"success": false,
		"error":   "Invalid credentials",
	})
}

// LogoutHandler handles logout requests
func (h *AuthHandler) LogoutHandler(c echo.Context) error {
	// Clear auth cookie
	cookie := new(http.Cookie)
	cookie.Name = cookieName
	cookie.Value = ""
	cookie.Path = "/"
	cookie.MaxAge = -1
	cookie.HttpOnly = true
	c.SetCookie(cookie)

	return c.JSON(http.StatusOK, map[string]interface{}{
		"success": true,
	})
}

// CheckAuthHandler checks if the user is authenticated
func (h *AuthHandler) CheckAuthHandler(c echo.Context) error {
	h.logger.Debugf("Auth check request received")

	// Log all cookies for debugging
	for _, cookie := range c.Cookies() {
		h.logger.Debugf("Cookie found: %s = %s", cookie.Name, cookie.Value)
	}

	// Log auth header if present
	authHeader := c.Request().Header.Get("Authorization")
	if authHeader != "" {
		h.logger.Debugf("Auth header found: %s", authHeader[:10]+"...")
	} else {
		h.logger.Debugf("No auth header found")
	}

	if h.CheckAuth(c.Request()) {
		h.logger.Debugf("Auth check successful")

		return c.JSON(http.StatusOK, map[string]interface{}{
			"authenticated": true,
		})
	}

	h.logger.Debugf("Auth check failed")

	return c.JSON(http.StatusUnauthorized, map[string]interface{}{
		"authenticated": false,
	})
}

// WebSocketConfigHandler returns the WebSocket configuration
func (h *AuthHandler) WebSocketConfigHandler(c echo.Context) error {
	// Determine the WebSocket URL based on the current host
	host := c.Request().Host
	wsProtocol := "ws"

	var wsPort string

	// Check if using HTTPS/WSS
	if c.Scheme() == "https" || c.Request().TLS != nil {
		wsProtocol = "wss"
	}

	// Extract hostname and port (handles both IPv4 and IPv6)
	hostname := host
	currentPort := ""

	if h, p, err := net.SplitHostPort(host); err == nil {
		hostname = h
		currentPort = p
	}
	// If SplitHostPort fails, assume it's just a hostname without port

	// Determine the port based on the environment
	isDevelopmentServer := false

	for _, devPort := range h.settings.Dashboard.DevServerPorts {
		if strings.Contains(host, fmt.Sprintf("localhost:%d", devPort)) {
			isDevelopmentServer = true
			break
		}
	}

	// Use a switch statement to determine the WebSocket port based on environment
	switch {
	case isDevelopmentServer:
		// Development environment - Vite dev server
		// WebSocket runs on the asset service port
		wsPort = h.settings.Dashboard.WebSocketPort
		hostname = "localhost"
	default:
		// For both local docker and production environments
		// The WebSocket is served from the same port as the dashboard
		// If behind a reverse proxy, the proxy handles the WebSocket upgrade
		wsPort = currentPort
	}

	// Build the WebSocket URL
	var wsURL string
	if wsPort != "" {
		wsURL = fmt.Sprintf("%s://%s:%s%s", wsProtocol, hostname, wsPort, h.settings.Dashboard.WebSocketPath)
	} else {
		// Standard ports (80/443) - no explicit port needed
		wsURL = fmt.Sprintf("%s://%s%s", wsProtocol, hostname, h.settings.Dashboard.WebSocketPath)
	}

	h.logger.Debugf("WebSocket config requested from %s, returning URL: %s", host, wsURL)

	return c.JSON(http.StatusOK, map[string]interface{}{
		"websocketUrl": wsURL,
	})
}

// NodeConfigHandler returns the current node configuration for local message filtering
func (h *AuthHandler) NodeConfigHandler(c echo.Context) error {
	// Get the Asset HTTP address from settings to determine the node's base URL
	assetHTTPAddress := h.settings.Asset.HTTPAddress

	// Parse the Asset HTTP address to get the hostname and port
	if assetHTTPAddress != "" {
		// The Asset HTTP address should be in format like "localhost:8090" or "teranode1:8090"
		// We need to construct the full URL for comparison with message base_urls
		var nodeBaseURL string
		if strings.HasPrefix(assetHTTPAddress, "http") {
			nodeBaseURL = assetHTTPAddress
		} else {
			nodeBaseURL = "http://" + assetHTTPAddress
		}

		h.logger.Debugf("Node config requested, returning base URL: %s", nodeBaseURL)

		return c.JSON(http.StatusOK, map[string]interface{}{
			"nodeBaseUrl": nodeBaseURL,
		})
	}

	h.logger.Warnf("Asset HTTP address not configured, cannot determine node base URL")

	return c.JSON(http.StatusOK, map[string]interface{}{
		"nodeBaseUrl": "",
	})
}

// AuthMiddleware is a middleware that checks if the user is authenticated
func (h *AuthHandler) AuthMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		// Skip auth check for login page and API endpoints
		path := c.Request().URL.Path
		if path == "/login" || path == authPathLogin || path == authPathCheck {
			return next(c)
		}

		// Check if the path is for the admin section
		if strings.HasPrefix(path, "/admin") {
			if !h.CheckAuth(c.Request()) {
				// If it's an API request, return 401
				if strings.HasPrefix(path, "/api/") {
					return c.JSON(http.StatusUnauthorized, map[string]interface{}{
						"error": "Authentication required",
					})
				}

				// Otherwise redirect to login page
				return c.Redirect(http.StatusFound, "/login?redirect="+path)
			}
		}

		return next(c)
	}
}

// PostAuthMiddleware is a middleware that requires authentication for all POST requests
func (h *AuthHandler) PostAuthMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		// Only apply authentication to POST requests
		if c.Request().Method == http.MethodPost {
			path := c.Request().URL.Path

			// Skip auth check for login and logout endpoints
			if path == authPathLogin {
				return next(c)
			}

			if !h.CheckAuth(c.Request()) {
				// Return 401 Unauthorized for API requests
				return c.JSON(http.StatusUnauthorized, map[string]interface{}{
					"success": false,
					"error":   "Authentication required for POST endpoints",
				})
			}
		}

		return next(c)
	}
}

// RequireAuthMiddleware is a middleware that requires authentication for all requests
func (h *AuthHandler) RequireAuthMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		if !h.CheckAuth(c.Request()) {
			return c.JSON(http.StatusUnauthorized, map[string]interface{}{
				"success": false,
				"error":   "Authentication required",
			})
		}

		return next(c)
	}
}
