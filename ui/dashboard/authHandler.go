package dashboard

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/labstack/echo/v4"
)

const cookieName = "auth"

var authLogger ulogger.Logger

func init() {
	authLogger = ulogger.New("authHandler")
}

// AuthHandler handles authentication for the dashboard UI
type AuthHandler struct {
	settings *settings.Settings
	authsha  [sha256.Size]byte
}

// NewAuthHandler creates a new authentication handler
func NewAuthHandler(settings *settings.Settings) *AuthHandler {
	var authsha [sha256.Size]byte

	rpcUser := settings.RPC.RPCUser
	rpcPass := settings.RPC.RPCPass

	if rpcUser != "" && rpcPass != "" {
		login := rpcUser + ":" + rpcPass
		auth := "Basic " + base64.StdEncoding.EncodeToString([]byte(login))
		authsha = sha256.Sum256([]byte(auth))

		// Debug log to help troubleshoot
		authLogger.Infof("Auth initialized with user: %s", rpcUser)
	} else {
		authLogger.Warnf("RPC authentication not configured properly. User: %s, Pass: %s",
			rpcUser != "", rpcPass != "")
	}

	return &AuthHandler{
		settings: settings,
		authsha:  authsha,
	}
}

// CheckAuth checks if the request has valid authentication credentials
func (h *AuthHandler) CheckAuth(r *http.Request) bool {
	// If no auth is configured, allow all requests
	if h.settings.RPC.RPCUser == "" || h.settings.RPC.RPCPass == "" {
		authLogger.Infof("No auth configured, allowing request")
		return true
	}

	// Get the Authorization header
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		// Check for auth in cookie
		cookie, err := r.Cookie(cookieName)
		if err != nil || cookie.Value == "" {
			authLogger.Debugf("No auth header or cookie found")
			return false
		}

		authHeader = cookie.Value
	}

	// Validate the auth header
	if !strings.HasPrefix(authHeader, "Basic ") {
		authLogger.Debugf("Auth header doesn't have Basic prefix")
		return false
	}

	// For direct comparison with credentials
	rpcUser := h.settings.RPC.RPCUser
	rpcPass := h.settings.RPC.RPCPass

	// Try to decode the auth header
	encodedCreds := strings.TrimPrefix(authHeader, "Basic ")

	decodedCreds, err := base64.StdEncoding.DecodeString(encodedCreds)
	if err != nil {
		authLogger.Debugf("Failed to decode auth header: %v", err)
		return false
	}

	// Split username and password
	parts := strings.SplitN(string(decodedCreds), ":", 2)
	if len(parts) != 2 {
		authLogger.Debugf("Invalid auth format")
		return false
	}

	username := parts[0]
	password := parts[1]

	// Direct credential comparison
	if username == rpcUser && password == rpcPass {
		authLogger.Debugf("Direct credential match successful")
		return true
	}

	// If direct comparison fails, try the hash comparison as a fallback
	authsha := sha256.Sum256([]byte(authHeader))
	isValid := authsha == h.authsha

	// Add debug logging
	if !isValid {
		expectedAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", rpcUser, rpcPass)))
		expectedHash := sha256.Sum256([]byte(expectedAuth))

		authLogger.Debugf("Auth validation failed. Got: %x, Expected: %x", authsha, expectedHash)
		authLogger.Debugf("Username comparison: '%s' vs '%s'", username, rpcUser)
		authLogger.Debugf("Password comparison: '%s' vs '%s'", password, rpcPass)
	}

	return isValid
}

// LoginHandler handles login requests
func (h *AuthHandler) LoginHandler(c echo.Context) error {
	authLogger.Infof("Login attempt received")

	// Check if this is a form submission
	username := c.FormValue("username")
	password := c.FormValue("password")

	if username != "" && password != "" {
		// Form-based authentication
		authLogger.Debugf("Form-based login attempt for user: %s", username)

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

			authLogger.Infof("Form-based login successful, cookie set: %s", cookie.Name)

			return c.JSON(http.StatusOK, map[string]interface{}{
				"success": true,
			})
		}

		authLogger.Infof("Form-based login failed")

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

		authLogger.Infof("Login successful, cookie set: %s", cookie.Name)

		return c.JSON(http.StatusOK, map[string]interface{}{
			"success": true,
		})
	}

	authLogger.Infof("Login failed")

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
	authLogger.Debugf("Auth check request received")

	// Log all cookies for debugging
	for _, cookie := range c.Cookies() {
		authLogger.Debugf("Cookie found: %s = %s", cookie.Name, cookie.Value)
	}

	// Log auth header if present
	authHeader := c.Request().Header.Get("Authorization")
	if authHeader != "" {
		authLogger.Debugf("Auth header found: %s", authHeader[:10]+"...")
	} else {
		authLogger.Debugf("No auth header found")
	}

	if h.CheckAuth(c.Request()) {
		authLogger.Debugf("Auth check successful")

		return c.JSON(http.StatusOK, map[string]interface{}{
			"authenticated": true,
		})
	}

	authLogger.Debugf("Auth check failed")

	return c.JSON(http.StatusUnauthorized, map[string]interface{}{
		"authenticated": false,
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
