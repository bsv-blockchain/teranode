package dashboard

import (
	"embed"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/labstack/echo/v4"
)

var (
	//go:embed all:build/*

	res embed.FS
)

func AppHandler(c echo.Context) error {

	var resource string

	path := c.Request().URL.Path

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
			resource = "build/index.html"
			b, err = res.ReadFile(resource)
			if err != nil {
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

	return c.Blob(http.StatusOK, mimeType, b)

}
