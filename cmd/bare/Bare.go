package bare

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

var txCounter atomic.Uint64
var byteCounter atomic.Uint64

func Bare() {
	e := echo.New()

	// Middleware
	e.Use(middleware.Recover())

	// Routes
	e.POST("/tx", func(c echo.Context) error {
		// Read the request body to track bytes sent
		data, err := io.ReadAll(c.Request().Body)
		if err != nil {
			return err
		}

		byteCounter.Add(uint64(len(data)))
		txCounter.Add(1)

		return c.String(http.StatusOK, "OK")
	})

	e.GET("/count", func(c echo.Context) error {
		// Return the total bytes sent
		return c.String(http.StatusOK, fmt.Sprintf("Total bytes sent: %d, %d requests\n", byteCounter.Load(), txCounter.Load()))
	})

	// Start the server with more optimized settings
	e.HideBanner = true                     // Hide the startup banner for performance reasons
	e.Pre(middleware.RemoveTrailingSlash()) // Remove trailing slashes from routes

	// Tune the server settings for production
	server := &http.Server{
		Addr:           "localhost:8833",
		Handler:        e,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	// Start the server
	log.Print(server.ListenAndServe())
}
