package main

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

var byteCounter atomic.Uint64

func main() {
	e := echo.New()

	// Middleware
	e.Use(middleware.Recover())

	throttler := make(chan struct{}, 10000)

	// Routes
	e.POST("/tx", func(c echo.Context) error {
		// Throttle the request
		throttler <- struct{}{}

		// Read the request body to track bytes sent
		data, err := io.ReadAll(c.Request().Body)
		if err != nil {
			return err
		}

		byteCounter.Add(uint64(len(data)))

		<-throttler

		return c.String(http.StatusOK, "OK")
	})

	e.GET("/count", func(c echo.Context) error {
		// Return the total bytes sent
		return c.String(http.StatusOK, fmt.Sprintf("Total bytes sent: %d", byteCounter.Load()))
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
