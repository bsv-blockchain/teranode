package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bitcoin-sv/ubsv/poc/binserve/foldermanager"
	"github.com/bitcoin-sv/ubsv/poc/binserve/handlers"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/ordishs/gocore"
)

func main() {
	logger := gocore.Log("bins")

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.Use(middleware.Recover())

	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{echo.GET},
	}))

	api := "/bin/:filename"

	/* The api syntax is:
	POST /bin/f2663be6fc5127f792bccc904da47168445307833264187bf2bd730000000000?ttl=15m
	PUT /bin/f2663be6fc5127f792bccc904da47168445307833264187bf2bd730000000000?ttl=15m
	PUT /bin/f2663be6fc5127f792bccc904da47168445307833264187bf2bd730000000000?ttl=0
	DELETE /bin/f2663be6fc5127f792bccc904da47168445307833264187bf2bd730000000000
	GET /bin/f2663be6fc5127f792bccc904da47168445307833264187bf2bd730000000000
	*/

	e.PUT(api, handlers.Upload) // query param of ttl= is optional and the body can be empty if you only want to update ttl
	e.DELETE(api, handlers.Delete)
	e.GET(api, handlers.Serve)

	fm := foldermanager.New(logger)

	stopCh := make(chan os.Signal, 1)

	signal.Notify(stopCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-stopCh

		fm.Stop()

		// Shutdown Echo server with a timeout
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := e.Shutdown(ctx); err != nil {
			logger.Fatal(err)
		}
	}()

	logger.Fatal(e.Start(":8080"))
}
