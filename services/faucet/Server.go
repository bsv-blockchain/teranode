package faucet

import (
	"context"
	"embed"
	"encoding/hex"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/services/coinbase"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/distributor"
	"github.com/bitcoin-sv/teranode/util/health"
	"github.com/bitcoin-sv/teranode/util/servicemanager"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/libsv/go-bt/v2"
)

//go:embed all:public/*
var embeddedFiles embed.FS

type Faucet struct {
	logger           ulogger.Logger
	settings         *settings.Settings
	e                *echo.Echo
	coinbaseClient   coinbase.ClientI
	distributor      *distributor.Distributor
	blockchainClient blockchain.ClientI
}

func New(logger ulogger.Logger, tSettings *settings.Settings, blockchainClient blockchain.ClientI) *Faucet {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.Use(middleware.Recover())

	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{echo.GET},
	}))

	f := &Faucet{
		logger:           logger,
		settings:         tSettings,
		e:                e,
		blockchainClient: blockchainClient,
	}

	return f
}

func (f *Faucet) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	checks := make([]health.Check, 0, 3)

	if f.blockchainClient != nil {
		checks = append(checks, health.Check{Name: "BlockchainClient", Check: f.blockchainClient.Health})
		checks = append(checks, health.Check{Name: "FSM", Check: blockchain.CheckFSM(f.blockchainClient)})
	}

	if f.coinbaseClient != nil {
		checks = append(checks, health.Check{Name: "CoinbaseClient", Check: f.coinbaseClient.Health})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

func (f *Faucet) Init(ctx context.Context) error {
	var err error

	f.coinbaseClient, err = coinbase.NewClient(ctx, f.logger, f.settings)
	if err != nil {
		return errors.NewProcessingError("could not create coinbase client", err)
	}

	f.distributor, err = distributor.NewDistributor(ctx, f.logger, f.settings, distributor.WithBackoffDuration(1*time.Second), distributor.WithRetryAttempts(3), distributor.WithFailureTolerance(0))
	if err != nil {
		return errors.NewServiceError("could not create distributor", err)
	}

	f.e.POST("/faucet/request", f.faucetHandler)
	f.e.POST("/faucet/submit", f.submitHandler)

	f.e.GET("/faucet/*", f.staticHandler)
	f.e.GET("/faucet", f.staticHandler)

	return nil
}

func (f *Faucet) Start(ctx context.Context) error {
	addr := f.settings.Faucet.HTTPListenAddress
	if addr == "" {
		return errors.NewConfigurationError("faucet_httpListenAddress is required")
	}

	mode := "HTTPS"
	if level := f.settings.SecurityLevelHTTP; level == 0 {
		mode = "HTTP"
	}

	f.logger.Infof("Faucet %s service listening on %s", mode, addr)

	go func() {
		<-ctx.Done()
		f.logger.Infof("[Faucet] %s service shutting down", mode)

		err := f.e.Shutdown(ctx)
		if err != nil {
			f.logger.Errorf("[Faucet] %s service shutdown error: %s", mode, err)
		}
	}()

	var err error

	if mode == "HTTP" {
		servicemanager.AddListenerInfo(fmt.Sprintf("Faucet HTTP listening on %s", addr))
		err = f.e.Start(addr)
	} else {
		certFile := f.settings.ServerCertFile
		if certFile == "" {
			return errors.NewConfigurationError("server_certFile is required for HTTPS")
		}

		keyFile := f.settings.ServerKeyFile
		if keyFile == "" {
			return errors.NewConfigurationError("server_keyFile is required for HTTPS")
		}

		servicemanager.AddListenerInfo(fmt.Sprintf("Faucet HTTPS listening on %s", addr))
		err = f.e.StartTLS(addr, certFile, keyFile)
	}

	if err != http.ErrServerClosed {
		return err
	}

	return nil
}

func (f *Faucet) Stop(ctx context.Context) error {
	return f.e.Shutdown(ctx)
}

type faucetPayload struct {
	Address string `json:"address"`
}

type faucetResponse struct {
	Tx string `json:"tx"`
}

func (f *Faucet) faucetHandler(c echo.Context) error {
	var payload faucetPayload

	if err := c.Bind(&payload); err != nil {
		return c.String(http.StatusBadRequest, "Invalid request")
	}

	tx, err := f.coinbaseClient.RequestFunds(c.Request().Context(), payload.Address, true)
	if err != nil {
		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, faucetResponse{
		Tx: hex.EncodeToString(tx.ExtendedBytes()),
	})
}

type submitPayload struct {
	Tx string `json:"tx"`
}

type submitResponse struct {
	Timestamp string                         `json:"timestamp"`
	Txid      string                         `json:"txid"`
	Responses []*distributor.ResponseWrapper `json:"responses"`
}

func (f *Faucet) submitHandler(c echo.Context) error {
	var payload submitPayload

	if err := c.Bind(&payload); err != nil {
		return c.String(http.StatusBadRequest, "Invalid request")
	}

	tx, err := bt.NewTxFromString(payload.Tx)
	if err != nil {
		return c.String(http.StatusBadRequest, "Invalid transaction")
	}

	if !tx.IsExtended() {
		return c.String(http.StatusBadRequest, "Transaction is not extended")
	}

	responses, _ := f.distributor.SendTransaction(c.Request().Context(), tx)

	resp := submitResponse{
		Timestamp: time.Now().Format(time.RFC3339),
		Txid:      tx.TxIDChainHash().String(),
		Responses: responses,
	}

	return c.JSON(http.StatusOK, resp)
}

func (f *Faucet) staticHandler(c echo.Context) error {
	var resource string

	path := c.Request().URL.Path

	if path == "/faucet" {
		resource = "public/index.html"
	} else {
		resource = fmt.Sprintf("public%s", path[7:])
	}

	// Remove trailing slash if present
	resource = strings.TrimSuffix(resource, "/")

	b, err := embeddedFiles.ReadFile(resource)
	if err != nil {
		// Just in case we're missing the /index.html, add it and try again...
		resource += "/index.html"

		b, err = embeddedFiles.ReadFile(resource)
		if err != nil {
			resource = "public/index.html"

			b, err = embeddedFiles.ReadFile(resource)
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
