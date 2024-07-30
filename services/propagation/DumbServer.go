package propagation

import (
	"context"
	"github.com/bitcoin-sv/ubsv/errors"
	"io"
	"net/http"
	"time"

	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
)

// DumbPropagationServer type carries the logger within it
type DumbPropagationServer struct {
	propagation_api.UnimplementedPropagationAPIServer
	logger ulogger.Logger
}

// NewDumbPropagationServer will return a server instance with the logger stored within it
func NewDumbPropagationServer() *DumbPropagationServer {
	initPrometheusMetrics()

	logger := ulogger.New("dumbPS")

	logger.Warnf("Using DumbPropagationServer (for testing only)")

	return &DumbPropagationServer{
		logger: logger,
	}
}

func (ps *DumbPropagationServer) Health(ctx context.Context) (int, string, error) {
	return 0, "", nil
}

func (ps *DumbPropagationServer) Init(_ context.Context) (err error) {
	return nil
}

// Start function
func (ps *DumbPropagationServer) Start(ctx context.Context) (err error) {
	httpAddress, ok := gocore.Config().Get("propagation_httpListenAddress")
	if ok {
		err = ps.startHTTPServer(ctx, httpAddress)
		if err != nil {
			return errors.NewServiceError("HTTP server failed", err)
		}
	}

	// this will block
	if err = util.StartGRPCServer(ctx, ps.logger, "propagation", func(server *grpc.Server) {
		propagation_api.RegisterPropagationAPIServer(server, ps)
	}); err != nil {
		return err
	}

	return nil
}

func (ps *DumbPropagationServer) Stop(_ context.Context) error {
	return nil
}

func (ps *DumbPropagationServer) HealthGRPC(_ context.Context, _ *propagation_api.EmptyMessage) (*propagation_api.HealthResponse, error) {
	prometheusHealth.Inc()
	return &propagation_api.HealthResponse{
		Ok:        true,
		Timestamp: uint32(time.Now().Unix()),
	}, nil
}

func (ps *DumbPropagationServer) ProcessTransaction(ctx context.Context, req *propagation_api.ProcessTransactionRequest) (*propagation_api.EmptyMessage, error) {
	prometheusProcessedTransactions.Inc()

	return &propagation_api.EmptyMessage{}, nil
}

func (ps *DumbPropagationServer) ProcessTransactionStream(srv propagation_api.PropagationAPI_ProcessTransactionStreamServer) error {
	for {
		_, err := srv.Recv()
		if err != nil {
			return err
		}

		if err := srv.Send(&propagation_api.EmptyMessage{}); err != nil {
			return err
		}

		prometheusProcessedTransactions.Inc()
	}
}

func (ps *DumbPropagationServer) startHTTPServer(ctx context.Context, listenAddr string) error {
	e := echo.New()
	e.HideBanner = true

	e.Use(middleware.Recover())

	e.POST("/tx", func(c echo.Context) error {
		body, err := io.ReadAll(c.Request().Body)
		if err != nil {
			return c.String(http.StatusBadRequest, "Invalid request body")
		}

		if _, err = ps.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
			Tx: body,
		}); err != nil {
			return c.String(http.StatusInternalServerError, "Failed to process transaction")
		}

		return c.String(http.StatusOK, "OK")
	})

	go func() {
		ps.logger.Infof("[propagation] HTTP service listening on %s", listenAddr)
		if err := e.Start(listenAddr); err != nil {
			ps.logger.Errorf("HTTP server failed [%s]", err)
		}
	}()

	return nil
}
