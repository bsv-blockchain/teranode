package propagation

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// PropagationServer type carries the logger within it
type DumbPropagationServer struct {
	logger utils.Logger
	propagation_api.UnsafePropagationAPIServer
}

// New will return a server instance with the logger stored within it
func NewDumbPropagationServer() *DumbPropagationServer {
	initPrometheusMetrics()

	logger := gocore.Log("dumbPS")

	logger.Warnf("Using DumbPropagationServer (for testing only)")

	return &DumbPropagationServer{
		logger: logger,
	}
}

func (ps *DumbPropagationServer) Init(_ context.Context) (err error) {
	return nil
}

// Start function
func (ps *DumbPropagationServer) Start(ctx context.Context) (err error) {
	httpAddress, ok := gocore.Config().Get("propagation_httpAddress")
	if ok {
		var serverURL *url.URL
		serverURL, err = url.Parse(httpAddress)
		if err != nil {
			return fmt.Errorf("HTTP server failed to parse URL [%w]", err)
		}
		err = ps.StartHTTPServer(ctx, serverURL)
		if err != nil {
			return fmt.Errorf("HTTP server failed [%w]", err)
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

func (ps *DumbPropagationServer) Health(_ context.Context, _ *emptypb.Empty) (*propagation_api.HealthResponse, error) {
	prometheusHealth.Inc()
	return &propagation_api.HealthResponse{
		Ok:        true,
		Timestamp: timestamppb.New(time.Now()),
	}, nil
}

func (ps *DumbPropagationServer) ProcessTransaction(ctx context.Context, req *propagation_api.ProcessTransactionRequest) (*emptypb.Empty, error) {
	prometheusProcessedTransactions.Inc()

	return &emptypb.Empty{}, nil
}

func (ps *DumbPropagationServer) StartHTTPServer(ctx context.Context, serverURL *url.URL) error {
	// start a simple http listener that handles incoming transaction requests
	http.HandleFunc("/tx", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
		if _, err = ps.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
			Tx: body,
		}); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(err.Error()))
			return
		}
	})

	go func() {
		ps.logger.Infof("[%s] HTTP service listening on %s", serverURL)
		if err := http.ListenAndServe(serverURL.Host, nil); err != nil {
			ps.logger.Errorf("HTTP server failed [%s]", err)
		}
	}()

	return nil
}
