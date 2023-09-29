package propagation

import (
	"context"
	"time"

	_ "github.com/bitcoin-sv/ubsv/k8sresolver"
	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/sercand/kuberesolver/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
)

type StreamingClient struct {
	txCh    chan []byte
	errorCh chan error
	conn    *grpc.ClientConn
	stream  propagation_api.PropagationAPI_ProcessTransactionStreamClient
}

func NewStreamingClient(ctx context.Context, logger utils.Logger) (*StreamingClient, error) {
	initResolver(logger)

	sc := &StreamingClient{
		txCh:    make(chan []byte),
		errorCh: make(chan error),
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				sc.errorCh <- ctx.Err()
				return

			case txBytes := <-sc.txCh:
				if sc.stream == nil {
					if err := sc.initStream(ctx); err != nil {
						return
					}
				}

				if err := sc.stream.Send(&propagation_api.ProcessTransactionRequest{
					Tx: txBytes,
				}); err != nil {
					sc.errorCh <- err
				}

				if _, err := sc.stream.Recv(); err != nil {
					sc.errorCh <- err
				}

				sc.errorCh <- nil

			case <-time.After(10 * time.Second):
				_ = sc.stream.CloseSend()
				_ = sc.conn.Close()
				sc.stream = nil
				sc.conn = nil
			}
		}
	}()

	return sc, nil
}

func (sc *StreamingClient) ProcessTransaction(ctx context.Context, txBytes []byte) error {
	sc.txCh <- txBytes
	return <-sc.errorCh
}

func initResolver(logger utils.Logger) {
	grpcResolver, _ := gocore.Config().Get("grpc_resolver")
	if grpcResolver == "k8s" {
		logger.Infof("[VALIDATOR] Using k8s resolver for clients")
		resolver.SetDefaultScheme("k8s")
	} else if grpcResolver == "kubernetes" {
		logger.Infof("[VALIDATOR] Using kubernetes resolver for clients")
		kuberesolver.RegisterInClusterWithSchema("k8s")
	}
}

func (sc *StreamingClient) initStream(ctx context.Context) error {
	var err error

	propagation_grpcAddress, _ := gocore.Config().Get("propagation_grpcAddresses")

	sc.conn, err = util.GetGRPCClient(ctx, propagation_grpcAddress, &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		return err
	}

	client := propagation_api.NewPropagationAPIClient(sc.conn)

	sc.stream, err = client.ProcessTransactionStream(ctx)
	if err != nil {
		return err
	}
	return nil
}
