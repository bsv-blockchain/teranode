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

type timing struct {
	total time.Duration
	count int
}

type StreamingClient struct {
	logger    utils.Logger
	txCh      chan []byte
	errorCh   chan error
	timingsCh chan chan timing
	conn      *grpc.ClientConn
	stream    propagation_api.PropagationAPI_ProcessTransactionStreamClient
	totalTime time.Duration
	count     int
}

func NewStreamingClient(ctx context.Context, logger utils.Logger, test ...bool) (*StreamingClient, error) {
	testMode := false
	if len(test) > 0 {
		testMode = test[0]
	}

	sc := &StreamingClient{
		logger:    logger,
		txCh:      make(chan []byte),
		errorCh:   make(chan error),
		timingsCh: make(chan chan timing),
	}

	sc.initResolver()

	go func() {
		for {
			select {
			case <-ctx.Done():
				sc.errorCh <- ctx.Err()
				return

			case getTimeCh := <-sc.timingsCh:
				getTimeCh <- timing{
					total: sc.totalTime,
					count: sc.count,
				}

			case txBytes := <-sc.txCh:
				if testMode {
					sc.errorCh <- nil
					continue
				}

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
				if sc.stream != nil {
					_ = sc.stream.CloseSend()
				}
				if sc.conn != nil {
					_ = sc.conn.Close()
				}
				sc.stream = nil
				sc.conn = nil
			}
		}
	}()

	return sc, nil
}

func (sc *StreamingClient) ProcessTransaction(txBytes []byte) error {
	start := time.Now()

	sc.txCh <- txBytes

	err := <-sc.errorCh

	sc.totalTime += time.Since(start)
	sc.count++

	return err
}

func (sc *StreamingClient) GetTimings() (time.Duration, int) {
	ch := make(chan timing)
	sc.timingsCh <- ch

	timings := <-ch

	return timings.total, timings.count
}

func (sc *StreamingClient) initResolver() {
	grpcResolver, _ := gocore.Config().Get("grpc_resolver")
	switch grpcResolver {
	case "k8s":
		sc.logger.Infof("[VALIDATOR] Using k8s resolver for clients")
		resolver.SetDefaultScheme("k8s")
	case "kubernetes":
		sc.logger.Infof("[VALIDATOR] Using kubernetes resolver for clients")
		kuberesolver.RegisterInClusterWithSchema("k8s")
	default:
		sc.logger.Infof("[VALIDATOR] Using default resolver for clients")
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
