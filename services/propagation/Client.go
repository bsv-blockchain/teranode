package propagation

import (
	"context"
	"time"

	_ "github.com/bitcoin-sv/ubsv/k8sresolver"
	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/sercand/kuberesolver/v5"
	"google.golang.org/grpc/resolver"
)

type Client struct {
	client propagation_api.PropagationAPIClient
}

func NewClient(ctx context.Context, logger utils.Logger) (*Client, error) {

	grpcResolver, _ := gocore.Config().Get("grpc_resolver")
	if grpcResolver == "k8s" {
		logger.Infof("[VALIDATOR] Using k8s resolver for clients")
		resolver.SetDefaultScheme("k8s")
	} else if grpcResolver == "kubernetes" {
		logger.Infof("[VALIDATOR] Using kubernetes resolver for clients")
		kuberesolver.RegisterInClusterWithSchema("k8s")
	}

	propagation_grpcAddress, _ := gocore.Config().Get("propagation_grpcAddress")
	conn, err := util.GetGRPCClient(ctx, propagation_grpcAddress, &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		return nil, err
	}

	client := propagation_api.NewPropagationAPIClient(conn)

	return &Client{
		client: client,
	}, nil
}

func (c *Client) Stop() {
	// TODO
}

func (c *Client) ProcessTransaction(ctx context.Context, tx *bt.Tx) error {
	_, err := c.client.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
		Tx: tx.ExtendedBytes(),
	})
	if err != nil {
		return err
	}

	return nil
}

func GetProcessTransactionChannel(ctx context.Context) (chan []byte, chan error, error) {
	errorCh := make(chan error)
	txCh := make(chan []byte)

	defer func() {
		ctx.Done()
		close(errorCh)
		close(txCh)
	}()

	propagation_grpcAddress, _ := gocore.Config().Get("propagation_grpcAddress")
	conn, err := util.GetGRPCClient(ctx, propagation_grpcAddress, &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		return nil, nil, err
	}

	client := propagation_api.NewPropagationAPIClient(conn)

	stream, err := client.ProcessTransactionStream(ctx)
	if err != nil {
		return nil, nil, err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				errorCh <- ctx.Err()
				return

			case txBytes := <-txCh:
				if err := stream.Send(&propagation_api.ProcessTransactionRequest{
					Tx: txBytes,
				}); err != nil {
					errorCh <- err
				}

			case <-time.After(10 * time.Second):
				propagation_grpcAddress, _ = gocore.Config().Get("propagation_grpcAddress")
				conn, err = util.GetGRPCClient(ctx, propagation_grpcAddress, &util.ConnectionOptions{
					OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
					Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
					MaxRetries:  3,
				})
				if err != nil {
					errorCh <- err
					return
				}
			}
		}
	}()

	return txCh, errorCh, nil

	// _, err := c.client.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{
	// 	Tx: tx.ExtendedBytes(),
	// })
	// if err != nil {
	// 	return err
	// }

	// return nil
}
