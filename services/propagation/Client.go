package propagation

import (
	"context"

	_ "github.com/bitcoin-sv/ubsv/k8sresolver"
	"github.com/bitcoin-sv/ubsv/services/propagation/propagation_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/sercand/kuberesolver/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/resolver"
)

type Client struct {
	client propagation_api.PropagationAPIClient
	conn   *grpc.ClientConn
}

func NewClient(ctx context.Context, logger utils.Logger) (*Client, error) {

	initResolver(logger)

	client, conn, err := getClientConn(ctx)
	if err != nil {
		return nil, err
	}

	return &Client{
		client: client,
		conn:   conn,
	}, nil
}

func (c *Client) Stop() {
	if c.client != nil && c.conn != nil {
		_ = c.conn.Close()
	}
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

func initResolver(logger utils.Logger) {
	grpcResolver, _ := gocore.Config().Get("grpc_resolver")
	switch grpcResolver {
	case "k8s":
		logger.Infof("[VALIDATOR] Using k8s resolver for clients")
		resolver.SetDefaultScheme("k8s")
	case "kubernetes":
		logger.Infof("[VALIDATOR] Using kubernetes resolver for clients")
		kuberesolver.RegisterInClusterWithSchema("k8s")
	default:
		logger.Infof("[VALIDATOR] Using default resolver for clients")
	}
}

func getClientConn(ctx context.Context) (propagation_api.PropagationAPIClient, *grpc.ClientConn, error) {
	propagation_grpcAddresses, _ := gocore.Config().GetMulti("propagation_grpcAddresses", "|")
	conn, err := util.GetGRPCClient(ctx, propagation_grpcAddresses[0], &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		return nil, nil, err
	}

	client := propagation_api.NewPropagationAPIClient(conn)

	return client, conn, nil
}
