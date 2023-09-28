package validator

import (
	"context"
	"fmt"

	_ "github.com/bitcoin-sv/ubsv/k8sresolver"
	"github.com/bitcoin-sv/ubsv/services/validator/validator_api"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"github.com/sercand/kuberesolver/v5"
	"google.golang.org/grpc/resolver"
)

type Client struct {
	client validator_api.ValidatorAPIClient
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

	validator_grpcAddress, _ := gocore.Config().Get("validator_grpcAddress")
	conn, err := util.GetGRPCClient(ctx, validator_grpcAddress, &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		return nil, err
	}

	client := validator_api.NewValidatorAPIClient(conn)

	return &Client{
		client: client,
	}, nil
}

func (c *Client) Stop() {
	// TODO
}

func (c *Client) Validate(ctx context.Context, tx *bt.Tx) error {
	resp, err := c.client.ValidateTransaction(ctx, &validator_api.ValidateTransactionRequest{
		TransactionData: tx.ExtendedBytes(),
	})
	if err != nil {
		return err
	}

	if !resp.Valid {
		return fmt.Errorf("invalid transaction: %s", resp.Reason)
	}

	return nil
}
