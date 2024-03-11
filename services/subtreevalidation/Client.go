package subtreevalidation

import (
	"context"

	"github.com/bitcoin-sv/ubsv/services/blockvalidation/blockvalidation_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/ordishs/gocore"
)

type Client struct {
	apiClient blockvalidation_api.BlockValidationAPIClient
	logger    ulogger.Logger
}

func NewClient(ctx context.Context, logger ulogger.Logger) *Client {
	blockValidationGrpcAddress, ok := gocore.Config().Get("blockvalidation_grpcAddress")
	if !ok {
		panic("no blockvalidation_grpcAddress setting found")
	}
	baConn, err := util.GetGRPCClient(ctx, blockValidationGrpcAddress, &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		panic(err)
	}

	client := &Client{
		apiClient: blockvalidation_api.NewBlockValidationAPIClient(baConn),
		logger:    logger,
	}

	return client
}

func (s *Client) Health(ctx context.Context) (bool, error) {
	_, err := s.apiClient.HealthGRPC(ctx, &blockvalidation_api.EmptyMessage{})
	if err != nil {
		return false, err
	}

	return true, nil
}

func (s *Client) Get(ctx context.Context, subtreeHash []byte) ([]byte, error) {
	req := &blockvalidation_api.GetSubtreeRequest{
		Hash: subtreeHash,
	}

	response, err := s.apiClient.Get(ctx, req)
	if err != nil {
		return nil, err
	}

	return response.Subtree, nil
}
