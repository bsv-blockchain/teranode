package subtreevalidation

import (
	"context"

	"github.com/bitcoin-sv/ubsv/services/subtreevalidation/subtreevalidation_api"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type Client struct {
	apiClient subtreevalidation_api.SubtreeValidationAPIClient
	logger    ulogger.Logger
}

func NewClient(ctx context.Context, logger ulogger.Logger) Interface {
	subtreeValidationGrpcAddress, ok := gocore.Config().Get("subtreevalidation_grpcAddress")
	if !ok {
		panic("no blockvalidation_grpcAddress setting found")
	}
	baConn, err := util.GetGRPCClient(ctx, subtreeValidationGrpcAddress, &util.ConnectionOptions{
		OpenTracing: gocore.Config().GetBool("use_open_tracing", true),
		Prometheus:  gocore.Config().GetBool("use_prometheus_grpc_metrics", true),
		MaxRetries:  3,
	})
	if err != nil {
		panic(err)
	}

	client := &Client{
		apiClient: subtreevalidation_api.NewSubtreeValidationAPIClient(baConn),
		logger:    logger,
	}

	return client
}

func (s *Client) Health(ctx context.Context) (int, string, error) {
	_, err := s.apiClient.HealthGRPC(ctx, &subtreevalidation_api.EmptyMessage{})
	if err != nil {
		return -1, "", err
	}

	return 0, "", nil
}

func (s *Client) CheckSubtree(ctx context.Context, subtreeHash chainhash.Hash, baseURL string, blockHeight uint32) error {
	req := &subtreevalidation_api.CheckSubtreeRequest{
		Hash:        subtreeHash[:],
		BaseUrl:     baseURL,
		BlockHeight: blockHeight,
	}

	_, err := s.apiClient.CheckSubtree(ctx, req)
	if err != nil {
		return err
	}

	return nil
}
