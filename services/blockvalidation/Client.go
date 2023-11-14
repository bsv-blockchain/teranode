package blockvalidation

import (
	"context"
	"fmt"
	"time"

	blockvalidation_api "github.com/bitcoin-sv/ubsv/services/blockvalidation/blockvalidation_api"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	apiClient blockvalidation_api.BlockValidationAPIClient
}

func NewClient(ctx context.Context) *Client {
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

	return &Client{
		apiClient: blockvalidation_api.NewBlockValidationAPIClient(baConn),
	}
}

func (s Client) Health(ctx context.Context) (bool, error) {
	_, err := s.apiClient.Health(ctx, &emptypb.Empty{})
	if err != nil {
		return false, err
	}

	return true, nil
}

func (s Client) BlockFound(ctx context.Context, blockHash *chainhash.Hash, baseUrl string) error {
	req := &blockvalidation_api.BlockFoundRequest{
		Hash:    blockHash.CloneBytes(),
		BaseUrl: baseUrl,
	}

	_, err := s.apiClient.BlockFound(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (s Client) SubtreeFound(ctx context.Context, subtreeHash *chainhash.Hash, baseUrl string) error {
	req := &blockvalidation_api.SubtreeFoundRequest{
		Hash:    subtreeHash.CloneBytes(),
		BaseUrl: baseUrl,
	}

	_, err := s.apiClient.SubtreeFound(ctx, req)
	if err != nil {
		return err
	}

	return nil
}

func (s Client) Get(ctx context.Context, subtreeHash []byte) ([]byte, error) {
	req := &blockvalidation_api.GetSubtreeRequest{
		Hash: subtreeHash,
	}

	response, err := s.apiClient.Get(ctx, req)
	if err != nil {
		return nil, err
	}

	return response.Subtree, nil
}

func (s Client) Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error {
	return fmt.Errorf("not implemented")
}

func (s Client) SetTTL(ctx context.Context, key []byte, ttl time.Duration) error {
	return fmt.Errorf("not implemented")
}
