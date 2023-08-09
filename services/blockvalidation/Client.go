package blockvalidation

import (
	"context"

	blockvalidation_api "github.com/TAAL-GmbH/ubsv/services/blockvalidation/blockvalidation_api"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Client struct {
	apiClient blockvalidation_api.BlockValidationAPIClient
}

func NewClient(ctx context.Context) *Client {
	blockValidationGrpcAddress, ok := gocore.Config().Get("blockvalidation_grpcAddress")
	if !ok {
		panic("no blockvalidation_grpcAddress setting found")
	}
	baConn, err := utils.GetGRPCClient(ctx, blockValidationGrpcAddress, &utils.ConnectionOptions{
		MaxRetries: 3,
	})
	if err != nil {
		panic(err)
	}

	return &Client{
		apiClient: blockvalidation_api.NewBlockValidationAPIClient(baConn),
	}
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
