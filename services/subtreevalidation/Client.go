package subtreevalidation

import (
	"context"
	"net/http"

	"github.com/bitcoin-sv/ubsv/errors"
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

func NewClient(ctx context.Context, logger ulogger.Logger, source string) (Interface, error) {
	subtreeValidationGrpcAddress, ok := gocore.Config().Get("subtreevalidation_grpcAddress")
	if !ok {
		return nil, errors.NewConfigurationError("no subtreevalidation_grpcAddress setting found")
	}

	baConn, err := util.GetGRPCClient(ctx, subtreeValidationGrpcAddress, &util.ConnectionOptions{
		MaxRetries: 3,
	})
	if err != nil {
		return nil, errors.NewServiceError("failed to init subtree validation service connection for '%s'", source, err)
	}

	client := &Client{
		apiClient: subtreevalidation_api.NewSubtreeValidationAPIClient(baConn),
		logger:    logger,
	}

	return client, nil
}

func (s *Client) Health(ctx context.Context) (int, string, error) {
	res, err := s.apiClient.HealthGRPC(ctx, &subtreevalidation_api.EmptyMessage{})
	if !res.Ok || err != nil {
		return http.StatusFailedDependency, res.Details, errors.UnwrapGRPC(err)
	}

	return http.StatusOK, res.Details, nil
}

func (s *Client) CheckSubtree(ctx context.Context, subtreeHash chainhash.Hash, baseURL string, blockHeight uint32, blockHash *chainhash.Hash) error {
	req := &subtreevalidation_api.CheckSubtreeRequest{
		Hash:        subtreeHash[:],
		BaseUrl:     baseURL,
		BlockHeight: blockHeight,
		BlockHash:   blockHash[:],
	}

	_, err := s.apiClient.CheckSubtree(ctx, req)
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}
