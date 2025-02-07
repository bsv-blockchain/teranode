// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"context"
	"net/http"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

type Client struct {
	apiClient subtreevalidation_api.SubtreeValidationAPIClient
	logger    ulogger.Logger
}

func NewClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, source string) (Interface, error) {
	subtreeValidationGrpcAddress := tSettings.SubtreeValidation.GRPCAddress
	if subtreeValidationGrpcAddress == "" {
		return nil, errors.NewConfigurationError("no subtreevalidation_grpcAddress setting found")
	}

	baConn, err := util.GetGRPCClient(ctx, subtreeValidationGrpcAddress, &util.ConnectionOptions{
		MaxRetries: 3,
	}, tSettings)
	if err != nil {
		return nil, errors.NewServiceError("failed to init subtree validation service connection for '%s'", source, err)
	}

	client := &Client{
		apiClient: subtreevalidation_api.NewSubtreeValidationAPIClient(baConn),
		logger:    logger,
	}

	return client, nil
}

func (s *Client) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	res, err := s.apiClient.HealthGRPC(ctx, &subtreevalidation_api.EmptyMessage{})
	if !res.GetOk() || err != nil {
		return http.StatusFailedDependency, res.GetDetails(), errors.UnwrapGRPC(err)
	}

	return http.StatusOK, res.GetDetails(), nil
}

func (s *Client) CheckSubtreeFromBlock(ctx context.Context, subtreeHash chainhash.Hash, baseURL string, blockHeight uint32, blockHash *chainhash.Hash) error {
	req := &subtreevalidation_api.CheckSubtreeFromBlockRequest{
		Hash:        subtreeHash[:],
		BaseUrl:     baseURL,
		BlockHeight: blockHeight,
		BlockHash:   blockHash[:],
	}

	_, err := s.apiClient.CheckSubtreeFromBlock(ctx, req)
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}
