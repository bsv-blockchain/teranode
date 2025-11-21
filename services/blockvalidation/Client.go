// Package blockvalidation implements block validation for Bitcoin SV nodes in Teranode.
//
// This package provides the core functionality for validating Bitcoin blocks, managing block subtrees,
// and processing transaction metadata. It is designed for high-performance operation at scale,
// supporting features like:
//
// - Concurrent block validation with optimistic mining support
// - Subtree-based block organization and validation
// - Transaction metadata caching and management
// - Automatic chain catchup when falling behind
// - Integration with Kafka for distributed operation
//
// The package exposes gRPC interfaces for block validation operations,
// making it suitable for use in distributed Teranode deployments.
package blockvalidation

import (
	"context"
	"net/http"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
)

// Client provides an interface to interact with the block validation service.
// It manages both gRPC and HTTP connections, providing fallback mechanisms
// and efficient communication channels for block validation operations.
type Client struct {
	// apiClient handles gRPC communication with the validation service
	apiClient blockvalidation_api.BlockValidationAPIClient

	// logger provides structured logging capabilities
	logger ulogger.Logger

	// settings contains service configuration parameters
	settings *settings.Settings
}

// NewClient creates a new block validation client with the specified configuration.
// It establishes connections to both gRPC and HTTP endpoints if configured, providing
// redundant communication paths for improved reliability.
//
// Parameters:
//   - ctx: Context for connection establishment and lifecycle management
//   - logger: Structured logging interface for operation monitoring
//   - tSettings: Service configuration parameters including endpoint addresses
//   - source: Identifier for the client instance, used in logging and metrics
//
// Returns:
//   - A configured Client instance and nil error on success
//   - nil and error if configuration is invalid or connection fails
func NewClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, source string) (*Client, error) {
	blockValidationGrpcAddress := tSettings.BlockValidation.GRPCAddress
	if blockValidationGrpcAddress == "" {
		return nil, errors.NewConfigurationError("no blockvalidation_grpcAddress setting found")
	}

	baConn, err := util.GetGRPCClient(ctx, blockValidationGrpcAddress, &util.ConnectionOptions{
		MaxRetries:   tSettings.GRPCMaxRetries,
		RetryBackoff: tSettings.GRPCRetryBackoff,
	}, tSettings)
	if err != nil {
		return nil, errors.NewServiceError("failed to init block validation service connection for '%s'", source, err)
	}

	client := &Client{
		apiClient: blockvalidation_api.NewBlockValidationAPIClient(baConn),
		logger:    logger,
		settings:  tSettings,
	}

	return client, nil
}

// Health performs a comprehensive health check of the block validation service.
// It verifies both basic service availability and, when requested, deeper dependency checks.
//
// The method supports two check modes:
//   - Liveness: Basic service responsiveness check
//   - Readiness: Full dependency and operational capability check
//
// Parameters:
//   - ctx: Context for the health check operation
//   - checkLiveness: When true, performs only basic health verification
//
// Returns:
//   - int: HTTP status code indicating health state
//   - string: Detailed health status message
//   - error: Any error encountered during health checking
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
	resp, err := s.apiClient.HealthGRPC(ctx, &blockvalidation_api.EmptyMessage{})
	if err != nil || !resp.GetOk() {
		return http.StatusFailedDependency, resp.GetDetails(), errors.UnwrapGRPC(err)
	}

	return http.StatusOK, resp.GetDetails(), nil
}

// BlockFound notifies the validation service about a newly discovered block.
// It initializes the validation process and optionally waits for completion.
//
// Parameters:
//   - ctx: Context for the notification operation
//   - blockHash: Unique identifier of the discovered block
//   - baseURL: Source URL from which the block can be retrieved
//   - waitToComplete: When true, waits for validation to complete before returning
//
// Returns an error if notification fails or validation encounters issues
func (s *Client) BlockFound(ctx context.Context, blockHash *chainhash.Hash, baseURL string, waitToComplete bool) error {
	req := &blockvalidation_api.BlockFoundRequest{
		Hash:           blockHash.CloneBytes(),
		BaseUrl:        baseURL,
		WaitToComplete: waitToComplete,
	}

	_, err := s.apiClient.BlockFound(ctx, req)

	unwrappedErr := errors.UnwrapGRPC(err)
	if unwrappedErr == nil {
		return nil
	}

	return unwrappedErr
}

// ProcessBlock submits a block for validation at a specified height.
// This method is typically used during initial block synchronization or
// when receiving blocks through legacy interfaces.
//
// Parameters:
//   - ctx: Context for the processing operation
//   - block: Complete block data to validate
//   - blockHeight: Expected chain height for the block
//
// Returns an error if block processing fails
func (s *Client) ProcessBlock(ctx context.Context, block *model.Block, blockHeight uint32, peerID, baseURL string) error {
	blockBytes, err := block.Bytes()
	if err != nil {
		return err
	}

	req := &blockvalidation_api.ProcessBlockRequest{
		Block:   blockBytes,
		Height:  blockHeight,
		PeerId:  peerID,
		BaseUrl: baseURL,
	}

	_, err = s.apiClient.ProcessBlock(ctx, req)
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// ValidateBlock performs comprehensive validation of a block using the validation service.
// It submits the complete block data for validation including transaction verification,
// consensus rule checking, and integration with the current blockchain state.
//
// Parameters:
//   - ctx: Context for the validation operation
//   - block: Complete block data to validate including header and transactions
//   - options: Optional validation options to control behavior (e.g., revalidation)
//
// Returns an error if block validation fails or service communication errors occur
func (s *Client) ValidateBlock(ctx context.Context, block *model.Block, options *ValidateBlockOptions) error {
	blockBytes, err := block.Bytes()
	if err != nil {
		return err
	}

	req := &blockvalidation_api.ValidateBlockRequest{
		Block:  blockBytes,
		Height: block.Height,
	}

	// Add revalidation flag if options are provided
	if options != nil && options.IsRevalidation {
		req.IsRevalidation = true
	}

	_, err = s.apiClient.ValidateBlock(ctx, req)
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// RevalidateBlock forces revalidation of a previously invalidated block.
// This is useful for scenarios where the block's validity may have changed
// due to updates in consensus rules or blockchain state.
//
// Parameters:
//   - ctx: Context for the revalidation operation
//   - blockHash: Unique identifier of the block to revalidate
//
// Returns an error if revalidation fails or service communication errors occur
func (s *Client) RevalidateBlock(ctx context.Context, blockHash chainhash.Hash) error {
	req := &blockvalidation_api.RevalidateBlockRequest{
		Hash: blockHash.CloneBytes(),
	}

	_, err := s.apiClient.RevalidateBlock(ctx, req)
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// GetCatchupStatus retrieves the current status of blockchain catchup operations.
// It queries the block validation service for information about ongoing or recent
// catchup attempts, including progress metrics and peer information.
//
// Parameters:
//   - ctx: Context for the operation, allowing for cancellation and timeouts
//
// Returns:
//   - *CatchupStatus: Current catchup status information
//   - error: Any error encountered during the status retrieval
func (s *Client) GetCatchupStatus(ctx context.Context) (*CatchupStatus, error) {
	resp, err := s.apiClient.GetCatchupStatus(ctx, &blockvalidation_api.EmptyMessage{})
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	status := &CatchupStatus{
		IsCatchingUp:         resp.IsCatchingUp,
		PeerID:               resp.PeerId,
		PeerURL:              resp.PeerUrl,
		TargetBlockHash:      resp.TargetBlockHash,
		TargetBlockHeight:    resp.TargetBlockHeight,
		CurrentHeight:        resp.CurrentHeight,
		TotalBlocks:          int(resp.TotalBlocks),
		BlocksFetched:        resp.BlocksFetched,
		BlocksValidated:      resp.BlocksValidated,
		StartTime:            resp.StartTime,
		DurationMs:           resp.DurationMs,
		ForkDepth:            resp.ForkDepth,
		CommonAncestorHash:   resp.CommonAncestorHash,
		CommonAncestorHeight: resp.CommonAncestorHeight,
	}

	if resp.PreviousAttempt != nil {
		status.PreviousAttempt = &PreviousAttempt{
			PeerID:            resp.PreviousAttempt.PeerId,
			PeerURL:           resp.PreviousAttempt.PeerUrl,
			TargetBlockHash:   resp.PreviousAttempt.TargetBlockHash,
			TargetBlockHeight: resp.PreviousAttempt.TargetBlockHeight,
			ErrorMessage:      resp.PreviousAttempt.ErrorMessage,
			ErrorType:         resp.PreviousAttempt.ErrorType,
			AttemptTime:       resp.PreviousAttempt.AttemptTime,
			DurationMs:        resp.PreviousAttempt.DurationMs,
			BlocksValidated:   resp.PreviousAttempt.BlocksValidated,
		}
	}

	return status, nil
}
