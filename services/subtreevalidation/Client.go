// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"context"
	"net/http"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
)

// Client provides a gRPC client interface for the subtree validation service.
//
// The Client struct encapsulates the connection and communication logic needed to
// interact with a remote subtree validation service via gRPC. It handles the
// low-level details of service communication while providing a clean, high-level
// interface for validation operations.
//
// The client is designed to be used by other services within the Teranode ecosystem
// that need to validate transaction subtrees, such as the block validation service,
// propagation service, or other components that process blockchain transactions.
//
// Key Features:
//   - gRPC-based communication with automatic connection management
//   - Retry logic with configurable backoff for resilient operation
//   - Comprehensive error handling and logging
//   - Integration with Teranode's settings and configuration system
//
// Thread Safety:
// The Client struct is safe for concurrent use across multiple goroutines.
// The underlying gRPC connection handles concurrent requests efficiently.
type Client struct {
	apiClient subtreevalidation_api.SubtreeValidationAPIClient
	// logger provides structured logging capabilities for client operations
	logger ulogger.Logger
	// settings contains the configuration parameters for the client and service connections
	settings *settings.Settings
}

// NewClient creates a new subtree validation service client with gRPC connectivity.
//
// This constructor function establishes a connection to the subtree validation service
// using the provided configuration settings and returns a client interface that can
// be used to perform validation operations. The function handles all the complexity
// of setting up the gRPC connection, including retry logic and connection options.
//
// The client is configured with retry mechanisms to handle transient network issues
// and service unavailability. Connection parameters are derived from the block
// validation settings, specifically the retry count and backoff duration settings.
//
// Parameters:
//   - ctx: Context for connection establishment and cancellation
//   - logger: Logger instance for structured logging of client operations
//   - tSettings: Teranode settings containing service addresses and connection parameters
//   - source: Identifier string for the calling service (used in error messages)
//
// Returns:
//   - Interface: A client interface for subtree validation operations
//   - error: Configuration error if gRPC address is missing, or service error if connection fails
//
// Configuration Requirements:
// The function requires the following settings to be configured:
//   - SubtreeValidation.GRPCAddress: The address of the subtree validation service
//   - BlockValidation.CheckSubtreeFromBlockRetries: Maximum number of retry attempts
//   - BlockValidation.CheckSubtreeFromBlockRetryBackoffDuration: Delay between retries
//
// Example Usage:
//
//	client, err := NewClient(ctx, logger, settings, "block-validation")
//	if err != nil {
//	    return fmt.Errorf("failed to create subtree validation client: %w", err)
//	}
func NewClient(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, source string) (Interface, error) {
	subtreeValidationGrpcAddress := tSettings.SubtreeValidation.GRPCAddress
	if subtreeValidationGrpcAddress == "" {
		return nil, errors.NewConfigurationError("no subtreevalidation_grpcAddress setting found")
	}

	baConn, err := util.GetGRPCClient(ctx, subtreeValidationGrpcAddress, &util.ConnectionOptions{
		MaxRetries:   tSettings.BlockValidation.CheckSubtreeFromBlockRetries,
		RetryBackoff: tSettings.BlockValidation.CheckSubtreeFromBlockRetryBackoffDuration,
	}, tSettings)
	if err != nil {
		return nil, errors.NewServiceError("failed to init subtree validation service connection for '%s'", source, err)
	}

	client := &Client{
		apiClient: subtreevalidation_api.NewSubtreeValidationAPIClient(baConn),
		logger:    logger,
		settings:  tSettings,
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

func (s *Client) CheckSubtreeFromBlock(ctx context.Context, subtreeHash chainhash.Hash, baseURL string, blockHeight uint32, blockHash, previousBlockHash *chainhash.Hash) error {
	req := &subtreevalidation_api.CheckSubtreeFromBlockRequest{
		Hash:              subtreeHash[:],
		BaseUrl:           baseURL,
		BlockHeight:       blockHeight,
		BlockHash:         blockHash[:],
		PreviousBlockHash: previousBlockHash[:],
	}

	_, err := s.apiClient.CheckSubtreeFromBlock(ctx, req)
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

func (s *Client) CheckBlockSubtrees(ctx context.Context, block *model.Block, peerID, baseURL string) error {
	blockBytes, err := block.Bytes()
	if err != nil {
		return errors.NewProcessingError("failed to serialize block for subtree validation", err)
	}

	if _, err = s.apiClient.CheckBlockSubtrees(ctx, &subtreevalidation_api.CheckBlockSubtreesRequest{
		Block:   blockBytes,
		BaseUrl: baseURL,
		PeerId:  peerID,
	}); err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}
