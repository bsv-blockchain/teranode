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
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockvalidation/blockvalidation_api"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
)

// Client provides an interface to interact with the block validation service.
// It manages both gRPC and HTTP connections, providing fallback mechanisms
// and efficient communication channels for block validation operations.
type Client struct {
	// apiClient handles gRPC communication with the validation service
	apiClient blockvalidation_api.BlockValidationAPIClient

	// httpAddress stores the HTTP endpoint for the validation service
	// This provides an alternative communication channel when available
	httpAddress string

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
		MaxRetries: 3,
	}, tSettings)
	if err != nil {
		return nil, errors.NewServiceError("failed to init block validation service connection for '%s'", source, err)
	}

	client := &Client{
		apiClient: blockvalidation_api.NewBlockValidationAPIClient(baConn),
		logger:    logger,
		settings:  tSettings,
	}

	httpAddress := tSettings.BlockValidation.HTTPAddress
	if httpAddress != "" {
		client.httpAddress = httpAddress
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
	if !resp.Ok || err != nil {
		return http.StatusFailedDependency, resp.Details, errors.UnwrapGRPC(err)
	}

	return http.StatusOK, resp.Details, nil
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
func (s *Client) ProcessBlock(ctx context.Context, block *model.Block, blockHeight uint32) error {
	blockBytes, err := block.Bytes()
	if err != nil {
		return err
	}

	req := &blockvalidation_api.ProcessBlockRequest{
		Block:  blockBytes,
		Height: blockHeight,
	}

	_, err = s.apiClient.ProcessBlock(ctx, req)
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// SubtreeFound notifies the validation service about a newly discovered subtree.
// This enables proper tracking and validation of block subtree structures.
//
// Parameters:
//   - ctx: Context for the notification operation
//   - subtreeHash: Unique identifier of the discovered subtree
//   - baseURL: Source URL from which the subtree can be retrieved
//
// Returns an error if notification fails
func (s *Client) SubtreeFound(ctx context.Context, subtreeHash *chainhash.Hash, baseURL string) error {
	req := &blockvalidation_api.SubtreeFoundRequest{
		Hash:    subtreeHash.CloneBytes(),
		BaseUrl: baseURL,
	}

	_, err := s.apiClient.SubtreeFound(ctx, req)
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// Get retrieves subtree data using a flexible multi-channel approach.
// It attempts HTTP retrieval first if configured, falling back to gRPC if necessary.
//
// Parameters:
//   - ctx: Context for the retrieval operation
//   - subtreeHash: Identifier of the subtree to retrieve
//
// Returns:
//   - The subtree data as bytes
//   - An error if retrieval fails through all available channels
func (s *Client) Get(ctx context.Context, subtreeHash []byte) ([]byte, error) {
	if s.httpAddress != "" {
		// try the http endpoint first, if that fails we can try the grpc endpoint
		subtreeBytes, err := util.DoHTTPRequest(ctx, s.httpAddress+"/subtree/"+utils.ReverseAndHexEncodeSlice(subtreeHash))
		if err != nil {
			s.logger.Warnf("error getting subtree %x from blockvalidation http endpoint: %s", subtreeHash, err)
		} else if subtreeBytes != nil {
			return subtreeBytes, nil
		}
	}

	req := &blockvalidation_api.GetSubtreeRequest{
		Hash: subtreeHash,
	}

	response, err := s.apiClient.Get(ctx, req)
	if err != nil {
		return nil, errors.UnwrapGRPC(err)
	}

	return response.Subtree, nil
}

// Exists verifies whether a specific subtree exists in the validation system.
// This provides efficient existence checking without requiring full data retrieval.
//
// Parameters:
//   - ctx: Context for the verification operation
//   - subtreeHash: Identifier of the subtree to check
//
// Returns:
//   - bool indicating existence status
//   - An error if verification fails
func (s *Client) Exists(ctx context.Context, subtreeHash []byte) (bool, error) {
	req := &blockvalidation_api.ExistsSubtreeRequest{
		Hash: subtreeHash,
	}

	response, err := s.apiClient.Exists(ctx, req)
	if err != nil {
		return false, errors.UnwrapGRPC(err)
	}

	return response.Exists, nil
}

// Set is a placeholder method required by an interface.
// This operation is not implemented in the client and returns an error if called.
func (s *Client) Set(_ context.Context, _ []byte, _ []byte, _ ...options.Options) error {
	return errors.NewError("not implemented")
}

// SetTTL is a placeholder method required by an interface.
// This operation is not implemented in the client and returns an error if called.
func (s *Client) SetTTL(_ context.Context, _ []byte, _ time.Duration) error {
	return errors.NewError("not implemented")
}

// SetTxMeta updates transaction metadata in the validation system.
// It handles efficient transmission of metadata while preserving original data.
//
// The method includes safety mechanisms to handle service unavailability
// and ensures proper data cleanup after transmission.
//
// Parameters:
//   - ctx: Context for the update operation
//   - txMetaData: Array of transaction metadata entries to update
//
// Returns an error if the update fails
func (s *Client) SetTxMeta(ctx context.Context, txMetaData []*meta.Data) error {
	func() {
		// can throw a segmentation violation when the blockvalidation service is not available :-(
		err := recover()
		if err != nil {
			s.logger.Errorf("Recovered from panic: %v", err)
		}
	}()

	txMetaDataSlice := make([][]byte, 0, len(txMetaData))

	for _, data := range txMetaData {
		hash := data.Tx.TxIDChainHash()

		b := hash.CloneBytes()

		temp := data.Tx
		data.Tx = nil // clear the tx from the data so we don't send it over the wire
		b = append(b, data.MetaBytes()...)
		data.Tx = temp // restore the tx, incase we need to try again

		txMetaDataSlice = append(txMetaDataSlice, b)
	}

	_, err := s.apiClient.SetTxMeta(ctx, &blockvalidation_api.SetTxMetaRequest{
		Data: txMetaDataSlice,
	})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// DelTxMeta removes transaction metadata from the validation system.
// This is typically used during chain reorganization or cleanup operations.
//
// Parameters:
//   - ctx: Context for the deletion operation
//   - hash: Identifier of the transaction metadata to remove
//
// Returns an error if deletion fails
func (s *Client) DelTxMeta(ctx context.Context, hash *chainhash.Hash) error {
	_, err := s.apiClient.DelTxMeta(ctx, &blockvalidation_api.DelTxMetaRequest{
		Hash: hash.CloneBytes(),
	})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}

// SetMinedMulti marks multiple transactions as mined in a specific block.
// This provides efficient batch updates for transaction mining status.
//
// Parameters:
//   - ctx: Context for the update operation
//   - hashes: Array of transaction hashes to mark as mined
//   - blockID: Identifier of the block containing these transactions
//
// Returns an error if the batch update fails
func (s *Client) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) (err error) {
	req := &blockvalidation_api.SetMinedMultiRequest{
		Hashes:  make([][]byte, 0, len(hashes)),
		BlockId: blockID,
	}

	for _, hash := range hashes {
		req.Hashes = append(req.Hashes, hash.CloneBytes())
	}

	_, err = s.apiClient.SetMinedMulti(ctx, req)
	if err != nil {
		return errors.UnwrapGRPC(err)
	}

	return nil
}
