package options

import (
	"context"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// GRPCPrunerClient implements PrunerClient using gRPC to communicate with the blockchain service.
// Note: This has been refactored to talk to the blockchain service for blob deletion scheduling.
// The blockchainClient interface uses int32 for storeType to avoid import cycles with blockchain_api.
type GRPCPrunerClient struct {
	blockchainClient interface {
		ScheduleBlobDeletionInt32(ctx context.Context, blobKey []byte, fileType string, storeType int32, deleteAtHeight uint32) (int64, bool, error)
	}
	logger ulogger.Logger
}

// NewGRPCPrunerClient creates a new gRPC-based pruner client.
// The client parameter should be a blockchain.ClientI instance wrapped with an adapter.
func NewGRPCPrunerClient(client interface{}, logger ulogger.Logger) *GRPCPrunerClient {
	// Accept any client that has the required methods
	if bc, ok := client.(interface {
		ScheduleBlobDeletionInt32(ctx context.Context, blobKey []byte, fileType string, storeType int32, deleteAtHeight uint32) (int64, bool, error)
	}); ok {
		return &GRPCPrunerClient{
			blockchainClient: bc,
			logger:           logger,
		}
	}

	// Legacy support - return a disabled client if the interface doesn't match
	logger.Warnf("Client does not implement blob deletion methods - blob deletion scheduling disabled")
	return &GRPCPrunerClient{
		blockchainClient: nil,
		logger:           logger,
	}
}

// ScheduleBlobDeletion schedules a blob for deletion at the specified blockchain height.
func (c *GRPCPrunerClient) ScheduleBlobDeletion(ctx context.Context, blobKey []byte, fileType fileformat.FileType, storeType int32, deleteAtHeight uint32) error {
	if c.blockchainClient == nil {
		c.logger.Debugf("Blob deletion scheduling disabled - blockchain client not configured")
		return nil
	}

	_, scheduled, err := c.blockchainClient.ScheduleBlobDeletionInt32(ctx, blobKey, string(fileType), storeType, deleteAtHeight)
	if err != nil {
		c.logger.Debugf("Failed to schedule blob deletion with blockchain service: %v", err)
		return err
	}

	if !scheduled {
		c.logger.Debugf("Blockchain service rejected blob deletion schedule")
	}

	return nil
}

// CancelBlobDeletion is deprecated and no longer supported.
// Blob deletions are managed by the blockchain service and cannot be cancelled from blob stores.
func (c *GRPCPrunerClient) CancelBlobDeletion(ctx context.Context, blobKey []byte, fileType fileformat.FileType, storeType int32) error {
	// No-op: cancellation is not supported in new architecture
	return nil
}
