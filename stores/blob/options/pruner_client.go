package options

import (
	"context"

	"github.com/bsv-blockchain/teranode/pkg/fileformat"
)

// PrunerClient defines the interface for scheduling blob deletions with the pruner service.
// This interface allows blob stores to register blobs for deletion at a specific blockchain height
// without directly depending on the pruner service implementation.
type PrunerClient interface {
	// ScheduleBlobDeletion schedules a blob for deletion at the specified blockchain height.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blobKey: The key identifying the blob to delete
	//   - fileType: The type of the blob file
	//   - storeType: The blob store type enum value (0=TXSTORE, 1=SUBTREESTORE, 2=BLOCKSTORE, 3=TEMPSTORE, 4=BLOCKPERSISTERSTORE)
	//   - deleteAtHeight: The blockchain height at which to delete the blob
	// Returns:
	//   - error: Any error that occurred during scheduling
	ScheduleBlobDeletion(ctx context.Context, blobKey []byte, fileType fileformat.FileType, storeType int32, deleteAtHeight uint32) error

	// CancelBlobDeletion cancels a previously scheduled blob deletion.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blobKey: The key identifying the blob
	//   - fileType: The type of the blob file
	//   - storeType: The blob store type enum value
	// Returns:
	//   - error: Any error that occurred during cancellation
	CancelBlobDeletion(ctx context.Context, blobKey []byte, fileType fileformat.FileType, storeType int32) error
}

// NullPrunerClient is a no-op implementation of PrunerClient.
// Used when pruner service is not available or DAH functionality is disabled.
type NullPrunerClient struct{}

// ScheduleBlobDeletion does nothing and returns nil.
func (n *NullPrunerClient) ScheduleBlobDeletion(ctx context.Context, blobKey []byte, fileType fileformat.FileType, storeType int32, deleteAtHeight uint32) error {
	return nil
}

// CancelBlobDeletion does nothing and returns nil.
func (n *NullPrunerClient) CancelBlobDeletion(ctx context.Context, blobKey []byte, fileType fileformat.FileType, storeType int32) error {
	return nil
}
