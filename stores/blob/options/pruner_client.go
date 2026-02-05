package options

import (
	"context"

	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
)

// BlobDeletionScheduler defines the interface for scheduling blob deletions.
// This interface is satisfied by blockchain.ClientI.
// It allows blob stores to schedule deletions without directly depending on the blockchain service.
type BlobDeletionScheduler interface {
	// ScheduleBlobDeletion schedules a blob for deletion at the specified blockchain height.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blobKey: The key identifying the blob to delete
	//   - fileType: The type of the blob file (as string)
	//   - storeType: The blob store type
	//   - deleteAtHeight: The blockchain height at which to delete the blob
	// Returns:
	//   - deletionID: The ID of the scheduled deletion
	//   - scheduled: Whether the deletion was scheduled (false if already scheduled)
	//   - error: Any error that occurred during scheduling
	ScheduleBlobDeletion(ctx context.Context, blobKey []byte, fileType string, storeType storetypes.BlobStoreType, deleteAtHeight uint32) (int64, bool, error)

	// CancelBlobDeletion cancels a previously scheduled blob deletion.
	// Parameters:
	//   - ctx: Context for the operation
	//   - blobKey: The key identifying the blob
	//   - fileType: The type of the blob file (as string)
	//   - storeType: The blob store type
	// Returns:
	//   - cancelled: Whether the deletion was found and cancelled
	//   - error: Any error that occurred during cancellation
	CancelBlobDeletion(ctx context.Context, blobKey []byte, fileType string, storeType storetypes.BlobStoreType) (bool, error)
}
