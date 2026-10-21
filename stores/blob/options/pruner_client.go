package options

import (
	"context"

	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
)

// BlobDeletionScheduler defines the interface for scheduling blob deletions.
// This interface is satisfied by blockchain.ClientI.
// It allows blob stores to schedule deletions without directly depending on the blockchain service.
//
// Where the promise actually lives, despite the name of THIS file: the
// deletion queue is owned and executed by the BLOCKCHAIN service
// (blockchain.ClientI - see ScheduleBlobDeletion there); the pruner does
// not implement this interface and does not run this queue. The file is
// named pruner_client.go for historical reasons and renaming it would churn
// imports for a comment-sized problem (reconciliation notes section 52,
// open question 30) - so read "pruner" in the filename as "blockchain".
//
// The asymmetry this interface exists to make visible: an unwired scheduler
// makes the write path fail rather than silently dropping the deletion
// promise, which is the right direction - a refused write is a report, a
// dropped promise is a slow leak. If you are tracing "who deletes blob
// files at height H", the answer is Blockchain's queue, not this package
// and not the pruner service.
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
