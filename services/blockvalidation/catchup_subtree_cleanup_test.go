package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// retryReadableSubtreeTypes are the peer-supplied blob types the catchup retry path can read back:
// the promoted .subtree marker, the subtreeToCheck file the catchup fetch writes (and which
// findLocalSubtreeFile prefers and model.Block.GetAndValidateSubtrees falls back to), the
// subtreeData file, and the subtreeMeta written during full subtree validation.
var retryReadableSubtreeTypes = []fileformat.FileType{
	fileformat.FileTypeSubtree,
	fileformat.FileTypeSubtreeToCheck,
	fileformat.FileTypeSubtreeMeta,
	fileformat.FileTypeSubtreeData,
}

// TestRemoveCatchupSubtreeFiles covers bitcoin-sv/teranode#4692: the cleanup ran
// on a failed quick validation but deleted only the .subtree marker, leaving the subtreeToCheck and
// subtreeData blobs the catchup fetch wrote — both readable back by the retry path. Content that had
// just FAILED its integrity check could therefore be re-applied on the next attempt, which is exactly
// what the helper exists to prevent. Both callers (the corrupt-body abort and the plain
// quick-validation failure) funnel through this helper, so it is the single point that must delete
// every type.
func TestRemoveCatchupSubtreeFiles(t *testing.T) {
	ctx := context.Background()

	t.Run("deletes every type the retry path can read back", func(t *testing.T) {
		store := blobmemory.New()

		subtreeHash := chainhash.HashH([]byte("catchup-blob-cleanup"))
		block := &model.Block{Subtrees: []*chainhash.Hash{&subtreeHash}}

		for _, fileType := range retryReadableSubtreeTypes {
			require.NoError(t, store.Set(ctx, subtreeHash[:], fileType, []byte{0x01}))
		}

		u := &Server{logger: ulogger.TestLogger{}, subtreeStore: store}
		require.NoError(t, u.removeCatchupSubtreeFiles(ctx, block))

		for _, fileType := range retryReadableSubtreeTypes {
			exists, err := store.Exists(ctx, subtreeHash[:], fileType)
			require.NoError(t, err)
			require.False(t, exists, "%s must be removed so failed content cannot be re-applied on retry", fileType)
		}
	})

	t.Run("a missing file is not an error", func(t *testing.T) {
		store := blobmemory.New()

		subtreeHash := chainhash.HashH([]byte("catchup-blob-cleanup-partial"))
		block := &model.Block{Subtrees: []*chainhash.Hash{&subtreeHash}}

		// Only the promoted marker was ever written; the other three types must be tolerated.
		require.NoError(t, store.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtree, []byte{0x01}))

		u := &Server{logger: ulogger.TestLogger{}, subtreeStore: store}
		require.NoError(t, u.removeCatchupSubtreeFiles(ctx, block))

		exists, err := store.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
		require.NoError(t, err)
		require.False(t, exists)
	})
}
