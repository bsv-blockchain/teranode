package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// deletableSubtreeTypes are the peer-supplied blob types removeCatchupSubtreeFiles will ever
// delete a (hash, fileType) pair for. FileTypeSubtreeMeta is deliberately excluded from this list
// (bitcoin-sv/teranode#4692): no catch-up producer ever proves it fresh, so it can never be deleted by
// this helper regardless of what freshlyWritten says.
var deletableSubtreeTypes = []fileformat.FileType{
	fileformat.FileTypeSubtree,
	fileformat.FileTypeSubtreeToCheck,
	fileformat.FileTypeSubtreeData,
}

// TestRemoveCatchupSubtreeFiles covers removeCatchupSubtreeFiles' cleanup rule (bitcoin-sv/teranode#4692):
// provenance is per-(hash, fileType), not per-hash — a peer can doctor a body to name an
// already-persisted, permanently-promoted hash, and cleanup must never delete a type it did not
// itself prove fresh for that hash. Each subtest below inverts the old wide, unconditional
// per-hash delete this helper used to perform.
func TestRemoveCatchupSubtreeFiles(t *testing.T) {
	ctx := context.Background()

	t.Run("a hash with every deletable type marked fresh: all three deleted", func(t *testing.T) {
		store := blobmemory.New()

		subtreeHash := chainhash.HashH([]byte("catchup-blob-cleanup-all-fresh"))
		for _, fileType := range deletableSubtreeTypes {
			require.NoError(t, store.Set(ctx, subtreeHash[:], fileType, []byte{0x01}))
		}

		freshlyWritten := map[chainhash.Hash]map[fileformat.FileType]struct{}{
			subtreeHash: {
				fileformat.FileTypeSubtree:        {},
				fileformat.FileTypeSubtreeToCheck: {},
				fileformat.FileTypeSubtreeData:    {},
			},
		}

		u := &Server{logger: ulogger.TestLogger{}, subtreeStore: store}
		require.NoError(t, u.removeCatchupSubtreeFiles(ctx, freshlyWritten))

		for _, fileType := range deletableSubtreeTypes {
			exists, err := store.Exists(ctx, subtreeHash[:], fileType)
			require.NoError(t, err)
			require.False(t, exists, "%s must be removed: this attempt itself freshly wrote it", fileType)
		}
	})

	t.Run("a hash absent from freshlyWritten entirely: nothing is deleted", func(t *testing.T) {
		store := blobmemory.New()

		// Simulates a doctored body naming an already-persisted, permanently-promoted hash this
		// attempt never fetched at all.
		subtreeHash := chainhash.HashH([]byte("catchup-blob-cleanup-not-fresh"))
		for _, fileType := range retryReadableSubtreeTypes {
			require.NoError(t, store.Set(ctx, subtreeHash[:], fileType, []byte{0x01}))
		}

		u := &Server{logger: ulogger.TestLogger{}, subtreeStore: store}
		require.NoError(t, u.removeCatchupSubtreeFiles(ctx, map[chainhash.Hash]map[fileformat.FileType]struct{}{}))

		for _, fileType := range retryReadableSubtreeTypes {
			exists, err := store.Exists(ctx, subtreeHash[:], fileType)
			require.NoError(t, err)
			require.True(t, exists, "%s must survive: this attempt never proved it fresh for this hash", fileType)
		}
	})

	t.Run("mixed provenance: only the freshly-written type is deleted, pre-existing siblings survive", func(t *testing.T) {
		store := blobmemory.New()

		subtreeHash := chainhash.HashH([]byte("catchup-blob-cleanup-mixed"))
		// Pre-existing, promoted-looking siblings this attempt never touched.
		require.NoError(t, store.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData, []byte{0x01}))
		require.NoError(t, store.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtree, []byte{0x01}))
		// The one type this attempt itself freshly wrote for the SAME hash.
		require.NoError(t, store.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck, []byte{0x01}))

		freshlyWritten := map[chainhash.Hash]map[fileformat.FileType]struct{}{
			subtreeHash: {fileformat.FileTypeSubtreeToCheck: {}},
		}

		u := &Server{logger: ulogger.TestLogger{}, subtreeStore: store}
		require.NoError(t, u.removeCatchupSubtreeFiles(ctx, freshlyWritten))

		exists, err := store.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
		require.NoError(t, err)
		require.False(t, exists, "the freshly-written type must be deleted")

		for _, fileType := range []fileformat.FileType{fileformat.FileTypeSubtreeData, fileformat.FileTypeSubtree} {
			exists, err := store.Exists(ctx, subtreeHash[:], fileType)
			require.NoError(t, err)
			require.True(t, exists, "%s pre-existed and was never freshly written — must survive (never regress to all-or-nothing per-hash deletion)", fileType)
		}
	})

	t.Run("mixed provenance, roles reversed: pre-existing SubtreeToCheck survives a freshly-written SubtreeData", func(t *testing.T) {
		store := blobmemory.New()

		subtreeHash := chainhash.HashH([]byte("catchup-blob-cleanup-mixed-reversed"))
		require.NoError(t, store.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck, []byte{0x01}))
		require.NoError(t, store.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData, []byte{0x01}))

		freshlyWritten := map[chainhash.Hash]map[fileformat.FileType]struct{}{
			subtreeHash: {fileformat.FileTypeSubtreeData: {}},
		}

		u := &Server{logger: ulogger.TestLogger{}, subtreeStore: store}
		require.NoError(t, u.removeCatchupSubtreeFiles(ctx, freshlyWritten))

		exists, err := store.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, err)
		require.False(t, exists, "the freshly-written type must be deleted")

		exists, err = store.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
		require.NoError(t, err)
		require.True(t, exists, "the pre-existing, non-fresh type must survive")
	})

	t.Run("FileTypeSubtreeMeta is never deleted, regardless of what else is fresh for that hash", func(t *testing.T) {
		store := blobmemory.New()

		subtreeHash := chainhash.HashH([]byte("catchup-blob-cleanup-meta-excluded"))
		require.NoError(t, store.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta, []byte{0x01}))
		require.NoError(t, store.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtree, []byte{0x01}))

		// No producer in this package ever marks FileTypeSubtreeMeta fresh, but assert the
		// guard holds explicitly even if it somehow appeared in the map.
		freshlyWritten := map[chainhash.Hash]map[fileformat.FileType]struct{}{
			subtreeHash: {
				fileformat.FileTypeSubtree:     {},
				fileformat.FileTypeSubtreeMeta: {},
			},
		}

		u := &Server{logger: ulogger.TestLogger{}, subtreeStore: store}
		require.NoError(t, u.removeCatchupSubtreeFiles(ctx, freshlyWritten))

		exists, err := store.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta)
		require.NoError(t, err)
		require.True(t, exists, "FileTypeSubtreeMeta must never be deleted by this helper")

		exists, err = store.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
		require.NoError(t, err)
		require.False(t, exists, "the freshly-written FileTypeSubtree must still be deleted")
	})

	t.Run("a missing file is not an error", func(t *testing.T) {
		store := blobmemory.New()

		subtreeHash := chainhash.HashH([]byte("catchup-blob-cleanup-partial"))
		freshlyWritten := map[chainhash.Hash]map[fileformat.FileType]struct{}{
			subtreeHash: {
				fileformat.FileTypeSubtree:        {},
				fileformat.FileTypeSubtreeToCheck: {},
				fileformat.FileTypeSubtreeData:    {},
			},
		}

		u := &Server{logger: ulogger.TestLogger{}, subtreeStore: store}
		require.NoError(t, u.removeCatchupSubtreeFiles(ctx, freshlyWritten), "a missing file must be tolerated, not returned as an error")
	})
}

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
