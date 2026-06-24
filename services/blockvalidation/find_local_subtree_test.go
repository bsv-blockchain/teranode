package blockvalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFindLocalSubtreeFile verifies that findLocalSubtreeFile locates a subtree
// under either FileTypeSubtreeToCheck (the download-from-peer marker) or
// FileTypeSubtree (the already-validated marker). The FileTypeSubtree case is
// the important regression guard for retry / legacy-catchup scenarios where
// the to-check file may have been cleaned up but the validated subtree is
// still on disk.
func TestFindLocalSubtreeFile(t *testing.T) {
	ctx := context.Background()

	var hash chainhash.Hash
	copy(hash[:], []byte("find_local_subtree_hash_32_bytes"))

	t.Run("FileTypeSubtreeToCheck present", func(t *testing.T) {
		store := memory.New()
		require.NoError(t, store.Set(ctx, hash[:], fileformat.FileTypeSubtreeToCheck, []byte("payload")))

		ft, exists, err := findLocalSubtreeFile(ctx, store, hash)
		require.NoError(t, err)
		require.True(t, exists)
		assert.Equal(t, fileformat.FileTypeSubtreeToCheck, ft)
	})

	t.Run("FileTypeSubtree only (retry/legacy)", func(t *testing.T) {
		store := memory.New()
		// Only the "already validated" marker exists — no FileTypeSubtreeToCheck.
		require.NoError(t, store.Set(ctx, hash[:], fileformat.FileTypeSubtree, []byte("payload")))

		ft, exists, err := findLocalSubtreeFile(ctx, store, hash)
		require.NoError(t, err)
		require.True(t, exists, "must find the subtree under FileTypeSubtree so the caller does not fall back to a peer fetch")
		assert.Equal(t, fileformat.FileTypeSubtree, ft)
	})

	t.Run("both present prefers FileTypeSubtreeToCheck", func(t *testing.T) {
		store := memory.New()
		require.NoError(t, store.Set(ctx, hash[:], fileformat.FileTypeSubtreeToCheck, []byte("payload")))
		require.NoError(t, store.Set(ctx, hash[:], fileformat.FileTypeSubtree, []byte("payload")))

		ft, exists, err := findLocalSubtreeFile(ctx, store, hash)
		require.NoError(t, err)
		require.True(t, exists)
		assert.Equal(t, fileformat.FileTypeSubtreeToCheck, ft, "FileTypeSubtreeToCheck is checked first")
	})

	t.Run("neither present", func(t *testing.T) {
		store := memory.New()

		ft, exists, err := findLocalSubtreeFile(ctx, store, hash)
		require.NoError(t, err)
		assert.False(t, exists)
		assert.Equal(t, fileformat.FileTypeUnknown, ft)
	})
}
