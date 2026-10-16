package blockpersister

import (
	"context"
	"io"
	"net/url"
	"sync"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	blobfile "github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// This test runs over the REAL file store (stores/blob/file) in a temporary directory,
// decorated with one hook so a second writer publishes the key as this writer's SetFromReader
// starts: after the caller's own existence check and before the store's pre-write check.
// Nothing about storage is faked. This writer does not ask for the file store's exclusive
// publish, so the pre-write check is where the store refuses it.

// cancellingScheduler records which file types had their deletion cancelled, which is what a
// SetDAH to zero does on the file store.
type cancellingScheduler struct {
	mu        sync.Mutex
	cancelled []string
}

func (s *cancellingScheduler) ScheduleBlobDeletion(context.Context, []byte, string, storetypes.BlobStoreType, uint32) (int64, bool, error) {
	return 0, true, nil
}

func (s *cancellingScheduler) CancelBlobDeletion(_ context.Context, _ []byte, fileType string, _ storetypes.BlobStoreType) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cancelled = append(s.cancelled, fileType)

	return true, nil
}

type publishRaceStore struct {
	*blobfile.File
	compete func(key []byte, fileType fileformat.FileType)
}

// SetFromReader lets the competitor publish first, then writes. The store's pre-write check
// then finds the competitor's file and refuses this write, which is the refusal a caller of
// the file store gets without options.WithExclusivePublish.
func (s *publishRaceStore) SetFromReader(ctx context.Context, key []byte, fileType fileformat.FileType, reader io.ReadCloser, opts ...options.FileOption) error {
	s.compete(key, fileType)

	return s.File.SetFromReader(ctx, key, fileType, reader, opts...)
}

// TestCreateSubtreeDataFileStreaming_ALostPublishRaceIsJudgedLikeAFileFoundFirst: block
// validation publishes the subtreeData after the persister found no file and before the store's
// pre-write check, so the store refuses the persister's write and Close reports it. The
// persister treats the other writer's file as it treats one found before the write: read back,
// kept and made permanent (data and structure file both) when it holds the subtree's
// transactions, removed when it does not, with an error that makes the block retry rather
// than one carrying the already-exists code the persister's loop reads as "block done".
func TestCreateSubtreeDataFileStreaming_ALostPublishRaceIsJudgedLikeAFileFoundFirst(t *testing.T) {
	block, _, _, mockUTXOStore, seeded, blockStore, blockchainClient, tSettings := setup(t)
	ctx := context.Background()
	root := block.Subtrees[0]

	subtreeBytes, err := seeded.Get(ctx, root[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)

	subtreeDataBytes, err := seeded.Get(ctx, root[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)

	run := func(t *testing.T, theirs []byte) (error, *blobfile.File, *cancellingScheduler) {
		storeURL, err := url.Parse("file://" + t.TempDir())
		require.NoError(t, err)

		scheduler := &cancellingScheduler{}

		plain, err := blobfile.New(ulogger.TestLogger{}, storeURL, options.WithBlobDeletionScheduler(scheduler))
		require.NoError(t, err)

		// Only the structure file is there; the persister has to write the data file.
		require.NoError(t, plain.Set(ctx, root[:], fileformat.FileTypeSubtree, subtreeBytes))

		// The hook runs on the file storer's goroutine, so it records and the test asserts.
		var competeErr error

		store := &publishRaceStore{File: plain}
		store.compete = func(key []byte, fileType fileformat.FileType) {
			competeErr = plain.Set(ctx, key, fileType, theirs, options.WithDeleteAt(100))
		}

		persister := New(ctx, ulogger.TestLogger{}, tSettings, blockStore, store, mockUTXOStore, blockchainClient)

		err = persister.CreateSubtreeDataFileStreaming(ctx, *root, block, 1)
		require.NoError(t, competeErr, "the other writer's publish must succeed")

		return err, plain, scheduler
	}

	t.Run("a file holding the subtree's transactions is kept and made permanent", func(t *testing.T) {
		err, plain, scheduler := run(t, subtreeDataBytes)
		require.NoError(t, err, "losing the publish to a writer of the same transactions is not a failure")

		got, err := plain.Get(ctx, root[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, err)
		require.Equal(t, subtreeDataBytes, got, "the other writer's file stands")

		scheduler.mu.Lock()
		defer scheduler.mu.Unlock()

		require.ElementsMatch(t, []string{string(fileformat.FileTypeSubtreeData), string(fileformat.FileTypeSubtree)}, scheduler.cancelled,
			"both files are made permanent, as they are when the data file was already there")
	})

	t.Run("a file that does not hold them is removed and the block retried", func(t *testing.T) {
		err, plain, scheduler := run(t, []byte("not the subtree's transactions"))
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrBlobAlreadyExists), "the error must not read as block already persisted: %v", err)

		exists, err := plain.Exists(ctx, root[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, err)
		require.False(t, exists, "the file that failed the read-back is gone, so the retry writes its own")

		scheduler.mu.Lock()
		defer scheduler.mu.Unlock()

		require.Empty(t, scheduler.cancelled, "nothing is made permanent")
	})
}
