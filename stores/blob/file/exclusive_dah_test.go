package file_test

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	blockchainsql "github.com/bsv-blockchain/teranode/stores/blockchain/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// sqlDeletionScheduler schedules blob deletions in a real blockchain store, whose upsert keeps
// the last delete-at-height written for a blob, as the blockchain service does.
type sqlDeletionScheduler struct {
	store *blockchainsql.SQL
}

func (s sqlDeletionScheduler) ScheduleBlobDeletion(ctx context.Context, key []byte, fileType string, storeType storetypes.BlobStoreType, dah uint32) (int64, bool, error) {
	id, err := s.store.ScheduleBlobDeletion(ctx, &blockchainsql.ScheduleRequest{BlobKey: key, FileType: fileType, StoreType: int32(storeType), DeleteAtHeight: dah})

	return id, err == nil, err
}

func (s sqlDeletionScheduler) CancelBlobDeletion(ctx context.Context, key []byte, fileType string, storeType storetypes.BlobStoreType) (bool, error) {
	return true, s.store.CancelBlobDeletion(ctx, key, fileType, int32(storeType))
}

func exclusiveDAHStores(t *testing.T) (*file.File, *blockchainsql.SQL) {
	t.Helper()

	sqlURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	bc, err := blockchainsql.New(ulogger.TestLogger{}, sqlURL, test.CreateBaseTestSettings(t))
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, bc.Close(context.Background())) })

	blobURL, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	f, err := file.New(ulogger.TestLogger{}, blobURL, options.WithBlobDeletionScheduler(sqlDeletionScheduler{store: bc}))
	require.NoError(t, err)

	return f, bc
}

func scheduledFor(t *testing.T, bc *blockchainsql.SQL, key []byte) []uint32 {
	t.Helper()

	all, _, err := bc.ListScheduledBlobDeletions(context.Background(), &blockchainsql.ListFilters{Limit: 100})
	require.NoError(t, err)

	var heights []uint32

	for _, d := range all {
		if bytes.Equal(d.BlobKey, key) {
			heights = append(heights, d.DeleteAtHeight)
		}
	}

	return heights
}

// The legacy subtree writer publishes each subtree file exclusively, and a second copy of a
// block deliberately loses that publish. The loser used to schedule its delete-at-height before
// the publish, and the scheduler's upsert is last-writer-wins, so a refused copy put a deletion
// back on a file the block persister had kept with SetDAH(0) (review of 2026-10-12). Only the
// writer whose publish succeeds now schedules one.
func TestAWriterThatLosesAnExclusivePublishSchedulesNoDeletion(t *testing.T) {
	ctx := context.Background()

	commit := func(f *file.File, key []byte, payload string, dah uint32) error {
		p, err := f.NewPendingFile(ctx, fileformat.FileTypeTesting)
		require.NoError(t, err)

		_, err = p.Write([]byte(payload))
		require.NoError(t, err)

		return p.Commit(ctx, key, options.WithDeleteAt(dah), options.WithExclusivePublish())
	}

	setFromReader := func(f *file.File, key []byte, payload string, dah uint32) error {
		return f.SetFromReader(ctx, key, fileformat.FileTypeTesting, io.NopCloser(bytes.NewReader([]byte(payload))), options.WithDeleteAt(dah), options.WithExclusivePublish())
	}

	for name, write := range map[string]func(*file.File, []byte, string, uint32) error{
		"pending file commit": commit,
		"set from reader":     setFromReader,
	} {
		t.Run(name, func(t *testing.T) {
			f, bc := exclusiveDAHStores(t)
			key := []byte("subtree-root-0000000000000000001")

			require.NoError(t, write(f, key, "first", 100))
			require.Equal(t, []uint32{100}, scheduledFor(t, bc, key), "the winner's deletion is scheduled")

			// The block persister keeps the file.
			require.NoError(t, f.SetDAH(ctx, key, fileformat.FileTypeTesting, 0))
			require.Empty(t, scheduledFor(t, bc, key))

			err := write(f, key, "second", 200)
			require.True(t, errors.Is(err, errors.ErrBlobAlreadyExists), "got %v", err)

			require.Empty(t, scheduledFor(t, bc, key), "the refused copy must not schedule a deletion over the kept file")

			got, err := f.Get(ctx, key, fileformat.FileTypeTesting)
			require.NoError(t, err)
			require.Equal(t, []byte("first"), got)
		})
	}
}

// A plain write, without an exclusive publish, still schedules its delete-at-height as before.
func TestAPlainWriteStillSchedulesItsDeletion(t *testing.T) {
	ctx := context.Background()
	f, bc := exclusiveDAHStores(t)
	key := []byte("plain-key-00000000000000000000001")

	require.NoError(t, f.Set(ctx, key, fileformat.FileTypeTesting, []byte("x"), options.WithDeleteAt(300)))
	require.Equal(t, []uint32{300}, scheduledFor(t, bc, key))
}
