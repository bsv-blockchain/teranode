package file

import (
	"context"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// A pending file is written before its key is known and renamed into place when it is. The
// legacy block converter streams each subtree's data file this way: the file is named by the
// subtree's root hash, which is only known once its last transaction has arrived, and holding
// the transactions until then kept 3.65 GB of parsed scripts live on mainnet on 2026-09-24.

func pendingStore(t *testing.T) (*File, string) {
	t.Helper()

	dir := t.TempDir()
	u, err := url.Parse("file://" + dir)
	require.NoError(t, err)

	f, err := New(ulogger.TestLogger{}, u)
	require.NoError(t, err)

	return f, dir
}

func tempFilesIn(t *testing.T, dir string) []string {
	t.Helper()

	var found []string

	require.NoError(t, filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".tmp") {
			found = append(found, path)
		}

		return err
	}))

	return found
}

func TestAPendingFileIsReadableUnderItsKeyOnceCommitted(t *testing.T) {
	ctx := context.Background()
	f, dir := pendingStore(t)

	p, err := f.NewPendingFile(ctx, fileformat.FileTypeTesting)
	require.NoError(t, err)

	for _, chunk := range []string{"stream", "ed ", "bytes"} {
		_, err = p.Write([]byte(chunk))
		require.NoError(t, err)
	}

	key := []byte("pending-key")
	require.NoError(t, p.Commit(ctx, key))

	got, err := f.Get(ctx, key, fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.Equal(t, []byte("streamed bytes"), got, "the same bytes a Set would have stored")

	require.Empty(t, tempFilesIn(t, dir), "the temporary file was renamed, not left behind")

	_, err = os.Stat(mustName(t, f, key) + checksumExtension)
	require.NoError(t, err, "a checksum sidecar is published, as for a Set")
}

func mustName(t *testing.T, f *File, key []byte) string {
	t.Helper()

	name, err := f.constructFilename(key, fileformat.FileTypeTesting, nil)
	require.NoError(t, err)

	return name
}

func TestAnAbortedPendingFileLeavesNothing(t *testing.T) {
	ctx := context.Background()
	f, dir := pendingStore(t)

	p, err := f.NewPendingFile(ctx, fileformat.FileTypeTesting)
	require.NoError(t, err)

	_, err = p.Write([]byte("never kept"))
	require.NoError(t, err)

	p.Abort()
	p.Abort()

	require.Empty(t, tempFilesIn(t, dir))
}

func TestCommittingOverAnExistingBlobReportsItAndLeavesNothing(t *testing.T) {
	ctx := context.Background()
	f, dir := pendingStore(t)

	key := []byte("taken")
	require.NoError(t, f.Set(ctx, key, fileformat.FileTypeTesting, []byte("first")))

	p, err := f.NewPendingFile(ctx, fileformat.FileTypeTesting)
	require.NoError(t, err)

	_, err = p.Write([]byte("second"))
	require.NoError(t, err)

	err = p.Commit(ctx, key)
	require.True(t, errors.Is(err, errors.ErrBlobAlreadyExists), "got %v", err)

	got, err := f.Get(ctx, key, fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.Equal(t, []byte("first"), got, "the existing blob is untouched")
	require.Empty(t, tempFilesIn(t, dir))
}
