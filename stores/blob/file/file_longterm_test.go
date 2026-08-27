package file

import (
	"context"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const persistSubDir = "persist"

// TestFileLongtermStorage tests the three-layer storage functionality in the File store
func TestFileLongtermStorage(t *testing.T) {
	// Get a temporary directory
	tempDir, err := os.MkdirTemp("", "file-longterm-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create the persistent directory explicitly
	persistDir := filepath.Join(tempDir, persistSubDir)
	err = os.MkdirAll(persistDir, 0755)
	require.NoError(t, err)

	// Create a URL from the tempDir
	u, err := url.Parse("file://" + tempDir)
	require.NoError(t, err)

	// Create a new File store with longterm storage option
	f, err := New(ulogger.TestLogger{}, u, options.WithLongtermStorage(persistSubDir, nil))
	require.NoError(t, err)

	t.Run("file retrieval from persistent storage", func(t *testing.T) {
		testKey := []byte("test-key")
		testValue := []byte("test-value")

		// 1. Create the file in primary storage
		err = f.Set(context.Background(), testKey, fileformat.FileTypeTesting, testValue)
		require.NoError(t, err)

		// File should exist
		exists, err := f.Exists(context.Background(), testKey, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.True(t, exists)

		// Get should succeed
		value, err := f.Get(context.Background(), testKey, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.Equal(t, testValue, value)

		// Get the filenames for verification
		primaryFilename, err := f.options.ConstructFilename(f.path, testKey, fileformat.FileTypeTesting)
		require.NoError(t, err)

		// Manually copy the file to the persistent directory to simulate what longterm storage does
		persistFilename, err := f.options.ConstructFilename(filepath.Join(f.path, persistSubDir), testKey, fileformat.FileTypeTesting)
		require.NoError(t, err)

		// Create the directory structure for the persist file if it doesn't exist
		err = os.MkdirAll(filepath.Dir(persistFilename), 0755)
		require.NoError(t, err)

		// Copy the file from primary to persistent storage
		primaryData, err := os.ReadFile(primaryFilename)
		require.NoError(t, err)

		//nolint:gosec // G306: Expect WriteFile permissions to be 0600 or less (gosec)
		err = os.WriteFile(persistFilename, primaryData, 0644)
		require.NoError(t, err)

		// Remove the file from primary storage to force checking persistent storage
		err = os.Remove(primaryFilename)
		require.NoError(t, err)

		// File should still exist
		exists, err = f.Exists(context.Background(), testKey, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.True(t, exists)

		// Get should still succeed
		value, err = f.Get(context.Background(), testKey, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.Equal(t, testValue, value)

		// GetIoReader should still succeed
		reader, err := f.GetIoReader(context.Background(), testKey, fileformat.FileTypeTesting)
		require.NoError(t, err)

		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, testValue, data)
		reader.Close()

		// Clean up
		err = os.Remove(persistFilename)
		require.NoError(t, err)
	})
}

// TestFileGetDAH tests the GetDAH functionality in the File store with longterm storage
func TestFileGetDAH(t *testing.T) {
	t.Skip("DAH functionality now requires pruner service - covered by e2e tests")
}

// TestFileWithLongtermStorageOption tests creating a File store with the WithLongtermStorage option
func TestFileWithLongtermStorageOption(t *testing.T) {
	// Create temp directory for testing
	tempDir := t.TempDir()

	// Create a mock longterm storage URL
	longtermURL, err := url.Parse("file://" + filepath.Join(tempDir, persistSubDir))
	require.NoError(t, err)

	// Create a URL from the tempDir
	u, err := url.Parse("file://" + tempDir)
	require.NoError(t, err)

	// Create a new File store with longterm storage option
	store, err := New(
		ulogger.TestLogger{},
		u,
		options.WithLongtermStorage(persistSubDir, longtermURL),
	)
	require.NoError(t, err)

	// Verify that persistSubDir is set correctly
	assert.Equal(t, persistSubDir, store.persistSubDir)

	// Verify that persistent directory was created
	persistPath := filepath.Join(tempDir, persistSubDir)
	_, err = os.Stat(persistPath)
	require.NoError(t, err)

	// Verify that longtermClient is not nil
	assert.NotNil(t, store.longtermClient)
}

// fixedErrLongtermStore is a longtermStore whose reads always fail with a given
// error, so a test can pin how that error is translated.
type fixedErrLongtermStore struct {
	err error
}

func (m *fixedErrLongtermStore) Get(_ context.Context, _ []byte, _ fileformat.FileType, _ ...options.FileOption) ([]byte, error) {
	return nil, m.err
}

func (m *fixedErrLongtermStore) GetIoReader(_ context.Context, _ []byte, _ fileformat.FileType, _ ...options.FileOption) (io.ReadCloser, error) {
	return nil, m.err
}

func (m *fixedErrLongtermStore) Exists(_ context.Context, _ []byte, _ fileformat.FileType, _ ...options.FileOption) (bool, error) {
	return false, m.err
}

// TestFileLongtermMissMapsToErrNotFound pins the miss translation of the longterm
// tier. Every blob backend reports a miss as the errors.ErrNotFound sentinel, not
// as os.ErrNotExist, so mapping only os.ErrNotExist turned an S3/HTTP longterm miss
// into a storage error. Callers key control flow off that distinction — the
// aerospike external store falls back to the .outputs blob only on a genuine miss —
// so a miss reported as a failure aborts an operation that should have succeeded.
func TestFileLongtermMissMapsToErrNotFound(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
	}{
		{name: "os.ErrNotExist", err: os.ErrNotExist},
		{name: "errors.ErrNotFound", err: errors.ErrNotFound},
		{name: "errors.ErrBlobNotFound", err: errors.ErrBlobNotFound},
		{name: "wrapped ErrNotFound", err: errors.NewStorageError("[S3] [b/k] failed", errors.ErrNotFound)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			store := newLongtermBackedStore(t, tt.err)

			_, err := store.GetIoReader(context.Background(), []byte("absent"), fileformat.FileTypeTesting)

			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrNotFound),
				"a longterm miss must map to ErrNotFound, got %v", err)
		})
	}
}

// TestFileLongtermFailureIsNotAMiss is the other half: a longterm tier that failed
// to look must not be reported as "not stored".
func TestFileLongtermFailureIsNotAMiss(t *testing.T) {
	store := newLongtermBackedStore(t, errors.NewServiceUnavailableError("read permits exhausted"))

	_, err := store.GetIoReader(context.Background(), []byte("absent"), fileformat.FileTypeTesting)

	require.Error(t, err)
	require.False(t, errors.Is(err, errors.ErrNotFound),
		"a longterm store failure must not be reported as a miss, got %v", err)
	require.True(t, errors.Is(err, errors.ErrServiceUnavailable),
		"the original failure must be preserved, got %v", err)
}

func newLongtermBackedStore(t *testing.T, longtermErr error) *File {
	t.Helper()

	u, err := url.Parse("file://" + t.TempDir())
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, u)
	require.NoError(t, err)

	store.longtermClient = &fixedErrLongtermStore{err: longtermErr}

	return store
}
