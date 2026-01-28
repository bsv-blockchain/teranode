package file

import (
	"context"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"testing"

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
