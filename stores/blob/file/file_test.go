package file

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/rand"
)

func TestFileGetWithAbsolutePath(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		// check if the directory is absolute
		_, err = os.Stat(tempDir)
		require.NoError(t, err)

		// check if the directory is not relative
		_, err = os.Stat(tempDir[1:])
		require.Error(t, err)

		// Create a URL from the tempDir
		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		err = f.Set(context.Background(), []byte("key"), fileformat.FileTypeTesting, []byte("value"))
		require.NoError(t, err)

		value, err := f.Get(context.Background(), []byte("key"), fileformat.FileTypeTesting)
		require.NoError(t, err)

		require.Equal(t, []byte("value"), value)

		err = f.Del(context.Background(), []byte("key"), fileformat.FileTypeTesting)
		require.NoError(t, err)
	})
}

func TestFileGetWithRelativePath(t *testing.T) {
	ctx := context.Background()
	// random directory name
	relativePath := "test-path-" + rand.String(12)

	// Create a URL from the relative path
	u, err := url.Parse("file://./" + relativePath)
	require.NoError(t, err)

	f, err := New(ulogger.TestLogger{}, u)
	require.NoError(t, err)

	// check if the directory is created
	_, err = os.Stat("./" + relativePath)
	require.NoError(t, err)

	// check if the directory is relative
	_, err = os.Stat("/" + relativePath)
	require.Error(t, err)

	err = f.Set(ctx, []byte("key"), fileformat.FileTypeTesting, []byte("value"))
	require.NoError(t, err)

	value, err := f.Get(ctx, []byte("key"), fileformat.FileTypeTesting)
	require.NoError(t, err)

	require.Equal(t, []byte("value"), value)

	err = f.Del(ctx, []byte("key"), fileformat.FileTypeTesting)
	require.NoError(t, err)

	// cleanup
	_ = os.RemoveAll(relativePath)

	f.Close(ctx)
}

func TestFileAbsoluteAndRelativePath(t *testing.T) {
	absoluteURL, err := url.ParseRequestURI("file:///absolute/path/to/file")
	require.NoError(t, err)
	require.Equal(t, "/absolute/path/to/file", GetPathFromURL(absoluteURL))

	relativeURL, err := url.ParseRequestURI("file://./relative/path/to/file")
	require.NoError(t, err)
	require.Equal(t, "relative/path/to/file", GetPathFromURL(relativeURL))
}

func GetPathFromURL(u *url.URL) string {
	if u.Host == "." {
		return u.Path[1:]
	}

	return u.Path
}

func TestFileNewWithEmptyPath(t *testing.T) {
	t.Run("empty path", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, nil)
		require.Error(t, err)
		require.Nil(t, f)
	})
}

func TestFileNewWithInvalidDirectory(t *testing.T) {
	t.Run("invalid directory", func(t *testing.T) {
		invalidPath := "/invalid-directory" // Assuming this path cannot be created

		u, err := url.Parse("file://" + invalidPath)
		require.NoError(t, err)

		_, err = New(ulogger.TestLogger{}, u)
		require.Error(t, err) // "mkdir /invalid-directory: read-only file system"
		require.Contains(t, err.Error(), "failed to create directory")
	})
}

// TestFileLoadDAHsCleanupTmpFiles removed - loadDAHs() functionality moved to pruner service

func TestFileConcurrentAccess(t *testing.T) {
	t.Run("concurrent set and get", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		var wg sync.WaitGroup

		concurrency := 100

		for i := 0; i < concurrency; i++ {
			wg.Add(1)

			go func(i int) {
				defer wg.Done()

				key := []byte(fmt.Sprintf("key-%d", i))
				value := []byte(fmt.Sprintf("value-%d", i))

				err := f.Set(context.Background(), key, fileformat.FileTypeTesting, value)
				require.NoError(t, err)

				retrievedValue, err := f.Get(context.Background(), key, fileformat.FileTypeTesting)
				require.NoError(t, err)
				require.Equal(t, value, retrievedValue)
			}(i)
		}

		wg.Wait()
	})
}

func TestFileSetWithSubdirectoryOptionIgnored(t *testing.T) {
	t.Run("subdirectory usage", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		subDir := "subDir"

		f, err := New(ulogger.TestLogger{}, u, options.WithDefaultSubDirectory(subDir))
		require.NoError(t, err)

		key := []byte("key")
		value := []byte("value")

		err = f.Set(context.Background(), key, fileformat.FileTypeTesting, value)
		require.NoError(t, err)

		// Construct the expected file path in the subdirectory
		expectedDir := filepath.Join(tempDir, subDir)
		expectedFilePath := filepath.Join(expectedDir, util.ReverseAndHexEncodeSlice(key)+"."+fileformat.FileTypeTesting.String())

		_, err = os.Stat(expectedFilePath)
		require.NoError(t, err)
	})
}

func TestFileSetWithSubdirectoryOption(t *testing.T) {
	t.Run("subdirectory usage", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		subDir := "subDir"

		f, err := New(ulogger.TestLogger{}, u, options.WithDefaultSubDirectory(subDir))
		require.NoError(t, err)

		key := []byte("key")
		value := []byte("value")

		err = f.Set(context.Background(), key, fileformat.FileTypeTesting, value, options.WithFilename("filename"))
		require.NoError(t, err)

		// Construct the expected file path in the subdirectory
		expectedDir := filepath.Join(tempDir, subDir)
		expectedFilePath := filepath.Join(expectedDir, "filename"+"."+fileformat.FileTypeTesting.String())

		// Check if the file was created in the subdirectory
		_, err = os.Stat(expectedFilePath)
		require.NoError(t, err, "expected file found in subdirectory")
	})
}

func TestFileWithHeader(t *testing.T) {
	t.Run("set and get with header", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "TestFileWithHeader")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("key-with-header")
		content := "This is the main content"

		// Test setting content with header using Set
		err = f.Set(context.Background(), key, fileformat.FileTypeTesting, []byte(content))
		require.NoError(t, err)

		// Verify content using Get
		value, err := f.Get(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.Equal(t, content, string(value))

		// Verify content using GetIoReader
		reader, err := f.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		// Read all the content from the reader
		readContent, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, content, string(readContent))

		// delete the file
		err = f.Del(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		// Test setting content with header using SetFromReader
		newContent := "New content from reader"

		contentReader := strings.NewReader(newContent)
		readCloser := io.NopCloser(contentReader)

		err = f.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, readCloser)
		require.NoError(t, err)

		// Verify new content using Get
		value, err = f.Get(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.Equal(t, newContent, string(value))

		// Verify new content using GetIoReader
		reader, err = f.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		readContent, err = io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, newContent, string(readContent))

		// delete the file
		err = f.Del(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
	})
}

func TestFileWithFooter(t *testing.T) {
	t.Run("set and get with footer", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "TestFileWithFooter")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("key-with-footer")
		content := "This is the main content"

		// Test setting content with footer using Set
		err = f.Set(context.Background(), key, fileformat.FileTypeTesting, []byte(content))
		require.NoError(t, err)

		// Verify content using Get
		value, err := f.Get(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.Equal(t, content, string(value))

		// Verify content using GetIoReader
		reader, err := f.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		// Read all the content from the reader
		readContent, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, content, string(readContent))

		// delete the file
		err = f.Del(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		// Test setting content with footer using SetFromReader
		newContent := "New content from reader"
		contentReader := strings.NewReader(newContent)
		readCloser := io.NopCloser(contentReader)

		err = f.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, readCloser)
		require.NoError(t, err)

		// Verify new content using Get
		value, err = f.Get(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.Equal(t, newContent, string(value))

		// Verify new content using GetIoReader
		reader, err = f.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		readContent, err = io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, newContent, string(readContent))

		// delete the file
		err = f.Del(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
	})
}

func TestFileSetFromReaderAndGetIoReader(t *testing.T) {
	t.Run("set content from reader", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("key")
		content := "This is test reader content"
		reader := strings.NewReader(content)

		// Wrap the reader to satisfy the io.ReadCloser interface
		readCloser := io.NopCloser(reader)

		err = f.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, readCloser)
		require.NoError(t, err)

		// Verify the content was correctly stored
		storedReader, err := f.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		// Read all the content from the storedReader
		storedContent, err := io.ReadAll(storedReader)
		require.NoError(t, err)
		assert.Equal(t, content, string(storedContent))
	})
}

// errorAfterNBytesReader returns an error after reading n bytes
type errorAfterNBytesReader struct {
	data          []byte
	pos           int
	errorAt       int
	errorToReturn error
}

func (r *errorAfterNBytesReader) Read(p []byte) (int, error) {
	if r.pos >= r.errorAt {
		return 0, r.errorToReturn
	}
	remaining := r.errorAt - r.pos
	if len(p) > remaining {
		p = p[:remaining]
	}
	n := copy(p, r.data[r.pos:r.pos+len(p)])
	r.pos += n
	if r.pos >= r.errorAt {
		return n, r.errorToReturn
	}
	return n, nil
}

func (r *errorAfterNBytesReader) Close() error {
	return nil
}

func TestSetFromReader_CleansUpTempFileOnError(t *testing.T) {
	t.Run("temp file is removed when reader returns error", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test-cleanup")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("cleanup-test-key")

		// Create a reader that returns an error after some data
		testData := make([]byte, 1000)
		for i := range testData {
			testData[i] = byte(i % 256)
		}
		readErr := errors.NewProcessingError("simulated reader error")
		errorReader := &errorAfterNBytesReader{
			data:          testData,
			errorAt:       500, // Error after 500 bytes
			errorToReturn: readErr,
		}

		// SetFromReader should fail with the reader error
		err = f.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, errorReader)
		require.Error(t, err, "SetFromReader should return an error when reader fails")

		// Verify no temp files remain in the directory
		files, err := os.ReadDir(tempDir)
		require.NoError(t, err)

		for _, file := range files {
			assert.False(t, strings.HasSuffix(file.Name(), ".tmp"),
				"Temp file should be cleaned up on error: %s", file.Name())
		}

		// Verify the final file was NOT created
		exists, err := f.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.False(t, exists, "File should not exist after failed SetFromReader")
	})
}

func TestSetFromReader_CleansUpTempFileOnPipeClose(t *testing.T) {
	t.Run("temp file is removed when pipe is closed with error", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test-pipe-cleanup")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("pipe-cleanup-test-key")

		// Create a pipe and close it with an error to simulate abort
		pr, pw := io.Pipe()

		// Start writing some data in a goroutine
		go func() {
			_, _ = pw.Write([]byte("some initial data"))
			// Close the pipe with an error to simulate abort
			_ = pw.CloseWithError(errors.NewProcessingError("simulated abort"))
		}()

		// SetFromReader should fail with the pipe error
		err = f.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, pr)
		require.Error(t, err, "SetFromReader should return an error when pipe is closed with error")

		// Verify no temp files remain
		files, err := os.ReadDir(tempDir)
		require.NoError(t, err)

		for _, file := range files {
			assert.False(t, strings.HasSuffix(file.Name(), ".tmp"),
				"Temp file should be cleaned up on pipe error: %s", file.Name())
		}

		// Verify the final file was NOT created
		exists, err := f.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.False(t, exists, "File should not exist after aborted SetFromReader")
	})
}

func TestFileGetHead(t *testing.T) {
	t.Run("get head of content", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("key")
		content := "This is test head content"
		reader := strings.NewReader(content)

		// Wrap the reader to satisfy the io.ReadCloser interface
		readCloser := io.NopCloser(reader)

		// First, set the content
		err = f.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, readCloser)
		require.NoError(t, err)

		// Get metadata using GetHead
		headReader, err := f.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		head := make([]byte, 1) // Read only the first byte
		_, err = headReader.Read(head)
		require.NoError(t, err)

		assert.NotNil(t, head)
		assert.Equal(t, content[:1], string(head), "head content doesn't match")
	})
}

func TestFileGetHeadWithHeader(t *testing.T) {
	t.Run("get head of content", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("key")
		content := "This is test head content"
		reader := strings.NewReader(content)

		// Wrap the reader to satisfy the io.ReadCloser interface
		readCloser := io.NopCloser(reader)

		// First, set the content
		err = f.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, readCloser)
		require.NoError(t, err)

		// Get metadata using GetHead
		headReader, err := f.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		head := make([]byte, 1) // Read only the first byte
		_, err = headReader.Read(head)
		require.NoError(t, err)

		assert.NotNil(t, head)
		assert.Equal(t, content[:1], string(head), "head content doesn't match")
	})
}

func TestFileExists(t *testing.T) {
	t.Run("check if content exists", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("key-exists")
		content := "This is test exists content"
		reader := strings.NewReader(content)

		// Content should not exist before setting
		exists, err := f.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.False(t, exists)

		// Wrap the reader to satisfy the io.ReadCloser interface
		readCloser := io.NopCloser(reader)

		// Set the content
		err = f.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, readCloser)
		require.NoError(t, err)

		// Now content should exist
		exists, err = f.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.True(t, exists)
	})
}

func TestFileDAHUntouchedOnExistingFileWhenOverwriteDisabled(t *testing.T) {
	t.Skip("DAH functionality now requires pruner service - covered by e2e tests")
}

func TestFileSetWithHashPrefix(t *testing.T) {
	u, err := url.Parse("file:///data/subtreestore?hashPrefix=2")
	require.NoError(t, err)
	require.Equal(t, "/data/subtreestore", u.Path)
	require.Equal(t, "2", u.Query().Get("hashPrefix"))

	u, err = url.Parse("null:///?localTTLStore=file&localTTLStorePath=./data/subtreestore-dah?hashPrefix=2")
	require.NoError(t, err)

	localTTLStoreURL := u.Query().Get("localTTLStorePath")
	u2, err := url.Parse(localTTLStoreURL)
	require.NoError(t, err)

	hashPrefix := u2.Query().Get("hashPrefix")
	require.Equal(t, "2", hashPrefix)
}

func TestFileSetHashPrefixOverride(t *testing.T) {
	u, err := url.Parse("file://./data/subtreestore?hashPrefix=2")
	require.NoError(t, err)

	f, err := New(ulogger.TestLogger{}, u, options.WithHashPrefix(1))
	require.NoError(t, err)

	// Even though the option is set to 1, the URL hashPrefix should override it
	require.Equal(t, 2, f.options.HashPrefix)
}

func TestFileHealth(t *testing.T) {
	t.Run("healthy state", func(t *testing.T) {
		// Setup
		tempDir, err := os.MkdirTemp("", "test-health")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		// Test
		status, message, err := f.Health(context.Background(), false)

		// Assert
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, "File Store: Healthy", message)
	})

	t.Run("non-existent path", func(t *testing.T) {
		// Setup
		nonExistentPath := "./not_exist"
		u, err := url.Parse("file://" + nonExistentPath)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		// creating a New store will create the folder
		// so we need to remove it before we test
		var path string
		if u.Host == "." {
			path = u.Path[1:] // relative path
		} else {
			path = u.Path // absolute path
		}

		err = os.RemoveAll(path)
		require.NoError(t, err)

		// Test
		status, message, err := f.Health(context.Background(), false)

		// Assert
		require.Error(t, err)
		require.Equal(t, http.StatusInternalServerError, status)
		require.Equal(t, "File Store: Path does not exist", message)
	})

	t.Run("read-only directory", func(t *testing.T) {
		// Setup
		tempDir, err := os.MkdirTemp("", "test-health-readonly")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		// Make the directory read-only
		err = os.Chmod(tempDir, 0o555)
		require.NoError(t, err)

		// nolint:errcheck
		defer os.Chmod(tempDir, 0o755) // Restore permissions for cleanup

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		// Test
		status, message, err := f.Health(context.Background(), false)

		// Assert
		require.Error(t, err)
		require.Equal(t, http.StatusInternalServerError, status)
		require.Equal(t, "File Store: Unable to create temporary file", message)
	})

	t.Run("write permission denied", func(t *testing.T) {
		// Setup
		tempDir, err := os.MkdirTemp("", "test-health-write-denied")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		// Make the directory read-only
		err = os.Chmod(tempDir, 0o555)
		require.NoError(t, err)

		// nolint:errcheck
		defer os.Chmod(tempDir, 0o755) // Restore permissions for cleanup

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		// Test
		status, message, err := f.Health(context.Background(), false)

		// Assert
		require.Error(t, err)
		require.Equal(t, http.StatusInternalServerError, status)
		require.Equal(t, "File Store: Unable to create temporary file", message)
	})
}

func TestFileWithURLHeaderFooter(t *testing.T) {
	t.Run("with header and footer in URL", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test-header-footer")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		// Create URL with header and footer parameters
		u, err := url.Parse(fmt.Sprintf("file://%s?header=START&eofmarker=END", tempDir))
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("test-key")
		content := "test content"

		// Test Set
		err = f.Set(context.Background(), key, fileformat.FileTypeTesting, []byte(content))
		require.NoError(t, err)

		// Read raw file to verify header and footer
		filename, err := f.options.ConstructFilename(tempDir, key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		rawData, err := os.ReadFile(filename)
		require.NoError(t, err)

		// Verify header and footer are present in raw data
		expectedData := append([]byte("TESTING "), []byte(content)...)
		assert.Equal(t, expectedData, rawData)

		// Test Get - should return content without header/footer
		value, err := f.Get(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.Equal(t, content, string(value))

		// Test GetIoReader - should return content without header/footer
		reader, err := f.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		defer reader.Close()

		readContent, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, content, string(readContent))
	})
}

func TestWithSHA256Checksum(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "file_store_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	logger := ulogger.TestLogger{}

	storeURL := "file://" + tempDir
	value := []byte("test data")
	key := []byte("testkey123")
	fileType := fileformat.FileTypeTesting

	// Parse URL
	u, err := url.Parse(storeURL)
	require.NoError(t, err)

	// Create file store
	store, err := New(logger, u)
	require.NoError(t, err)

	// Set data with extension
	err = store.Set(context.Background(), key, fileType, value)
	require.NoError(t, err)

	// Construct expected filename
	filename, err := store.options.ConstructFilename(tempDir, key, fileType)
	require.NoError(t, err)

	// Verify main file exists and contains correct data
	data, err := os.ReadFile(filename)
	require.NoError(t, err)

	expectedData := make([]byte, len(fileType.ToMagicBytes())+len(value))
	ft := fileType.ToMagicBytes()
	copy(expectedData, ft[:])
	copy(expectedData[len(ft):], value)

	require.Equal(t, expectedData, data)

	// Check SHA256 file
	sha256Filename := filename + checksumExtension
	_, err = os.Stat(sha256Filename)

	require.NoError(t, err, "SHA256 file should exist")

	// Read and verify SHA256 file content
	hashFileContent, err := os.ReadFile(sha256Filename)
	require.NoError(t, err)

	// Calculate expected hash
	hasher := sha256.New()
	hasher.Write(ft[:])
	hasher.Write(value)
	expectedHash := fmt.Sprintf("%x", hasher.Sum(nil))

	// Verify hash file format
	hashFileStr := string(hashFileContent)
	parts := strings.Fields(hashFileStr)
	require.Len(t, parts, 2, "Hash file should have hash and filename separated by two spaces")

	// Verify hash matches
	require.Equal(t, expectedHash, parts[0], "Hash in file should match calculated hash")

	// Verify filename part
	expectedFilename := fmt.Sprintf("%x.%s", bt.ReverseBytes(key), fileType)
	require.Equal(t, expectedFilename, parts[1], "Filename in hash file should match expected format")
}

func TestSetFromReaderWithSHA256(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "file_store_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	logger := ulogger.TestLogger{}
	u, err := url.Parse("file://" + tempDir)
	require.NoError(t, err)

	store, err := New(logger, u)
	require.NoError(t, err)

	// Test data
	testData := []byte("test data for SetFromReader")
	key := []byte("testreaderkey")

	// Create reader
	reader := io.NopCloser(bytes.NewReader(testData))

	// Set data
	err = store.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, reader)
	require.NoError(t, err)

	// Construct filename
	merged := options.MergeOptions(store.options, []options.FileOption{})
	filename, err := merged.ConstructFilename(tempDir, key, fileformat.FileTypeTesting)
	require.NoError(t, err)

	// Verify main file
	data, err := os.ReadFile(filename)
	require.NoError(t, err)

	expectedData := make([]byte, len(fileformat.FileTypeTesting.ToMagicBytes())+len(testData))
	ft := fileformat.FileTypeTesting.ToMagicBytes()
	copy(expectedData, ft[:])
	copy(expectedData[len(ft):], testData)

	require.Equal(t, expectedData, data)

	// Verify SHA256 file
	sha256Filename := filename + checksumExtension
	hashFileContent, err := os.ReadFile(sha256Filename)
	require.NoError(t, err)

	// Calculate expected hash
	hasher := sha256.New()
	hasher.Write(ft[:])
	hasher.Write(testData)
	expectedHash := fmt.Sprintf("%x", hasher.Sum(nil))

	// Verify hash file format
	hashFileStr := string(hashFileContent)
	parts := strings.Fields(hashFileStr)
	require.Len(t, parts, 2)
	require.Equal(t, expectedHash, parts[0])

	// Verify filename part
	expectedFilename := fmt.Sprintf("%x.%s", bt.ReverseBytes(key), fileformat.FileTypeTesting)
	require.Equal(t, expectedFilename, parts[1])
}

func TestSHA256WithHeaderFooter(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "file_store_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	logger := ulogger.TestLogger{}
	u, err := url.Parse("file://" + tempDir)
	require.NoError(t, err)

	store, err := New(logger, u)
	require.NoError(t, err)

	// Test data
	data := []byte("test data")
	key := []byte("testheaderfooter")

	// Set data
	err = store.Set(context.Background(), key, fileformat.FileTypeTesting, data)
	require.NoError(t, err)

	// Construct filename
	merged := options.MergeOptions(store.options, []options.FileOption{})
	filename, err := merged.ConstructFilename(tempDir, key, fileformat.FileTypeTesting)
	require.NoError(t, err)

	// Verify main file includes header and footer
	fileContent, err := os.ReadFile(filename)
	require.NoError(t, err)

	expectedData := make([]byte, len(fileformat.FileTypeTesting.ToMagicBytes())+len(data))
	ft := fileformat.FileTypeTesting.ToMagicBytes()
	copy(expectedData, ft[:])
	copy(expectedData[len(ft):], data)

	assert.Equal(t, expectedData, fileContent)

	// Verify SHA256 file
	sha256Filename := filename + checksumExtension
	hashFileContent, err := os.ReadFile(sha256Filename)
	require.NoError(t, err)

	// Calculate expected hash (including header and footer)
	hasher := sha256.New()
	hasher.Write(expectedData)
	expectedHash := fmt.Sprintf("%x", hasher.Sum(nil))

	// Verify hash file format
	hashFileStr := string(hashFileContent)
	parts := strings.Fields(hashFileStr)
	require.Len(t, parts, 2)
	require.Equal(t, expectedHash, parts[0])
}

func TestFile_SetFromReader_WithHeaderAndFooter(t *testing.T) {
	dir := t.TempDir()

	// Use TestLogger instead of NewSimpleLogger
	logger := ulogger.TestLogger{}

	storeURL, _ := url.Parse("file://" + dir)
	q := storeURL.Query()
	q.Set("header", "header")

	storeURL.RawQuery = q.Encode()

	store, err := New(logger, storeURL)
	require.NoError(t, err)

	// Test data
	key := []byte("test")
	data := []byte("test data")
	reader := io.NopCloser(bytes.NewReader(data))

	// Set data
	err = store.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, reader)
	require.NoError(t, err)

	// Read file directly
	filename, err := store.options.ConstructFilename(dir, key, fileformat.FileTypeTesting)
	require.NoError(t, err)

	content, err := os.ReadFile(filename)
	require.NoError(t, err)

	// Verify content includes header
	expectedData := make([]byte, len(fileformat.FileTypeTesting.ToMagicBytes())+len(data))
	ft := fileformat.FileTypeTesting.ToMagicBytes()
	copy(expectedData, ft[:])
	copy(expectedData[len(ft):], data)

	assert.Equal(t, expectedData, content)
}

func TestFile_Set_WithHeaderAndFooter(t *testing.T) {
	dir := t.TempDir()

	store, err := New(
		ulogger.TestLogger{},
		&url.URL{Path: dir})
	require.NoError(t, err)

	// Test data
	key := []byte("test")
	data := []byte("test data")

	// Set data
	err = store.Set(context.Background(), key, fileformat.FileTypeTesting, data)
	require.NoError(t, err)

	// Read file directly
	filename, err := store.options.ConstructFilename(dir, key, fileformat.FileTypeTesting)
	require.NoError(t, err)

	content, err := os.ReadFile(filename)
	require.NoError(t, err)

	// Verify content includes header and footer
	expectedData := make([]byte, len(fileformat.FileTypeTesting.ToMagicBytes())+len(data))
	ft := fileformat.FileTypeTesting.ToMagicBytes()
	copy(expectedData, ft[:])
	copy(expectedData[len(ft):], data)

	assert.Equal(t, expectedData, content)
}

func TestFile_Get_WithHeaderAndFooter(t *testing.T) {
	dir := t.TempDir()

	store, err := New(
		ulogger.TestLogger{},
		&url.URL{Path: dir})
	require.NoError(t, err)

	// Test data
	key := []byte("test")
	data := []byte("test data")

	// Set data
	err = store.Set(context.Background(), key, fileformat.FileTypeTesting, data)
	require.NoError(t, err)

	// Get data
	retrieved, err := store.Get(context.Background(), key, fileformat.FileTypeTesting)
	require.NoError(t, err)

	// Verify retrieved data matches original (without header)
	assert.Equal(t, data, retrieved)
}

func TestFile_GetIoReader_WithHeaderAndFooter(t *testing.T) {
	dir := t.TempDir()

	store, err := New(
		ulogger.TestLogger{},
		&url.URL{Path: dir})
	require.NoError(t, err)

	// Test data
	key := []byte("test")
	data := []byte("test data")

	// Set data
	err = store.Set(context.Background(), key, fileformat.FileTypeTesting, data)
	require.NoError(t, err)

	// Get reader
	reader, err := store.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
	require.NoError(t, err)
	defer reader.Close()

	// Read data from reader
	retrieved, err := io.ReadAll(reader)
	require.NoError(t, err)

	// Verify retrieved data matches original (without header/footer)
	assert.Equal(t, data, retrieved)
}

func TestFileGetAndSetDAH(t *testing.T) {
	t.Skip("DAH functionality now requires pruner service - covered by e2e tests")
}

func TestFileURLParameters(t *testing.T) {
	t.Run("hashSuffix from URL", func(t *testing.T) {
		u, err := url.Parse("file://./data/subtreestore?hashSuffix=3")
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u, options.WithHashPrefix(1))
		require.NoError(t, err)

		// hashSuffix in URL should set HashPrefix to negative value
		require.Equal(t, -3, f.options.HashPrefix)
	})

	t.Run("invalid hashSuffix in URL", func(t *testing.T) {
		u, err := url.Parse("file://./data/subtreestore?hashSuffix=invalid")
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.Error(t, err)
		require.Nil(t, f)
		require.Contains(t, err.Error(), "failed to parse hashSuffix")
	})
}

func TestFileGetNonExistent(t *testing.T) {
	t.Run("get non-existent file", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test-get-nonexistent")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		// Try to get non-existent file
		key := []byte("nonexistent-key-1")
		_, err = f.Get(context.Background(), key, fileformat.FileTypeTesting)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound))

		// Try to get non-existent file with options
		_, err = f.Get(context.Background(), key, fileformat.FileTypeTesting)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound))
	})

	t.Run("get io reader for non-existent file", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test-get-reader-nonexistent")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		// Try to get reader for non-existent file
		key := []byte("nonexistent-key-2")
		reader, err := f.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound))
		require.Nil(t, reader)

		// Try to get reader for non-existent file with options
		reader, err = f.GetIoReader(context.Background(), key, fileformat.FileTypeTesting)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound))
		require.Nil(t, reader)
	})
}

func TestFileChecksumNotDeletedOnDelete(t *testing.T) {
	// Get a temporary directory
	tempDir, err := os.MkdirTemp("", "test-checksum-delete-on-delete")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	u, err := url.Parse("file://" + tempDir)
	require.NoError(t, err)

	f, err := New(ulogger.TestLogger{}, u)
	require.NoError(t, err)

	key := "test-key-delete-checksum"
	content := []byte("test content")

	// Put a file with checksum
	err = f.Set(context.Background(), []byte(key), fileformat.FileTypeTesting, content)
	require.NoError(t, err)

	// Construct filename
	merged := options.MergeOptions(f.options, []options.FileOption{})
	filename, err := merged.ConstructFilename(tempDir, []byte(key), fileformat.FileTypeTesting)
	require.NoError(t, err)

	// Verify the checksum file exists
	_, err = os.Stat(filename)
	require.NoError(t, err, "checksum file should exist")

	// Delete the file
	err = f.Del(context.Background(), []byte(key), fileformat.FileTypeTesting)
	require.NoError(t, err, "file deletion should succeed")

	// Check if checksum file still exists - this is the bug
	_, err = os.Stat(filename)
	require.True(t, os.IsNotExist(err), "Checksum file should be removed when content file is deleted")
}

func TestDAHZeroHandling(t *testing.T) {
	t.Skip("DAH functionality now requires pruner service - covered by e2e tests")
	t.Run("readDAHFromFile with DAH 0 returns error", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-dah-zero")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		// Create a DAH file with value 0
		dahFile := filepath.Join(tempDir, "test.dah")
		err = os.WriteFile(dahFile, []byte("0"), 0o600)
		require.NoError(t, err)

		// Try to read it
		dah, err := f.readDAHFromFile(dahFile)
		require.Error(t, err, "should return error for DAH 0")
		require.Contains(t, err.Error(), "invalid DAH value 0")
		require.Equal(t, uint32(0), dah)
	})

	t.Run("readDAHFromFile with empty file returns error", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-dah-empty")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		// Create an empty DAH file
		dahFile := filepath.Join(tempDir, "test.dah")
		err = os.WriteFile(dahFile, []byte(""), 0o600)
		require.NoError(t, err)

		// Try to read it
		dah, err := f.readDAHFromFile(dahFile)
		require.Error(t, err, "should return error for empty DAH file")
		require.Contains(t, err.Error(), "DAH file")
		require.Contains(t, err.Error(), "is empty")
		require.Equal(t, uint32(0), dah)
	})

	t.Run("readDAHFromFile with whitespace only returns error", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-dah-whitespace")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		// Create a DAH file with only whitespace
		dahFile := filepath.Join(tempDir, "test.dah")
		err = os.WriteFile(dahFile, []byte("  \n\t  "), 0o600)
		require.NoError(t, err)

		// Try to read it
		dah, err := f.readDAHFromFile(dahFile)
		require.Error(t, err, "should return error for whitespace-only DAH file")
		require.Contains(t, err.Error(), "DAH file")
		require.Contains(t, err.Error(), "is empty")
		require.Equal(t, uint32(0), dah)
	})

	t.Run("SetDAH with 0 removes DAH file", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-setdah-zero")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := "test-key"
		content := []byte("test content")

		// Put a file
		err = f.Set(context.Background(), []byte(key), fileformat.FileTypeTesting, content)
		require.NoError(t, err)

		// Set DAH to a valid value first
		err = f.SetDAH(context.Background(), []byte(key), fileformat.FileTypeTesting, 100)
		require.NoError(t, err)

		// Verify DAH file exists
		merged := options.MergeOptions(f.options, []options.FileOption{})
		filename, err := merged.ConstructFilename(tempDir, []byte(key), fileformat.FileTypeTesting)
		require.NoError(t, err)
		dahFile := filename + ".dah"
		_, err = os.Stat(dahFile)
		require.NoError(t, err, "DAH file should exist")

		// Set DAH to 0
		err = f.SetDAH(context.Background(), []byte(key), fileformat.FileTypeTesting, 0)
		require.NoError(t, err)

		// Verify DAH file is removed
		_, err = os.Stat(dahFile)
		require.True(t, os.IsNotExist(err), "DAH file should be removed when DAH is set to 0")

		// Verify blob file still exists
		_, err = os.Stat(filename)
		require.NoError(t, err, "Blob file should still exist")
	})

	t.Run("writeDAHToFile validation prevents DAH 0", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-write-dah-zero")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		dahFile := filepath.Join(tempDir, "test.dah")

		// Attempt to write DAH 0
		err = f.writeDAHToFile(dahFile, 0)
		require.Error(t, err, "Should error when attempting to write DAH 0")
		require.Contains(t, err.Error(), "invalid DAH value 0")

		// Verify no file was created
		_, err = os.Stat(dahFile)
		require.True(t, os.IsNotExist(err), "DAH file should not exist")

		// Verify no temp file was left behind
		_, err = os.Stat(dahFile + ".tmp")
		require.True(t, os.IsNotExist(err), "Temp file should not exist")
	})

	t.Run("writeDAHToFile uses fsync", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-write-dah-fsync")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		dahFile := filepath.Join(tempDir, "test.dah")

		// Write a valid DAH
		err = f.writeDAHToFile(dahFile, 12345)
		require.NoError(t, err)

		// Verify file exists and contains correct value
		content, err := os.ReadFile(dahFile)
		require.NoError(t, err)
		require.Equal(t, "12345", string(content))

		// Verify no temp file was left behind
		_, err = os.Stat(dahFile + ".tmp")
		require.True(t, os.IsNotExist(err), "Temp file should be cleaned up")
	})
}
