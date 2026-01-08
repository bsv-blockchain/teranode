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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/ordishs/go-utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
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

func TestFileLoadDAHs(t *testing.T) {
	t.Run("load DAHs", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		key := []byte("key")
		value := []byte("value")

		ctx := context.Background()

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := newStore(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		err = f.Set(ctx, key, fileformat.FileTypeTesting, value)
		require.NoError(t, err)

		err = f.SetDAH(ctx, key, fileformat.FileTypeTesting, 11)
		require.NoError(t, err)

		var fileDAHs map[string]uint32

		f.fileDAHsMu.Lock()
		fileDAHs = f.fileDAHs
		require.Contains(t, fileDAHs, filepath.Join(tempDir, utils.ReverseAndHexEncodeSlice(key))+"."+fileformat.FileTypeTesting.String())
		f.fileDAHsMu.Unlock()

		f.SetCurrentBlockHeight(11)
		f.cleanupExpiredFiles()

		f.fileDAHsMu.Lock()
		require.NotContains(t, f.fileDAHs, filepath.Join(tempDir, utils.ReverseAndHexEncodeSlice(key)))
		f.fileDAHsMu.Unlock()

		err = f.Del(ctx, key, fileformat.FileTypeTesting)
		require.NoError(t, err) // deleting non-existing file should not return error
	})
}

func BenchmarkFileLoadDAHs(b *testing.B) {
	// Get a temporary directory
	tempDir, err := os.MkdirTemp("", "test")
	require.NoError(b, err)
	defer os.RemoveAll(tempDir)

	ctx := context.Background()

	u, err := url.Parse("file://" + tempDir)
	require.NoError(b, err)

	f, err := newStore(ulogger.TestLogger{}, u)
	require.NoError(b, err)

	keys := make([][]byte, b.N)

	for i := 0; i < b.N; i++ {
		key := chainhash.HashH([]byte(fmt.Sprintf("key-%d", i)))
		value := []byte("value")

		err = f.Set(ctx, key[:], fileformat.FileTypeTesting, value, options.WithDeleteAt(10))
		require.NoError(b, err)

		err = f.SetDAH(ctx, key[:], fileformat.FileTypeTesting, 11)
		require.NoError(b, err)

		keys[i] = key[:]
	}

	g := errgroup.Group{}

	for _, key := range keys {
		// unset all the DAH
		g.Go(func() error {
			return f.SetDAH(ctx, key, fileformat.FileTypeTesting, 0)
		})
	}

	require.NoError(b, g.Wait())

	var fileDAHs map[string]uint32

	f.fileDAHsMu.Lock()

	fileDAHs = f.fileDAHs

	for _, key := range keys {
		require.NotContains(b, fileDAHs, filepath.Join(tempDir, utils.ReverseAndHexEncodeSlice(key)))
	}

	f.fileDAHsMu.Unlock()
}

func TestFileLoadDAHsCleanupTmpFiles(t *testing.T) {
	t.Run("cleanup only old dah.tmp files", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test-dah-tmp-cleanup")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		// Create an old .dah.tmp file (>10 minutes old)
		oldTmpFile := filepath.Join(tempDir, "old.tx.dah.tmp")
		err = os.WriteFile(oldTmpFile, []byte("12345"), 0o600)
		require.NoError(t, err)

		// Modify the file's mod time to be 15 minutes ago
		oldTime := time.Now().Add(-15 * time.Minute)
		err = os.Chtimes(oldTmpFile, oldTime, oldTime)
		require.NoError(t, err)

		// Create a new .dah.tmp file (recent)
		newTmpFile := filepath.Join(tempDir, "new.tx.dah.tmp")
		err = os.WriteFile(newTmpFile, []byte("67890"), 0o600)
		require.NoError(t, err)

		// Create file store
		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		_, err = New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		// Wait a moment for the background loadDAHs to complete
		time.Sleep(100 * time.Millisecond)

		// Check that old tmp file was removed
		_, err = os.Stat(oldTmpFile)
		require.True(t, os.IsNotExist(err), "Old tmp file should have been removed")

		// Check that new tmp file still exists
		_, err = os.Stat(newTmpFile)
		require.NoError(t, err, "New tmp file should still exist")

		// Clean up the new tmp file
		os.Remove(newTmpFile)
	})
}

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
		expectedFilePath := filepath.Join(expectedDir, utils.ReverseAndHexEncodeSlice(key)+"."+fileformat.FileTypeTesting.String())

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
	t.Run("check if DAH remains unchanged when file overwriting disabled using setFromReader", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("key-exists")
		content := "This is test content for setFromReader"
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

		// Set DAH to 0
		err = f.SetDAH(context.Background(), key, fileformat.FileTypeTesting, 0)
		require.NoError(t, err)

		// Set the content again with overwrite disabled
		err = f.SetFromReader(context.Background(), key, fileformat.FileTypeTesting, readCloser, options.WithAllowOverwrite(false))
		require.Error(t, err)

		// Check the DAH again, should still be 0
		dah, err := f.GetDAH(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.Equal(t, uint32(0), dah)
	})

	t.Run("check if DAH remains unchanged when file overwriting disabled using Set", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("key-exists")
		content := "This is test content for set"

		// Content should not exist before setting
		exists, err := f.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.False(t, exists)

		// Set the content
		err = f.Set(context.Background(), key, fileformat.FileTypeTesting, []byte(content))
		require.NoError(t, err)

		// Now content should exist
		exists, err = f.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.True(t, exists)

		// Set DAH to 0
		err = f.SetDAH(context.Background(), key, fileformat.FileTypeTesting, 0)
		require.NoError(t, err)

		// Set the content again with overwrite disabled
		err = f.Set(context.Background(), key, fileformat.FileTypeTesting, []byte(content), options.WithAllowOverwrite(false))
		require.Error(t, err)

		// Check the DAH again, should still be 0
		dah, err := f.GetDAH(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.Equal(t, uint32(0), dah)
	})
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
	t.Run("get and set DAH", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test-dah")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("dah-test-key")
		content := []byte("test content")

		// Set initial content without DAH
		err = f.Set(context.Background(), key, fileformat.FileTypeTesting, content)
		require.NoError(t, err)

		// Initially there should be no DAH
		dah, err := f.GetDAH(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.Zero(t, dah)

		// Set a DAH
		newDAH := uint32(10)
		err = f.SetDAH(context.Background(), key, fileformat.FileTypeTesting, newDAH)
		require.NoError(t, err)

		// Get and verify DAH
		dah, err = f.GetDAH(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.Equal(t, newDAH, dah)

		// Remove DAH by setting it to 0
		err = f.SetDAH(context.Background(), key, fileformat.FileTypeTesting, 0)
		require.NoError(t, err)

		// Verify DAH is removed
		dah, err = f.GetDAH(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.Zero(t, dah)
	})

	t.Run("get DAH for non-existent key", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-dah-nonexistent")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("nonexistent-key-1")

		// Try to get DAH for non-existent key
		_, err = f.GetDAH(context.Background(), key, fileformat.FileTypeTesting)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound))
	})

	t.Run("set DAH for non-existent key", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-dah-set-nonexistent")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("nonexistent-key-2")
		newDAH := uint32(6)

		// Try to set DAH for non-existent key
		err = f.SetDAH(context.Background(), key, fileformat.FileTypeTesting, newDAH)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound))
	})

	t.Run("DAH expiration", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-dah-expiration")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("expiring-key")
		content := []byte("test content")

		// Set content with a short DAH
		shortDAH := uint32(200)
		err = f.Set(context.Background(), key, fileformat.FileTypeTesting, content, options.WithDeleteAt(shortDAH))
		require.NoError(t, err)

		// Verify content exists initially
		exists, err := f.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.True(t, exists)

		// run cleaner - don't wait for cleaner to run automatically
		f.SetCurrentBlockHeight(shortDAH + 1)
		f.cleanupExpiredFiles()

		// Verify content is removed after DAH expiration
		exists, err = f.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.False(t, exists)

		// Verify DAH file is also removed
		filename, err := f.options.ConstructFilename(tempDir, key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		_, err = os.Stat(filename + ".dah")
		require.True(t, os.IsNotExist(err))
	})

	t.Run("update DAH before expiration", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-dah-update-1")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("updating-key-1")
		content := []byte("test content")

		err = f.Set(context.Background(), key, fileformat.FileTypeTesting, content, options.WithDeleteAt(10))
		require.NoError(t, err)

		exists, err := f.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.True(t, exists)

		dah, err := f.GetDAH(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		assert.Equal(t, uint32(10), dah)

		// Update DAH
		newDAH := uint32(100)
		err = f.SetDAH(context.Background(), key, fileformat.FileTypeTesting, newDAH)
		require.NoError(t, err)

		f.SetCurrentBlockHeight(12)
		f.cleanupExpiredFiles()

		exists, err = f.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.True(t, exists)

		f.SetCurrentBlockHeight(100)
		f.cleanupExpiredFiles()

		exists, err = f.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.False(t, exists)
	})
	t.Run("set DAH, delete DAH file, no expiration", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-dah-update-2")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("updating-key-2")
		content := []byte("test content")

		// Set content with initial short DAH
		err = f.Set(context.Background(), key, fileformat.FileTypeTesting, content, options.WithDeleteAt(101))
		require.NoError(t, err)

		f.cleanupExpiredFiles()

		exists, err := f.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.True(t, exists)

		// delete DAH file
		filename, err := f.options.ConstructFilename(tempDir, key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		err = os.Remove(filename + ".dah")
		require.NoError(t, err)

		f.SetCurrentBlockHeight(102)
		f.cleanupExpiredFiles()

		exists, err = f.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.True(t, exists)
	})
	t.Run("set DAH, manually change DAH file, no expiration", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-dah-update-3")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("updating-key-3")
		content := []byte("test content")

		initialDAH := uint32(200)
		err = f.Set(context.Background(), key, fileformat.FileTypeTesting, content, options.WithDeleteAt(initialDAH))
		require.NoError(t, err)

		// change DAH file
		filename, err := f.options.ConstructFilename(tempDir, key, fileformat.FileTypeTesting)
		require.NoError(t, err)

		err = os.WriteFile(filename+".dah", []byte(strconv.FormatUint(uint64(initialDAH+10), 10)), 0o644) // nolint:gosec
		require.NoError(t, err)

		f.SetCurrentBlockHeight(initialDAH + 9)
		f.cleanupExpiredFiles()

		exists, err := f.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.True(t, exists)

		// try again with a past time
		err = os.WriteFile(filename+".dah", []byte(strconv.FormatUint(uint64(initialDAH+5), 10)), 0o644) // nolint:gosec
		require.NoError(t, err)

		f.cleanupExpiredFiles()

		exists, err = f.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.True(t, exists)
	})
}

func TestFileCleanupExpiredFiles(t *testing.T) {
	t.Run("clean expired files with various scenarios", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-clean-expired")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		// Test cases
		tests := []struct {
			key       []byte
			content   []byte
			dah       uint32
			modifyDAH bool // If true, modify DAH file after creation
		}{
			{
				key:     []byte("normal-expiring"),
				content: []byte("normal content"),
				dah:     300,
			},
			{
				key:       []byte("modified-dah"),
				content:   []byte("content with modified dah"),
				dah:       300,
				modifyDAH: true,
			},
		}

		// Set up test files
		for _, tc := range tests {
			err := f.Set(context.Background(), tc.key, fileformat.FileTypeTesting, tc.content, options.WithDeleteAt(tc.dah))
			require.NoError(t, err)

			if tc.modifyDAH {
				err = f.SetDAH(context.Background(), tc.key, fileformat.FileTypeTesting, tc.dah+100)
				require.NoError(t, err)
			}
		}

		// run cleaner - don't wait for cleaner to run automatically
		f.SetCurrentBlockHeight(300)
		f.cleanupExpiredFiles()

		// Verify results
		for _, tc := range tests {
			exists, err := f.Exists(context.Background(), tc.key, fileformat.FileTypeTesting)
			require.NoError(t, err)

			filename, err := f.options.ConstructFilename(tempDir, tc.key, fileformat.FileTypeTesting)
			require.NoError(t, err)

			if tc.modifyDAH {
				// File with modified (future) DAH should still exist
				require.True(t, exists, "file with modified DAH should still exist")

				_, err = os.Stat(filename + ".dah")
				require.NoError(t, err, "DAH file should still exist")
			} else {
				// Normal expired and corrupt DAH files should be removed
				require.False(t, exists, "expired file should be removed")

				_, err = os.Stat(filename + ".dah")
				require.True(t, os.IsNotExist(err), "DAH file should be removed")
			}
		}
	})

	t.Run("concurrent DAH modifications", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-concurrent-dah")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("concurrent-key")
		content := []byte("concurrent content")

		// Set initial content with DAH
		err = f.Set(context.Background(), key, fileformat.FileTypeTesting, content, options.WithDeleteAt(600))
		require.NoError(t, err)

		// Start multiple goroutines to modify DAH
		var wg sync.WaitGroup

		for i := 0; i < 5; i++ {
			wg.Add(1)

			go func(i int) {
				defer wg.Done()
				time.Sleep(time.Duration(i*50) * time.Millisecond)

				// Alternate between extending and shortening DAH
				var dah uint32
				if i%2 == 0 {
					dah = 400
				} else {
					dah = 200
				}

				err := f.SetDAH(context.Background(), key, fileformat.FileTypeTesting, dah)
				require.NoError(t, err)
			}(i)
		}

		wg.Wait()

		time.Sleep(400 * time.Millisecond)

		// run cleaner - don't wait for cleaner to run automatically
		f.SetCurrentBlockHeight(600)
		f.cleanupExpiredFiles()

		// Verify final state
		exists, err := f.Exists(context.Background(), key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		require.False(t, exists, "file should be removed after DAH expiration")

		filename, err := f.options.ConstructFilename(tempDir, key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		_, err = os.Stat(filename + ".dah")
		require.True(t, os.IsNotExist(err), "DAH file should be removed")
	})

	t.Run("cleaner with missing files", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-missing-files")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("missing-file")
		content := []byte("content")

		// Set content with DAH
		err = f.Set(context.Background(), key, fileformat.FileTypeTesting, content, options.WithDeleteAt(800))
		require.NoError(t, err)

		// Manually delete the content file but leave DAH file
		filename, err := f.options.ConstructFilename(tempDir, key, fileformat.FileTypeTesting)
		require.NoError(t, err)
		err = os.Remove(filename)
		require.NoError(t, err)

		// run cleaner - don't wait for cleaner to run automatically
		f.SetCurrentBlockHeight(800)
		f.cleanupExpiredFiles()

		// Verify DAH file is also cleaned up
		_, err = os.Stat(filename + ".dah")
		require.True(t, os.IsNotExist(err), "DAH file should be removed when content file is missing")
	})
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

func TestFileChecksumNotDeletedOnTTLExpiry(t *testing.T) {
	// Get a temporary directory
	tempDir, err := os.MkdirTemp("", "test-checksum-delete-on-ttl-expiry")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	u, err := url.Parse("file://" + tempDir)
	require.NoError(t, err)

	f, err := New(ulogger.TestLogger{}, u)
	require.NoError(t, err)

	key := "test-key-ttl-checksum"
	content := []byte("test content")

	// Put a file with checksum
	err = f.Set(context.Background(), []byte(key), fileformat.FileTypeTesting, content, options.WithDeleteAt(1))
	require.NoError(t, err)

	// Construct filename
	merged := options.MergeOptions(f.options, []options.FileOption{})
	filename, err := merged.ConstructFilename(tempDir, []byte(key), fileformat.FileTypeTesting)
	require.NoError(t, err)

	// Verify the checksum file exists
	_, err = os.Stat(filename)
	require.NoError(t, err, "checksum file should exist")

	// run cleaner - don't wait for cleaner to run automatically
	f.SetCurrentBlockHeight(1000)
	f.cleanupExpiredFiles()

	// Check if the file has expired
	exists, err := f.Exists(context.Background(), []byte(key), fileformat.FileTypeTesting)
	require.NoError(t, err)
	require.False(t, exists, "file should be expired due to TTL")

	// Check if checksum file still exists - this is the bug
	_, err = os.Stat(filename)
	require.True(t, os.IsNotExist(err), "Checksum file should be removed when content file has expired")
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

	t.Run("cleanupExpiredFile removes invalid DAH file but keeps blob", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-cleanup-dah-zero")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		// Create a blob file
		blobFile := filepath.Join(tempDir, "test.tx")
		err = os.WriteFile(blobFile, []byte("blob content"), 0o600)
		require.NoError(t, err)

		// Create a DAH file with value 0
		dahFile := filepath.Join(tempDir, "test.tx.dah")
		err = os.WriteFile(dahFile, []byte("0"), 0o600)
		require.NoError(t, err)

		// Add to in-memory map
		f.fileDAHsMu.Lock()
		f.fileDAHs[blobFile] = 999
		f.fileDAHsMu.Unlock()

		// Run cleanup
		f.cleanupExpiredFile(blobFile)

		// Verify DAH file is removed
		_, err = os.Stat(dahFile)
		require.True(t, os.IsNotExist(err), "DAH file should be removed")

		// Verify blob file still exists
		_, err = os.Stat(blobFile)
		require.NoError(t, err, "Blob file should still exist")

		// Verify entry is removed from map
		f.fileDAHsMu.Lock()
		_, exists := f.fileDAHs[blobFile]
		f.fileDAHsMu.Unlock()
		require.False(t, exists, "Entry should be removed from map")
	})

	t.Run("loadDAHs handles invalid DAH files", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-load-dah-invalid")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		// Create some DAH files
		validDAHFile := filepath.Join(tempDir, "valid.tx.dah")
		err = os.WriteFile(validDAHFile, []byte("100"), 0o600)
		require.NoError(t, err)

		invalidDAHFile1 := filepath.Join(tempDir, "invalid1.tx.dah")
		err = os.WriteFile(invalidDAHFile1, []byte("0"), 0o600)
		require.NoError(t, err)

		invalidDAHFile2 := filepath.Join(tempDir, "invalid2.tx.dah")
		err = os.WriteFile(invalidDAHFile2, []byte(""), 0o600)
		require.NoError(t, err)

		// Create File struct directly to test loadDAHs without the New() function's
		// fileCleanerOnce mechanism, which can prevent loadDAHs from running if another
		// test has already registered a URL (package-level sync.Map is never cleared)
		f := &File{
			path:     tempDir,
			logger:   ulogger.TestLogger{},
			fileDAHs: make(map[string]uint32),
		}

		// Call loadDAHs directly
		err = f.loadDAHs()
		require.NoError(t, err)

		// Verify valid DAH is loaded
		validDAHKey := filepath.Join(tempDir, "valid.tx")
		f.fileDAHsMu.Lock()
		validDAH, exists := f.fileDAHs[validDAHKey]
		f.fileDAHsMu.Unlock()
		require.True(t, exists, "Valid DAH should be loaded")
		require.Equal(t, uint32(100), validDAH)

		// Verify invalid DAH file 1 is removed
		_, err = os.Stat(invalidDAHFile1)
		require.True(t, os.IsNotExist(err), "Invalid DAH file 1 should be removed")

		// Verify invalid DAH file 2 is removed
		_, err = os.Stat(invalidDAHFile2)
		require.True(t, os.IsNotExist(err), "Invalid DAH file 2 should be removed")

		// Verify invalid DAHs are not in map
		f.fileDAHsMu.Lock()
		_, exists1 := f.fileDAHs[filepath.Join(tempDir, "invalid1.tx")]
		_, exists2 := f.fileDAHs[filepath.Join(tempDir, "invalid2.tx")]
		f.fileDAHsMu.Unlock()
		require.False(t, exists1, "Invalid DAH 1 should not be in map")
		require.False(t, exists2, "Invalid DAH 2 should not be in map")
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
