package file

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/libsv/go-bt"
	"github.com/ordishs/go-utils"
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

		err = f.Set(context.Background(), []byte("key"), []byte("value"))
		require.NoError(t, err)

		value, err := f.Get(context.Background(), []byte("key"))
		require.NoError(t, err)

		require.Equal(t, []byte("value"), value)

		err = f.Del(context.Background(), []byte("key"))
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

	err = f.Set(ctx, []byte("key"), []byte("value"))
	require.NoError(t, err)

	value, err := f.Get(ctx, []byte("key"))
	require.NoError(t, err)

	require.Equal(t, []byte("value"), value)

	err = f.Del(ctx, []byte("key"))
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

func TestFileLoadTTLs(t *testing.T) {
	t.Run("load TTLs", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		key := []byte("key")
		value := []byte("value")

		ttl := 100 * time.Millisecond
		ttlInterval := 10 * time.Millisecond

		ctx := context.Background()

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := newStore(ulogger.TestLogger{}, u, ttlInterval)
		require.NoError(t, err)

		err = f.Set(ctx, key, value, options.WithTTL(ttl))
		require.NoError(t, err)

		err = f.SetTTL(ctx, key, ttl)
		require.NoError(t, err)

		var fileTTLs map[string]time.Time

		f.fileTTLsMu.Lock()
		fileTTLs = f.fileTTLs
		require.Contains(t, fileTTLs, filepath.Join(tempDir, utils.ReverseAndHexEncodeSlice(key)))
		f.fileTTLsMu.Unlock()

		time.Sleep(ttl * 2)

		f.fileTTLsMu.Lock()
		fileTTLs = f.fileTTLs

		require.NoError(t, err)
		require.NotContains(t, fileTTLs, filepath.Join(tempDir, utils.ReverseAndHexEncodeSlice(key)))
		f.fileTTLsMu.Unlock()

		err = f.Del(ctx, key)
		require.Error(t, err)
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

				err := f.Set(context.Background(), key, value)
				require.NoError(t, err)

				retrievedValue, err := f.Get(context.Background(), key)
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

		err = f.Set(context.Background(), key, value)
		require.NoError(t, err)

		// Construct the expected file path in the subdirectory
		expectedDir := filepath.Join(tempDir, subDir)
		expectedFilePath := filepath.Join(expectedDir, utils.ReverseAndHexEncodeSlice(key))

		// Check if the file was NOT created in the subdirectory (it is supposed to be ignored)
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

		err = f.Set(context.Background(), key, value, options.WithFilename("filename"))
		require.NoError(t, err)

		// Construct the expected file path in the subdirectory
		expectedDir := filepath.Join(tempDir, subDir)
		expectedFilePath := filepath.Join(expectedDir, "filename")

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

		header := "This is the header"

		f, err := New(ulogger.TestLogger{}, u, options.WithHeader([]byte(header)))
		require.NoError(t, err)

		key := []byte("key-with-header")
		content := "This is the main content"

		// Test setting content with header using Set
		err = f.Set(context.Background(), key, []byte(content))
		require.NoError(t, err)

		// Verify content using Get
		value, err := f.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, content, string(value))

		// Verify content using GetIoReader
		reader, err := f.GetIoReader(context.Background(), key)
		require.NoError(t, err)

		// Read all the content from the reader
		readContent, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, content, string(readContent))

		// delete the file
		err = f.Del(context.Background(), key)
		require.NoError(t, err)

		// Test setting content with header using SetFromReader
		newContent := "New content from reader"

		contentReader := strings.NewReader(newContent)
		readCloser := readCloser{Reader: contentReader}

		err = f.SetFromReader(context.Background(), key, readCloser)
		require.NoError(t, err)

		// Verify new content using Get
		value, err = f.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, newContent, string(value))

		// Verify new content using GetIoReader
		reader, err = f.GetIoReader(context.Background(), key)
		require.NoError(t, err)

		readContent, err = io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, newContent, string(readContent))

		// delete the file
		err = f.Del(context.Background(), key)
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

		footerStr := "This is the footer"
		f, err := New(ulogger.TestLogger{}, u, options.WithFooter(options.NewFooter(len(footerStr), []byte(footerStr), nil)))
		require.NoError(t, err)

		key := []byte("key-with-footer")
		content := "This is the main content"

		// Test setting content with footer using Set
		err = f.Set(context.Background(), key, []byte(content))
		require.NoError(t, err)

		// Verify content using Get
		value, err := f.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, content, string(value))

		// Verify content using GetIoReader
		reader, err := f.GetIoReader(context.Background(), key)
		require.NoError(t, err)

		// Read all the content from the reader
		readContent, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, content, string(readContent))

		// delete the file
		err = f.Del(context.Background(), key)
		require.NoError(t, err)

		// Test setting content with footer using SetFromReader
		newContent := "New content from reader"
		contentReader := strings.NewReader(newContent)
		readCloser := readCloser{Reader: contentReader}

		err = f.SetFromReader(context.Background(), key, readCloser)
		require.NoError(t, err)

		// Verify new content using Get
		value, err = f.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, newContent, string(value))

		// Verify new content using GetIoReader
		reader, err = f.GetIoReader(context.Background(), key)
		require.NoError(t, err)

		readContent, err = io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, newContent, string(readContent))

		// delete the file
		err = f.Del(context.Background(), key)
		require.NoError(t, err)
	})
}

// A simple wrapper to add the Close method to a Reader
type readCloser struct {
	io.Reader
}

func (rc readCloser) Close() error {
	return nil
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
		readCloser := readCloser{Reader: reader}

		err = f.SetFromReader(context.Background(), key, readCloser)
		require.NoError(t, err)

		// Verify the content was correctly stored
		storedReader, err := f.GetIoReader(context.Background(), key)
		require.NoError(t, err)

		// Read all the content from the storedReader
		storedContent, err := io.ReadAll(storedReader)
		require.NoError(t, err)
		require.Equal(t, content, string(storedContent))
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
		readCloser := readCloser{Reader: reader}

		// First, set the content
		err = f.SetFromReader(context.Background(), key, readCloser)
		require.NoError(t, err)

		// Get metadata using GetHead
		head, err := f.GetHead(context.Background(), key, 1)
		require.NoError(t, err)
		require.NotNil(t, head)
		require.Equal(t, content[:1], string(head), "head content doesn't match")
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
		exists, err := f.Exists(context.Background(), key)
		require.NoError(t, err)
		require.False(t, exists)

		// Wrap the reader to satisfy the io.ReadCloser interface
		readCloser := readCloser{Reader: reader}

		// Set the content
		err = f.SetFromReader(context.Background(), key, readCloser)
		require.NoError(t, err)

		// Now content should exist
		exists, err = f.Exists(context.Background(), key)
		require.NoError(t, err)
		require.True(t, exists)
	})
}

func TestFileSetWithHashPrefix(t *testing.T) {
	u, err := url.Parse("file:///data/subtreestore?hashPrefix=2")
	require.NoError(t, err)
	require.Equal(t, "/data/subtreestore", u.Path)
	require.Equal(t, "2", u.Query().Get("hashPrefix"))

	u, err = url.Parse("null:///?localTTLStore=file&localTTLStorePath=./data/subtreestore-ttl?hashPrefix=2")
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
		nonExistentPath := "./path/that/does/not/exist"
		u, err := url.Parse("file://" + nonExistentPath)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		// creating a New store will create the folder
		// so we need to remove it before we test
		err = os.RemoveAll(nonExistentPath)
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
		err = os.Chmod(tempDir, 0555)
		require.NoError(t, err)

		// nolint:errcheck
		defer os.Chmod(tempDir, 0755) // Restore permissions for cleanup

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
		err = os.Chmod(tempDir, 0555)
		require.NoError(t, err)

		// nolint:errcheck
		defer os.Chmod(tempDir, 0755) // Restore permissions for cleanup

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
		err = f.Set(context.Background(), key, []byte(content))
		require.NoError(t, err)

		// Read raw file to verify header and footer
		filename, err := f.options.ConstructFilename(tempDir, key)
		require.NoError(t, err)

		rawData, err := os.ReadFile(filename)
		require.NoError(t, err)

		// Verify header and footer are present in raw data
		expectedData := append([]byte("START"), []byte(content)...)
		expectedData = append(expectedData, []byte("END")...)
		require.Equal(t, expectedData, rawData)

		// Test Get - should return content without header/footer
		value, err := f.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, content, string(value))

		// Test GetIoReader - should return content without header/footer
		reader, err := f.GetIoReader(context.Background(), key)
		require.NoError(t, err)
		defer reader.Close()

		readContent, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, content, string(readContent))
	})
}

func TestWithSHA256Checksum(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "file_store_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	logger := ulogger.TestLogger{}

	tests := []struct {
		name           string
		storeURL       string
		opts           []options.StoreOption
		data           []byte
		key            []byte
		extension      string
		expectSHA256   bool
		expectedFormat string
	}{
		{
			name:         "SHA256 enabled via option",
			storeURL:     "file://" + tempDir,
			opts:         []options.StoreOption{options.WithSHA256Checksum()},
			data:         []byte("test data"),
			key:          []byte("testkey123"),
			extension:    "txt",
			expectSHA256: true,
		},
		{
			name:         "SHA256 enabled via URL",
			storeURL:     "file://" + tempDir + "?checksum=true",
			opts:         nil,
			data:         []byte("test data"),
			key:          []byte("testkey456"),
			extension:    "txt",
			expectSHA256: true,
		},
		{
			name:         "SHA256 disabled (default)",
			storeURL:     "file://" + tempDir,
			opts:         nil,
			data:         []byte("test data"),
			key:          []byte("testkey789"),
			extension:    "txt",
			expectSHA256: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse URL
			u, err := url.Parse(tt.storeURL)
			require.NoError(t, err)

			// Create file store
			store, err := New(logger, u, tt.opts...)
			require.NoError(t, err)

			// Set data with extension
			err = store.Set(context.Background(), tt.key, tt.data, options.WithFileExtension(tt.extension))
			require.NoError(t, err)

			// Construct expected filename
			merged := options.MergeOptions(store.options, []options.FileOption{options.WithFileExtension(tt.extension)})
			filename, err := merged.ConstructFilename(tempDir, tt.key)
			require.NoError(t, err)

			// Verify main file exists and contains correct data
			data, err := os.ReadFile(filename)
			require.NoError(t, err)
			require.Equal(t, tt.data, data)

			// Check SHA256 file
			sha256Filename := filename + ".sha256"
			_, err = os.Stat(sha256Filename)

			if tt.expectSHA256 {
				require.NoError(t, err, "SHA256 file should exist")

				// Read and verify SHA256 file content
				hashFileContent, err := os.ReadFile(sha256Filename)
				require.NoError(t, err)

				// Calculate expected hash
				hasher := sha256.New()
				hasher.Write(tt.data)
				expectedHash := fmt.Sprintf("%x", hasher.Sum(nil))

				// Verify hash file format
				hashFileStr := string(hashFileContent)
				parts := strings.Fields(hashFileStr)
				require.Len(t, parts, 2, "Hash file should have hash and filename separated by two spaces")

				// Verify hash matches
				require.Equal(t, expectedHash, parts[0], "Hash in file should match calculated hash")

				// Verify filename part
				expectedFilename := fmt.Sprintf("%x.%s", bt.ReverseBytes(tt.key), tt.extension)
				require.Equal(t, expectedFilename, parts[1], "Filename in hash file should match expected format")
			} else {
				require.True(t, os.IsNotExist(err), "SHA256 file should not exist")
			}
		})
	}
}

func TestSetFromReaderWithSHA256(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "file_store_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	logger := ulogger.TestLogger{}
	u, err := url.Parse("file://" + tempDir)
	require.NoError(t, err)

	store, err := New(logger, u, options.WithSHA256Checksum())
	require.NoError(t, err)

	// Test data
	testData := []byte("test data for SetFromReader")
	key := []byte("testreaderkey")
	extension := "txt"

	// Create reader
	reader := io.NopCloser(bytes.NewReader(testData))

	// Set data
	err = store.SetFromReader(context.Background(), key, reader, options.WithFileExtension(extension))
	require.NoError(t, err)

	// Construct filename
	merged := options.MergeOptions(store.options, []options.FileOption{options.WithFileExtension(extension)})
	filename, err := merged.ConstructFilename(tempDir, key)
	require.NoError(t, err)

	// Verify main file
	data, err := os.ReadFile(filename)
	require.NoError(t, err)
	require.Equal(t, testData, data)

	// Verify SHA256 file
	sha256Filename := filename + ".sha256"
	hashFileContent, err := os.ReadFile(sha256Filename)
	require.NoError(t, err)

	// Calculate expected hash
	hasher := sha256.New()
	hasher.Write(testData)
	expectedHash := fmt.Sprintf("%x", hasher.Sum(nil))

	// Verify hash file format
	hashFileStr := string(hashFileContent)
	parts := strings.Fields(hashFileStr)
	require.Len(t, parts, 2)
	require.Equal(t, expectedHash, parts[0])

	// Verify filename part
	expectedFilename := fmt.Sprintf("%x.%s", bt.ReverseBytes(key), extension)
	require.Equal(t, expectedFilename, parts[1])
}

func TestSHA256WithHeaderFooter(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "file_store_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	logger := ulogger.TestLogger{}
	u, err := url.Parse("file://" + tempDir)
	require.NoError(t, err)

	header := []byte("header-")
	footerBytes := []byte("-footer")

	store, err := New(logger, u,
		options.WithSHA256Checksum(),
		options.WithHeader(header),
		options.WithFooter(options.NewFooter(len(footerBytes), footerBytes, nil)))
	require.NoError(t, err)

	// Test data
	data := []byte("test data")
	key := []byte("testheaderfooter")
	extension := "txt"

	// Set data
	err = store.Set(context.Background(), key, data, options.WithFileExtension(extension))
	require.NoError(t, err)

	// Construct filename
	merged := options.MergeOptions(store.options, []options.FileOption{options.WithFileExtension(extension)})
	filename, err := merged.ConstructFilename(tempDir, key)
	require.NoError(t, err)

	// Verify main file includes header and footer
	fileContent, err := os.ReadFile(filename)
	require.NoError(t, err)

	expectedContent := append(append(header, data...), footerBytes...)
	require.Equal(t, expectedContent, fileContent)

	// Verify SHA256 file
	sha256Filename := filename + ".sha256"
	hashFileContent, err := os.ReadFile(sha256Filename)
	require.NoError(t, err)

	// Calculate expected hash (including header and footer)
	hasher := sha256.New()
	hasher.Write(expectedContent)
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

	eofMarkerStr := "footer"
	q.Set("eofmarker", eofMarkerStr)
	storeURL.RawQuery = q.Encode()

	store, err := New(logger, storeURL)
	require.NoError(t, err)

	// Test data
	key := []byte("test")
	data := []byte("test data")
	reader := io.NopCloser(bytes.NewReader(data))

	// Set data
	err = store.SetFromReader(context.Background(), key, reader)
	require.NoError(t, err)

	// Read file directly
	filename := filepath.Join(dir, hex.EncodeToString(bt.ReverseBytes(key)))
	content, err := os.ReadFile(filename)
	require.NoError(t, err)

	// Verify content includes header and footer
	expected := append(append([]byte("header"), data...), []byte("footer")...)
	require.Equal(t, expected, content)
}

func TestFile_Set_WithHeaderAndFooter(t *testing.T) {
	dir := t.TempDir()

	store, err := New(
		ulogger.TestLogger{},
		&url.URL{Path: dir},
		options.WithHeader([]byte("header")),
		options.WithFooter(options.NewFooter(len([]byte("footer")), []byte("footer"), nil)))
	require.NoError(t, err)

	// Test data
	key := []byte("test")
	data := []byte("test data")

	// Set data
	err = store.Set(context.Background(), key, data)
	require.NoError(t, err)

	// Read file directly
	filename := filepath.Join(dir, hex.EncodeToString(bt.ReverseBytes(key)))
	content, err := os.ReadFile(filename)
	require.NoError(t, err)

	// Verify content includes header and footer
	expected := append(append([]byte("header"), data...), []byte("footer")...)
	require.Equal(t, expected, content)
}

func TestFile_Get_WithHeaderAndFooter(t *testing.T) {
	dir := t.TempDir()

	eofMarkerBytes := []byte("footer")
	store, err := New(
		ulogger.TestLogger{},
		&url.URL{Path: dir},
		options.WithHeader([]byte("header")),
		options.WithFooter(options.NewFooter(len(eofMarkerBytes), eofMarkerBytes, nil)))
	require.NoError(t, err)

	// Test data
	key := []byte("test")
	data := []byte("test data")

	// Set data
	err = store.Set(context.Background(), key, data)
	require.NoError(t, err)

	// Get data
	retrieved, err := store.Get(context.Background(), key)
	require.NoError(t, err)

	// Verify retrieved data matches original (without header/footer)
	require.Equal(t, data, retrieved)
}

func TestFile_GetIoReader_WithHeaderAndFooter(t *testing.T) {
	dir := t.TempDir()

	footerBytes := []byte("footer")
	store, err := New(
		ulogger.TestLogger{},
		&url.URL{Path: dir},
		options.WithHeader([]byte("header")),
		options.WithFooter(options.NewFooter(len(footerBytes), footerBytes, nil)))
	require.NoError(t, err)

	// Test data
	key := []byte("test")
	data := []byte("test data")

	// Set data
	err = store.Set(context.Background(), key, data)
	require.NoError(t, err)

	// Get reader
	reader, err := store.GetIoReader(context.Background(), key)
	require.NoError(t, err)
	defer reader.Close()

	// Read data from reader
	retrieved, err := io.ReadAll(reader)
	require.NoError(t, err)

	// Verify retrieved data matches original (without header/footer)
	require.Equal(t, data, retrieved)
}

func TestFileGetMetaData(t *testing.T) {
	t.Run("get metadata for existing file", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test-metadata")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		eofMarker := []byte("eof marker")
		metaData := []byte("meta data")

		f, err := New(ulogger.TestLogger{}, u, options.WithFooter(options.NewFooter(len(eofMarker)+len(metaData), eofMarker, func() []byte {
			return metaData
		})))
		require.NoError(t, err)

		key := []byte("metadata-test-key")
		content := []byte("test content for metadata")

		// Set content
		err = f.Set(context.Background(), key, content)
		require.NoError(t, err)

		// Get metadata
		metaBytes, err := f.GetFooterMetaData(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, metaData, metaBytes)
	})

	t.Run("get metadata for non-existent file", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-metadata-nonexistent")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u, options.WithFooter(options.NewFooter(len([]byte("footer")), []byte("eof marker"), nil)))
		require.NoError(t, err)

		key := []byte("nonexistent-key")

		// Try to get metadata for non-existent file
		_, err = f.GetFooterMetaData(context.Background(), key)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound), "error should indicate file does not exist")
	})

	t.Run("get metadata with header and footer", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-metadata-header-footer")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		header := []byte("header-")
		eofMarker := []byte("-eof marker")
		metaData := []byte("meta data")

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u,
			options.WithHeader(header),
			options.WithFooter(options.NewFooter(len(eofMarker)+len(metaData), eofMarker, func() []byte {
				return metaData
			})))
		require.NoError(t, err)

		key := []byte("metadata-test-key-hf")
		content := []byte("test content for metadata")

		// Set content
		err = f.Set(context.Background(), key, content)
		require.NoError(t, err)

		// Get metadata
		metaBytes, err := f.GetFooterMetaData(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, metaData, metaBytes)

		// Verify actual file size on disk includes header and footer
		filename, err := f.options.ConstructFilename(tempDir, key)
		require.NoError(t, err)

		fileInfo, err := os.Stat(filename)
		require.NoError(t, err)
		require.Equal(t, int64(len(header)+len(content)+len(eofMarker)+len(metaData)), fileInfo.Size(),
			"actual file size should include header and footer")
	})

	t.Run("get metadata for file with content that include exact eof marker", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test-metadata-eof")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		eofMarker := []byte("EOF_MARKER")
		metaData := []byte("meta data")

		f, err := New(ulogger.TestLogger{}, u, options.WithFooter(options.NewFooter(len(eofMarker)+len(metaData), eofMarker, func() []byte {
			return metaData
		})))
		require.NoError(t, err)

		key := []byte("metadata-test-key-eof")
		// Create content that includes the exact EOF marker
		content := []byte("This is some content with EOF_MARKER in the middle and more content after")

		// Set content
		err = f.Set(context.Background(), key, content)
		require.NoError(t, err)

		// Get metadata
		metaBytes, err := f.GetFooterMetaData(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, metaData, metaBytes, "metadata should be correctly extracted even when content contains EOF marker")

		// Verify the content is still retrievable
		retrievedContent, err := f.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, content, retrievedContent, "content should be retrieved correctly even with EOF marker in it")
	})

	t.Run("get metadata for file with content that include exact eof marker", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test-metadata-eof")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		eofMarker := []byte("EOF_MARKER")
		metaData := []byte("meta data")

		f, err := New(ulogger.TestLogger{}, u, options.WithFooter(options.NewFooter(len(eofMarker)+len(metaData), eofMarker, func() []byte {
			return metaData
		})))
		require.NoError(t, err)

		key := []byte("metadata-test-key-eof")
		// Create content that includes the exact EOF marker
		content := []byte("This is some content with market at then end of the content")
		content = append(content, eofMarker...)

		// Set content
		err = f.Set(context.Background(), key, content)
		require.NoError(t, err)

		// Get metadata
		metaBytes, err := f.GetFooterMetaData(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, metaData, metaBytes, "metadata should be correctly extracted even when content contains EOF marker")

		// Verify the content is still retrievable
		retrievedContent, err := f.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, content, retrievedContent, "content should be retrieved correctly even with EOF marker in it")
	})

	t.Run("get metadata for file with content that include exact eof marker", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test-metadata-eof")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		eofMarker := []byte("EOF_MARKER")
		metaData := []byte("meta data")

		f, err := New(ulogger.TestLogger{}, u, options.WithFooter(options.NewFooter(len(eofMarker)+len(metaData), eofMarker, func() []byte {
			return metaData
		})))
		require.NoError(t, err)

		key := []byte("metadata-test-key-eof")
		// Create content that includes what looks like a marker + meta data at the end of the content
		content := []byte("This is some content with marker + meta data at then end of the content")
		content = append(content, eofMarker...)
		content = append(content, metaData...)

		// Set content
		err = f.Set(context.Background(), key, content)
		require.NoError(t, err)

		// Get metadata
		metaBytes, err := f.GetFooterMetaData(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, metaData, metaBytes, "metadata should be correctly extracted even when content contains EOF marker")

		// Verify the content is still retrievable
		retrievedContent, err := f.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, content, retrievedContent, "content should be retrieved correctly even with EOF marker in it")
	})
}

func TestFileGetHeader(t *testing.T) {
	t.Run("get header from file with header", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test-header")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		header := []byte("custom-header-")
		f, err := New(ulogger.TestLogger{}, u, options.WithHeader(header))
		require.NoError(t, err)

		key := []byte("header-test-key")
		content := []byte("test content")

		// Set content
		err = f.Set(context.Background(), key, content)
		require.NoError(t, err)

		// Get header
		retrievedHeader, err := f.GetHeader(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, header, retrievedHeader, "retrieved header should match original header")

		// Verify the content is still retrievable and correct
		retrievedContent, err := f.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, content, retrievedContent, "content should be retrieved correctly")
	})

	t.Run("get header from file without header", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-no-header")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		// Create store without header option
		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("no-header-key")
		content := []byte("test content")

		err = f.Set(context.Background(), key, content)
		require.NoError(t, err)

		// Get header should return empty bytes
		header, err := f.GetHeader(context.Background(), key)
		require.NoError(t, err)
		require.Empty(t, header, "header should be empty for file without header")
	})

	t.Run("get header from non-existent file", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-nonexistent")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u, options.WithHeader([]byte("header-")))
		require.NoError(t, err)

		key := []byte("nonexistent-key")

		// Try to get header from non-existent file
		_, err = f.GetHeader(context.Background(), key)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound), "error should indicate file does not exist")
	})

	t.Run("get header with both header and footer", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-header-footer")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		header := []byte("header-")
		footer := []byte("-footer")

		f, err := New(ulogger.TestLogger{}, u,
			options.WithHeader(header),
			options.WithFooter(options.NewFooter(len(footer), footer, nil)))
		require.NoError(t, err)

		key := []byte("header-footer-key")
		content := []byte("test content")

		// Set content
		err = f.Set(context.Background(), key, content)
		require.NoError(t, err)

		// Get header
		retrievedHeader, err := f.GetHeader(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, header, retrievedHeader, "retrieved header should match original header")

		// Verify file on disk contains header, content, and footer
		filename, err := f.options.ConstructFilename(tempDir, key)
		require.NoError(t, err)

		fileContent, err := os.ReadFile(filename)
		require.NoError(t, err)

		expectedFileContent := append(append(header, content...), footer...)
		require.Equal(t, expectedFileContent, fileContent, "file should contain header, content, and footer")
	})
}

func TestFileGetAndSetTTL(t *testing.T) {
	t.Run("get and set TTL", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test-ttl")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("ttl-test-key")
		content := []byte("test content")

		// Set initial content without TTL
		err = f.Set(context.Background(), key, content)
		require.NoError(t, err)

		// Initially there should be no TTL
		ttl, err := f.GetTTL(context.Background(), key)
		require.NoError(t, err)
		require.Zero(t, ttl)

		// Set a TTL
		newTTL := 1 * time.Hour
		err = f.SetTTL(context.Background(), key, newTTL)
		require.NoError(t, err)

		// Get and verify TTL
		ttl, err = f.GetTTL(context.Background(), key)
		require.NoError(t, err)
		require.Greater(t, ttl, 59*time.Minute) // Allow for slight timing differences
		require.LessOrEqual(t, ttl, newTTL)

		// Remove TTL by setting it to 0
		err = f.SetTTL(context.Background(), key, 0)
		require.NoError(t, err)

		// Verify TTL is removed
		ttl, err = f.GetTTL(context.Background(), key)
		require.NoError(t, err)
		require.Zero(t, ttl)
	})

	t.Run("get TTL for non-existent key", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-ttl-nonexistent")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("nonexistent-key")

		// Try to get TTL for non-existent key
		_, err = f.GetTTL(context.Background(), key)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound))
	})

	t.Run("set TTL for non-existent key", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-ttl-set-nonexistent")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("nonexistent-key")
		newTTL := 1 * time.Hour

		// Try to set TTL for non-existent key
		err = f.SetTTL(context.Background(), key, newTTL)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound))
	})

	t.Run("TTL expiration", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-ttl-expiration")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("expiring-key")
		content := []byte("test content")

		// Set content with a short TTL
		shortTTL := 200 * time.Millisecond
		err = f.Set(context.Background(), key, content, options.WithTTL(shortTTL))
		require.NoError(t, err)

		// Verify content exists initially
		exists, err := f.Exists(context.Background(), key)
		require.NoError(t, err)
		require.True(t, exists)

		// Wait for TTL to expire and cleaner to run
		time.Sleep(shortTTL)

		// run cleaner - don't wait for cleaner to run automatically
		cleanExpiredFiles(f)

		// Verify content is removed after TTL expiration
		exists, err = f.Exists(context.Background(), key)
		require.NoError(t, err)
		require.False(t, exists)

		// Verify TTL file is also removed
		filename, err := f.options.ConstructFilename(tempDir, key)
		require.NoError(t, err)
		_, err = os.Stat(filename + ".ttl")
		require.True(t, os.IsNotExist(err))
	})

	t.Run("update TTL before expiration", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-ttl-update")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("updating-key")
		content := []byte("test content")

		// Set content with initial short TTL
		initialTTL := 300 * time.Millisecond
		err = f.Set(context.Background(), key, content, options.WithTTL(initialTTL))
		require.NoError(t, err)

		// no sleep, run clean and check file is still there

		// run cleaner - don't wait for cleaner to run automatically
		cleanExpiredFiles(f)
		exists, err := f.Exists(context.Background(), key)
		require.NoError(t, err)
		require.True(t, exists)

		// Wait for some time but not until expiration
		time.Sleep(100 * time.Millisecond)

		// Update TTL
		newTTL := 400 * time.Millisecond
		err = f.SetTTL(context.Background(), key, newTTL)
		require.NoError(t, err)

		// Verify content still exists after original TTL would have expired
		time.Sleep(initialTTL - (100 * time.Millisecond))

		// run cleaner - don't wait for cleaner to run automatically
		cleanExpiredFiles(f)

		exists, err = f.Exists(context.Background(), key)
		require.NoError(t, err)
		require.True(t, exists)

		// Wait for new TTL to expire
		time.Sleep(newTTL)

		// run cleaner - don't wait for cleaner to run automatically
		cleanExpiredFiles(f)

		exists, err = f.Exists(context.Background(), key)
		require.NoError(t, err)
		require.False(t, exists)
	})
	t.Run("set TTL, delete TTL file, no expiration", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-ttl-update")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("updating-key")
		content := []byte("test content")

		// Set content with initial short TTL
		initialTTL := 300 * time.Millisecond
		err = f.Set(context.Background(), key, content, options.WithTTL(initialTTL))
		require.NoError(t, err)

		// no sleep, run clean and check file is still there

		// run cleaner - don't wait for cleaner to run automatically
		cleanExpiredFiles(f)
		exists, err := f.Exists(context.Background(), key)
		require.NoError(t, err)
		require.True(t, exists)

		// delete ttl file
		filename, err := f.options.ConstructFilename(tempDir, key)
		require.NoError(t, err)
		err = os.Remove(filename + ".ttl")
		require.NoError(t, err)

		// Wait till expiration due
		time.Sleep(300 * time.Millisecond)

		// run cleaner - don't wait for cleaner to run automatically
		cleanExpiredFiles(f)

		exists, err = f.Exists(context.Background(), key)
		require.NoError(t, err)
		require.True(t, exists)
	})
	t.Run("set TTL, manually change ttl file, no expiration", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-ttl-update")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("updating-key")
		content := []byte("test content")

		// Set content with initial short TTL
		initialTTL := 300 * time.Millisecond
		err = f.Set(context.Background(), key, content, options.WithTTL(initialTTL))
		require.NoError(t, err)

		// change ttl file
		filename, err := f.options.ConstructFilename(tempDir, key)
		require.NoError(t, err)

		// try with a future time
		ttl := time.Now().Add(+1 * time.Hour).Format(time.RFC3339)

		err = os.WriteFile(filename+".ttl", []byte(ttl), 0644) // nolint:gosec
		require.NoError(t, err)

		time.Sleep(initialTTL * 2)

		// run cleaner - don't wait for cleaner to run automatically
		cleanExpiredFiles(f)

		exists, err := f.Exists(context.Background(), key)
		require.NoError(t, err)
		require.True(t, exists)

		// try again with a past time
		ttl = time.Now().Add(-1 * time.Hour).Format(time.RFC3339)

		err = os.WriteFile(filename+".ttl", []byte(ttl), 0644) // nolint:gosec
		require.NoError(t, err)

		time.Sleep(initialTTL * 2)

		// run cleaner - don't wait for cleaner to run automatically
		cleanExpiredFiles(f)

		exists, err = f.Exists(context.Background(), key)
		require.NoError(t, err)
		require.True(t, exists)
	})
}

func TestFileCleanExpiredFiles(t *testing.T) {
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
			ttl       time.Duration
			modifyTTL bool // If true, modify TTL file after creation
		}{
			{
				key:     []byte("normal-expiring"),
				content: []byte("normal content"),
				ttl:     200 * time.Millisecond,
			},
			{
				key:       []byte("modified-ttl"),
				content:   []byte("content with modified ttl"),
				ttl:       200 * time.Millisecond,
				modifyTTL: true,
			},
		}

		// Set up test files
		for _, tc := range tests {
			err := f.Set(context.Background(), tc.key, tc.content, options.WithTTL(tc.ttl))
			require.NoError(t, err)

			if tc.modifyTTL {
				err = f.SetTTL(context.Background(), tc.key, 1*time.Hour)
				require.NoError(t, err)
			}

			time.Sleep(tc.ttl)
		}

		// run cleaner - don't wait for cleaner to run automatically
		cleanExpiredFiles(f)

		// Verify results
		for _, tc := range tests {
			exists, err := f.Exists(context.Background(), tc.key)
			require.NoError(t, err)

			filename, err := f.options.ConstructFilename(tempDir, tc.key)
			require.NoError(t, err)

			if tc.modifyTTL {
				// File with modified (future) TTL should still exist
				require.True(t, exists, "file with modified TTL should still exist")

				_, err = os.Stat(filename + ".ttl")
				require.NoError(t, err, "TTL file should still exist")
			} else {
				// Normal expired and corrupt TTL files should be removed
				require.False(t, exists, "expired file should be removed")

				_, err = os.Stat(filename + ".ttl")
				require.True(t, os.IsNotExist(err), "TTL file should be removed")
			}
		}
	})

	t.Run("concurrent TTL modifications", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "test-concurrent-ttl")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		u, err := url.Parse("file://" + tempDir)
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		key := []byte("concurrent-key")
		content := []byte("concurrent content")

		// Set initial content with TTL
		err = f.Set(context.Background(), key, content, options.WithTTL(500*time.Millisecond))
		require.NoError(t, err)

		// Start multiple goroutines to modify TTL
		var wg sync.WaitGroup

		for i := 0; i < 5; i++ {
			wg.Add(1)

			go func(i int) {
				defer wg.Done()
				time.Sleep(time.Duration(i*50) * time.Millisecond)

				// Alternate between extending and shortening TTL
				var ttl time.Duration
				if i%2 == 0 {
					ttl = 400 * time.Millisecond
				} else {
					ttl = 200 * time.Millisecond
				}

				err := f.SetTTL(context.Background(), key, ttl)
				require.NoError(t, err)
			}(i)
		}

		wg.Wait()

		time.Sleep(400 * time.Millisecond)

		// run cleaner - don't wait for cleaner to run automatically
		cleanExpiredFiles(f)

		// Verify final state
		exists, err := f.Exists(context.Background(), key)
		require.NoError(t, err)
		require.False(t, exists, "file should be removed after TTL expiration")

		filename, err := f.options.ConstructFilename(tempDir, key)
		require.NoError(t, err)
		_, err = os.Stat(filename + ".ttl")
		require.True(t, os.IsNotExist(err), "TTL file should be removed")
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

		// Set content with TTL
		err = f.Set(context.Background(), key, content, options.WithTTL(200*time.Millisecond))
		require.NoError(t, err)

		// Manually delete the content file but leave TTL file
		filename, err := f.options.ConstructFilename(tempDir, key)
		require.NoError(t, err)
		err = os.Remove(filename)
		require.NoError(t, err)

		time.Sleep(200 * time.Millisecond)
		// run cleaner - don't wait for cleaner to run automatically
		cleanExpiredFiles(f)

		// Verify TTL file is also cleaned up
		_, err = os.Stat(filename + ".ttl")
		require.True(t, os.IsNotExist(err), "TTL file should be removed when content file is missing")
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

	t.Run("ttlCleanerInterval from URL", func(t *testing.T) {
		// Create a temporary directory
		tempDir, err := os.MkdirTemp("", "test-ttl-cleaner")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		// Set up test file with TTL
		key := []byte("test-key")
		content := []byte("test content")
		ttl := 200 * time.Millisecond

		// Create URL with custom ttlCleanerInterval
		u, err := url.Parse(fmt.Sprintf("file://%s?ttlCleanerInterval=50ms", tempDir))
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.NoError(t, err)

		// Set content with TTL
		err = f.Set(context.Background(), key, content, options.WithTTL(ttl))
		require.NoError(t, err)

		// Verify content exists
		exists, err := f.Exists(context.Background(), key)
		require.NoError(t, err)
		require.True(t, exists)

		// Wait for TTL to expire and cleaner to run
		time.Sleep(ttl + 100*time.Millisecond)

		// Content should be removed due to shorter cleaner interval
		exists, err = f.Exists(context.Background(), key)
		require.NoError(t, err)
		require.False(t, exists)
	})

	t.Run("invalid ttlCleanerInterval in URL", func(t *testing.T) {
		u, err := url.Parse("file://./data/subtreestore?ttlCleanerInterval=invalid")
		require.NoError(t, err)

		f, err := New(ulogger.TestLogger{}, u)
		require.Error(t, err)
		require.Nil(t, f)
		require.Contains(t, err.Error(), "failed to parse ttlCleanerInterval")
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
		key := []byte("nonexistent-key")
		_, err = f.Get(context.Background(), key)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound))

		// Try to get non-existent file with options
		_, err = f.Get(context.Background(), key, options.WithFileExtension("txt"))
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
		key := []byte("nonexistent-key")
		reader, err := f.GetIoReader(context.Background(), key)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound))
		require.Nil(t, reader)

		// Try to get reader for non-existent file with options
		reader, err = f.GetIoReader(context.Background(), key, options.WithFileExtension("txt"))
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.ErrNotFound))
		require.Nil(t, reader)
	})
}
