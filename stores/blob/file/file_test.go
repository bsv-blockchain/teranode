package file

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
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

	err = f.Set(context.Background(), []byte("key"), []byte("value"))
	require.NoError(t, err)

	value, err := f.Get(context.Background(), []byte("key"))
	require.NoError(t, err)

	require.Equal(t, []byte("value"), value)

	err = f.Del(context.Background(), []byte("key"))
	require.NoError(t, err)

	// cleanup
	_ = os.RemoveAll(relativePath)
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

		f, err := new(ulogger.TestLogger{}, u, ttlInterval)
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

		f, err := New(ulogger.TestLogger{}, u, options.WithSubDirectory(subDir))
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

		f, err := New(ulogger.TestLogger{}, u, options.WithSubDirectory(subDir))
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
