package file

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/rand"
)

var testDir = "/tmp/ubsv-tests/" + rand.String(12)

func cleanup() {
	_ = os.RemoveAll(testDir)
}

func TestFile_GetWithAbsolutePath(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, []string{testDir})
		require.NoError(t, err)

		// check if the directory is absolute
		_, err = os.Stat(testDir)
		require.NoError(t, err)

		// check if the directory is not relative
		_, err = os.Stat(testDir[1:])
		require.Error(t, err)

		err = f.Set(context.Background(), []byte("key"), []byte("value"))
		require.NoError(t, err)

		value, err := f.Get(context.Background(), []byte("key"))
		require.NoError(t, err)

		require.Equal(t, []byte("value"), value)

		err = f.Del(context.Background(), []byte("key"))
		require.NoError(t, err)

		cleanup()
	})
}

func TestFile_GetWithRelativePath(t *testing.T) {
	// random directory name
	relativePath := "test-path-" + rand.String(12)
	f, err := New(ulogger.TestLogger{}, []string{relativePath})
	require.NoError(t, err)

	// check if the directory is created
	_, err =
		os.Stat(relativePath)
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

func TestFile_filename(t *testing.T) {
	t.Run("1 path", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, []string{testDir})
		require.NoError(t, err)

		filename := f.filename([]byte("key"))
		assert.Equal(t, testDir+"/79656b", filename)

		cleanup()
	})

	t.Run("1 paths", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, []string{"/tmp/ubsv-tests1"})
		require.NoError(t, err)

		filename := f.filename([]byte("1key"))
		assert.Equal(t, "/tmp/ubsv-tests1/79656b31", filename)

		filename = f.filename([]byte("2key"))
		assert.Equal(t, "/tmp/ubsv-tests1/79656b32", filename)

		filename = f.filename([]byte("3key"))
		assert.Equal(t, "/tmp/ubsv-tests1/79656b33", filename)

		filename = f.filename([]byte("4key"))
		assert.Equal(t, "/tmp/ubsv-tests1/79656b34", filename)

		cleanup()
	})

	t.Run("2 paths", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, []string{"/tmp/ubsv-tests1", "/tmp/ubsv-tests2"})
		require.NoError(t, err)

		filename := f.filename([]byte("1key"))
		assert.Equal(t, "/tmp/ubsv-tests2/79656b31", filename)

		filename = f.filename([]byte("2key"))
		assert.Equal(t, "/tmp/ubsv-tests1/79656b32", filename)

		filename = f.filename([]byte("3key"))
		assert.Equal(t, "/tmp/ubsv-tests2/79656b33", filename)

		filename = f.filename([]byte("4key"))
		assert.Equal(t, "/tmp/ubsv-tests1/79656b34", filename)

		cleanup()
	})

	t.Run("4 paths", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, []string{
			"/tmp/ubsv-tests1",
			"/tmp/ubsv-tests2",
			"/tmp/ubsv-tests3",
			"/tmp/ubsv-tests4",
		})
		require.NoError(t, err)

		filename := f.filename([]byte("1key"))
		assert.Equal(t, "/tmp/ubsv-tests2/79656b31", filename)

		filename = f.filename([]byte("2key"))
		assert.Equal(t, "/tmp/ubsv-tests3/79656b32", filename)

		filename = f.filename([]byte("3key"))
		assert.Equal(t, "/tmp/ubsv-tests4/79656b33", filename)

		filename = f.filename([]byte("4key"))
		assert.Equal(t, "/tmp/ubsv-tests1/79656b34", filename)

		cleanup()
	})
}

func TestFile_AbsoluteAndRelativePath(t *testing.T) {
	absoluteUrl, err := url.ParseRequestURI("file:///absolute/path/to/file")
	require.NoError(t, err)
	require.Equal(t, "/absolute/path/to/file", GetPathFromURL(absoluteUrl))

	relativeUrl, err := url.ParseRequestURI("file://./relative/path/to/file")
	require.NoError(t, err)
	require.Equal(t, "relative/path/to/file", GetPathFromURL(relativeUrl))

}

func GetPathFromURL(u *url.URL) string {
	if u.Host == "." {
		return u.Path[1:]
	}
	return u.Path
}

func TestFile_NewWithEmptyPaths(t *testing.T) {
	t.Run("empty paths", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, []string{})
		require.NoError(t, err)
		require.NotNil(t, f)
	})
}

func TestFile_NewWithInvalidDirectory(t *testing.T) {
	t.Run("invalid directory", func(t *testing.T) {
		invalidPath := "/invalid-directory" // Assuming this path cannot be created
		_, err := New(ulogger.TestLogger{}, []string{invalidPath})
		require.Error(t, err) // "mkdir /invalid-directory: read-only file system"
		require.Contains(t, err.Error(), "failed to create directory")
	})
}

func TestFile_loadTTLs(t *testing.T) {
	t.Run("load TTLs", func(t *testing.T) {
		key := []byte("key")
		value := []byte("value")
		ttl := 1 * time.Second
		ttlInterval := 1 * time.Second
		ctx := context.Background()

		f, err := new(ulogger.TestLogger{}, []string{testDir}, ttlInterval)
		require.NoError(t, err)

		err = f.Set(ctx, key, value, options.WithTTL(100))
		require.NoError(t, err)

		err = f.SetTTL(ctx, key, ttl)
		require.NoError(t, err)

		var fileTTLs map[string]time.Time

		f.fileTTLsMu.Lock()
		fileTTLs = f.fileTTLs
		require.NoError(t, err)
		require.Contains(t, fileTTLs, f.filename(key))
		f.fileTTLsMu.Unlock()

		time.Sleep(ttlInterval * 3)

		f.fileTTLsMu.Lock()
		fileTTLs = f.fileTTLs
		require.NoError(t, err)
		require.NotContains(t, fileTTLs, f.filename(key))
		f.fileTTLsMu.Unlock()

		err = f.Del(ctx, key)
		require.Error(t, err)
	})
}

func TestFile_ConcurrentAccess(t *testing.T) {
	t.Run("concurrent set and get", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, []string{testDir})
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

func TestFile_SetWithSubdirectoryOptionIgnored(t *testing.T) {
	t.Run("subdirectory usage", func(t *testing.T) {
		subDir := "subdir"
		f, err := New(ulogger.TestLogger{}, []string{testDir}, options.WithSubDirectory(subDir))
		require.NoError(t, err)

		key := []byte("key")
		value := []byte("value")

		err = f.Set(context.Background(), key, value)
		require.NoError(t, err)

		// Construct the expected file path in the subdirectory
		expectedDir := filepath.Join(testDir, subDir)
		expectedFilePath := filepath.Join(expectedDir, string(f.filename(key)))

		// Check if the file was NOT created in the subdirectory (it is supposed to be ignored)
		_, err = os.Stat(expectedFilePath)
		require.Error(t, err, "expected file not found in subdirectory - the options should be ignored")
	})
}

func TestFile_SetWithSubdirectoryOption(t *testing.T) {
	t.Run("subdirectory usage", func(t *testing.T) {
		subDir := "subdir"
		f, err := New(ulogger.TestLogger{}, []string{testDir}, options.WithSubDirectory(subDir))
		require.NoError(t, err)

		key := []byte("key")
		value := []byte("value")

		err = f.Set(context.Background(), key, value, options.WithFileName("filename"))
		require.NoError(t, err)

		// Construct the expected file path in the subdirectory
		expectedDir := filepath.Join(testDir, subDir)
		expectedFilePath := filepath.Join(expectedDir, "filename")

		// Check if the file was created in the subdirectory
		_, err = os.Stat(expectedFilePath)
		require.NoError(t, err, "expected file found in subdirectoryß")
	})
}

// A simple wrapper to add the Close method to a Reader
type readCloser struct {
	io.Reader
}

func (rc readCloser) Close() error {
	return nil
}

func TestFile_SetFromReaderAndGetIoReader(t *testing.T) {
	t.Run("set content from reader", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, []string{testDir})
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
		storedContent, err := ioutil.ReadAll(storedReader)
		require.NoError(t, err)
		require.Equal(t, content, string(storedContent))
	})
}

func TestFile_GetHead(t *testing.T) {
	t.Run("get head of content", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, []string{testDir})
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

func TestFile_Exists(t *testing.T) {
	t.Run("check if content exists", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, []string{testDir})
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
