package lustre

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileNilS3(t *testing.T) {
	// Get a temporary directory
	tempDir, err := os.MkdirTemp("", "test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	t.Run("no s3", func(t *testing.T) {
		var s3Client s3Store

		require.Nil(t, s3Client)

		f, err := NewLustreStore(ulogger.TestLogger{}, s3Client, tempDir, "persist")
		require.NoError(t, err)

		// should not exist
		exists, err := f.Exists(context.Background(), []byte("key"))
		require.NoError(t, err)
		require.False(t, exists)

		// should fail
		value, err := f.Get(context.Background(), []byte("key"))
		require.Error(t, err)
		require.Nil(t, value)

		// should fail
		value, err = f.GetHead(context.Background(), []byte("key"), 3)
		require.Error(t, err)
		require.Nil(t, value)

		// create the file
		err = f.Set(context.Background(), []byte("key"), []byte("value"))
		require.NoError(t, err)

		// should exist
		exists, err = f.Exists(context.Background(), []byte("key"))
		require.NoError(t, err)
		require.True(t, exists)

		// should succeed
		value, err = f.Get(context.Background(), []byte("key"))
		require.NoError(t, err)
		require.Equal(t, []byte("value"), value)

		// should succeed
		value, err = f.GetHead(context.Background(), []byte("key"), 3)
		require.NoError(t, err)
		require.Equal(t, []byte("val"), value)

		// cleanup
		err = f.Del(context.Background(), []byte("key"))
		require.NoError(t, err)
	})
}

func TestFileGet(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		s3Client := NewS3Store()
		f, err := NewLustreStore(ulogger.TestLogger{}, s3Client, "/tmp/teranode-tests", "persist")
		require.NoError(t, err)

		s3Client.Set(nil, fs.ErrNotExist)

		// should not exist
		exists, err := f.Exists(context.Background(), []byte("key"))
		require.NoError(t, err)
		require.False(t, exists)

		// should fail
		value, err := f.Get(context.Background(), []byte("key"))
		require.Error(t, err)
		require.Nil(t, value)

		// should fail
		value, err = f.GetHead(context.Background(), []byte("key"), 3)
		require.Error(t, err)
		require.Nil(t, value)

		// create the file
		err = f.Set(context.Background(), []byte("key"), []byte("value"))
		require.NoError(t, err)

		// should exist
		exists, err = f.Exists(context.Background(), []byte("key"))
		require.NoError(t, err)
		require.True(t, exists)

		// should succeed
		value, err = f.Get(context.Background(), []byte("key"))
		require.NoError(t, err)
		require.Equal(t, []byte("value"), value)

		// should succeed
		value, err = f.GetHead(context.Background(), []byte("key"), 3)
		require.NoError(t, err)
		require.Equal(t, []byte("val"), value)

		// cleanup
		err = f.Del(context.Background(), []byte("key"))
		require.NoError(t, err)
	})

	t.Run("ttl", func(t *testing.T) {
		s3Client := NewS3Store()
		f, err := NewLustreStore(ulogger.TestLogger{}, s3Client, "/tmp/teranode-tests", "persist")
		require.NoError(t, err)

		s3Client.Set(nil, fs.ErrNotExist)

		filename := "/tmp/teranode-tests/79656b"
		persistFilename := "/tmp/teranode-tests/persist/79656b"

		// should not exist
		exists, err := f.Exists(context.Background(), []byte("key"))
		require.NoError(t, err)
		require.False(t, exists)

		// should fail
		value, err := f.Get(context.Background(), []byte("key"))
		require.Error(t, err)
		require.Nil(t, value)

		// should fail
		value, err = f.GetHead(context.Background(), []byte("key"), 3)
		require.Error(t, err)
		require.Nil(t, value)

		// create the file
		err = f.Set(context.Background(), []byte("key"), []byte("value"), options.WithTTL(1*time.Minute))
		require.NoError(t, err)

		value, err = f.Get(context.Background(), []byte("key"))
		require.NoError(t, err)
		require.Equal(t, []byte("value"), value)

		info, err := os.Stat(filename)
		require.NoError(t, err)
		require.Equal(t, info.Name(), "79656b")

		_, err = os.Stat(persistFilename)
		require.Error(t, err)

		err = f.SetTTL(context.Background(), []byte("key"), 0)
		require.NoError(t, err)

		// file should still be there, we copy it
		_, err = os.Stat(filename)
		require.NoError(t, err)

		// file should have been persisted - moved to the persists directory
		info, err = os.Stat(persistFilename)
		require.NoError(t, err)
		require.Equal(t, info.Name(), "79656b")

		// should exist
		exists, err = f.Exists(context.Background(), []byte("key"))
		require.NoError(t, err)
		require.True(t, exists)

		// remove from base dir, force logic to look in persist folder
		err = os.Remove(filename)
		require.NoError(t, err)

		// remove from persist dir, force logic to use S3
		// err = os.Remove(persistFilename)
		// require.NoError(t, err)
		err = f.Del(context.Background(), []byte("key"))
		require.NoError(t, err)

		// still not there because our mock is still returning fs.ErrNotExist
		value, err = f.Get(context.Background(), []byte("key"))
		require.Error(t, err)
		require.Nil(t, value)

		s3Client.Set([]byte("value"), nil)

		value, err = f.Get(context.Background(), []byte("key"))
		require.NoError(t, err)
		require.Equal(t, []byte("value"), value)
	})
}

func TestFileGetFromReader(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		s3Client := NewS3Store()
		f, err := NewLustreStore(ulogger.TestLogger{}, s3Client, tempDir, "persist")
		require.NoError(t, err)

		s3Client.Set(nil, fs.ErrNotExist)

		err = f.Set(context.Background(), []byte("key"), []byte("value"))
		require.NoError(t, err)

		value, err := f.Get(context.Background(), []byte("key"))
		require.NoError(t, err)

		require.Equal(t, []byte("value"), value)

		err = f.Del(context.Background(), []byte("key"))
		require.NoError(t, err)
	})

	t.Run("ttl", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		s3Client := NewS3Store()
		f, err := NewLustreStore(ulogger.TestLogger{}, s3Client, tempDir, "persist")
		require.NoError(t, err)

		s3Client.Set(nil, fs.ErrNotExist)

		filename := tempDir + "/79656b"
		persistFilename := tempDir + "/persist/79656b"

		// should not exist
		reader, err := f.GetIoReader(context.Background(), []byte("key"))
		require.Error(t, err)
		require.Nil(t, reader)

		// create the file
		value := []byte("value")
		byteReader := bytes.NewReader(value)   // Create a bytes.Reader from []byte
		readCloser := io.NopCloser(byteReader) // Wrap the bytes.Reader with io.NopCloser
		err = f.SetFromReader(context.Background(), []byte("key"), readCloser, options.WithTTL(1*time.Minute))
		require.NoError(t, err)

		// should exist
		reader, err = f.GetIoReader(context.Background(), []byte("key"))
		require.NoError(t, err)
		require.NotNil(t, reader)

		readValue := make([]byte, len(value))
		_, err = reader.Read(readValue)
		require.NoError(t, err)
		require.Equal(t, []byte("value"), readValue)

		info, err := os.Stat(filename)
		require.NoError(t, err)
		require.Equal(t, info.Name(), "79656b")

		_, err = os.Stat(persistFilename)
		require.Error(t, err)

		err = f.SetTTL(context.Background(), []byte("key"), 0)
		require.NoError(t, err)

		// file should still be there, we copy it
		_, err = os.Stat(filename)
		require.NoError(t, err)

		// remove from base dir, force logic to look in persist folder
		err = os.Remove(filename)
		require.NoError(t, err)

		// file should have been persisted - moved to the persists directory
		info, err = os.Stat(persistFilename)
		require.NoError(t, err)
		require.Equal(t, info.Name(), "79656b")

		// should exist
		reader, err = f.GetIoReader(context.Background(), []byte("key"))
		require.NoError(t, err)
		require.NotNil(t, reader)

		// re-set TTL to positive number, should move file back to base dir
		err = f.SetTTL(context.Background(), []byte("key"), 12*time.Hour)
		require.NoError(t, err)

		// file should still be there, we copy it
		_, err = os.Stat(filename)
		require.NoError(t, err)

		// file should have been removed from persisted folder
		_, err = os.Stat(persistFilename)
		require.Error(t, err)

		// cleanup
		err = f.Del(context.Background(), []byte("key"))
		require.NoError(t, err)
	})
}

func TestFileNew(t *testing.T) {
	url, err := url.ParseRequestURI("lustre:///teranode?localDir=/data/subtrees&localPersist=s3")
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, url, "data", "persist")
	require.NoError(t, err)
	require.NotNil(t, store)

	err = store.Close(context.Background())
	require.NoError(t, err)
}

func NewS3Store() *s3StoreMock {
	return &s3StoreMock{}
}

type s3StoreMock struct {
	value []byte
	err   error
}

func (s *s3StoreMock) Set(value []byte, err error) {
	s.value = value
	s.err = err
}

func (s *s3StoreMock) Get(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error) {
	return s.value, s.err
}

func (s *s3StoreMock) GetIoReader(ctx context.Context, key []byte, opts ...options.FileOption) (io.ReadCloser, error) {
	if s.value == nil {
		return nil, s.err
	}

	return io.NopCloser(bytes.NewReader(s.value)), s.err
}

func (s *s3StoreMock) Exists(ctx context.Context, key []byte, opts ...options.FileOption) (bool, error) {
	if s.value == nil {
		return false, s.err
	}

	return true, s.err
}

func (s *s3StoreMock) GetFooterMetaData(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error) {
	return nil, s.err
}

func TestFileNameForPersist(t *testing.T) {
	url, err := url.ParseRequestURI("lustre://s3.com/teranode")
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, url, "data", "persist", options.WithHashPrefix(2), options.WithDefaultSubDirectory("sub"))
	require.NoError(t, err)
	require.NotNil(t, store)

	filename, err := store.options.ConstructFilename("data/subtreestore", []byte("key"))
	require.NoError(t, err)
	assert.Equal(t, "data/subtreestore/sub/79/79656b", filename)

	var opts []options.FileOption

	filename, err = store.getFilenameForSet([]byte("key"), opts)
	require.NoError(t, err)
	assert.Equal(t, "data/persist/sub/79/79656b", filename)

	opts = append(opts, options.WithTTL(2*time.Second))

	filename, err = store.getFilenameForSet([]byte("key"), opts)
	require.NoError(t, err)
	assert.Equal(t, "data/sub/79/79656b", filename)
}

func TestHealth(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "lustre_health_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	persistDir := "persist"

	t.Run("Healthy state", func(t *testing.T) {
		s3Client := NewHealthyS3StoreMock()
		store, err := NewLustreStore(ulogger.TestLogger{}, s3Client, tempDir, persistDir)
		require.NoError(t, err)

		status, message, err := store.Health(context.Background(), false)
		assert.Equal(t, http.StatusOK, status)
		assert.Equal(t, "Lustre blob Store healthy", message)
		assert.NoError(t, err)
	})

	t.Run("Main path issue", func(t *testing.T) {
		s3Client := NewHealthyS3StoreMock()
		store, err := NewLustreStore(ulogger.TestLogger{}, s3Client, "./nonexistent", persistDir)
		require.NoError(t, err)

		// creating a new LustreStore will create the ./nonexistent folder
		// so we need to delete it before we can test
		err = os.RemoveAll("./nonexistent")
		require.NoError(t, err)

		status, message, err := store.Health(context.Background(), false)
		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.Contains(t, message, "Main path issue")
		assert.NoError(t, err)
	})

	t.Run("Persist subdirectory issue", func(t *testing.T) {
		s3Client := NewHealthyS3StoreMock()
		store, err := NewLustreStore(ulogger.TestLogger{}, s3Client, tempDir, "./nonexistentpersist")
		require.NoError(t, err)

		// creating a new LustreStore will create the ./nonexistent folder
		// so we need to delete it before we can test
		err = os.RemoveAll(filepath.Join(tempDir, "nonexistentpersist"))
		require.NoError(t, err)

		status, message, err := store.Health(context.Background(), false)
		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.Contains(t, message, "Persist subdirectory issue")
		assert.NoError(t, err)
	})

	t.Run("S3 client issue", func(t *testing.T) {
		s3Client := NewUnhealthyS3StoreMock()
		store, err := NewLustreStore(ulogger.TestLogger{}, s3Client, tempDir, persistDir)
		require.NoError(t, err)

		status, message, err := store.Health(context.Background(), false)
		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.Contains(t, message, "S3 client issue")
		assert.NoError(t, err)
	})

	t.Run("Multiple issues", func(t *testing.T) {
		s3Client := NewUnhealthyS3StoreMock()
		store, err := NewLustreStore(ulogger.TestLogger{}, s3Client, "./nonexistent", "./nonexistentpersist")
		require.NoError(t, err)

		// creating a new LustreStore will create the ./nonexistent folder
		// so we need to delete it before we can test
		err = os.RemoveAll("./nonexistent")
		require.NoError(t, err)
		err = os.RemoveAll(filepath.Join(tempDir, "nonexistentpersist"))
		require.NoError(t, err)

		status, message, err := store.Health(context.Background(), false)
		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.Contains(t, message, "Main path issue")
		assert.Contains(t, message, "Persist subdirectory issue")
		assert.Contains(t, message, "S3 client issue")
		assert.NoError(t, err)
	})
}

// Mock S3 stores for testing
type HealthyS3StoreMock struct{}

func NewHealthyS3StoreMock() *HealthyS3StoreMock {
	return &HealthyS3StoreMock{}
}

func (s *HealthyS3StoreMock) Get(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error) {
	return nil, nil
}

func (s *HealthyS3StoreMock) GetIoReader(ctx context.Context, key []byte, opts ...options.FileOption) (io.ReadCloser, error) {
	return nil, nil
}

func (s *HealthyS3StoreMock) Exists(ctx context.Context, key []byte, opts ...options.FileOption) (bool, error) {
	return false, nil
}

func (s *HealthyS3StoreMock) GetFooterMetaData(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error) {
	return nil, nil
}

type UnhealthyS3StoreMock struct{}

func NewUnhealthyS3StoreMock() *UnhealthyS3StoreMock {
	return &UnhealthyS3StoreMock{}
}

func (s *UnhealthyS3StoreMock) Get(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error) {
	return nil, nil
}

func (s *UnhealthyS3StoreMock) GetIoReader(ctx context.Context, key []byte, opts ...options.FileOption) (io.ReadCloser, error) {
	return nil, nil
}

func (s *UnhealthyS3StoreMock) Exists(ctx context.Context, key []byte, opts ...options.FileOption) (bool, error) {
	return false, assert.AnError
}

func (s *UnhealthyS3StoreMock) GetFooterMetaData(ctx context.Context, key []byte, opts ...options.FileOption) ([]byte, error) {
	return nil, assert.AnError
}

func TestLustreWithHeader(t *testing.T) {
	t.Run("set and get with header", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "TestLustreWithHeader")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		header := "This is the header"
		l, err := NewLustreStore(ulogger.TestLogger{}, nil, tempDir, "persist", options.WithHeader([]byte(header)))
		require.NoError(t, err)

		key := []byte("key-with-header")
		content := "This is the main content - TestLustreWithHeader"

		// Test setting content with header using Set
		err = l.Set(context.Background(), key, []byte(content))
		require.NoError(t, err)

		// Verify content using Get
		value, err := l.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, content, string(value))

		// Verify content using GetIoReader
		reader, err := l.GetIoReader(context.Background(), key)
		require.NoError(t, err)

		// Read all the content from the reader
		readContent, err := io.ReadAll(reader)
		require.NoError(t, err)
		reader.Close()
		require.Equal(t, content, string(readContent))

		// delete the file
		err = l.Del(context.Background(), key)
		require.NoError(t, err)

		// Test setting content with header using SetFromReader
		newContent := "New content from reader"
		contentReader := strings.NewReader(newContent)
		readCloser := io.NopCloser(contentReader)

		err = l.SetFromReader(context.Background(), key, readCloser)
		require.NoError(t, err)

		// Verify new content using Get
		value, err = l.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, newContent, string(value))

		// Verify new content using GetIoReader
		reader, err = l.GetIoReader(context.Background(), key)
		require.NoError(t, err)

		readContent, err = io.ReadAll(reader)
		require.NoError(t, err)
		reader.Close()
		require.Equal(t, newContent, string(readContent))

		// delete the file
		err = l.Del(context.Background(), key)
		require.NoError(t, err)
	})
}

func TestLustreWithFooter(t *testing.T) {
	t.Run("set and get with footer", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "TestLustreWithFooter")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		footer := "This is the footer"
		l, err := NewLustreStore(ulogger.TestLogger{}, nil, tempDir, "persist", options.WithFooter(options.NewFooter(len(footer), []byte(footer), nil)))
		require.NoError(t, err)

		key := []byte("key-with-footer")
		content := "This is the main content - TestLustreWithFooter"

		// Test setting content with footer using Set
		err = l.Set(context.Background(), key, []byte(content))
		require.NoError(t, err)

		// Verify content using Get
		value, err := l.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, content, string(value))

		// Verify content using GetIoReader
		reader, err := l.GetIoReader(context.Background(), key)
		require.NoError(t, err)

		// Read all the content from the reader
		readContent, err := io.ReadAll(reader)
		require.NoError(t, err)
		reader.Close()
		require.Equal(t, content, string(readContent))

		// delete the file
		err = l.Del(context.Background(), key)
		require.NoError(t, err)

		// Test setting content with footer using SetFromReader
		newContent := "New content from reader"
		contentReader := strings.NewReader(newContent)
		readCloser := io.NopCloser(contentReader)

		err = l.SetFromReader(context.Background(), key, readCloser)
		require.NoError(t, err)

		// Verify new content using Get
		value, err = l.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, newContent, string(value))

		// Verify new content using GetIoReader
		reader, err = l.GetIoReader(context.Background(), key)
		require.NoError(t, err)

		readContent, err = io.ReadAll(reader)
		require.NoError(t, err)
		reader.Close()
		require.Equal(t, newContent, string(readContent))

		// delete the file
		err = l.Del(context.Background(), key)
		require.NoError(t, err)
	})
}

func TestLustreWithHeaderAndFooter(t *testing.T) {
	t.Run("set and get with both header and footer", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "TestLustreWithHeaderAndFooter")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		header := "This is the header"
		footer := "This is the footer"

		l, err := NewLustreStore(ulogger.TestLogger{}, nil, tempDir, "persist", options.WithHeader([]byte(header)), options.WithFooter(options.NewFooter(len(footer), []byte(footer), nil)))
		require.NoError(t, err)

		key := []byte("key-with-both")
		content := "This is the main content - set and get with both header and footer"

		// Test setting content with both header and footer
		err = l.Set(context.Background(), key, []byte(content))
		require.NoError(t, err)

		// Verify content using Get
		value, err := l.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, content, string(value))

		// Verify content using GetIoReader
		reader, err := l.GetIoReader(context.Background(), key)
		require.NoError(t, err)

		readContent, err := io.ReadAll(reader)
		require.NoError(t, err)
		reader.Close()
		require.Equal(t, content, string(readContent))

		// delete the file
		err = l.Del(context.Background(), key)
		require.NoError(t, err)
	})
}

func TestLustreWithURLHeaderFooter(t *testing.T) {
	t.Run("with header and footer in URL", func(t *testing.T) {
		// Get a temporary directory
		tempDir, err := os.MkdirTemp("", "test-lustre-header-footer")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		// Create persist directory
		persistDir := filepath.Join(tempDir, "persist")
		err = os.MkdirAll(persistDir, 0755)
		require.NoError(t, err)

		// Create URL with header and footer parameters
		u, err := url.Parse("lustre://s3.com/ubsv?header=START&eofmarker=END")
		require.NoError(t, err)

		l, err := New(ulogger.TestLogger{}, u, tempDir, "persist")
		require.NoError(t, err)

		key := []byte("test-key")
		content := "test content"

		// Test Set with TTL to ensure it goes to the main directory
		err = l.Set(context.Background(), key, []byte(content), options.WithTTL(time.Hour))
		require.NoError(t, err)

		// Read raw file to verify header and footer
		filename, err := l.options.ConstructFilename(tempDir, key)
		require.NoError(t, err)

		// Ensure directory exists
		err = os.MkdirAll(filepath.Dir(filename), 0755)
		require.NoError(t, err)

		rawData, err := os.ReadFile(filename)
		require.NoError(t, err)

		// Verify header and footer are present in raw data
		expectedData := append([]byte("START"), []byte(content)...)
		expectedData = append(expectedData, []byte("END")...)
		require.Equal(t, expectedData, rawData)

		// Test Get - should return content without header/footer
		value, err := l.Get(context.Background(), key)
		require.NoError(t, err)
		require.Equal(t, content, string(value))

		// Test GetIoReader - should return content without header/footer
		reader, err := l.GetIoReader(context.Background(), key)
		require.NoError(t, err)
		defer reader.Close()

		readContent, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, content, string(readContent))

		// Clean up
		err = l.Del(context.Background(), key)
		require.NoError(t, err)
	})
}

func TestLustre_GetHeader(t *testing.T) {
	url, err := url.ParseRequestURI("lustre://s3.com/ubsv?localDir=/data/subtrees&localPersist=s3")
	require.NoError(t, err)

	dir := t.TempDir()
	store, err := New(ulogger.TestLogger{}, url, dir, "persist")
	require.NoError(t, err)

	ctx := context.Background()
	key := []byte("test-key")
	value := []byte("test-value")
	header := []byte("header-bytes")

	tests := []struct {
		name    string
		key     []byte
		value   []byte
		header  []byte
		setup   func()
		wantErr bool
	}{
		{
			name:    "successful header retrieval",
			key:     key,
			value:   value,
			header:  header,
			wantErr: false,
			setup: func() {
				store.options = options.NewStoreOptions(options.WithHeader(header))
				_ = store.Del(ctx, key)
				err := store.Set(ctx, key, value)
				require.NoError(t, err)
			},
		},
		{
			name:  "no header configured",
			key:   key,
			value: value,
			setup: func() {
				store.options = options.NewStoreOptions() // Reset options
				_ = store.Del(ctx, key)
				err = store.Set(ctx, key, value)
				require.NoError(t, err)
			},
			wantErr: false,
		},
		{
			name:   "non-existent key",
			key:    []byte("non-existent"),
			header: header,
			setup: func() {
				store.options = options.NewStoreOptions(options.WithHeader(header))
				_ = store.Del(ctx, key)
			},
			wantErr: true,
		},
		{
			name:    "header mismatch",
			key:     key,
			value:   value,
			header:  header,
			wantErr: true,
			setup: func() {
				// Set up file with different header
				store.options = options.NewStoreOptions(options.WithHeader([]byte("different-header")))
				_ = store.Del(ctx, key)
				err := store.Set(ctx, key, value)
				require.NoError(t, err)

				// Switch to expected header for test
				store.options = options.NewStoreOptions(options.WithHeader(header))
			},
		},
		{
			name:    "empty file",
			key:     []byte("empty-file"),
			value:   []byte{},
			header:  header,
			wantErr: true,
			setup: func() {
				store.options = options.NewStoreOptions(options.WithHeader(header))
				_ = store.Del(ctx, key)
				err := store.Set(ctx, key, []byte{})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()

			got, err := store.GetHeader(ctx, tt.key)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.header == nil {
				assert.Nil(t, got)
			} else {
				assert.Equal(t, tt.header, got)
			}
		})
	}
}

func TestLustre_GetMetaData(t *testing.T) {
	url, err := url.ParseRequestURI("lustre://s3.com/ubsv?localDir=/data/subtrees&localPersist=s3")
	require.NoError(t, err)

	dir := t.TempDir()

	var store *Lustre

	ctx := context.Background()
	key := []byte("test-key")
	value := []byte("test-value")
	footer := []byte("footer-bytes")
	metadata := []byte("metadata-content")
	footerObj := options.NewFooter(len(footer)+len(metadata), footer, func() []byte { return metadata })

	tests := []struct {
		name     string
		key      []byte
		value    []byte
		footer   []byte
		metadata []byte
		setup    func()
		wantErr  bool
	}{
		{
			name:     "successful metadata retrieval",
			key:      key,
			value:    value,
			footer:   footer,
			metadata: metadata,
			wantErr:  false,
			setup: func() {
				store, err = New(ulogger.TestLogger{}, url, dir, "persist", options.WithFooter(footerObj))
				require.NoError(t, err)
				_ = store.Del(ctx, key)
				err = store.Set(ctx, key, value)
				require.NoError(t, err)
			},
		},
		{
			name:     "successful metadata retrieval with ttl",
			key:      key,
			value:    value,
			footer:   footer,
			metadata: metadata,
			wantErr:  false,
			setup: func() {
				store, err = New(ulogger.TestLogger{}, url, dir, "persist", options.WithFooter(footerObj))
				require.NoError(t, err)
				_ = store.Del(ctx, key)
				err = store.Set(ctx, key, value, options.WithTTL(time.Hour))
				require.NoError(t, err)
			},
		},
		{
			name:  "no footer configured",
			key:   key,
			value: value,
			setup: func() {
				store, err = New(ulogger.TestLogger{}, url, dir, "persist")
				require.NoError(t, err)
				_ = store.Del(ctx, key)
				err = store.Set(ctx, key, value)
				require.NoError(t, err)
			},
			wantErr: false,
		},
		{
			name:     "non-existent key",
			key:      []byte("non-existent"),
			footer:   footer,
			metadata: metadata,
			setup: func() {
				store, err = New(ulogger.TestLogger{}, url, dir, "persist", options.WithFooter(footerObj))
				require.NoError(t, err)
				_ = store.Del(ctx, key)
			},
			wantErr: true,
		},
		{
			name:     "invalid footer",
			key:      key,
			value:    value,
			footer:   footer,
			metadata: metadata,
			wantErr:  true,
			setup: func() {
				// Set up file with invalid footer
				invalidFooter := options.NewFooter(len([]byte("invalid-footer")), []byte("invalid-footer"), nil)
				store, err = New(ulogger.TestLogger{}, url, dir, "persist", options.WithFooter(invalidFooter))
				require.NoError(t, err)
				_ = store.Del(ctx, key)
				err = store.Set(ctx, key, value)
				require.NoError(t, err)

				// Switch to expected footer for test
				store, err = New(ulogger.TestLogger{}, url, dir, "persist", options.WithFooter(footerObj))
				require.NoError(t, err)
			},
		},
		{
			name:     "empty file",
			key:      []byte("empty-file"),
			value:    []byte{},
			footer:   footer,
			metadata: metadata,
			wantErr:  true,
			setup: func() {
				store, err = New(ulogger.TestLogger{}, url, dir, "persist", options.WithFooter(footerObj))
				require.NoError(t, err)
				_ = store.Del(ctx, key)
				err := store.Set(ctx, key, []byte{})
				require.NoError(t, err)
			},
		},
		{
			name:     "file too small for footer",
			key:      key,
			value:    []byte("small"),
			footer:   footer,
			metadata: metadata,
			wantErr:  true,
			setup: func() {
				store, err = New(ulogger.TestLogger{}, url, dir, "persist")
				require.NoError(t, err)
				_ = store.Del(ctx, key)
				err := store.Set(ctx, key, []byte("small"))
				require.NoError(t, err)
				store.options = options.NewStoreOptions(options.WithFooter(footerObj))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setup != nil {
				tt.setup()
			}

			got, err := store.GetFooterMetaData(ctx, tt.key)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			if tt.metadata == nil {
				assert.Nil(t, got)
			} else {
				assert.Equal(t, tt.metadata, got)
			}
		})
	}
}

func TestLustre_GetTTL(t *testing.T) {
	// Create temp directories for testing
	mainDir := t.TempDir()
	persistDir := "persist"

	logger := ulogger.TestLogger{}
	defaultTTL := 24 * time.Hour

	store, err := NewLustreStore(
		logger,
		nil, // no S3 client needed for this test
		mainDir,
		persistDir,
		options.WithDefaultTTL(defaultTTL),
	)
	if err != nil {
		t.Fatalf("Failed to create lustre store: %v", err)
	}

	testKey := []byte("testkey123")
	testData := []byte("test data")

	t.Run("file in main directory should return default TTL", func(t *testing.T) {
		_ = store.Del(context.Background(), testKey)
		// Set file in main directory
		err = store.Set(context.Background(), testKey, testData)
		if err != nil {
			t.Fatalf("Failed to set test data: %v", err)
		}

		ttl, err := store.GetTTL(context.Background(), testKey)
		if err != nil {
			t.Errorf("GetTTL failed: %v", err)
		}

		if ttl != defaultTTL {
			t.Errorf("Expected TTL %v, got %v", defaultTTL, ttl)
		}
	})

	t.Run("file in persist directory should return 0 TTL", func(t *testing.T) {
		_ = store.Del(context.Background(), testKey)
		// Set file and move it to persist directory
		err = store.Set(context.Background(), testKey, testData)
		if err != nil {
			t.Fatalf("Failed to set test data: %v", err)
		}

		// Set TTL to 0 to move file to persist directory
		err = store.SetTTL(context.Background(), testKey, 0)
		if err != nil {
			t.Fatalf("Failed to set TTL to 0: %v", err)
		}

		ttl, err := store.GetTTL(context.Background(), testKey)
		if err != nil {
			t.Errorf("GetTTL failed: %v", err)
		}

		if ttl != 0 {
			t.Errorf("Expected TTL 0, got %v", ttl)
		}
	})

	t.Run("non-existent file should return error", func(t *testing.T) {
		nonExistentKey := []byte("nonexistent")

		_, err := store.GetTTL(context.Background(), nonExistentKey)
		if !errors.Is(err, errors.ErrNotFound) {
			t.Errorf("Expected ErrNotFound, got %v", err)
		}
	})

	t.Run("file moved between directories should return correct TTL", func(t *testing.T) {
		_ = store.Del(context.Background(), testKey)
		// First set file in main directory
		err = store.Set(context.Background(), testKey, testData)
		if err != nil {
			t.Fatalf("Failed to set test data: %v", err)
		}

		// Verify default TTL
		ttl, err := store.GetTTL(context.Background(), testKey)
		if err != nil {
			t.Errorf("GetTTL failed: %v", err)
		}

		if ttl != defaultTTL {
			t.Errorf("Expected TTL %v, got %v", defaultTTL, ttl)
		}

		// Move to persist directory
		err = store.SetTTL(context.Background(), testKey, 0)
		if err != nil {
			t.Fatalf("Failed to set TTL to 0: %v", err)
		}

		// Verify 0 TTL
		ttl, err = store.GetTTL(context.Background(), testKey)
		if err != nil {
			t.Errorf("GetTTL failed: %v", err)
		}

		if ttl != 0 {
			t.Errorf("Expected TTL 0, got %v", ttl)
		}

		// Move back to main directory
		err = store.SetTTL(context.Background(), testKey, defaultTTL)
		if err != nil {
			t.Fatalf("Failed to set TTL to default: %v", err)
		}

		// Verify default TTL again
		ttl, err = store.GetTTL(context.Background(), testKey)
		if err != nil {
			t.Errorf("GetTTL failed: %v", err)
		}

		if ttl != defaultTTL {
			t.Errorf("Expected TTL %v, got %v", defaultTTL, ttl)
		}
	})
}

func TestLustre_GetHead_LargeFile(t *testing.T) {
	// Get a temporary directory
	tempDir, err := os.MkdirTemp("", "test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create header, data, and footer
	header := []byte("HEADER123")   // 9-byte header
	data := make([]byte, 1024*1024) // 1MB of data
	footer := []byte("FOOTER123")   // 9-byte footer

	// Fill data with a recognizable pattern
	for i := 0; i < len(data); i++ {
		data[i] = byte(i % 256)
	}

	// Create the file (header + data + footer)
	hash := []byte("testfile")
	filePath := filepath.Join(tempDir, "testfile")

	fullContent := append(
		append(header, data...),
		footer...,
	)

	err = os.WriteFile(filePath, fullContent, 0600)
	require.NoError(t, err)

	// Create a footer option
	footerOpt := options.NewFooter(len(footer), footer, nil)

	// Create Lustre store with our header and footer configuration
	s3Client := NewS3Store()
	lustreStore, err := NewLustreStore(
		ulogger.TestLogger{},
		s3Client,
		tempDir,
		"persist",
		options.WithHeader(header),
		options.WithFooter(footerOpt),
	)
	require.NoError(t, err)

	// Set filename option to match our test file
	opts := []options.FileOption{
		options.WithFilename("testfile"),
	}

	// Request only the first 16 bytes of data (after header)
	bytesToGet := 16
	result, err := lustreStore.GetHead(context.Background(), hash, bytesToGet, opts...)
	require.NoError(t, err)
	require.NotNil(t, result)

	// Verify the result contains only the requested data (first 16 bytes of our data section)
	require.Equal(t, bytesToGet, len(result), "Should return exactly the requested number of bytes")

	for i := 0; i < bytesToGet; i++ {
		require.Equal(t, data[i], result[i], "Data at index %d should match", i)
	}

	// Create a very large file that would be inefficient to read entirely
	veryLargeFilePath := filepath.Join(tempDir, "verylargefile")
	veryLargeHash := []byte("verylargefile")

	// Write header to the large file
	f, err := os.Create(veryLargeFilePath)
	require.NoError(t, err)

	_, err = f.Write(header)
	require.NoError(t, err)

	// Write a portion of data that we'll request
	testData := make([]byte, 100)
	for i := 0; i < len(testData); i++ {
		testData[i] = byte(i)
	}

	_, err = f.Write(testData)
	require.NoError(t, err)

	// Add more data to make the file very large
	extraData := make([]byte, 1024*1024*10) // 10MB
	_, err = f.Write(extraData)
	require.NoError(t, err)

	// Add footer at the end
	_, err = f.Write(footer)
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)

	// Set options with filename matching the large file
	largeFileOpts := []options.FileOption{
		options.WithFilename("verylargefile"),
	}

	// Request only the first 50 bytes after the header
	bytesToGetFromLargeFile := 50
	largeFileResult, err := lustreStore.GetHead(context.Background(), veryLargeHash, bytesToGetFromLargeFile, largeFileOpts...)
	require.NoError(t, err)
	require.NotNil(t, largeFileResult)

	// Verify the result contains only the requested data
	require.Equal(t, bytesToGetFromLargeFile, len(largeFileResult), "Should return exactly the requested number of bytes from large file")

	for i := 0; i < bytesToGetFromLargeFile; i++ {
		require.Equal(t, testData[i], largeFileResult[i], "Data at index %d should match in large file test", i)
	}
}
