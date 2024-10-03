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
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"

	"github.com/bitcoin-sv/ubsv/ulogger"
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
		f, err := NewLustreStore(ulogger.TestLogger{}, s3Client, "/tmp/ubsv-tests", "persist")
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
		f, err := NewLustreStore(ulogger.TestLogger{}, s3Client, "/tmp/ubsv-tests", "persist")
		require.NoError(t, err)

		s3Client.Set(nil, fs.ErrNotExist)

		filename := "/tmp/ubsv-tests/79656b"
		persistFilename := "/tmp/ubsv-tests/persist/79656b"

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
	url, err := url.ParseRequestURI("lustre://s3.com/ubsv?localDir=/data/subtrees&localPersist=s3")
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

func TestFileNameForPersist(t *testing.T) {
	url, err := url.ParseRequestURI("lustre://s3.com/ubsv")
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, url, "data", "persist", options.WithHashPrefix(2), options.WithSubDirectory("sub"))
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

		status, message, err := store.Health(context.Background())
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

		status, message, err := store.Health(context.Background())
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

		status, message, err := store.Health(context.Background())
		assert.Equal(t, http.StatusServiceUnavailable, status)
		assert.Contains(t, message, "Persist subdirectory issue")
		assert.NoError(t, err)
	})

	t.Run("S3 client issue", func(t *testing.T) {
		s3Client := NewUnhealthyS3StoreMock()
		store, err := NewLustreStore(ulogger.TestLogger{}, s3Client, tempDir, persistDir)
		require.NoError(t, err)

		status, message, err := store.Health(context.Background())
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

		status, message, err := store.Health(context.Background())
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
