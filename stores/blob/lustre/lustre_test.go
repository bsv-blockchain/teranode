package lustre

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFile_NilS3(t *testing.T) {
	t.Run("no s3", func(t *testing.T) {

		f, err := NewLustreStore(ulogger.TestLogger{}, nil, "/tmp/ubsv-tests", "persist")
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

func TestFile_Get(t *testing.T) {
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

func TestFile_GetFromReader(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		s3Client := NewS3Store()
		f, err := NewLustreStore(ulogger.TestLogger{}, s3Client, "/tmp/ubsv-tests-reader", "persist")
		require.NoError(t, err)

		s3Client.Set(nil, fs.ErrNotExist)

		err = f.Set(context.Background(), []byte("key"), []byte("value"))
		require.NoError(t, err)

		value, err := f.Get(context.Background(), []byte("key"))
		require.NoError(t, err)

		require.Equal(t, []byte("value"), value)

		err = f.Del(context.Background(), []byte("key"))
		require.NoError(t, err)

		_ = os.RemoveAll("/tmp/ubsv-tests-reader")
	})

	t.Run("ttl", func(t *testing.T) {
		_ = os.RemoveAll("/tmp/ubsv-tests-reader")

		s3Client := NewS3Store()
		f, err := NewLustreStore(ulogger.TestLogger{}, s3Client, "/tmp/ubsv-tests-reader", "persist")
		require.NoError(t, err)

		s3Client.Set(nil, fs.ErrNotExist)

		filename := "/tmp/ubsv-tests-reader/79656b"
		persistFilename := "/tmp/ubsv-tests-reader/persist/79656b"

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

		// file should have been rempved from persisted folder
		_, err = os.Stat(persistFilename)
		require.Error(t, err)

		// cleanup
		err = f.Del(context.Background(), []byte("key"))
		require.NoError(t, err)

		_ = os.RemoveAll("/tmp/ubsv-tests-reader")
	})
}

func TestFile_filename(t *testing.T) {
	t.Run("filename", func(t *testing.T) {
		s3Client := NewS3Store()
		f, err := NewLustreStore(ulogger.TestLogger{}, s3Client, "/tmp/ubsv-tests", "persist")
		require.NoError(t, err)

		s3Client.Set(nil, fs.ErrNotExist)

		filename := f.filename([]byte("key"))
		assert.Equal(t, "/tmp/ubsv-tests/79656b", filename)
		persistFilename := f.getFileNameForPersist(filename)
		assert.Equal(t, "/tmp/ubsv-tests/persist/79656b", persistFilename)
	})
	t.Run("getFileNameForGet", func(t *testing.T) {
		s3Client := NewS3Store()
		f, err := NewLustreStore(ulogger.TestLogger{}, s3Client, "/tmp/ubsv-tests", "persist")
		require.NoError(t, err)

		s3Client.Set(nil, fs.ErrNotExist)

		filename, err := f.getFileNameForGet([]byte("key"))
		assert.NoError(t, err)
		assert.Equal(t, "/tmp/ubsv-tests/79656b", filename)
		persistFilename := f.getFileNameForPersist(filename)
		assert.Equal(t, "/tmp/ubsv-tests/persist/79656b", persistFilename)

		filename, err = f.getFileNameForGet([]byte("key"), options.WithFileName("filename-1234"))
		assert.NoError(t, err)
		assert.Equal(t, "/tmp/ubsv-tests/filename-1234", filename)

		filename, err = f.getFileNameForGet([]byte("key"), options.WithFileName("filename-1234"), options.WithSubDirectory("./data/"))
		assert.NoError(t, err)
		assert.Equal(t, "/tmp/ubsv-tests/data/filename-1234", filename)

		filename, err = f.getFileNameForGet([]byte("key"), options.WithFileName("filename-1234"), options.WithSubDirectory("/data/"))
		assert.NoError(t, err)
		assert.Equal(t, "/data/filename-1234", filename)
	})

	t.Run("getFileNameForGet with extension", func(t *testing.T) {
		s3Client := NewS3Store()
		f, err := NewLustreStore(ulogger.TestLogger{}, s3Client, "/tmp/ubsv-tests", "persist")
		require.NoError(t, err)

		s3Client.Set(nil, fs.ErrNotExist)

		filename, err := f.getFileNameForGet([]byte("key"), options.WithFileExtension("meta"))
		assert.NoError(t, err)
		assert.Equal(t, "/tmp/ubsv-tests/79656b.meta", filename)
		persistFilename := f.getFileNameForPersist(filename)
		assert.Equal(t, "/tmp/ubsv-tests/persist/79656b.meta", persistFilename)
	})
}

func TestFile_New(t *testing.T) {
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

func (s *s3StoreMock) Get(ctx context.Context, key []byte, opts ...options.Options) ([]byte, error) {
	return s.value, s.err
}

func (s *s3StoreMock) GetIoReader(ctx context.Context, key []byte, opts ...options.Options) (io.ReadCloser, error) {
	if s.value == nil {
		return nil, s.err
	}

	return io.NopCloser(bytes.NewReader(s.value)), s.err
}

func (s *s3StoreMock) Exists(ctx context.Context, key []byte, opts ...options.Options) (bool, error) {
	if s.value == nil {
		return false, s.err
	}

	return true, s.err
}
