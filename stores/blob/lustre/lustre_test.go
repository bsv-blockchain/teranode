package lustre

import (
	"bytes"
	"context"
	"io"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/blob/options"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	s3Url, _ = url.Parse("s3://s3.com/ubsv-test-bucket")
)

func TestFile_Get(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, s3Url, "/tmp/ubsv-tests", "persist")
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

	t.Run("ttl", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, s3Url, "/tmp/ubsv-tests", "persist")
		require.NoError(t, err)

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

		value, err = f.Get(context.Background(), []byte("key"))
		require.NoError(t, err)
		require.Equal(t, []byte("value"), value)

		err = f.Del(context.Background(), []byte("key"))
		require.NoError(t, err)
	})
}

func TestFile_GetFromReader(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, s3Url, "/tmp/ubsv-tests-reader", "persist")
		require.NoError(t, err)

		err = f.Set(context.Background(), []byte("key"), []byte("value"))
		require.NoError(t, err)

		value, err := f.Get(context.Background(), []byte("key"))
		require.NoError(t, err)

		require.Equal(t, []byte("value"), value)

		err = f.Del(context.Background(), []byte("key"))
		require.NoError(t, err)
	})

	t.Run("ttl", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, s3Url, "/tmp/ubsv-tests-reader", "persist")
		require.NoError(t, err)

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

		// cleanup
		err = f.Del(context.Background(), []byte("key"))
		require.NoError(t, err)
	})
}

func TestFile_filename(t *testing.T) {
	t.Run("filename", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, s3Url, "/tmp/ubsv-tests", "persist")
		require.NoError(t, err)

		filename := f.filename([]byte("key"))
		assert.Equal(t, "/tmp/ubsv-tests/79656b", filename)
		persistFilename := f.getFileNameForPersist(filename)
		assert.Equal(t, "/tmp/ubsv-tests/persist/79656b", persistFilename)
	})
	t.Run("getFileNameForGet", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, s3Url, "/tmp/ubsv-tests", "persist")
		require.NoError(t, err)

		filename, err := f.getFileNameForGet([]byte("key"))
		assert.NoError(t, err)
		assert.Equal(t, "/tmp/ubsv-tests/79656b", filename)
		persistFilename := f.getFileNameForPersist(filename)
		assert.Equal(t, "/tmp/ubsv-tests/persist/79656b", persistFilename)
	})
	t.Run("getFileNameForGet with extension", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, s3Url, "/tmp/ubsv-tests", "persist")
		require.NoError(t, err)

		filename, err := f.getFileNameForGet([]byte("key"), options.WithFileExtension("meta"))
		assert.NoError(t, err)
		assert.Equal(t, "/tmp/ubsv-tests/79656b.meta", filename)
		persistFilename := f.getFileNameForPersist(filename)
		assert.Equal(t, "/tmp/ubsv-tests/persist/79656b.meta", persistFilename)
	})
}
