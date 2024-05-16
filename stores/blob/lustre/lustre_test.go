package lustre

import (
	"context"
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

		err = f.Set(context.Background(), []byte("key"), []byte("value"))
		require.NoError(t, err)

		value, err := f.Get(context.Background(), []byte("key"))
		require.NoError(t, err)

		require.Equal(t, []byte("value"), value)

		err = f.Del(context.Background(), []byte("key"))
		require.NoError(t, err)
	})

	t.Run("ttl", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, s3Url, "/tmp/ubsv-tests", "persist")
		require.NoError(t, err)

		filename := "/tmp/ubsv-tests/79656b"
		persistFilename := "/tmp/ubsv-tests/persist/79656b"

		err = f.Set(context.Background(), []byte("key"), []byte("value"), options.WithTTL(1*time.Minute))
		require.NoError(t, err)

		value, err := f.Get(context.Background(), []byte("key"))
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

		value, err = f.Get(context.Background(), []byte("key"))
		require.NoError(t, err)
		require.Equal(t, []byte("value"), value)

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
