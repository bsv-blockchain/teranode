package file

import (
	"context"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/rand"
	"os"
	"testing"
)

var testDir = "/tmp/ubsv-tests/" + rand.String(12)

func cleanup() {
	_ = os.RemoveAll(testDir)
}

func TestFile_Get(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, testDir)
		require.NoError(t, err)

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

func TestFile_filename(t *testing.T) {
	t.Run("1 path", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, testDir)
		require.NoError(t, err)

		filename := f.filename([]byte("key"))
		assert.Equal(t, testDir+"/79656b", filename)

		cleanup()
	})

	t.Run("1 paths", func(t *testing.T) {
		f, err := New(ulogger.TestLogger{}, testDir, []string{"/tmp/ubsv-tests1"})
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
		f, err := New(ulogger.TestLogger{}, testDir, []string{"/tmp/ubsv-tests1", "/tmp/ubsv-tests2"})
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
		f, err := New(ulogger.TestLogger{}, testDir, []string{
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
