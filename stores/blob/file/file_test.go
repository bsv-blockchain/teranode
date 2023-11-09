package file

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFile_Get(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		f, err := New("/tmp/ubsv-tests")
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
