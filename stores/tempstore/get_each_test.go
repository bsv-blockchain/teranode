package tempstore

import (
	"fmt"
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

func TestBadgerTempStore_GetEach(t *testing.T) {
	store, err := New(Options{BasePath: t.TempDir(), Prefix: "geteach"})
	require.NoError(t, err)

	defer store.Close()

	keys := make([][]byte, 0, 100)

	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("k%03d", i))
		keys = append(keys, k)

		if i%10 != 0 {
			require.NoError(t, store.Put(k, []byte(fmt.Sprintf("v%03d", i))))
		}
	}

	seen := 0

	err = store.GetEach(len(keys), func(i int) []byte { return keys[i] }, func(i int, value []byte) error {
		seen++

		if i%10 == 0 {
			require.Nil(t, value, "key %d should be absent", i)
		} else {
			require.Equal(t, fmt.Sprintf("v%03d", i), string(value))
		}

		return nil
	})
	require.NoError(t, err)
	require.Equal(t, len(keys), seen)
}

func TestBadgerTempStore_GetEachStopsOnError(t *testing.T) {
	store, err := New(Options{BasePath: t.TempDir(), Prefix: "geteach"})
	require.NoError(t, err)

	defer store.Close()

	require.NoError(t, store.Put([]byte("a"), []byte("1")))

	stop := errors.NewProcessingError("stop")
	calls := 0

	err = store.GetEach(3, func(int) []byte { return []byte("a") }, func(int, []byte) error {
		calls++
		return stop
	})
	require.ErrorIs(t, err, stop)
	require.Equal(t, 1, calls)
}
