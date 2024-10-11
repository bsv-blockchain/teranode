package util

import (
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestExpiringConcurrentCache_GetOrSet(t *testing.T) {
	t.Run("value is already in the cache", func(t *testing.T) {
		cache := NewExpiringConcurrentCache[chainhash.Hash, int](120 * time.Second)
		key, err := chainhash.NewHashFromStr("1")
		require.NoError(t, err)

		key2, err := chainhash.NewHashFromStr("2")
		require.NoError(t, err)

		var counter int

		g := errgroup.Group{}

		// Test the case where the value is already in the cache
		for i := 0; i < 1_000_000; i++ {
			g.Go(func() error {
				val, err := cache.GetOrSet(*key, func() (int, error) {
					counter++
					return 1, nil
				})
				require.NoError(t, err)
				require.Equal(t, 1, val)

				return nil
			})
			g.Go(func() error {
				val, err := cache.GetOrSet(*key2, func() (int, error) {
					counter++
					return 2, nil
				})
				require.NoError(t, err)
				require.Equal(t, 2, val)

				return nil
			})
		}

		err = g.Wait()
		require.NoError(t, err)

		assert.Equal(t, 2, counter)
	})

	t.Run("missing value", func(t *testing.T) {
		cache := NewExpiringConcurrentCache[chainhash.Hash, int](120 * time.Second)
		key, err := chainhash.NewHashFromStr("1")
		require.NoError(t, err)

		var counter int

		g := errgroup.Group{}

		// Test the case where the value is already in the cache
		for i := 0; i < 1_000_000; i++ {
			g.Go(func() error {
				val, err := cache.GetOrSet(*key, func() (int, error) {
					counter++
					return 1, errors.NewStorageError("error file does not exist")
				})
				require.Error(t, err)
				require.Equal(t, cache.zeroValue, val)

				return nil
			})
		}

		err = g.Wait()
		require.NoError(t, err)

		assert.Greater(t, counter, 1, "should have tried more than once to read the value")
	})
}
