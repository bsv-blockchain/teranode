//go:build test_all || test_util

package util

import (
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// go test -v -tags test_util ./test/...

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
		for i := 0; i < 100_000; i++ {
			g.Go(func() error {
				val, err := cache.GetOrSet(*key, func() (int, bool, error) {
					counter++
					return 1, true, nil
				})
				require.NoError(t, err)
				require.Equal(t, 1, val)

				return nil
			})
			g.Go(func() error {
				val, err := cache.GetOrSet(*key2, func() (int, bool, error) {
					counter++
					return 2, true, nil
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
		for i := 0; i < 100_000; i++ {
			g.Go(func() error {
				val, err := cache.GetOrSet(*key, func() (int, bool, error) {
					counter++
					return 1, false, errors.NewStorageError("error file does not exist")
				})
				require.Error(t, err)
				require.Equal(t, cache.ZeroValue, val)

				return nil
			})
		}

		err = g.Wait()
		require.NoError(t, err)

		assert.Greater(t, counter, 1, "should have tried more than once to read the value")
	})

	t.Run("disallow caching", func(t *testing.T) {
		cache := NewExpiringConcurrentCache[chainhash.Hash, int](120 * time.Second)
		key, err := chainhash.NewHashFromStr("1")
		require.NoError(t, err)

		var counter int

		g := errgroup.Group{}

		// Test the case where the value is already in the cache
		for i := 0; i < 100_000; i++ {
			g.Go(func() error {
				val, err := cache.GetOrSet(*key, func() (int, bool, error) {
					counter++
					return 1, false, nil
				})
				require.NoError(t, err)
				require.Equal(t, 1, val)

				return nil
			})
		}

		err = g.Wait()
		require.NoError(t, err)

		// check that the item was retrieved more than once, since caching is disabled
		assert.Greater(t, counter, 1)
	})

}
