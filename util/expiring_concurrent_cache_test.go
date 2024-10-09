package util

import (
	"testing"
	"time"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpiringConcurrentCache_GetOrSet(t *testing.T) {
	cache := NewExpiringConcurrentCache[chainhash.Hash, int](120 * time.Second)
	key, err := chainhash.NewHashFromStr("1")
	require.NoError(t, err)

	key2, err := chainhash.NewHashFromStr("2")
	require.NoError(t, err)

	var counter int

	// Test the case where the value is already in the cache
	for i := 0; i < 1_000_000; i++ {
		go func() {
			val, err := cache.GetOrSet(*key, func() (int, error) {
				counter++
				return 1, nil
			})
			require.NoError(t, err)
			require.Equal(t, 1, val)
		}()
		go func() {
			val, err := cache.GetOrSet(*key2, func() (int, error) {
				counter++
				return 2, nil
			})
			require.NoError(t, err)
			require.Equal(t, 2, val)
		}()
	}

	assert.Equal(t, 2, counter)
}
