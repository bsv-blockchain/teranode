package txmetacache

import (
	"crypto/rand"
	"encoding/binary"
	"testing"
	"time"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestImprovedCache_Get(t *testing.T) {
	// initialize improved cache with 1MB capacity
	cache := NewImprovedCache(1 * 1024 * 1024)
	cache.Set([]byte("key"), []byte("value"))
	dst := make([]byte, 0)
	cache.Get(&dst, []byte("key"))
	require.Equal(t, []byte("value"), dst)

}

func TestImprovedCache_GetBigKV(t *testing.T) {
	cache := NewImprovedCache(1 * 1024 * 1024)
	key, value := make([]byte, (1*1024)), make([]byte, (1*1024))
	binary.LittleEndian.PutUint64(key, uint64(0))
	hash := chainhash.Hash(key)
	key = hash[:]
	_, err := rand.Read(value)
	require.NoError(t, err)
	cache.Set(key, value)

	// get the value
	dst := make([]byte, 0)
	cache.Get(&dst, key)
	require.Equal(t, value, dst)
}

func TestImprovedCache_SetMulti(t *testing.T) {
	cache := NewImprovedCache(100 * 1024 * 1024)
	allKeys := make([]byte, 0)
	value := []byte("value")
	for i := 0; i < bucketsCount*1_000; i++ {
		key := make([]byte, 32)
		_, err := rand.Read(key)
		require.NoError(t, err)
		allKeys = append(allKeys, key...)
	}

	startTime := time.Now()
	cache.SetMulti(allKeys, value, chainhash.HashSize)
	t.Log("SetMulti took:", time.Since(startTime))

	for i := 0; i < len(allKeys); i += chainhash.HashSize {
		dst := make([]byte, 0)
		cache.Get(&dst, allKeys[i:i+chainhash.HashSize])
		require.Equal(t, value, dst)
	}
}
