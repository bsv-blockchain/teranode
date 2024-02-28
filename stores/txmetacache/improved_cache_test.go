package txmetacache

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestImprovedCache_SetGetTest(t *testing.T) {
	// initialize improved cache with 1MB capacity
	cache := NewImprovedCache(256 * 1024 * 1024)
	cache.Set([]byte("key"), []byte("value"))
	dst := make([]byte, 0)
	_ = cache.Get(&dst, []byte("key"))
	require.Equal(t, []byte("value"), dst)

}

func TestImprovedCache_SetGetTestUnallocated(t *testing.T) {
	// initialize improved cache with 1MB capacity
	cache := NewImprovedCache(1*1024*1024, true)
	cache.Set([]byte("key"), []byte("value"))
	dst := make([]byte, 0)
	_ = cache.Get(&dst, []byte("key"))
	require.Equal(t, []byte("value"), dst)

}
func TestImprovedCache_GetBigKV(t *testing.T) {
	cache := NewImprovedCache(256 * 1024 * 1024)
	key, value := make([]byte, (1*1024)), make([]byte, (1*1024))
	binary.LittleEndian.PutUint64(key, uint64(0))
	hash := chainhash.Hash(key)
	key = hash[:]
	_, err := rand.Read(value)
	require.NoError(t, err)
	cache.Set(key, value)

	// get the value
	dst := make([]byte, 0)
	_ = cache.Get(&dst, key)
	require.Equal(t, value, dst)
}

func TestImprovedCache_GetBigKVUnallocated(t *testing.T) {
	cache := NewImprovedCache(256*1024*1024, true)
	key, value, tooBigValue := make([]byte, (2048)), make([]byte, (2047)), make([]byte, (2048))
	binary.LittleEndian.PutUint64(key, uint64(0))
	hash := chainhash.Hash(key)
	key = hash[:]
	_, err := rand.Read(value)
	require.NoError(t, err)
	err = cache.Set(key, value)
	require.NoError(t, err)

	_, err = rand.Read(tooBigValue)
	require.NoError(t, err)
	err = cache.Set(key, tooBigValue)
	require.Error(t, err)

	// get the value
	dst := make([]byte, 0)
	err = cache.Get(&dst, key)
	require.NoError(t, err)
	require.Equal(t, value, dst)
}

func TestImprovedCache_GetSetMultiKeysSingleValue(t *testing.T) {
	cache := NewImprovedCache(1*1024*2, true) //100 * 1024 * 1024)
	allKeys := make([]byte, 0)
	value := []byte("first")
	valueSecond := []byte("second")
	numberOfKeys := 6 //2_00 * bucketsCount

	for i := 0; i < numberOfKeys; i++ {
		key := make([]byte, 32)
		_, err := rand.Read(key)
		require.NoError(t, err)
		allKeys = append(allKeys, key...)
	}

	startTime := time.Now()
	err := cache.SetMultiKeysSingleValueAppended(allKeys, value, chainhash.HashSize)
	require.NoError(t, err)
	t.Log("SetMultiKeysSingleValue took:", time.Since(startTime))

	for i := 0; i < len(allKeys); i += chainhash.HashSize {
		dst := make([]byte, 0)
		err = cache.Get(&dst, allKeys[i:i+chainhash.HashSize])
		require.NoError(t, err)
		require.Equal(t, value, dst)
	}

	err = cache.SetMultiKeysSingleValueAppended(allKeys, valueSecond, chainhash.HashSize)
	require.NoError(t, err)

	for i := 0; i < len(allKeys); i += chainhash.HashSize {
		dst := make([]byte, 0)
		err = cache.Get(&dst, allKeys[i:i+chainhash.HashSize])
		if err != nil {
			fmt.Println("SECOND error at index:", i/chainhash.HashSize, "error:", err)
		}
	}

}

func TestImprovedCache_GetSetMultiKeyAppended(t *testing.T) {
	// We test appending performance, so we will use unallocated cache
	cache := NewImprovedCache(100*1024*1024, true)
	allKeys := make([][]byte, 0)
	key := make([]byte, 32)
	numberOfKeys := 2_000 * bucketsCount

	for i := 0; i < numberOfKeys; i++ {
		_, err := rand.Read(key)
		require.NoError(t, err)
		allKeys = append(allKeys, key)
	}
	startTime := time.Now()
	//var prevValue []byte
	for i := 0; i < numberOfKeys; i++ {
		val := make([]byte, 0) // Create a new slice for each iteration
		_ = cache.Get(&val, allKeys[i])
		cache.Set(allKeys[i], []byte("valuevalue"))
	}
	t.Log("Set took:", time.Since(startTime))

	for i := 0; i < numberOfKeys; i++ {
		dst := make([]byte, 0)
		_ = cache.Get(&dst, allKeys[i])
		require.Equal(t, []byte("valuevalue"), dst)
	}
}

func TestImprovedCache_SetMulti(t *testing.T) {
	util.SkipVeryLongTests(t)

	cache := NewImprovedCache(256 * 1024 * 1024)
	allKeys := make([][]byte, 0)
	allValues := make([][]byte, 0)
	var err error
	numberOfKeys := 1_000 * bucketsCount

	// cache size : 256 * 1024 * 1024 bytes -> 256 MB
	// number of buckets: 1024
	// bucket size: 256 MB / 1024 = 256 KB
	// chunk size: 2 * 1024 * 16 = 32 KB
	// 256 KB / 32 KB = 8 chunks per bucket
	// 32 KB to bytes = 32 * 1024 = 32768 bytes
	// 32768 / 68  = 481 key-value pairs per chunk
	// 481 * 8 = 3848 key-value pairs per bucket, at most.

	for i := 0; i < numberOfKeys; i++ {
		key := make([]byte, 32)
		value := make([]byte, 32)
		_, err = rand.Read(key)
		require.NoError(t, err)
		allKeys = append(allKeys, key)
		binary.LittleEndian.PutUint64(value, uint64(i))
		allValues = append(allValues, value)
	}

	startTime := time.Now()
	err = cache.SetMulti(allKeys, allValues)
	require.NoError(t, err)
	t.Log("SetMulti took:", time.Since(startTime))

	for i, key := range allKeys {
		dst := make([]byte, 0)
		err = cache.Get(&dst, key)
		require.NoError(t, err)
		require.Equal(t, allValues[i], dst)
	}

	err = cache.SetMulti(allKeys, allValues)
	require.NoError(t, err)

	for i, key := range allKeys {
		dst := make([]byte, 0)
		err := cache.Get(&dst, key)
		require.NoError(t, err)
		require.Equal(t, allValues[i], dst)
	}
}

func TestImprovedCache_TestSetMultiWithExpectedMisses(t *testing.T) {
	util.SkipVeryLongTests(t)

	cache := NewImprovedCache(256 * 1024 * 1024)
	allKeys := make([][]byte, 0)
	allValues := make([][]byte, 0)
	var err error
	numberOfKeys := 3936256 + 1

	// cache size : 256 * 1024 * 1024 bytes -> 256 MB
	// number of buckets: 1024
	// bucket size: 256 MB / 1024 = 256 KB
	// chunk size: 2 * 1024 * 16 = 32 KB
	// 256 KB / 32 KB = 8 chunks per bucket
	// 32 KB to bytes = 32 * 1024 = 32768 bytes
	// 32768 / 68  = 481 key-value pairs per chunk
	// 481 * 8 = 3848 key-value pairs per bucket
	// 3848 * 1024 = 3936256 key-value pairs per cache

	for i := 0; i < numberOfKeys; i++ {
		key := make([]byte, 32)
		value := make([]byte, 32)
		_, err = rand.Read(key)
		require.NoError(t, err)
		allKeys = append(allKeys, key)
		binary.LittleEndian.PutUint64(value, uint64(i))
		allValues = append(allValues, value)
	}

	startTime := time.Now()
	err = cache.SetMulti(allKeys, allValues)
	require.NoError(t, err)
	t.Log("SetMulti took:", time.Since(startTime))

	errCounter := 0
	for _, key := range allKeys {
		dst := make([]byte, 0)
		err = cache.Get(&dst, key)
		if err != nil {
			errCounter++
		}
	}
	fmt.Println("errors:", errCounter)

	// X times call cleanLockedMap
	// 2 chunks are deleted per adjustment
	// there are 481 keys per chunk: 32768 bytes per chunk
	// 1 adjustment = 481 * 2 = 962 keys are deleted.

	require.NotZero(t, errCounter)
}
