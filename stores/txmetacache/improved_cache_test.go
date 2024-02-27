package txmetacache

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
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
	_ = cache.Get(&dst, []byte("key"))
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
	_ = cache.Get(&dst, key)
	require.Equal(t, value, dst)
}

func TestImprovedCache_GetSetMultiKeysSingleValue(t *testing.T) {
	cache := NewImprovedCache(1 * 1024 * 2) //100 * 1024 * 1024)
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
		// if err != nil {
		// 	fmt.Println("FIRST error at index:", i/chainhash.HashSize, "error:", err)
		// }
		// else {
		// 	fmt.Println("FIRST value at index:", i/chainhash.HashSize, "value:", dst)
		// }
	}

	fmt.Println("\n\n\n Second Part")

	err = cache.SetMultiKeysSingleValueAppended(allKeys, valueSecond, chainhash.HashSize)
	require.NoError(t, err)

	for i := 0; i < len(allKeys); i += chainhash.HashSize {
		dst := make([]byte, 0)
		err = cache.Get(&dst, allKeys[i:i+chainhash.HashSize])
		if err != nil {
			fmt.Println("SECOND error at index:", i/chainhash.HashSize, "error:", err)
		}
		// else {
		// 	fmt.Println("SECOND value at index:", i/chainhash.HashSize, "value:", dst)
		// }
		//require.Equal(t, []byte("secondfirst"), dst)
	}

}

func TestImprovedCache_GetSetMultiKeyAppended(t *testing.T) {
	cache := NewImprovedCache(100 * 1024 * 1024)
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

/*
	func TestImprovedCache_SetMulti(t *testing.T) {
		cache := NewImprovedCache(200 * 1024 * 1024)
		allKeys := make([][]byte, 0)
		allValues := make([][]byte, 0)
		var err error
		numberOfKeys := 100 * bucketsCount

		// cache size : 2048 * 1024 * 1024 bytes -> 2GB -> 2048 MB
		// number of buckets: 512
		// bucket size: 2048 MB / 512 = 4096 KB -> 4 MB
		// chunk size: 2 * 1024 * 16 = 32 KB
		// number of total chunks: 2 GB / 32 KB = 65536 chunks
		// number of chunks per bucket: 65536 / 512 = 128 chunks per bucket
		// key size: 32 bytes, value size: 32 bytes, kvLen: 4 bytes. Total size: 68 bytes for each key-value pair
		// number of key-value pairs per chunk: 32 KB / 68 bytes = 470 key-value pairs per chunk
		// number of key-value pairs per bucket: 470 * 128 = 60160 key-value pairs per bucket

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
*/
func TestImprovedCache_Trimming(t *testing.T) {
	cache := NewImprovedCache(1 * 1024 * 1024) // (256 * 1024 * 1024)
	allKeys := make([][]byte, 0)
	allValues := make([][]byte, 0)
	var err error
	//numberOfKeys := (3760 * bucketsCount) + 1
	numberOfKeys := (2000 * bucketsCount)

	// cache size : 128 * 1024 * 1024 bytes -> 128 KB
	// number of buckets: 512
	// bucket size: 128 MB / 512 = 256 KB
	// chunk size: 2 * 1024 * 16 = 32 KB
	// number of total chunks: 128MB / 32 KB = 4096 chunks
	// number of chunks per bucket: 4096 / 512 = 8 chunks per bucket
	// key size: 32 bytes, value size: 32 bytes, kvLen: 4 bytes. Total size: 68 bytes for each key-value pair
	// number of key-value pairs per chunk: 32 KB / 68 bytes = 470 key-value pairs per chunk
	// number of key-value pairs per bucket: 470 * 8 = 3760 key-value pairs per bucket -> even if everything is perfectly balanced

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
	require.NotZero(t, errCounter)
}

func TestImprovedCache_SetMulti(t *testing.T) {
	cache := NewImprovedCache(1 * 1024 * 2) // 100 * 1024 * 1024
	allKeys := make([][]byte, 0)
	allValues := make([][]byte, 0)
	var err error
	numberOfKeys := 15 //100 * bucketsCount

	// cache size : 1 * 1024 * 2 bytes -> 2 KB
	// number of buckets: 4
	// max bucket size: 512 bytes -> 512 * 4 = 2048 bytes -> 2 KB
	// chunk size: 2 * 64 = 128 bytes
	// number of total chunks: 2 KB / 128 bytes = 16 chunks
	// number of chunks per bucket: 16 / 4 = 4 chunks per bucket
	// max value size in KB: 2

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
			//fmt.Println("error at index:", i, "error:", err)
			errCounter++
		}

		//require.NoError(t, err)
		//require.Equal(t, allValues[i], dst)
	}
	fmt.Println("errors:", errCounter)

	// err = cache.SetMulti(allKeys, allValues)
	// require.NoError(t, err)

	// for i, key := range allKeys {
	// 	dst := make([]byte, 0)
	// 	err := cache.Get(&dst, key)
	// 	require.NoError(t, err)
	// 	require.Equal(t, append(allValues[i], allValues[i]...), dst)
	// }
}
