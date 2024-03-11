package txmetacache

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/types"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func init() {
	go func() {
		log.Println("Starting pprof server on http://localhost:6060")
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
}

func TestImprovedCache_SetGetTest(t *testing.T) {
	// skip due to size requirements of the cache, use cache size / 1024 and number of buckets / 1024 for testing
	util.SkipVeryLongTests(t)
	// initialize improved cache with 1MB capacity
	cache := NewImprovedCache(256*1024*1024, types.Unallocated)
	err := cache.Set([]byte("key"), []byte("value"))
	require.NoError(t, err)
	dst := make([]byte, 0)
	err = cache.Get(&dst, []byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), dst)
}

func TestImprovedCache_SetGetTestUnallocated(t *testing.T) {
	// skip due to size requirements of the cache, use cache size / 1024 and number of buckets / 1024 for testing
	util.SkipVeryLongTests(t)
	// initialize improved cache with 1MB capacity
	cache := NewImprovedCache(256*1024*1024, types.Unallocated)
	err := cache.Set([]byte("key"), []byte("value"))
	require.NoError(t, err)
	dst := make([]byte, 0)
	_ = cache.Get(&dst, []byte("key"))
	require.Equal(t, []byte("value"), dst)
}
func TestImprovedCache_GetBigKV(t *testing.T) {
	// skip due to size requirements of the cache, use cache size / 1024 and number of buckets / 1024 for testing
	util.SkipVeryLongTests(t)
	cache := NewImprovedCache(256*1024*1024, types.Unallocated)
	key, value := make([]byte, (1*1024)), make([]byte, (1*1024))
	binary.LittleEndian.PutUint64(key, uint64(0))
	hash := chainhash.Hash(key)
	key = hash[:]
	_, err := rand.Read(value)
	require.NoError(t, err)
	err = cache.Set(key, value)
	require.NoError(t, err)

	// get the value
	dst := make([]byte, 0)
	err = cache.Get(&dst, key)
	require.NoError(t, err)
	require.Equal(t, value, dst)
}

func TestImprovedCache_GetBigKVUnallocated(t *testing.T) {
	// skip due to size requirements of the cache, use cache size / 1024 and number of buckets / 1024 for testing
	util.SkipVeryLongTests(t)
	cache := NewImprovedCache(256*1024*1024, types.Unallocated)
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
	// skip due to size requirements of the cache, use cache size / 1024 and number of buckets / 1024 for testing
	util.SkipVeryLongTests(t)
	cache := NewImprovedCache(256*1024*1024, types.Unallocated) //100 * 1024 * 1024)
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
	// skip due to size requirements of the cache, use cache size / 1024 and number of buckets / 1024 for testing
	util.SkipVeryLongTests(t)
	// We test appending performance, so we will use unallocated cache
	cache := NewImprovedCache(256*1024*1024, types.Unallocated)
	allKeys := make([][]byte, 0)
	key := make([]byte, 32)
	numberOfKeys := 2_000 * bucketsCount
	var err error

	for i := 0; i < numberOfKeys; i++ {
		_, err := rand.Read(key)
		require.NoError(t, err)
		allKeys = append(allKeys, key)
	}

	startTime := time.Now()
	for i := 0; i < numberOfKeys; i++ {
		val := make([]byte, 0) // Create a new slice for each iteration
		err = cache.Set(allKeys[i], []byte("valuevalue"))
		require.NoError(t, err)
		err = cache.Get(&val, allKeys[i])
		require.NoError(t, err)
	}
	t.Log("Set took:", time.Since(startTime))

	for i := 0; i < numberOfKeys; i++ {
		dst := make([]byte, 0)
		_ = cache.Get(&dst, allKeys[i])
		require.Equal(t, []byte("valuevalue"), dst)
	}
}

func TestImprovedCache_SetMulti(t *testing.T) {
	// skip due to size requirements of the cache, use cache size / 1024 and number of buckets / 1024 for testing
	//util.SkipVeryLongTests(t)
	cache := NewImprovedCache(128*1024*1024, types.Trimmed)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	t.Logf("0) Total memory used: %v kilobytes", m.Alloc/(1024*1024))
	allKeys := make([][]byte, 0)
	allValues := make([][]byte, 0)
	var err error
	numberOfKeys := 1_000 * bucketsCount

	// cache size : 128 * 1024 * 1024 bytes -> 128 MB // 128 GB in the Scaling
	// number of buckets: 8 // 8 * 1024 = 8192 in the scaling
	// bucket size: 128 MB / 8 = 16 MB per bucket // 128 GB / (8*1024) = 16 MB per bucket
	// chunk size: 4 KB
	// 16 MB / 4 KB = 4096 chunks per bucket

	// f, _ := os.Create("mem.prof")
	// defer f.Close()

	f, err := os.Create("mem2.prof")
	if err != nil {
		t.Fatalf("could not create memory profile: %v", err)
	}
	defer f.Close()

	for i := 0; i < numberOfKeys; i++ {
		key := make([]byte, 32)
		value := make([]byte, 32)
		_, err = rand.Read(key)
		require.NoError(t, err)
		allKeys = append(allKeys, key)
		binary.LittleEndian.PutUint64(value, uint64(i))
		allValues = append(allValues, value)
	}

	runtime.ReadMemStats(&m)
	t.Logf("1) Total memory used: %v kilobytes", m.Alloc/(1024*1024))

	startTime := time.Now()
	err = cache.SetMulti(allKeys, allValues)
	require.NoError(t, err)
	t.Log("SetMulti took:", time.Since(startTime))

	//var m runtime.MemStats
	runtime.ReadMemStats(&m)
	t.Logf("2) Total memory used: %v kilobytes", m.Alloc/(1024*1024))

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

	err = pprof.WriteHeapProfile(f)
	if err != nil {
		t.Fatalf("could not write memory profile: %v", err)
	}

	//var m runtime.MemStats
	runtime.ReadMemStats(&m)
	t.Logf("2) Total memory used: %v kilobytes", m.Alloc/(1024*1024))

	s := &Stats{}
	cache.UpdateStats(s)
	fmt.Println("Stats, total map size:", s.TotalMapSize)
}

func TestImprovedCache_TestSetMultiWithExpectedMisses(t *testing.T) {
	util.SkipVeryLongTests(t)
	cache := NewImprovedCache(128*1024*1024, types.Trimmed)
	allKeys := make([][]byte, 0)
	allValues := make([][]byte, 0)
	var err error
	numberOfKeys := 3_000_000

	// cache size : 128 * 1024 * 1024 bytes -> 128 MB // 128 GB in the Scaling
	// number of buckets: 8 // 8 * 1024 = 8192 in the scaling
	// bucket size: 128 MB / 8 = 16 MB per bucket // 128 GB / (8*1024) = 16 MB per bucket
	// chunk size: 4 KB
	// 16 MB / 4 KB = 4096 chunks per bucket
	// 4096 bytes / 68 bytes = 60 key-value pairs per chunk
	// 60 * 8192 = 491520 key-value pairs per bucket
	// 491520 * 8 = 3932160 key-value pairs per cache

	for i := 0; i < numberOfKeys; i++ {
		// convert int i to byte array element key
		key := make([]byte, 32)
		_, err = rand.Read(key)
		require.NoError(t, err)
		allKeys = append(allKeys, key)

		value := make([]byte, 32)
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
	fmt.Println("Cache Number or errors:", errCounter)

	// X times call cleanLockedMap
	// 2 chunks are deleted per adjustment
	// there are 481 keys per chunk: 32768 bytes per chunk
	// 1 adjustment = 481 * 2 = 962 keys are deleted.
	require.NotZero(t, errCounter)

	s := &Stats{}
	cache.UpdateStats(s)
	fmt.Println("Stats, total map size:", s.TotalMapSize)
}

func TestImprovedCache_TestSetMultiWithExpectedMisses_Small(t *testing.T) {
	// skipping this test, because config is not suitable for it.
	// It is necessary to keep the test for manual inspection with suitable config.
	util.SkipVeryLongTests(t)

	cache := NewImprovedCache(4*1024, types.Trimmed)
	allKeys := make([][]byte, 0)
	allValues := make([][]byte, 0)
	var err error
	numberOfKeys := 50

	// cache size : 4 KB
	// 4 buckets
	// bucket size: 4 KB / 4 = 1 KB
	// chunk size: 2 * 128 = 256 bytes
	// 1 KB / 256 bytes = 4 chunks per bucket
	// 256 bytes / 68  = 3 key-value pairs per chunk
	// 3 * 4 = 12 key-value pairs per bucket
	// 12 * 4 = 48 key-value pairs per cache

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
	t.Log("Number or errors:", errCounter)

	// X times call cleanLockedMap
	// 2 chunks are deleted per adjustment
	// there are 481 keys per chunk: 32768 bytes per chunk
	// 1 adjustment = 481 * 2 = 962 keys are deleted.
	require.NotZero(t, errCounter)

	s := &Stats{}
	cache.UpdateStats(s)
	t.Log("Stats, total map size:", s.TotalMapSize)
}
