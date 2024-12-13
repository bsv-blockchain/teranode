//go:build test_all || test_stores || test_txmetacache || test_longlong

package txmetacache

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"testing"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/txmetacache"
	"github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/types"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// go test -v -tags test_txmetacache ./test/...

func init() {
	go func() {
		log.Println("Starting pprof server on http://localhost:6060")
		//nolint:gosec // G114: Use of net/http serve function that has no support for setting timeouts (gosec)
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
}

func TestImprovedCache_SetGetTest(t *testing.T) {
	// initialize improved cache with 1MB capacity
	cache, _ := txmetacache.NewImprovedCache(256*1024*1024, types.Unallocated)
	err := cache.Set([]byte("key"), []byte("value"))
	require.NoError(t, err)
	dst := make([]byte, 0)
	err = cache.Get(&dst, []byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), dst)
}

func TestImprovedCache_SetGetTestUnallocated(t *testing.T) {
	// initialize improved cache with 1MB capacity
	cache, _ := txmetacache.NewImprovedCache(1*1024*1024, types.Unallocated)
	err := cache.Set([]byte("key"), []byte("value"))
	require.NoError(t, err)
	dst := make([]byte, 0)
	_ = cache.Get(&dst, []byte("key"))
	require.Equal(t, []byte("value"), dst)
}
func TestImprovedCache_GetBigKV(t *testing.T) {
	cache, _ := txmetacache.NewImprovedCache(1*1024*1024, types.Unallocated)
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
	cache, _ := txmetacache.NewImprovedCache(256*1024*1024, types.Unallocated)
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
	cache, _ := txmetacache.NewImprovedCache(256*1024*1024, types.Unallocated) //100 * 1024 * 1024)
	allKeys := make([]byte, 0)
	value := []byte("first")
	valueSecond := []byte("second")
	numberOfKeys := 6 //2_00 * BucketsCount

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
	cache, _ := txmetacache.NewImprovedCache(256*1024*1024, types.Unallocated)
	allKeys := make([][]byte, 0)
	key := make([]byte, 32)
	numberOfKeys := 2_000 * txmetacache.BucketsCount
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
	cache, _ := txmetacache.NewImprovedCache(128*1024*1024, types.Trimmed)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	t.Logf("0) Total memory used: %v kilobytes", m.Alloc/(1024*1024))
	allKeys := make([][]byte, 0)
	allValues := make([][]byte, 0)
	var err error
	numberOfKeys := 1_000 * txmetacache.BucketsCount

	// cache size : 128 * 1024 * 1024 bytes -> 128 MB
	// number of buckets: 8
	// bucket size: 128 MB / 8 = 16 MB per bucket
	// chunk size: 4 KB
	// 16 MB / 4 KB = 4096 chunks per bucket

	fmt.Println("BucketsCount:", txmetacache.BucketsCount, ", numberOfKeys:", numberOfKeys)

	//f, err := os.Create("mem.prof")
	//if err != nil {
	//	t.Fatalf("could not create memory profile: %v", err)
	//}
	//defer f.Close()

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

	//err = pprof.WriteHeapProfile(f)
	//if err != nil {
	//	t.Fatalf("could not write memory profile: %v", err)
	//}

	//var m runtime.MemStats
	runtime.ReadMemStats(&m)
	t.Logf("2) Total memory used: %v kilobytes", m.Alloc/(1024*1024))

	s := &txmetacache.Stats{}
	cache.UpdateStats(s)
	fmt.Println("Stats, total map size:", s.TotalMapSize)
}

func TestImprovedCache_TestSetMultiWithExpectedMisses(t *testing.T) {
	cache, _ := txmetacache.NewImprovedCache(128*1024*1024, types.Trimmed)
	allKeys := make([][]byte, 0)
	allValues := make([][]byte, 0)
	var err error
	numberOfKeys := 4_000_000

	// cache size : 128 * 1024 * 1024 bytes -> 128 MB
	// number of buckets: 8
	// bucket size: 128 MB / 8 = 16 MB per bucket
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

	s := &txmetacache.Stats{}
	cache.UpdateStats(s)
	fmt.Println("Stats, total map size:", s.TotalMapSize)
}

func Test_txMetaCache_GetMeta_Expiry(t *testing.T) {
	ctx := context.Background()
	c, _ := txmetacache.NewTxMetaCache(ctx, ulogger.TestLogger{}, memory.New(ulogger.TestLogger{}), 2048)
	cache := c.(*txmetacache.TxMetaCache)
	var err error

	for i := 0; i < 1_000_000; i++ {
		hash := chainhash.HashH([]byte(string(rune(i))))
		err = cache.SetCache(&hash, &meta.Data{})
		require.NoError(t, err)
	}

	//make sure newly added items are not expired
	hash := chainhash.HashH([]byte(string(rune(1000000000))))
	err = cache.SetCache(&hash, &meta.Data{})
	require.NoError(t, err)

	txmetaLatest, err := cache.Get(ctx, &hash)
	require.NoError(t, err)
	require.NotNil(t, txmetaLatest)
}
