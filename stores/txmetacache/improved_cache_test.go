package txmetacache

import (
	"crypto/rand"
	"encoding/binary"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func init() {
	go func() {
		log.Println("Starting pprof server on http://localhost:6060")
		//nolint:gosec // G114: Use of net/http serve function that has no support for setting timeouts (gosec)
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
}

func TestImprovedCache_TestSetMultiWithExpectedMisses(t *testing.T) {
	cache, _ := NewImprovedCache(128*1024*1024, Trimmed)
	allKeys := make([][]byte, 0)
	allValues := make([][]byte, 0)

	var err error

	// cache size : 128 MB
	// 8 * 1024 buckets
	// bucket size: 128 MB / 8 * 1024 = 16 KB per bucket
	// chunk size: 4 KB
	// 16 KB / 4 KB = 4 chunks per bucket
	// 4 KB / 68  = 60 key-value pairs per chunk
	// 60 * 4 = 240 key-value pairs per bucket
	// 240 * 8 * 1024 =  1966080 key-value pairs per cache

	numberOfKeys := 1_967_000

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
	require.NotZero(t, errCounter)

	s := &Stats{}
	cache.UpdateStats(s)
	require.Equal(t, s.TotalElementsAdded, uint64(len(allKeys)))
	require.Equal(t, uint64(errCounter)+s.EntriesCount, uint64(len(allKeys)))

	t.Log("Stats, current elements size: ", s.EntriesCount)
	t.Log("Stats, total elements added: ", s.TotalElementsAdded)
}

// TestInitTimes measures NewImprovedCache startup across X runs and logs the average per bucket type.
func TestInitTimes(t *testing.T) {
	// testing with 256 MB cache
	const (
		maxBytes = 256 * 1024 * 1024
		runs     = 3
	)

	buckets := []struct {
		name string
		typ  BucketType
	}{
		{"Unallocated", Unallocated},
		{"Trimmed", Trimmed},
		{"Preallocated", Preallocated},
	}

	for _, bc := range buckets {
		var total time.Duration

		for i := 0; i < runs; i++ {
			start := time.Now()

			cache, err := NewImprovedCache(maxBytes, bc.typ)
			require.NoError(t, err, "init %s iteration %d", bc.name, i)

			total += time.Since(start)

			cache.Reset()
		}

		avg := total / runs
		t.Logf("%s average init over %d runs: %v", bc.name, runs, avg)
	}
}
