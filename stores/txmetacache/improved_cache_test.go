package txmetacache

import (
	"crypto/rand"
	"encoding/binary"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/util/types"
	"github.com/stretchr/testify/require"
)

func init() {
	go func() {
		log.Println("Starting pprof server on http://localhost:6060")
		//nolint:gosec // G114: Use of net/http serve function that has no support for setting timeouts (gosec)
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
}

func TestImprovedCache_TestSetMultiWithExpectedMisses_Small(t *testing.T) {
	cache, _ := NewImprovedCache(128*1024, types.Trimmed)
	allKeys := make([][]byte, 0)
	allValues := make([][]byte, 0)

	var err error

	numberOfKeys := 2000

	// cache size : 128 KB
	// 8 buckets
	// bucket size: 128 KB / 8 = 16 KB per bucket
	// chunk size: 4 KB
	// 16 KB / 4 KB = 4 chunks per bucket
	// 4 KB / 68  = 60 key-value pairs per chunk
	// 60 * 4 = 240 key-value pairs per bucket
	// 240 * 8 = 1920 key-value pairs per cache

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
