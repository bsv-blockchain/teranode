package subtreeprocessor

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

func newTestDiskTxMap(t *testing.T) *DiskTxMap {
	t.Helper()
	m, err := NewDiskTxMap(DiskTxMapOptions{
		BasePath:       t.TempDir(),
		Prefix:         "test",
		FilterCapacity: 10_000,
	})
	require.NoError(t, err)
	t.Cleanup(func() { m.Close() })
	return m
}

func makeInpoints(subtreeIdx int16) *subtreepkg.TxInpoints {
	ip := subtreepkg.NewTxInpoints()
	ip.SubtreeIndex = subtreeIdx
	return &ip
}

func TestDiskTxMap_SetIfNotExists(t *testing.T) {
	m := newTestDiskTxMap(t)

	hash := chainhash.HashH([]byte("tx1"))
	ip := makeInpoints(-1)

	// First insert should succeed
	_, wasSet := m.SetIfNotExists(hash, ip)
	require.True(t, wasSet)
	require.Equal(t, 1, m.Length())

	// Second insert of same hash should fail (duplicate)
	_, wasSet = m.SetIfNotExists(hash, ip)
	require.False(t, wasSet)
	require.Equal(t, 1, m.Length())
}

func TestDiskTxMap_Exists(t *testing.T) {
	m := newTestDiskTxMap(t)

	hash := chainhash.HashH([]byte("tx1"))
	require.False(t, m.Exists(hash))

	m.SetIfNotExists(hash, makeInpoints(-1))
	require.True(t, m.Exists(hash))
}

func TestDiskTxMap_Get(t *testing.T) {
	m := newTestDiskTxMap(t)

	hash := chainhash.HashH([]byte("tx1"))
	ip := makeInpoints(5)

	_, ok := m.Get(hash)
	require.False(t, ok)

	m.SetIfNotExists(hash, ip)

	got, ok := m.Get(hash)
	require.True(t, ok)
	require.Equal(t, int16(5), got.SubtreeIndex)
}

func TestDiskTxMap_Delete(t *testing.T) {
	m := newTestDiskTxMap(t)

	hash := chainhash.HashH([]byte("tx1"))
	m.SetIfNotExists(hash, makeInpoints(-1))
	require.Equal(t, 1, m.Length())

	m.Delete(hash)
	require.Equal(t, 0, m.Length())
	require.False(t, m.Exists(hash))
}

func TestDiskTxMap_Clear(t *testing.T) {
	m := newTestDiskTxMap(t)

	for i := 0; i < 100; i++ {
		data := make([]byte, 4)
		data[0] = byte(i)
		data[1] = byte(i >> 8)
		hash := chainhash.HashH(data)
		m.SetIfNotExists(hash, makeInpoints(-1))
	}

	require.Equal(t, 100, m.Length())

	m.Clear()
	require.Equal(t, 0, m.Length())
}

func TestDiskTxMap_Set(t *testing.T) {
	m := newTestDiskTxMap(t)

	hash := chainhash.HashH([]byte("tx1"))
	m.Set(hash, makeInpoints(3))

	require.Equal(t, 1, m.Length())
	got, ok := m.Get(hash)
	require.True(t, ok)
	require.Equal(t, int16(3), got.SubtreeIndex)

	// Set again (overwrite)
	m.Set(hash, makeInpoints(7))
	require.Equal(t, 1, m.Length())
	got, ok = m.Get(hash)
	require.True(t, ok)
	require.Equal(t, int16(7), got.SubtreeIndex)
}

func TestDiskTxMap_ConcurrentSetIfNotExists(t *testing.T) {
	m := newTestDiskTxMap(t)

	const numGoroutines = 16
	const numPerGoroutine = 1000

	var wg sync.WaitGroup
	insertedCounts := make([]int, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < numPerGoroutine; i++ {
				// All goroutines try to insert the same hashes
				data := make([]byte, 4)
				data[0] = byte(i)
				data[1] = byte(i >> 8)
				hash := chainhash.HashH(data)
				_, wasSet := m.SetIfNotExists(hash, makeInpoints(-1))
				if wasSet {
					insertedCounts[goroutineID]++
				}
			}
		}(g)
	}

	wg.Wait()

	// Each unique hash should only have been inserted once across all goroutines
	totalInserted := 0
	for _, c := range insertedCounts {
		totalInserted += c
	}
	require.Equal(t, numPerGoroutine, totalInserted,
		"each unique hash should be inserted exactly once")
	require.Equal(t, numPerGoroutine, m.Length())
}

func TestDiskTxMap_SerializationRoundtrip(t *testing.T) {
	ip := makeInpoints(42)
	ip.ParentTxHashes = append(ip.ParentTxHashes, chainhash.HashH([]byte("parent1")))
	ip.Idxs = append(ip.Idxs, []uint32{0, 1})

	serialized := serializeTxMapValue(ip)
	deserialized := deserializeTxMapValue(serialized)

	require.NotNil(t, deserialized)
	require.Equal(t, int16(42), deserialized.SubtreeIndex)
	require.Equal(t, len(ip.ParentTxHashes), len(deserialized.ParentTxHashes))
}
