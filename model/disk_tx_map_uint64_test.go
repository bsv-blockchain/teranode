package model

import (
	"fmt"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/stretchr/testify/require"
)

func newTestDiskTxMapUint64(t *testing.T) *DiskTxMapUint64 {
	t.Helper()
	m, err := NewDiskTxMapUint64(DiskTxMapUint64Options{
		BasePaths:      []string{t.TempDir()},
		Prefix:         "test-txmap",
		FilterCapacity: 10_000,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func makeHash(i int) chainhash.Hash {
	var h chainhash.Hash
	// Little-endian so the low bytes (used for shard selection) vary first
	h[0] = byte(i)
	h[1] = byte(i >> 8)
	h[2] = byte(i >> 16)
	h[3] = byte(i >> 24)
	return h
}

func TestDiskTxMapUint64_PutFlushGet(t *testing.T) {
	m := newTestDiskTxMapUint64(t)

	h := makeHash(42)
	require.NoError(t, m.Put(h, 100))
	require.Equal(t, 1, m.Length())

	require.NoError(t, m.Flush())

	val, ok := m.Get(h)
	require.True(t, ok)
	require.Equal(t, uint64(100), val)
}

func TestDiskTxMapUint64_PutDuplicate(t *testing.T) {
	m := newTestDiskTxMapUint64(t)

	h := makeHash(1)
	require.NoError(t, m.Put(h, 10))

	err := m.Put(h, 20)
	require.Error(t, err)
	require.ErrorIs(t, err, txmap.ErrHashAlreadyExists)
}

func TestDiskTxMapUint64_PutDuplicateAfterFlush(t *testing.T) {
	m := newTestDiskTxMapUint64(t)

	h := makeHash(99)
	require.NoError(t, m.Put(h, 10))
	require.NoError(t, m.Flush())

	err := m.Put(h, 20)
	require.Error(t, err)
	require.ErrorIs(t, err, txmap.ErrHashAlreadyExists)
}

func TestDiskTxMapUint64_GetNonexistent(t *testing.T) {
	m := newTestDiskTxMapUint64(t)

	h := makeHash(999)
	val, ok := m.Get(h)
	require.False(t, ok)
	require.Equal(t, uint64(0), val)
}

func TestDiskTxMapUint64_Exists(t *testing.T) {
	m := newTestDiskTxMapUint64(t)

	h := makeHash(5)
	require.False(t, m.Exists(h))

	require.NoError(t, m.Put(h, 50))
	require.True(t, m.Exists(h))
}

func TestDiskTxMapUint64_ConcurrentPut(t *testing.T) {
	m := newTestDiskTxMapUint64(t)

	const n = 1000
	var wg sync.WaitGroup
	wg.Add(n)
	errs := make([]error, n)

	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			errs[idx] = m.Put(makeHash(idx), uint64(idx))
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "error at index %d", i)
	}
	require.Equal(t, n, m.Length())

	require.NoError(t, m.Flush())

	for i := 0; i < n; i++ {
		val, ok := m.Get(makeHash(i))
		require.True(t, ok, "missing hash at index %d", i)
		require.Equal(t, uint64(i), val, "wrong value at index %d", i)
	}
}

func TestDiskTxMapUint64_ConcurrentPutDuplicateDetection(t *testing.T) {
	m := newTestDiskTxMapUint64(t)

	h := makeHash(42)
	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	results := make([]error, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			results[idx] = m.Put(h, uint64(idx))
		}(i)
	}
	wg.Wait()

	successCount := 0
	for _, err := range results {
		if err == nil {
			successCount++
		}
	}
	require.Equal(t, 1, successCount, "exactly one goroutine should succeed")
}

func TestDiskTxMapUint64_MultiDisk(t *testing.T) {
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	m, err := NewDiskTxMapUint64(DiskTxMapUint64Options{
		BasePaths:      dirs,
		Prefix:         "test-multi",
		FilterCapacity: 10_000,
	})
	require.NoError(t, err)
	defer func() { _ = m.Close() }()

	const n = 500
	for i := 0; i < n; i++ {
		require.NoError(t, m.Put(makeHash(i), uint64(i)))
	}
	require.Equal(t, n, m.Length())

	require.NoError(t, m.Flush())

	for i := 0; i < n; i++ {
		val, ok := m.Get(makeHash(i))
		require.True(t, ok, "missing hash at index %d", i)
		require.Equal(t, uint64(i), val)
	}
}

func TestDiskTxMapUint64_ImplementsTxMap(t *testing.T) {
	var _ txmap.TxMap = (*DiskTxMapUint64)(nil)
}

func TestDiskTxMapUint64_NoPaths(t *testing.T) {
	_, err := NewDiskTxMapUint64(DiskTxMapUint64Options{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "at least one base path")
}

func TestDiskTxMapUint64_LargeValues(t *testing.T) {
	m := newTestDiskTxMapUint64(t)

	h := makeHash(1)
	largeVal := uint64(1<<63 - 1)
	require.NoError(t, m.Put(h, largeVal))
	require.NoError(t, m.Flush())

	val, ok := m.Get(h)
	require.True(t, ok)
	require.Equal(t, largeVal, val)
}

func TestDiskTxMapUint64_ManyEntries(t *testing.T) {
	m := newTestDiskTxMapUint64(t)

	const n = 50_000
	for i := 0; i < n; i++ {
		require.NoError(t, m.Put(makeHash(i), uint64(i)))
	}

	require.NoError(t, m.Flush())
	require.Equal(t, n, m.Length())

	// spot check
	for _, idx := range []int{0, 1, 100, 999, 25000, 49999} {
		val, ok := m.Get(makeHash(idx))
		require.True(t, ok, "missing at %d", idx)
		require.Equal(t, uint64(idx), val, fmt.Sprintf("wrong value at %d", idx))
	}
}

func TestDiskTxMapUint64_Stats(t *testing.T) {
	m := newTestDiskTxMapUint64(t)

	stats := m.Stats()
	require.Equal(t, int64(0), stats.Entries)
	require.Greater(t, stats.FilterMemBytes, int64(0), "filter memory should be non-zero at construction")
	require.Equal(t, int64(0), stats.DiskBytesWritten)

	const n = 1000
	for i := 0; i < n; i++ {
		require.NoError(t, m.Put(makeHash(i), uint64(i)))
	}

	require.NoError(t, m.Flush())

	stats = m.Stats()
	require.Equal(t, int64(n), stats.Entries)
	require.Greater(t, stats.FilterMemBytes, int64(0))
	// Each entry = 32B hash key + 8B uint64 value = 40B
	require.Equal(t, int64(n*40), stats.DiskBytesWritten)
}
