package model

import (
	"encoding/binary"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

func newTestDiskParentSpendsMap(t *testing.T) *DiskParentSpendsMap {
	t.Helper()
	m, err := NewDiskParentSpendsMap(DiskParentSpendsMapOptions{
		BasePaths:      []string{t.TempDir()},
		Prefix:         "test-parentspends",
		FilterCapacity: 100_000, // sized for TestDiskParentSpendsMap_ManyEntries (50K inserts)
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func makeInpoint(hashIdx, index int) subtreepkg.Inpoint {
	return subtreepkg.Inpoint{
		Hash:  makeHash(hashIdx),
		Index: uint32(index),
	}
}

func TestDiskParentSpendsMap_SetIfNotExists_New(t *testing.T) {
	m := newTestDiskParentSpendsMap(t)

	ip := makeInpoint(1, 0)
	inserted, err := m.SetIfNotExists(ip)
	require.NoError(t, err)
	require.True(t, inserted)
}

func TestDiskParentSpendsMap_SetIfNotExists_Duplicate(t *testing.T) {
	m := newTestDiskParentSpendsMap(t)

	ip := makeInpoint(1, 0)
	got1, err := m.SetIfNotExists(ip)
	require.NoError(t, err)
	require.True(t, got1)
	got2, err := m.SetIfNotExists(ip)
	require.NoError(t, err)
	require.False(t, got2)
}

func TestDiskParentSpendsMap_DifferentIndexes(t *testing.T) {
	m := newTestDiskParentSpendsMap(t)

	ip0 := makeInpoint(1, 0)
	ip1 := makeInpoint(1, 1)

	got, err := m.SetIfNotExists(ip0)
	require.NoError(t, err)
	require.True(t, got)
	got, err = m.SetIfNotExists(ip1)
	require.NoError(t, err)
	require.True(t, got)
	got, err = m.SetIfNotExists(ip0)
	require.NoError(t, err)
	require.False(t, got)
	got, err = m.SetIfNotExists(ip1)
	require.NoError(t, err)
	require.False(t, got)
}

func TestDiskParentSpendsMap_ConcurrentSetIfNotExists(t *testing.T) {
	m := newTestDiskParentSpendsMap(t)

	const n = 1000
	var wg sync.WaitGroup
	wg.Add(n)
	results := make([]bool, n)

	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			inserted, err := m.SetIfNotExists(makeInpoint(idx, 0))
			require.NoError(t, err)
			results[idx] = inserted
		}(i)
	}
	wg.Wait()

	for i, inserted := range results {
		require.True(t, inserted, "unique inpoint %d should have been inserted", i)
	}
}

func TestDiskParentSpendsMap_ConcurrentDuplicateDetection(t *testing.T) {
	m := newTestDiskParentSpendsMap(t)

	ip := makeInpoint(42, 0)
	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	results := make([]bool, goroutines)

	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			inserted, err := m.SetIfNotExists(ip)
			require.NoError(t, err)
			results[idx] = inserted
		}(i)
	}
	wg.Wait()

	insertCount := 0
	for _, inserted := range results {
		if inserted {
			insertCount++
		}
	}
	require.Equal(t, 1, insertCount, "exactly one goroutine should succeed")
}

func TestDiskParentSpendsMap_MultiDisk(t *testing.T) {
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	m, err := NewDiskParentSpendsMap(DiskParentSpendsMapOptions{
		BasePaths:      dirs,
		Prefix:         "test-multi-ps",
		FilterCapacity: 10_000,
	})
	require.NoError(t, err)
	defer func() { _ = m.Close() }()

	const n = 500
	for i := 0; i < n; i++ {
		got, err := m.SetIfNotExists(makeInpoint(i, 0))
		require.NoError(t, err)
		require.True(t, got)
	}

	// all duplicates should be detected
	for i := 0; i < n; i++ {
		got, err := m.SetIfNotExists(makeInpoint(i, 0))
		require.NoError(t, err)
		require.False(t, got)
	}

	// verify routing actually spread entries across disks (not all on disk 0)
	var nonEmpty int
	var total int64
	for _, tbl := range m.tables {
		if tbl.Len() > 0 {
			nonEmpty++
		}
		total += tbl.Len()
	}
	require.Equal(t, int64(n), total, "every entry must be accounted for across disk tables")
	require.GreaterOrEqual(t, nonEmpty, 2, "entries should be distributed across multiple disks")
}

func TestDiskParentSpendsMap_ImplementsInterface(t *testing.T) {
	var _ ParentSpendsMap = (*DiskParentSpendsMap)(nil)
	var _ ParentSpendsMap = (*SplitSyncedParentMap)(nil)
}

func TestDiskParentSpendsMap_NoPaths(t *testing.T) {
	_, err := NewDiskParentSpendsMap(DiskParentSpendsMapOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "at least one base path")
}

func TestDiskParentSpendsMap_ManyEntries(t *testing.T) {
	m := newTestDiskParentSpendsMap(t)

	const n = 50_000
	for i := 0; i < n; i++ {
		// Use different hash values to get good distribution across shards
		var h chainhash.Hash
		h[0] = byte(i >> 24)
		h[1] = byte(i >> 16)
		h[2] = byte(i >> 8)
		h[3] = byte(i)
		h[4] = byte(i >> 12) // extra entropy

		ip := subtreepkg.Inpoint{Hash: h, Index: uint32(i % 10)}
		got, err := m.SetIfNotExists(ip)
		require.NoError(t, err)
		require.True(t, got, "insert failed at %d", i)
	}

	// verify count
	require.Equal(t, int64(n), m.Stats().Entries)
}

func TestDiskParentSpendsMap_SetIfNotExists_Overflow(t *testing.T) {
	// FilterCapacity 1 -> a single minimal segment (minSegSlots slots). Inserting
	// many unique inpoints (all in the single segment) must eventually overflow,
	// and that overflow MUST surface as a non-nil error -- never a silent false.
	m, err := NewDiskParentSpendsMap(DiskParentSpendsMapOptions{
		BasePaths:      []string{t.TempDir()},
		FilterCapacity: 1,
	})
	require.NoError(t, err)
	defer m.Close()

	var gotErr error
	for i := uint64(0); i < 100000 && gotErr == nil; i++ {
		var h chainhash.Hash
		binary.LittleEndian.PutUint64(h[0:8], i)
		binary.LittleEndian.PutUint64(h[8:16], i*0x9e3779b97f4a7c15)
		_, e := m.SetIfNotExists(subtreepkg.Inpoint{Hash: h, Index: uint32(i)})
		gotErr = e
	}
	require.Error(t, gotErr, "filling beyond capacity must surface an error, not a silent false")
}

func TestDiskParentSpendsMap_Stats(t *testing.T) {
	m, err := NewDiskParentSpendsMap(DiskParentSpendsMapOptions{
		BasePaths:      []string{t.TempDir()},
		Prefix:         "test-stats-ps",
		FilterCapacity: 10_000,
	})
	require.NoError(t, err)

	stats := m.Stats()
	require.Equal(t, int64(0), stats.Entries)
	require.Equal(t, int64(0), stats.FilterMemBytes, "mmap impl has no in-RAM filter")
	require.Equal(t, int64(0), stats.DiskBytesWritten)

	const n = 500
	for i := 0; i < n; i++ {
		got, err := m.SetIfNotExists(makeInpoint(i, 0))
		require.NoError(t, err)
		require.True(t, got)
	}

	// Close is still valid to call; mmap tables flush on close.
	require.NoError(t, m.Close())

	stats = m.Stats()
	require.Equal(t, int64(n), stats.Entries)
	require.Equal(t, int64(0), stats.FilterMemBytes, "mmap impl has no in-RAM filter")
	// Each entry = 36B inpoint key + 1B slot marker = 37B
	require.Equal(t, int64(n*37), stats.DiskBytesWritten)
}

func inpointSeed(i uint64) subtreepkg.Inpoint {
	var h chainhash.Hash
	binary.LittleEndian.PutUint64(h[0:8], i)
	binary.LittleEndian.PutUint64(h[8:16], i*0x9e3779b97f4a7c15)
	binary.LittleEndian.PutUint64(h[16:24], i*2654435761)
	return subtreepkg.Inpoint{Hash: h, Index: uint32(i % 5)}
}

// TestDiskParentSpendsMap_ParityWithInMemory drives the same op sequence
// through the in-memory SplitSyncedParentMap and the disk-backed map and
// asserts identical SetIfNotExists return sequences.
func TestDiskParentSpendsMap_ParityWithInMemory(t *testing.T) {
	mem := NewSplitSyncedParentMap(256, 20000)
	disk, err := NewDiskParentSpendsMap(DiskParentSpendsMapOptions{
		BasePaths:      []string{t.TempDir(), t.TempDir()},
		FilterCapacity: 20000,
	})
	require.NoError(t, err)
	defer disk.Close()

	// include deliberate duplicates: seeds 0..9999 once, then 0..4999 again
	seq := make([]uint64, 0, 15000)
	for i := uint64(0); i < 10000; i++ {
		seq = append(seq, i)
	}
	for i := uint64(0); i < 5000; i++ {
		seq = append(seq, i) // duplicates
	}

	for _, i := range seq {
		ip := inpointSeed(i)
		gotMem, errMem := mem.SetIfNotExists(ip)
		require.NoError(t, errMem)
		gotDisk, errDisk := disk.SetIfNotExists(ip)
		require.NoError(t, errDisk)
		require.Equalf(t, gotMem, gotDisk, "divergence at seed %d", i)
	}
}

func FuzzDiskParentSpendsMap_Parity(f *testing.F) {
	f.Add([]byte{1, 2, 3, 1, 2, 3})
	f.Fuzz(func(t *testing.T, data []byte) {
		mem := NewSplitSyncedParentMap(16, 256)
		disk, err := NewDiskParentSpendsMap(DiskParentSpendsMapOptions{
			BasePaths:      []string{t.TempDir()},
			FilterCapacity: 1024,
		})
		if err != nil {
			t.Skip()
		}
		defer disk.Close()
		for _, b := range data {
			ip := inpointSeed(uint64(b))
			gotMem, errMem := mem.SetIfNotExists(ip)
			require.NoError(t, errMem)
			gotDisk, errDisk := disk.SetIfNotExists(ip)
			require.NoError(t, errDisk)
			require.Equal(t, gotMem, gotDisk)
		}
	})
}

func TestNewDiskParentSpendsMap_RejectsZeroCapacity(t *testing.T) {
	// Fixed-capacity table: zero FilterCapacity must be rejected up-front, not
	// silently produce a minimal table that overflows immediately.
	_, err := NewDiskParentSpendsMap(DiskParentSpendsMapOptions{
		BasePaths:      []string{t.TempDir()},
		FilterCapacity: 0,
	})
	require.Error(t, err)
}

// TestDiskParentSpendsMap_SameParentClustering covers ordishs's review point:
// inpoints that share one parent hash but differ only in output index land in
// the SAME segment and SAME start bucket (the index lives in key[32:36],
// outside both the segment window key[8:16] and bucket window key[0:8]), so a
// consolidation/sweep of many outputs of one parent forms a probe chain in a
// single segment. This asserts correctness under that clustering: every
// distinct (hash, index) is inserted exactly once, and re-inserting any is
// detected as a duplicate — with capacity sized for the load.
func TestDiskParentSpendsMap_SameParentClustering(t *testing.T) {
	const n = 20000 // many outputs of ONE parent, all clustering in one segment

	m, err := NewDiskParentSpendsMap(DiskParentSpendsMapOptions{
		BasePaths:      []string{t.TempDir()},
		FilterCapacity: n, // size for the clustered load
	})
	require.NoError(t, err)
	defer m.Close()

	var parent chainhash.Hash
	binary.LittleEndian.PutUint64(parent[0:8], 0xDEADBEEF)
	binary.LittleEndian.PutUint64(parent[8:16], 0xC0FFEE) // fixes segment+bucket for all

	// First pass: every distinct output index is a new entry.
	for i := uint64(0); i < n; i++ {
		got, err := m.SetIfNotExists(subtreepkg.Inpoint{Hash: parent, Index: uint32(i)})
		require.NoErrorf(t, err, "insert of index %d (same-parent cluster) must not overflow at sized capacity", i)
		require.Truef(t, got, "index %d should be newly inserted", i)
	}
	require.Equal(t, int64(n), m.Stats().Entries)

	// Second pass: every one is now a duplicate (probe chain still resolves correctly).
	for i := uint64(0); i < n; i++ {
		got, err := m.SetIfNotExists(subtreepkg.Inpoint{Hash: parent, Index: uint32(i)})
		require.NoError(t, err)
		require.Falsef(t, got, "index %d should be detected as duplicate", i)
	}

	// A different parent (different hash) with the same indices is still distinct.
	var other chainhash.Hash
	binary.LittleEndian.PutUint64(other[0:8], 0xFEEDFACE)
	got, err := m.SetIfNotExists(subtreepkg.Inpoint{Hash: other, Index: 0})
	require.NoError(t, err)
	require.True(t, got, "different parent hash must be a distinct entry")
}
