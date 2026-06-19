package mmaphash

import (
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNextPow2(t *testing.T) {
	cases := []struct{ in, want uint64 }{
		{0, 1}, {1, 1}, {2, 2}, {3, 4}, {5, 8}, {1 << 20, 1 << 20}, {(1 << 20) + 1, 1 << 21},
	}
	for _, c := range cases {
		if got := nextPow2(c.in); got != c.want {
			t.Fatalf("nextPow2(%d)=%d want %d", c.in, got, c.want)
		}
	}
}

func TestComputeLayout(t *testing.T) {
	// expected 0 -> minimal table, K=1, slotsPerSeg=minSegSlots
	l := computeLayout(0, 0.5)
	if l.numSeg != 1 || l.slotsPerSeg != minSegSlots {
		t.Fatalf("empty: got numSeg=%d slotsPerSeg=%d", l.numSeg, l.slotsPerSeg)
	}
	// large expected -> K clamped to maxSeg, slotsPerSeg pow2, capacity >= expected/LF
	l = computeLayout(1_000_000_000, 0.5)
	if l.numSeg != maxSeg {
		t.Fatalf("large: numSeg=%d want %d", l.numSeg, maxSeg)
	}
	total := l.numSeg * l.slotsPerSeg
	if float64(total)*defaultLoadFactor < 1_000_000_000 {
		t.Fatalf("capacity too small: total=%d", total)
	}
	if l.slotsPerSeg&(l.slotsPerSeg-1) != 0 {
		t.Fatalf("slotsPerSeg not pow2: %d", l.slotsPerSeg)
	}

	// ceiling division must guarantee capacity >= expected at defaultLoadFactor
	for _, exp := range []uint64{1, 10, 262145, 262143, 524291, 1_000_000, 3_000_000} {
		ll := computeLayout(exp, defaultLoadFactor)
		total := ll.numSeg * ll.slotsPerSeg
		if uint64(float64(total)*defaultLoadFactor) < exp {
			t.Fatalf("undersized for expected=%d: total=%d capacity=%d", exp, total, uint64(float64(total)*defaultLoadFactor))
		}
	}
}

func TestComputeLayoutDefaultLoadFactor(t *testing.T) {
	// loadFactor <= 0 must behave identically to defaultLoadFactor
	a := computeLayout(100000, 0)
	b := computeLayout(100000, defaultLoadFactor)
	if a != b {
		t.Fatalf("loadFactor<=0 default mismatch: got %+v want %+v", a, b)
	}
	c := computeLayout(100000, -1)
	if c != b {
		t.Fatalf("negative loadFactor default mismatch: got %+v want %+v", c, b)
	}
}

func TestComputeLayoutClampsBadLoadFactor(t *testing.T) {
	// Any loadFactor outside (0,1] — including NaN/Inf — must fall back to the
	// default rather than producing an undersized or collapsed table.
	want := computeLayout(100_000, defaultLoadFactor)
	for _, lf := range []float64{0, -0.5, 1.0001, 2, math.NaN(), math.Inf(1), math.Inf(-1)} {
		require.Equalf(t, want, computeLayout(100_000, lf), "loadFactor %v should fall back to default", lf)
	}
	// a valid in-range factor is honored (0.25 -> larger table than default 0.5)
	denser := computeLayout(100_000, 0.25)
	require.NotEqual(t, want, denser)
	require.Greater(t, denser.numSeg*denser.slotsPerSeg, want.numSeg*want.slotsPerSeg)
}

func TestComputeLayoutMinSegSlotsFloor(t *testing.T) {
	// small non-zero expected -> perSeg computed below minSegSlots -> floored
	l := computeLayout(10, 0.5)
	if l.numSeg != 1 {
		t.Fatalf("numSeg=%d want 1", l.numSeg)
	}
	if l.slotsPerSeg != minSegSlots {
		t.Fatalf("slotsPerSeg=%d want minSegSlots=%d (floor not applied)", l.slotsPerSeg, minSegSlots)
	}
}

func TestTableCreateClose(t *testing.T) {
	dir := t.TempDir()
	tbl, err := New(Options{Dir: dir, Prefix: "t", KeySize: 32, ValueSize: 8, Expected: 1000})
	require.NoError(t, err)

	// backing file was unlinked at creation: the dir should contain no regular files
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		require.Truef(t, e.IsDir(), "unexpected leftover file: %s", filepath.Join(dir, e.Name()))
	}

	require.Equal(t, int64(0), tbl.Len())

	// mmap region is writable for the whole capacity
	require.Greater(t, len(tbl.data), tbl.slotSize)
	tbl.data[0] = 1
	tbl.data[len(tbl.data)-1] = 1

	require.NoError(t, tbl.Close())
	// idempotent second close must not error
	require.NoError(t, tbl.Close())
}

func TestTableNewKeySizeBoundary(t *testing.T) {
	// keySize must be >= 16 (we read key[8:16] for segment selection)
	_, err := New(Options{Dir: t.TempDir(), Prefix: "k", KeySize: 8, ValueSize: 0, Expected: 10})
	require.Error(t, err)
	_, err = New(Options{Dir: t.TempDir(), Prefix: "k", KeySize: 15, ValueSize: 0, Expected: 10})
	require.Error(t, err, "15 is below the minimum")
	tbl, err := New(Options{Dir: t.TempDir(), Prefix: "k", KeySize: 16, ValueSize: 0, Expected: 10})
	require.NoError(t, err, "16 is the valid boundary")
	require.NoError(t, tbl.Close())
}

func TestTableNewRejectsNegativeValueSize(t *testing.T) {
	_, err := New(Options{Dir: t.TempDir(), Prefix: "v", KeySize: 32, ValueSize: -1, Expected: 10})
	require.Error(t, err)
}

func TestTableNewRejectsUnsupportedValueSize(t *testing.T) {
	for _, vs := range []int{1, 4, 7, 9, 16, -1} {
		_, err := New(Options{Dir: t.TempDir(), Prefix: "vs", KeySize: 32, ValueSize: vs, Expected: 10})
		require.Errorf(t, err, "ValueSize %d must be rejected", vs)
	}
	// 0 and 8 are the only valid sizes
	for _, vs := range []int{0, 8} {
		tbl, err := New(Options{Dir: t.TempDir(), Prefix: "vs", KeySize: 32, ValueSize: vs, Expected: 10})
		require.NoErrorf(t, err, "ValueSize %d must be accepted", vs)
		require.NoError(t, tbl.Close())
	}
}

func mkKey(keySize int, seed uint64) []byte {
	k := make([]byte, keySize)
	binary.LittleEndian.PutUint64(k[0:8], seed)
	binary.LittleEndian.PutUint64(k[8:16], seed*0x9e3779b97f4a7c15)
	if keySize >= 24 {
		binary.LittleEndian.PutUint64(k[16:24], seed*2654435761)
	}
	return k
}

func TestUpsertSetSemantics(t *testing.T) {
	tbl, err := New(Options{Dir: t.TempDir(), Prefix: "set", KeySize: 36, ValueSize: 0, Expected: 10000})
	require.NoError(t, err)
	defer tbl.Close()

	for i := uint64(0); i < 5000; i++ {
		_, inserted, err := tbl.Upsert(mkKey(36, i), 0)
		require.NoError(t, err)
		require.True(t, inserted, "first insert of %d should be new", i)
	}
	for i := uint64(0); i < 5000; i++ {
		_, inserted, err := tbl.Upsert(mkKey(36, i), 0)
		require.NoError(t, err)
		require.False(t, inserted, "second insert of %d should be duplicate", i)
	}
	require.Equal(t, int64(5000), tbl.Len())
}

func TestUpsertKeyValueSemantics(t *testing.T) {
	tbl, err := New(Options{Dir: t.TempDir(), Prefix: "kv", KeySize: 32, ValueSize: 8, Expected: 10000})
	require.NoError(t, err)
	defer tbl.Close()

	for i := uint64(0); i < 5000; i++ {
		v, inserted, err := tbl.Upsert(mkKey(32, i), i*7)
		require.NoError(t, err)
		require.True(t, inserted)
		require.Equal(t, i*7, v)
	}
	for i := uint64(0); i < 5000; i++ {
		got, found, err := tbl.Lookup(mkKey(32, i))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, i*7, got)
	}
	// duplicate Upsert returns existing value, inserted=false
	existing, inserted, err := tbl.Upsert(mkKey(32, 3), 999)
	require.NoError(t, err)
	require.False(t, inserted)
	require.Equal(t, uint64(21), existing) // 3*7

	_, found, err := tbl.Lookup(mkKey(32, 999999))
	require.NoError(t, err)
	require.False(t, found)
}

func TestUpsertAllZeroKeyAndZeroValue(t *testing.T) {
	tbl, err := New(Options{Dir: t.TempDir(), Prefix: "z", KeySize: 32, ValueSize: 8, Expected: 100})
	require.NoError(t, err)
	defer tbl.Close()

	zero := make([]byte, 32) // all-zero key
	v, inserted, err := tbl.Upsert(zero, 0)
	require.NoError(t, err)
	require.True(t, inserted)
	require.Equal(t, uint64(0), v)

	got, found, err := tbl.Lookup(zero)
	require.NoError(t, err)
	require.True(t, found, "all-zero key with value 0 must be found (state byte disambiguates)")
	require.Equal(t, uint64(0), got)

	_, inserted, err = tbl.Upsert(zero, 0)
	require.NoError(t, err)
	require.False(t, inserted, "all-zero key second insert is a duplicate")
}

// collidingKey makes keys that all land in the same segment and same start
// bucket (low 8 and second 8 bytes equal), differing only in bytes [24:32],
// forcing linear probing within one segment.
func collidingKey(n uint64) []byte {
	k := make([]byte, 32)
	// key[0:8] and key[8:16] identical across all n -> same segment+start bucket
	binary.LittleEndian.PutUint64(k[0:8], 0xAAAAAAAA)
	binary.LittleEndian.PutUint64(k[8:16], 0xBBBBBBBB)
	binary.LittleEndian.PutUint64(k[24:32], n) // distinguishes the keys
	return k
}

func TestProbingHandlesCollisions(t *testing.T) {
	tbl, err := New(Options{Dir: t.TempDir(), Prefix: "c", KeySize: 32, ValueSize: 8, Expected: 100})
	require.NoError(t, err)
	defer tbl.Close()

	const n = 40 // < minSegSlots (64); all collide on one segment+bucket
	for i := uint64(0); i < n; i++ {
		_, inserted, err := tbl.Upsert(collidingKey(i), i)
		require.NoError(t, err)
		require.True(t, inserted)
	}
	for i := uint64(0); i < n; i++ {
		v, found, err := tbl.Lookup(collidingKey(i))
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, i, v)
	}
}

func TestSegmentFullReturnsError(t *testing.T) {
	// Expected tiny -> 1 segment of minSegSlots (64). Fill the segment with
	// colliding keys until it overflows.
	tbl, err := New(Options{Dir: t.TempDir(), Prefix: "f", KeySize: 32, ValueSize: 8, Expected: 1})
	require.NoError(t, err)
	defer tbl.Close()
	require.Equal(t, uint64(minSegSlots), tbl.slotsPerSeg)
	require.Equal(t, uint64(0), tbl.segMask) // single segment

	var gotFull bool
	for i := uint64(0); i < minSegSlots+5; i++ {
		_, _, err := tbl.Upsert(collidingKey(i), i)
		if err != nil {
			require.ErrorIs(t, err, ErrTableFull)
			gotFull = true
			break
		}
	}
	require.True(t, gotFull, "expected ErrTableFull when segment overflows")
}

func TestConcurrentUpsertExactlyOnce(t *testing.T) {
	tbl, err := New(Options{Dir: t.TempDir(), Prefix: "conc", KeySize: 32, ValueSize: 8, Expected: 200000})
	require.NoError(t, err)
	defer tbl.Close()

	const keys = 50000
	const writersPerKey = 4
	var insertedCount atomic.Int64
	var wg sync.WaitGroup

	for w := 0; w < writersPerKey; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := uint64(0); i < keys; i++ {
				_, inserted, err := tbl.Upsert(mkKey(32, i), i)
				if err != nil {
					t.Errorf("upsert: %v", err)
					return
				}
				if inserted {
					insertedCount.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int64(keys), insertedCount.Load(), "each key inserted exactly once across all goroutines")
	require.Equal(t, int64(keys), tbl.Len())
}

func TestTableScale(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test skipped under -short")
	}
	// The default `make test` target runs every package with -race and a 10m
	// per-package timeout and does NOT pass -short, so guarding on -short alone
	// is not enough: 50M race-tracked Upserts blow the timeout (and starve
	// neighbouring packages) on CI runners. Gate behind an explicit env var,
	// matching the repo convention for long opt-in tests.
	if os.Getenv("RUN_MMAPHASH_SCALE") != "true" {
		t.Skip("scale test skipped; set RUN_MMAPHASH_SCALE=true to run (50M entries, minutes under -race)")
	}
	const n = 50_000_000
	tbl, err := New(Options{Dir: t.TempDir(), Prefix: "scale", KeySize: 36, ValueSize: 0, Expected: n})
	require.NoError(t, err)
	defer tbl.Close()

	for i := uint64(0); i < n; i++ {
		_, _, err := tbl.Upsert(mkKey(36, i), 0)
		require.NoError(t, err) // must never overflow at correct sizing
	}
	require.Equal(t, int64(n), tbl.Len())

	// spot-check membership
	for _, i := range []uint64{0, 1, n / 2, n - 1} {
		_, found, err := tbl.Lookup(mkKey(36, i))
		require.NoError(t, err)
		require.True(t, found)
	}
}

func TestTableRejectsWrongKeySize(t *testing.T) {
	tbl, err := New(Options{Dir: t.TempDir(), Prefix: "ks", KeySize: 32, ValueSize: 8, Expected: 100})
	require.NoError(t, err)
	defer tbl.Close()

	for _, n := range []int{0, 16, 31, 33, 64} {
		_, _, uErr := tbl.Upsert(make([]byte, n), 0)
		require.Errorf(t, uErr, "Upsert must reject key length %d", n)
		_, _, lErr := tbl.Lookup(make([]byte, n))
		require.Errorf(t, lErr, "Lookup must reject key length %d", n)
	}

	// exact key size is accepted
	_, inserted, err := tbl.Upsert(make([]byte, 32), 7)
	require.NoError(t, err)
	require.True(t, inserted)
}

func TestNewRejectsExcessiveExpected(t *testing.T) {
	_, err := New(Options{Dir: t.TempDir(), Prefix: "big", KeySize: 32, ValueSize: 0, Expected: maxExpected + 1})
	require.Error(t, err)
}

// TestNewCreatesMissingDir ensures New creates the backing directory (including
// missing parents) rather than failing — block_diskMapDirs may point at a path
// that does not exist yet, and os.CreateTemp does not create parent dirs.
func TestNewCreatesMissingDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "does", "not", "exist", "yet")
	tbl, err := New(Options{Dir: dir, Prefix: "mk", KeySize: 32, ValueSize: 8, Expected: 100})
	require.NoError(t, err)
	defer tbl.Close()

	info, statErr := os.Stat(dir)
	require.NoError(t, statErr)
	require.True(t, info.IsDir())

	// table is usable
	_, inserted, err := tbl.Upsert(mkKey(32, 1), 7)
	require.NoError(t, err)
	require.True(t, inserted)
}
