// Package mmaphash provides an off-heap, file-backed, open-addressing hash
// table used by block validation for ephemeral (per-block) membership and
// key/value maps. The backing file is created sparse, mmap'd, and unlinked
// immediately, so it is reclaimed on Close or process exit. There is no
// durability: the table exists only for the lifetime of one block validation.
package mmaphash

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/teranode/errors"
	"golang.org/x/sys/unix"
)

const (
	minSegSlots       = 64    // smallest segment so linear probing has room
	maxSeg            = 4096  // max independently-locked segments
	segTarget         = 65536 // ~entries per segment used to pick segment count
	defaultLoadFactor = 0.5
	// maxExpected caps Options.Expected far below the ranges where nextPow2
	// fails to terminate (n > 2^63) or totalSlots*slotSize overflows int64.
	// 2^44 entries is orders of magnitude beyond any real block.
	maxExpected = 1 << 44
)

// nextPow2 returns the smallest power of two >= n (and >= 1). Inputs must be <= 2^63; above that the shift loop never terminates (inputs here are bounded by transaction counts).
func nextPow2(n uint64) uint64 {
	if n <= 1 {
		return 1
	}
	p := uint64(1)
	for p < n {
		p <<= 1
	}
	return p
}

// clampU64 bounds v to [lo, hi].
func clampU64(v, lo, hi uint64) uint64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// layout describes the segment geometry of a table.
type layout struct {
	numSeg      uint64 // power of two, the K segments
	slotsPerSeg uint64 // power of two slots in each segment
}

// computeLayout picks segment count and per-segment slot count for the
// expected number of entries and target load factor.
func computeLayout(expected uint64, loadFactor float64) layout {
	// A valid load factor is in (0, 1]. Reject <=0, >1 (would undersize),
	// and NaN/Inf (the !(...) form catches NaN since all comparisons with NaN
	// are false). Fall back to the safe default rather than producing a table
	// that overflows unexpectedly at runtime.
	if !(loadFactor > 0 && loadFactor <= 1) {
		loadFactor = defaultLoadFactor
	}
	numSeg := clampU64(nextPow2(expected/segTarget), 1, maxSeg)
	// slots needed across all segments to hold expected at loadFactor
	needed := uint64(float64(expected)/loadFactor) + 1
	perSeg := nextPow2((needed + numSeg - 1) / numSeg)
	if perSeg < minSegSlots {
		perSeg = minSegSlots
	}
	return layout{numSeg: numSeg, slotsPerSeg: perSeg}
}

// Options configures a Table.
type Options struct {
	Dir        string  // directory for the backing file (one physical disk)
	Prefix     string  // file name prefix
	KeySize    int     // bytes of key compared for equality (>=16)
	ValueSize  int     // bytes of value (0 for a pure set)
	Expected   uint64  // expected entries in THIS table
	LoadFactor float64 // 0 => defaultLoadFactor
}

type seg struct {
	mu sync.RWMutex
}

// maxTotalSlots is an arithmetic sanity guard on a grown table, NOT the real
// capacity ceiling. A grow doubles slotsPerSeg; if the new total would exceed
// this, grow refuses and ErrTableFull surfaces (fail-safe). At 2^45 slots a
// real table (slotSize ~37) is a ~1.3 PB mapping — far past the OS per-process
// address space (~128 TB on x86-64), so in practice grow fails earlier on the
// mmap call (also error → halt, still fail-safe) and this bound is only reached
// in theory. Its job is to keep totalSlots*slotSize from overflowing int64.
const maxTotalSlots = 1 << 45

// Table is a concurrent, off-heap, open-addressing hash table.
//
// growMu coordinates transparent growth (issue #1080): Upsert/Lookup hold it as
// readers (so many run concurrently across independently-locked segments), and
// grow holds it exclusively, draining all readers before swapping t.data and
// t.slotsPerSeg. The segment count (segMask, len(segs)) never changes on grow —
// only per-segment capacity doubles — so an entry never moves between segments
// and the per-segment locks are stable.
type Table struct {
	growMu      sync.RWMutex
	data        []byte
	slotSize    int
	keySize     int
	valueSize   int
	slotsPerSeg uint64
	segMask     uint64
	segs        []seg
	count       atomic.Int64
	gen         atomic.Uint64 // bumped on each grow; lets Upsert skip redundant grows
	dir         string        // backing-file directory, retained for grow
	prefix      string        // backing-file prefix, retained for grow
}

// ErrTableFull is returned when a segment has no empty slot (capacity exceeded).
// Uses the threshold-exceeded code so callers and tests can discriminate it via
// errors.Is from generic processing errors.
var ErrTableFull = errors.NewThresholdExceededError("mmaphash: table segment full")

// New creates a Table backed by a sparse, immediately-unlinked mmap file.
func New(opts Options) (*Table, error) {
	if opts.KeySize < 16 {
		return nil, errors.NewProcessingError("mmaphash: KeySize must be >= 16, got %d", opts.KeySize)
	}
	if opts.ValueSize != 0 && opts.ValueSize != 8 {
		return nil, errors.NewProcessingError("mmaphash: ValueSize must be 0 or 8, got %d", opts.ValueSize)
	}
	// Bound Expected well below the ranges where nextPow2 would not terminate
	// (n > 2^63) or where totalSlots*slotSize would overflow int64. maxExpected
	// is far larger than any real block, so this only rejects misconfiguration.
	if opts.Expected > maxExpected {
		return nil, errors.NewProcessingError("mmaphash: Expected %d exceeds maximum %d", opts.Expected, uint64(maxExpected))
	}

	l := computeLayout(opts.Expected, opts.LoadFactor)
	slotSize := 1 + opts.KeySize + opts.ValueSize
	totalSlots := l.numSeg * l.slotsPerSeg
	fileBytes := int64(totalSlots) * int64(slotSize)

	data, err := mapRegion(opts.Dir, opts.Prefix, fileBytes)
	if err != nil {
		return nil, err
	}

	return &Table{
		data:        data,
		slotSize:    slotSize,
		keySize:     opts.KeySize,
		valueSize:   opts.ValueSize,
		slotsPerSeg: l.slotsPerSeg,
		segMask:     l.numSeg - 1,
		segs:        make([]seg, l.numSeg),
		dir:         opts.Dir,
		prefix:      opts.Prefix,
	}, nil
}

// mapRegion creates a sparse, immediately-unlinked backing file of fileBytes
// and mmaps it. The open mapping keeps the inode alive; space is reclaimed on
// munmap or process exit, even after a crash. Used by New and by grow.
func mapRegion(dir, prefix string, fileBytes int64) ([]byte, error) {
	// os.CreateTemp does not create parent directories, so ensure Dir exists
	// first. The Badger-backed implementation this replaced did the equivalent
	// MkdirAll; without it, configuring block_diskMapDirs to a not-yet-created
	// path makes every block fail validation. Empty Dir means os.TempDir().
	if dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, errors.NewStorageError("mmaphash: create dir %s", dir, err)
		}
	}

	f, err := os.CreateTemp(dir, prefix+"-*.mmh")
	if err != nil {
		shownDir := dir
		if shownDir == "" {
			shownDir = os.TempDir() // os.CreateTemp falls back to this when dir is empty
		}
		return nil, errors.NewStorageError("mmaphash: create temp file in %s", shownDir, err)
	}
	// Unlink now: the open fd and mmap keep the inode alive; space is reclaimed
	// on Close (munmap + last fd close) or on process exit, even after a crash.
	// If unlink fails we cannot honour the ephemeral/crash-safe contract, so fail.
	if rmErr := os.Remove(f.Name()); rmErr != nil {
		_ = f.Close()
		return nil, errors.NewStorageError("mmaphash: unlink temp file %s", f.Name(), rmErr)
	}

	// Truncate makes the file sparse — blocks are committed lazily on first
	// write-fault, which is what keeps RSS tracking live entries (untouched
	// segments never allocate). Known limitation: if the backing disk is full
	// when a slot is first written, that store fault delivers SIGBUS (process
	// crash) rather than a returnable error. Committing blocks up front
	// (fallocate) would trade that crash for an early error but defeats the
	// sparse design (a grow would commit the full doubled size). Left sparse;
	// disk capacity for the maps is an operator/provisioning concern.
	if err = f.Truncate(fileBytes); err != nil {
		_ = f.Close()
		return nil, errors.NewStorageError("mmaphash: ftruncate %d bytes", fileBytes, err)
	}

	data, err := unix.Mmap(int(f.Fd()), 0, int(fileBytes), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		_ = f.Close()
		return nil, errors.NewStorageError("mmaphash: mmap %d bytes", fileBytes, err)
	}
	// Random access pattern: disable readahead so a fault pages in 4K, not 128K.
	_ = unix.Madvise(data, unix.MADV_RANDOM)
	// fd no longer needed; mapping survives close.
	_ = f.Close()

	return data, nil
}

// Close unmaps the region, releasing RSS and reclaiming the file's blocks.
// t.data is cleared only after a successful munmap, so a failed Close does not
// lose the mapping reference (a caller could retry); a successful Close is
// idempotent via the nil guard.
func (t *Table) Close() error {
	if t.data == nil {
		return nil
	}
	if err := unix.Munmap(t.data); err != nil {
		return errors.NewStorageError("mmaphash: munmap", err)
	}
	t.data = nil
	return nil
}

// Len returns the number of entries inserted.
func (t *Table) Len() int64 { return t.count.Load() }

// locate derives the segment index and the in-segment start bucket from the
// key. Keys are uniformly-random hash output, so raw bytes are used as the
// hash. The bucket uses key[0:8] and the segment uses key[8:16]; the
// disk-backed map wrappers route across disks using a further-disjoint window
// (key[16:18]) so disk selection does not correlate with intra-table placement.
func (t *Table) locate(key []byte) (segIdx, bucket uint64) {
	segHash := binary.LittleEndian.Uint64(key[8:16])
	bucketHash := binary.LittleEndian.Uint64(key[0:8])
	segIdx = segHash & t.segMask
	bucket = bucketHash & (t.slotsPerSeg - 1)
	return segIdx, bucket
}

// probe scans segment segIdx starting at the start bucket. It returns the byte
// offset of the matching slot (found=true) or the first empty slot
// (found=false). full=true means the whole segment was scanned with no empty
// slot. Probing wraps within the segment only.
func (t *Table) probe(segIdx, start uint64, key []byte) (off int, found, full bool) {
	base := segIdx * t.slotsPerSeg
	mask := t.slotsPerSeg - 1
	for i := uint64(0); i < t.slotsPerSeg; i++ {
		local := (start + i) & mask
		o := int(base+local) * t.slotSize
		if t.data[o] == 0 { // empty
			return o, false, false
		}
		if bytes.Equal(t.data[o+1:o+1+t.keySize], key) {
			return o, true, false
		}
	}
	return 0, false, true
}

func (t *Table) readValue(off int) uint64 {
	if t.valueSize == 0 {
		return 0
	}
	return binary.LittleEndian.Uint64(t.data[off+1+t.keySize : off+1+t.keySize+8])
}

func (t *Table) writeSlot(off int, key []byte, value uint64) {
	copy(t.data[off+1:off+1+t.keySize], key)
	if t.valueSize >= 8 {
		binary.LittleEndian.PutUint64(t.data[off+1+t.keySize:off+1+t.keySize+8], value)
	}
	t.data[off] = 1 // publish occupied state last
}

// Upsert inserts key (with value) if absent. Returns (existingOrNewValue,
// inserted, error). inserted=false with no error means the key was already
// present and the returned value is the stored one. If a segment is full the
// table grows transparently (doubling per-segment capacity) and the insert is
// retried; ErrTableFull is only returned once growth hits the absolute slot
// cap (fail-safe — never reachable for any real block).
func (t *Table) Upsert(key []byte, value uint64) (uint64, bool, error) {
	if len(key) != t.keySize {
		return 0, false, errors.NewProcessingError("mmaphash: key length %d != table key size %d", len(key), t.keySize)
	}
	for {
		t.growMu.RLock()
		segIdx, start := t.locate(key)
		s := &t.segs[segIdx]
		s.mu.Lock()

		off, found, full := t.probe(segIdx, start, key)
		if !found && !full {
			t.writeSlot(off, key, value)
			t.count.Add(1)
			s.mu.Unlock()
			t.growMu.RUnlock()
			return value, true, nil
		}
		if found {
			v := t.readValue(off)
			s.mu.Unlock()
			t.growMu.RUnlock()
			return v, false, nil
		}
		// segment full: grow (under the exclusive lock) and retry.
		s.mu.Unlock()
		gen := t.gen.Load()
		t.growMu.RUnlock()

		if err := t.grow(gen); err != nil {
			return 0, false, err
		}
	}
}

// Lookup returns (value, found).
func (t *Table) Lookup(key []byte) (uint64, bool, error) {
	if len(key) != t.keySize {
		return 0, false, errors.NewProcessingError("mmaphash: key length %d != table key size %d", len(key), t.keySize)
	}
	t.growMu.RLock()
	defer t.growMu.RUnlock()

	segIdx, start := t.locate(key)
	s := &t.segs[segIdx]
	s.mu.RLock()
	defer s.mu.RUnlock()

	// full is irrelevant for Lookup: if the segment is full but the key isn't
	// present, the key genuinely isn't in the table. ErrTableFull is write-only.
	off, found, _ := t.probe(segIdx, start, key)
	if !found {
		return 0, false, nil
	}
	return t.readValue(off), true, nil
}

// grow doubles the per-segment slot capacity, rehashing every live entry into a
// fresh mmap, then swaps it in. It holds growMu exclusively, so all concurrent
// Upsert/Lookup readers are drained before t.data/t.slotsPerSeg change — no torn
// reads. The segment count is unchanged, so an entry keeps its segment and only
// its in-segment bucket is recomputed.
//
// observedGen is the gen value the caller saw before releasing its read lock; if
// another goroutine already grew in the meantime, this grow is a no-op and the
// caller simply retries into the larger table (avoids redundant doublings under
// a thundering herd).
func (t *Table) grow(observedGen uint64) error {
	t.growMu.Lock()
	defer t.growMu.Unlock()

	if t.gen.Load() != observedGen {
		return nil // someone else already grew; retry will find room
	}

	numSeg := t.segMask + 1
	newSlotsPerSeg := t.slotsPerSeg * 2
	// Division form: the product stays <= 2^46 given the per-grow invariant, but
	// checking newSlotsPerSeg against maxTotalSlots/numSeg keeps the bound safe
	// without any chance of a uint64 multiply overflowing. numSeg is always >= 1.
	if newSlotsPerSeg > maxTotalSlots/numSeg {
		// Astronomically large (only reachable under pathological single-parent
		// clustering far beyond any real block). Stay fail-safe.
		return ErrTableFull
	}

	fileBytes := int64(numSeg*newSlotsPerSeg) * int64(t.slotSize)
	newData, err := mapRegion(t.dir, t.prefix, fileBytes)
	if err != nil {
		return err
	}

	// Rehash: scan old slots; each occupied slot keeps its segment (its global
	// index / old slotsPerSeg) and is re-probed within that segment's larger
	// window. Copying the whole slot preserves state byte, key and value.
	newMask := newSlotsPerSeg - 1
	oldSlots := numSeg * t.slotsPerSeg
	for i := uint64(0); i < oldSlots; i++ {
		o := int(i) * t.slotSize
		if t.data[o] == 0 {
			continue // empty
		}
		segIdx := i / t.slotsPerSeg
		key := t.data[o+1 : o+1+t.keySize]
		bucket := binary.LittleEndian.Uint64(key[0:8]) & newMask
		base := segIdx * newSlotsPerSeg
		placed := false
		for j := uint64(0); j < newSlotsPerSeg; j++ {
			local := (bucket + j) & newMask
			no := int(base+local) * t.slotSize
			if newData[no] == 0 {
				copy(newData[no:no+t.slotSize], t.data[o:o+t.slotSize])
				placed = true
				break
			}
		}
		// A segment held at most old-slotsPerSeg entries, so they always fit in
		// the doubled window. If that invariant is ever violated we must NOT
		// silently drop the entry: a lost parentSpendsMap entry is a missed
		// duplicate-input, i.e. accepting an invalid block. Fail the grow loudly
		// instead — newData is not yet swapped in, so t is left untouched.
		if !placed {
			_ = unix.Munmap(newData)
			return errors.NewProcessingError("mmaphash: rehash could not place entry; segment overflow in doubled window (unexpected)")
		}
	}

	old := t.data
	t.data = newData
	t.slotsPerSeg = newSlotsPerSeg
	t.gen.Add(1)

	// Release the old mapping. The swap is already published; a munmap error
	// only leaks the old region (reclaimed at process exit), so do not fail.
	_ = unix.Munmap(old)
	return nil
}
