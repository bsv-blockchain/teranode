package subtreeprocessor

import (
	"cmp"
	"encoding/binary"
	"math"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// errNotFrozen is the panic value for reading a SortedHashSet before Freeze. Every
// reader in the block assembly flow gets its set from CreateTransactionMap, which
// freezes it, so reaching this is a programming error and must never be answered
// with a guess.
const errNotFrozen = "subtreeprocessor: SortedHashSet read before Freeze"

// maxSortedHashSetSize is the most hashes a set holds: directory offsets are uint32.
const maxSortedHashSetSize = math.MaxUint32

// sortedHashSetChunkSize is the number of hashes per append chunk while building.
const sortedHashSetChunkSize = 1024

// SortedHashSet is an exact, build-then-freeze set of tx hashes. It holds the txs of a
// block while the subtree processor moves forward onto it, and answers "is this tx in
// the block" for every tx of the processor's own subtrees.
//
// While building, hashes are partitioned by their top bits and appended in fixed size
// chunks, one lock per partition. Freeze lays all hashes out sorted in one contiguous
// array, frees the chunks and builds a directory with one uint32 offset per ~4 hashes,
// indexed by the top bits of the hash. Exists reads one directory entry and one or two
// cache lines of hashes. Hashes sharing their top bits make a slot long, which falls
// back to binary search: slower, never wrong. The frozen set costs about 33 bytes per
// hash, against ~54 for the swiss maps it replaces.
//
// All answers are exact and independent of layout: partition count, directory size,
// worker count and insertion order change performance only. Duplicate hashes are kept
// but counted once by Length and visited once by Iter.
type SortedHashSet struct {
	pbits  uint
	parts  []sortedHashPart
	frozen atomic.Bool

	// frozen layout, written by Freeze and read-only afterwards
	gbits uint             // directory index bits, >= pbits
	keys  []chainhash.Hash // all hashes, sorted
	dir   []uint32         // slot g is keys[dir[g]:dir[g+1]]
	dups  atomic.Int64     // duplicate hashes in keys
}

type sortedHashPart struct {
	mu     sync.Mutex
	chunks [][]chainhash.Hash
	n      int
	_      [24]byte // keep neighbouring partition locks off the same cache line
}

// NewSortedHashSet returns a set sized for about capacity hashes. The capacity only
// picks the number of partitions (~64K hashes each, at most 65536); a wrong guess
// never changes results.
func NewSortedHashSet(capacity int) *SortedHashSet {
	pbits := uint(0)
	for pbits < 16 && capacity>>pbits > 1<<16 {
		pbits++
	}

	return &SortedHashSet{pbits: pbits, parts: make([]sortedHashPart, 1<<pbits)}
}

func hashWord0(h *chainhash.Hash) uint64 { return binary.BigEndian.Uint64(h[0:8]) }

// compareHash orders hashes as big-endian numbers, the order of the frozen array.
func compareHash(a, b *chainhash.Hash) int {
	for i := 0; i < 32; i += 8 {
		if c := cmp.Compare(binary.BigEndian.Uint64(a[i:]), binary.BigEndian.Uint64(b[i:])); c != 0 {
			return c
		}
	}

	return 0
}

func hashTopBits(h *chainhash.Hash, bits uint) int {
	if bits == 0 {
		return 0
	}

	return int(hashWord0(h) >> (64 - bits))
}

// Partitions returns the number of partitions PutMultiBucket accepts.
func (s *SortedHashSet) Partitions() int { return len(s.parts) }

// Partition returns the partition of a hash, for PutMultiBucket.
func (s *SortedHashSet) Partition(h chainhash.Hash) int { return hashTopBits(&h, s.pbits) }

func (p *sortedHashPart) appendLocked(hashes ...chainhash.Hash) {
	p.n += len(hashes)

	for len(hashes) > 0 {
		if len(p.chunks) == 0 || len(p.chunks[len(p.chunks)-1]) == sortedHashSetChunkSize {
			p.chunks = append(p.chunks, make([]chainhash.Hash, 0, sortedHashSetChunkSize))
		}

		last := &p.chunks[len(p.chunks)-1]
		n := min(len(hashes), sortedHashSetChunkSize-len(*last))
		*last = append(*last, hashes[:n]...)
		hashes = hashes[n:]
	}
}

// Put adds one hash. Safe for concurrent use until Freeze.
func (s *SortedHashSet) Put(hash chainhash.Hash) error {
	if s.frozen.Load() {
		return errors.NewProcessingError("SortedHashSet is frozen, Put rejected for %s", hash.String())
	}

	p := &s.parts[s.Partition(hash)]
	p.mu.Lock()
	p.appendLocked(hash)
	p.mu.Unlock()

	return nil
}

// PutMultiBucket adds hashes that all belong to the given partition (see Partition)
// under one lock. Safe for concurrent use until Freeze.
func (s *SortedHashSet) PutMultiBucket(partition int, hashes []chainhash.Hash) error {
	if s.frozen.Load() {
		return errors.NewProcessingError("SortedHashSet is frozen, PutMultiBucket rejected for partition %d", partition)
	}

	if partition < 0 || partition >= len(s.parts) {
		return errors.NewProcessingError("SortedHashSet partition %d out of range [0, %d)", partition, len(s.parts))
	}

	p := &s.parts[partition]
	p.mu.Lock()
	p.appendLocked(hashes...)
	p.mu.Unlock()

	return nil
}

func checkSortedHashSetSize(total int) error {
	if uint64(total) > maxSortedHashSetSize {
		return errors.NewProcessingError("SortedHashSet holds at most %d hashes, got %d", uint64(maxSortedHashSetSize), total)
	}

	return nil
}

// Freeze lays every partition out on up to workers goroutines and makes the set
// read-only. All Put and PutMultiBucket calls must have returned before (that join is
// the happens-before edge for the lock-free readers). It fails, leaving the set
// unfrozen, if the set holds more than 2^32-1 hashes or is already frozen.
func (s *SortedHashSet) Freeze(workers int) error {
	if s.frozen.Load() {
		return errors.NewProcessingError("SortedHashSet is already frozen")
	}

	total := 0
	bases := make([]int, len(s.parts))

	for i := range s.parts {
		bases[i] = total
		total += s.parts[i].n
	}

	if err := checkSortedHashSetSize(total); err != nil {
		return err
	}

	// one directory slot per ~4 hashes; partitions own disjoint slot ranges
	s.gbits = s.pbits
	for s.gbits < 64 && total>>s.gbits > 4 {
		s.gbits++
	}

	s.keys = make([]chainhash.Hash, total)
	s.dir = make([]uint32, (1<<s.gbits)+1)
	s.dir[len(s.dir)-1] = uint32(total) //nolint:gosec // bounded by checkSortedHashSetSize

	slotsPerPart := 1 << (s.gbits - s.pbits)

	parallelFor(len(s.parts), workers, func(i int) {
		s.layoutPartition(&s.parts[i], bases[i], i*slotsPerPart, slotsPerPart)
	})

	// Publish last: readers and writers check frozen, and the layout above is complete.
	s.frozen.Store(true)

	return nil
}

// parallelFor runs fn(0..n-1) on up to workers goroutines.
func parallelFor(n, workers int, fn func(i int)) {
	workers = max(1, min(workers, n))

	if workers == 1 {
		for i := 0; i < n; i++ {
			fn(i)
		}

		return
	}

	var (
		next atomic.Int64
		wg   sync.WaitGroup
	)

	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()

			for {
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}

				fn(i)
			}
		}()
	}

	wg.Wait()
}

// layoutPartition scatters a partition's hashes into their directory slots starting at
// keys[base], sorts each small slot and fills dir[firstSlot:firstSlot+slots]. Linear for
// uniformly spread hashes; an overloaded slot falls back to a comparison sort.
func (s *SortedHashSet) layoutPartition(p *sortedHashPart, base, firstSlot, slots int) {
	dir := s.dir[firstSlot : firstSlot+slots]
	counts := make([]uint32, slots)

	for _, c := range p.chunks {
		for k := range c {
			counts[hashTopBits(&c[k], s.gbits)-firstSlot]++
		}
	}

	next := make([]uint32, slots)
	off := uint32(base) //nolint:gosec // bounded by checkSortedHashSetSize

	for sl := range dir {
		dir[sl], next[sl] = off, off
		off += counts[sl]
	}

	for j, c := range p.chunks {
		for k := range c {
			sl := hashTopBits(&c[k], s.gbits) - firstSlot
			s.keys[next[sl]] = c[k]
			next[sl]++
		}

		p.chunks[j] = nil
	}

	p.chunks = nil

	dups := 0

	for sl := range dir {
		run := s.keys[dir[sl]:next[sl]]

		if len(run) > 32 {
			slices.SortFunc(run, func(a, b chainhash.Hash) int { return compareHash(&a, &b) })
		} else {
			for i := 1; i < len(run); i++ {
				for j := i; j > 0 && compareHash(&run[j], &run[j-1]) < 0; j-- {
					run[j], run[j-1] = run[j-1], run[j]
				}
			}
		}

		for i := 1; i < len(run); i++ {
			if run[i] == run[i-1] {
				dups++
			}
		}
	}

	s.dups.Add(int64(dups))
}

// Exists reports whether hash is in the set. It panics if the set is not frozen.
func (s *SortedHashSet) Exists(hash chainhash.Hash) bool {
	if !s.frozen.Load() {
		panic(errNotFrozen)
	}

	if len(s.keys) == 0 {
		return false
	}

	g := hashTopBits(&hash, s.gbits)
	keys := s.keys[s.dir[g]:s.dir[g+1]]

	if len(keys) <= 16 {
		w := hashWord0(&hash)

		for k := range keys {
			if hashWord0(&keys[k]) == w && keys[k] == hash {
				return true
			}
		}

		return false
	}

	_, found := slices.BinarySearchFunc(keys, hash, func(a, b chainhash.Hash) int { return compareHash(&a, &b) })

	return found
}

// Length is the number of distinct hashes. It panics if the set is not frozen.
func (s *SortedHashSet) Length() int {
	if !s.frozen.Load() {
		panic(errNotFrozen)
	}

	return len(s.keys) - int(s.dups.Load())
}

// Iter calls f with every distinct hash in ascending order until f returns false. It
// panics if the set is not frozen.
func (s *SortedHashSet) Iter(f func(hash chainhash.Hash) bool) {
	if !s.frozen.Load() {
		panic(errNotFrozen)
	}

	for i := range s.keys {
		if i > 0 && s.keys[i] == s.keys[i-1] {
			continue
		}

		if !f(s.keys[i]) {
			return
		}
	}
}
