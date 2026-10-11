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

// maxSortedHashPartitionSize is the most hashes one partition holds: directory offsets
// are uint32 and relative to the partition. The set as a whole has no such limit.
//
// A partition receives about capacity/2^pbits hashes, where pbits grows until that is
// at most 64K (or 2^16 partitions are reached). Even for a 2^40 tx block that is ~16M
// hashes per partition; an honest block cannot approach 2^32 per partition, which
// would take ~2.8e14 txs. Only a block of hashes ground to share their top 16 bits
// gets there, and it fails Freeze with an error instead of misbehaving.
const maxSortedHashPartitionSize = math.MaxUint32

// sortedHashSetChunkSize is the number of hashes per append chunk while building.
const sortedHashSetChunkSize = 1024

// SortedHashSet is an exact, build-then-freeze set of tx hashes. It holds the txs of a
// block while the subtree processor moves forward onto it, and answers "is this tx in
// the block" for every tx of the processor's own subtrees.
//
// While building, hashes are partitioned by their top bits and appended in fixed size
// chunks, one lock per partition. Freeze lays all hashes out sorted in one contiguous
// array, frees the chunks and builds a directory with one uint32 offset (relative to its partition) per ~4 hashes,
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
	bases []uint64         // partition p owns keys[bases[p]:bases[p+1]]
	dir   []uint32         // slot g of partition p is keys[bases[p]+dir[g+p] : bases[p]+dir[g+p+1]]
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

// partitionBases returns where each partition starts in the sorted array (plus the
// end at index len(parts)) and the total. Only a single partition is limited to
// 2^32-1 hashes.
func partitionBases(parts []sortedHashPart) (bases []uint64, total uint64, err error) {
	bases = make([]uint64, len(parts)+1)

	for i := range parts {
		if uint64(parts[i].n) > maxSortedHashPartitionSize {
			return nil, 0, errors.NewProcessingError("SortedHashSet partition %d holds %d hashes, at most %d fit", i, parts[i].n, uint64(maxSortedHashPartitionSize))
		}

		bases[i] = total
		total += uint64(parts[i].n)
	}

	bases[len(parts)] = total

	return bases, total, nil
}

// Freeze lays every partition out on up to workers goroutines and makes the set
// read-only. All Put and PutMultiBucket calls must have returned before (that join is
// the happens-before edge for the lock-free readers). It fails, leaving the set
// unfrozen, if one partition holds more than 2^32-1 hashes or the set is already
// frozen.
func (s *SortedHashSet) Freeze(workers int) error {
	if s.frozen.Load() {
		return errors.NewProcessingError("SortedHashSet is already frozen")
	}

	bases, total, err := partitionBases(s.parts)
	if err != nil {
		return err
	}

	// one directory slot per ~4 hashes; partitions own disjoint slot ranges
	s.gbits = s.pbits
	for s.gbits < 64 && total>>s.gbits > 4 {
		s.gbits++
	}

	slotsPerPart := 1 << (s.gbits - s.pbits)

	s.bases = bases
	s.keys = make([]chainhash.Hash, total)
	// every partition has slotsPerPart+1 entries: its slot starts and its end
	s.dir = make([]uint32, len(s.parts)*(slotsPerPart+1))

	parallelFor(len(s.parts), workers, func(i int) {
		s.layoutPartition(&s.parts[i], i, slotsPerPart)
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

// layoutPartition scatters the given partition's hashes into its directory slots, sorts each
// small slot and fills the partition's directory entries with offsets relative to the
// partition. Linear for uniformly spread hashes; an overloaded slot falls back to a
// comparison sort.
func (s *SortedHashSet) layoutPartition(p *sortedHashPart, part, slots int) {
	firstSlot := part * slots
	keys := s.keys[s.bases[part]:s.bases[part+1]]
	dir := s.dir[part*(slots+1) : (part+1)*(slots+1)]
	counts := make([]uint32, slots)

	for _, c := range p.chunks {
		for k := range c {
			counts[hashTopBits(&c[k], s.gbits)-firstSlot]++
		}
	}

	next := make([]uint32, slots)
	off := uint32(0)

	for sl := 0; sl < slots; sl++ {
		dir[sl], next[sl] = off, off
		off += counts[sl]
	}

	dir[slots] = off

	for j, c := range p.chunks {
		for k := range c {
			sl := hashTopBits(&c[k], s.gbits) - firstSlot
			keys[next[sl]] = c[k]
			next[sl]++
		}

		p.chunks[j] = nil
	}

	p.chunks = nil

	dups := 0

	for sl := 0; sl < slots; sl++ {
		run := keys[dir[sl]:next[sl]]

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
	part := g >> (s.gbits - s.pbits)
	base := s.bases[part]
	keys := s.keys[base+uint64(s.dir[g+part]) : base+uint64(s.dir[g+part+1])]

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

// Iter calls f with every distinct hash in ascending order until f returns true (stop),
// like swiss.Map.Iter. It panics if the set is not frozen.
func (s *SortedHashSet) Iter(f func(hash chainhash.Hash) bool) {
	if !s.frozen.Load() {
		panic(errNotFrozen)
	}

	for i := range s.keys {
		if i > 0 && s.keys[i] == s.keys[i-1] {
			continue
		}

		if f(s.keys[i]) {
			return
		}
	}
}
