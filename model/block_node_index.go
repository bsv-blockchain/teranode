package model

import (
	"hash/maphash"
	"math"
	mathbits "math/bits"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
)

// blockTxMap is what block validation needs from the map of a block's tx hash to
// its global index (subIdx*subtreeSize+txIdx): the in-memory blockNodeIndex, the
// disk-backed DiskTxMapUint64, or the 64-bit SplitSwissMapUint64 kept for blocks
// the 32-bit blockNodeIndex cannot address.
type blockTxMap interface {
	// Put adds hash at idx and fails if hash is already in the map.
	Put(hash chainhash.Hash, idx uint64) error
	Get(hash chainhash.Hash) (uint64, bool)
	// Length is the number of entries.
	Length() int
	// Freeze makes the map read-only. All Puts must have completed before it.
	Freeze()
}

// nodeIndex maps a tx hash to its 32-bit global index in the block without storing
// the hash. A slot holds a 16-bit fingerprint and the index, 6 bytes against ~60
// for the swiss map it replaces, ~11 per entry at the presized load. A fingerprint match is confirmed against the node
// hash the block already holds (hashAt), so answers are exact: a false fingerprint
// match is only ever a wasted read. A hash seed, the insertion order and the number
// of writers change the layout, never the answers.
//
// Slots are grouped in buckets of 10, one 64-byte cache line. An entry goes into the
// first free slot from its home bucket and a lookup scans until a free slot; there are
// no deletes, so the scan can't stop earlier. The index is presized to 60% full, so a
// key that is not in it, like a parent mined in an earlier block, is mostly turned away
// by its home bucket. The index is split into shards, each with its own lock and
// buckets, so concurrent Puts on different shards don't contend.
type nodeIndex struct {
	shards    []nodeShard
	shardMask uint64
	hashAt    func(idx uint32) *chainhash.Hash
	hash      func(chainhash.Hash) uint64
	frozen    atomic.Bool
}

const nodeBucketSlots = 10

type nodeBucket struct {
	fps  [nodeBucketSlots]uint16 // 0 = free slot
	idxs [nodeBucketSlots]uint32
	_    [4]byte // pad to a 64-byte cache line
}

type nodeShard struct {
	mu      sync.RWMutex
	buckets []nodeBucket
	n       int
	_       [64 - 24 - 24 - 8]byte // keep shard locks on separate cache lines
}

// maxNodeShards is the shard count at which blocks of any size stop getting more
// locks, like the 8192 buckets of the swiss map.
const maxNodeShards = 8192

// newNodeIndex sizes the index for capacity entries. hashAt must return the node hash
// for every idx that has been Put.
func newNodeIndex(capacity int, hashAt func(idx uint32) *chainhash.Hash, hash func(chainhash.Hash) uint64) *nodeIndex {
	// one shard per ~2K entries so small blocks still spread concurrent Puts over a few locks
	numShards := 1
	for numShards < maxNodeShards && numShards*2048 < capacity {
		numShards *= 2
	}

	x := &nodeIndex{
		shards:    make([]nodeShard, numShards),
		shardMask: uint64(numShards - 1), // nolint: gosec
		hashAt:    hashAt,
		hash:      hash,
	}

	// 6 standard deviations of headroom for uneven shards, so a presized index doesn't grow
	mean := capacity / numShards
	perShard := mean + 6*int(math.Sqrt(float64(mean))) + 8

	for i := range x.shards {
		x.shards[i].buckets = make([]nodeBucket, numNodeBuckets(perShard))
	}

	return x
}

// numNodeBuckets sizes for a 60% load. A shard only grows past 90% (Put).
func numNodeBuckets(entries int) int {
	return max(1, (entries*10+nodeBucketSlots*6-1)/(nodeBucketSlots*6))
}

// split derives the home bucket and the fingerprint from the hash. The shard comes from
// other bits (shard).
func (x *nodeIndex) split(h uint64, s *nodeShard) (int, uint16) {
	b, _ := mathbits.Mul64(h, uint64(len(s.buckets)))

	fp := uint16(h) // nolint: gosec
	if fp == 0 {
		fp = 1
	}

	return int(b), fp // nolint: gosec
}

func (x *nodeIndex) shard(h uint64) *nodeShard {
	return &x.shards[x.shardIndex(h)]
}

func (x *nodeIndex) shardIndex(h uint64) uint64 {
	return (h >> 16) & x.shardMask
}

// find returns the bucket and slot holding hash, or the first free slot and found=false.
func (x *nodeIndex) find(s *nodeShard, hash *chainhash.Hash, h uint64) (b, slot int, found bool) {
	b, fp := x.split(h, s)

	for range s.buckets {
		bk := &s.buckets[b]

		for slot = 0; slot < nodeBucketSlots; slot++ {
			switch bk.fps[slot] {
			case 0:
				return b, slot, false
			case fp:
				if *x.hashAt(bk.idxs[slot]) == *hash {
					return b, slot, true
				}
			}
		}

		if b++; b == len(s.buckets) {
			b = 0
		}
	}

	panic("node index shard full") // unreachable: shards grow at 90% load
}

// Put adds hash at idx. It returns a TxExists error (whose text putSubtreeInTxMap also matches) if hash is already in the
// index, like a duplicate tx in a block. The node at idx must already be readable
// through hashAt.
func (x *nodeIndex) Put(hash chainhash.Hash, idx uint32) error {
	if x.frozen.Load() {
		return errors.NewProcessingError("map is frozen")
	}

	h := x.hash(hash)
	s := x.shard(h)

	s.mu.Lock()
	defer s.mu.Unlock()

	if !x.insert(s, &hash, h, idx) {
		return errors.NewTxExistsError("hash already exists in map")
	}

	return nil
}

// insert adds hash at idx to shard s, whose lock the caller holds. It returns false,
// and adds nothing, if hash is already in the index.
func (x *nodeIndex) insert(s *nodeShard, hash *chainhash.Hash, h uint64, idx uint32) bool {
	b, slot, found := x.find(s, hash, h)
	if found {
		return false
	}

	if s.n+1 > len(s.buckets)*(nodeBucketSlots-1) {
		x.grow(s)
		b, slot, _ = x.find(s, hash, h)
	}

	_, fp := x.split(h, s)
	s.buckets[b].fps[slot] = fp
	s.buckets[b].idxs[slot] = idx
	s.n++

	return true
}

// putRangeChunk is how many entries putRange groups by shard at a time. A shard of the
// largest index (8192 shards) gets about 32 of them per lock; the scratch for a full
// chunk is 8MB.
const putRangeChunk = 1 << 18

// nodePut is an entry of putRange: a node index and the hash of its node hash.
type nodePut struct {
	h   uint64
	idx uint32
}

type putRangeScratch struct {
	in, out []nodePut
	ends    []uint32
}

// putRangeScratchPool holds scratch sized for the ranges it has seen, so a small block
// only allocates what its subtrees need.
var putRangeScratchPool = sync.Pool{New: func() any { return &putRangeScratch{} }}

// putRange does a Put of the node at every index of [lo, lo+n), and returns at the first
// index whose node hash is already in the index or earlier in the range, with a TxExists
// error. On that error indexes after it may have been added.
//
// A Put takes its shard's lock, and an uncontended lock still costs a cache line moved
// between cores, then misses the cache on the home bucket. putRange instead sorts a chunk
// of entries by shard, takes each lock once for all of a shard's entries, and reads their
// home buckets before inserting any, so the cache misses overlap instead of following one
// another.
func (x *nodeIndex) putRange(lo uint32, n int) (uint32, error) {
	if x.frozen.Load() {
		return 0, errors.NewProcessingError("map is frozen")
	}

	if n <= 0 {
		return 0, nil
	}

	// with about one entry per shard there is nothing to batch
	if n < 2*len(x.shards) {
		return x.putEach(lo, n)
	}

	sc := putRangeScratchPool.Get().(*putRangeScratch)
	defer putRangeScratchPool.Put(sc)

	for start := 0; start < n; start += putRangeChunk {
		if dup, ok := x.putChunk(lo+uint32(start), min(putRangeChunk, n-start), sc); !ok { // nolint: gosec
			return dup, errors.NewTxExistsError("hash already exists in map")
		}
	}

	return 0, nil
}

// putEach is putRange as a Put of each index in order.
func (x *nodeIndex) putEach(lo uint32, n int) (uint32, error) {
	for i := 0; i < n; i++ {
		idx := lo + uint32(i) // nolint: gosec
		h := x.hash(*x.hashAt(idx))
		s := x.shard(h)

		s.mu.Lock()
		ok := x.insert(s, x.hashAt(idx), h, idx)
		s.mu.Unlock()

		if !ok {
			return idx, errors.NewTxExistsError("hash already exists in map")
		}
	}

	return 0, nil
}

// putChunk is putRange for at most putRangeChunk entries. It returns false and the first
// duplicate index if there is one.
func (x *nodeIndex) putChunk(lo uint32, n int, sc *putRangeScratch) (uint32, bool) {
	numShards := len(x.shards)

	if cap(sc.in) < n {
		sc.in, sc.out = make([]nodePut, n), make([]nodePut, n)
	}

	if cap(sc.ends) < numShards {
		sc.ends = make([]uint32, numShards)
	}

	ends := sc.ends[:numShards]
	clear(ends)

	in, out := sc.in[:n], sc.out[:n]

	// hash outside the locks, counting entries per shard
	for i := range in {
		idx := lo + uint32(i) // nolint: gosec
		h := x.hash(*x.hashAt(idx))
		in[i] = nodePut{h: h, idx: idx}
		ends[x.shardIndex(h)]++
	}

	// ends[s] becomes the start of shard s, then its end as the entries are placed
	var pos uint32
	for s := range ends {
		pos, ends[s] = pos+ends[s], pos
	}

	for _, e := range in {
		s := x.shardIndex(e.h)
		out[ends[s]] = e
		ends[s]++
	}

	var (
		dup   uint32
		found bool
	)

	// Writers that walked the shards in the same order would queue behind each other on
	// every lock, so each chunk starts at a shard of its own.
	first := int(x.shardIndex(in[0].h))

	for k := range ends {
		s := (first + k) & int(x.shardMask) // nolint: gosec

		start := uint32(0)
		if s > 0 {
			start = ends[s-1]
		}

		if ends[s] > start {
			if d, ok := x.putGroup(&x.shards[s], out[start:ends[s]]); !ok && (!found || d < dup) {
				dup, found = d, true
			}
		}
	}

	return dup, !found
}

// putGroup inserts entries of shard s in order under one lock, after reading their home
// buckets. It returns false and the first duplicate index if there is one; the
// entries are in index order, so that is the first one found.
func (x *nodeIndex) putGroup(s *nodeShard, group []nodePut) (uint32, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// independent loads, so the cache misses of the whole group are in flight together
	var sink uint16

	for _, e := range group {
		b, _ := x.split(e.h, s)
		sink ^= s.buckets[b].fps[0]
	}

	runtime.KeepAlive(sink)

	var (
		dup   uint32
		found bool
	)

	for _, e := range group {
		if !x.insert(s, x.hashAt(e.idx), e.h, e.idx) && !found {
			dup, found = e.idx, true
		}
	}

	return dup, !found
}

// nodeLookup is one key of getBatch: hash in, idx and found out.
type nodeLookup struct {
	hash  *chainhash.Hash
	h     uint64
	idx   uint64
	found bool
}

// getBatch does a Get of every lookup in ls. On a frozen index it reads the home buckets of
// all of them, then the node hashes their fingerprints point at, before the lookups
// themselves, so the cache misses of the batch overlap instead of following one another.
func (x *nodeIndex) getBatch(ls []nodeLookup) {
	if !x.frozen.Load() {
		for i := range ls {
			idx, ok := x.Get(*ls[i].hash)
			ls[i].idx, ls[i].found = uint64(idx), ok
		}

		return
	}

	var sink uint16

	for i := range ls {
		ls[i].h = x.hash(*ls[i].hash)
	}

	for i := range ls {
		s := x.shard(ls[i].h)
		b, _ := x.split(ls[i].h, s)
		sink ^= s.buckets[b].fps[0]
	}

	for i := range ls {
		s := x.shard(ls[i].h)
		b, fp := x.split(ls[i].h, s)
		bk := &s.buckets[b]

		for slot := 0; slot < nodeBucketSlots && bk.fps[slot] != 0; slot++ {
			if bk.fps[slot] == fp {
				sink ^= uint16(x.hashAt(bk.idxs[slot])[0])
				break
			}
		}
	}

	runtime.KeepAlive(sink)

	for i := range ls {
		s := x.shard(ls[i].h)

		b, slot, found := x.find(s, ls[i].hash, ls[i].h)
		if found {
			ls[i].idx, ls[i].found = uint64(s.buckets[b].idxs[slot]), true
		} else {
			ls[i].idx, ls[i].found = 0, false
		}
	}
}

// Get returns the index of hash. After Freeze it takes no lock.
func (x *nodeIndex) Get(hash chainhash.Hash) (uint32, bool) {
	h := x.hash(hash)
	s := x.shard(h)

	if !x.frozen.Load() {
		s.mu.RLock()
		defer s.mu.RUnlock()
	}

	b, slot, found := x.find(s, &hash, h)
	if !found {
		return 0, false
	}

	return s.buckets[b].idxs[slot], true
}

// grow doubles a shard, rehashing every entry from its node hash.
func (x *nodeIndex) grow(s *nodeShard) {
	old := s.buckets
	s.buckets = make([]nodeBucket, 2*len(old))

	for _, bk := range old {
		for slot := 0; slot < nodeBucketSlots && bk.fps[slot] != 0; slot++ {
			b, fp := x.split(x.hash(*x.hashAt(bk.idxs[slot])), s)

			for {
				nb := &s.buckets[b]

				free := 0
				for free < nodeBucketSlots && nb.fps[free] != 0 {
					free++
				}

				if free < nodeBucketSlots {
					nb.fps[free] = fp
					nb.idxs[free] = bk.idxs[slot]

					break
				}

				if b++; b == len(s.buckets) {
					b = 0
				}
			}
		}
	}
}

// Freeze makes the index read-only so Get skips the shard locks. All Puts must have
// completed before Freeze.
func (x *nodeIndex) Freeze() {
	x.frozen.Store(true)
}

// Length is the number of entries.
func (x *nodeIndex) Length() int {
	frozen := x.frozen.Load()
	n := 0

	for i := range x.shards {
		s := &x.shards[i]

		if !frozen {
			s.mu.RLock()
		}

		n += s.n

		if !frozen {
			s.mu.RUnlock()
		}
	}

	return n
}

// slots is the allocated slot count, for checking growth in tests.
func (x *nodeIndex) slots() int {
	n := 0
	for i := range x.shards {
		n += len(x.shards[i].buckets) * nodeBucketSlots
	}

	return n
}

// blockNodeIndex is the in-memory blockTxMap: a nodeIndex over the node arrays of the
// block's own subtrees. It stores no hashes, so a node array registered with it must
// stay readable until the map is dropped. The arrays are the subtrees' Nodes slices,
// and the map lives no longer than Valid (releaseTxMap). Registering keeps a heap
// array reachable for the GC, but a release of the block's subtrees during Valid
// would otherwise pool it, and the next subtree load overwrite it, or unmap an mmap
// one. The block is pinned while the map is live (Block.pinNodeIndex), which stops
// both, and Close unpins it.
type blockNodeIndex struct {
	unpin     func()
	unpinOnce sync.Once

	idx         *nodeIndex
	subtreeSize uint64
	// nodes[s] is subtree s's node array, set once by register before any Put of
	// one of its txs. A reader only reaches it through an entry found under a shard
	// lock, or after Freeze, both of which order it after that write.
	nodes [][]subtreepkg.Node

	// A subtree other than the last that is longer than subtreeSize overlaps the index space
	// of the next, so its txs can't be found through hashAt. Such a block is invalid and the
	// load reports it once every loader has finished, but the old map took those txs, so the
	// duplicate check must too: they are kept in overflow under the index they had there. It
	// is empty for any block that isn't malformed. overflowing[s] is written by register
	// and read by the goroutine that owns subtree s only.
	overflowing []bool
	hasOverflow atomic.Bool
	overflowMu  sync.Mutex
	overflow    map[chainhash.Hash]uint64
}

// newBlockNodeIndex returns an index for numSubtrees subtrees whose index space is
// subIdx*subtreeSize+txIdx, sized for capacity entries.
func newBlockNodeIndex(capacity uint64, numSubtrees, subtreeSize int) *blockNodeIndex {
	m := &blockNodeIndex{
		subtreeSize: uint64(subtreeSize), // nolint: gosec
		nodes:       make([][]subtreepkg.Node, numSubtrees),
		overflowing: make([]bool, numSubtrees),
	}

	seed := maphash.MakeSeed()
	m.idx = newNodeIndex(int(min(capacity, uint64(math.MaxInt32))), m.hashAt, // nolint: gosec
		func(h chainhash.Hash) uint64 { return maphash.Comparable(seed, h) })

	return m
}

// hashAt returns the hash of the node at global index idx. Every subtree but the last is
// at most subtreeSize long (register), so idx/subtreeSize is its subtree; the last may be
// longer, which only moves its tail past the end of the index space of a full one.
func (m *blockNodeIndex) hashAt(idx uint32) *chainhash.Hash {
	s := min(uint64(idx)/m.subtreeSize, uint64(len(m.nodes)-1))

	return &m.nodes[s][uint64(idx)-s*m.subtreeSize].Hash
}

// register makes the nodes of subtree sIdx readable through the index. It must run before
// the first Put of one of them. It never judges the block: a subtree other than the last that
// is longer than the first one's capacity is only marked (overflows), and reported by the load
// after all subtrees are in, like any other length mismatch.
func (m *blockNodeIndex) register(sIdx int, nodes []subtreepkg.Node) error {
	if sIdx < 0 || sIdx >= len(m.nodes) {
		return errors.NewProcessingError("subtree %d is outside the %d subtrees of the block", sIdx, len(m.nodes))
	}

	m.overflowing[sIdx] = sIdx != len(m.nodes)-1 && uint64(len(nodes)) > m.subtreeSize
	m.nodes[sIdx] = nodes

	return nil
}

// overflows reports whether subtree sIdx was registered too long for its slot, so its txs go
// through putOverflow.
func (m *blockNodeIndex) overflows(sIdx int) bool {
	return m.overflowing[sIdx]
}

// putOverflow is Put for a tx of an overflowing subtree. idx is the index the old map gave it,
// which may overlap another subtree's.
func (m *blockNodeIndex) putOverflow(hash chainhash.Hash, idx uint64) error {
	m.overflowMu.Lock()

	if m.overflow == nil {
		m.overflow = make(map[chainhash.Hash]uint64)
	}

	if _, dup := m.overflow[hash]; dup {
		m.overflowMu.Unlock()

		return errors.NewTxExistsError("hash already exists in map")
	}

	m.overflow[hash] = idx
	m.hasOverflow.Store(true)
	m.overflowMu.Unlock()

	// Put checks overflow after inserting and this checks the index after inserting, so
	// whichever of two equal hashes comes second sees the other.
	if _, dup := m.idx.Get(hash); dup {
		return errors.NewTxExistsError("hash already exists in map")
	}

	return nil
}

func (m *blockNodeIndex) inOverflow(hash chainhash.Hash) (uint64, bool) {
	if !m.hasOverflow.Load() {
		return 0, false
	}

	m.overflowMu.Lock()
	defer m.overflowMu.Unlock()

	idx, ok := m.overflow[hash]

	return idx, ok
}

// Put adds hash at idx. An idx above the 32-bit range is an error, never wrapped.
func (m *blockNodeIndex) Put(hash chainhash.Hash, idx uint64) error {
	if idx > math.MaxUint32 {
		return errors.NewProcessingError("tx index %d does not fit the 32-bit in-memory tx index", idx)
	}

	if err := m.idx.Put(hash, uint32(idx)); err != nil {
		return err
	}

	if _, dup := m.inOverflow(hash); dup {
		return errors.NewTxExistsError("hash already exists in map")
	}

	return nil
}

// putRange does a Put of the node at every index of [lo, lo+n) (nodeIndex.putRange). On an
// error it returns the index of the node it failed at: the first duplicate, or the first
// index above the 32-bit range.
func (m *blockNodeIndex) putRange(lo uint64, n int) (uint64, error) {
	if n <= 0 {
		return 0, nil
	}

	if last := lo + uint64(n) - 1; last > math.MaxUint32 || last < lo { // nolint: gosec
		at := max(lo, math.MaxUint32+1)
		return at, errors.NewProcessingError("tx index %d does not fit the 32-bit in-memory tx index", at)
	}

	dup, err := m.idx.putRange(uint32(lo), n)
	if err != nil && !errors.Is(err, errors.ErrTxExists) {
		return lo, err
	}

	// as in Put: a node is also a duplicate of an overflow entry, and the first
	// duplicate either way is the one reported
	limit := n
	if err != nil {
		limit = int(uint64(dup) - lo) // nolint: gosec
	}

	if m.hasOverflow.Load() {
		for i := 0; i < limit; i++ {
			if _, ok := m.inOverflow(*m.hashAt(uint32(lo) + uint32(i))); ok { // nolint: gosec
				return lo + uint64(i), errors.NewTxExistsError("hash already exists in map") // nolint: gosec
			}
		}
	}

	return uint64(dup), err
}

// getBatch does a Get of every lookup in ls (nodeIndex.getBatch).
func (m *blockNodeIndex) getBatch(ls []nodeLookup) {
	m.idx.getBatch(ls)

	if !m.hasOverflow.Load() {
		return
	}

	for i := range ls {
		if !ls[i].found {
			ls[i].idx, ls[i].found = m.inOverflow(*ls[i].hash)
		}
	}
}

// Get returns the index of hash.
func (m *blockNodeIndex) Get(hash chainhash.Hash) (uint64, bool) {
	if idx, ok := m.idx.Get(hash); ok {
		return uint64(idx), true
	}

	return m.inOverflow(hash)
}

// Freeze makes the index read-only so Get takes no lock.
func (m *blockNodeIndex) Freeze() { m.idx.Freeze() }

// Close unpins the block's node arrays; the map must not be read after it.
// Idempotent.
func (m *blockNodeIndex) Close() error {
	m.unpinOnce.Do(func() {
		if m.unpin != nil {
			m.unpin()
		}
	})

	return nil
}

// Length is the number of txs in the index.
func (m *blockNodeIndex) Length() int {
	m.overflowMu.Lock()
	defer m.overflowMu.Unlock()

	return m.idx.Length() + len(m.overflow)
}

// txIndexEnd is one above the index of the last of length nodes of subtree sIdx, whose
// index space starts at sIdx*subtreeSize. It saturates at MaxUint64.
func txIndexEnd(sIdx, subtreeSize, length int) uint64 {
	hi, lo := mathbits.Mul64(uint64(sIdx), uint64(subtreeSize)) // nolint: gosec
	if hi != 0 {
		return math.MaxUint64
	}

	end, carry := mathbits.Add64(lo, uint64(length), 0) // nolint: gosec
	if carry != 0 {
		return math.MaxUint64
	}

	return end
}

// maxTxIndex is one above the highest index checkDuplicateTransactions can give a tx of
// slices, subtreeSize being the first subtree's capacity.
func maxTxIndex(slices []*subtreepkg.Subtree, subtreeSize int) uint64 {
	var limit uint64

	for sIdx, st := range slices {
		if st != nil {
			limit = max(limit, txIndexEnd(sIdx, subtreeSize, len(st.Nodes)))
		}
	}

	return limit
}

// newInMemoryBlockTxMap returns the in-memory map for numSubtrees subtrees of subtreeSize
// capacity, sized for entries, whose highest index is below limit (maxTxIndex). It is a
// blockNodeIndex when every index fits 32 bits and the 64-bit swiss map otherwise: a block
// with more than 2^32 txs is valid (issue 1428) and must neither wrap nor fail. A zero
// subtreeSize cannot address anything, so it also gets the swiss map.
func newInMemoryBlockTxMap(entries uint64, numSubtrees, subtreeSize int, limit uint64) blockTxMap {
	if subtreeSize <= 0 || numSubtrees <= 0 || limit > math.MaxUint32+1 {
		return newBlockTxMap(entries)
	}

	return newBlockNodeIndex(min(entries, maxTxMapPrealloc), numSubtrees, subtreeSize)
}
