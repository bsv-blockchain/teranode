package txmetacache

import (
	"crypto/rand"
	"encoding/binary"
	"math/bits"
	"sync/atomic"
	"unsafe"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// rawLockFreeCache is the byte-level API both lock-free layouts share, so the
// contract and stress tests can run against each.
type rawLockFreeCache interface {
	Set(key, value []byte) error
	SetMulti(keys, values [][]byte) error
	Get(dst *[]byte, key []byte) bool
	GetMulti(dst *[]byte, keys []chainhash.Hash, spans [][2]int)
	Del(key []byte)
	Reset()
	Oversize() uint64
	UpdateStats(s *Stats)
}

var (
	_ rawLockFreeCache = (*LockFreeCache)(nil)
	_ rawLockFreeCache = (*LaneCache)(nil)
)

// LockFreeCache is the per-shard-ring layout LaneCache replaced. It lives in a
// test file so the contract and stress tests, and benchmarks, can still
// compare the two; it does not ship.
//
// LockFreeCache is an off-heap txmeta cache whose reads take no lock and do no
// atomic read-modify-write, and whose writes never block readers or rebuild an
// index.
//
// Layout: the cache is split into shards (a power of two, at most
// BucketsCount), chosen by the first 8 bytes of the key. A key is a SHA256d
// transaction hash, so those bytes are already uniform and are not hashed
// again. Each shard has:
//
//   - a ring of R bytes holding records [8B header][32B key][value, padded to
//     8 bytes], appended at a monotonic 64-bit cursor (tail) that never wraps.
//     The record at offset o lives at ring position o % R.
//   - a set-associative index: sets of 8 uint64 slots, one cache line per set.
//     A slot is [16-bit fingerprint | offset/8], 0 when empty. The set and the
//     fingerprint come from key bytes 8..15 mixed with a per-process seed, so
//     a ground set of transaction ids cannot target one set.
//
// Validity: a writer reserves [a, b) of the cursor before writing, and that
// overwrites the ring positions of offsets [a-R, b-R). So the record at
// offset o is intact as long as o >= tail-R, and since offsets never wrap that
// one comparison replaces generations. Index slots that point at overwritten
// records are simply stale: lookups ignore them and inserts reuse them, so the
// index is never cleaned, rebuilt or resized.
//
// Reads are optimistic: load the slot, copy the record, then re-load tail. If
// the record was lapped while it was being copied, the copy is discarded and
// the lookup is a miss. A record's length field can be garbage mid-overwrite,
// so it is bounds-checked before anything is copied.
//
// The tail check is not enough on its own: a writer that is descheduled in the
// middle of writing its record can be lapped by the other writers, and its
// late stores then land on a newer record whose slot is live. So the header
// also carries a 48-bit checksum of key and value, and a copy whose checksum
// does not match is a miss. A torn copy can only pass if it hashes to the
// stored checksum, about one chance in 2^48 per torn read.
//
// Writes: reserve space with a CAS on the shard's tail, write the record, then
// publish its slot with a CAS. Several writers may share a shard (Kafka
// partitions, store write-back, GetMeta fills).
type LockFreeCache struct {
	shards   []lfShard
	shardBit uint64 // shard count - 1
	ringSize uint64 // R, bytes per shard ring, multiple of 8
	setMask  uint64 // sets per shard - 1
	seed     uint64

	region []byte // the single mmap holding every index and ring

	oversize atomic.Uint64

	// inserts is counted once per call, not per insert: a per-insert atomic
	// on the shard's line would be a second locked instruction on the line
	// the reservation CAS just took.
	inserts stripedCounter
}

// lfShard is padded to a cache line so writers on different shards never
// share one.
type lfShard struct {
	tail  atomic.Uint64
	ring  unsafe.Pointer // R bytes
	index unsafe.Pointer // (setMask+1)*8 uint64 slots
	_     [40]byte
}

const (
	lfOffsetBits = 48
	lfOffsetMask = 1<<lfOffsetBits - 1

	// lfMinShardRing keeps each shard's ring large enough to hold many maximum
	// size records; small caches get fewer shards instead of tiny rings.
	lfMinShardRing = 32 * 1024
)

// NewLockFreeCache allocates a cache of about maxBytes, rings and index
// included, in one mmap outside the Go heap.
func NewLockFreeCache(maxBytes int) (*LockFreeCache, error) {
	if maxBytes <= 0 {
		return nil, errors.NewServiceError("maxBytes must be greater than 0; got %d", maxBytes)
	}

	// Rings get 4/5 of the budget; the index takes at most R/4 per shard.
	ringBudget := uint64(maxBytes) / 5 * 4 // nolint:gosec // maxBytes > 0

	shards := uint64(BucketsCount)
	for shards > 1 && ringBudget/shards < lfMinShardRing {
		shards >>= 1
	}

	ringSize := max(ringBudget/shards, lfMinShardRing) &^ 7

	sets := nextPow2((ringSize/lfBytesPerSlot + lfSlotsPerSet - 1) / lfSlotsPerSet)
	indexBytes := sets * lfSlotsPerSet * 8

	if bits.Len64(sets-1) > 64-lfOffsetBits {
		return nil, errors.NewProcessingError("lock-free cache: %d sets per shard leave no room for the fingerprint", sets)
	}

	total := shards * (indexBytes + ringSize)
	if total > uint64(maxInt) {
		return nil, errors.NewProcessingError("lock-free cache: size %d overflows int", total)
	}

	// The seed first: a failure after the mmap would leak the region.
	var seed [8]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return nil, errors.NewProcessingError("lock-free cache: cannot read seed", err)
	}

	region, err := allocateNamedMmap(int(total))
	if err != nil {
		return nil, err
	}

	adviseHugePages(region)

	c := &LockFreeCache{
		shards:   make([]lfShard, shards),
		shardBit: shards - 1,
		ringSize: ringSize,
		setMask:  sets - 1,
		seed:     binary.LittleEndian.Uint64(seed[:]),
		region:   region,
	}

	// Indexes first (each a multiple of 64 bytes, so every set stays on one
	// cache line of the page-aligned mmap), then the rings.
	base := unsafe.Pointer(&region[0])
	ringsStart := shards * indexBytes

	for i := range c.shards {
		s := &c.shards[i]
		s.index = unsafe.Add(base, uint64(i)*indexBytes)         //nolint:gosec // inside region
		s.ring = unsafe.Add(base, ringsStart+uint64(i)*ringSize) //nolint:gosec // inside region
		// Offset 0 is reserved: a slot of 0 means empty.
		s.tail.Store(lfHeaderBytes)
	}

	return c, nil
}

// locate returns the key's shard, the address of its index set, and its
// fingerprint.
func (c *LockFreeCache) locate(key []byte) (*lfShard, unsafe.Pointer, uint64) {
	s := &c.shards[binary.LittleEndian.Uint64(key[0:8])&c.shardBit]
	h := fmix64(binary.LittleEndian.Uint64(key[8:16]) ^ c.seed)

	set := unsafe.Add(s.index, (h&c.setMask)*lfSlotsPerSet*8) //nolint:gosec // setMask bounds it

	return s, set, h >> lfOffsetBits
}

// storeWordPlain writes one record word; ordering comes from storeFence and
// from the CAS that publishes the record's slot.
func storeWordPlain(p *uint64, v uint64) {
	*p = v
}

func (c *LockFreeCache) wordAt(s *lfShard, pos uint64) *uint64 {
	return (*uint64)(unsafe.Add(s.ring, pos)) //nolint:gosec // callers bound pos
}

// live reports whether offset o has not been lapped as of tail t.
func (c *LockFreeCache) live(o, t uint64) bool {
	return o+c.ringSize >= t
}

// keyAt compares the 32-byte key stored in the record at ring position pos
// with key.
func (c *LockFreeCache) keyAt(s *lfShard, pos uint64, key []byte) bool {
	for w := uint64(0); w < 4; w++ {
		if atomic.LoadUint64(c.wordAt(s, pos+lfHeaderBytes+w*8)) != binary.LittleEndian.Uint64(key[w*8:]) {
			return false
		}
	}

	return true
}

// Set stores value under key, replacing any previous value. key must be a
// 32-byte transaction hash and value shorter than 2 KB. value is copied before
// Set returns, so the caller may reuse it.
func (c *LockFreeCache) Set(key, value []byte) error {
	if err := c.checkEntry(key, value); err != nil {
		return err
	}

	s, set, fp := c.locate(key)
	c.setLocated(s, set, fp, key, value)
	c.inserts.Add(1)

	return nil
}

func (c *LockFreeCache) checkEntry(key, value []byte) error {
	if len(key) != lfKeyBytes {
		return errors.NewProcessingError("lock-free cache: key must be %d bytes, got %d", lfKeyBytes, len(key))
	}

	if len(value) >= 1<<maxValueSizeLog {
		c.oversize.Add(1)
		return errors.NewProcessingError("lock-free cache: value of %d bytes exceeds the %d byte limit", len(value), 1<<maxValueSizeLog-1)
	}

	return nil
}

func (c *LockFreeCache) setLocated(s *lfShard, set unsafe.Pointer, fp uint64, key, value []byte) {
	need := recordBytes(len(value))
	o := c.reserve(s, need)

	c.writeRecord(s, o%c.ringSize, key, value)

	// Lapped while writing: the record may already be overwritten, and our
	// stores may have damaged the newer record that lapped it (its checksum
	// catches that). Either way there is nothing worth indexing.
	if !c.live(o, s.tail.Load()) {
		return
	}

	c.publish(s, set, fp, o)
}

// SetMulti stores every entry, carrying on past rejected ones, and returns the
// first error. Like GetMulti it works in groups whose cache misses (each
// entry's index set, and the ring line its record will be written to) are
// started together before any entry is written.
func (c *LockFreeCache) SetMulti(keys, values [][]byte) error {
	if len(keys) != len(values) {
		return errors.NewProcessingError("keys and values length mismatch; got %d keys and %d values", len(keys), len(values))
	}

	var (
		first  error
		sink   uint64
		stored uint64
		shards [lfPrefetchGroup]*lfShard
		sets   [lfPrefetchGroup]unsafe.Pointer
		fps    [lfPrefetchGroup]uint64
		ok     [lfPrefetchGroup]bool
	)

	for g := 0; g < len(keys); g += lfPrefetchGroup {
		n := min(lfPrefetchGroup, len(keys)-g)

		// Plain loads of the off-heap arena, used only to start the misses
		// (see GetMulti). The tail is a hint for where the record will go.
		for j := 0; j < n; j++ {
			key, value := keys[g+j], values[g+j]

			if err := c.checkEntry(key, value); err != nil {
				ok[j] = false

				if first == nil {
					first = err
				}

				continue
			}

			ok[j] = true
			shards[j], sets[j], fps[j] = c.locate(key)
			sink ^= *(*uint64)(sets[j])

			// tail is written atomically by other writers, so read it
			// atomically (a plain MOV on amd64).
			sink ^= *c.wordAt(shards[j], (shards[j].tail.Load()%c.ringSize)&^7)
		}

		for j := 0; j < n; j++ {
			if ok[j] {
				c.setLocated(shards[j], sets[j], fps[j], keys[g+j], values[g+j])
				stored++
			}
		}
	}

	c.inserts.Add(stored)

	if sink == 0x5eed {
		lfPrefetchSink.Add(1)
	}

	return first
}

// reserve claims need bytes of the shard's cursor and returns the record's
// offset. A record never straddles the ring's end: if it would, the tail of
// the ring is skipped and the record starts at position 0.
func (c *LockFreeCache) reserve(s *lfShard, need uint64) uint64 {
	for {
		t := s.tail.Load()
		start := t

		if pos := t % c.ringSize; pos+need > c.ringSize {
			start = t + c.ringSize - pos
		}

		if s.tail.CompareAndSwap(t, start+need) {
			return start
		}
	}
}

// writeRecord writes key, value and then the header at ring position pos. The
// fence first makes sure none of these stores can become visible before the
// reservation that made the space ours: a reader relies on that to detect a
// lap.
func (c *LockFreeCache) writeRecord(s *lfShard, pos uint64, key, value []byte) {
	storeFence()

	h := keyChecksum(key)

	for w := uint64(0); w < 4; w++ {
		storeWordPlain(c.wordAt(s, pos+lfHeaderBytes+w*8), binary.LittleEndian.Uint64(key[w*8:]))
	}

	p := pos + lfHeaderBytes + lfKeyBytes
	full := len(value) &^ 7

	for i := 0; i < full; i += 8 {
		word := binary.LittleEndian.Uint64(value[i:])
		h = checksumStep(h, word)
		storeWordPlain(c.wordAt(s, p+uint64(i)), word) //nolint:gosec // i >= 0
	}

	if rest := len(value) - full; rest > 0 {
		var last [8]byte

		copy(last[:], value[full:])
		word := binary.LittleEndian.Uint64(last[:])
		h = checksumStep(h, word)
		storeWordPlain(c.wordAt(s, p+uint64(full)), word) //nolint:gosec // full >= 0
	}

	storeWordPlain(c.wordAt(s, pos), checksumFinal(h, uint64(len(value)))|uint64(len(value))) //nolint:gosec // len < 2KB
}

// publish points the key's index slot at offset o. Preference: a slot that
// already holds this key's fingerprint (the new value replaces it), then an
// empty or stale slot, then the slot holding the oldest record in the set.
func (c *LockFreeCache) publish(s *lfShard, set unsafe.Pointer, fp, o uint64) {
	newSlot := fp<<lfOffsetBits | o>>3

	for attempt := 0; attempt < lfMaxCASRetries; attempt++ {
		t := s.tail.Load()

		var (
			victim    = -1
			victimVal uint64
			free      = -1
			freeVal   uint64
			oldest    = -1
			oldestVal uint64
			oldestOff = uint64(1<<64 - 1)
		)

		for i := 0; i < lfSlotsPerSet; i++ {
			v := atomic.LoadUint64(slotAt(set, i))
			if v == 0 {
				if free < 0 {
					free, freeVal = i, 0
				}

				continue
			}

			so := (v & lfOffsetMask) << 3
			if so == o {
				// Already published by an earlier attempt that we lost track of;
				// cannot happen with a single publisher per record, kept for safety.
				return
			}

			if !c.live(so, t) {
				if free < 0 {
					free, freeVal = i, v
				}

				continue
			}

			// A fingerprint match is taken to be this key without reading the
			// old record: that read is a cold, random cache miss on every
			// re-insert. A false match (shard, set and 16 fingerprint bits all
			// equal, about one insert in 8k) evicts the other key's entry,
			// which costs it a later miss but can never return wrong data:
			// reads compare the full key and the checksum.
			if v>>lfOffsetBits == fp {
				// A newer record of this key is already indexed (a writer
				// that reserved after us published first): ours is stale,
				// and indexing it too could let a lookup return it.
				if so > o {
					return
				}

				victim, victimVal = i, v

				break
			}

			if so < oldestOff {
				oldest, oldestVal, oldestOff = i, v, so
			}
		}

		switch {
		case victim >= 0:
		case free >= 0:
			victim, victimVal = free, freeVal
		case oldestOff < o:
			victim, victimVal = oldest, oldestVal
		default:
			// Every slot holds a newer record: ours is the oldest, drop it.
			return
		}

		if atomic.CompareAndSwapUint64(slotAt(set, victim), victimVal, newSlot) {
			return
		}
	}
}

// Get appends the value stored under key to *dst and reports whether it was
// found. On a miss *dst is unchanged.
func (c *LockFreeCache) Get(dst *[]byte, key []byte) bool {
	if len(key) != lfKeyBytes {
		return false
	}

	s, set, fp := c.locate(key)

	return c.getLocated(s, set, fp, key, dst)
}

// getLocated is Get for a key whose shard, set and fingerprint are known.
func (c *LockFreeCache) getLocated(s *lfShard, set unsafe.Pointer, fp uint64, key []byte, dst *[]byte) bool {
	for i := 0; i < lfSlotsPerSet; i++ {
		v := atomic.LoadUint64(slotAt(set, i))
		if v == 0 || v>>lfOffsetBits != fp {
			continue
		}

		if c.read(s, (v&lfOffsetMask)<<3, key, dst) {
			return true
		}
	}

	return false
}

// read copies the value of the record at offset o into *dst if the record
// holds key and was not lapped while it was read.
func (c *LockFreeCache) read(s *lfShard, o uint64, key []byte, dst *[]byte) bool {
	if !c.live(o, s.tail.Load()) {
		return false
	}

	pos := o % c.ringSize

	// Plain loads: the slot was loaded with acquire semantics, and loadFence
	// below orders all of these before the validating tail load.
	header := *c.wordAt(s, pos)
	valueLen := header & lfLenMask

	// A record being overwritten can show any length: bound it before using it.
	if valueLen >= 1<<maxValueSizeLog || pos+recordBytes(int(valueLen)) > c.ringSize { //nolint:gosec // bounded above
		return false
	}

	for w := uint64(0); w < 4; w++ {
		if *c.wordAt(s, pos+lfHeaderBytes+w*8) != binary.LittleEndian.Uint64(key[w*8:]) {
			return false
		}
	}

	start := len(*dst)
	out := *dst
	h := keyChecksum(key)

	p := pos + lfHeaderBytes + lfKeyBytes
	for i := uint64(0); i < valueLen; i += 8 {
		word := *c.wordAt(s, p+i)
		h = checksumStep(h, word)
		out = binary.LittleEndian.AppendUint64(out, word)
	}

	loadFence()

	// The copy only counts if nothing lapped the record meanwhile, and if no
	// late writer damaged it (see the type comment).
	if !c.live(o, s.tail.Load()) || checksumFinal(h, valueLen) != header&^lfLenMask {
		return false
	}

	*dst = out[:uint64(start)+valueLen] //nolint:gosec // start >= 0

	return true
}

// GetMulti looks up every key, appending found values to *dst: value i is
// (*dst)[spans[i][0]:spans[i][1]], or spans[i] = {-1, -1} on a miss.
func (c *LockFreeCache) GetMulti(dst *[]byte, keys []chainhash.Hash, spans [][2]int) {
	var (
		sink   uint64
		shards [lfPrefetchGroup]*lfShard
		sets   [lfPrefetchGroup]unsafe.Pointer
		fps    [lfPrefetchGroup]uint64
	)

	for g := 0; g < len(keys); g += lfPrefetchGroup {
		group := keys[g:min(g+lfPrefetchGroup, len(keys))]

		// Plain loads, used only to start the misses: on arm64 an atomic load
		// is an acquire and would make each miss wait for the previous one.
		// Nothing below trusts what they return.
		for j := range group {
			shards[j], sets[j], fps[j] = c.locate(group[j][:])
			sink ^= *(*uint64)(sets[j])
		}

		for j := range group {
			for i := 0; i < lfSlotsPerSet; i++ {
				if v := *slotAt(sets[j], i); v != 0 && v>>lfOffsetBits == fps[j] {
					sink ^= *c.wordAt(shards[j], ((v&lfOffsetMask)<<3)%c.ringSize)
				}
			}
		}

		for j := range group {
			i := g + j
			start := len(*dst)

			if c.getLocated(shards[j], sets[j], fps[j], keys[i][:], dst) {
				spans[i] = [2]int{start, len(*dst)}
			} else {
				spans[i] = [2]int{-1, -1}
			}
		}
	}

	if sink == 0x5eed {
		lfPrefetchSink.Add(1)
	}
}

// Del removes key. Every slot holding it is cleared, in case two writers
// inserted it concurrently into different slots.
func (c *LockFreeCache) Del(key []byte) {
	if len(key) != lfKeyBytes {
		return
	}

	s, set, fp := c.locate(key)

	for i := 0; i < lfSlotsPerSet; i++ {
		slot := slotAt(set, i)

		v := atomic.LoadUint64(slot)
		if v == 0 || v>>lfOffsetBits != fp {
			continue
		}

		so := (v & lfOffsetMask) << 3
		if !c.live(so, s.tail.Load()) || !c.keyAt(s, so%c.ringSize, key) {
			continue
		}

		// A failed CAS means the slot changed under us: either someone else
		// cleared it or a newer insert took it, and that insert wins.
		atomic.CompareAndSwapUint64(slot, v, 0)
	}
}

// Reset drops every entry by advancing each shard's cursor a full ring: every
// existing offset is then lapped.
func (c *LockFreeCache) Reset() {
	for i := range c.shards {
		c.shards[i].tail.Add(c.ringSize)
	}

	c.inserts.reset()
}

// Oversize is the number of values rejected for exceeding the size limit.
func (c *LockFreeCache) Oversize() uint64 {
	return c.oversize.Load()
}

// UpdateStats adds the cache's entry estimate to s. The live-entry count is
// estimated from inserts and bytes written rather than by scanning the index,
// which is tens of GB on a large cache.
func (c *LockFreeCache) UpdateStats(s *Stats) {
	inserts := c.inserts.Load()

	var written uint64

	for i := range c.shards {
		written += c.shards[i].tail.Load() - lfHeaderBytes
	}

	live := inserts
	if capacity := uint64(len(c.shards)) * c.ringSize; written > capacity {
		live = uint64(float64(inserts) * float64(capacity) / float64(written))
	}

	s.CurrentGenEntries += live
	s.ValidEntriesCount += live
	s.TotalMapSize += live
	s.TotalElementsAdded += inserts
}
