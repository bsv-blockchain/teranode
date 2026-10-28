package txmetacache

import (
	"crypto/rand"
	"encoding/binary"
	mrand "math/rand/v2"
	"runtime"
	"sync/atomic"
	"unsafe"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// LaneCache is an off-heap txmeta cache whose reads take no lock, keeping its
// records in per-writer lane rings. (An earlier per-shard-ring layout,
// LockFreeCache, is kept in a test file for comparison; lanes measured 30-60%
// faster for both ingest and reads on the production subtree nodes.)
//
// A writer takes a lane for a whole batch (one CAS) and appends its records to
// that lane's ring one after another, so a batch's record writes are
// sequential: the next record's lines are usually already in cache or on their
// way, and a 4 KB page is walked once for ~30 records rather than once per
// record. Because a lane has one writer at a time, nothing can lap a record
// while it is being written, and the writer needs no CAS per record to reserve
// space: it publishes the lane's new tail once per chunk.
//
// The index is one set-associative table (8 uint64 slots per cache line),
// chosen from key bytes 8..15 mixed with a per-process seed. A slot is
// [16-bit fingerprint | 8-bit lane | 40-bit offset/8]. Lane offsets are
// monotonic and only their low 43 bits are stored; the full offset is rebuilt
// from the lane's tail, and an offset more than one ring behind the tail is
// stale. An offset 8 TB behind would alias onto a recent one; the record's key
// comparison and checksum reject it.
//
// Reads are optimistic: check the lane's tail, copy the record, issue a load
// fence, and check the tail again; a copy of a record that was lapped meanwhile
// is a miss. A record's length can be garbage mid-overwrite, so it is bounded
// before anything is copied. The 48-bit key and value checksum (see
// lockfree_common.go) backs the tail check up.
type LaneCache struct {
	lanes    []lfLane
	ringSize uint64 // bytes per lane ring, a power of two
	ringMask uint64 // ringSize - 1
	setMask  uint64
	index    unsafe.Pointer
	seed     uint64

	region []byte

	oversize  atomic.Uint64
	inserts   stripedCounter
	laneWaits atomic.Uint64

	// lapped is the highest lapped-entry estimate UpdateStats has reported
	// since the last Reset, so the evictions it feeds never go down between
	// calls. resetEvicted counts the entries every Reset dropped.
	lapped       atomic.Uint64
	resetEvicted atomic.Uint64

	// resetWritten is the lane bytes written up to the last Reset, so the
	// estimate only sees what was written since.
	resetWritten atomic.Uint64
}

// lfLane is three cache lines: owner (scanned by writers looking for a lane),
// the writer-side tail, and the read-mostly tail snapshot.
type lfLane struct {
	// owner is on its own cache line: writers looking for a free lane scan
	// it, and it changes twice per batch.
	owner atomic.Uint32
	_     [60]byte

	tail atomic.Uint64
	ring unsafe.Pointer
	_    [48]byte

	// snap is the lane's tail as of its last refresh, on its own cache line.
	// Writers judge other lanes' slots (stale? oldest?) by it rather than by
	// tail: tail's line is written by the lane's writer every few records, so
	// reading it for up to eight slots per insert took that many cache misses
	// on lines other cores were writing. snap is refreshed every 1/16 ring, so
	// its line stays shared. Lagging behind tail only makes a slot look live
	// for longer, which costs at most a slightly worse eviction choice.
	snap atomic.Uint64
	_    [56]byte
}

const (
	lnLaneBits = 8
	lnOffBits  = 40
	lnOffMask  = 1<<lnOffBits - 1
	lnMaxLanes = 1 << lnLaneBits
	lnFpShift  = lnLaneBits + lnOffBits
	lnMinRing  = 64 * 1024
)

// NewLaneCache allocates a cache of about maxBytes in one mmap outside the Go
// heap.
func NewLaneCache(maxBytes int) (*LaneCache, error) {
	if maxBytes <= 0 {
		return nil, errors.NewServiceError("maxBytes must be greater than 0; got %d", maxBytes)
	}

	ringBudget := uint64(maxBytes) / 5 * 4 // nolint:gosec // maxBytes > 0

	// A power-of-two ring turns every offset-to-position reduction into a
	// mask instead of a 64-bit division (one per insert and one per read).
	// The ring is the smallest power of two that keeps the lane count within
	// lnMaxLanes; the lane count then absorbs the rounding, so less than one
	// ring's worth of the budget goes unused, and a large cache gets between
	// half and all of lnMaxLanes. Doubling the lane count (9-bit lane ids) to
	// stay above the subtree node's peak writer count (up to 256 BatchDecorate
	// write-back lanes plus the Kafka partition workers) measured about 30%
	// slower at the production shape: twice the write streams on rings half
	// the size. So writers can outnumber lanes in a burst; a writer that finds
	// none free waits and is counted in LaneWaits.
	ringSize := max(nextPow2((ringBudget+lnMaxLanes-1)/lnMaxLanes), lnMinRing)
	for ringSize > lnMinRing && ringSize > ringBudget {
		ringSize >>= 1
	}

	lanes := max(1, ringBudget/ringSize)

	sets := nextPow2((lanes*ringSize/lfBytesPerSlot + lfSlotsPerSet - 1) / lfSlotsPerSet)
	indexBytes := sets * lfSlotsPerSet * 8

	total := indexBytes + lanes*ringSize
	if total > uint64(maxInt) {
		return nil, errors.NewProcessingError("lane cache: size %d overflows int", total)
	}

	// The seed first: a failure after the mmap would leak the region.
	var seed [8]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return nil, errors.NewProcessingError("lane cache: cannot read seed", err)
	}

	region, err := allocateNamedMmap(int(total))
	if err != nil {
		return nil, err
	}

	adviseHugePages(region)

	c := &LaneCache{
		lanes:    make([]lfLane, lanes),
		ringSize: ringSize,
		ringMask: ringSize - 1,
		setMask:  sets - 1,
		index:    unsafe.Pointer(&region[0]),
		seed:     binary.LittleEndian.Uint64(seed[:]),
		region:   region,
	}

	for i := range c.lanes {
		c.lanes[i].ring = unsafe.Add(c.index, indexBytes+uint64(i)*ringSize) //nolint:gosec // inside region
		// Offset 0 is reserved so that no valid slot is 0.
		c.lanes[i].tail.Store(lfHeaderBytes)
		c.lanes[i].snap.Store(lfHeaderBytes)
	}

	return c, nil
}

// locate returns the key's index set and fingerprint.
func (c *LaneCache) locate(key []byte) (unsafe.Pointer, uint64) {
	h := fmix64(binary.LittleEndian.Uint64(key[8:16]) ^ c.seed)

	return unsafe.Add(c.index, (h&c.setMask)*lfSlotsPerSet*8), h >> lnFpShift //nolint:gosec // setMask bounds it
}

func (c *LaneCache) wordAt(l *lfLane, pos uint64) *uint64 {
	return (*uint64)(unsafe.Add(l.ring, pos)) //nolint:gosec // callers bound pos
}

// decode rebuilds a slot's lane and full offset; ok is false if the offset is
// more than one ring behind the lane's tail.
func (c *LaneCache) decode(v uint64) (l *lfLane, o uint64, ok bool) {
	lane := (v >> lnOffBits) & (lnMaxLanes - 1)
	if lane >= uint64(len(c.lanes)) {
		return nil, 0, false
	}

	l = &c.lanes[lane]
	t := l.tail.Load()
	d := ((t >> 3) - (v & lnOffMask)) & lnOffMask * 8

	return l, t - d, d <= c.ringSize
}

// ageBySnapshot is how far slot v's record is behind its lane's tail snapshot,
// and whether it is still within one ring of it (see lfLane.snap).
func (c *LaneCache) ageBySnapshot(v uint64) (uint64, bool) {
	lane := (v >> lnOffBits) & (lnMaxLanes - 1)
	if lane >= uint64(len(c.lanes)) {
		return 0, false
	}

	snap := c.lanes[lane].snap.Load()
	d := ((snap >> 3) - (v & lnOffMask)) & lnOffMask * 8

	// d wraps to near 2^43 for a record written after the snapshot: that one
	// is young, not stale.
	if d > lnOffMask*8/2 {
		return 0, true
	}

	return d, d <= c.ringSize
}

func (c *LaneCache) slotValue(fp uint64, lane int, o uint64) uint64 {
	return fp<<lnFpShift | uint64(lane)<<lnOffBits | (o>>3)&lnOffMask //nolint:gosec // lane < 256
}

// acquire takes a free lane, starting at a random one so that lanes fill
// evenly and eviction stays close to global FIFO.
func (c *LaneCache) acquire() int {
	n := len(c.lanes)
	start := int(mrand.Uint64() % uint64(n)) //nolint:gosec // not security relevant

	for {
		for i := 0; i < n; i++ {
			lane := (start + i) % n
			if c.lanes[lane].owner.Load() == 0 && c.lanes[lane].owner.CompareAndSwap(0, 1) {
				return lane
			}
		}

		// Every lane is busy: more writers than lanes right now.
		c.laneWaits.Add(1)
		runtime.Gosched()
	}
}

func (c *LaneCache) release(lane int) {
	c.lanes[lane].owner.Store(0)
}

func (c *LaneCache) checkEntry(key, value []byte) error {
	if len(key) != lfKeyBytes {
		return errors.NewProcessingError("lane cache: key must be %d bytes, got %d", lfKeyBytes, len(key))
	}

	if len(value) >= 1<<maxValueSizeLog {
		c.oversize.Add(1)
		return errors.NewProcessingError("lane cache: value of %d bytes exceeds the %d byte limit", len(value), 1<<maxValueSizeLog-1)
	}

	return nil
}

// Set stores value under key, replacing any previous value.
func (c *LaneCache) Set(key, value []byte) error {
	return c.SetMulti([][]byte{key}, [][]byte{value})
}

// SetMulti stores every entry, carrying on past rejected ones, and returns the
// first error.
func (c *LaneCache) SetMulti(keys, values [][]byte) error {
	if len(keys) != len(values) {
		return errors.NewProcessingError("keys and values length mismatch; got %d keys and %d values", len(keys), len(values))
	}

	var (
		first  error
		stored uint64
		sink   uint64
		sets   [lfPrefetchGroup]unsafe.Pointer
		fps    [lfPrefetchGroup]uint64
		offs   [lfPrefetchGroup]uint64
		ok     [lfPrefetchGroup]bool
	)

	lane := c.acquire()
	l := &c.lanes[lane]
	t := l.tail.Load()

	for g := 0; g < len(keys); g += lfPrefetchGroup {
		n := min(lfPrefetchGroup, len(keys)-g)

		// Locate the group and start its index sets' misses (plain loads of
		// the off-heap index whose values are not used; see GetMulti), and
		// lay its records out after the lane tail.
		end := t

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
			sets[j], fps[j] = c.locate(key)
			sink ^= *(*uint64)(sets[j])

			need := recordBytes(len(value))
			if pos := end & c.ringMask; pos+need > c.ringSize {
				end += c.ringSize - pos
			}

			offs[j] = end
			end += need
		}

		// Publish the tail before overwriting anything, so a reader of an
		// older record at these positions sees it lapped. storeFence keeps
		// the record stores from becoming visible before the tail on arm64;
		// on amd64 the atomic store is itself a full fence.
		l.tail.Store(end)
		storeFence()

		if end-l.snap.Load() > c.ringSize/16 {
			l.snap.Store(end)
		}

		for j := 0; j < n; j++ {
			if ok[j] {
				writeLaneRecord(c, l, offs[j]&c.ringMask, keys[g+j], values[g+j])
			}
		}

		for j := 0; j < n; j++ {
			if ok[j] {
				c.publish(sets[j], fps[j], c.slotValue(fps[j], lane, offs[j]))
				stored++
			}
		}

		t = end
	}

	c.release(lane)
	c.inserts.Add(stored)

	if sink == 0x5eed {
		lfPrefetchSink.Add(1)
	}

	return first
}

// writeLaneRecord writes header, key and value at ring position pos, in the
// record format described in lockfree_common.go.
func writeLaneRecord(c *LaneCache, l *lfLane, pos uint64, key, value []byte) {
	h := keyChecksum(key)

	for w := uint64(0); w < 4; w++ {
		*c.wordAt(l, pos+lfHeaderBytes+w*8) = binary.LittleEndian.Uint64(key[w*8:])
	}

	p := pos + lfHeaderBytes + lfKeyBytes
	full := len(value) &^ 7

	for i := 0; i < full; i += 8 {
		word := binary.LittleEndian.Uint64(value[i:])
		h = checksumStep(h, word)
		*c.wordAt(l, p+uint64(i)) = word //nolint:gosec // i >= 0
	}

	if len(value) > full {
		var last [8]byte

		copy(last[:], value[full:])
		word := binary.LittleEndian.Uint64(last[:])
		h = checksumStep(h, word)
		*c.wordAt(l, p+uint64(full)) = word //nolint:gosec // full >= 0
	}

	*c.wordAt(l, pos) = checksumFinal(h, uint64(len(value))) | uint64(len(value)) //nolint:gosec // len < 2KB
}

// publish points a slot of the set at the new record. Preference: a slot with
// this key's fingerprint (the new value replaces it), then an empty or stale
// slot, then the slot whose record is closest to being overwritten.
//
// A fingerprint match is taken to be this key without reading the old record:
// that read would be a cold, random cache miss on every re-insert. A false
// match (same set and same 16 fingerprint bits) evicts the other key's entry,
// which costs it a later miss but never returns wrong data: reads compare the
// full key and the checksum.
//
// Records in different lanes cannot be ordered, so when two writers race on
// one key the one that publishes last wins, even if it reserved first. Cached
// txmeta for a transaction is the same whoever writes it, so this does not
// change what a lookup returns in practice.
func (c *LaneCache) publish(set unsafe.Pointer, fp, newSlot uint64) {
	for attempt := 0; attempt < lfMaxCASRetries; attempt++ {
		victim, oldest := -1, -1

		var victimVal, oldestVal, maxAge uint64

		for i := 0; i < lfSlotsPerSet; i++ {
			v := atomic.LoadUint64(slotAt(set, i))
			if v == 0 {
				if victim < 0 {
					victim, victimVal = i, 0
				}

				continue
			}

			if v>>lnFpShift == fp {
				victim, victimVal = i, v
				break
			}

			age, live := c.ageBySnapshot(v)
			if !live {
				if victim < 0 {
					victim, victimVal = i, v
				}

				continue
			}

			if oldest < 0 || age > maxAge {
				oldest, oldestVal, maxAge = i, v, age
			}
		}

		if victim < 0 {
			victim, victimVal = oldest, oldestVal
		}

		if atomic.CompareAndSwapUint64(slotAt(set, victim), victimVal, newSlot) {
			return
		}
	}
}

// Get appends the value stored under key to *dst and reports whether it was
// found. On a miss *dst is unchanged.
func (c *LaneCache) Get(dst *[]byte, key []byte) bool {
	if len(key) != lfKeyBytes {
		return false
	}

	set, fp := c.locate(key)

	return c.getLocated(set, fp, key, dst)
}

func (c *LaneCache) getLocated(set unsafe.Pointer, fp uint64, key []byte, dst *[]byte) bool {
	for i := 0; i < lfSlotsPerSet; i++ {
		v := atomic.LoadUint64(slotAt(set, i))
		if v == 0 || v>>lnFpShift != fp {
			continue
		}

		if c.read(v, key, dst) {
			return true
		}
	}

	return false
}

// read copies the value of the record slot v points at, if it holds key, was
// not lapped while being read, and its checksum verifies.
func (c *LaneCache) read(v uint64, key []byte, dst *[]byte) bool {
	l, o, live := c.decode(v)
	if !live {
		return false
	}

	pos := o & c.ringMask
	header := *c.wordAt(l, pos)
	valueLen := header & lfLenMask

	if valueLen >= 1<<maxValueSizeLog || pos+recordBytes(int(valueLen)) > c.ringSize { //nolint:gosec // bounded above
		return false
	}

	for w := uint64(0); w < 4; w++ {
		if *c.wordAt(l, pos+lfHeaderBytes+w*8) != binary.LittleEndian.Uint64(key[w*8:]) {
			return false
		}
	}

	start := len(*dst)
	out := *dst
	h := keyChecksum(key)

	p := pos + lfHeaderBytes + lfKeyBytes
	for i := uint64(0); i < valueLen; i += 8 {
		word := *c.wordAt(l, p+i)
		h = checksumStep(h, word)
		out = binary.LittleEndian.AppendUint64(out, word)
	}

	loadFence()

	if o+c.ringSize < l.tail.Load() || checksumFinal(h, valueLen) != header&^lfLenMask {
		return false
	}

	*dst = out[:uint64(start)+valueLen] //nolint:gosec // start >= 0

	return true
}

// GetMulti looks up every key, appending found values to *dst: value i is
// (*dst)[spans[i][0]:spans[i][1]], or spans[i] = {-1, -1} on a miss.
//
// Lookups are handled in groups of lfPrefetchGroup: first every index set is
// touched, then every candidate record, then the records are read, so the
// group's cache misses overlap. The touches are plain loads whose values are
// not used: on arm64 an atomic load is an acquire and would make each miss wait
// for the previous one.
func (c *LaneCache) GetMulti(dst *[]byte, keys []chainhash.Hash, spans [][2]int) {
	var (
		sink uint64
		sets [lfPrefetchGroup]unsafe.Pointer
		fps  [lfPrefetchGroup]uint64
	)

	for g := 0; g < len(keys); g += lfPrefetchGroup {
		group := keys[g:min(g+lfPrefetchGroup, len(keys))]

		for j := range group {
			sets[j], fps[j] = c.locate(group[j][:])
			sink ^= *(*uint64)(sets[j])
		}

		for j := range group {
			for i := 0; i < lfSlotsPerSet; i++ {
				if v := *slotAt(sets[j], i); v != 0 && v>>lnFpShift == fps[j] {
					if lane := (v >> lnOffBits) & (lnMaxLanes - 1); lane < uint64(len(c.lanes)) {
						sink ^= *c.wordAt(&c.lanes[lane], ((v&lnOffMask)<<3)&c.ringMask)
					}
				}
			}
		}

		for j := range group {
			i := g + j
			start := len(*dst)

			if c.getLocated(sets[j], fps[j], keys[i][:], dst) {
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

// Del removes key. Every slot holding it is cleared.
func (c *LaneCache) Del(key []byte) {
	if len(key) != lfKeyBytes {
		return
	}

	set, fp := c.locate(key)

	for i := 0; i < lfSlotsPerSet; i++ {
		slot := slotAt(set, i)

		v := atomic.LoadUint64(slot)
		if v == 0 || v>>lnFpShift != fp || !c.keyAt(v, key) {
			continue
		}

		// A failed CAS means the slot changed under us: someone else cleared
		// it or a newer insert took it, and that insert wins.
		atomic.CompareAndSwapUint64(slot, v, 0)
	}
}

// keyAt reports whether the record slot v points at holds key, comparing the
// key in place rather than copying the record.
func (c *LaneCache) keyAt(v uint64, key []byte) bool {
	lane := (v >> lnOffBits) & (lnMaxLanes - 1)
	if lane >= uint64(len(c.lanes)) {
		return false
	}

	l := &c.lanes[lane]

	pos := ((v & lnOffMask) << 3) & c.ringMask
	if pos+lfHeaderBytes+lfKeyBytes > c.ringSize {
		return false
	}

	for w := uint64(0); w < 4; w++ {
		if atomic.LoadUint64(c.wordAt(l, pos+lfHeaderBytes+w*8)) != binary.LittleEndian.Uint64(key[w*8:]) {
			return false
		}
	}

	return true
}

// Reset drops every entry by advancing each lane's tail a full ring. It must
// not run concurrently with writers.
func (c *LaneCache) Reset() {
	for i := range c.lanes {
		// Bring the snapshot along, so publish sees the old records as the
		// stale slots they now are (see lfLane.snap).
		c.lanes[i].snap.Store(c.lanes[i].tail.Add(c.ringSize + 8))
	}

	// Every entry is gone: bank them all as evicted and restart the estimate.
	c.resetEvicted.Add(c.inserts.Load())
	c.resetWritten.Store(c.written())
	c.lapped.Store(0)
	c.inserts.reset()
}

// Oversize is the number of values rejected for exceeding the size limit.
func (c *LaneCache) Oversize() uint64 {
	return c.oversize.Load()
}

// written returns the bytes written to all lanes since the cache was made.
func (c *LaneCache) written() uint64 {
	var written uint64

	for i := range c.lanes {
		written += c.lanes[i].tail.Load() - lfHeaderBytes
	}

	return written
}

// UpdateStats adds the cache's entry estimate to s.
func (c *LaneCache) UpdateStats(s *Stats) {
	inserts := c.inserts.Load()

	written := c.written() - c.resetWritten.Load()
	capacity := uint64(len(c.lanes)) * c.ringSize

	live := inserts
	if written > capacity {
		live = uint64(float64(inserts) * float64(capacity) / float64(written))
	}

	// Entries a ring wrap overwrote are the cache's evictions. The estimate
	// can dip slightly as the average record size drifts; report the highest
	// value seen so a rate() over the metric never reads a dip as a reset.
	lapped := inserts - live
	for {
		prev := c.lapped.Load()
		if lapped <= prev {
			lapped = prev
			break
		}

		if c.lapped.CompareAndSwap(prev, lapped) {
			break
		}
	}

	// A concurrent Reset can leave the high-water mark above the restarted
	// insert count for a moment.
	lapped = min(lapped, inserts)
	live = inserts - lapped

	// An estimate: a record superseded by a later Set of the same key, or
	// deleted, also counts once the ring wraps over it.
	s.Evictions += c.resetEvicted.Load() + lapped
	s.CurrentGenEntries += live
	s.ValidEntriesCount += live
	s.TotalMapSize += live
	s.TotalElementsAdded += inserts
	s.LaneWaits += c.laneWaits.Load()
	s.Bytes += min(written, capacity)
	s.CapacityBytes += capacity
}
