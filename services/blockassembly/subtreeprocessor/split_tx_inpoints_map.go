package subtreeprocessor

import (
	"encoding/binary"
	"runtime"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/dolthub/swiss"
)

// SplitTxInpointsMap is the in-memory currentTxMap: the inpoints of every tx held
// in block assembly, sharded over nrOfBuckets independently locked buckets.
//
// A bucket does not keep a *TxInpoints per tx. That is a heap object graph of
// several allocations per tx (the struct, the parent hash slice, the packed vout
// slice) which the GC has to mark on every cycle, and at hundreds of millions of
// txs the mark time dominated everything else. Instead each bucket serializes the
// inpoints into byte chunks it owns, and a swiss index maps the tx hash to the
// offset of the record. The index and the chunks hold no pointers, so the GC does
// not scan them, and there is no per-tx allocation to keep alive.
//
// A record is [2B subtree index][1B flags][4B payload length][payload]. The
// payload is the TxInpoints.Serialize wire format, so the stored bytes can be
// appended to subtree meta, or copied to the other half of the double buffer,
// without decoding them. The flags only exist to give back exactly what was
// stored for the shapes the wire format cannot tell apart (see the flag consts).
//
// Records are append-only. Set over an existing hash and Delete only touch the
// index and leave the old record behind; Clear releases all of it, so the retired
// half of the double buffer holds no arena next to the current one. Clear runs on
// each half of the double buffer once per block, so the garbage is bounded by one block's churn, provided
// callers do not re-put records they already hold (reChainSubtrees does not).
//
// Lifetime: no method hands out arena bytes. Every read copies the record under
// the bucket lock, into a fresh TxInpoints (Get), into the caller's buffer
// (AppendSerialized, AppendMetaInpoints) or into another map (MoveTo,
// MoveBucketFrom). The owner clears a half only when it is retired, but a reader
// that captured the half (the subtree meta builder runs on its own goroutine) can
// still be running; with the copy under the lock such a reader sees either the
// whole record or a miss, exactly as it did when Clear emptied the swiss maps of
// pointers. It never sees reused bytes of another record.
type SplitTxInpointsMap struct {
	buckets     []txInpointsBucket
	nrOfBuckets uint16
}

type txInpointsBucket struct {
	mu sync.Mutex
	// m maps the tx hash to the handle of its record: chunk<<32 | offset. It is
	// created by the first insert, so a map that holds nothing (a small node, or
	// the retired half) does not pay for 16384 empty indexes.
	m      *swiss.Map[chainhash.Hash, uint64]
	chunks [][]byte // released by Clear
	cur    int      // chunk being filled
	// hint is the bytes the last cleared generation used; the first chunk of the
	// next fill takes that size, so a refill of about the same size fits in one
	// chunk instead of a doubling series that holds up to twice what it uses
	hint int
}

const (
	// txInpointsFirstChunk and txInpointsMaxChunk bound the chunk size: a bucket
	// starts small and doubles up to the max, so small nodes stay small and large
	// nodes do not make millions of tiny allocations. A record larger than the max
	// gets a chunk of its own.
	txInpointsFirstChunk = 512
	txInpointsMaxChunk   = 64 << 10

	// txInpointsRecordHeader is the subtree index, the flags and the payload length.
	txInpointsRecordHeader = 2 + 1 + 4

	// txInpointsIndexHint is the capacity the index of a bucket starts with. The
	// index grows on demand.
	txInpointsIndexHint = 8

	// txInpointsScratch is the stack buffer a read copies a record into before it
	// releases the lock; bigger records spill to the heap.
	txInpointsScratch = 256
)

// Record flags. The wire format cannot tell these apart, but the TxInpoints that
// went in can, and callers (subtree meta serialization refuses nil parents) do.
const (
	// recordNil: a nil *TxInpoints was stored; Get returns nil, true.
	recordNil byte = 1 << iota
	// recordParentsSet: the parent hash slice was non-nil. Only matters when it is empty.
	recordParentsSet
	// recordNoVouts: the inpoints could not be serialized (parent hashes without
	// matching vouts). The payload holds only the parent count and hashes, and
	// decodes to parent hashes without vouts, which is what failed to serialize in
	// the first place.
	recordNoVouts
)

var errNilInpoints = errors.NewProcessingError("the stored inpoints are nil")

func NewSplitTxInpointsMap(nrOfBuckets uint16) *SplitTxInpointsMap {
	return &SplitTxInpointsMap{
		buckets:     make([]txInpointsBucket, nrOfBuckets),
		nrOfBuckets: nrOfBuckets,
	}
}

func (s *SplitTxInpointsMap) bucket(hash chainhash.Hash) *txInpointsBucket {
	return &s.buckets[txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)]
}

// has reports whether hash is indexed; the caller holds the bucket lock.
func (b *txInpointsBucket) has(hash chainhash.Hash) bool {
	return b.m != nil && b.m.Has(hash)
}

// handle returns the handle of the record of hash; the caller holds the bucket lock.
func (b *txInpointsBucket) handle(hash chainhash.Hash) (uint64, bool) {
	if b.m == nil {
		return 0, false
	}

	return b.m.Get(hash)
}

// record returns the arena bytes of the record at handle h. They are only valid
// while the caller holds the bucket lock.
func (b *txInpointsBucket) record(h uint64) []byte {
	chunk, off := b.chunks[h>>32], uint32(h)
	n := binary.LittleEndian.Uint32(chunk[off+3:])

	return chunk[off : off+txInpointsRecordHeader+n]
}

// copyRecord appends the record of hash to dst and reports whether hash exists.
func (b *txInpointsBucket) copyRecord(dst []byte, hash chainhash.Hash) ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	h, ok := b.handle(hash)
	if !ok {
		return dst, false
	}

	return append(dst, b.record(h)...), true
}

// alloc reserves size bytes in the arena and returns their handle and the slice
// to write them to; the caller holds the bucket lock.
func (b *txInpointsBucket) alloc(size int) (uint64, []byte) {
	if len(b.chunks) == 0 || len(b.chunks[b.cur])+size > cap(b.chunks[b.cur]) {
		next := max(txInpointsFirstChunk, b.hint)

		if len(b.chunks) > 0 {
			next = min(2*cap(b.chunks[b.cur]), txInpointsMaxChunk)
		}

		b.chunks = append(b.chunks, make([]byte, 0, max(next, size)))
		b.cur = len(b.chunks) - 1
	}

	off := len(b.chunks[b.cur])
	b.chunks[b.cur] = b.chunks[b.cur][:off+size]

	return uint64(b.cur)<<32 | uint64(off), b.chunks[b.cur][off : off+size]
}

// putRecord copies a complete record into the arena and indexes it; the caller
// holds the bucket lock.
func (b *txInpointsBucket) putRecord(hash chainhash.Hash, rec []byte) {
	h, dst := b.alloc(len(rec))
	copy(dst, rec)
	b.index().Put(hash, h)
}

// put encodes inpoints into the arena and indexes them under hash; the caller
// holds the bucket lock.
func (b *txInpointsBucket) put(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints) {
	payload, flags := payloadOf(inpoints)

	if testHookAfterSizing != nil {
		testHookAfterSizing(inpoints)
	}

	h, rec := b.alloc(txInpointsRecordHeader + payload)
	writeRecord(rec, inpoints, flags)
	b.index().Put(hash, h)
}

func (b *txInpointsBucket) index() *swiss.Map[chainhash.Hash, uint64] {
	if b.m == nil {
		b.m = swiss.NewMap[chainhash.Hash, uint64](txInpointsIndexHint)
	}

	return b.m
}

// testHookAfterSizing, when set by a test, runs between sizing a record and
// writing it, to change the inpoints the way a racing caller would.
var testHookAfterSizing func(*subtreepkg.TxInpoints)

// payloadOf returns the payload length and the flags of inpoints.
//
// The payload is what TxInpoints.Serialize returns, byte for byte, sized by
// SerializedSize. If Serialize refuses the inpoints, the record keeps only the
// parent hashes and says so.
func payloadOf(inpoints *subtreepkg.TxInpoints) (payload int, flags byte) {
	if inpoints == nil {
		return 0, recordNil
	}

	if inpoints.ParentTxHashes != nil {
		flags |= recordParentsSet
	}

	if size, err := inpoints.SerializedSize(); err == nil {
		return size, flags
	}

	// Serialize refuses these: keep the hashes and say so
	return 4 + 32*len(inpoints.ParentTxHashes), flags | recordNoVouts
}

// writeRecord writes the record of inpoints into rec, which is exactly as long
// as payloadOf said.
func writeRecord(rec []byte, inpoints *subtreepkg.TxInpoints, flags byte) {
	var subtreeIndex int16
	if inpoints != nil {
		subtreeIndex = inpoints.SubtreeIndex
	}

	binary.LittleEndian.PutUint16(rec, uint16(subtreeIndex)) //nolint:gosec // the same bits come back through int16
	rec[2] = flags
	binary.LittleEndian.PutUint32(rec[3:], uint32(len(rec)-txInpointsRecordHeader)) //nolint:gosec // bounded by the tx size

	w := rec[txInpointsRecordHeader:]

	switch {
	case flags&recordNil != 0:
		return
	case flags&recordNoVouts != 0:
		binary.LittleEndian.PutUint32(w, uint32(len(inpoints.ParentTxHashes))) //nolint:gosec // bounded by the tx size
		w = w[4:]

		for i := range inpoints.ParentTxHashes {
			w = w[copy(w, inpoints.ParentTxHashes[i][:]):]
		}

		return
	}

	// w is exactly SerializedSize long and capped there, so this fills it in place
	// and never writes past the record. Anything else means inpoints changed since
	// payloadOf sized it (a caller race): fail loudly rather than keep a record
	// that is not what Serialize returns.
	out, err := inpoints.AppendSerialized(w[:0:len(w)])
	if err != nil || len(out) != len(w) || (len(w) > 0 && &out[0] != &w[0]) {
		panic("SplitTxInpointsMap: inpoints modified while being stored")
	}
}

// decodeRecord builds the TxInpoints a record holds. Nothing in the result aliases rec.
func decodeRecord(rec []byte) *subtreepkg.TxInpoints {
	flags := rec[2]
	if flags&recordNil != 0 {
		return nil
	}

	subtreeIndex := int16(binary.LittleEndian.Uint16(rec)) //nolint:gosec // the bits written by writeRecord
	payload := rec[txInpointsRecordHeader:]

	parentCount := int(binary.LittleEndian.Uint32(payload))
	if parentCount == 0 {
		p := &subtreepkg.TxInpoints{SubtreeIndex: subtreeIndex}
		if flags&recordParentsSet != 0 {
			p.ParentTxHashes = []chainhash.Hash{}
		}

		return p
	}

	parents := make([]chainhash.Hash, parentCount)
	payload = payload[4:]

	for i := range parents {
		payload = payload[copy(parents[i][:], payload):]
	}

	if flags&recordNoVouts != 0 {
		return &subtreepkg.TxInpoints{ParentTxHashes: parents, SubtreeIndex: subtreeIndex}
	}

	// the rest is the packed layout of TxInpoints: per parent a count word and the vouts
	vouts := make([]uint32, len(payload)/4)
	for i := range vouts {
		vouts[i] = binary.LittleEndian.Uint32(payload[4*i:])
	}

	p := subtreepkg.NewTxInpointsFromPacked(parents, vouts)
	p.SubtreeIndex = subtreeIndex

	return &p
}

func (s *SplitTxInpointsMap) Delete(hash chainhash.Hash) bool {
	b := s.bucket(hash)

	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.has(hash) {
		return false
	}

	b.m.Delete(hash)

	return true
}

func (s *SplitTxInpointsMap) Exists(hash chainhash.Hash) bool {
	b := s.bucket(hash)

	b.mu.Lock()
	defer b.mu.Unlock()

	return b.has(hash)
}

// Get returns a copy of the inpoints stored under hash; nothing in it aliases the map.
func (s *SplitTxInpointsMap) Get(hash chainhash.Hash) (*subtreepkg.TxInpoints, bool) {
	var scratch [txInpointsScratch]byte

	rec, ok := s.bucket(hash).copyRecord(scratch[:0], hash)
	if !ok {
		return nil, false
	}

	return decodeRecord(rec), true
}

// AppendSerialized appends the inpoints of hash to dst in TxInpoints.Serialize's
// wire format, without decoding them. found reports whether hash is in the map;
// err is what Serialize would have returned for the stored inpoints (they cannot
// be serialized, or are nil), in which case dst is returned unchanged.
func (s *SplitTxInpointsMap) AppendSerialized(dst []byte, hash chainhash.Hash) (out []byte, found bool, err error) {
	return s.appendPayload(dst, hash, false)
}

// AppendMetaInpoints is AppendSerialized with the check subtreepkg.Meta.Serialize
// makes on every node but the first: inpoints whose parent hashes were never set
// (nil, as opposed to empty) are refused with subtreepkg.ErrCannotSerializeSubtreeNotSet.
func (s *SplitTxInpointsMap) AppendMetaInpoints(dst []byte, hash chainhash.Hash) (out []byte, found bool, err error) {
	return s.appendPayload(dst, hash, true)
}

func (s *SplitTxInpointsMap) appendPayload(dst []byte, hash chainhash.Hash, requireParents bool) ([]byte, bool, error) {
	b := s.bucket(hash)

	b.mu.Lock()
	defer b.mu.Unlock()

	h, ok := b.handle(hash)
	if !ok {
		return dst, false, nil
	}

	rec := b.record(h)

	switch flags := rec[2]; {
	case flags&recordNil != 0:
		return dst, true, errNilInpoints
	case flags&recordNoVouts != 0:
		return dst, true, subtreepkg.ErrParentTxHashesMismatch
	case requireParents && flags&recordParentsSet == 0:
		return dst, true, subtreepkg.ErrCannotSerializeSubtreeNotSet
	}

	return append(dst, rec[txInpointsRecordHeader:]...), true, nil
}

func (s *SplitTxInpointsMap) Length() int {
	length := 0

	for i := range s.buckets {
		b := &s.buckets[i]

		b.mu.Lock()

		if b.m != nil {
			length += b.m.Count()
		}

		b.mu.Unlock()
	}

	return length
}

func (s *SplitTxInpointsMap) Set(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints) {
	b := s.bucket(hash)

	b.mu.Lock()
	defer b.mu.Unlock() // put can panic (writeRecord), and block assembly recovers

	b.put(hash, inpoints)
}

func (s *SplitTxInpointsMap) SetIfNotExists(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints) (*subtreepkg.TxInpoints, bool) {
	b := s.bucket(hash)

	var scratch [txInpointsScratch]byte

	rec, inserted := func() ([]byte, bool) {
		b.mu.Lock()
		defer b.mu.Unlock() // put can panic (writeRecord), and block assembly recovers

		if h, ok := b.handle(hash); ok {
			return append(scratch[:0], b.record(h)...), false
		}

		b.put(hash, inpoints)

		return nil, true
	}()

	if inserted {
		return inpoints, true
	}

	// decoded outside the lock
	return decodeRecord(rec), false
}

// MoveTo copies the record of hash into dst, as it is and without decoding it,
// unless dst already has the hash. found reports whether s has hash, moved
// whether dst took it. s keeps its record.
//
// The source lock is released before the destination lock is taken, so two
// maps that move into each other cannot deadlock.
func (s *SplitTxInpointsMap) MoveTo(dst *SplitTxInpointsMap, hash chainhash.Hash) (moved, found bool) {
	var scratch [txInpointsScratch]byte

	rec, found := s.bucket(hash).copyRecord(scratch[:0], hash)
	if !found {
		return false, false
	}

	return dst.putRecordIfNotExists(hash, rec), true
}

func (s *SplitTxInpointsMap) putRecordIfNotExists(hash chainhash.Hash, rec []byte) bool {
	b := s.bucket(hash)

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.has(hash) {
		return false
	}

	b.putRecord(hash, rec)

	return true
}

// MoveBucketFrom is MoveTo for hashes that all belong to the bucket of s named
// by bucket, as the caller partitioned them with BucketFor: it moves them from
// src in order and sets inserted[i] to whether s took hashes[i]. It stops at the
// first hash src does not have and returns its position, or -1 when src had
// them all; the hashes before it have been moved.
//
// On a missing hash s is left holding some of the bucket, and inserted is only
// filled up to that position: the contents of s are undefined after an error and
// the caller must drop s. The processor does: a failed moveForwardBlock goes
// through swapCurrentTxMapBack, which clears the half that was being written
// before it becomes the retired half again.
func (s *SplitTxInpointsMap) MoveBucketFrom(src *SplitTxInpointsMap, bucket uint16, hashes []chainhash.Hash, inserted []bool) int {
	var scratch [txInpointsScratch]byte

	dst := &s.buckets[bucket]

	for i, hash := range hashes {
		rec, found := src.bucket(hash).copyRecord(scratch[:0], hash)
		if !found {
			return i
		}

		dst.mu.Lock()

		if dst.has(hash) {
			inserted[i] = false
		} else {
			dst.putRecord(hash, rec)
			inserted[i] = true
		}

		dst.mu.Unlock()
	}

	return -1
}

// Clear empties every bucket, keeping the index capacity (the swiss maps hold no
// pointers to records, and re-growing them every block is what the capacity
// saves) but releasing the arena chunks. Clear retires a half of the
// currentTxMap double buffer, and a retired half that kept its chunks held a
// whole block's records next to the current half's: about twice the map at
// steady state. Chunks hold no pointers, so the GC frees them without scanning.
// Each bucket keeps only how many bytes it used, to size the next fill's first
// chunk (see txInpointsBucket.hint).
func (s *SplitTxInpointsMap) Clear() {
	for i := range s.buckets {
		b := &s.buckets[i]

		b.mu.Lock()

		if b.m != nil {
			b.m.Clear()
		}

		b.hint = 0
		for _, c := range b.chunks {
			b.hint += len(c)
		}

		b.chunks = nil
		b.cur = 0
		b.mu.Unlock()
	}
}

// usedBytes is the size of the records in the arena, live or not.
func (s *SplitTxInpointsMap) usedBytes() int {
	n := 0

	for i := range s.buckets {
		b := &s.buckets[i]

		b.mu.Lock()

		for _, c := range b.chunks {
			n += len(c)
		}

		b.mu.Unlock()
	}

	return n
}

// retainedBytes is the capacity of the chunks the map holds.
func (s *SplitTxInpointsMap) retainedBytes() int {
	n := 0

	for i := range s.buckets {
		b := &s.buckets[i]

		b.mu.Lock()

		for _, c := range b.chunks {
			n += cap(c)
		}

		b.mu.Unlock()
	}

	return n
}

// ParallelBulkSetIfNotExists inserts multiple entries in parallel, grouped by bucket.
// Each bucket is processed by a separate goroutine with a single lock acquisition.
// wasSet[i] is set to true if hashes[i] was newly inserted (not already present).
func (s *SplitTxInpointsMap) ParallelBulkSetIfNotExists(
	hashes []chainhash.Hash,
	inpoints []*subtreepkg.TxInpoints,
	wasSet []bool,
) {
	n := len(hashes)
	if n == 0 {
		return
	}

	if len(inpoints) != n {
		panic("SplitTxInpointsMap.ParallelBulkSetIfNotExists: len(inpoints) must equal len(hashes)")
	}
	if wasSet == nil || len(wasSet) != n {
		panic("SplitTxInpointsMap.ParallelBulkSetIfNotExists: len(wasSet) must equal len(hashes)")
	}

	// Phase 1: Group indices by bucket (O(N), no locks)
	bucketIndices := make([][]int, s.nrOfBuckets)
	for i := 0; i < n; i++ {
		bucket := txmap.Bytes2Uint16Buckets(hashes[i], s.nrOfBuckets)
		bucketIndices[bucket] = append(bucketIndices[bucket], i)
	}

	// Phase 2: Process buckets with a bounded pool of stride workers. Spawning
	// one goroutine per non-empty bucket would create up to nrOfBuckets (default
	// 16*1024) goroutines for a large bulk insert, where scheduler churn and
	// memory spikes can dominate. Cap fan-out at GOMAXPROCS (matching
	// bulkBuildSubtrees); each worker strides over buckets.
	numWorkers := runtime.GOMAXPROCS(0)
	if numWorkers > int(s.nrOfBuckets) {
		numWorkers = int(s.nrOfBuckets)
	}
	if numWorkers < 1 {
		numWorkers = 1
	}

	processBucket := func(bIdx uint16) {
		indices := bucketIndices[bIdx]
		if len(indices) == 0 {
			return
		}
		b := &s.buckets[bIdx]
		b.mu.Lock()
		defer b.mu.Unlock()
		for _, idx := range indices {
			if !b.has(hashes[idx]) {
				b.put(hashes[idx], inpoints[idx])
				wasSet[idx] = true
			} else {
				// Explicitly record the existing-key result so the contract
				// (wasSet[i] reflects the outcome for hashes[i]) holds even if
				// the caller reuses a slice with stale true values.
				wasSet[idx] = false
			}
		}
	}

	if numWorkers == 1 {
		for bIdx := uint16(0); bIdx < s.nrOfBuckets; bIdx++ {
			processBucket(bIdx)
		}
		return
	}

	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for w := 0; w < numWorkers; w++ {
		start := uint16(w)
		go func() {
			defer wg.Done()
			for bIdx := start; bIdx < s.nrOfBuckets; bIdx += uint16(numWorkers) {
				processBucket(bIdx)
			}
		}()
	}
	wg.Wait()
}

// Buckets returns the configured bucket count.
func (s *SplitTxInpointsMap) Buckets() uint16 {
	return s.nrOfBuckets
}

// BucketFor returns the destination bucket index for a given hash, matching the
// scheme used by Get/Set/SetIfNotExists.
func (s *SplitTxInpointsMap) BucketFor(hash chainhash.Hash) uint16 {
	return txmap.Bytes2Uint16Buckets(hash, s.nrOfBuckets)
}

// PutMultiBucketTxInpoints inserts a batch of (hash, inpoints) pairs that all
// belong to the same bucket. Takes the per-bucket lock exactly once for the
// whole batch instead of once per entry.
//
// All entries MUST belong to the named bucket; callers are expected to have
// partitioned by BucketFor beforehand. The returned slice has length
// min(len(keys), len(values)); wasInserted[i] is true if keys[i] was newly
// added, false if it already existed.
func (s *SplitTxInpointsMap) PutMultiBucketTxInpoints(bucket uint16, keys []chainhash.Hash, values []*subtreepkg.TxInpoints) []bool {
	n := len(keys)
	if len(values) < n {
		n = len(values)
	}
	wasInserted := make([]bool, n)
	if n == 0 {
		return wasInserted
	}
	b := &s.buckets[bucket]
	b.mu.Lock()
	defer b.mu.Unlock() // put can panic (writeRecord), and block assembly recovers
	for i := 0; i < n; i++ {
		if !b.has(keys[i]) {
			b.put(keys[i], values[i])
			wasInserted[i] = true
		}
	}
	return wasInserted
}
