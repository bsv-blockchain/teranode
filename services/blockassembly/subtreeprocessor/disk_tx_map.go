package subtreeprocessor

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/tempstore"
	cuckoo "github.com/seiflotfy/cuckoofilter"
)

const (
	defaultFilterCapacity = 10_000_000
	numFilterShards       = 4096
	writeChBuffer         = 1_000_000
	writerFlushThreshold  = 50_000
)

// filterShard is one of 4096 independent existence-check segments.
type filterShard struct {
	mu     sync.Mutex
	slowMu sync.Mutex // serializes slow-path (disk check) to prevent clearRecent race
	filter *cuckoo.Filter
	recent map[chainhash.Hash]struct{}
}

// writeEntry is a pending Badger write sent via channel.
type writeEntry struct {
	key       chainhash.Hash
	inpoints  *subtreepkg.TxInpoints
	flushDone chan struct{} // non-nil = flush request
}

// diskShard is one Badger instance on a single disk, with its own writer goroutine.
// Multiple disk shards across physical disks give linear I/O scaling.
type diskShard struct {
	store        *tempstore.BadgerTempStore
	batch        *tempstore.WriteBatch
	writeCh      chan writeEntry
	done         chan struct{}
	path         string
	prefix       string
	bytesWritten int64 // only touched by the single writer goroutine — no atomic needed
}

// DiskTxMap implements TxInpointsMap using sharded cuckoo filters for fast
// in-memory existence checks and multi-disk BadgerDB for on-disk TxInpoints storage.
//
// Architecture (designed for 10M+ TPS):
//   - 4096 independent cuckoo filter shards (~1 GB total at 1B entries)
//   - Hot path: per-shard lock + filter check + map insert + channel send = ~100-200ns
//   - N Badger instances across N physical disks, each with a dedicated writer goroutine
//   - Write throughput scales linearly with disk count (~500K/s per disk)
//   - TxInpoints read only during rare ops (reorg, removeTx)
type DiskTxMap struct {
	shards         [numFilterShards]filterShard
	disks          []diskShard
	numDisks       int
	count          atomic.Int64
	basePaths      []string
	prefix         string
	capacity       uint
	filterMemBytes int64 // total cuckoo filter memory, computed once at construction
}

// DiskTxMapOptions configures the DiskTxMap.
type DiskTxMapOptions struct {
	// BasePaths is a list of directories for Badger storage, each ideally on a separate
	// physical disk for I/O parallelism. If empty, BasePath is used as a single disk.
	BasePaths []string
	// BasePath is used when BasePaths is empty (single-disk mode).
	BasePath       string
	Prefix         string
	FilterCapacity uint
}

// NewDiskTxMap creates a new DiskTxMap with N Badger disk shards.
func NewDiskTxMap(opts DiskTxMapOptions) (*DiskTxMap, error) {
	capacity := opts.FilterCapacity
	if capacity == 0 {
		capacity = defaultFilterCapacity
	}

	prefix := opts.Prefix
	if prefix == "" {
		prefix = "disktxmap"
	}

	// Resolve disk paths
	paths := opts.BasePaths
	if len(paths) == 0 {
		paths = []string{opts.BasePath}
	}

	m := &DiskTxMap{
		numDisks:  len(paths),
		basePaths: paths,
		prefix:    prefix,
		capacity:  capacity,
		disks:     make([]diskShard, len(paths)),
	}

	// Create Badger stores — one per disk path
	for i, path := range paths {
		store, err := tempstore.New(tempstore.Options{
			BasePath:   path,
			Prefix:     fmt.Sprintf("%s-disk%d", prefix, i),
			SyncWrites: false,
		})
		if err != nil {
			// Clean up already-created stores
			for j := 0; j < i; j++ {
				_ = m.disks[j].store.Close()
			}
			return nil, errors.NewServiceError("failed to create badger store for disk %d (%s)", i, path, err)
		}

		m.disks[i] = diskShard{
			store:   store,
			batch:   store.NewWriteBatch(),
			writeCh: make(chan writeEntry, writeChBuffer/len(paths)),
			done:    make(chan struct{}),
			path:    path,
			prefix:  fmt.Sprintf("%s-disk%d", prefix, i),
		}
	}

	// Initialize filter shards
	perShard := capacity / numFilterShards
	if perShard < 1024 {
		perShard = 1024
	}
	for i := range m.shards {
		m.shards[i].filter = cuckoo.NewFilter(uint(perShard))
		m.shards[i].recent = make(map[chainhash.Hash]struct{}, 64)
	}

	m.filterMemBytes = int64(numFilterShards) * int64(dtmGetNextPow2(uint(perShard)))

	// Start one writer goroutine per disk
	for i := range m.disks {
		go m.writerLoop(i)
	}

	return m, nil
}

// writerLoop is the dedicated goroutine for a single disk shard.
func (m *DiskTxMap) writerLoop(diskIdx int) {
	d := &m.disks[diskIdx]
	pending := 0
	for entry := range d.writeCh {
		if entry.flushDone != nil {
			if pending > 0 {
				_ = d.batch.Flush()
				m.clearRecentMapsForDisk(diskIdx)
				pending = 0
			}
			close(entry.flushDone)
			continue
		}

		value := serializeTxMapValue(entry.inpoints)
		_ = d.batch.Set(entry.key[:], value)
		d.bytesWritten += int64(chainhash.HashSize + len(value))
		pending++

		if pending >= writerFlushThreshold {
			_ = d.batch.Flush()
			m.clearRecentMapsForDisk(diskIdx)
			pending = 0
		}
	}

	if pending > 0 {
		_ = d.batch.Flush()
		m.clearRecentMapsForDisk(diskIdx)
	}
	close(d.done)
}

// diskOf returns the disk shard index for a hash.
func (m *DiskTxMap) diskOf(hash chainhash.Hash) int {
	return int(shardOf(hash)) % m.numDisks
}

// SetIfNotExists atomically checks if hash exists and inserts it if not.
func (m *DiskTxMap) SetIfNotExists(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints) (*subtreepkg.TxInpoints, bool) {
	s := &m.shards[shardOf(hash)]

	s.mu.Lock()

	if s.filter.Lookup(hash[:]) {
		if _, inRecent := s.recent[hash]; inRecent {
			s.mu.Unlock()
			return nil, false
		}

		s.mu.Unlock()

		// Serialize slow path: prevents clearRecentMapsForDisk from clearing
		// an entry between another goroutine's insert and our re-check.
		// Held through channel send so the next slow-path entrant's flushDisk
		// is guaranteed to find our entry on disk (FIFO channel ordering).
		s.slowMu.Lock()

		s.mu.Lock()
		if _, inRecent := s.recent[hash]; inRecent {
			s.mu.Unlock()
			s.slowMu.Unlock()
			return nil, false
		}
		s.mu.Unlock()

		existing := m.getFromStore(hash)
		if existing != nil {
			s.slowMu.Unlock()
			return existing, false
		}

		s.mu.Lock()
		if _, inRecent := s.recent[hash]; inRecent {
			s.mu.Unlock()
			s.slowMu.Unlock()
			return nil, false
		}

		s.filter.Insert(hash[:])
		s.recent[hash] = struct{}{}
		s.mu.Unlock()

		m.disks[m.diskOf(hash)].writeCh <- writeEntry{key: hash, inpoints: inpoints}
		m.count.Add(1)

		s.slowMu.Unlock()
		return nil, true
	}

	// Fast path: filter negative (new entry)
	s.filter.Insert(hash[:])
	s.recent[hash] = struct{}{}
	s.mu.Unlock()

	m.disks[m.diskOf(hash)].writeCh <- writeEntry{key: hash, inpoints: inpoints}
	m.count.Add(1)

	return nil, true
}

// Exists returns true if the hash has been seen.
func (m *DiskTxMap) Exists(hash chainhash.Hash) bool {
	s := &m.shards[shardOf(hash)]
	s.mu.Lock()
	exists := s.filter.Lookup(hash[:])
	s.mu.Unlock()
	return exists
}

// Get retrieves the TxInpoints for a hash from the correct disk shard.
//
// This goes straight to disk without checking the cuckoo filter. The filter is
// optimized for absence checks (SetIfNotExists duplicate detection), but Get is
// predominantly called for hashes that ARE in the map. The filter would add
// lock+lookup overhead on every call while almost never avoiding a disk read.
// BadgerDB's own SST bloom filters handle the "key not found" case efficiently.
func (m *DiskTxMap) Get(hash chainhash.Hash) (*subtreepkg.TxInpoints, bool) {
	inpoints := m.getFromStore(hash)
	if inpoints == nil {
		return nil, false
	}
	return inpoints, true
}

// Delete removes a hash from the filter, recent map, and the correct disk shard.
// Flushes the disk shard first to prevent a pending write from re-creating the entry.
func (m *DiskTxMap) Delete(hash chainhash.Hash) bool {
	s := &m.shards[shardOf(hash)]
	s.mu.Lock()
	s.filter.Delete(hash[:])
	delete(s.recent, hash)
	s.mu.Unlock()

	diskIdx := m.diskOf(hash)
	m.flushDisk(diskIdx)
	_ = m.disks[diskIdx].store.Delete(hash[:])
	m.count.Add(-1)
	return true
}

// Length returns the number of entries.
func (m *DiskTxMap) Length() int {
	return int(m.count.Load())
}

// Set stores inpoints for a hash, overwriting any existing entry.
func (m *DiskTxMap) Set(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints) {
	s := &m.shards[shardOf(hash)]
	s.mu.Lock()
	isNew := !s.filter.Lookup(hash[:])
	if isNew {
		s.filter.Insert(hash[:])
	}
	s.recent[hash] = struct{}{}
	s.mu.Unlock()

	if isNew {
		m.count.Add(1)
	}
	m.disks[m.diskOf(hash)].writeCh <- writeEntry{key: hash, inpoints: inpoints}
}

// Clear removes all entries and recreates filters and stores.
func (m *DiskTxMap) Clear() {
	m.flushAllDisks()

	perShard := m.capacity / numFilterShards
	if perShard < 1024 {
		perShard = 1024
	}
	for i := range m.shards {
		m.shards[i].mu.Lock()
		m.shards[i].filter = cuckoo.NewFilter(uint(perShard))
		m.shards[i].recent = make(map[chainhash.Hash]struct{}, 64)
		m.shards[i].mu.Unlock()
	}

	for i := range m.disks {
		d := &m.disks[i]
		d.batch.Cancel()
		oldStore := d.store

		store, err := tempstore.New(tempstore.Options{
			BasePath:   d.path,
			Prefix:     d.prefix,
			SyncWrites: false,
		})
		if err != nil {
			// Keep old store to avoid nil-pointer panics on subsequent operations
			d.batch = oldStore.NewWriteBatch()
			continue
		}

		_ = oldStore.Close()
		d.store = store
		d.batch = store.NewWriteBatch()
	}

	m.count.Store(0)
}

// Flush persists all pending writes across all disk shards.
func (m *DiskTxMap) Flush() error {
	m.flushAllDisks()
	return nil
}

// Close releases all resources across all disk shards.
func (m *DiskTxMap) Close() error {
	for i := range m.disks {
		close(m.disks[i].writeCh)
	}
	for i := range m.disks {
		<-m.disks[i].done
	}
	var lastErr error
	for i := range m.disks {
		if err := m.disks[i].store.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// UpdateSubtreeIndex updates the SubtreeIndex for a hash in the correct disk shard.
func (m *DiskTxMap) UpdateSubtreeIndex(hash chainhash.Hash, subtreeIndex int16) error {
	m.flushDisk(m.diskOf(hash))

	d := &m.disks[m.diskOf(hash)]
	val, err := d.store.Get(hash[:])
	if err != nil || val == nil {
		return errors.NewNotFoundError("entry not found for hash %s", hash.String())
	}

	if len(val) >= 2 {
		// Copy before modifying — BadgerDB returned bytes must not be mutated in-place
		valCopy := make([]byte, len(val))
		copy(valCopy, val)
		binary.LittleEndian.PutUint16(valCopy[:2], uint16(subtreeIndex))
		return d.store.Put(hash[:], valCopy)
	}

	return errors.NewProcessingError("value too short for hash %s", hash.String())
}

// flushDisk sends a flush request to a single disk shard and waits for completion.
func (m *DiskTxMap) flushDisk(diskIdx int) {
	done := make(chan struct{})
	m.disks[diskIdx].writeCh <- writeEntry{flushDone: done}
	<-done
}

// flushAllDisks flushes all disk shards in parallel.
func (m *DiskTxMap) flushAllDisks() {
	dones := make([]chan struct{}, m.numDisks)
	for i := range m.disks {
		dones[i] = make(chan struct{})
		m.disks[i].writeCh <- writeEntry{flushDone: dones[i]}
	}
	for _, done := range dones {
		<-done
	}
}

// clearRecentMapsForDisk clears recent maps for filter shards mapped to a specific disk.
func (m *DiskTxMap) clearRecentMapsForDisk(diskIdx int) {
	for i := range m.shards {
		if i%m.numDisks != diskIdx {
			continue
		}
		m.shards[i].mu.Lock()
		if len(m.shards[i].recent) > 0 {
			m.shards[i].recent = make(map[chainhash.Hash]struct{}, 64)
		}
		m.shards[i].mu.Unlock()
	}
}

// getFromStore retrieves and deserializes TxInpoints from the correct disk shard.
func (m *DiskTxMap) getFromStore(hash chainhash.Hash) *subtreepkg.TxInpoints {
	diskIdx := m.diskOf(hash)
	m.flushDisk(diskIdx)

	val, err := m.disks[diskIdx].store.Get(hash[:])
	if err != nil || val == nil {
		return nil
	}

	return deserializeTxMapValue(val)
}

// shardOf returns the filter shard index for a hash.
func shardOf(hash chainhash.Hash) uint16 {
	return binary.LittleEndian.Uint16(hash[:2]) % numFilterShards
}

// serializeTxMapValue encodes SubtreeIndex + TxInpoints into bytes.
func serializeTxMapValue(inpoints *subtreepkg.TxInpoints) []byte {
	serialized, err := inpoints.Serialize()
	if err != nil {
		buf := make([]byte, 2)
		binary.LittleEndian.PutUint16(buf, uint16(inpoints.SubtreeIndex))
		return buf
	}

	buf := make([]byte, 2+len(serialized))
	binary.LittleEndian.PutUint16(buf[:2], uint16(inpoints.SubtreeIndex))
	copy(buf[2:], serialized)

	return buf
}

// deserializeTxMapValue decodes SubtreeIndex + TxInpoints from bytes.
func deserializeTxMapValue(data []byte) *subtreepkg.TxInpoints {
	if len(data) < 2 {
		return nil
	}

	subtreeIndex := int16(binary.LittleEndian.Uint16(data[:2]))

	inpoints := &subtreepkg.TxInpoints{
		SubtreeIndex: subtreeIndex,
	}

	if len(data) > 2 {
		parsed, err := subtreepkg.NewTxInpointsFromBytes(data[2:])
		if err == nil {
			parsed.SubtreeIndex = subtreeIndex
			return &parsed
		}
	}

	return inpoints
}

// DiskMapStats holds lightweight metrics for a disk-backed map.
type DiskMapStats struct {
	Entries          int64
	FilterMemBytes   int64
	DiskBytesWritten int64
}

// Stats returns current metrics. Safe to call after Flush() when writer goroutines are idle.
func (m *DiskTxMap) Stats() DiskMapStats {
	var diskBytes int64
	for i := range m.disks {
		diskBytes += m.disks[i].bytesWritten
	}
	return DiskMapStats{
		Entries:          m.count.Load(),
		FilterMemBytes:   m.filterMemBytes,
		DiskBytesWritten: diskBytes,
	}
}

// dtmGetNextPow2 mirrors the cuckoo filter library's unexported bucket allocation function.
func dtmGetNextPow2(n uint) uint {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}
