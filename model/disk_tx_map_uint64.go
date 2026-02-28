package model

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/tempstore"
	cuckoo "github.com/seiflotfy/cuckoofilter"
)

const (
	dtmDefaultFilterCapacity = 10_000_000
	dtmNumFilterShards       = 4096
	dtmWriteChBuffer         = 1_000_000
	dtmWriterFlushThreshold  = 50_000
)

// Compile-time check that DiskTxMapUint64 implements txmap.TxMap.
var _ txmap.TxMap = (*DiskTxMapUint64)(nil)

// dtmFilterShard is one of 4096 independent existence-check segments.
type dtmFilterShard struct {
	mu     sync.Mutex
	slowMu sync.Mutex // serializes slow-path (disk check) to prevent clearRecent race
	filter *cuckoo.Filter
	recent map[chainhash.Hash]struct{}
}

// dtmWriteEntry is a pending Badger write sent via channel.
type dtmWriteEntry struct {
	key       chainhash.Hash
	value     uint64
	flushDone chan struct{} // non-nil = flush request
}

// dtmDiskShard is one Badger instance on a single disk, with its own writer goroutine.
type dtmDiskShard struct {
	store        *tempstore.BadgerTempStore
	batch        *tempstore.WriteBatch
	writeCh      chan dtmWriteEntry
	done         chan struct{}
	path         string
	prefix       string
	bytesWritten int64 // only touched by the single writer goroutine — no atomic needed
}

// DiskTxMapUint64 implements txmap.TxMap using sharded cuckoo filters for fast
// in-memory existence checks and multi-disk BadgerDB for on-disk uint64 storage.
//
// Architecture (designed for 1B+ entries during block validation):
//   - 4096 independent cuckoo filter shards (~1 GB total at 1B entries)
//   - Hot path: per-shard lock + filter check + map insert + channel send
//   - N Badger instances across N physical disks, each with a dedicated writer goroutine
//   - Write throughput scales linearly with disk count
type DiskTxMapUint64 struct {
	shards         [dtmNumFilterShards]dtmFilterShard
	disks          []dtmDiskShard
	numDisks       int
	count          atomic.Int64
	capacity       uint
	filterMemBytes int64 // total cuckoo filter memory, computed once at construction
}

// DiskTxMapUint64Options configures the DiskTxMapUint64.
type DiskTxMapUint64Options struct {
	// BasePaths is a list of directories for Badger storage, each ideally on a separate
	// physical disk for I/O parallelism.
	BasePaths      []string
	Prefix         string
	FilterCapacity uint
}

// NewDiskTxMapUint64 creates a new DiskTxMapUint64 with N Badger disk shards.
func NewDiskTxMapUint64(opts DiskTxMapUint64Options) (*DiskTxMapUint64, error) {
	capacity := opts.FilterCapacity
	if capacity == 0 {
		capacity = dtmDefaultFilterCapacity
	}

	prefix := opts.Prefix
	if prefix == "" {
		prefix = "disktxmap-u64"
	}

	paths := opts.BasePaths
	if len(paths) == 0 {
		return nil, errors.NewProcessingError("DiskTxMapUint64: at least one base path is required")
	}

	m := &DiskTxMapUint64{
		numDisks: len(paths),
		capacity: capacity,
		disks:    make([]dtmDiskShard, len(paths)),
	}

	for i, path := range paths {
		store, err := tempstore.New(tempstore.Options{
			BasePath:   path,
			Prefix:     fmt.Sprintf("%s-disk%d", prefix, i),
			SyncWrites: false,
		})
		if err != nil {
			for j := 0; j < i; j++ {
				_ = m.disks[j].store.Close()
			}
			return nil, errors.NewServiceError("DiskTxMapUint64: failed to create badger store for disk %d (%s)", i, path, err)
		}

		m.disks[i] = dtmDiskShard{
			store:   store,
			batch:   store.NewWriteBatch(),
			writeCh: make(chan dtmWriteEntry, dtmWriteChBuffer/len(paths)),
			done:    make(chan struct{}),
			path:    path,
			prefix:  fmt.Sprintf("%s-disk%d", prefix, i),
		}
	}

	perShard := capacity / dtmNumFilterShards
	if perShard < 1024 {
		perShard = 1024
	}
	for i := range m.shards {
		m.shards[i].filter = cuckoo.NewFilter(uint(perShard))
		m.shards[i].recent = make(map[chainhash.Hash]struct{}, 64)
	}

	m.filterMemBytes = int64(dtmNumFilterShards) * int64(getNextPow2(uint(perShard)))

	for i := range m.disks {
		go m.writerLoop(i)
	}

	return m, nil
}

// writerLoop is the dedicated goroutine for a single disk shard.
func (m *DiskTxMapUint64) writerLoop(diskIdx int) {
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

		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], entry.value)
		_ = d.batch.Set(entry.key[:], buf[:])
		d.bytesWritten += int64(chainhash.HashSize + 8)
		pending++

		if pending >= dtmWriterFlushThreshold {
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

// shardOf returns the filter shard index for a hash.
func dtmShardOf(hash chainhash.Hash) uint16 {
	return binary.LittleEndian.Uint16(hash[:2]) % dtmNumFilterShards
}

// diskOf returns the disk shard index for a hash.
func (m *DiskTxMapUint64) diskOf(hash chainhash.Hash) int {
	return int(dtmShardOf(hash)) % m.numDisks
}

// Put adds a new hash with an associated uint64 value to the map.
// Returns an error if the hash already exists (duplicate detection).
func (m *DiskTxMapUint64) Put(hash chainhash.Hash, value uint64) error {
	s := &m.shards[dtmShardOf(hash)]

	s.mu.Lock()

	if s.filter.Lookup(hash[:]) {
		if _, inRecent := s.recent[hash]; inRecent {
			s.mu.Unlock()
			return txmap.ErrHashAlreadyExists
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
			return txmap.ErrHashAlreadyExists
		}
		s.mu.Unlock()

		if m.existsInStore(hash) {
			s.slowMu.Unlock()
			return txmap.ErrHashAlreadyExists
		}

		s.mu.Lock()
		if _, inRecent := s.recent[hash]; inRecent {
			s.mu.Unlock()
			s.slowMu.Unlock()
			return txmap.ErrHashAlreadyExists
		}

		s.filter.Insert(hash[:])
		s.recent[hash] = struct{}{}
		s.mu.Unlock()

		m.disks[m.diskOf(hash)].writeCh <- dtmWriteEntry{key: hash, value: value}
		m.count.Add(1)

		s.slowMu.Unlock()
		return nil
	}

	// Fast path: filter negative (new entry)
	s.filter.Insert(hash[:])
	s.recent[hash] = struct{}{}
	s.mu.Unlock()

	m.disks[m.diskOf(hash)].writeCh <- dtmWriteEntry{key: hash, value: value}
	m.count.Add(1)

	return nil
}

// Get retrieves the uint64 value associated with the given hash.
// Call Flush() before reading to ensure all writes are visible (block validation
// calls Flush() once between the write phase and the read phase).
//
// This goes straight to disk without checking the cuckoo filter. The filter is
// optimized for absence checks (Put duplicate detection), but Get is called in
// Phase 2 where the common case is presence (~94%+ of lookups hit). The filter
// would add lock+lookup overhead on every call while almost never avoiding a
// disk read. BadgerDB's own SST bloom filters handle the "key not found" case
// efficiently enough.
func (m *DiskTxMapUint64) Get(hash chainhash.Hash) (uint64, bool) {
	diskIdx := m.diskOf(hash)
	val, err := m.disks[diskIdx].store.Get(hash[:])
	if err != nil || val == nil {
		return 0, false
	}

	if len(val) < 8 {
		return 0, false
	}

	return binary.LittleEndian.Uint64(val), true
}

// Exists returns true if the hash has been seen.
func (m *DiskTxMapUint64) Exists(hash chainhash.Hash) bool {
	s := &m.shards[dtmShardOf(hash)]
	s.mu.Lock()
	exists := s.filter.Lookup(hash[:])
	s.mu.Unlock()
	return exists
}

// Length returns the number of entries.
func (m *DiskTxMapUint64) Length() int {
	return int(m.count.Load())
}

// Flush persists all pending writes across all disk shards.
func (m *DiskTxMapUint64) Flush() error {
	m.flushAllDisks()
	return nil
}

// Close releases all resources across all disk shards.
func (m *DiskTxMapUint64) Close() error {
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

// flushDisk sends a flush request to a single disk shard and waits for completion.
func (m *DiskTxMapUint64) flushDisk(diskIdx int) {
	done := make(chan struct{})
	m.disks[diskIdx].writeCh <- dtmWriteEntry{flushDone: done}
	<-done
}

// flushAllDisks flushes all disk shards in parallel.
func (m *DiskTxMapUint64) flushAllDisks() {
	dones := make([]chan struct{}, m.numDisks)
	for i := range m.disks {
		dones[i] = make(chan struct{})
		m.disks[i].writeCh <- dtmWriteEntry{flushDone: dones[i]}
	}
	for _, done := range dones {
		<-done
	}
}

// clearRecentMapsForDisk clears recent maps for filter shards mapped to a specific disk.
func (m *DiskTxMapUint64) clearRecentMapsForDisk(diskIdx int) {
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

// existsInStore checks if a hash exists in the Badger store for its disk shard.
func (m *DiskTxMapUint64) existsInStore(hash chainhash.Hash) bool {
	diskIdx := m.diskOf(hash)
	m.flushDisk(diskIdx)

	val, err := m.disks[diskIdx].store.Get(hash[:])
	return err == nil && val != nil
}

// --- Unused interface methods (block validation only uses Put, Get, Exists, Length) ---

// Delete is not used during block validation.
func (m *DiskTxMapUint64) Delete(_ chainhash.Hash) error {
	return errors.NewProcessingError("DiskTxMapUint64: Delete not supported")
}

// Keys is not used during block validation.
func (m *DiskTxMapUint64) Keys() []chainhash.Hash {
	return nil
}

// Set is not used during block validation.
func (m *DiskTxMapUint64) Set(_ chainhash.Hash, _ uint64) error {
	return errors.NewProcessingError("DiskTxMapUint64: Set not supported")
}

// SetIfExists is not used during block validation.
func (m *DiskTxMapUint64) SetIfExists(_ chainhash.Hash, _ uint64) (bool, error) {
	return false, errors.NewProcessingError("DiskTxMapUint64: SetIfExists not supported")
}

// SetIfNotExists is not used during block validation.
func (m *DiskTxMapUint64) SetIfNotExists(_ chainhash.Hash, _ uint64) (bool, error) {
	return false, errors.NewProcessingError("DiskTxMapUint64: SetIfNotExists not supported")
}

// PutMulti is not used during block validation.
func (m *DiskTxMapUint64) PutMulti(_ []chainhash.Hash, _ uint64) error {
	return errors.NewProcessingError("DiskTxMapUint64: PutMulti not supported")
}

// Iter is not used during block validation.
func (m *DiskTxMapUint64) Iter(_ func(hash chainhash.Hash, value uint64) bool) {
}

// DiskMapStats holds lightweight metrics for a disk-backed map.
type DiskMapStats struct {
	Entries          int64
	FilterMemBytes   int64
	DiskBytesWritten int64
}

// Stats returns current metrics. Safe to call after Flush() when writer goroutines are idle.
func (m *DiskTxMapUint64) Stats() DiskMapStats {
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

// getNextPow2 mirrors the cuckoo filter library's unexported bucket allocation function.
func getNextPow2(n uint) uint {
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
