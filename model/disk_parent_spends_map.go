package model

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
	dpsDefaultFilterCapacity = 10_000_000
	dpsNumFilterShards       = 4096
	dpsWriteChBuffer         = 1_000_000
	dpsWriterFlushThreshold  = 50_000
	dpsInpointKeySize        = chainhash.HashSize + 4 // 32-byte hash + 4-byte index
)

// Compile-time check that DiskParentSpendsMap implements ParentSpendsMap.
var _ ParentSpendsMap = (*DiskParentSpendsMap)(nil)

// dpsFilterShard is one of 4096 independent existence-check segments.
type dpsFilterShard struct {
	mu     sync.Mutex
	slowMu sync.Mutex // serializes slow-path (disk check) to prevent clearRecent race
	filter *cuckoo.Filter
	recent map[subtreepkg.Inpoint]struct{}
}

// dpsWriteEntry is a pending Badger write sent via channel.
type dpsWriteEntry struct {
	key       [dpsInpointKeySize]byte
	flushDone chan struct{} // non-nil = flush request
}

// dpsDiskShard is one Badger instance on a single disk, with its own writer goroutine.
type dpsDiskShard struct {
	store        *tempstore.BadgerTempStore
	batch        *tempstore.WriteBatch
	writeCh      chan dpsWriteEntry
	done         chan struct{}
	path         string
	prefix       string
	bytesWritten int64 // only touched by the single writer goroutine — no atomic needed
}

// DiskParentSpendsMap tracks spent inpoints using sharded cuckoo filters for fast
// in-memory existence checks and multi-disk BadgerDB for authoritative lookups.
//
// Architecture (designed for 3B+ inpoints during block validation):
//   - 4096 independent cuckoo filter shards
//   - N Badger instances across N physical disks, each with a dedicated writer goroutine
//   - Write throughput scales linearly with disk count
type DiskParentSpendsMap struct {
	shards         [dpsNumFilterShards]dpsFilterShard
	disks          []dpsDiskShard
	numDisks       int
	count          atomic.Int64
	capacity       uint
	filterMemBytes int64 // total cuckoo filter memory, computed once at construction
}

// DiskParentSpendsMapOptions configures the DiskParentSpendsMap.
type DiskParentSpendsMapOptions struct {
	// BasePaths is a list of directories for Badger storage, each ideally on a separate
	// physical disk for I/O parallelism.
	BasePaths      []string
	Prefix         string
	FilterCapacity uint
}

// NewDiskParentSpendsMap creates a new DiskParentSpendsMap with N Badger disk shards.
func NewDiskParentSpendsMap(opts DiskParentSpendsMapOptions) (*DiskParentSpendsMap, error) {
	capacity := opts.FilterCapacity
	if capacity == 0 {
		capacity = dpsDefaultFilterCapacity
	}

	prefix := opts.Prefix
	if prefix == "" {
		prefix = "disk-parentspends"
	}

	paths := opts.BasePaths
	if len(paths) == 0 {
		return nil, errors.NewProcessingError("DiskParentSpendsMap: at least one base path is required")
	}

	m := &DiskParentSpendsMap{
		numDisks: len(paths),
		capacity: capacity,
		disks:    make([]dpsDiskShard, len(paths)),
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
			return nil, errors.NewServiceError("DiskParentSpendsMap: failed to create badger store for disk %d (%s)", i, path, err)
		}

		m.disks[i] = dpsDiskShard{
			store:   store,
			batch:   store.NewWriteBatch(),
			writeCh: make(chan dpsWriteEntry, dpsWriteChBuffer/len(paths)),
			done:    make(chan struct{}),
			path:    path,
			prefix:  fmt.Sprintf("%s-disk%d", prefix, i),
		}
	}

	perShard := capacity / dpsNumFilterShards
	if perShard < 1024 {
		perShard = 1024
	}
	for i := range m.shards {
		m.shards[i].filter = cuckoo.NewFilter(uint(perShard))
		m.shards[i].recent = make(map[subtreepkg.Inpoint]struct{}, 64)
	}

	m.filterMemBytes = int64(dpsNumFilterShards) * int64(getNextPow2(uint(perShard)))

	for i := range m.disks {
		go m.writerLoop(i)
	}

	return m, nil
}

// writerLoop is the dedicated goroutine for a single disk shard.
func (m *DiskParentSpendsMap) writerLoop(diskIdx int) {
	d := &m.disks[diskIdx]
	existsMarker := []byte{0x01}
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

		_ = d.batch.Set(entry.key[:], existsMarker)
		d.bytesWritten += int64(dpsInpointKeySize + 1)
		pending++

		if pending >= dpsWriterFlushThreshold {
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

// dpsShardOf returns the filter shard index for an inpoint, using the first 2 bytes of the hash.
func dpsShardOf(inpoint subtreepkg.Inpoint) uint16 {
	return binary.LittleEndian.Uint16(inpoint.Hash[:2]) % dpsNumFilterShards
}

// diskOf returns the disk shard index for an inpoint.
func (m *DiskParentSpendsMap) diskOf(inpoint subtreepkg.Inpoint) int {
	return int(dpsShardOf(inpoint)) % m.numDisks
}

// inpointToKey serializes an Inpoint to a fixed-size byte array for Badger storage.
func inpointToKey(inpoint subtreepkg.Inpoint) [dpsInpointKeySize]byte {
	var key [dpsInpointKeySize]byte
	copy(key[:chainhash.HashSize], inpoint.Hash[:])
	binary.BigEndian.PutUint32(key[chainhash.HashSize:], inpoint.Index)
	return key
}

// inpointToFilterBytes returns the byte representation for cuckoo filter operations.
func inpointToFilterBytes(inpoint subtreepkg.Inpoint) []byte {
	key := inpointToKey(inpoint)
	return key[:]
}

// SetIfNotExists atomically checks if an inpoint exists and inserts it if not.
// Returns true if the inpoint was inserted (new), false if it already existed (duplicate).
func (m *DiskParentSpendsMap) SetIfNotExists(inpoint subtreepkg.Inpoint) bool {
	filterBytes := inpointToFilterBytes(inpoint)
	s := &m.shards[dpsShardOf(inpoint)]

	s.mu.Lock()

	if s.filter.Lookup(filterBytes) {
		if _, inRecent := s.recent[inpoint]; inRecent {
			s.mu.Unlock()
			return false
		}

		s.mu.Unlock()

		// Serialize slow path: prevents clearRecentMapsForDisk from clearing
		// an entry between another goroutine's insert and our re-check.
		// Held through channel send so the next slow-path entrant's flushDisk
		// is guaranteed to find our entry on disk (FIFO channel ordering).
		s.slowMu.Lock()

		s.mu.Lock()
		if _, inRecent := s.recent[inpoint]; inRecent {
			s.mu.Unlock()
			s.slowMu.Unlock()
			return false
		}
		s.mu.Unlock()

		if m.existsInStore(inpoint) {
			s.slowMu.Unlock()
			return false
		}

		s.mu.Lock()
		if _, inRecent := s.recent[inpoint]; inRecent {
			s.mu.Unlock()
			s.slowMu.Unlock()
			return false
		}

		s.filter.Insert(filterBytes)
		s.recent[inpoint] = struct{}{}
		s.mu.Unlock()

		key := inpointToKey(inpoint)
		m.disks[m.diskOf(inpoint)].writeCh <- dpsWriteEntry{key: key}
		m.count.Add(1)

		s.slowMu.Unlock()
		return true
	}

	// Fast path: filter negative (new entry)
	s.filter.Insert(filterBytes)
	s.recent[inpoint] = struct{}{}
	s.mu.Unlock()

	key := inpointToKey(inpoint)
	m.disks[m.diskOf(inpoint)].writeCh <- dpsWriteEntry{key: key}
	m.count.Add(1)

	return true
}

// Close releases all resources across all disk shards.
func (m *DiskParentSpendsMap) Close() error {
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
func (m *DiskParentSpendsMap) flushDisk(diskIdx int) {
	done := make(chan struct{})
	m.disks[diskIdx].writeCh <- dpsWriteEntry{flushDone: done}
	<-done
}

// clearRecentMapsForDisk clears recent maps for filter shards mapped to a specific disk.
func (m *DiskParentSpendsMap) clearRecentMapsForDisk(diskIdx int) {
	for i := range m.shards {
		if i%m.numDisks != diskIdx {
			continue
		}
		m.shards[i].mu.Lock()
		if len(m.shards[i].recent) > 0 {
			m.shards[i].recent = make(map[subtreepkg.Inpoint]struct{}, 64)
		}
		m.shards[i].mu.Unlock()
	}
}

// existsInStore checks if an inpoint exists in the Badger store for its disk shard.
func (m *DiskParentSpendsMap) existsInStore(inpoint subtreepkg.Inpoint) bool {
	diskIdx := m.diskOf(inpoint)
	m.flushDisk(diskIdx)

	key := inpointToKey(inpoint)
	val, err := m.disks[diskIdx].store.Get(key[:])
	return err == nil && val != nil
}

// Stats returns current metrics. Safe to call after Flush() when writer goroutines are idle.
func (m *DiskParentSpendsMap) Stats() DiskMapStats {
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
