package subtreeprocessor

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/tempstore"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	cuckoo "github.com/seiflotfy/cuckoofilter"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
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
	batch     *[]writeEntry // non-nil = several writes in one message (pointer keeps the channel element small)
}

// shardBatch is the writer's pending Badger batch; tempstore.WriteBatch
// implements it.
type shardBatch interface {
	Set(key, value []byte) error
	Flush() error
	Cancel()
}

// diskShard is one Badger instance on a single disk, with its own writer goroutine.
// Multiple disk shards across physical disks give linear I/O scaling.
type diskShard struct {
	store        *tempstore.BadgerTempStore
	batch        shardBatch
	newBatch     func() shardBatch // creates a fresh batch against the current store; recreated by Clear
	writeCh      chan writeEntry
	done         chan struct{}
	path         string
	prefix       string
	bytesWritten int64 // written by this disk's writerLoop, read concurrently by Stats(); access via sync/atomic

	// pending bounds entries sent by SetBatch and not yet written; released
	// by the writer. Set is bounded by writeCh's capacity instead.
	pending    *semaphore.Weighted
	pendingCap int64
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

	// errMu guards err, the first storage error since the last TakeErr. Map
	// operations have no error return and writes are asynchronous, so failures
	// are recorded here and surfaced by the subtree processor.
	errMu sync.Mutex
	err   error

	// closeWarnMu guards closeWarn: a benign cleanup failure closing a
	// generation Clear has already successfully rotated away from. The
	// rotation and the data are both fine - only the discarded generation's
	// directory is left behind - so this is kept separate from err/recordErr,
	// which would fail the caller's operation. See TakeCloseWarn.
	closeWarnMu sync.Mutex
	closeWarn   error
}

// recordCloseWarn keeps closeWarn if no earlier warning is pending, the same
// take-once contract as recordErr/TakeErr but for a warning that must never
// fail an operation.
func (m *DiskTxMap) recordCloseWarn(err error) {
	if err == nil {
		return
	}

	m.closeWarnMu.Lock()
	if m.closeWarn == nil {
		m.closeWarn = err
	}
	m.closeWarnMu.Unlock()
}

// TakeCloseWarn returns, and clears, the first Close warning recorded since
// the last call. Callers that rotate this map (Clear) should check this
// afterward and log/count it - never fail on it, since by the time Clear
// records one, the rotation itself has already succeeded.
func (m *DiskTxMap) TakeCloseWarn() error {
	m.closeWarnMu.Lock()
	defer m.closeWarnMu.Unlock()

	err := m.closeWarn
	m.closeWarn = nil

	return err
}

// recordErr keeps err if no earlier error is pending.
func (m *DiskTxMap) recordErr(err error) {
	if err == nil {
		return
	}

	m.errMu.Lock()
	if m.err == nil {
		m.err = err
	}
	m.errMu.Unlock()
}

// TakeErr returns the first storage error recorded since the last call, and
// clears it. A non-nil error means earlier writes, reads or deletes did not
// reach disk; writes in a failed flush are lost.
func (m *DiskTxMap) TakeErr() error {
	m.errMu.Lock()
	defer m.errMu.Unlock()

	err := m.err
	m.err = nil

	return err
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

	// MaxPendingWrites bounds the entries SetBatch has queued for the disk
	// writers across all disks (0 = default): SetBatch waits for room before
	// sending. It doesn't bound the caller's own batch, which SetBatch groups
	// by disk before sending.
	MaxPendingWrites int
}

// staleDiskTxMapDirPattern matches the Badger directory names the subtree
// processor's disk tx maps create: tempstore.New's <prefix>-<unixnano>-<pid>,
// where NewDiskTxMap's prefix is <map prefix>-disk<i> and the map prefix is
// ba-txmap, ba-txmap-shadow or ba-txmap-reorg (see SubtreeProcessor). The
// submatches are the creation time and the pid.
var staleDiskTxMapDirPattern = regexp.MustCompile(`^ba-txmap(?:-shadow|-reorg)?-disk\d+-(\d+)-(\d+)$`)

// processStartNanos is when this process started, for telling its own disk
// tx map dirs from a previous run's with the same pid.
var processStartNanos = time.Now().UnixNano()

// createdByThisProcess reports whether a dir name matched by
// staleDiskTxMapDirPattern carries this process's pid and a creation time
// after it started. A previous run with the same pid (a container's process
// is often pid 1 on every start) is older.
func createdByThisProcess(match []string) bool {
	nanos, nanosErr := strconv.ParseInt(match[1], 10, 64)
	pid, pidErr := strconv.Atoi(match[2])

	return nanosErr == nil && pidErr == nil && pid == os.Getpid() && nanos >= processStartNanos
}

// removeStaleDiskTxMapDirs removes the disk tx map directories under paths
// left by previous runs. Only Close removes a map's directories, so a process
// that exited without closing its maps (killed, or a Stop that timed out on a
// running handler) leaves them behind. Dirs this process created are kept: it
// can run several subtree processors on the same paths (a multi-node test
// daemon), and those dirs may be live. It returns the directories removed and
// the first error; a path that doesn't exist yet is not an error.
func removeStaleDiskTxMapDirs(paths []string) (removed []string, err error) {
	for _, path := range paths {
		entries, readErr := os.ReadDir(path)
		if readErr != nil {
			if !os.IsNotExist(readErr) && err == nil {
				err = errors.NewStorageError("failed to list disk tx map dir %s", path, readErr)
			}

			continue
		}

		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}

			match := staleDiskTxMapDirPattern.FindStringSubmatch(entry.Name())
			if match == nil || createdByThisProcess(match) {
				continue
			}

			dir := filepath.Join(path, entry.Name())

			if rmErr := os.RemoveAll(dir); rmErr != nil {
				if err == nil {
					err = errors.NewStorageError("failed to remove stale disk tx map dir %s", dir, rmErr)
				}

				continue
			}

			removed = append(removed, dir)
		}
	}

	return removed, err
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

	maxPending := opts.MaxPendingWrites
	if maxPending <= 0 {
		maxPending = writeChBuffer
	}

	pendingPerDisk := int64(maxPending / len(paths))
	if pendingPerDisk < 1 {
		pendingPerDisk = 1
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
			store:    store,
			batch:    store.NewWriteBatch(),
			newBatch: func() shardBatch { return store.NewWriteBatch() },
			writeCh:  make(chan writeEntry, writeChBuffer/len(paths)),
			done:     make(chan struct{}),
			path:     path,
			prefix:   fmt.Sprintf("%s-disk%d", prefix, i),

			pending:    semaphore.NewWeighted(pendingPerDisk),
			pendingCap: pendingPerDisk,
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
				m.flushBatch(diskIdx)
				m.clearRecentMapsForDisk(diskIdx)
				pending = 0
			}
			close(entry.flushDone)
			continue
		}

		if entry.batch != nil {
			entries := *entry.batch
			for i := range entries {
				value := serializeTxMapValue(entries[i].inpoints)
				m.recordSetErr(diskIdx, d.batch.Set(entries[i].key[:], value))
				atomic.AddInt64(&d.bytesWritten, int64(chainhash.HashSize+len(value)))
			}

			pending += len(entries)
			d.pending.Release(int64(len(entries)))
		} else {
			value := serializeTxMapValue(entry.inpoints)
			m.recordSetErr(diskIdx, d.batch.Set(entry.key[:], value))
			atomic.AddInt64(&d.bytesWritten, int64(chainhash.HashSize+len(value)))
			pending++
		}

		if pending >= writerFlushThreshold {
			m.flushBatch(diskIdx)
			m.clearRecentMapsForDisk(diskIdx)
			pending = 0
		}
	}

	if pending > 0 {
		m.flushBatch(diskIdx)
		m.clearRecentMapsForDisk(diskIdx)
	}
	close(d.done)
}

// flushBatch flushes the disk's pending batch and records any error. A failed
// Badger batch stays failed - Badger keeps the commit error and marks the
// batch finished, so every subsequent Set/Flush on it fails too - so on error
// this cancels it and installs a fresh one from newBatch, recovering the shard
// for later writes instead of failing every write until the next Clear
// rotation happens to succeed.
func (m *DiskTxMap) flushBatch(diskIdx int) {
	d := &m.disks[diskIdx]

	err := d.batch.Flush()
	m.recordFlushErr(diskIdx, err)

	if err != nil {
		d.batch.Cancel()
		d.batch = d.newBatch()
	}
}

func (m *DiskTxMap) recordSetErr(diskIdx int, err error) {
	if err != nil {
		m.recordErr(errors.NewStorageError("disk tx map: queueing write on disk %d", diskIdx, err))
	}
}

func (m *DiskTxMap) recordFlushErr(diskIdx int, err error) {
	if err != nil {
		m.recordErr(errors.NewStorageError("disk tx map: writing batch on disk %d", diskIdx, err))
	}
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

	// A failed delete leaves the value readable through Get, which skips the
	// filter. The recorded error requests a reset (diskTxMapErr), which
	// rebuilds the map.
	if err := m.disks[diskIdx].store.Delete(hash[:]); err != nil {
		m.recordErr(errors.NewStorageError("disk tx map: deleting %s on disk %d", hash, diskIdx, err))
	}

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

// SetBatch stores each tx's inpoints like Set, sending one message per disk
// instead of one per tx. It marks the whole batch as recently written before
// sending any of it, so it must not run concurrently with SetIfNotExists for
// the same hashes (AddNodesDirectly, its caller, runs alone).
func (m *DiskTxMap) SetBatch(txs []*utxostore.UnminedTransaction) {
	perDisk := make([][]writeEntry, m.numDisks)

	var added int64

	for _, tx := range txs {
		hash := tx.Hash
		s := &m.shards[shardOf(hash)]

		s.mu.Lock()
		if !s.filter.Lookup(hash[:]) {
			s.filter.Insert(hash[:])
			added++
		}
		s.recent[hash] = struct{}{}
		s.mu.Unlock()

		d := m.diskOf(hash)
		perDisk[d] = append(perDisk[d], writeEntry{key: hash, inpoints: tx.TxInpoints})
	}

	m.count.Add(added)

	for d, entries := range perDisk {
		disk := &m.disks[d]

		for len(entries) > 0 {
			n := min(int64(len(entries)), disk.pendingCap)

			// Background never cancels, so Acquire only waits for the writer.
			_ = disk.pending.Acquire(context.Background(), n)

			chunk := entries[:n]
			disk.writeCh <- writeEntry{batch: &chunk}
			entries = entries[n:]
		}
	}
}

// Clear removes all entries and recreates filters and stores.
// Clear is all-or-nothing: either every disk shard rotates onto a fresh Badger
// generation and the filters reset with it, or nothing changes at all.
//
// A partial clear is silently corrupting rather than merely stale. Get reads
// straight from the store without consulting the cuckoo filter, so fresh
// filters over a store that still holds the previous generation's keys answer
// lookups that should miss with the old inpoints, while count reports an empty
// map. Callers that need an empty map must therefore check Length() afterwards
// rather than assume this succeeded — resetSubtreeState does, and fails the
// block instead of installing a half that is still populated.
func (m *DiskTxMap) Clear() {
	m.flushAllDisks()

	// Build every replacement before touching a single shard, so a failure
	// part-way through leaves nothing half-rotated.
	replacements := make([]*tempstore.BadgerTempStore, len(m.disks))

	for i := range m.disks {
		store, err := tempstore.New(tempstore.Options{
			BasePath:   m.disks[i].path,
			Prefix:     m.disks[i].prefix,
			SyncWrites: false,
		})
		if err != nil {
			for _, created := range replacements[:i] {
				_ = created.Close()
			}

			m.recordErr(errors.NewStorageError("disk tx map: rotating store on disk %d", i, err))

			return
		}

		replacements[i] = store
	}

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

		if err := d.store.Close(); err != nil {
			// Rotation for every disk already succeeded by this point (the
			// replacement loop above returns early on any failure) - this is
			// a leaked directory from the discarded generation, not a data
			// problem, so it must not fail the caller. See TakeCloseWarn.
			m.recordCloseWarn(errors.NewStorageError("disk tx map: closing previous generation on disk %d", i, err))
		}

		store := replacements[i]
		d.store = store
		d.batch = store.NewWriteBatch()
		d.newBatch = func() shardBatch { return store.NewWriteBatch() }
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
	// Collect only non-nil errors: teranode errors.Join panics on a nil
	// argument after a non-nil first one.
	var errs []error
	for i := range m.disks {
		if err := m.disks[i].store.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}

// UpdateSubtreeIndex updates the SubtreeIndex for a hash in the correct disk shard.
func (m *DiskTxMap) UpdateSubtreeIndex(hash chainhash.Hash, subtreeIndex int16) error {
	m.flushDisk(m.diskOf(hash))

	d := &m.disks[m.diskOf(hash)]
	val, err := d.store.Get(hash[:])
	if err != nil {
		return errors.NewStorageError("disk tx map: reading %s for subtree index update", hash.String(), err)
	}

	if val == nil {
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

// UpdateSubtreeIndexBatch sets the SubtreeIndex of every node's entry, like one
// UpdateSubtreeIndex call per node, but with a single flush and, per disk, one
// read transaction and one write batch. Nodes without an entry are skipped.
//
// Like UpdateSubtreeIndex, it rewrites each value it read, so the caller must
// not write these entries concurrently (the subtree processor goroutine owns
// the map).
func (m *DiskTxMap) UpdateSubtreeIndexBatch(nodes []subtreepkg.Node, subtreeIndex int16) error {
	if len(nodes) == 0 {
		return nil
	}

	perDisk := make([][]int32, m.numDisks)
	for i := range nodes {
		d := m.diskOf(nodes[i].Hash)
		perDisk[d] = append(perDisk[d], int32(i)) //nolint:gosec // subtree node counts fit in int32
	}

	m.flushAllDisks()

	var g errgroup.Group

	for diskIdx, idxs := range perDisk {
		if len(idxs) == 0 {
			continue
		}

		g.Go(func() error {
			return m.updateSubtreeIndexOnDisk(diskIdx, nodes, idxs, subtreeIndex)
		})
	}

	return g.Wait()
}

func (m *DiskTxMap) updateSubtreeIndexOnDisk(diskIdx int, nodes []subtreepkg.Node, idxs []int32, subtreeIndex int16) error {
	store := m.disks[diskIdx].store
	wb := store.NewWriteBatch()

	// Always release the batch: tempstore's Flush opens a fresh Badger batch,
	// and an abandoned one pins Badger's read watermark for the life of the
	// store, retaining every later commit's conflict keys.
	defer wb.Cancel()

	err := store.GetEach(len(idxs), func(i int) []byte {
		return nodes[idxs[i]].Hash[:]
	}, func(i int, val []byte) error {
		if len(val) < 2 {
			return nil
		}

		// The value is only valid during the callback and the batch keeps it.
		updated := make([]byte, len(val))
		copy(updated, val)
		binary.LittleEndian.PutUint16(updated[:2], uint16(subtreeIndex)) //nolint:gosec // stored as the raw int16 bits, as in UpdateSubtreeIndex

		return wb.Set(nodes[idxs[i]].Hash[:], updated)
	})
	if err != nil {
		return errors.NewStorageError("updating subtree index on disk %d", diskIdx, err)
	}

	if err = wb.Flush(); err != nil {
		return errors.NewStorageError("flushing subtree index on disk %d", diskIdx, err)
	}

	return nil
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

// getFromStore retrieves and deserializes TxInpoints from the correct disk
// shard, recording any read error on the map for a later operation boundary
// to report.
func (m *DiskTxMap) getFromStore(hash chainhash.Hash) *subtreepkg.TxInpoints {
	inpoints, err := m.getFromStoreOrErr(hash)
	if err != nil {
		m.recordErr(err)
		return nil
	}

	return inpoints
}

// getFromStoreOrErr is getFromStore without the side effect of recording the
// error on the map. Used by GetWithErr, whose callers run concurrently with
// processor operations (e.g. the async subtree storer) and must handle their
// own read errors rather than have them land on whichever operation happens
// to check the map's pending error next.
func (m *DiskTxMap) getFromStoreOrErr(hash chainhash.Hash) (*subtreepkg.TxInpoints, error) {
	diskIdx := m.diskOf(hash)
	m.flushDisk(diskIdx)

	val, err := m.disks[diskIdx].store.Get(hash[:])
	if err != nil {
		return nil, errors.NewStorageError("disk tx map: reading %s on disk %d", hash, diskIdx, err)
	}

	if val == nil {
		return nil, nil
	}

	return deserializeTxMapValue(val), nil
}

// GetWithErr behaves like Get but returns the read error to the caller instead
// of recording it on the map. Callers that run concurrently with processor
// operations must use this so a read error they cause is not misattributed to
// whichever operation next checks the map's pending error.
func (m *DiskTxMap) GetWithErr(hash chainhash.Hash) (*subtreepkg.TxInpoints, bool, error) {
	inpoints, err := m.getFromStoreOrErr(hash)
	if err != nil {
		return nil, false, err
	}

	if inpoints == nil {
		return nil, false, nil
	}

	return inpoints, true, nil
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

// Stats returns current metrics. It may be called concurrently with active
// writer goroutines (e.g. reportDiskMapStats during moveForwardBlock before
// Clear()), so bytesWritten is read atomically.
func (m *DiskTxMap) Stats() DiskMapStats {
	var diskBytes int64
	for i := range m.disks {
		diskBytes += atomic.LoadInt64(&m.disks[i].bytesWritten)
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
