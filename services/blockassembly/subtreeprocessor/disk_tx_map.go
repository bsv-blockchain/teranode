package subtreeprocessor

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"golang.org/x/sys/unix"
)

const (
	numIndexShards = 4096

	// logSegmentsPerDir is how many payload log files each configured dir
	// holds per generation. Appends to one segment are serialized, so more
	// segments means less contention between the parallel dequeue filters.
	logSegmentsPerDir = 64

	// logBufferSize is how much a segment buffers before writing it out.
	logBufferSize = 512 << 10

	// logMaxBuffered bounds what a segment holds in memory while a write is
	// in progress: an append past it waits for the disk (backpressure).
	logMaxBuffered = 8 * logBufferSize

	// logReadAhead is how much a read of a written payload fetches in one
	// call; larger payloads take a second read.
	logReadAhead = 512

	// recordMagic starts every payload record, so a record lost to a failed
	// write (a zero-filled hole or the end of the file) reads as an error
	// instead of as an empty TxInpoints.
	recordMagic      = 0xA5
	recordHeaderSize = 5 // magic + uint32 payload length

	// indexBytesPerEntry is the measured RAM cost of one index entry (Go map
	// of 32-byte key to uint64 value, averaged over map growth), for Stats.
	indexBytesPerEntry = 80

	// maxLogOffset is the largest offset an index entry can hold (48 bits).
	maxLogOffset = 1<<48 - 1

	// mapWindowSize is the size of each read-only window a log segment is
	// mapped in, as the file grows. A multiple of the page size.
	mapWindowSize = 64 << 20

	// subtreeIndexChunkMin is the fewest nodes UpdateSubtreeIndexBatch hands
	// to one goroutine; smaller batches aren't worth splitting.
	subtreeIndexChunkMin = 16 << 10
)

// indexShard is one of 4096 independent segments of the in-RAM index.
//
// readers counts the reads in progress on this shard's entries, per
// generation parity (see generation): Clear and Close wait for the old
// generation's counts to drain before unmapping its files. Counting per shard
// keeps the per-read cost on a cache line the read already holds.
type indexShard struct {
	mu      sync.Mutex
	index   map[chainhash.Hash]uint64 // see packEntry
	readers [2]atomic.Int32
}

// generation is one set of payload log files: a directory under every base
// path with its segments. Clear replaces the whole generation. parity
// alternates between consecutive generations and selects the indexShard
// reader count that pins this one.
type generation struct {
	dirs   []string
	logs   []payloadLog
	parity int
}

// logOf returns the payload log segment for a hash.
func (g *generation) logOf(hash chainhash.Hash) int {
	return int(shardOf(hash)) % len(g.logs)
}

// logFile is the part of *os.File a payload log segment uses; tests replace
// it to inject failures.
type logFile interface {
	io.WriterAt
	io.ReaderAt
	Close() error
}

// payloadLog is one append-only segment of serialized TxInpoints. Records
// are appended to buf. Once buf is full it becomes inflight and is written
// outside mu, so appends and reads never wait for the disk: bytes below
// flushed are in the file and immutable, bytes in [flushed, flushed+len(inflight))
// are in inflight, and later bytes are in buf. wmu serializes the writes.
//
// Written bytes are read through windows, read-only shared mappings of
// consecutive mapWindowSize ranges of the file, added as writes reach them: a
// read is a copy instead of a pread syscall. Only bytes below written (the end
// of the last successful write) are read through them, since touching a
// mapping past the end of the file faults. A record that straddles two
// windows, or lies past the last one, is read with pread. Records never span
// two buffers, so a record in a lost range was lost whole.
type payloadLog struct {
	mu       sync.Mutex
	wmu      sync.Mutex
	file     logFile
	fd       int // for mapping windows; -1 once mapping is off
	buf      []byte
	inflight []byte // being written; nil when no write is in progress
	spare    []byte // the last written buffer, reused as the next buf
	flushed  int64
	written  int64
	lost     [][2]int64 // [start, end) of every buffer a failed write lost
	windows  [][]byte   // window i maps [i*mapWindowSize, (i+1)*mapWindowSize)
	mapped   [][]byte   // every mapping made, for close; tests clear windows only
}

// DiskTxMap implements TxInpointsMap with an exact in-RAM index and the
// TxInpoints themselves in append-only log files on disk.
//
//   - Membership (SetIfNotExists, Exists) and the SubtreeIndex are answered by
//     the index alone: hash -> (payload offset, SubtreeIndex), ~80 bytes per
//     entry. Dedup never touches disk.
//   - Payloads are appended to one of logSegmentsPerDir segments per
//     configured dir, chosen by hash, and read back only by Get.
//   - A map generation is never compacted: Delete forgets the hash and Set
//     appends a new payload. Clear discards the whole generation, which is
//     how the subtree processor recycles each half of its double buffer.
//   - Reads may run concurrently with Clear and Close (the async subtree meta
//     storer reads a half the processor rotates): a read pins the generation
//     its entry points into, and Clear and Close wait for those pins before
//     unmapping it. Writes must not run concurrently with Clear or Close.
type DiskTxMap struct {
	shards    [numIndexShards]indexShard
	gen       atomic.Pointer[generation] // nil once closed
	basePaths []string
	prefix    string

	// bytesWritten counts payload bytes written over the map's life, across
	// generations, for Stats.
	bytesWritten atomic.Int64

	// errMu guards err, the first storage error since the last TakeErr. Map
	// operations have no error return, so failures are recorded here and
	// surfaced by the subtree processor.
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
// clears it. A non-nil error means earlier writes or reads did not reach
// disk; payloads in a failed write are lost and read back as errors.
func (m *DiskTxMap) TakeErr() error {
	m.errMu.Lock()
	defer m.errMu.Unlock()

	err := m.err
	m.err = nil

	return err
}

// DiskTxMapOptions configures the DiskTxMap.
type DiskTxMapOptions struct {
	// BasePaths is a list of directories for the payload logs, each ideally on
	// a separate physical disk. If empty, BasePath is used as a single disk.
	BasePaths []string
	// BasePath is used when BasePaths is empty (single-disk mode).
	BasePath string
	Prefix   string
}

// staleDiskTxMapDirPattern matches the generation directory names the subtree
// processor's disk tx maps create: <prefix>-disk<i>-<unixnano>-<pid>, where
// the map prefix is ba-txmap, ba-txmap-shadow or ba-txmap-reorg (see
// SubtreeProcessor). The submatches are the creation time and the pid.
var staleDiskTxMapDirPattern = regexp.MustCompile(`^ba-txmap(?:-shadow|-reorg)?-disk\d+-(\d+)-(\d+)$`)

// generationClock stamps generation directory names; tests replace it.
var generationClock = time.Now

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

// NewDiskTxMap creates a new DiskTxMap with its payload logs spread over the
// configured dirs.
func NewDiskTxMap(opts DiskTxMapOptions) (*DiskTxMap, error) {
	prefix := opts.Prefix
	if prefix == "" {
		prefix = "disktxmap"
	}

	paths := opts.BasePaths
	if len(paths) == 0 {
		paths = []string{opts.BasePath}
	}

	m := &DiskTxMap{
		basePaths: paths,
		prefix:    prefix,
	}

	g, err := m.openGeneration(0)
	if err != nil {
		return nil, err
	}

	m.gen.Store(g)

	for i := range m.shards {
		m.shards[i].index = make(map[chainhash.Hash]uint64)
	}

	return m, nil
}

// openGeneration creates a fresh directory under every base path with its
// log segments. It is all-or-nothing: on failure it removes whatever it
// created.
func (m *DiskTxMap) openGeneration(parity int) (*generation, error) {
	dirs := make([]string, 0, len(m.basePaths))
	logs := make([]payloadLog, len(m.basePaths)*logSegmentsPerDir)
	opened := 0

	fail := func(err error) (*generation, error) {
		for i := 0; i < opened; i++ {
			_ = logs[i].close()
		}

		for _, dir := range dirs {
			_ = os.RemoveAll(dir)
		}

		return nil, err
	}

	for i, path := range m.basePaths {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return fail(errors.NewStorageError("disk tx map: creating directory on disk %d (%s)", i, path, err))
		}

		// The directory must be new: another map in this process with the
		// same prefix can pick the same timestamp, and sharing its directory
		// would truncate its live files. Step the timestamp until it's free.
		for nanos := generationClock().UnixNano(); ; nanos++ {
			dir := filepath.Join(path, fmt.Sprintf("%s-disk%d-%d-%d", m.prefix, i, nanos, os.Getpid()))

			err := os.Mkdir(dir, 0o700)
			if err == nil {
				dirs = append(dirs, dir)
				break
			}

			if !os.IsExist(err) {
				return fail(errors.NewStorageError("disk tx map: creating directory on disk %d (%s)", i, path, err))
			}
		}
	}

	// Segment k lives under base path k % len(basePaths), so consecutive
	// segments, and the hashes that map to them, alternate between disks.
	for k := range logs {
		name := filepath.Join(dirs[k%len(dirs)], fmt.Sprintf("seg-%d.log", k))

		f, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return fail(errors.NewStorageError("disk tx map: creating log segment %s", name, err))
		}

		logs[k].file = f
		logs[k].fd = int(f.Fd()) //nolint:gosec // fd fits in int
		opened++
	}

	return &generation{dirs: dirs, logs: logs, parity: parity}, nil
}

// packEntry packs a payload offset and a SubtreeIndex into an index value.
func packEntry(offset int64, subtreeIndex int16) uint64 {
	return uint64(offset)<<16 | uint64(uint16(subtreeIndex)) //nolint:gosec // offset < 2^48, SubtreeIndex stored as raw bits
}

func entryOffset(e uint64) int64 { return int64(e >> 16) } //nolint:gosec // 48-bit value

func entrySubtreeIndex(e uint64) int16 { return int16(uint16(e)) } //nolint:gosec // raw bits, as stored

func withSubtreeIndex(e uint64, subtreeIndex int16) uint64 {
	return e&^0xFFFF | uint64(uint16(subtreeIndex)) //nolint:gosec // raw bits
}

// shardOf returns the index shard for a hash.
func shardOf(hash chainhash.Hash) uint16 {
	return binary.LittleEndian.Uint16(hash[:2]) % numIndexShards
}

// serializePayload returns the bytes stored for inpoints. A TxInpoints that
// can't be serialized is recorded as an error and stored as an empty payload,
// which reads back as an error.
func (m *DiskTxMap) serializePayload(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints) []byte {
	payload, err := inpoints.Serialize()
	if err != nil {
		m.recordErr(errors.NewProcessingError("disk tx map: serializing inpoints of %s", hash.String(), err))
		return nil
	}

	return payload
}

// appendPayload appends a record to the hash's log segment and returns its
// offset, or -1 (recorded as an error) if the segment is past what an index
// entry can address. Callers append before taking the hash's index shard lock
// and publish the entry after, so the shard lock never waits on a segment
// lock; a record whose entry loses a race to a duplicate is dead space until
// the generation is cleared.
func (m *DiskTxMap) appendPayload(g *generation, hash chainhash.Hash, payload []byte) int64 {
	logIdx := g.logOf(hash)
	l := &g.logs[logIdx]

	l.mu.Lock()
	offset := m.appendLocked(l, logIdx, payload)
	m.afterAppendUnlock(g, logIdx)

	return offset
}

// appendLocked appends one record to segment l and returns its offset, or -1
// (recorded as an error) past the addressable offset. The caller holds l.mu.
func (m *DiskTxMap) appendLocked(l *payloadLog, logIdx int, payload []byte) int64 {
	offset := l.flushed + int64(len(l.inflight)) + int64(len(l.buf))
	if offset > maxLogOffset {
		m.recordErr(errors.NewStorageError("disk tx map: log segment %d exceeds the maximum offset", logIdx))
		return -1
	}

	var header [recordHeaderSize]byte
	header[0] = recordMagic
	binary.LittleEndian.PutUint32(header[1:], uint32(len(payload))) //nolint:gosec // a TxInpoints is far below 4GB

	if l.buf == nil {
		l.buf = make([]byte, 0, logBufferSize+logReadAhead)
	}

	l.buf = append(l.buf, header[:]...)
	l.buf = append(l.buf, payload...)

	return offset
}

// afterAppendUnlock releases segment logIdx's lock after appends, then
// starts a write if its buffer is full, or waits for the disk if too much is
// buffered behind a write in progress. The caller holds the segment's mu.
func (m *DiskTxMap) afterAppendUnlock(g *generation, logIdx int) {
	l := &g.logs[logIdx]

	// Hand a full buffer to a write unless one is already in progress; then
	// buf keeps growing and the next append past the threshold starts one.
	startWrite := len(l.buf) >= logBufferSize && l.takeBufLocked()
	mustWait := !startWrite && len(l.buf) >= logMaxBuffered

	l.mu.Unlock()

	switch {
	case startWrite:
		l.wmu.Lock()
		m.writeInflight(g, logIdx)
		l.wmu.Unlock()
	case mustWait:
		// The disk is behind: wait for the write in progress, then write what
		// has piled up, rather than let buf grow without bound.
		m.drainSegment(g, logIdx)
	}
}

// takeBufLocked makes buf the in-flight buffer, reporting whether it did (not
// when a write is already in progress or buf is empty). The caller holds mu.
func (l *payloadLog) takeBufLocked() bool {
	if l.inflight != nil || len(l.buf) == 0 {
		return false
	}

	l.inflight = l.buf
	l.buf = l.spare[:0]
	l.spare = nil

	if l.buf == nil {
		l.buf = make([]byte, 0, logBufferSize+logReadAhead)
	}

	return true
}

// writeInflight writes segment logIdx's in-flight buffer, if any, to its
// file, without holding mu. On failure the buffered records are lost: they
// read back as errors, and later records are written after them as usual.
// The caller holds wmu.
func (m *DiskTxMap) writeInflight(g *generation, logIdx int) {
	l := &g.logs[logIdx]

	l.mu.Lock()
	data, at, f := l.inflight, l.flushed, l.file
	l.mu.Unlock()

	if data == nil {
		return
	}

	n, err := f.WriteAt(data, at)

	m.bytesWritten.Add(int64(n))

	l.mu.Lock()
	defer l.mu.Unlock()

	if err != nil {
		m.recordErr(errors.NewStorageError("disk tx map: writing log segment %d", logIdx, err))

		// A partial write leaves some of these bytes in the file, and a later
		// write past them makes them readable: mark the whole range lost.
		l.lost = append(l.lost, [2]int64{at, at + int64(len(data))})
	} else {
		l.written = at + int64(n)
		l.mapWindowsLocked()
	}

	l.flushed = at + int64(len(data))
	l.inflight = nil
	l.spare = data
}

// readPayload returns the payload of the record at offset in segment logIdx
// of g. The caller has pinned g (pinEntry), so its mappings stay valid.
func readPayload(g *generation, logIdx int, offset int64) ([]byte, error) {
	l := &g.logs[logIdx]

	l.mu.Lock()

	if offset >= l.flushed {
		// Still in memory: in the buffer being written, or in buf after it.
		mem, rel := l.inflight, offset-l.flushed
		if rel >= int64(len(l.inflight)) {
			mem, rel = l.buf, rel-int64(len(l.inflight))
		}

		if rel+recordHeaderSize > int64(len(mem)) {
			l.mu.Unlock()
			return nil, errors.NewStorageError("disk tx map: record at %d on log segment %d is past the end of the log", offset, logIdx)
		}

		payload, err := parseRecord(mem[rel:], logIdx, offset)
		if err == nil {
			payload = append([]byte(nil), payload...)
		}

		l.mu.Unlock()

		return payload, err
	}

	f, windows, written := l.file, l.windows, l.written

	for _, r := range l.lost {
		if offset >= r[0] && offset < r[1] {
			l.mu.Unlock()
			return nil, errors.NewStorageError("disk tx map: no record at %d on log segment %d (a lost write)", offset, logIdx)
		}
	}

	l.mu.Unlock()

	// Bytes below flushed never change, so they are read unlocked. Past
	// written they were lost to a failed write.
	if offset+recordHeaderSize > written {
		return nil, errors.NewStorageError("disk tx map: no record at %d on log segment %d (a lost write)", offset, logIdx)
	}

	if w, rel := offset/mapWindowSize, offset%mapWindowSize; w < int64(len(windows)) && rel+recordHeaderSize <= mapWindowSize {
		window := windows[w]
		end := rel + recordHeaderSize + int64(binary.LittleEndian.Uint32(window[rel+1:rel+recordHeaderSize]))

		if window[rel] != recordMagic || offset+(end-rel) > written {
			return nil, errors.NewStorageError("disk tx map: no record at %d on log segment %d (a lost write)", offset, logIdx)
		}

		if end <= mapWindowSize {
			// Decoding copies, so the payload can alias the mapping.
			return parseRecord(window[rel:end], logIdx, offset)
		}
		// The record straddles two windows: read it with pread.
	}

	buf := make([]byte, logReadAhead)

	n, err := f.ReadAt(buf, offset)
	if err != nil && !(errors.Is(err, io.EOF) && n >= recordHeaderSize) {
		return nil, errors.NewStorageError("disk tx map: reading record at %d on log segment %d", offset, logIdx, err)
	}

	buf = buf[:n]

	if buf[0] != recordMagic {
		return nil, errors.NewStorageError("disk tx map: no record at %d on log segment %d (a lost write)", offset, logIdx)
	}

	size := int(binary.LittleEndian.Uint32(buf[1:recordHeaderSize]))
	if recordHeaderSize+size <= len(buf) {
		return parseRecord(buf, logIdx, offset)
	}

	// Don't size an allocation from a length the file doesn't back.
	if offset+int64(recordHeaderSize+size) > written {
		return nil, errors.NewStorageError("disk tx map: truncated record at %d on log segment %d", offset, logIdx)
	}

	full := make([]byte, recordHeaderSize+size)
	copy(full, buf)

	if _, err = f.ReadAt(full[len(buf):], offset+int64(len(buf))); err != nil {
		return nil, errors.NewStorageError("disk tx map: reading record at %d on log segment %d", offset, logIdx, err)
	}

	return parseRecord(full, logIdx, offset)
}

// parseRecord returns the payload of the record at the start of b.
func parseRecord(b []byte, logIdx int, offset int64) ([]byte, error) {
	if len(b) < recordHeaderSize || b[0] != recordMagic {
		return nil, errors.NewStorageError("disk tx map: no record at %d on log segment %d (a lost write)", offset, logIdx)
	}

	size := int(binary.LittleEndian.Uint32(b[1:recordHeaderSize]))
	if size == 0 || recordHeaderSize+size > len(b) {
		return nil, errors.NewStorageError("disk tx map: truncated or empty record at %d on log segment %d", offset, logIdx)
	}

	return b[recordHeaderSize : recordHeaderSize+size], nil
}

// pinEntry returns hash's entry and pins the generation it points into, so
// that generation's files stay mapped until unpin. The entry and the
// generation are read under the same shard lock that Clear takes before it
// swaps generations, so they always belong together.
func (m *DiskTxMap) pinEntry(hash chainhash.Hash) (s *indexShard, g *generation, entry uint64, ok bool) {
	s = &m.shards[shardOf(hash)]

	s.mu.Lock()

	entry, ok = s.index[hash]
	if ok {
		if g = m.gen.Load(); g == nil {
			ok = false
		} else {
			s.readers[g.parity].Add(1)
		}
	}

	s.mu.Unlock()

	return s, g, entry, ok
}

func unpin(s *indexShard, g *generation) {
	s.readers[g.parity].Add(-1)
}

// waitForReaders waits until no read pins a generation with this parity.
func (m *DiskTxMap) waitForReaders(parity int) {
	for i := range m.shards {
		for spins := 0; m.shards[i].readers[parity].Load() != 0; spins++ {
			// Reads are short copies; back off to sleeping in case one is
			// stuck in a slow pread rather than burn a core.
			if spins < 100 {
				runtime.Gosched()
			} else {
				time.Sleep(50 * time.Microsecond)
			}
		}
	}
}

// SetIfNotExists inserts hash with inpoints if it is not in the map. Whether
// it is in the map is decided by the in-RAM index alone. The existing
// inpoints are not returned (no caller uses them): a duplicate returns nil,
// false.
func (m *DiskTxMap) SetIfNotExists(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints) (*subtreepkg.TxInpoints, bool) {
	s := &m.shards[shardOf(hash)]

	s.mu.Lock()
	if _, exists := s.index[hash]; exists {
		s.mu.Unlock()
		return nil, false
	}
	s.mu.Unlock()

	// Serialized and appended outside the lock; wasted only on a racing
	// duplicate.
	offset := m.appendPayload(m.gen.Load(), hash, m.serializePayload(hash, inpoints))
	if offset < 0 {
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.index[hash]; exists {
		return nil, false
	}

	s.index[hash] = packEntry(offset, inpoints.SubtreeIndex)

	return nil, true
}

// MoveFrom inserts every hash in hashes with the inpoints and SubtreeIndex it
// has in src, like src.Get followed by SetIfNotExists per hash, setting
// wasSet[i] for each, but copies the stored bytes instead of decoding and
// re-encoding them. Hashes already in m are not copied. It groups the work so
// each index shard and log segment lock is taken once per group instead of
// once per hash.
//
// It returns -1 and nil on success. Otherwise nothing is inserted into m, and
// it returns the position of the first hash (in input order) src doesn't
// have, or the position of a hash src couldn't read with the read error,
// which is also recorded on src, as Get records it.
func (m *DiskTxMap) MoveFrom(src *DiskTxMap, hashes []chainhash.Hash, wasSet []bool) (int, error) {
	n := len(hashes)
	if n == 0 {
		return -1, nil
	}

	// Group positions by index shard (counting sort).
	var shardStart [numIndexShards + 1]int32
	for i := range hashes {
		shardStart[shardOf(hashes[i])+1]++
	}

	for sh := 1; sh <= numIndexShards; sh++ {
		shardStart[sh] += shardStart[sh-1]
	}

	byShard := make([]int32, n)
	fill := shardStart

	for i := range hashes {
		sh := shardOf(hashes[i])
		byShard[fill[sh]] = int32(i) //nolint:gosec // n fits in int32
		fill[sh]++
	}

	// Skip hashes m already has: a duplicate costs neither a read nor a
	// dead record.
	skip := make([]bool, n)

	for sh := 0; sh < numIndexShards; sh++ {
		group := byShard[shardStart[sh]:shardStart[sh+1]]
		if len(group) == 0 {
			continue
		}

		s := &m.shards[sh]
		s.mu.Lock()

		for _, i := range group {
			_, skip[i] = s.index[hashes[i]]
		}

		s.mu.Unlock()
	}

	// Look up and pin every source entry, one shard lock per group.
	entries := make([]uint64, n)

	var pinned [numIndexShards]*generation

	defer func() {
		for sh, g := range pinned {
			if g != nil {
				unpin(&src.shards[sh], g)
			}
		}
	}()

	missing := n

	for sh := 0; sh < numIndexShards; sh++ {
		group := byShard[shardStart[sh]:shardStart[sh+1]]
		if len(group) == 0 {
			continue
		}

		ss := &src.shards[sh]
		ss.mu.Lock()

		g := src.gen.Load()

		for _, i := range group {
			if skip[i] {
				continue
			}

			e, ok := ss.index[hashes[i]]
			if !ok || g == nil {
				missing = min(missing, int(i))
				continue
			}

			entries[i] = e
		}

		if g != nil {
			ss.readers[g.parity].Add(1)
			pinned[sh] = g
		}

		ss.mu.Unlock()
	}

	if missing < n {
		return missing, nil
	}

	// Copy the payloads, grouped by destination segment: one segment lock
	// per group for the appends.
	dg := m.gen.Load()
	numLogs := len(dg.logs)

	segStart := make([]int32, numLogs+1)
	for i := range hashes {
		segStart[int(shardOf(hashes[i]))%numLogs+1]++
	}

	for k := 1; k <= numLogs; k++ {
		segStart[k] += segStart[k-1]
	}

	bySeg := make([]int32, n)
	segFill := append([]int32(nil), segStart...)

	for i := range hashes {
		k := int(shardOf(hashes[i])) % numLogs
		bySeg[segFill[k]] = int32(i) //nolint:gosec // n fits in int32
		segFill[k]++
	}

	offsets := make([]int64, n)
	payloads := make([][]byte, 0, 256)

	for k := 0; k < numLogs; k++ {
		group := bySeg[segStart[k]:segStart[k+1]]
		if len(group) == 0 {
			continue
		}

		payloads = payloads[:0]
		todo := group[:0:0]

		for _, i := range group {
			if skip[i] {
				continue
			}

			sg := pinned[shardOf(hashes[i])]

			payload, err := readPayload(sg, sg.logOf(hashes[i]), entryOffset(entries[i]))
			if err != nil {
				src.recordErr(err)
				return int(i), err
			}

			payloads = append(payloads, payload)
			todo = append(todo, i)
		}

		l := &dg.logs[k]
		l.mu.Lock()

		// Let a write start (or the buffer bound apply) every logBufferSize
		// bytes, as single appends would.
		appended := 0

		for j, i := range todo {
			offsets[i] = m.appendLocked(l, k, payloads[j])

			if appended += recordHeaderSize + len(payloads[j]); appended >= logBufferSize {
				m.afterAppendUnlock(dg, k)
				l.mu.Lock()

				appended = 0
			}
		}

		m.afterAppendUnlock(dg, k)
	}

	// Publish, one destination shard lock per group.
	for sh := 0; sh < numIndexShards; sh++ {
		group := byShard[shardStart[sh]:shardStart[sh+1]]
		if len(group) == 0 {
			continue
		}

		s := &m.shards[sh]
		s.mu.Lock()

		for _, i := range group {
			if _, exists := s.index[hashes[i]]; skip[i] || exists || offsets[i] < 0 {
				wasSet[i] = false
				continue
			}

			s.index[hashes[i]] = packEntry(offsets[i], entrySubtreeIndex(entries[i]))
			wasSet[i] = true
		}

		s.mu.Unlock()
	}

	return -1, nil
}

// Exists returns true if the hash is in the map.
func (m *DiskTxMap) Exists(hash chainhash.Hash) bool {
	s := &m.shards[shardOf(hash)]
	s.mu.Lock()
	_, exists := s.index[hash]
	s.mu.Unlock()

	return exists
}

// Get retrieves the TxInpoints for a hash, recording a read error on the map.
func (m *DiskTxMap) Get(hash chainhash.Hash) (*subtreepkg.TxInpoints, bool) {
	inpoints, found, err := m.GetWithErr(hash)
	if err != nil {
		m.recordErr(err)
		return nil, false
	}

	return inpoints, found
}

// GetWithErr behaves like Get but returns the read error to the caller instead
// of recording it on the map. Callers that run concurrently with processor
// operations must use this so a read error they cause is not misattributed to
// whichever operation next checks the map's pending error.
func (m *DiskTxMap) GetWithErr(hash chainhash.Hash) (*subtreepkg.TxInpoints, bool, error) {
	s, g, entry, exists := m.pinEntry(hash)
	if !exists {
		return nil, false, nil
	}

	defer unpin(s, g)

	logIdx := g.logOf(hash)

	payload, err := readPayload(g, logIdx, entryOffset(entry))
	if err != nil {
		return nil, false, err
	}

	inpoints, err := subtreepkg.NewTxInpointsFromBytes(payload)
	if err != nil {
		return nil, false, errors.NewStorageError("disk tx map: decoding inpoints of %s on log segment %d", hash.String(), logIdx, err)
	}

	inpoints.SubtreeIndex = entrySubtreeIndex(entry)

	return &inpoints, true, nil
}

// Delete removes a hash from the map, reporting whether it was there. Its
// payload stays in the log until the generation is cleared.
func (m *DiskTxMap) Delete(hash chainhash.Hash) bool {
	s := &m.shards[shardOf(hash)]
	s.mu.Lock()
	_, exists := s.index[hash]
	delete(s.index, hash)
	s.mu.Unlock()

	return exists
}

// Length returns the number of entries. It locks every index shard, so it is
// for block boundaries, not the per-tx path.
func (m *DiskTxMap) Length() int {
	n := 0

	for i := range m.shards {
		m.shards[i].mu.Lock()
		n += len(m.shards[i].index)
		m.shards[i].mu.Unlock()
	}

	return n
}

// Set stores inpoints for a hash, overwriting any existing entry.
func (m *DiskTxMap) Set(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints) {
	offset := m.appendPayload(m.gen.Load(), hash, m.serializePayload(hash, inpoints))
	if offset < 0 {
		return
	}

	s := &m.shards[shardOf(hash)]
	s.mu.Lock()
	s.index[hash] = packEntry(offset, inpoints.SubtreeIndex)
	s.mu.Unlock()
}

// SetBatch stores each tx's inpoints like Set.
func (m *DiskTxMap) SetBatch(txs []*utxostore.UnminedTransaction) {
	for _, tx := range txs {
		m.Set(tx.Hash, tx.TxInpoints)
	}
}

// Clear removes all entries by moving onto a fresh generation of log files.
// Clear is all-or-nothing: either every dir gets a fresh generation and the
// index resets with it, or nothing changes and the failure is recorded.
// Callers that need an empty map must therefore check Length() afterwards
// rather than assume this succeeded - resetSubtreeState does, and fails the
// block instead of installing a half that is still populated.
func (m *DiskTxMap) Clear() {
	old := m.gen.Load()

	g, err := m.openGeneration(old.parity ^ 1)
	if err != nil {
		m.recordErr(errors.NewStorageError("disk tx map: rotating to a fresh generation", err))
		return
	}

	m.retire(old, g)

	// Reuse the old generation's buffers: nothing reads or writes old any
	// more, and what they still hold belongs to the discarded generation.
	for k := range g.logs {
		if k < len(old.logs) {
			g.logs[k].buf, old.logs[k].buf = old.logs[k].buf[:0], nil
			g.logs[k].spare, old.logs[k].spare = old.logs[k].spare, nil
		}
	}

	// The rotation has succeeded: failing to discard the old generation only
	// leaves its directory behind, so it must not fail the caller. See
	// TakeCloseWarn.
	if err = closeGeneration(old); err != nil {
		m.recordCloseWarn(errors.NewStorageError("disk tx map: discarding previous generation", err))
	}
}

// retire empties the index, installs next (nil on Close) in place of old, and
// waits for every read that pinned old. Every shard is emptied before the
// swap, so no read can pair an old entry with the next generation.
func (m *DiskTxMap) retire(old, next *generation) {
	m.clearIndex()

	m.gen.Store(next)
	m.waitForReaders(old.parity)
}

// clearIndex empties every index shard by giving it a new map. clear() would
// keep each map's capacity, and a cleared half of the subtree processor's
// double buffer sits empty for a whole block interval, so a busy node held
// twice its index RAM. In BenchmarkDiskTxMap_IndexRefill (32M entries)
// regrowing costs ~5ns per entry over the next block interval, and the
// remainder share MoveFrom inserts inside moveForwardBlock was no slower.
// Replacing a map is O(1), unlike clear(), so one goroutine does all 4096.
// Every shard is empty when it returns.
func (m *DiskTxMap) clearIndex() {
	for i := range m.shards {
		m.shards[i].mu.Lock()
		m.shards[i].index = make(map[chainhash.Hash]uint64)
		m.shards[i].mu.Unlock()
	}
}

// mapWindowsLocked maps windows until they cover every written byte. A
// failed mapping turns mapping off for the segment; reads then use pread.
// The caller holds the segment lock.
func (l *payloadLog) mapWindowsLocked() {
	for l.fd >= 0 && int64(len(l.windows))*mapWindowSize < l.written {
		window, err := unix.Mmap(l.fd, int64(len(l.windows))*mapWindowSize, mapWindowSize, unix.PROT_READ, unix.MAP_SHARED)
		if err != nil {
			l.fd = -1
			return
		}

		l.windows = append(l.windows, window)
		l.mapped = append(l.mapped, window)
	}
}

// close unmaps and closes the segment's file.
func (l *payloadLog) close() error {
	var unmapErr error

	for _, window := range l.mapped {
		if err := unix.Munmap(window); err != nil && unmapErr == nil {
			unmapErr = err
		}
	}

	l.windows, l.mapped = nil, nil

	if err := l.file.Close(); err != nil {
		return err
	}

	return unmapErr
}

// closeGeneration closes a generation's log segments and removes its dirs.
func closeGeneration(g *generation) error {
	// Collect only non-nil errors: teranode errors.Join panics on a nil
	// argument after a non-nil first one.
	var errs []error

	for i := range g.logs {
		if err := g.logs[i].close(); err != nil {
			errs = append(errs, err)
		}
	}

	for _, dir := range g.dirs {
		if err := os.RemoveAll(dir); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) == 0 {
		return nil
	}

	return errors.Join(errs...)
}

// Flush writes every segment's buffered records to its file. Write errors
// are recorded on the map, not returned.
func (m *DiskTxMap) Flush() error {
	g := m.gen.Load()

	for i := range g.logs {
		m.drainSegment(g, i)
	}

	return nil
}

// drainSegment writes whatever segment logIdx has in flight and then the rest
// of its buffer.
func (m *DiskTxMap) drainSegment(g *generation, logIdx int) {
	l := &g.logs[logIdx]

	l.wmu.Lock()
	defer l.wmu.Unlock()

	for {
		m.writeInflight(g, logIdx)

		l.mu.Lock()
		more := l.takeBufLocked()
		l.mu.Unlock()

		if !more {
			return
		}
	}
}

// Close empties the map, waits for reads in progress, and releases the log
// files and directories. The map must not be used afterwards; a read that
// races Close finds nothing.
func (m *DiskTxMap) Close() error {
	old := m.gen.Load()
	if old == nil {
		return nil
	}

	m.retire(old, nil)

	return closeGeneration(old)
}

// UpdateSubtreeIndex sets the SubtreeIndex of hash's entry.
func (m *DiskTxMap) UpdateSubtreeIndex(hash chainhash.Hash, subtreeIndex int16) error {
	s := &m.shards[shardOf(hash)]
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.index[hash]
	if !exists {
		return errors.NewNotFoundError("entry not found for hash %s", hash.String())
	}

	s.index[hash] = withSubtreeIndex(entry, subtreeIndex)

	return nil
}

// UpdateSubtreeIndexBatch sets the SubtreeIndex of every node's entry, like
// one UpdateSubtreeIndex call per node. Nodes without an entry are skipped.
// It only touches the in-RAM index, split over GOMAXPROCS goroutines since the
// subtree processor calls it for every completed subtree.
func (m *DiskTxMap) UpdateSubtreeIndexBatch(nodes []subtreepkg.Node, subtreeIndex int16) error {
	update := func(part []subtreepkg.Node) {
		for i := range part {
			s := &m.shards[shardOf(part[i].Hash)]
			s.mu.Lock()

			if entry, exists := s.index[part[i].Hash]; exists {
				s.index[part[i].Hash] = withSubtreeIndex(entry, subtreeIndex)
			}

			s.mu.Unlock()
		}
	}

	chunk := max(subtreeIndexChunkMin, (len(nodes)+runtime.GOMAXPROCS(0)-1)/runtime.GOMAXPROCS(0))
	if chunk >= len(nodes) {
		update(nodes)
		return nil
	}

	var wg sync.WaitGroup

	for start := 0; start < len(nodes); start += chunk {
		part := nodes[start:min(start+chunk, len(nodes))]

		wg.Go(func() { update(part) })
	}

	wg.Wait()

	return nil
}

// DiskMapStats holds lightweight metrics for a disk-backed map.
type DiskMapStats struct {
	Entries          int64
	IndexMemBytes    int64 // estimated RAM held by the index entries
	DiskBytesWritten int64 // payload bytes written over the map's life
}

// Stats returns current metrics. It may be called concurrently with writes
// (e.g. reportDiskMapStats during moveForwardBlock), so the counters are
// atomic.
func (m *DiskTxMap) Stats() DiskMapStats {
	entries := int64(m.Length())

	return DiskMapStats{
		Entries:          entries,
		IndexMemBytes:    entries * indexBytesPerEntry,
		DiskBytesWritten: m.bytesWritten.Load(),
	}
}
