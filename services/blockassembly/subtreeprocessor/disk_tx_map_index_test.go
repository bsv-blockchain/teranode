package subtreeprocessor

import (
	"fmt"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// failingLogFile stands in for a payload log segment's file. It fails the
// next failWrites writes and every read while readErr is set, delegating to
// the real file otherwise.
type failingLogFile struct {
	mu         sync.Mutex
	real       logFile
	failWrites int
	readErr    error
}

func (f *failingLogFile) WriteAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	if f.failWrites > 0 {
		f.failWrites--
		f.mu.Unlock()

		return 0, errors.NewStorageError("write failed")
	}
	f.mu.Unlock()

	return f.real.WriteAt(p, off)
}

func (f *failingLogFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	err := f.readErr
	f.mu.Unlock()

	if err != nil {
		return 0, err
	}

	return f.real.ReadAt(p, off)
}

func (f *failingLogFile) Close() error { return f.real.Close() }

// failDiskTxMapLogs wraps every payload log segment of m in a failingLogFile
// and returns the wrappers.
func failDiskTxMapLogs(m *DiskTxMap, failWrites int, readErr error) []*failingLogFile {
	g := m.gen.Load()
	wrappers := make([]*failingLogFile, len(g.logs))

	for i := range g.logs {
		l := &g.logs[i]
		l.mu.Lock()
		wrappers[i] = &failingLogFile{real: l.file, failWrites: failWrites, readErr: readErr}
		l.file = wrappers[i]

		if readErr != nil {
			// read through the file, where the failure is injected
			l.windows = nil
			l.fd = -1
		}
		l.mu.Unlock()
	}

	return wrappers
}

// Dedup is answered by the in-RAM index alone: a known hash is refused even
// when the payload log can't be read, and nothing is recorded.
func TestDiskTxMap_SetIfNotExistsNeverReadsPayload(t *testing.T) {
	m := newErrTestDiskTxMap(t)
	defer m.Close()

	hash := batchTestHash(1)
	inp := subtreepkg.TxInpoints{}

	_, wasSet := m.SetIfNotExists(hash, &inp)
	require.True(t, wasSet)
	require.NoError(t, m.Flush())

	failDiskTxMapLogs(m, 0, io.ErrUnexpectedEOF)

	_, wasSet = m.SetIfNotExists(hash, &inp)
	require.False(t, wasSet, "a known hash is a duplicate, whatever the payload log says")
	require.Equal(t, 1, m.Length())
	require.True(t, m.Exists(hash))
	require.NoError(t, m.TakeErr(), "dedup must not touch the payload log")
}

// The subtree index lives in the RAM index: updating it never reads or
// rewrites the payload, so it works with the log unreadable.
func TestDiskTxMap_UpdateSubtreeIndexBatchIsRAMOnly(t *testing.T) {
	m := newErrTestDiskTxMap(t)
	defer m.Close()

	nodes := make([]subtreepkg.Node, 0, 100)

	for i := 0; i < 100; i++ {
		inp := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{batchTestHash(i + 1000)}, []uint32{1, uint32(i)})
		m.Set(batchTestHash(i), &inp)
		nodes = append(nodes, subtreepkg.Node{Hash: batchTestHash(i)})
	}

	require.NoError(t, m.Flush())

	wrappers := failDiskTxMapLogs(m, 0, io.ErrUnexpectedEOF)

	require.NoError(t, m.UpdateSubtreeIndexBatch(nodes, 9))
	require.NoError(t, m.UpdateSubtreeIndex(nodes[0].Hash, 11))
	require.NoError(t, m.TakeErr())

	for _, w := range wrappers {
		w.readErr = nil
	}

	for i, node := range nodes {
		got, ok := m.Get(node.Hash)
		require.True(t, ok)

		want := int16(9)
		if i == 0 {
			want = 11
		}

		require.Equal(t, want, got.SubtreeIndex, "subtree index of %d", i)
		require.Equal(t, []chainhash.Hash{batchTestHash(i + 1000)}, got.GetParentTxHashes(), "inpoints of %d", i)
	}
}

// A payload whose write failed must read back as an error, never as an empty
// TxInpoints: an empty parent list would silently drop the tx's parents.
func TestDiskTxMap_LostWriteReadsAsError(t *testing.T) {
	m := newErrTestDiskTxMap(t)
	defer m.Close()

	failDiskTxMapLogs(m, 1, nil)

	lost := batchTestHash(1)
	inp := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{batchTestHash(2)}, []uint32{1, 0})
	m.Set(lost, &inp)
	require.NoError(t, m.Flush())
	require.Error(t, m.TakeErr(), "the failed write is reported")

	// A later write to the same segment succeeds and lands after the lost one.
	kept := batchTestHash(3)
	for i := 4; m.gen.Load().logOf(kept) != m.gen.Load().logOf(lost); i++ {
		kept = batchTestHash(i)
	}

	m.Set(kept, &inp)
	require.NoError(t, m.Flush())
	require.NoError(t, m.TakeErr(), "the log recovers after a failed write")

	got, ok := m.Get(kept)
	require.True(t, ok)
	require.Equal(t, []chainhash.Hash{batchTestHash(2)}, got.GetParentTxHashes())

	_, ok, err := m.GetWithErr(lost)
	require.False(t, ok)
	require.Error(t, err, "the lost payload must be an error, not an empty TxInpoints")
}

// Reads are served from the unwritten buffer and from the file alike, for
// payloads of every size, including ones larger than a single read.
func TestDiskTxMap_GetAcrossBufferAndFile(t *testing.T) {
	m := newErrTestDiskTxMap(t)
	defer m.Close()

	const n = 3000

	want := make([]subtreepkg.TxInpoints, n)

	for i := 0; i < n; i++ {
		parents := make([]chainhash.Hash, i%40+1) // up to ~1.5KB per payload
		vouts := make([]uint32, 0, 2*len(parents))

		for p := range parents {
			parents[p] = batchTestHash(i*100 + p)
			vouts = append(vouts, 1, uint32(p))
		}

		want[i] = subtreepkg.NewTxInpointsFromPacked(parents, vouts)
		m.Set(batchTestHash(i), &want[i])

		if i == n/2 {
			require.NoError(t, m.Flush(), "half on disk, half still buffered")
		}
	}

	for i := 0; i < n; i++ {
		got, ok := m.Get(batchTestHash(i))
		require.True(t, ok, "entry %d", i)
		require.Equal(t, want[i].GetParentTxHashes(), got.GetParentTxHashes(), "entry %d", i)
		require.Equal(t, want[i].GetTxInpoints(), got.GetTxInpoints(), "entry %d", i)
	}

	require.NoError(t, m.TakeErr())
}

// Set overwrites the payload; Delete forgets the hash.
func TestDiskTxMap_SetOverwritesAndDeleteForgets(t *testing.T) {
	m := newErrTestDiskTxMap(t)
	defer m.Close()

	hash := batchTestHash(1)
	first := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{batchTestHash(10)}, []uint32{1, 0})
	second := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{batchTestHash(20)}, []uint32{1, 3})

	m.Set(hash, &first)
	m.Set(hash, &second)
	require.Equal(t, 1, m.Length())

	got, ok := m.Get(hash)
	require.True(t, ok)
	require.Equal(t, []chainhash.Hash{batchTestHash(20)}, got.GetParentTxHashes())

	require.True(t, m.Delete(hash))
	require.False(t, m.Exists(hash))
	require.Equal(t, 0, m.Length())
	require.False(t, m.Delete(hash), "deleting an unknown hash reports false")

	_, wasSet := m.SetIfNotExists(hash, &first)
	require.True(t, wasSet, "a deleted hash can be added again")
}

// Concurrent moves of the same hashes insert each exactly once, and every
// entry reads back its own payload, whichever mover won.
func TestDiskTxMap_MoveFromConcurrentSameHashes(t *testing.T) {
	src := newErrTestDiskTxMap(t)
	defer src.Close()

	dst := newErrTestDiskTxMap(t)
	defer dst.Close()

	const n = 5000

	for i := 0; i < n; i++ {
		inp := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{batchTestHash(i + n)}, []uint32{1, uint32(i)})
		src.Set(batchTestHash(i), &inp)
	}

	var (
		wg       sync.WaitGroup
		inserted = make([]int32, n)
		mu       sync.Mutex
	)

	hashes := make([]chainhash.Hash, n)
	for i := range hashes {
		hashes[i] = batchTestHash(i)
	}

	for w := 0; w < 8; w++ {
		wg.Go(func() {
			moved := make([]bool, n)

			missing, err := dst.MoveFrom(src, hashes, moved)
			require.NoError(t, err)
			require.Equal(t, -1, missing)

			mu.Lock()
			for i, ok := range moved {
				if ok {
					inserted[i]++
				}
			}
			mu.Unlock()
		})
	}

	wg.Wait()

	require.Equal(t, n, dst.Length())

	for i := 0; i < n; i++ {
		require.Equal(t, int32(1), inserted[i], "hash %d inserted once", i)

		got, ok := dst.Get(batchTestHash(i))
		require.True(t, ok)
		require.Equal(t, []chainhash.Hash{batchTestHash(i + n)}, got.GetParentTxHashes(), "entry %d", i)
		require.Equal(t, []subtreepkg.Inpoint{{Hash: batchTestHash(i + n), Index: uint32(i)}}, got.GetTxInpoints(), "entry %d", i)
	}

	require.NoError(t, dst.TakeErr())
}

// blockingLogFile holds every write until release is closed.
type blockingLogFile struct {
	logFile
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *blockingLogFile) WriteAt(p []byte, off int64) (int, error) {
	f.once.Do(func() { close(f.started) })
	<-f.release

	return f.logFile.WriteAt(p, off)
}

// A segment write happens outside the segment lock: while it is in progress,
// appends to the same segment don't wait for it, and the records being
// written stay readable.
func TestDiskTxMap_AppendAndReadDuringSegmentWrite(t *testing.T) {
	m := newErrTestDiskTxMap(t)
	defer m.Close()

	// Every hash below goes to segment 0.
	var hashes []chainhash.Hash
	for i := 0; len(hashes) < 30_000; i++ {
		if h := batchTestHash(i); m.gen.Load().logOf(h) == 0 {
			hashes = append(hashes, h)
		}
	}

	l := &m.gen.Load().logs[0]
	blocking := &blockingLogFile{logFile: l.file, started: make(chan struct{}), release: make(chan struct{})}

	l.mu.Lock()
	l.file = blocking
	l.mu.Unlock()

	inp := func(i int) *subtreepkg.TxInpoints {
		in := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{batchTestHash(i + 1_000_000)}, []uint32{1, uint32(i)})
		return &in
	}

	// Fill segment 0 past logBufferSize; the append that crosses it starts
	// the write, which blocks.
	next := 0

	go func() {
		for ; next < len(hashes); next++ {
			m.Set(hashes[next], inp(next))

			select {
			case <-blocking.started:
				next++
				return
			default:
			}
		}
	}()

	select {
	case <-blocking.started:
	case <-time.After(10 * time.Second):
		t.Fatal("no segment write started")
	}

	done := make(chan struct{})

	go func() {
		defer close(done)

		// Records in the buffer being written.
		got, ok := m.Get(hashes[0])
		require.True(t, ok)
		require.Equal(t, []chainhash.Hash{batchTestHash(1_000_000)}, got.GetParentTxHashes())

		// Appends to the same segment.
		extra := batchTestHash(-1)
		for m.gen.Load().logOf(extra) != 0 {
			extra[0]++
		}

		m.Set(extra, inp(7))

		got, ok = m.Get(extra)
		require.True(t, ok)
		require.Equal(t, []chainhash.Hash{batchTestHash(1_000_007)}, got.GetParentTxHashes())
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		close(blocking.release)
		t.Fatal("an append or read waited for the segment write")
	}

	close(blocking.release)
	require.NoError(t, m.Flush())

	got, ok := m.Get(hashes[0])
	require.True(t, ok)
	require.Equal(t, []chainhash.Hash{batchTestHash(1_000_000)}, got.GetParentTxHashes())
	require.NoError(t, m.TakeErr())
}

// Reads racing Clear and Close (the async subtree meta storer reads a half
// while the processor rotates it) must never fault or return another
// generation's payload: each read is either the right entry, not found, or
// an error.
func TestDiskTxMap_ReadsDuringClearAndClose(t *testing.T) {
	m, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{t.TempDir()}})
	require.NoError(t, err)

	const n = 2000

	fill := func(round int) {
		for i := 0; i < n; i++ {
			inp := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{batchTestHash(round*n + i + 1_000_000)}, []uint32{1, 0})
			m.Set(batchTestHash(round*n+i), &inp)
		}

		_ = m.Flush()
	}

	fill(0)

	stop := make(chan struct{})

	var wg sync.WaitGroup

	for w := 0; w < 8; w++ {
		wg.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}

				key := i % (4 * n) // spans every round's hashes
				got, found, getErr := m.GetWithErr(batchTestHash(key))

				if getErr == nil && found {
					require.Equal(t, []chainhash.Hash{batchTestHash(key + 1_000_000)}, got.GetParentTxHashes(), "hash %d read another generation's payload", key)
				}
			}
		})
	}

	for round := 1; round < 4; round++ {
		m.Clear()
		fill(round)
	}

	require.NoError(t, m.Close())

	close(stop)
	wg.Wait()
}

// While a segment write is stuck, appends to that segment stop once
// logMaxBuffered is buffered instead of growing the buffer without bound.
func TestDiskTxMap_AppendBackpressure(t *testing.T) {
	m := newErrTestDiskTxMap(t)
	defer m.Close()

	g := m.gen.Load()
	l := &g.logs[0]
	blocking := &blockingLogFile{logFile: l.file, started: make(chan struct{}), release: make(chan struct{})}

	l.mu.Lock()
	l.file = blocking
	l.mu.Unlock()

	payload := make([]byte, 1024)
	payload[0] = 1 // any non-empty payload

	var hash chainhash.Hash // segment 0: shardOf(hash) % len(logs) == 0

	appended := make(chan int, 1)

	go func() {
		n := 0
		for ; n < 4*logMaxBuffered/len(payload); n++ {
			m.appendPayload(g, hash, payload)
		}

		appended <- n
	}()

	<-blocking.started

	// Give the appender time to hit the bound.
	time.Sleep(300 * time.Millisecond)

	l.mu.Lock()
	buffered := len(l.buf)
	l.mu.Unlock()

	require.LessOrEqual(t, buffered, logMaxBuffered+len(payload)+recordHeaderSize, "buf must stop growing at the bound")

	select {
	case <-appended:
		t.Fatal("appends must wait for the stuck write")
	default:
	}

	close(blocking.release)

	select {
	case <-appended:
	case <-time.After(10 * time.Second):
		t.Fatal("appends did not resume after the write finished")
	}

	require.NoError(t, m.TakeErr())
}

// partialLogFile writes half of the next failing write and then fails it,
// like a disk filling up mid-write.
type partialLogFile struct {
	logFile
	failNext bool
}

func (f *partialLogFile) WriteAt(p []byte, off int64) (int, error) {
	if f.failNext {
		f.failNext = false
		n, _ := f.logFile.WriteAt(p[:len(p)/2], off)

		return n, errors.NewStorageError("no space left")
	}

	return f.logFile.WriteAt(p, off)
}

// Records in a partly written buffer read back as errors, even after a later
// write makes those bytes part of the file.
func TestDiskTxMap_PartialWriteReadsAsError(t *testing.T) {
	m := newErrTestDiskTxMap(t)
	defer m.Close()

	g := m.gen.Load()

	var hashes []chainhash.Hash
	for i := 0; len(hashes) < 3; i++ {
		if h := batchTestHash(i); g.logOf(h) == 0 {
			hashes = append(hashes, h)
		}
	}

	l := &g.logs[0]
	partial := &partialLogFile{logFile: l.file, failNext: true}

	l.mu.Lock()
	l.file = partial
	l.mu.Unlock()

	inp := func(i int) *subtreepkg.TxInpoints {
		in := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{batchTestHash(i + 500)}, []uint32{1, 0})
		return &in
	}

	// Two records in the failing buffer: the first lands in the written half,
	// the second straddles the cut.
	m.Set(hashes[0], inp(0))
	m.Set(hashes[1], inp(1))
	require.NoError(t, m.Flush())
	require.Error(t, m.TakeErr())

	m.Set(hashes[2], inp(2))
	require.NoError(t, m.Flush())
	require.NoError(t, m.TakeErr())

	for _, h := range hashes[:2] {
		_, ok, err := m.GetWithErr(h)
		require.False(t, ok)
		require.Error(t, err, "a record from a failed write must not decode")
	}

	got, ok := m.Get(hashes[2])
	require.True(t, ok)
	require.Equal(t, []chainhash.Hash{batchTestHash(502)}, got.GetParentTxHashes())
}

// Bytes written keep counting across Clear, and the index memory gauge drops
// with the entries: Clear releases the index instead of keeping its capacity.
func TestDiskTxMap_StatsSurviveClear(t *testing.T) {
	m := newErrTestDiskTxMap(t)
	defer m.Close()

	for i := 0; i < 1000; i++ {
		inp := subtreepkg.TxInpoints{}
		m.Set(batchTestHash(i), &inp)
	}

	require.NoError(t, m.Flush())

	before := m.Stats()
	require.Positive(t, before.DiskBytesWritten)
	require.Equal(t, int64(1000*indexBytesPerEntry), before.IndexMemBytes)

	m.Clear()

	after := m.Stats()
	require.Equal(t, int64(0), after.Entries)
	require.Equal(t, before.DiskBytesWritten, after.DiskBytesWritten, "bytes written must not reset on Clear")
	require.Zero(t, after.IndexMemBytes, "a cleared index holds no entries and no retained capacity")
}

// Clear must give the index memory back. The retired half of the subtree
// processor's double buffer sits empty for a whole block interval; holding the
// high-water capacity there doubled the index RAM of a busy node.
func TestDiskTxMap_ClearReleasesIndexMemory(t *testing.T) {
	m := newErrTestDiskTxMap(t)
	defer m.Close()

	const n = 1 << 20

	for _, h := range genBenchHashes(n, 7) {
		s := &m.shards[shardOf(h)]
		s.index[h] = packEntry(0, 0)
	}

	heapInuse := func() uint64 {
		runtime.GC()
		runtime.GC()

		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)

		return ms.HeapInuse
	}

	before := heapInuse()

	m.Clear()

	after := heapInuse()

	require.Zero(t, m.Length())
	// 1M entries take ~40-90 MB of map; require at least 32 MB back.
	require.Greaterf(t, int64(before)-int64(after), int64(32<<20),
		"Clear released only %d bytes of heap", int64(before)-int64(after))
}

// MoveFrom moves a whole set of hashes like one Get + SetIfNotExists per hash:
// payloads and SubtreeIndex arrive intact whether the source record is
// buffered or written, hashes already in dst are reported as not set, and
// every set hash is inserted exactly once.
func TestDiskTxMap_MoveFrom(t *testing.T) {
	for _, dirs := range []int{1, 2} {
		t.Run(fmt.Sprintf("%ddir", dirs), func(t *testing.T) {
			paths := func() []string {
				p := make([]string, dirs)
				for i := range p {
					p[i] = t.TempDir()
				}

				return p
			}

			src, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: paths()})
			require.NoError(t, err)

			defer src.Close()

			dst, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: paths()})
			require.NoError(t, err)

			defer dst.Close()

			const n = 20_000

			hashes := make([]chainhash.Hash, n)

			for i := 0; i < n; i++ {
				hashes[i] = batchTestHash(i)
				inp := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{batchTestHash(i + n)}, []uint32{1, uint32(i)})
				inp.SubtreeIndex = int16(i % 7)
				src.Set(hashes[i], &inp)

				if i == n/2 {
					require.NoError(t, src.Flush(), "half written, half still buffered")
				}
			}

			// Already in dst: must come back as not set, keeping dst's own entry.
			own := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{batchTestHash(99_999)}, []uint32{1, 0})
			dst.Set(hashes[3], &own)

			wasSet := make([]bool, n)
			missing, err := dst.MoveFrom(src, hashes, wasSet)
			require.NoError(t, err)
			require.Equal(t, -1, missing)

			for i := 0; i < n; i++ {
				require.Equal(t, i != 3, wasSet[i], "wasSet of %d", i)

				got, ok := dst.Get(hashes[i])
				require.True(t, ok, "entry %d", i)

				if i == 3 {
					require.Equal(t, []chainhash.Hash{batchTestHash(99_999)}, got.GetParentTxHashes())
					continue
				}

				require.Equal(t, int16(i%7), got.SubtreeIndex, "entry %d", i)
				require.Equal(t, []subtreepkg.Inpoint{{Hash: batchTestHash(i + n), Index: uint32(i)}}, got.GetTxInpoints(), "entry %d", i)
			}

			require.Equal(t, n, dst.Length())
			require.NoError(t, src.TakeErr())
			require.NoError(t, dst.TakeErr())
		})
	}
}

// A hash src doesn't have stops the move and is reported by its position.
func TestDiskTxMap_MoveFromMissing(t *testing.T) {
	src := newErrTestDiskTxMap(t)
	defer src.Close()

	dst := newErrTestDiskTxMap(t)
	defer dst.Close()

	inp := subtreepkg.TxInpoints{}
	src.Set(batchTestHash(1), &inp)

	hashes := []chainhash.Hash{batchTestHash(1), batchTestHash(2)}
	missing, err := dst.MoveFrom(src, hashes, make([]bool, 2))
	require.NoError(t, err)
	require.Equal(t, 1, missing)
	require.False(t, dst.Exists(batchTestHash(1)), "nothing is inserted on a miss")
}

// A source read error is recorded on src and reported like a missing hash.
func TestDiskTxMap_MoveFromReadError(t *testing.T) {
	src := newErrTestDiskTxMap(t)
	defer src.Close()

	dst := newErrTestDiskTxMap(t)
	defer dst.Close()

	inp := subtreepkg.TxInpoints{}
	src.Set(batchTestHash(1), &inp)
	require.NoError(t, src.Flush())

	failDiskTxMapLogs(src, 0, errors.NewStorageError("read failed"))

	missing, err := dst.MoveFrom(src, []chainhash.Hash{batchTestHash(1)}, make([]bool, 1))
	require.Error(t, err, "the read error is returned")
	require.Equal(t, 0, missing)
	require.Error(t, src.TakeErr(), "and recorded on src")
	require.False(t, dst.Exists(batchTestHash(1)))
}

// Maps created back to back on the same prefix and path (several subtree
// processors sharing txMapDirs in one process) must never share, and so
// truncate, each other's files, even when the clock gives them the same
// timestamp.
func TestDiskTxMap_GenerationsNeverShareDirs(t *testing.T) {
	dir := t.TempDir()

	// Every generation gets the same timestamp.
	fixed := time.Unix(1_800_000_000, 0)
	generationClock = func() time.Time { return fixed }

	t.Cleanup(func() { generationClock = time.Now })

	const n = 64

	maps := make([]*DiskTxMap, n)

	for i := range maps {
		m, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{dir}, Prefix: "same"})
		require.NoError(t, err)

		t.Cleanup(func() { _ = m.Close() })

		inp := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{batchTestHash(i + 1000)}, []uint32{1, 0})
		m.Set(batchTestHash(0), &inp)
		require.NoError(t, m.Flush())

		maps[i] = m
	}

	seen := make(map[string]bool)

	for i, m := range maps {
		for _, d := range m.gen.Load().dirs {
			require.False(t, seen[d], "map %d reuses directory %s", i, d)
			seen[d] = true
		}

		got, ok := m.Get(batchTestHash(0))
		require.True(t, ok, "map %d lost its entry", i)
		require.Equal(t, []chainhash.Hash{batchTestHash(i + 1000)}, got.GetParentTxHashes(), "map %d reads another map's payload", i)
	}
}

// A hash already in dst is a duplicate without reading src, so an unreadable
// source record can't fail the move for it.
func TestDiskTxMap_MoveFromSkipsDuplicatesWithoutReading(t *testing.T) {
	src := newErrTestDiskTxMap(t)
	defer src.Close()

	dst := newErrTestDiskTxMap(t)
	defer dst.Close()

	inp := subtreepkg.TxInpoints{}
	src.Set(batchTestHash(1), &inp)
	require.NoError(t, src.Flush())
	dst.Set(batchTestHash(1), &inp)

	failDiskTxMapLogs(src, 0, errors.NewStorageError("read failed"))

	moved := make([]bool, 1)
	missing, err := dst.MoveFrom(src, []chainhash.Hash{batchTestHash(1)}, moved)
	require.NoError(t, err)
	require.Equal(t, -1, missing)
	require.False(t, moved[0])
	require.NoError(t, src.TakeErr(), "src was never read")
}
