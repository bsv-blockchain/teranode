// Package unminedsort orders the unmined transactions block assembly reloads at
// startup by CreatedAt, spilling sorted runs to disk once an in-memory buffer
// fills, so the reload's memory is bounded by the buffer rather than by the
// size of the unmined set.
package unminedsort

import (
	"bufio"
	"cmp"
	"container/heap"
	"context"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
)

const (
	// runDirPrefix names the per-sorter directory holding its run files. New
	// removes any leftover directory with this prefix from an earlier crash.
	runDirPrefix = "unmined-sort-"

	// ioBufferSize is the bufio size per run file, for writing and merging.
	ioBufferSize = 1 << 20

	// minChunk is the smallest slice of the buffer worth sorting on its own
	// goroutine.
	minChunk = 64 * 1024
)

// Options configures a Sorter.
type Options struct {
	// Dirs are where sorted runs are spilled, round-robin, so capacity and I/O
	// bandwidth scale with the number of local disks. Empty means never spill:
	// the whole set is sorted in memory.
	Dirs []string

	// BufferRecords is the number of buffered records that triggers a spill.
	BufferRecords int

	// WithInpoints carries each transaction's TxInpoints through the sort.
	// When false, every drained transaction gets empty inpoints.
	WithInpoints bool

	// SortWorkers bounds the goroutines sorting a buffer; 0 means GOMAXPROCS.
	SortWorkers int
}

// record is one buffered transaction. seq is its position in the buffer at
// insertion, which makes (createdAt, seq) a total order that reproduces a
// stable sort, and indexes the inpoints arena offsets.
type record struct {
	createdAt int64
	node      subtreepkg.Node
	seq       uint32
}

// batchBuf is one buffer of records. arena holds serialized inpoints; the
// inpoints of the record with seq i are arena[offs[i]:offs[i+1]]. arena and
// offs are only used WithInpoints.
type batchBuf struct {
	recs  []record
	arena []byte
	offs  []uint64
}

func (b *batchBuf) reset(withInpoints bool) {
	b.recs = b.recs[:0]
	b.arena = b.arena[:0]

	if withInpoints {
		b.offs = append(b.offs[:0], 0)
	}
}

func (b *batchBuf) inpoints(seq uint32) []byte {
	return b.arena[b.offs[seq]:b.offs[seq+1]]
}

// Sorter accumulates transactions with Add and emits them with Drain in
// CreatedAt order, ties broken by insertion order. It is not safe for
// concurrent use.
//
// A full buffer is sorted and written in the background while Add fills a
// second buffer, so the sorter holds up to two buffers of BufferRecords.
type Sorter struct {
	opts Options
	dirs []string

	cur   *batchBuf
	spare *batchBuf

	// spillDone is closed when the in-flight spill finishes; spillErr and the
	// returned spare buffer are only read after receiving from it.
	spillDone chan struct{}
	spillErr  error

	runs         []string
	spilledCount int
	drained      bool
}

// New creates a Sorter. With a spill directory it first removes run
// directories left behind by an earlier process.
func New(opts Options) (*Sorter, error) {
	if opts.BufferRecords <= 0 {
		return nil, errors.NewInvalidArgumentError("unminedsort: BufferRecords must be positive, got %d", opts.BufferRecords)
	}

	if opts.SortWorkers <= 0 {
		opts.SortWorkers = runtime.GOMAXPROCS(0)
	}

	s := &Sorter{opts: opts, cur: &batchBuf{}}

	bases, err := uniqueDirs(opts.Dirs)
	if err != nil {
		return nil, err
	}

	// Sweep every directory before creating any run directory, so a sweep can
	// never remove a run directory this sorter just made.
	for _, base := range bases {
		if err := sweepStaleRunDirs(base); err != nil {
			return nil, err
		}
	}

	for _, base := range bases {
		dir, err := os.MkdirTemp(base, runDirPrefix)
		if err != nil {
			_ = s.Close()
			return nil, errors.NewStorageError("unminedsort: creating run directory in %s", base, err)
		}

		s.dirs = append(s.dirs, dir)
	}

	s.cur.reset(opts.WithInpoints)

	return s, nil
}

// uniqueDirs creates the spill directories and returns them resolved, with
// empty entries dropped and duplicates (the same directory spelled
// differently, or reached through a symlink) removed.
func uniqueDirs(dirs []string) ([]string, error) {
	seen := make(map[string]struct{}, len(dirs))
	out := make([]string, 0, len(dirs))

	for _, d := range dirs {
		d = strings.TrimSpace(d)
		if d == "" {
			continue
		}

		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, errors.NewStorageError("unminedsort: creating %s", d, err)
		}

		resolved, err := filepath.EvalSymlinks(d)
		if err != nil {
			return nil, errors.NewStorageError("unminedsort: resolving %s", d, err)
		}

		if resolved, err = filepath.Abs(resolved); err != nil {
			return nil, errors.NewStorageError("unminedsort: resolving %s", d, err)
		}

		if _, dup := seen[resolved]; dup {
			continue
		}

		seen[resolved] = struct{}{}
		out = append(out, resolved)
	}

	return out, nil
}

func sweepStaleRunDirs(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return errors.NewStorageError("unminedsort: reading %s", dir, err)
	}

	for _, e := range entries {
		if !e.IsDir() || !strings.HasPrefix(e.Name(), runDirPrefix) {
			continue
		}

		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return errors.NewStorageError("unminedsort: removing stale run directory %s", e.Name(), err)
		}
	}

	return nil
}

// Len returns the number of transactions added.
func (s *Sorter) Len() int {
	return len(s.cur.recs) + s.spilledCount
}

// buffered returns the number of records in the buffer being filled.
func (s *Sorter) buffered() int {
	return len(s.cur.recs)
}

// Add buffers one transaction. inpoints is ignored unless the sorter was
// created WithInpoints; nil means no inpoints.
func (s *Sorter) Add(createdAt int64, node subtreepkg.Node, inpoints *subtreepkg.TxInpoints) error {
	if s.drained {
		return errors.NewProcessingError("unminedsort: Add after Drain")
	}

	b := s.cur

	if len(b.recs) >= 1<<32-1 {
		return errors.NewProcessingError("unminedsort: buffer exceeds %d records; set a spill directory or lower BufferRecords", 1<<32-1)
	}

	b.recs = append(b.recs, record{createdAt: createdAt, node: node, seq: uint32(len(b.recs))}) //nolint:gosec // bounded above

	if s.opts.WithInpoints {
		if inpoints != nil {
			ser, err := inpoints.Serialize()
			if err != nil {
				return errors.NewProcessingError("unminedsort: serializing inpoints for %s", node.Hash, err)
			}

			b.arena = append(b.arena, ser...)
		}

		b.offs = append(b.offs, uint64(len(b.arena)))
	}

	if len(s.dirs) > 0 && len(b.recs) >= s.opts.BufferRecords {
		return s.startSpill()
	}

	return nil
}

// startSpill hands the full buffer to a background writer and continues on the
// spare buffer. At most one spill is in flight; a second waits for the first.
func (s *Sorter) startSpill() error {
	if err := s.waitSpill(); err != nil {
		return err
	}

	job := s.cur
	path := filepath.Join(s.dirs[len(s.runs)%len(s.dirs)], strconv.Itoa(len(s.runs))+".run")

	s.runs = append(s.runs, path)
	s.spilledCount += len(job.recs)

	if s.spare == nil {
		s.spare = &batchBuf{}
	}

	s.cur, s.spare = s.spare, nil
	s.cur.reset(s.opts.WithInpoints)

	done := make(chan struct{})
	s.spillDone = done

	go func() {
		defer close(done)

		s.spillErr = s.writeRun(job, path)
		s.spare = job
	}()

	return nil
}

// waitSpill waits for the in-flight spill, if any, and returns its error.
func (s *Sorter) waitSpill() error {
	if s.spillDone == nil {
		return nil
	}

	<-s.spillDone
	s.spillDone = nil

	return s.spillErr
}

// sortedChunks sorts the buffer in parallel as contiguous chunks and returns
// them. Each chunk is ordered by (createdAt, seq); seq is unique, so merging
// the chunks by the same key yields the stable order of the whole buffer.
func (s *Sorter) sortedChunks(b *batchBuf) [][]record {
	n := len(b.recs)
	if n == 0 {
		return nil
	}

	workers := s.opts.SortWorkers
	if maxWorkers := (n + minChunk - 1) / minChunk; workers > maxWorkers {
		workers = maxWorkers
	}

	chunkSize := (n + workers - 1) / workers
	chunks := make([][]record, 0, workers)

	for start := 0; start < n; start += chunkSize {
		chunks = append(chunks, b.recs[start:min(start+chunkSize, n)])
	}

	var wg sync.WaitGroup

	for _, c := range chunks {
		wg.Add(1)

		go func(c []record) {
			defer wg.Done()

			slices.SortFunc(c, compareRecords)
		}(c)
	}

	wg.Wait()

	return chunks
}

func compareRecords(a, b record) int {
	if c := cmp.Compare(a.createdAt, b.createdAt); c != 0 {
		return c
	}

	return cmp.Compare(a.seq, b.seq)
}

// writeRun sorts a buffer and writes it to path as one sorted run.
func (s *Sorter) writeRun(b *batchBuf, path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return errors.NewStorageError("unminedsort: creating run %s", path, err)
	}

	w := bufio.NewWriterSize(f, ioBufferSize)
	m := s.newMemMerge(s.sortedChunks(b))

	var (
		prev    int64
		scratch []byte
	)

	for {
		cur, ok := m.pop()
		if !ok {
			break
		}

		rec := &cur.chunk[cur.pos-1]
		scratch = binary.AppendVarint(scratch[:0], rec.createdAt-prev)
		scratch = append(scratch, rec.node.Hash[:]...)
		scratch = binary.AppendUvarint(scratch, rec.node.Fee)
		scratch = binary.AppendUvarint(scratch, rec.node.SizeInBytes)

		if s.opts.WithInpoints {
			inp := b.inpoints(rec.seq)
			scratch = binary.AppendUvarint(scratch, uint64(len(inp)))
			scratch = append(scratch, inp...)
		}

		prev = rec.createdAt

		if _, err = w.Write(scratch); err != nil {
			_ = f.Close()
			return errors.NewStorageError("unminedsort: writing run %s", path, err)
		}
	}

	if err = w.Flush(); err != nil {
		_ = f.Close()
		return errors.NewStorageError("unminedsort: flushing run %s", path, err)
	}

	if err = f.Close(); err != nil {
		return errors.NewStorageError("unminedsort: closing run %s", path, err)
	}

	return nil
}

// memCursor walks one sorted in-memory chunk. pos is the index of the next
// record; after pop, the popped record is chunk[pos-1].
type memCursor struct {
	chunk []record
	pos   int
}

// memMerge merges sorted chunks of one buffer by (createdAt, seq).
type memMerge []*memCursor

func (m memMerge) Len() int { return len(m) }
func (m memMerge) Less(i, j int) bool {
	return compareRecords(m[i].chunk[m[i].pos], m[j].chunk[m[j].pos]) < 0
}
func (m memMerge) Swap(i, j int) { m[i], m[j] = m[j], m[i] }
func (m *memMerge) Push(x any)   { *m = append(*m, x.(*memCursor)) }
func (m *memMerge) Pop() any {
	old := *m
	c := old[len(old)-1]
	*m = old[:len(old)-1]

	return c
}

func (s *Sorter) newMemMerge(chunks [][]record) *memMerge {
	m := make(memMerge, 0, len(chunks))
	for _, c := range chunks {
		if len(c) > 0 {
			m = append(m, &memCursor{chunk: c})
		}
	}

	heap.Init(&m)

	return &m
}

// pop advances the smallest cursor and returns it; the popped record is
// cur.chunk[cur.pos-1].
func (m *memMerge) pop() (*memCursor, bool) {
	if len(*m) == 0 {
		return nil, false
	}

	cur := (*m)[0]
	cur.pos++

	if cur.pos == len(cur.chunk) {
		heap.Pop(m)
	} else {
		heap.Fix(m, 0)
	}

	return cur, true
}

// source yields the records of one sorted run, from disk or from memory.
type source interface {
	// next loads the next record into the source's current entry; false at end.
	next() (bool, error)
	current() *entry
	close() error
}

// entry is a decoded record ready to emit.
type entry struct {
	createdAt int64
	node      subtreepkg.Node
	inpoints  []byte
}

type fileSource struct {
	f     *os.File
	r     *bufio.Reader
	cur   entry
	prev  int64
	inp   []byte
	withI bool
}

func openFileSource(path string, withInpoints bool) (*fileSource, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.NewStorageError("unminedsort: opening run %s", path, err)
	}

	return &fileSource{f: f, r: bufio.NewReaderSize(f, ioBufferSize), withI: withInpoints}, nil
}

func (fs *fileSource) next() (bool, error) {
	delta, err := binary.ReadVarint(fs.r)
	if err == io.EOF {
		return false, nil
	}

	if err != nil {
		return false, fs.readErr(err)
	}

	fs.cur.createdAt = fs.prev + delta
	fs.prev = fs.cur.createdAt

	if _, err = io.ReadFull(fs.r, fs.cur.node.Hash[:]); err != nil {
		return false, fs.readErr(err)
	}

	if fs.cur.node.Fee, err = binary.ReadUvarint(fs.r); err != nil {
		return false, fs.readErr(err)
	}

	if fs.cur.node.SizeInBytes, err = binary.ReadUvarint(fs.r); err != nil {
		return false, fs.readErr(err)
	}

	fs.cur.inpoints = nil

	if fs.withI {
		n, err := binary.ReadUvarint(fs.r)
		if err != nil {
			return false, fs.readErr(err)
		}

		if n > 0 {
			fs.inp = slices.Grow(fs.inp[:0], int(n))[:n] //nolint:gosec // length written by this package
			if _, err = io.ReadFull(fs.r, fs.inp); err != nil {
				return false, fs.readErr(err)
			}

			fs.cur.inpoints = fs.inp
		}
	}

	return true, nil
}

func (fs *fileSource) readErr(err error) error {
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}

	return errors.NewStorageError("unminedsort: reading run %s", fs.f.Name(), err)
}

func (fs *fileSource) current() *entry { return &fs.cur }
func (fs *fileSource) close() error    { return fs.f.Close() }

// memSource yields the in-memory tail buffer, merged across its sorted chunks.
type memSource struct {
	b     *batchBuf
	m     *memMerge
	withI bool
	cur   entry
}

func (ms *memSource) next() (bool, error) {
	c, ok := ms.m.pop()
	if !ok {
		return false, nil
	}

	rec := &c.chunk[c.pos-1]
	ms.cur.createdAt = rec.createdAt
	ms.cur.node = rec.node
	ms.cur.inpoints = nil

	if ms.withI {
		ms.cur.inpoints = ms.b.inpoints(rec.seq)
	}

	return true, nil
}

func (ms *memSource) current() *entry { return &ms.cur }
func (ms *memSource) close() error    { return nil }

// runMerge merges sources by (createdAt, source index). Sources are ordered by
// when their records were added, so the source index reproduces insertion
// order on ties.
type runMerge struct {
	srcs []source
	h    []int
}

func (rm *runMerge) Len() int { return len(rm.h) }
func (rm *runMerge) Less(i, j int) bool {
	a, b := rm.srcs[rm.h[i]].current(), rm.srcs[rm.h[j]].current()
	if a.createdAt != b.createdAt {
		return a.createdAt < b.createdAt
	}

	return rm.h[i] < rm.h[j]
}
func (rm *runMerge) Swap(i, j int) { rm.h[i], rm.h[j] = rm.h[j], rm.h[i] }
func (rm *runMerge) Push(x any)    { rm.h = append(rm.h, x.(int)) }
func (rm *runMerge) Pop() any {
	x := rm.h[len(rm.h)-1]
	rm.h = rm.h[:len(rm.h)-1]

	return x
}

// txPair backs one emitted UnminedTransaction and the Node it points at; the
// slab of pairs is reused from batch to batch.
type txPair struct {
	tx   utxo.UnminedTransaction
	node subtreepkg.Node
}

// Drain emits every added transaction in (CreatedAt, insertion) order, calling
// fn with batches of about batchSize. A batch only ends where CreatedAt
// changes, so an equal-CreatedAt group is never split across batches.
//
// fn must not retain the batch slice, the *UnminedTransaction or its *Node
// after it returns: they are reused for the next batch. Each TxInpoints is a
// fresh allocation and may be retained. Drain may be called once.
func (s *Sorter) Drain(ctx context.Context, batchSize int, fn func(batch []*utxo.UnminedTransaction) error) error {
	if s.drained {
		return errors.NewProcessingError("unminedsort: Drain called twice")
	}

	s.drained = true

	if err := s.waitSpill(); err != nil {
		return err
	}

	// The last spilled buffer is not needed again; don't hold it through the merge.
	s.spare = nil

	if batchSize <= 0 {
		batchSize = 1
	}

	srcs := make([]source, 0, len(s.runs)+1)

	defer func() {
		for _, src := range srcs {
			_ = src.close()
		}
	}()

	for _, path := range s.runs {
		fs, err := openFileSource(path, s.opts.WithInpoints)
		if err != nil {
			return err
		}

		srcs = append(srcs, fs)
	}

	if len(s.cur.recs) > 0 {
		srcs = append(srcs, &memSource{b: s.cur, m: s.newMemMerge(s.sortedChunks(s.cur)), withI: s.opts.WithInpoints})
	}

	rm := &runMerge{srcs: srcs, h: make([]int, 0, len(srcs))}

	for i, src := range srcs {
		ok, err := src.next()
		if err != nil {
			return err
		}

		if ok {
			rm.h = append(rm.h, i)
		}
	}

	heap.Init(rm)

	pairs := make([]txPair, 0, batchSize)
	ptrs := make([]*utxo.UnminedTransaction, 0, batchSize)

	flush := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}

		ptrs = ptrs[:0]
		for i := range pairs {
			pairs[i].tx.Node = &pairs[i].node
			ptrs = append(ptrs, &pairs[i].tx)
		}

		err := fn(ptrs)
		pairs = pairs[:0]

		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	for rm.Len() > 0 {
		top := rm.h[0]
		e := srcs[top].current()

		if len(pairs) >= batchSize && e.createdAt != int64(pairs[len(pairs)-1].tx.CreatedAt) {
			if err := flush(); err != nil {
				return err
			}
		}

		inpoints := &subtreepkg.TxInpoints{}
		if len(e.inpoints) > 0 {
			var err error
			if *inpoints, err = subtreepkg.NewTxInpointsFromBytes(e.inpoints); err != nil {
				return errors.NewProcessingError("unminedsort: decoding inpoints for %s", e.node.Hash, err)
			}
		}

		pairs = append(pairs, txPair{
			tx: utxo.UnminedTransaction{
				TxInpoints: inpoints,
				CreatedAt:  int(e.createdAt),
			},
			node: e.node,
		})

		ok, err := srcs[top].next()
		if err != nil {
			return err
		}

		if ok {
			heap.Fix(rm, 0)
		} else {
			heap.Pop(rm)
		}
	}

	if len(pairs) > 0 {
		return flush()
	}

	return nil
}

// Close removes the sorter's run files. It is safe to call more than once.
func (s *Sorter) Close() error {
	_ = s.waitSpill()

	s.cur = &batchBuf{}
	s.spare = nil

	dirs := s.dirs
	s.dirs = nil
	s.runs = nil

	var firstErr error

	for _, dir := range dirs {
		if err := os.RemoveAll(dir); err != nil && firstErr == nil {
			firstErr = errors.NewStorageError("unminedsort: removing run directory %s", dir, err)
		}
	}

	return firstErr
}
