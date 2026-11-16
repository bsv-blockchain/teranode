package blockassembly

import (
	"bufio"
	"context"
	"io"
	mathbits "math/bits"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// netUnmined computes the transactions of the move-back blocks that are in none
// of the move-forward blocks, and hands them to emit, p.ChunkSize at a time, in
// no particular order. It is the set difference Reset used to compute with one
// in-memory map holding every move-forward hash and one slice holding every
// result, which does not fit in memory when a reset spans billions of
// transactions.
//
// A forward set of at most p.MaxForwardInMemory hashes is held in one map, the
// move-back blocks are streamed against it, and no file is written: a reset of
// ordinary size needs no disk.
//
// A larger one is split by hash range into scratch files under p.Dir, removed on
// return. Memory is then bounded by one range of the forward set, one chunk, and
// the subtrees the streams have read and not yet handed over:
//
//  1. Stream the move-forward blocks, writing each hash to the scratch file of
//     its range.
//  2. Stream the move-back blocks the same way.
//  3. For each range: load its forward hashes into a set, read its move-back
//     hashes, and emit the ones the set does not have.
//
// A move-forward block a stream reports unreadable fails the whole computation
// before anything is emitted, with ForwardIncomplete set, because an incomplete
// forward set would report move-forward transactions as net unmined. An
// unreadable move-back block is listed in the result; what was read of it is
// still emitted, since every hash read is a move-back transaction. An error from
// a stream, from emit, from a scratch file or from the context is returned and
// is neither of those.
//
// Both streams may call visit from several goroutines, and neither may retain
// the slice passed to it. emit is never called concurrently and may not retain
// its chunk.
func netUnmined(
	ctx context.Context,
	backBlocks int, back txHashStream,
	forwardBlocks int, forward txHashStream,
	p netUnminedParams,
	emit func(chunk []chainhash.Hash) error,
) (res netUnminedResult, err error) {
	if p.ChunkSize <= 0 {
		return res, errors.NewProcessingError("[netUnmined] chunk size must be positive, got %d", p.ChunkSize)
	}

	beat := p.Beat
	if beat == nil {
		beat = func() {}
	}

	if p.SweepStale {
		// Removing a large directory left by a killed run can take a while.
		beat()
		removeStaleNetUnminedScratch(p.Dir)
		beat()
	}

	out := &netUnminedEmitter{chunk: make([]chainhash.Hash, 0, p.ChunkSize), size: p.ChunkSize, emit: emit, beat: beat}

	if p.ForwardCount <= p.MaxForwardInMemory {
		err = netUnminedInMemory(ctx, backBlocks, back, forwardBlocks, forward, beat, out, &res)
	} else {
		err = netUnminedOnDisk(ctx, backBlocks, back, forwardBlocks, forward, p, beat, out, &res)
	}

	res.Emitted = out.emitted

	return res, err
}

// netUnminedEmitter gathers hashes into chunks and hands each full one to emit,
// one at a time. After emit fails it takes nothing more.
type netUnminedEmitter struct {
	mu      sync.Mutex
	chunk   []chainhash.Hash
	size    int
	emit    func([]chainhash.Hash) error
	beat    func()
	emitted uint64
	err     error
}

func (e *netUnminedEmitter) add(hashes []chainhash.Hash) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for len(hashes) > 0 && e.err == nil {
		n := min(e.size-len(e.chunk), len(hashes))
		e.chunk = append(e.chunk, hashes[:n]...)
		hashes = hashes[n:]

		if len(e.chunk) == e.size {
			e.flushLocked()
		}
	}

	return e.err
}

func (e *netUnminedEmitter) flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.err == nil {
		e.flushLocked()
	}

	return e.err
}

func (e *netUnminedEmitter) flushLocked() {
	if len(e.chunk) == 0 {
		return
	}

	e.beat()

	if e.err = e.emit(e.chunk); e.err != nil {
		return
	}

	e.emitted += uint64(len(e.chunk))
	e.chunk = e.chunk[:0]
}

// netUnminedSink is where a stream's hashes go. Its err is an error of ours, a
// failed emit or scratch write, as opposed to a block that cannot be read.
type netUnminedSink struct {
	mu    sync.Mutex
	count uint64
	err   error
}

func (s *netUnminedSink) add(n int) {
	s.mu.Lock()
	s.count += uint64(n)
	s.mu.Unlock()
}

func (s *netUnminedSink) fail(err error) error {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()

	return err
}

func (s *netUnminedSink) failed() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.err
}

// readForward streams every move-forward block into visit. A block that cannot
// be read sets ForwardIncomplete; a failure of visit itself or of the context
// does not.
func readForward(ctx context.Context, blocks int, forward txHashStream, sink *netUnminedSink, beat func(), visit func([]chainhash.Hash) error, res *netUnminedResult) error {
	for i := 0; i < blocks; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Per block, read or not: visit only beats for a subtree that was read.
		beat()

		unreadable, err := forward(ctx, i, visit)

		// Ours first: a failed write cancels the block's other reads, and the
		// stream may report one of those instead.
		if ours := sink.failed(); ours != nil {
			return ours
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err != nil {
			return err
		}

		if unreadable != nil {
			res.ForwardIncomplete = true

			return errors.NewProcessingError("[netUnmined] error reading move-forward block %d, so the move-forward set is incomplete", i, unreadable)
		}
	}

	res.Forward = sink.count

	return nil
}

// readBack streams every move-back block into visit. A block that cannot be read
// is listed in the result and the next one is read.
func readBack(ctx context.Context, blocks int, back txHashStream, sink *netUnminedSink, beat func(), visit func([]chainhash.Hash) error, res *netUnminedResult) error {
	for i := 0; i < blocks; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		beat()

		unreadable, err := back(ctx, i, visit)

		if ours := sink.failed(); ours != nil {
			return ours
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err != nil {
			return err
		}

		if unreadable != nil {
			res.SkippedBackBlocks = append(res.SkippedBackBlocks, netUnminedSkippedBlock{Block: i, Err: unreadable})
		}
	}

	res.Back = sink.count

	return nil
}

func netUnminedInMemory(
	ctx context.Context,
	backBlocks int, back txHashStream,
	forwardBlocks int, forward txHashStream,
	beat func(), out *netUnminedEmitter, res *netUnminedResult,
) error {
	var (
		inForward   = make(map[chainhash.Hash]struct{})
		forwardSink netUnminedSink
	)

	if err := readForward(ctx, forwardBlocks, forward, &forwardSink, beat, func(hashes []chainhash.Hash) error {
		beat()

		forwardSink.mu.Lock()
		for _, h := range hashes {
			inForward[h] = struct{}{}
		}

		forwardSink.count += uint64(len(hashes))
		forwardSink.mu.Unlock()

		return nil
	}, res); err != nil {
		return err
	}

	// The set is complete and only read from here on.
	res.PeakForwardInMemory = uint64(len(inForward))

	var backSink netUnminedSink

	if err := readBack(ctx, backBlocks, back, &backSink, beat, func(hashes []chainhash.Hash) error {
		beat()
		backSink.add(len(hashes))

		kept := make([]chainhash.Hash, 0, len(hashes))

		for _, h := range hashes {
			if _, ok := inForward[h]; !ok {
				kept = append(kept, h)
			}
		}

		if err := out.add(kept); err != nil {
			return backSink.fail(err)
		}

		return nil
	}, res); err != nil {
		return err
	}

	return out.flush()
}

func netUnminedOnDisk(
	ctx context.Context,
	backBlocks int, back txHashStream,
	forwardBlocks int, forward txHashStream,
	p netUnminedParams,
	beat func(), out *netUnminedEmitter, res *netUnminedResult,
) (err error) {
	rangeBits := netUnminedRangeBits(p.ForwardCount, p.MaxForwardInMemory)
	ranges := 1 << rangeBits

	dir, err := os.MkdirTemp(p.Dir, netUnminedScratchName+"*")
	if err != nil {
		return errors.NewStorageError("[netUnmined] error creating the scratch directory under %q", p.Dir, err)
	}

	defer func() {
		// Removing billions of records can take a while. A directory that stays is
		// reported, not an error: by now every chunk is emitted, or err says why not.
		beat()

		if rmErr := netUnminedRemoveAll(dir); rmErr != nil {
			res.ScratchLeft = errors.NewStorageError("[netUnmined] error removing the scratch directory %s", dir, rmErr)
		}

		beat()
	}()

	// Forward first: a forward block that cannot be read ends the computation,
	// and then the move-back blocks need not be read at all.
	if err = netUnminedToRangeFiles(dir, "forward", ranges, rangeBits, beat, func(sink *netUnminedSink, visit func([]chainhash.Hash) error) error {
		return readForward(ctx, forwardBlocks, forward, sink, beat, visit, res)
	}); err != nil {
		return err
	}

	if err = netUnminedToRangeFiles(dir, "back", ranges, rangeBits, beat, func(sink *netUnminedSink, visit func([]chainhash.Hash) error) error {
		return readBack(ctx, backBlocks, back, sink, beat, visit, res)
	}); err != nil {
		return err
	}

	// One set, cleared between ranges: a new one per range would leave the last
	// one for the collector while the next is filled.
	inForward := make(map[chainhash.Hash]struct{})

	for r := 0; r < ranges; r++ {
		if err = ctx.Err(); err != nil {
			return err
		}

		beat()
		clear(inForward)

		if err = forEachScratchHash(netUnminedRangePath(dir, "forward", r), beat, nil, func(h []chainhash.Hash) error {
			for i := range h {
				inForward[h[i]] = struct{}{}
			}

			return nil
		}); err != nil {
			return err
		}

		res.PeakForwardInMemory = max(res.PeakForwardInMemory, uint64(len(inForward)))

		if err = forEachScratchHash(netUnminedRangePath(dir, "back", r), beat, inForward, out.add); err != nil {
			return err
		}
	}

	// The last chunk, before the scratch files go.
	return out.flush()
}

// netUnminedToRangeFiles runs read with a visitor that writes every hash to the
// scratch file of its range, and closes the files.
func netUnminedToRangeFiles(dir, side string, ranges int, rangeBits uint, beat func(),
	read func(sink *netUnminedSink, visit func([]chainhash.Hash) error) error) error {
	files, err := newRangeWriters(dir, side, ranges)
	if err != nil {
		return err
	}

	defer func() { _ = files.closeAll() }() // the explicit close below reports the error; this one covers early returns

	var sink netUnminedSink

	if err = read(&sink, func(hashes []chainhash.Hash) error {
		beat()

		for i := range hashes {
			if werr := files.write(netUnminedRangeOf(hashes[i], rangeBits), hashes[i][:]); werr != nil {
				return sink.fail(werr)
			}
		}

		sink.add(len(hashes))

		return nil
	}); err != nil {
		return err
	}

	return files.closeAll()
}

// txHashStream calls visit with the transaction hashes of one block, one subtree
// per call, leaving out the coinbase placeholder. It may call visit from several
// goroutines. unreadable says the block could not be read in full, and why: a
// file of it is gone or is not what its key says. err is everything else: an
// error visit returned, a store that failed, a cancelled context.
type txHashStream func(ctx context.Context, block int, visit func(hashes []chainhash.Hash) error) (unreadable, err error)

type netUnminedParams struct {
	// Dir holds the scratch files of a forward set too large for memory; a unique
	// subdirectory is made inside it.
	Dir string
	// ForwardCount is how many transactions the move-forward blocks hold.
	ForwardCount uint64
	// MaxForwardInMemory is the most move-forward hashes held in memory at once:
	// the whole set when it has no more than that, otherwise one range of it.
	MaxForwardInMemory uint64
	// ChunkSize is the most hashes handed to emit at once.
	ChunkSize int
	// Beat is called regularly, so a liveness probe sees progress.
	Beat func()
	// SweepStale removes scratch directories an earlier run left under Dir. Set it
	// only when Dir is used by this process alone: a directory another live
	// process is working in would be deleted under it.
	SweepStale bool
}

type netUnminedSkippedBlock struct {
	Block int
	Err   error
}

type netUnminedResult struct {
	// Back and Forward count the transactions streamed from each side.
	Back, Forward uint64
	// Emitted counts the net unmined transactions handed to emit.
	Emitted uint64
	// SkippedBackBlocks lists the move-back blocks that could not be read in full.
	SkippedBackBlocks []netUnminedSkippedBlock
	// PeakForwardInMemory is the most move-forward hashes held in memory at once:
	// the whole set, or the largest range of it.
	PeakForwardInMemory uint64
	// ForwardIncomplete is set when a move-forward block could not be read, so
	// nothing was emitted.
	ForwardIncomplete bool
	// ScratchLeft is set when the scratch directory could not be removed.
	ScratchLeft error
}

const (
	// maxNetUnminedRangeBits caps the number of ranges, and with it the open
	// scratch files, at 1024.
	maxNetUnminedRangeBits = 10

	// netUnminedScratchPrefix starts the name of every scratch directory this
	// feature makes.
	netUnminedScratchPrefix = "ba-reset-netunmined-"

	netUnminedWriterBuffer = 64 * 1024
	netUnminedReaderBuffer = 1024 * 1024

	// netUnminedReadBatch is how many hashes a range pass reads from a scratch
	// file at a time.
	netUnminedReadBatch = 4096
)

// netUnminedRemoveAll removes a scratch directory. A variable so a test can make
// it fail.
var netUnminedRemoveAll = os.RemoveAll

// netUnminedWriteScratch appends a record to a scratch file. A variable so a
// test can make it fail.
var netUnminedWriteScratch = func(w *bufio.Writer, rec []byte) (int, error) { return w.Write(rec) }

// netUnminedBeatEvery is how many records a range pass reads between beats, so
// a range holding billions of records still shows a liveness probe progress. A
// variable so a test can shrink it.
var netUnminedBeatEvery = 1 << 16

// netUnminedScratchName is the name prefix of this process's scratch
// directories: the feature prefix and the process start time, so a directory
// left by an earlier run, which a kill mid-reset leaves behind, is told apart
// from this run's even when a container's process id is the same.
var netUnminedScratchName = netUnminedScratchPrefix + strconv.FormatInt(time.Now().UnixNano(), 10) + "-"

// removeStaleNetUnminedScratch deletes scratch directories under dir that an
// earlier run left behind. Best effort: one that cannot be removed is left for
// the next call. Only directories with this feature's prefix and another run's
// start time are touched. A reset runs one at a time, so none of them is in use.
func removeStaleNetUnminedScratch(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	for _, e := range entries {
		name := e.Name()
		if e.IsDir() && strings.HasPrefix(name, netUnminedScratchPrefix) && !strings.HasPrefix(name, netUnminedScratchName) {
			_ = os.RemoveAll(filepath.Join(dir, name))
		}
	}
}

// netUnminedRangeBits returns the fewest range bits for which a forward set of
// forwardCount hashes, spread evenly over the ranges, has at most maxPerRange
// hashes in each.
func netUnminedRangeBits(forwardCount, maxPerRange uint64) uint {
	if maxPerRange == 0 {
		return maxNetUnminedRangeBits
	}

	need := (forwardCount + maxPerRange - 1) / maxPerRange
	if need <= 1 {
		return 0
	}

	return min(uint(mathbits.Len64(need-1)), maxNetUnminedRangeBits)
}

// netUnminedRangeOf is the range of h: its leading bits. Transaction ids are
// hashes, so the ranges fill evenly.
func netUnminedRangeOf(h chainhash.Hash, rangeBits uint) int {
	if rangeBits == 0 {
		return 0
	}

	return int((uint16(h[0])<<8 | uint16(h[1])) >> (16 - rangeBits))
}

// rangeWriters appends records to one buffered scratch file per range.
type rangeWriters struct {
	files   []*os.File
	writers []*bufio.Writer
	locks   []sync.Mutex
	closed  bool
}

func netUnminedRangePath(dir, side string, r int) string {
	return filepath.Join(dir, side+"-"+strconv.Itoa(r))
}

func newRangeWriters(dir, side string, ranges int) (*rangeWriters, error) {
	w := &rangeWriters{
		files:   make([]*os.File, ranges),
		writers: make([]*bufio.Writer, ranges),
		locks:   make([]sync.Mutex, ranges),
	}

	for r := 0; r < ranges; r++ {
		f, err := os.Create(netUnminedRangePath(dir, side, r))
		if err != nil {
			_ = w.closeAll()

			return nil, errors.NewStorageError("[netUnmined] error creating a scratch file for range %d", r, err)
		}

		w.files[r] = f
		w.writers[r] = bufio.NewWriterSize(f, netUnminedWriterBuffer)
	}

	return w, nil
}

func (w *rangeWriters) write(r int, rec []byte) error {
	w.locks[r].Lock()
	defer w.locks[r].Unlock()

	if _, err := netUnminedWriteScratch(w.writers[r], rec); err != nil {
		return errors.NewStorageError("[netUnmined] error writing a scratch file for range %d", r, err)
	}

	return nil
}

// closeAll flushes and closes every file; safe to call again.
func (w *rangeWriters) closeAll() error {
	if w.closed {
		return nil
	}

	w.closed = true

	var first error

	for r := range w.files {
		if w.files[r] == nil {
			continue
		}

		if err := w.writers[r].Flush(); err != nil && first == nil {
			first = errors.NewStorageError("[netUnmined] error flushing a scratch file for range %d", r, err)
		}

		if err := w.files[r].Close(); err != nil && first == nil {
			first = errors.NewStorageError("[netUnmined] error closing a scratch file for range %d", r, err)
		}
	}

	return first
}

// forEachScratchHash reads the hashes of a scratch file and hands them to visit,
// a batch at a time, leaving out the ones in skip.
func forEachScratchHash(path string, beat func(), skip map[chainhash.Hash]struct{}, visit func([]chainhash.Hash) error) error {
	f, err := os.Open(path)
	if err != nil {
		return errors.NewStorageError("[netUnmined] error opening the scratch file %s", path, err)
	}

	defer f.Close()

	rd := bufio.NewReaderSize(f, netUnminedReaderBuffer)
	batch := make([]chainhash.Hash, 0, netUnminedReadBatch)

	var h chainhash.Hash

	for n := 1; ; n++ {
		if n%netUnminedBeatEvery == 0 {
			beat()
		}

		_, err = io.ReadFull(rd, h[:])
		if err != nil && err != io.EOF { //nolint:errorlint // ReadFull returns io.EOF itself, at a record boundary
			return errors.NewStorageError("[netUnmined] error reading the scratch file %s", path, err)
		}

		if err == nil {
			if _, ok := skip[h]; !ok {
				batch = append(batch, h)
			}
		}

		if len(batch) == cap(batch) || (err != nil && len(batch) > 0) {
			if verr := visit(batch); verr != nil {
				return verr
			}

			batch = batch[:0]
		}

		if err != nil {
			return nil
		}
	}
}
