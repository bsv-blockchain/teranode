package blockassembly

import (
	"bufio"
	"context"
	"encoding/binary"
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

// netUnminedInOrder computes the transactions of the move-back blocks that are
// in none of the move-forward blocks, and hands them to emit in move-back order:
// block by block, subtree by subtree, position by position, exactly as the
// blocks list them. It is the set difference Reset used to compute with one
// in-memory map holding every move-forward hash and one slice holding every
// result, which does not fit in memory when a reset spans billions of
// transactions.
//
// Memory is bounded by the size of one hash range of the forward set, one bit
// per move-back transaction, and one chunk; everything else lives in scratch
// files under p.Dir, removed on return.
//
//  1. Stream the move-back blocks in order. Each hash gets its position in that
//     order and is written, with the position, to the scratch file of its hash
//     range.
//  2. Stream the move-forward blocks, in any order, writing each hash to the
//     scratch file of its range.
//  3. For each range: load its forward hashes into a set, read its move-back
//     records, and set the bit of every position whose hash is in the set.
//  4. Stream the move-back blocks in order a second time, counting positions the
//     same way, and emit the hashes whose bit is clear, p.ChunkSize at a time.
//
// The order never depends on the ranges: they only decide which hashes are
// compared in which pass. Positions are only valid if both reads of a block
// agree, so a move-back block that fails the second read fails the whole
// computation.
//
// A move-back block that cannot be read in step 1 is left out whole, and listed
// in the result: its positions are reserved but never emitted. A move-forward
// block that cannot be read fails the whole computation before anything is
// emitted, because an incomplete forward set would report move-forward
// transactions as net unmined.
//
// forward may call visit from several goroutines; back may not. Neither may
// retain the slice passed to visit, and emit may not retain its chunk.
func netUnminedInOrder(
	ctx context.Context,
	backBlocks int, back txHashStream,
	forwardBlocks int, forward txHashStream,
	p netUnminedParams,
	emit func(chunk []chainhash.Hash) error,
) (res netUnminedResult, err error) {
	if p.ChunkSize <= 0 {
		return res, errors.NewProcessingError("[netUnmined] chunk size must be positive, got %d", p.ChunkSize)
	}

	if p.RangeBits > maxNetUnminedRangeBits {
		return res, errors.NewProcessingError("[netUnmined] range bits %d above the maximum %d", p.RangeBits, maxNetUnminedRangeBits)
	}

	beat := p.Beat
	if beat == nil {
		beat = func() {}
	}

	removeStaleNetUnminedScratch(p.Dir)

	dir, err := os.MkdirTemp(p.Dir, netUnminedScratchName+"*")
	if err != nil {
		return res, errors.NewStorageError("[netUnmined] error creating the scratch directory under %q", p.Dir, err)
	}

	defer func() {
		if rmErr := os.RemoveAll(dir); rmErr != nil && err == nil {
			err = errors.NewStorageError("[netUnmined] error removing the scratch directory %s", dir, rmErr)
		}
	}()

	ranges := 1 << p.RangeBits

	backFiles, err := newRangeWriters(dir, "back", ranges)
	if err != nil {
		return res, err
	}

	defer func() { _ = backFiles.closeAll() }() // the explicit close below reports the error; this one covers early returns

	blockStart := make([]uint64, backBlocks)
	blockEnd := make([]uint64, backBlocks)
	failed := make([]bool, backBlocks)

	var pos uint64

	for i := 0; i < backBlocks; i++ {
		if err = ctx.Err(); err != nil {
			return res, err
		}

		blockStart[i] = pos

		var scratchErr error

		blockErr := back(ctx, i, func(hashes []chainhash.Hash) error {
			beat()

			var rec [netUnminedBackRecordSize]byte

			for _, h := range hashes {
				binary.LittleEndian.PutUint64(rec[:8], pos)
				copy(rec[8:], h[:])

				if scratchErr = backFiles.write(netUnminedRangeOf(h, p.RangeBits), rec[:]); scratchErr != nil {
					return scratchErr
				}

				pos++
			}

			return nil
		})

		blockEnd[i] = pos

		if blockErr != nil {
			// A cancelled context or a scratch-file write failure is ours, not the
			// block's: it stops the computation instead of leaving the block out.
			if ctx.Err() != nil {
				return res, ctx.Err()
			}

			if scratchErr != nil {
				return res, scratchErr
			}

			failed[i] = true
			res.SkippedBackBlocks = append(res.SkippedBackBlocks, i)
		}
	}

	res.Back = pos

	if err = backFiles.closeAll(); err != nil {
		return res, err
	}

	forwardFiles, err := newRangeWriters(dir, "forward", ranges)
	if err != nil {
		return res, err
	}

	defer func() { _ = forwardFiles.closeAll() }()

	var forwardCount uint64

	var (
		forwardMu         sync.Mutex
		forwardScratchErr error
	)

	for i := 0; i < forwardBlocks; i++ {
		if err = ctx.Err(); err != nil {
			return res, err
		}

		if err = forward(ctx, i, func(hashes []chainhash.Hash) error {
			beat()

			for _, h := range hashes {
				if werr := forwardFiles.write(netUnminedRangeOf(h, p.RangeBits), h[:]); werr != nil {
					forwardMu.Lock()
					forwardScratchErr = werr
					forwardMu.Unlock()

					return werr
				}
			}

			forwardMu.Lock()
			forwardCount += uint64(len(hashes))
			forwardMu.Unlock()

			return nil
		}); err != nil {
			forwardMu.Lock()
			scratchFailed := forwardScratchErr != nil
			forwardMu.Unlock()

			// A full scratch disk is ours, not an unreadable block's.
			res.ForwardIncomplete = ctx.Err() == nil && !scratchFailed

			return res, errors.NewProcessingError("[netUnmined] error reading move-forward block %d, so the move-forward set is incomplete", i, err)
		}
	}

	res.Forward = forwardCount

	if err = forwardFiles.closeAll(); err != nil {
		return res, err
	}

	inForward := make([]uint64, (pos+63)/64)

	for r := 0; r < ranges; r++ {
		if err = ctx.Err(); err != nil {
			return res, err
		}

		beat()

		if err = markRangeInForward(dir, r, inForward, beat); err != nil {
			return res, err
		}
	}

	// Second ordered read of the move-back blocks: emit what no forward block has.
	chunk := make([]chainhash.Hash, 0, p.ChunkSize)

	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}

		beat()

		if eerr := emit(chunk); eerr != nil {
			return eerr
		}

		res.Emitted += uint64(len(chunk))
		chunk = chunk[:0]

		return nil
	}

	for i := 0; i < backBlocks; i++ {
		if failed[i] {
			continue
		}

		if err = ctx.Err(); err != nil {
			return res, err
		}

		at := blockStart[i]

		if err = back(ctx, i, func(hashes []chainhash.Hash) error {
			beat()

			for _, h := range hashes {
				if at >= blockEnd[i] {
					return errors.NewProcessingError("[netUnmined] move-back block %d has more transactions on the second read than on the first", i)
				}

				if inForward[at>>6]&(1<<(at&63)) == 0 {
					chunk = append(chunk, h)

					if len(chunk) == p.ChunkSize {
						if ferr := flush(); ferr != nil {
							return ferr
						}
					}
				}

				at++
			}

			return nil
		}); err != nil {
			return res, err
		}

		if at != blockEnd[i] {
			return res, errors.NewProcessingError("[netUnmined] move-back block %d has %d transactions on the second read, %d on the first", i, at-blockStart[i], blockEnd[i]-blockStart[i])
		}
	}

	if err = flush(); err != nil {
		return res, err
	}

	return res, nil
}

// txHashStream calls visit with the transaction hashes of one block, one subtree
// per call, in the block's order, leaving out the coinbase placeholder.
type txHashStream func(ctx context.Context, block int, visit func(hashes []chainhash.Hash) error) error

type netUnminedParams struct {
	// Dir holds the scratch files; a unique subdirectory is made inside it.
	Dir string
	// RangeBits splits the hash space into 1<<RangeBits ranges by leading bits.
	RangeBits uint
	// ChunkSize is the most hashes handed to emit at once.
	ChunkSize int
	// Beat is called regularly, so a liveness probe sees progress.
	Beat func()
}

type netUnminedResult struct {
	// Back and Forward count the transactions streamed from each side.
	Back, Forward uint64
	// Emitted counts the net unmined transactions handed to emit.
	Emitted uint64
	// SkippedBackBlocks lists the move-back blocks that could not be read.
	SkippedBackBlocks []int
	// ForwardIncomplete is set when a move-forward block could not be read, so
	// nothing was emitted.
	ForwardIncomplete bool
}

const (
	// maxNetUnminedRangeBits caps the number of ranges, and with it the open
	// scratch files, at 1024 per side.
	maxNetUnminedRangeBits = 10

	// netUnminedBackRecordSize is a move-back record: position, then hash.
	netUnminedBackRecordSize = 8 + chainhash.HashSize

	// netUnminedScratchPrefix starts the name of every scratch directory this
	// feature makes.
	netUnminedScratchPrefix = "ba-reset-netunmined-"

	netUnminedWriterBuffer = 64 * 1024
	netUnminedReaderBuffer = 1024 * 1024
)

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

	if _, err := w.writers[r].Write(rec); err != nil {
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

// markRangeInForward sets, in inForward, the bit of every move-back position of
// range r whose hash is among the range's forward hashes.
func markRangeInForward(dir string, r int, inForward []uint64, beat func()) error {
	set, err := loadForwardRange(netUnminedRangePath(dir, "forward", r), beat)
	if err != nil {
		return err
	}

	if len(set) == 0 {
		return nil
	}

	f, err := os.Open(netUnminedRangePath(dir, "back", r))
	if err != nil {
		return errors.NewStorageError("[netUnmined] error opening the move-back scratch file for range %d", r, err)
	}

	defer f.Close()

	rd := bufio.NewReaderSize(f, netUnminedReaderBuffer)

	var rec [netUnminedBackRecordSize]byte

	for n := 1; ; n++ {
		if n%netUnminedBeatEvery == 0 {
			beat()
		}

		if _, err = io.ReadFull(rd, rec[:]); err != nil {
			if err == io.EOF { //nolint:errorlint // ReadFull returns io.EOF itself, at a record boundary
				return nil
			}

			return errors.NewStorageError("[netUnmined] error reading the move-back scratch file for range %d", r, err)
		}

		var h chainhash.Hash

		copy(h[:], rec[8:])

		if _, ok := set[h]; ok {
			at := binary.LittleEndian.Uint64(rec[:8])
			inForward[at>>6] |= 1 << (at & 63)
		}
	}
}

func loadForwardRange(path string, beat func()) (map[chainhash.Hash]struct{}, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.NewStorageError("[netUnmined] error opening the move-forward scratch file %s", path, err)
	}

	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, errors.NewStorageError("[netUnmined] error sizing the move-forward scratch file %s", path, err)
	}

	set := make(map[chainhash.Hash]struct{}, info.Size()/chainhash.HashSize)
	rd := bufio.NewReaderSize(f, netUnminedReaderBuffer)

	var h chainhash.Hash

	for n := 1; ; n++ {
		if n%netUnminedBeatEvery == 0 {
			beat()
		}

		if _, err = io.ReadFull(rd, h[:]); err != nil {
			if err == io.EOF { //nolint:errorlint // ReadFull returns io.EOF itself, at a record boundary
				return set, nil
			}

			return nil, errors.NewStorageError("[netUnmined] error reading the move-forward scratch file %s", path, err)
		}

		set[h] = struct{}{}
	}
}
