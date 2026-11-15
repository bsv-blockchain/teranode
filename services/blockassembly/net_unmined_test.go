package blockassembly

import (
	"bufio"
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// blockHashes is one block's transaction hashes, in subtree order, split into
// subtrees of subtreeLen hashes the way a block stores them.
type blockHashes struct {
	hashes     []chainhash.Hash
	subtreeLen int
	failAfter  int // when > 0, the stream errors after this many subtrees
}

func randomHash(r *rand.Rand) chainhash.Hash {
	var h chainhash.Hash

	_, _ = r.Read(h[:])

	return h
}

func makeBlocks(r *rand.Rand, sizes []int, subtreeLen int) []blockHashes {
	blocks := make([]blockHashes, len(sizes))

	for i, n := range sizes {
		blocks[i] = blockHashes{subtreeLen: subtreeLen, hashes: make([]chainhash.Hash, n)}
		for j := range blocks[i].hashes {
			blocks[i].hashes[j] = randomHash(r)
		}
	}

	return blocks
}

func streamOf(blocks []blockHashes) txHashStream {
	return func(ctx context.Context, block int, visit func([]chainhash.Hash) error) (error, error) {
		b := blocks[block]
		sent := 0

		for start := 0; start < len(b.hashes); start += b.subtreeLen {
			if b.failAfter > 0 && sent == b.failAfter {
				return errors.NewStorageError("subtree %d of block %d is gone", sent, block), nil
			}

			if err := ctx.Err(); err != nil {
				return nil, err
			}

			// A fresh copy each call: the visitor must not retain the slice, and
			// a stream that reuses its buffer must not be relied on either.
			chunk := append([]chainhash.Hash(nil), b.hashes[start:min(start+b.subtreeLen, len(b.hashes))]...)
			if err := visit(chunk); err != nil {
				return nil, err
			}

			sent++
		}

		return nil, nil
	}
}

// concurrentStreamOf calls visit for every subtree of a block from its own
// goroutine, the way the reset reads a block's subtrees.
func concurrentStreamOf(blocks []blockHashes) txHashStream {
	return func(_ context.Context, block int, visit func([]chainhash.Hash) error) (error, error) {
		b := blocks[block]

		var (
			wg    sync.WaitGroup
			mu    sync.Mutex
			first error
		)

		for start := 0; start < len(b.hashes); start += b.subtreeLen {
			wg.Add(1)

			go func(chunk []chainhash.Hash) {
				defer wg.Done()

				if err := visit(chunk); err != nil {
					mu.Lock()
					if first == nil {
						first = err
					}
					mu.Unlock()
				}
			}(append([]chainhash.Hash(nil), b.hashes[start:min(start+b.subtreeLen, len(b.hashes))]...))
		}

		wg.Wait()

		return nil, first
	}
}

// referenceNetUnmined is the map the Reset used to build: every forward hash in
// one set, every back hash kept if it is not in it.
func referenceNetUnmined(back, forward []blockHashes, skippedBack map[int]bool) []chainhash.Hash {
	inForward := make(map[chainhash.Hash]struct{})

	for _, b := range forward {
		for _, h := range b.hashes {
			inForward[h] = struct{}{}
		}
	}

	var out []chainhash.Hash

	for i, b := range back {
		if skippedBack[i] {
			continue
		}

		for _, h := range b.hashes {
			if _, ok := inForward[h]; !ok {
				out = append(out, h)
			}
		}
	}

	return out
}

// collect appends without a lock: emit is never called concurrently, which the
// race detector checks here.
func collect(out *[]chainhash.Hash) func([]chainhash.Hash) error {
	return func(chunk []chainhash.Hash) error {
		*out = append(*out, chunk...)

		return nil
	}
}

// overlap makes every third hash of back[0] and every fifth of back[1] also a
// forward hash, so the difference is neither empty nor everything.
func overlap(back, forward []blockHashes) {
	for i := 0; i < len(back[0].hashes); i += 3 {
		forward[0].hashes[i%len(forward[0].hashes)] = back[0].hashes[i]
	}

	for i := 0; i < len(back[1].hashes); i += 5 {
		forward[len(forward)-1].hashes[i%len(forward[len(forward)-1].hashes)] = back[1].hashes[i]
	}
}

func countHashes(blocks []blockHashes) uint64 {
	var n uint64
	for _, b := range blocks {
		n += uint64(len(b.hashes))
	}

	return n
}

// netUnminedModes runs a test in memory and on disk. maxInMemory is the most
// forward hashes held in memory at once: at or above the forward count the whole
// set is in memory and no scratch file is made.
var netUnminedModes = []struct {
	name        string
	maxInMemory uint64
}{
	{"in memory", 1 << 40},
	{"on disk, two ranges", 0}, // filled in per test: half the forward count
	{"on disk, max ranges", 1},
}

func paramsFor(t *testing.T, maxInMemory uint64, forward []blockHashes, chunk int) netUnminedParams {
	t.Helper()

	n := countHashes(forward)
	if maxInMemory == 0 {
		maxInMemory = max(n/2, 1)
	}

	return netUnminedParams{Dir: t.TempDir(), ForwardCount: n, MaxForwardInMemory: maxInMemory, ChunkSize: chunk}
}

func TestNetUnmined_MatchesTheMap(t *testing.T) {
	for _, mode := range netUnminedModes {
		t.Run(mode.name, func(t *testing.T) {
			r := rand.New(rand.NewSource(1))
			back := makeBlocks(r, []int{1000, 777, 1}, 100)
			forward := makeBlocks(r, []int{2500, 31, 900}, 64)
			overlap(back, forward)

			var got []chainhash.Hash

			res, err := netUnmined(context.Background(), len(back), streamOf(back), len(forward), streamOf(forward),
				paramsFor(t, mode.maxInMemory, forward, 7), collect(&got))
			require.NoError(t, err)

			want := referenceNetUnmined(back, forward, nil)
			require.NotEmpty(t, want)
			require.Less(t, len(want), 1000+777+1, "some back hashes must be dropped for the test to mean anything")
			require.ElementsMatch(t, want, got, "the same hashes as the map")
			require.EqualValues(t, len(want), res.Emitted)
			require.EqualValues(t, 1000+777+1, res.Back)
			require.EqualValues(t, 2500+31+900, res.Forward)
			require.Empty(t, res.SkippedBackBlocks)
			require.NoError(t, res.ScratchLeft)
		})
	}
}

func TestNetUnmined_ChunksAreBounded(t *testing.T) {
	for _, mode := range netUnminedModes {
		t.Run(mode.name, func(t *testing.T) {
			r := rand.New(rand.NewSource(2))
			back := makeBlocks(r, []int{500}, 50)
			forward := makeBlocks(r, []int{10}, 10)

			var sizes []int

			var got []chainhash.Hash

			_, err := netUnmined(context.Background(), 1, streamOf(back), 1, streamOf(forward),
				paramsFor(t, mode.maxInMemory, forward, 64), func(chunk []chainhash.Hash) error {
					sizes = append(sizes, len(chunk))
					got = append(got, chunk...)

					return nil
				})
			require.NoError(t, err)
			require.ElementsMatch(t, back[0].hashes, got)

			for i, n := range sizes {
				require.LessOrEqual(t, n, 64)

				if i < len(sizes)-1 {
					require.Equal(t, 64, n, "only the last chunk may be short")
				}
			}
		})
	}
}

// A move-back block that cannot be read in full still gives what was read of it:
// every one of those is a move-back transaction, so marking it is right.
func TestNetUnmined_ABackBlockThatCannotBeReadKeepsWhatWasRead(t *testing.T) {
	for _, mode := range netUnminedModes {
		t.Run(mode.name, func(t *testing.T) {
			r := rand.New(rand.NewSource(3))
			back := makeBlocks(r, []int{300, 300, 300}, 50)
			forward := makeBlocks(r, []int{200}, 50)
			overlap(back, forward)

			back[1].failAfter = 2 // two subtrees read, then the third is gone

			var got []chainhash.Hash

			res, err := netUnmined(context.Background(), len(back), streamOf(back), len(forward), streamOf(forward),
				paramsFor(t, mode.maxInMemory, forward, 40), collect(&got))
			require.NoError(t, err)
			require.Len(t, res.SkippedBackBlocks, 1)
			require.Equal(t, 1, res.SkippedBackBlocks[0].Block)
			require.Error(t, res.SkippedBackBlocks[0].Err, "the cause is kept for the log")

			back[1].failAfter = 0
			others := referenceNetUnmined(back, forward, map[int]bool{1: true})
			all := referenceNetUnmined(back, forward, nil)

			require.Subset(t, got, others, "the readable blocks are complete")
			require.Subset(t, all, got, "nothing that is not net unmined")
			require.Len(t, got, len(others)+len(referenceNetUnmined([]blockHashes{{hashes: back[1].hashes[:100]}}, forward, nil)),
				"the two subtrees read of the failed block are kept")
		})
	}
}

func TestNetUnmined_AForwardBlockThatCannotBeReadEmitsNothing(t *testing.T) {
	for _, mode := range netUnminedModes {
		t.Run(mode.name, func(t *testing.T) {
			r := rand.New(rand.NewSource(4))
			back := makeBlocks(r, []int{100}, 50)
			forward := makeBlocks(r, []int{100, 100}, 50)
			p := paramsFor(t, mode.maxInMemory, forward, 10)
			forward[1].failAfter = 1

			emitted := 0

			res, err := netUnmined(context.Background(), 1, streamOf(back), 2, streamOf(forward), p,
				func(chunk []chainhash.Hash) error {
					emitted += len(chunk)

					return nil
				})
			require.Error(t, err)
			require.True(t, res.ForwardIncomplete)
			require.Zero(t, emitted, "an incomplete forward set must emit nothing: every back hash would look net unmined")
		})
	}
}

func TestNetUnmined_EmitErrorStopsAndIsReturned(t *testing.T) {
	for _, mode := range netUnminedModes {
		for _, stream := range []struct {
			name string
			of   func([]blockHashes) txHashStream
		}{{"sequential", streamOf}, {"concurrent", concurrentStreamOf}} {
			t.Run(mode.name+", "+stream.name, func(t *testing.T) {
				r := rand.New(rand.NewSource(6))
				back := makeBlocks(r, []int{100, 100}, 10)
				forward := makeBlocks(r, []int{5}, 5)

				markFailed := errors.NewStorageError("mark failed")
				calls := 0

				res, err := netUnmined(context.Background(), 2, stream.of(back), 1, stream.of(forward),
					paramsFor(t, mode.maxInMemory, forward, 10), func([]chainhash.Hash) error {
						calls++

						return markFailed
					})
				require.ErrorIs(t, err, markFailed, "the emit error is the one returned, not a read it cancelled")
				require.Equal(t, 1, calls, "no chunk after a failed one")
				require.False(t, res.ForwardIncomplete)
				require.Empty(t, res.SkippedBackBlocks, "a failed emit is not an unreadable block")
			})
		}
	}
}

func TestNetUnmined_CancelledContextStops(t *testing.T) {
	for _, mode := range netUnminedModes {
		t.Run(mode.name, func(t *testing.T) {
			r := rand.New(rand.NewSource(7))
			back := makeBlocks(r, []int{100}, 10)
			forward := makeBlocks(r, []int{100}, 10)

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			res, err := netUnmined(ctx, 1, streamOf(back), 1, streamOf(forward),
				paramsFor(t, mode.maxInMemory, forward, 10), func([]chainhash.Hash) error { return nil })
			require.ErrorIs(t, err, context.Canceled)
			require.False(t, res.ForwardIncomplete, "a cancelled read is not an unreadable block")
			require.Empty(t, res.SkippedBackBlocks)
		})
	}
}

// A forward set that fits in memory needs no disk at all, so a reset of ordinary
// size works where no scratch directory can be made.
func TestNetUnmined_InMemoryTouchesNoDisk(t *testing.T) {
	r := rand.New(rand.NewSource(15))
	back := makeBlocks(r, []int{100}, 10)
	forward := makeBlocks(r, []int{100}, 10)
	overlap(append(back, back[0]), forward)

	nowhere := filepath.Join(t.TempDir(), "not", "there")

	var got []chainhash.Hash

	_, err := netUnmined(context.Background(), 1, streamOf(back), 1, streamOf(forward),
		netUnminedParams{Dir: nowhere, ForwardCount: 100, MaxForwardInMemory: 100, ChunkSize: 10}, collect(&got))
	require.NoError(t, err)
	require.ElementsMatch(t, referenceNetUnmined(back, forward, nil), got)

	// One hash more than fits and the scratch directory is needed.
	got = nil

	res, err := netUnmined(context.Background(), 1, streamOf(back), 1, streamOf(forward),
		netUnminedParams{Dir: nowhere, ForwardCount: 101, MaxForwardInMemory: 100, ChunkSize: 10}, collect(&got))
	require.Error(t, err)
	require.Empty(t, got)
	require.False(t, res.ForwardIncomplete, "a scratch directory that cannot be made is not an unreadable block")
}

func TestNetUnmined_OnDiskSplitsTheForwardSetIntoRanges(t *testing.T) {
	r := rand.New(rand.NewSource(16))
	back := makeBlocks(r, []int{50}, 10)
	forward := makeBlocks(r, []int{100}, 10)
	dir := t.TempDir()

	var files []string

	_, err := netUnmined(context.Background(), 1, streamOf(back), 1, streamOf(forward),
		netUnminedParams{Dir: dir, ForwardCount: 100, MaxForwardInMemory: 30, ChunkSize: 100}, func([]chainhash.Hash) error {
			if files == nil {
				files, _ = filepath.Glob(filepath.Join(dir, netUnminedScratchPrefix+"*", "forward-*"))
			}

			return nil
		})
	require.NoError(t, err)
	require.Len(t, files, 4, "100 forward hashes at 30 a range need 4 ranges")
}

func TestNetUnmined_RemovesItsScratchFiles(t *testing.T) {
	r := rand.New(rand.NewSource(8))
	back := makeBlocks(r, []int{100}, 10)
	forward := makeBlocks(r, []int{100}, 10)
	dir := t.TempDir()
	p := netUnminedParams{Dir: dir, ForwardCount: 100, MaxForwardInMemory: 25, ChunkSize: 10}

	_, err := netUnmined(context.Background(), 1, streamOf(back), 1, streamOf(forward), p, func([]chainhash.Hash) error { return nil })
	require.NoError(t, err)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)

	// And on failure too.
	forward[0].failAfter = 1
	_, err = netUnmined(context.Background(), 1, streamOf(back), 1, streamOf(forward), p, func([]chainhash.Hash) error { return nil })
	require.Error(t, err)

	entries, err = os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

// The scratch files are only a means: once every chunk is emitted, a directory
// that cannot be removed is reported, and does not fail a finished computation.
func TestNetUnmined_ScratchThatCannotBeRemovedDoesNotFail(t *testing.T) {
	old := netUnminedRemoveAll
	netUnminedRemoveAll = func(string) error { return errors.NewStorageError("device busy") }

	t.Cleanup(func() { netUnminedRemoveAll = old })

	r := rand.New(rand.NewSource(17))
	back := makeBlocks(r, []int{40}, 10)
	forward := makeBlocks(r, []int{40}, 10)

	var got []chainhash.Hash

	res, err := netUnmined(context.Background(), 1, streamOf(back), 1, streamOf(forward),
		netUnminedParams{Dir: t.TempDir(), ForwardCount: 40, MaxForwardInMemory: 10, ChunkSize: 10}, collect(&got))
	require.NoError(t, err)
	require.Len(t, got, 40)
	require.Error(t, res.ScratchLeft)
}

func TestNetUnmined_BothSidesMayBeStreamedConcurrently(t *testing.T) {
	for _, mode := range netUnminedModes {
		t.Run(mode.name, func(t *testing.T) {
			r := rand.New(rand.NewSource(9))
			back := makeBlocks(r, []int{400, 400}, 40)
			forward := makeBlocks(r, []int{600}, 20)

			for i := 0; i < len(back[0].hashes); i += 2 {
				forward[0].hashes[i%600] = back[0].hashes[i]
			}

			var got []chainhash.Hash

			res, err := netUnmined(context.Background(), 2, concurrentStreamOf(back), 1, concurrentStreamOf(forward),
				paramsFor(t, mode.maxInMemory, forward, 33), collect(&got))
			require.NoError(t, err)
			require.ElementsMatch(t, referenceNetUnmined(back, forward, nil), got)
			require.EqualValues(t, 800, res.Back)
			require.EqualValues(t, 600, res.Forward)
		})
	}
}

func TestNetUnmined_NoForwardBlocksEmitsEveryBackHash(t *testing.T) {
	r := rand.New(rand.NewSource(10))
	back := makeBlocks(r, []int{120, 80}, 30)

	var got []chainhash.Hash

	_, err := netUnmined(context.Background(), 2, streamOf(back), 0, nil,
		netUnminedParams{Dir: filepath.Join(t.TempDir(), "unused"), ChunkSize: 25}, collect(&got))
	require.NoError(t, err)
	require.ElementsMatch(t, append(append([]chainhash.Hash{}, back[0].hashes...), back[1].hashes...), got)
}

func TestNetUnminedRangeBits(t *testing.T) {
	require.EqualValues(t, 0, netUnminedRangeBits(0, 1000))
	require.EqualValues(t, 0, netUnminedRangeBits(1000, 1000))
	require.EqualValues(t, 1, netUnminedRangeBits(1001, 1000))
	require.EqualValues(t, 7, netUnminedRangeBits(4_082_589_033, 32_000_000), "the scale-3 forward range: 128 ranges of ~32M")
	require.EqualValues(t, maxNetUnminedRangeBits, netUnminedRangeBits(1<<62, 1), "capped")
}

func TestNetUnminedRangeOf_UsesTheLeadingBits(t *testing.T) {
	var h chainhash.Hash

	h[0], h[1] = 0b1010_0000, 0xff
	require.EqualValues(t, 0, netUnminedRangeOf(h, 0))
	require.EqualValues(t, 1, netUnminedRangeOf(h, 1))
	require.EqualValues(t, 0b101, netUnminedRangeOf(h, 3))
	require.EqualValues(t, 0b1010_0000_11, netUnminedRangeOf(h, 10))
}

func TestNetUnmined_DuplicatesAreEmittedAsOftenAsTheyAppear(t *testing.T) {
	for _, mode := range netUnminedModes {
		t.Run(mode.name, func(t *testing.T) {
			r := rand.New(rand.NewSource(11))
			back := makeBlocks(r, []int{60, 40}, 25)
			forward := makeBlocks(r, []int{30}, 10)

			// back repeats a hash inside a block and across blocks; forward repeats one too
			back[0].hashes[10] = back[0].hashes[3]
			back[1].hashes[5] = back[0].hashes[3]
			back[1].hashes[6] = back[0].hashes[20]
			forward[0].hashes[0] = back[0].hashes[20]
			forward[0].hashes[1] = back[0].hashes[20]
			forward[0].hashes[2] = back[1].hashes[30]

			var got []chainhash.Hash

			_, err := netUnmined(context.Background(), 2, streamOf(back), 1, streamOf(forward),
				paramsFor(t, mode.maxInMemory, forward, 9), collect(&got))
			require.NoError(t, err)
			require.ElementsMatch(t, referenceNetUnmined(back, forward, nil), got)
		})
	}
}

func TestNetUnmined_BeatsInsideALongRange(t *testing.T) {
	old := netUnminedBeatEvery
	netUnminedBeatEvery = 1000

	t.Cleanup(func() { netUnminedBeatEvery = old })

	r := rand.New(rand.NewSource(12))
	back := makeBlocks(r, []int{100_000}, 50_000)
	forward := makeBlocks(r, []int{100_000}, 50_000)

	beats := 0

	_, err := netUnmined(context.Background(), 1, streamOf(back), 1, streamOf(forward),
		netUnminedParams{Dir: t.TempDir(), ForwardCount: 100_000, MaxForwardInMemory: 50_000, ChunkSize: 1_000_000, Beat: func() { beats++ }},
		func([]chainhash.Hash) error { return nil })
	require.NoError(t, err)

	// Two ranges hold every record. Without a beat inside the loops that is a
	// handful of beats in total; a liveness probe would see none for a whole range.
	require.Greater(t, beats, 150)
}

func TestNetUnmined_RemovesScratchDirsLeftByAnEarlierRun(t *testing.T) {
	dir := t.TempDir()

	stale := filepath.Join(dir, netUnminedScratchPrefix+"1111-abc")
	require.NoError(t, os.MkdirAll(stale, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(stale, "back-0"), []byte("x"), 0o600))

	unrelated := filepath.Join(dir, "ba-txmap-disk0-1-2")
	require.NoError(t, os.MkdirAll(unrelated, 0o750))

	// A directory carrying this process's own start time is another reset's in
	// this process, so it stays.
	ours := filepath.Join(dir, netUnminedScratchName+"inuse")
	require.NoError(t, os.MkdirAll(ours, 0o750))

	back := makeBlocks(rand.New(rand.NewSource(14)), []int{5}, 5)

	// Without SweepStale nothing is touched: the directory may be shared.
	_, err := netUnmined(context.Background(), 1, streamOf(back), 0, nil,
		netUnminedParams{Dir: dir, ChunkSize: 5}, func([]chainhash.Hash) error { return nil })
	require.NoError(t, err)
	require.DirExists(t, stale)

	// With it, a reset of any size sweeps, including one that needs no scratch
	// itself: what an earlier, larger reset left is not held until the next large one.
	beats := 0

	_, err = netUnmined(context.Background(), 0, nil, 0, nil,
		netUnminedParams{Dir: dir, ChunkSize: 10, SweepStale: true, Beat: func() { beats++ }}, func([]chainhash.Hash) error { return nil })
	require.NoError(t, err)

	require.NoDirExists(t, stale)
	require.DirExists(t, unrelated, "only this feature's own scratch directories are swept")
	require.DirExists(t, ours, "a directory of this process is never swept")
	require.Equal(t, 2, beats, "the sweep, the only work here, has a beat before and after")
}

// A scratch file that cannot be written is ours, not a block that cannot be
// read: it must not look like an incomplete forward set, which skips the marker,
// nor like a move-back block to leave out.
func TestNetUnmined_AScratchWriteThatFailsIsReturned(t *testing.T) {
	for _, side := range []string{"forward", "back"} {
		for _, stream := range []struct {
			name string
			of   func([]blockHashes) txHashStream
		}{{"sequential", streamOf}, {"concurrent", concurrentStreamOf}} {
			t.Run(side+", "+stream.name, func(t *testing.T) {
				r := rand.New(rand.NewSource(18))
				back := makeBlocks(r, []int{40}, 10)
				forward := makeBlocks(r, []int{40}, 10)

				diskFull := errors.NewStorageError("no space left on device")
				var writes atomic.Int64

				old := netUnminedWriteScratch
				netUnminedWriteScratch = func(w *bufio.Writer, rec []byte) (int, error) {
					// The first 40 writes are the forward side's.
					if n := writes.Add(1); side == "forward" || n > 40 {
						return 0, diskFull
					}

					return w.Write(rec)
				}

				t.Cleanup(func() { netUnminedWriteScratch = old })

				emitted := 0

				res, err := netUnmined(context.Background(), 1, stream.of(back), 1, stream.of(forward),
					netUnminedParams{Dir: t.TempDir(), ForwardCount: 40, MaxForwardInMemory: 39, ChunkSize: 10},
					func(chunk []chainhash.Hash) error {
						emitted += len(chunk)
						return nil
					})
				require.ErrorIs(t, err, diskFull)
				require.False(t, res.ForwardIncomplete)
				require.Empty(t, res.SkippedBackBlocks)
				require.Zero(t, emitted)
			})
		}
	}
}

func TestForEachScratchHash_ATruncatedRecordIsAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "back-0")
	require.NoError(t, os.WriteFile(path, make([]byte, chainhash.HashSize+8), 0o600))

	seen := 0
	err := forEachScratchHash(path, func() {}, nil, func(h []chainhash.Hash) error {
		seen += len(h)
		return nil
	})
	require.Error(t, err)
	require.Zero(t, seen, "nothing of a file that cannot be read in full is used")
}

// An error a stream returns is not an unreadable block: a store that failed to
// answer must not skip the marker or leave a block out.
func TestNetUnmined_AStreamErrorIsReturned(t *testing.T) {
	for _, mode := range netUnminedModes {
		for _, side := range []string{"forward", "back"} {
			t.Run(mode.name+", "+side, func(t *testing.T) {
				r := rand.New(rand.NewSource(19))
				back := makeBlocks(r, []int{40}, 10)
				forward := makeBlocks(r, []int{40}, 10)

				storeDown := errors.NewServiceError("subtree store unreachable")
				failing := func(context.Context, int, func([]chainhash.Hash) error) (error, error) { return nil, storeDown }

				backStream, forwardStream := streamOf(back), streamOf(forward)
				if side == "back" {
					backStream = failing
				} else {
					forwardStream = failing
				}

				emitted := 0

				res, err := netUnmined(context.Background(), 1, backStream, 1, forwardStream,
					paramsFor(t, mode.maxInMemory, forward, 100), func(chunk []chainhash.Hash) error {
						emitted += len(chunk)
						return nil
					})
				require.ErrorIs(t, err, storeDown)
				require.False(t, res.ForwardIncomplete)
				require.Empty(t, res.SkippedBackBlocks)
				require.Zero(t, emitted)
			})
		}
	}
}
