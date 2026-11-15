package blockassembly

import (
	"context"
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
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
	return func(ctx context.Context, block int, visit func([]chainhash.Hash) error) error {
		b := blocks[block]
		sent := 0

		for start := 0; start < len(b.hashes); start += b.subtreeLen {
			if b.failAfter > 0 && sent == b.failAfter {
				return errors.NewStorageError("subtree %d of block %d is gone", sent, block)
			}

			if err := ctx.Err(); err != nil {
				return err
			}

			// A fresh copy each call: the visitor must not retain the slice, and
			// a stream that reuses its buffer must not be relied on either.
			chunk := append([]chainhash.Hash(nil), b.hashes[start:min(start+b.subtreeLen, len(b.hashes))]...)
			if err := visit(chunk); err != nil {
				return err
			}

			sent++
		}

		return nil
	}
}

// referenceNetUnmined is the map the Reset used to build: every forward hash in
// one set, every back hash kept if it is not in it, in back order.
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

func TestNetUnminedInOrder_MatchesTheMapAndKeepsBackOrder(t *testing.T) {
	for _, tc := range []struct {
		name      string
		rangeBits uint
		chunk     int
	}{
		{"one range", 0, 7},
		{"few ranges, small chunks", 3, 5},
		{"many ranges, chunk larger than the input", 8, 100_000},
		{"max ranges", 10, 64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := rand.New(rand.NewSource(1))
			back := makeBlocks(r, []int{1000, 777, 1}, 100)
			forward := makeBlocks(r, []int{2500, 31, 900}, 64)
			overlap(back, forward)

			var got []chainhash.Hash

			res, err := netUnminedInOrder(context.Background(), len(back), streamOf(back), len(forward), streamOf(forward),
				netUnminedParams{Dir: t.TempDir(), RangeBits: tc.rangeBits, ChunkSize: tc.chunk}, collect(&got))
			require.NoError(t, err)

			want := referenceNetUnmined(back, forward, nil)
			require.NotEmpty(t, want)
			require.Less(t, len(want), 1000+777+1, "some back hashes must be dropped for the test to mean anything")
			require.Equal(t, want, got, "same hashes, same order as the map")
			require.EqualValues(t, len(want), res.Emitted)
			require.EqualValues(t, 1000+777+1, res.Back)
			require.EqualValues(t, 2500+31+900, res.Forward)
			require.Empty(t, res.SkippedBackBlocks)
		})
	}
}

func TestNetUnminedInOrder_ChunksAreBoundedAndContiguous(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	back := makeBlocks(r, []int{500}, 50)
	forward := makeBlocks(r, []int{10}, 10)

	var sizes []int

	var got []chainhash.Hash

	_, err := netUnminedInOrder(context.Background(), 1, streamOf(back), 1, streamOf(forward),
		netUnminedParams{Dir: t.TempDir(), RangeBits: 2, ChunkSize: 64}, func(chunk []chainhash.Hash) error {
			sizes = append(sizes, len(chunk))
			got = append(got, chunk...)

			return nil
		})
	require.NoError(t, err)
	require.Equal(t, back[0].hashes, got)

	for i, n := range sizes {
		require.LessOrEqual(t, n, 64)

		if i < len(sizes)-1 {
			require.Equal(t, 64, n, "only the last chunk may be short")
		}
	}
}

func TestNetUnminedInOrder_ABackBlockThatCannotBeReadIsLeftOutWhole(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	back := makeBlocks(r, []int{300, 300, 300}, 50)
	forward := makeBlocks(r, []int{200}, 50)
	overlap(back, forward)

	// The last block overlaps too, so a failed block that did not reserve its
	// positions would shift this one's bits onto the wrong transactions.
	for i := 0; i < len(back[2].hashes); i += 4 {
		forward[0].hashes[i%len(forward[0].hashes)] = back[2].hashes[i]
	}

	back[1].failAfter = 2 // two subtrees read, then the third is gone

	var got []chainhash.Hash

	res, err := netUnminedInOrder(context.Background(), len(back), streamOf(back), len(forward), streamOf(forward),
		netUnminedParams{Dir: t.TempDir(), RangeBits: 3, ChunkSize: 40}, collect(&got))
	require.NoError(t, err)
	require.Equal(t, []int{1}, res.SkippedBackBlocks)

	// Block 1 contributes nothing, not even its first two subtrees.
	back[1].failAfter = 0
	require.Equal(t, referenceNetUnmined(back, forward, map[int]bool{1: true}), got)
}

func TestNetUnminedInOrder_AForwardBlockThatCannotBeReadFailsTheWholeComputation(t *testing.T) {
	r := rand.New(rand.NewSource(4))
	back := makeBlocks(r, []int{100}, 50)
	forward := makeBlocks(r, []int{100, 100}, 50)
	forward[1].failAfter = 1

	emitted := 0

	res, err := netUnminedInOrder(context.Background(), 1, streamOf(back), 2, streamOf(forward),
		netUnminedParams{Dir: t.TempDir(), RangeBits: 2, ChunkSize: 10}, func(chunk []chainhash.Hash) error {
			emitted += len(chunk)

			return nil
		})
	require.Error(t, err)
	require.True(t, res.ForwardIncomplete)
	require.Zero(t, emitted, "an incomplete forward set must emit nothing: every back hash would look net unmined")
}

func TestNetUnminedInOrder_BackStreamFailingOnTheSecondReadFails(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	back := makeBlocks(r, []int{200}, 50)
	forward := makeBlocks(r, []int{50}, 50)

	calls := 0
	flaky := func(ctx context.Context, block int, visit func([]chainhash.Hash) error) error {
		calls++
		if calls == 2 {
			return errors.NewStorageError("gone on the second read")
		}

		return streamOf(back)(ctx, block, visit)
	}

	_, err := netUnminedInOrder(context.Background(), 1, flaky, 1, streamOf(forward),
		netUnminedParams{Dir: t.TempDir(), RangeBits: 1, ChunkSize: 10}, func([]chainhash.Hash) error { return nil })
	require.Error(t, err, "positions are only valid if both reads of a block agree")
}

func TestNetUnminedInOrder_EmitErrorStops(t *testing.T) {
	r := rand.New(rand.NewSource(6))
	back := makeBlocks(r, []int{100}, 50)
	forward := makeBlocks(r, []int{5}, 5)

	calls := 0

	_, err := netUnminedInOrder(context.Background(), 1, streamOf(back), 1, streamOf(forward),
		netUnminedParams{Dir: t.TempDir(), RangeBits: 1, ChunkSize: 10}, func([]chainhash.Hash) error {
			calls++

			return errors.NewStorageError("mark failed")
		})
	require.Error(t, err)
	require.Equal(t, 1, calls, "no chunk after a failed one")
}

func TestNetUnminedInOrder_CancelledContextStops(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	back := makeBlocks(r, []int{100}, 10)
	forward := makeBlocks(r, []int{100}, 10)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := netUnminedInOrder(ctx, 1, streamOf(back), 1, streamOf(forward),
		netUnminedParams{Dir: t.TempDir(), RangeBits: 1, ChunkSize: 10}, func([]chainhash.Hash) error { return nil })
	require.Error(t, err)
}

func TestNetUnminedInOrder_RemovesItsScratchFiles(t *testing.T) {
	r := rand.New(rand.NewSource(8))
	back := makeBlocks(r, []int{100}, 10)
	forward := makeBlocks(r, []int{100}, 10)
	dir := t.TempDir()

	_, err := netUnminedInOrder(context.Background(), 1, streamOf(back), 1, streamOf(forward),
		netUnminedParams{Dir: dir, RangeBits: 2, ChunkSize: 10}, func([]chainhash.Hash) error { return nil })
	require.NoError(t, err)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)

	// And on failure too.
	forward[0].failAfter = 1
	_, err = netUnminedInOrder(context.Background(), 1, streamOf(back), 1, streamOf(forward),
		netUnminedParams{Dir: dir, RangeBits: 2, ChunkSize: 10}, func([]chainhash.Hash) error { return nil })
	require.Error(t, err)

	entries, err = os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestNetUnminedInOrder_ForwardMayBeStreamedConcurrently(t *testing.T) {
	r := rand.New(rand.NewSource(9))
	back := makeBlocks(r, []int{400}, 40)
	forward := makeBlocks(r, []int{600}, 20)

	for i := 0; i < len(back[0].hashes); i += 2 {
		forward[0].hashes[i%600] = back[0].hashes[i]
	}

	concurrent := func(ctx context.Context, block int, visit func([]chainhash.Hash) error) error {
		b := forward[block]

		var wg sync.WaitGroup

		errs := make(chan error, len(b.hashes))

		for start := 0; start < len(b.hashes); start += b.subtreeLen {
			wg.Add(1)

			go func(chunk []chainhash.Hash) {
				defer wg.Done()

				if err := visit(chunk); err != nil {
					errs <- err
				}
			}(append([]chainhash.Hash(nil), b.hashes[start:min(start+b.subtreeLen, len(b.hashes))]...))
		}

		wg.Wait()
		close(errs)

		return <-errs
	}

	var got []chainhash.Hash

	_, err := netUnminedInOrder(context.Background(), 1, streamOf(back), 1, concurrent,
		netUnminedParams{Dir: t.TempDir(), RangeBits: 3, ChunkSize: 33}, collect(&got))
	require.NoError(t, err)
	require.Equal(t, referenceNetUnmined(back, forward, nil), got)
}

func TestNetUnminedInOrder_NoForwardBlocksEmitsEveryBackHashInOrder(t *testing.T) {
	r := rand.New(rand.NewSource(10))
	back := makeBlocks(r, []int{120, 80}, 30)

	var got []chainhash.Hash

	_, err := netUnminedInOrder(context.Background(), 2, streamOf(back), 0, nil,
		netUnminedParams{Dir: t.TempDir(), RangeBits: 2, ChunkSize: 25}, collect(&got))
	require.NoError(t, err)
	require.Equal(t, append(append([]chainhash.Hash{}, back[0].hashes...), back[1].hashes...), got)
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

	// The record layout is part of the contract with the pass that reads it back.
	var rec [netUnminedBackRecordSize]byte

	binary.LittleEndian.PutUint64(rec[:8], 42)
	require.Equal(t, 40, len(rec))

	_ = filepath.Join // keep the import used if the layout test grows
}

func TestNetUnminedInOrder_DuplicatesGetTheirOwnPositions(t *testing.T) {
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

	_, err := netUnminedInOrder(context.Background(), 2, streamOf(back), 1, streamOf(forward),
		netUnminedParams{Dir: t.TempDir(), RangeBits: 2, ChunkSize: 9}, collect(&got))
	require.NoError(t, err)
	require.Equal(t, referenceNetUnmined(back, forward, nil), got)
}

func TestNetUnminedInOrder_BeatsInsideALongRange(t *testing.T) {
	old := netUnminedBeatEvery
	netUnminedBeatEvery = 1000

	t.Cleanup(func() { netUnminedBeatEvery = old })

	r := rand.New(rand.NewSource(12))
	back := makeBlocks(r, []int{100_000}, 50_000)
	forward := makeBlocks(r, []int{100_000}, 50_000)

	beats := 0

	_, err := netUnminedInOrder(context.Background(), 1, streamOf(back), 1, streamOf(forward),
		netUnminedParams{Dir: t.TempDir(), RangeBits: 0, ChunkSize: 1_000_000, Beat: func() { beats++ }},
		func([]chainhash.Hash) error { return nil })
	require.NoError(t, err)

	// One range holds every record. Without a beat inside the loops that is a
	// handful of beats in total; a liveness probe would see none for the whole range.
	require.Greater(t, beats, 150)
}

func TestNetUnminedInOrder_RemovesScratchDirsLeftByAnEarlierRun(t *testing.T) {
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

	// Without SweepStale nothing is touched: the directory may be shared.
	_, err := netUnminedInOrder(context.Background(), 1, streamOf(makeBlocks(rand.New(rand.NewSource(14)), []int{5}, 5)), 0, nil,
		netUnminedParams{Dir: dir, ChunkSize: 5}, func([]chainhash.Hash) error { return nil })
	require.NoError(t, err)
	require.DirExists(t, stale)

	r := rand.New(rand.NewSource(13))
	back := makeBlocks(r, []int{10}, 10)

	_, err = netUnminedInOrder(context.Background(), 1, streamOf(back), 0, nil,
		netUnminedParams{Dir: dir, RangeBits: 0, ChunkSize: 10, SweepStale: true}, func([]chainhash.Hash) error { return nil })
	require.NoError(t, err)

	require.NoDirExists(t, stale)
	require.DirExists(t, unrelated, "only this feature's own scratch directories are swept")
	require.DirExists(t, ours, "a directory of this process is never swept")
}
