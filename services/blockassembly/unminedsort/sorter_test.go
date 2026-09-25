package unminedsort

import (
	"cmp"
	"context"
	"encoding/binary"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

type input struct {
	createdAt int64
	node      subtreepkg.Node
	inpoints  subtreepkg.TxInpoints
}

func hashOf(i int) chainhash.Hash {
	var h chainhash.Hash
	binary.LittleEndian.PutUint64(h[:], uint64(i)+1)

	return h
}

// makeInputs builds n txs whose createdAt values collide heavily, so both the
// ordering and the tie handling get exercised.
func makeInputs(n int, withInpoints bool) []input {
	rng := rand.New(rand.NewPCG(1, 2))
	in := make([]input, n)

	for i := range in {
		in[i] = input{
			createdAt: 1_700_000_000_000 + rng.Int64N(int64(n/8+1)),
			node: subtreepkg.Node{
				Hash:        hashOf(i),
				Fee:         rng.Uint64N(1 << 40),
				SizeInBytes: 100 + rng.Uint64N(1<<20),
			},
		}

		if withInpoints && i > 0 {
			in[i].inpoints = subtreepkg.NewTxInpointsFromPacked(
				[]chainhash.Hash{hashOf(i - 1)},
				[]uint32{1, uint32(i % 7)},
			)
		}
	}

	return in
}

// expectedOrder is the reference: stable sort by createdAt, insertion order on ties.
func expectedOrder(in []input) []int {
	idx := make([]int, len(in))
	for i := range idx {
		idx[i] = i
	}

	// insertion sort would be O(n²); use a stable sort on a copy.
	sortStableByCreatedAt(idx, in)

	return idx
}

type drained struct {
	createdAt int64
	node      subtreepkg.Node
	inpoints  *subtreepkg.TxInpoints
}

func drainAll(t *testing.T, s *Sorter, batchSize int) ([]drained, [][]int64) {
	t.Helper()

	var (
		out     []drained
		batches [][]int64
	)

	err := s.Drain(context.Background(), batchSize, func(batch []*utxo.UnminedTransaction) error {
		created := make([]int64, 0, len(batch))
		for _, tx := range batch {
			out = append(out, drained{createdAt: int64(tx.CreatedAt), node: *tx.Node, inpoints: tx.TxInpoints})
			created = append(created, int64(tx.CreatedAt))
		}

		batches = append(batches, created)

		return nil
	})
	require.NoError(t, err)

	return out, batches
}

func requireOrder(t *testing.T, in []input, out []drained, withInpoints bool) {
	t.Helper()

	want := expectedOrder(in)
	require.Len(t, out, len(want))

	for pos, i := range want {
		require.Equal(t, in[i].createdAt, out[pos].createdAt, "createdAt at %d", pos)
		require.Equal(t, in[i].node, out[pos].node, "node at %d", pos)
		require.NotNil(t, out[pos].inpoints)

		if withInpoints {
			require.Equal(t, in[i].inpoints.GetParentTxHashes(), out[pos].inpoints.GetParentTxHashes(), "inpoints at %d", pos)
			require.Equal(t, in[i].inpoints.GetTxInpoints(), out[pos].inpoints.GetTxInpoints(), "inpoints at %d", pos)
		} else {
			require.Empty(t, out[pos].inpoints.GetParentTxHashes())
		}
	}
}

// A batch may only end where createdAt changes, so the caller can reorder each
// equal-createdAt group (parents first) without it straddling two batches.
func requireBatchesCutAtGroupBoundaries(t *testing.T, batches [][]int64) {
	t.Helper()

	for i := 1; i < len(batches); i++ {
		prev := batches[i-1]
		require.NotEqual(t, prev[len(prev)-1], batches[i][0], "batch %d splits a createdAt group", i)
	}
}

func addAll(t *testing.T, s *Sorter, in []input, withInpoints bool) {
	t.Helper()

	for i := range in {
		var inp *subtreepkg.TxInpoints
		if withInpoints {
			inp = &in[i].inpoints
		}

		require.NoError(t, s.Add(in[i].createdAt, in[i].node, inp))
	}
}

func countRunFiles(t *testing.T, dir string) int {
	t.Helper()

	n := 0

	err := filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if !d.IsDir() {
			n++
		}

		return nil
	})
	require.NoError(t, err)

	return n
}

func TestSorter_InMemory(t *testing.T) {
	in := makeInputs(20_000, false)

	s, err := New(Options{BufferRecords: 1 << 30})
	require.NoError(t, err)

	defer s.Close()

	addAll(t, s, in, false)
	require.Equal(t, len(in), s.Len())

	out, batches := drainAll(t, s, 1000)
	requireOrder(t, in, out, false)
	requireBatchesCutAtGroupBoundaries(t, batches)
}

func TestSorter_SpillsAndMerges(t *testing.T) {
	for _, withInpoints := range []bool{false, true} {
		t.Run(map[bool]string{false: "no inpoints", true: "inpoints"}[withInpoints], func(t *testing.T) {
			dir := t.TempDir()
			in := makeInputs(50_000, withInpoints)

			s, err := New(Options{Dirs: []string{dir}, BufferRecords: 3_000, WithInpoints: withInpoints})
			require.NoError(t, err)

			addAll(t, s, in, withInpoints)
			require.Positive(t, countRunFiles(t, dir), "expected spilled run files")

			out, batches := drainAll(t, s, 777)
			requireOrder(t, in, out, withInpoints)
			requireBatchesCutAtGroupBoundaries(t, batches)

			require.NoError(t, s.Close())
			require.Zero(t, countRunFiles(t, dir), "run files must be removed on Close")
		})
	}
}

// Without a spill directory the sorter must keep everything in memory, however
// far past the buffer threshold it grows.
func TestSorter_NoDirNeverSpills(t *testing.T) {
	in := makeInputs(10_000, false)

	s, err := New(Options{BufferRecords: 100})
	require.NoError(t, err)

	defer s.Close()

	addAll(t, s, in, false)

	out, _ := drainAll(t, s, 500)
	requireOrder(t, in, out, false)
}

// Sorted runs store createdAt as a delta and fee/size as varints; a typical
// record must stay well under the raw 56 bytes so 10B txs fit on local NVMe.
func TestSorter_RunEncodingIsCompact(t *testing.T) {
	dir := t.TempDir()
	in := makeInputs(100_000, false)

	for i := range in {
		in[i].node.Fee = 50 + uint64(i%1000)
		in[i].node.SizeInBytes = 200 + uint64(i%500)
	}

	s, err := New(Options{Dirs: []string{dir}, BufferRecords: 10_000})
	require.NoError(t, err)

	defer s.Close()

	addAll(t, s, in, false)

	var total int64

	err = filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		total += info.Size()

		return nil
	})
	require.NoError(t, err)

	perRecord := float64(total) / float64(len(in)-s.buffered())
	require.Less(t, perRecord, 40.0)
}

// Every drained tx must get its own TxInpoints: the subtree processor keeps the
// pointer in its tx map (and DiskTxMap hands it to an async writer).
func TestSorter_FreshInpointsPerTx(t *testing.T) {
	dir := t.TempDir()
	in := makeInputs(5_000, false)

	s, err := New(Options{Dirs: []string{dir}, BufferRecords: 1_000})
	require.NoError(t, err)

	defer s.Close()

	addAll(t, s, in, false)

	seen := make(map[*subtreepkg.TxInpoints]struct{}, len(in))
	err = s.Drain(context.Background(), 256, func(batch []*utxo.UnminedTransaction) error {
		for _, tx := range batch {
			_, dup := seen[tx.TxInpoints]
			require.False(t, dup, "TxInpoints pointer reused")
			seen[tx.TxInpoints] = struct{}{}
		}

		return nil
	})
	require.NoError(t, err)
	require.Len(t, seen, len(in))
}

func TestSorter_CallbackErrorAborts(t *testing.T) {
	in := makeInputs(10_000, false)

	s, err := New(Options{Dirs: []string{t.TempDir()}, BufferRecords: 1_000})
	require.NoError(t, err)

	defer s.Close()

	addAll(t, s, in, false)

	boom := errors.NewProcessingError("boom")
	calls := 0

	err = s.Drain(context.Background(), 100, func([]*utxo.UnminedTransaction) error {
		calls++
		return boom
	})
	require.ErrorIs(t, err, boom)
	require.Equal(t, 1, calls)
}

func TestSorter_ContextCancelAborts(t *testing.T) {
	in := makeInputs(10_000, false)

	s, err := New(Options{Dirs: []string{t.TempDir()}, BufferRecords: 1_000})
	require.NoError(t, err)

	defer s.Close()

	addAll(t, s, in, false)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = s.Drain(ctx, 100, func([]*utxo.UnminedTransaction) error { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// A crash mid-load leaves run files behind; the next sorter on the same
// directory must remove them instead of letting them pile up on the disk.
func TestSorter_SweepsStaleRunDirs(t *testing.T) {
	dir := t.TempDir()

	stale := filepath.Join(dir, runDirPrefix+"stale")
	require.NoError(t, os.MkdirAll(stale, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(stale, "0.run"), []byte("x"), 0o600))

	unrelated := filepath.Join(dir, "keep-me")
	require.NoError(t, os.WriteFile(unrelated, []byte("x"), 0o600))

	s, err := New(Options{Dirs: []string{dir}, BufferRecords: 10})
	require.NoError(t, err)

	defer s.Close()

	_, err = os.Stat(stale)
	require.True(t, os.IsNotExist(err), "stale run dir not swept")

	_, err = os.Stat(unrelated)
	require.NoError(t, err, "unrelated file removed")
}

func TestSorter_Empty(t *testing.T) {
	s, err := New(Options{Dirs: []string{t.TempDir()}, BufferRecords: 10})
	require.NoError(t, err)

	defer s.Close()

	calls := 0
	require.NoError(t, s.Drain(context.Background(), 10, func([]*utxo.UnminedTransaction) error {
		calls++
		return nil
	}))
	require.Zero(t, calls)
}

func BenchmarkSorter_SpillAndDrain(b *testing.B) {
	in := makeInputs(2_000_000, false)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		s, err := New(Options{Dirs: []string{b.TempDir()}, BufferRecords: 250_000})
		if err != nil {
			b.Fatal(err)
		}

		for j := range in {
			if err := s.Add(in[j].createdAt, in[j].node, nil); err != nil {
				b.Fatal(err)
			}
		}

		if err := s.Drain(context.Background(), 1<<20, func([]*utxo.UnminedTransaction) error { return nil }); err != nil {
			b.Fatal(err)
		}

		_ = s.Close()
	}

	b.ReportMetric(float64(b.N*len(in))/b.Elapsed().Seconds(), "records/s")
}

func sortStableByCreatedAt(idx []int, in []input) {
	slices.SortStableFunc(idx, func(a, b int) int {
		return cmp.Compare(in[a].createdAt, in[b].createdAt)
	})
}

// Runs are spread round-robin over every spill directory so capacity and I/O
// bandwidth scale with the number of local disks.
func TestSorter_SpreadsRunsAcrossDirs(t *testing.T) {
	dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	in := makeInputs(30_000, false)

	s, err := New(Options{Dirs: dirs, BufferRecords: 2_000})
	require.NoError(t, err)

	addAll(t, s, in, false)

	for _, d := range dirs {
		require.Positive(t, countRunFiles(t, d), "no runs in %s", d)
	}

	out, _ := drainAll(t, s, 1000)
	requireOrder(t, in, out, false)

	require.NoError(t, s.Close())

	for _, d := range dirs {
		require.Zero(t, countRunFiles(t, d), "run files left in %s", d)
	}
}

// The same directory listed twice (or with a trailing separator, which yields
// an empty entry) must not make the sweep delete the run directory created for
// an earlier entry.
func TestSorter_DuplicateAndEmptyDirs(t *testing.T) {
	d := t.TempDir()
	in := makeInputs(5_000, false)

	s, err := New(Options{Dirs: []string{d, d + "/", "", filepath.Join(d, ".")}, BufferRecords: 500})
	require.NoError(t, err)

	addAll(t, s, in, false)

	out, _ := drainAll(t, s, 1000)
	requireOrder(t, in, out, false)

	require.NoError(t, s.Close())
	require.Zero(t, countRunFiles(t, d))
}

// Drain must not keep the last spilled buffer alive: it is as large as the
// active one and the merge runs while the subtree processor's tx map grows.
func TestSorter_DrainReleasesSpareBuffer(t *testing.T) {
	in := makeInputs(5_000, false)

	s, err := New(Options{Dirs: []string{t.TempDir()}, BufferRecords: 1_000})
	require.NoError(t, err)

	defer s.Close()

	addAll(t, s, in, false)

	require.NoError(t, s.Drain(context.Background(), 1000, func([]*utxo.UnminedTransaction) error { return nil }))
	require.Nil(t, s.spare)
}
