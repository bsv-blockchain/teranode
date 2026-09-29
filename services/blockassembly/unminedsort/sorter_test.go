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

func TestSorter_InvalidOptions(t *testing.T) {
	_, err := New(Options{BufferRecords: 0})
	require.Error(t, err)

	// A sort "directory" that is a file.
	file := filepath.Join(t.TempDir(), "not-a-dir")
	require.NoError(t, os.WriteFile(file, []byte("x"), 0o600))

	_, err = New(Options{Dirs: []string{file}, BufferRecords: 10})
	require.Error(t, err)
}

func TestSorter_AddAndDrainAfterDrain(t *testing.T) {
	s, err := New(Options{BufferRecords: 10})
	require.NoError(t, err)

	defer s.Close()

	require.NoError(t, s.Drain(context.Background(), 10, func([]*utxo.UnminedTransaction) error { return nil }))
	require.Error(t, s.Add(1, subtreepkg.Node{}, nil), "Add after Drain")
	require.Error(t, s.Drain(context.Background(), 10, func([]*utxo.UnminedTransaction) error { return nil }), "second Drain")
}

// A spill that can't be written must fail the sorter, not lose the run.
func TestSorter_SpillWriteFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}

	dir := t.TempDir()

	s, err := New(Options{Dirs: []string{dir}, BufferRecords: 100})
	require.NoError(t, err)

	defer func() {
		for _, d := range s.dirs {
			_ = os.Chmod(d, 0o755)
		}

		_ = s.Close()
	}()

	for _, d := range s.dirs {
		require.NoError(t, os.Chmod(d, 0o500))
	}

	in := makeInputs(1000, false)

	var addErr error
	for i := range in {
		if addErr = s.Add(in[i].createdAt, in[i].node, nil); addErr != nil {
			break
		}
	}

	if addErr == nil {
		addErr = s.Drain(context.Background(), 100, func([]*utxo.UnminedTransaction) error { return nil })
	}

	require.Error(t, addErr)
}

// A run cut short on disk must fail the drain rather than silently dropping
// the missing transactions.
func TestSorter_TruncatedRun(t *testing.T) {
	for _, withInpoints := range []bool{false, true} {
		t.Run(map[bool]string{false: "no inpoints", true: "inpoints"}[withInpoints], func(t *testing.T) {
			dir := t.TempDir()
			in := makeInputs(3000, withInpoints)

			s, err := New(Options{Dirs: []string{dir}, BufferRecords: 1000, WithInpoints: withInpoints})
			require.NoError(t, err)

			defer s.Close()

			addAll(t, s, in, withInpoints)
			require.NoError(t, s.waitSpill())
			require.NotEmpty(t, s.runs)

			info, err := os.Stat(s.runs[0])
			require.NoError(t, err)
			require.NoError(t, os.Truncate(s.runs[0], info.Size()-5))

			err = s.Drain(context.Background(), 500, func([]*utxo.UnminedTransaction) error { return nil })
			require.Error(t, err)
		})
	}
}

// Inpoints that no longer decode must fail the drain.
func TestSorter_CorruptInpoints(t *testing.T) {
	s, err := New(Options{BufferRecords: 1 << 20, WithInpoints: true})
	require.NoError(t, err)

	defer s.Close()

	in := makeInputs(10, true)
	addAll(t, s, in, true)

	for i := range s.cur.arena {
		s.cur.arena[i] = 0xFF
	}

	err = s.Drain(context.Background(), 10, func([]*utxo.UnminedTransaction) error { return nil })
	require.Error(t, err)
}

// A run that ends cleanly at a record boundary but holds fewer records than
// were written to it (cut to nothing, or to its header) must fail the drain:
// a silently dropped parent leaves an orphan child in the template.
func TestSorter_RunCutAtRecordBoundary(t *testing.T) {
	for _, withInpoints := range []bool{false, true} {
		for _, keep := range []int64{0, runHeaderSize} {
			t.Run(map[bool]string{false: "no inpoints", true: "inpoints"}[withInpoints]+map[int64]string{0: " empty", runHeaderSize: " header only"}[keep], func(t *testing.T) {
				dir := t.TempDir()
				in := makeInputs(3000, withInpoints)

				s, err := New(Options{Dirs: []string{dir}, BufferRecords: 1000, WithInpoints: withInpoints})
				require.NoError(t, err)

				defer s.Close()

				addAll(t, s, in, withInpoints)
				require.NoError(t, s.waitSpill())
				require.NotEmpty(t, s.runs)

				require.NoError(t, os.Truncate(s.runs[0], keep))

				err = s.Drain(context.Background(), 500, func([]*utxo.UnminedTransaction) error { return nil })
				require.Error(t, err)
			})
		}
	}
}

// A run's reader checks the record count in its header, so a run cut at a
// record boundary is an error at the reader, not a clean end.
func TestFileSource_ChecksHeaderCount(t *testing.T) {
	dir := t.TempDir()

	s, err := New(Options{Dirs: []string{dir}, BufferRecords: 1000})
	require.NoError(t, err)

	defer s.Close()

	addAll(t, s, makeInputs(1000, false), false)
	require.NoError(t, s.waitSpill())
	require.Len(t, s.runs, 1)

	require.NoError(t, os.Truncate(s.runs[0], runHeaderSize))

	fs, err := openFileSource(s.runs[0], false)
	require.NoError(t, err)

	defer fs.close()

	ok, err := fs.next()
	require.False(t, ok)
	require.ErrorContains(t, err, "ended after 0 of 1000 records")
}

// A run holding more records than its header says is corrupt too.
func TestFileSource_RejectsRecordsBeyondHeaderCount(t *testing.T) {
	dir := t.TempDir()

	s, err := New(Options{Dirs: []string{dir}, BufferRecords: 1000})
	require.NoError(t, err)

	defer s.Close()

	addAll(t, s, makeInputs(1000, false), false)
	require.NoError(t, s.waitSpill())
	require.Len(t, s.runs, 1)

	f, err := os.OpenFile(s.runs[0], os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteAt(binary.LittleEndian.AppendUint64(nil, 999), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	fs, err := openFileSource(s.runs[0], false)
	require.NoError(t, err)

	defer fs.close()

	for i := 0; i < 999; i++ {
		ok, err := fs.next()
		require.NoError(t, err)
		require.True(t, ok)
	}

	ok, err := fs.next()
	require.False(t, ok)
	require.ErrorContains(t, err, "holds more than its 999 records")
}

// Drain checks it emitted every added transaction, whatever lost one.
func TestSorter_DrainFailsWhenCountDiffers(t *testing.T) {
	s, err := New(Options{BufferRecords: 1 << 20})
	require.NoError(t, err)

	defer s.Close()

	addAll(t, s, makeInputs(10, false), false)
	s.spilledCount++ // one transaction that no source holds

	err = s.Drain(context.Background(), 10, func([]*utxo.UnminedTransaction) error { return nil })
	require.Error(t, err)
}

// A failed spill poisons the sorter: its run is incomplete, so every later Add
// and Drain fails, even once the directory is writable again.
func TestSorter_SpillErrorIsSticky(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}

	dir := t.TempDir()

	s, err := New(Options{Dirs: []string{dir}, BufferRecords: 100})
	require.NoError(t, err)

	defer s.Close()

	for _, d := range s.dirs {
		require.NoError(t, os.Chmod(d, 0o500))
	}

	in := makeInputs(1000, false)

	var addErr error

	next := 0
	for ; next < len(in) && addErr == nil; next++ {
		addErr = s.Add(in[next].createdAt, in[next].node, nil)
	}

	require.Error(t, addErr, "the second spill surfaces the first one's failure")

	for _, d := range s.dirs {
		require.NoError(t, os.Chmod(d, 0o755))
	}

	require.Error(t, s.Add(in[next].createdAt, in[next].node, nil), "an Add after a failed spill fails too")
	require.Error(t, s.Drain(context.Background(), 100, func([]*utxo.UnminedTransaction) error { return nil }))
}

// Inpoints are buffered too, so a buffer also spills once its bytes reach what
// BufferRecords records alone would take; otherwise consolidation-heavy input
// grows it far past the documented size. The output is still complete and in
// order.
func TestSorter_SpillsOnBytesWithLargeInpoints(t *testing.T) {
	dir := t.TempDir()
	in := makeInputs(200, false)

	parents := make([]chainhash.Hash, 50)
	idxs := make([]uint32, 0, 2*len(parents))

	for i := range parents {
		parents[i] = hashOf(10_000 + i)
		idxs = append(idxs, 1, uint32(i)) //nolint:gosec // test data
	}

	for i := range in {
		in[i].inpoints = subtreepkg.NewTxInpointsFromPacked(parents, idxs)
	}

	s, err := New(Options{Dirs: []string{dir}, BufferRecords: 1000, WithInpoints: true})
	require.NoError(t, err)

	defer s.Close()

	addAll(t, s, in, true)
	require.NoError(t, s.waitSpill())
	require.NotEmpty(t, s.runs, "200 records are under BufferRecords, but their inpoints exceed the byte budget")

	out, _ := drainAll(t, s, 50)
	requireOrder(t, in, out, true)
}
