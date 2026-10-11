package model

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	tererrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// dedupInpoint builds an inpoint from two small numbers so tests can create
// many distinct, reproducible ones.
func dedupInpoint(parent, index uint32) subtreepkg.Inpoint {
	var in subtreepkg.Inpoint

	in.Hash[0] = byte(parent)
	in.Hash[1] = byte(parent >> 8)
	in.Hash[2] = byte(parent >> 16)
	in.Hash[3] = byte(parent >> 24)
	in.Index = index

	return in
}

// refEarliestDuplicate is the plain Go map model: scan the txs in block order
// and report the first tx that spends an inpoint already spent by an earlier tx
// or by an earlier input of itself. base is the global index of txs[0].
func refEarliestDuplicate(txs [][]subtreepkg.Inpoint, base uint64) (uint64, bool) {
	seen := make(map[subtreepkg.Inpoint]struct{})

	for i, ins := range txs {
		for _, in := range ins {
			if _, ok := seen[in]; ok {
				return base + uint64(i), true
			}

			seen[in] = struct{}{}
		}
	}

	return 0, false
}

type dedupCase struct {
	hash     func(subtreepkg.Inpoint) uint64 // nil = random seed
	expected uint64                          // expected inputs, drives the partition count
	workers  int
	runs     int
	base     uint64 // global index of txs[0]
	total    uint64 // total tx count handed to the constructor; 0 = base+len(txs)
	shuffle  *rand.Rand
}

// runDedup loads txs into a dedup split over c.runs runs (contiguous tx ranges,
// each filled by its own goroutine, finished in an arbitrary order) and returns
// the detected tx plus how many times the resolver was asked for a tx.
func runDedup(t testing.TB, txs [][]subtreepkg.Inpoint, c dedupCase) (uint64, bool, int64) {
	t.Helper()

	if c.runs < 1 {
		c.runs = 1
	}

	if c.workers < 1 {
		c.workers = 1
	}

	total := c.total
	if total == 0 {
		total = c.base + uint64(len(txs))
	}

	d := newInpointDedup(c.runs, c.expected, total, c.hash)

	// contiguous tx ranges per run
	bounds := make([]int, c.runs+1)
	for r := 0; r <= c.runs; r++ {
		bounds[r] = r * len(txs) / c.runs
	}

	order := make([]int, c.runs)
	for i := range order {
		order[i] = i
	}

	if c.shuffle != nil {
		c.shuffle.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	}

	var wg sync.WaitGroup

	for _, r := range order {
		wg.Add(1)

		go func() {
			defer wg.Done()

			rb := d.newRun(r, 0)
			for i := bounds[r]; i < bounds[r+1]; i++ {
				rb.addTx(c.base+uint64(i), txs[i])
			}

			rb.finish()
		}()
	}

	wg.Wait()

	var calls atomic.Int64

	tx, found, err := d.detect(context.Background(), c.workers, func() func(uint64) ([]subtreepkg.Inpoint, error) {
		return func(tx uint64) ([]subtreepkg.Inpoint, error) {
			calls.Add(1)
			return txs[tx-c.base], nil
		}
	})
	require.NoError(t, err)

	return tx, found, calls.Load()
}

func requireMatchesReference(t testing.TB, txs [][]subtreepkg.Inpoint, c dedupCase) {
	t.Helper()

	wantTx, wantFound := refEarliestDuplicate(txs, c.base)
	gotTx, gotFound, _ := runDedup(t, txs, c)

	require.Equal(t, wantFound, gotFound)

	if wantFound {
		require.Equal(t, wantTx, gotTx)
	}
}

// constHash puts every inpoint in one hash group, so every pair of inputs is a
// fingerprint collision that only the real-inpoint check can settle.
func constHash(subtreepkg.Inpoint) uint64 { return 42 }

// fewBitsHash leaves only 2 bits of hash, so groups mix real duplicates and
// colliding distinct inpoints.
func fewBitsHash(in subtreepkg.Inpoint) uint64 {
	return uint64(in.Hash[0]^byte(in.Index)) & 3
}

// topBitsHash gives every inpoint the same top 20 bits (so they all land in one
// partition however many there are) and distinguishes them below that.
func topBitsHash(in subtreepkg.Inpoint) uint64 {
	return 0xABCDE<<44 | uint64(in.Hash[0])<<8 | uint64(in.Index&0xff)
}

func randomTxs(rng *rand.Rand, nTx, maxIns, universe int) [][]subtreepkg.Inpoint {
	txs := make([][]subtreepkg.Inpoint, nTx)

	for i := range txs {
		n := rng.Intn(maxIns + 1)
		for j := 0; j < n; j++ {
			txs[i] = append(txs[i], dedupInpoint(uint32(rng.Intn(universe)), uint32(rng.Intn(3)))) //nolint:gosec // test
		}
	}

	return txs
}

// uniqueTxs returns nTx txs with nIn distinct inputs each, no duplicates.
func uniqueTxs(nTx, nIn int) [][]subtreepkg.Inpoint {
	txs := make([][]subtreepkg.Inpoint, nTx)
	next := uint32(1)

	for i := range txs {
		for j := 0; j < nIn; j++ {
			txs[i] = append(txs[i], dedupInpoint(next, 0))
			next++
		}
	}

	return txs
}

func TestInpointDedup_EntrySizes(t *testing.T) {
	require.EqualValues(t, 12, unsafe.Sizeof(dedupEntry[uint32]{}), "narrow entry must stay 12 bytes")
	require.EqualValues(t, 16, unsafe.Sizeof(dedupEntry[uint64]{}))
}

func TestInpointDedup_SmallShapes(t *testing.T) {
	in := dedupInpoint

	tests := []struct {
		name string
		txs  [][]subtreepkg.Inpoint
	}{
		{"no txs", nil},
		{"one tx without inputs", [][]subtreepkg.Inpoint{{}}},
		{"coinbase placeholder only", [][]subtreepkg.Inpoint{nil}},
		{"one tx one input", [][]subtreepkg.Inpoint{{in(1, 0)}}},
		{"one tx with two equal inputs", [][]subtreepkg.Inpoint{{in(1, 0), in(1, 0)}}},
		{"one tx two distinct inputs same parent", [][]subtreepkg.Inpoint{{in(1, 0), in(1, 1)}}},
		{"two txs same inpoint", [][]subtreepkg.Inpoint{{in(1, 0)}, {in(1, 0)}}},
		{"two txs different index same parent", [][]subtreepkg.Inpoint{{in(1, 0)}, {in(1, 1)}}},
		{"duplicate at the end", [][]subtreepkg.Inpoint{{in(1, 0)}, {in(2, 0)}, {in(3, 0)}, {in(1, 0)}}},
		{"three-way duplicate reports second spender", [][]subtreepkg.Inpoint{{in(1, 0)}, {in(2, 0)}, {in(1, 0)}, {in(1, 0)}}},
		{"two groups, earliest is the lower of the later spenders", [][]subtreepkg.Inpoint{
			{in(1, 0)}, {in(2, 0)}, {in(2, 0)}, {in(1, 0)}, {in(3, 0)},
		}},
		{"later group has the earlier second spender", [][]subtreepkg.Inpoint{
			{in(1, 0)}, {in(2, 0)}, {in(1, 0)}, {in(2, 0)},
		}},
		{"dup between first and last of many", append(append([][]subtreepkg.Inpoint{{in(7, 7)}}, uniqueTxs(50, 2)...), []subtreepkg.Inpoint{in(7, 7)})},
	}

	hashes := map[string]func(subtreepkg.Inpoint) uint64{
		"seeded": nil, "const": constHash, "fewbits": fewBitsHash, "topbits": topBitsHash,
	}

	for _, tc := range tests {
		for hn, h := range hashes {
			for _, runs := range []int{1, 2, 5} {
				t.Run(tc.name+"/"+hn, func(t *testing.T) {
					requireMatchesReference(t, tc.txs, dedupCase{hash: h, runs: runs, workers: 3})
				})
			}
		}
	}
}

func TestInpointDedup_RandomVsReference(t *testing.T) {
	rng := rand.New(rand.NewSource(1))

	hashes := map[string]func(subtreepkg.Inpoint) uint64{
		"seeded": nil, "const": constHash, "fewbits": fewBitsHash, "topbits": topBitsHash,
	}

	for iter := 0; iter < 300; iter++ {
		nTx := rng.Intn(60)
		universe := 1 + rng.Intn(400) // small universe: mostly duplicates; large: mostly none
		txs := randomTxs(rng, nTx, 4, universe*10)

		for hn, h := range hashes {
			c := dedupCase{
				hash:     h,
				runs:     1 + rng.Intn(6),
				workers:  1 + rng.Intn(5),
				expected: uint64(rng.Intn(1 << 18)), // exercises many partition counts
				base:     uint64(rng.Intn(3)) << 20,
				shuffle:  rng,
			}

			wantTx, wantFound := refEarliestDuplicate(txs, c.base)
			gotTx, gotFound, _ := runDedup(t, txs, c)
			require.Equalf(t, wantFound, gotFound, "iter %d hash %s", iter, hn)

			if wantFound {
				require.Equalf(t, wantTx, gotTx, "iter %d hash %s", iter, hn)
			}
		}
	}
}

// TestInpointDedup_PartitionBoundaries drives expected-input counts around the
// thresholds where the partition count steps, with few real entries.
func TestInpointDedup_PartitionBoundaries(t *testing.T) {
	txs := [][]subtreepkg.Inpoint{{dedupInpoint(1, 0)}, {dedupInpoint(2, 0)}, {dedupInpoint(1, 0)}}

	for _, expected := range []uint64{0, 1, 1 << 16, 1<<16 + 1, 1 << 17, 1<<17 + 1, 1 << 20, 1<<20 + 1, 1 << 28, 1<<28 + 1, 1 << 40, math.MaxUint64} {
		requireMatchesReference(t, txs, dedupCase{expected: expected, runs: 2, workers: 4})
		requireMatchesReference(t, uniqueTxs(7, 3), dedupCase{expected: expected, runs: 3, workers: 4})
	}
}

// TestInpointDedup_GroundHashes grinds inpoints whose real 64-bit seeded hashes
// agree on the top bits, so they pile into one partition, and checks the answer.
func TestInpointDedup_GroundHashes(t *testing.T) {
	const topBits = 12

	d := newInpointDedup(1, 1<<28, 1, nil) // 12 partition bits
	hash := d.(*dedupImpl[uint32]).hash

	var ground []subtreepkg.Inpoint

	for i := uint32(0); len(ground) < 300; i++ {
		in := dedupInpoint(i, 0)
		if hash(in)>>(64-topBits) == 5 {
			ground = append(ground, in)
		}
	}

	txs := make([][]subtreepkg.Inpoint, 0, len(ground)+1)
	for _, in := range ground {
		txs = append(txs, []subtreepkg.Inpoint{in})
	}

	// all distinct: no duplicate however crowded the partition
	gotTx, found := detectWith(t, d, txs)
	require.False(t, found, "tx %d", gotTx)

	txs = append(txs, []subtreepkg.Inpoint{ground[17]})
	gotTx, found = detectWith(t, d, txs)
	require.True(t, found)
	require.EqualValues(t, len(ground), gotTx)
}

// detectWith fills d (one run) from txs and detects.
func detectWith(t *testing.T, d inpointDedupper, txs [][]subtreepkg.Inpoint) (uint64, bool) {
	t.Helper()

	rb := d.newRun(0, len(txs))
	for i, ins := range txs {
		rb.addTx(uint64(i), ins)
	}

	rb.finish()

	tx, found, err := d.detect(context.Background(), 2, func() func(uint64) ([]subtreepkg.Inpoint, error) {
		return func(tx uint64) ([]subtreepkg.Inpoint, error) { return txs[tx], nil }
	})
	require.NoError(t, err)

	return tx, found
}

// TestInpointDedup_Deterministic runs the same txs through different worker
// counts, run layouts, run completion orders and hash seeds: the answer is
// always the one a sequential scan gives.
func TestInpointDedup_Deterministic(t *testing.T) {
	rng := rand.New(rand.NewSource(7))

	for iter := 0; iter < 40; iter++ {
		txs := randomTxs(rng, 200, 3, 600)
		want, wantFound := refEarliestDuplicate(txs, 0)

		for _, workers := range []int{1, 2, 3, 8, 64} {
			for _, runs := range []int{1, 2, 7, 64, 200} {
				for seed := 0; seed < 3; seed++ { // newInpointDedup seeds randomly each time
					got, found, _ := runDedup(t, txs, dedupCase{
						workers: workers, runs: runs, expected: uint64(rng.Intn(1 << 20)), shuffle: rng,
					})
					require.Equal(t, wantFound, found)

					if wantFound {
						require.Equal(t, want, got, "iter %d workers %d runs %d", iter, workers, runs)
					}
				}
			}
		}
	}
}

// TestInpointDedup_WideIndex covers a block with more than 2^32 txs: the 16 byte
// entries are used and the answer is still exact.
func TestInpointDedup_WideIndex(t *testing.T) {
	rng := rand.New(rand.NewSource(3))

	for iter := 0; iter < 50; iter++ {
		txs := randomTxs(rng, 40, 3, 100)
		base := uint64(math.MaxUint32) - 10 + uint64(rng.Intn(30)) // straddles 2^32

		c := dedupCase{base: base, total: math.MaxUint32 + 100, runs: 1 + rng.Intn(4), workers: 3, shuffle: rng}
		d := newInpointDedup(c.runs, 1000, c.total, nil)
		_, isWide := d.(*dedupImpl[uint64])
		require.True(t, isWide)

		requireMatchesReference(t, txs, c)
	}

	_, isNarrow := newInpointDedup(1, 10, math.MaxUint32, nil).(*dedupImpl[uint32])
	require.True(t, isNarrow, "up to 2^32-1 txs fit the 12 byte entry")
}

// TestInpointDedup_ResolverOnlyOnHashEquality pins that the real inpoints are
// only fetched for txs whose input hashes are equal.
func TestInpointDedup_ResolverOnlyOnHashEquality(t *testing.T) {
	_, found, calls := runDedup(t, uniqueTxs(500, 3), dedupCase{runs: 4, workers: 4})
	require.False(t, found)
	require.Zero(t, calls, "no hash match, nothing to resolve")

	// a duplicate involves its two txs and nobody else
	txs := uniqueTxs(500, 3)
	txs[400][1] = txs[10][2]
	tx, found, calls := runDedup(t, txs, dedupCase{runs: 4, workers: 1})
	require.True(t, found)
	require.EqualValues(t, 400, tx)
	require.EqualValues(t, 2, calls)
}

// TestInpointDedup_ManyDuplicatesBounded pins that a block where every tx spends
// the same outpoint costs a number of resolver calls that does not grow with the
// group, rather than one per pair.
func TestInpointDedup_ManyDuplicatesBounded(t *testing.T) {
	txs := make([][]subtreepkg.Inpoint, 5000)
	for i := range txs {
		txs[i] = []subtreepkg.Inpoint{dedupInpoint(9, 9)}
	}

	tx, found, calls := runDedup(t, txs, dedupCase{runs: 5, workers: 1})
	require.True(t, found)
	require.EqualValues(t, 1, tx)
	require.LessOrEqual(t, calls, int64(2))

	// the same under total hash collision with distinct inpoints that are not
	// duplicates: one resolve per tx, linear
	distinct := uniqueTxs(2000, 1)
	tx, found, calls = runDedup(t, distinct, dedupCase{hash: constHash, runs: 3, workers: 1})
	require.False(t, found, "tx %d", tx)
	require.EqualValues(t, 2000, calls)
}

func TestInpointDedup_ResolverError(t *testing.T) {
	txs := [][]subtreepkg.Inpoint{{dedupInpoint(1, 0)}, {dedupInpoint(1, 0)}}
	d := newInpointDedup(1, 10, 2, nil)

	rb := d.newRun(0, 2)
	for i, ins := range txs {
		rb.addTx(uint64(i), ins)
	}

	rb.finish()

	boom := tererrors.NewProcessingError("boom")
	_, _, err := d.detect(context.Background(), 2, func() func(uint64) ([]subtreepkg.Inpoint, error) {
		return func(uint64) ([]subtreepkg.Inpoint, error) { return nil, boom }
	})
	require.ErrorIs(t, err, boom)
}

func TestInpointDedup_ContextCancelled(t *testing.T) {
	d := newInpointDedup(1, 10, 2, nil)
	rb := d.newRun(0, 0)
	rb.addTx(0, []subtreepkg.Inpoint{dedupInpoint(1, 0)})
	rb.finish()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := d.detect(ctx, 2, func() func(uint64) ([]subtreepkg.Inpoint, error) { return nil })
	require.ErrorIs(t, err, context.Canceled)
}

// TestInpointDedup_RunWithoutFinish pins that a run that never finished (empty
// subtree, or one never started) is skipped, not dereferenced.
func TestInpointDedup_RunWithoutFinish(t *testing.T) {
	d := newInpointDedup(3, 10, 4, nil)

	rb := d.newRun(1, 0)
	rb.addTx(0, []subtreepkg.Inpoint{dedupInpoint(1, 0)})
	rb.addTx(1, []subtreepkg.Inpoint{dedupInpoint(1, 0)})
	rb.finish()

	tx, found, err := d.detect(context.Background(), 2, func() func(uint64) ([]subtreepkg.Inpoint, error) {
		return func(uint64) ([]subtreepkg.Inpoint, error) { return []subtreepkg.Inpoint{dedupInpoint(1, 0)}, nil }
	})
	require.NoError(t, err)
	require.True(t, found)
	require.EqualValues(t, 1, tx)
}

// TestInpointDedup_ConcurrentBuild is meant for go test -race: many goroutines
// fill their own runs while another dedup is detected concurrently.
func TestInpointDedup_ConcurrentBuild(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	txs := randomTxs(rng, 5000, 3, 20000)

	var wg sync.WaitGroup

	for i := 0; i < 4; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			requireMatchesReference(t, txs, dedupCase{runs: 32, workers: 8, expected: 1 << 18, shuffle: rand.New(rand.NewSource(int64(i)))}) //nolint:gosec // test
		}()
	}

	wg.Wait()
}

func FuzzInpointDedup(f *testing.F) {
	f.Add([]byte{}, uint8(1), uint8(1), uint8(0))
	f.Add([]byte{1, 0, 1, 0}, uint8(2), uint8(2), uint8(1))
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4}, uint8(3), uint8(5), uint8(2))
	f.Add([]byte{9, 9, 9, 9, 9, 9, 9, 9, 9, 9}, uint8(1), uint8(1), uint8(3))

	hashes := []func(subtreepkg.Inpoint) uint64{nil, constHash, fewBitsHash, topBitsHash}

	f.Fuzz(func(t *testing.T, data []byte, runs, workers, hashSel uint8) {
		// bytes are consumed as (parent, index, endOfTx) triples
		var txs [][]subtreepkg.Inpoint

		cur := []subtreepkg.Inpoint{}

		for i := 0; i+2 < len(data); i += 3 {
			cur = append(cur, dedupInpoint(uint32(data[i]%32), uint32(data[i+1]%4)))

			if data[i+2]%2 == 0 {
				txs = append(txs, cur)
				cur = []subtreepkg.Inpoint{}
			}
		}

		txs = append(txs, cur)

		c := dedupCase{
			hash:    hashes[int(hashSel)%len(hashes)],
			runs:    1 + int(runs)%8,
			workers: 1 + int(workers)%8,
			base:    uint64(hashSel) << 30,
			shuffle: rand.New(rand.NewSource(int64(len(data)))), //nolint:gosec // fuzz
		}

		requireMatchesReference(t, txs, c)
	})
}

func BenchmarkInpointDedup(b *testing.B) {
	for _, nTx := range []int{1 << 14, 1 << 17, 1 << 20} {
		txs := uniqueTxs(nTx, 2)
		runs := 16

		b.Run(sizeName(nTx), func(b *testing.B) {
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				d := newInpointDedup(runs, uint64(nTx)*2, uint64(nTx), nil)

				var wg sync.WaitGroup

				for r := 0; r < runs; r++ {
					wg.Add(1)

					go func() {
						defer wg.Done()

						rb := d.newRun(r, 0)
						for t := r * nTx / runs; t < (r+1)*nTx/runs; t++ {
							rb.addTx(uint64(t), txs[t])
						}

						rb.finish()
					}()
				}

				wg.Wait()

				_, found, err := d.detect(context.Background(), 8, func() func(uint64) ([]subtreepkg.Inpoint, error) { return nil })
				if err != nil || found {
					b.Fatal("unexpected result")
				}
			}
		})
	}
}

func sizeName(n int) string {
	switch {
	case n >= 1<<20:
		return "txs=1M"
	case n >= 1<<17:
		return "txs=128K"
	default:
		return "txs=16K"
	}
}

// offsFootprint is the number of partition offsets held across all runs.
func offsFootprint(d inpointDedupper) int {
	n := 0
	for _, r := range d.(*dedupImpl[uint32]).runs {
		n += len(r.offs)
	}

	return n
}

// TestInpointDedup_ManyTinyRuns pins that the per-run bookkeeping scales with
// the inputs, not with runs x partitions: a block cut into very many small
// subtrees must not pay for the partition count of a big block per subtree.
func TestInpointDedup_ManyTinyRuns(t *testing.T) {
	const runs, perRun = 50_000, 3 // 150K inputs

	for _, k := range []uint64{1, 2, 4, 64} { // estimate = k x the real inputs
		expected := k * runs * perRun
		d := newInpointDedup(runs, expected, runs, nil)

		for r := 0; r < runs; r++ {
			rb := d.newRun(r, perRun)
			rb.addTx(uint64(r), []subtreepkg.Inpoint{dedupInpoint(uint32(r), 0), dedupInpoint(uint32(r), 1), dedupInpoint(uint32(r), 2)})
			rb.finish()
		}

		require.LessOrEqualf(t, uint64(offsFootprint(d)), expected, "estimate x%d: offsets must stay within the estimated entries", k)

		_, found, err := d.detect(context.Background(), 4, func() func(uint64) ([]subtreepkg.Inpoint, error) { return nil })
		require.NoError(t, err)
		require.False(t, found)
	}

	// runs without inputs cost nothing
	d := newInpointDedup(1000, 1<<30, 1000, nil)
	for r := 0; r < 1000; r++ {
		rb := d.newRun(r, 0)
		rb.addTx(uint64(r), nil)
		rb.finish()
	}

	require.Zero(t, offsFootprint(d))
}

// TestInpointDedup_PartitionsFollowActualInputs pins that an overestimated input
// count does not leave the work in a swarm of empty partitions, and that an
// exact answer is still given when partitions are coarsened.
func TestInpointDedup_PartitionsFollowActualInputs(t *testing.T) {
	txs := uniqueTxs(40, 2)
	txs[33][1] = txs[3][0]

	for _, expected := range []uint64{0, 80, 1 << 24, 1 << 40} {
		requireMatchesReference(t, txs, dedupCase{expected: expected, runs: 4, workers: 8})
	}
}

// TestInpointDedup_ResolverErrorPrecedence pins that when a group's real
// inpoints cannot be read, the result for a given block and store state does
// not depend on which worker finishes first: a confirmed duplicate below every
// failing group's lowest possible tx wins, otherwise the error of the failing
// group with the lowest possible tx does.
func TestInpointDedup_ResolverErrorPrecedence(t *testing.T) {
	in := dedupInpoint

	type result struct {
		tx    uint64
		found bool
		err   string
	}

	tests := []struct {
		name string
		txs  [][]subtreepkg.Inpoint
		fail map[uint64]string // tx -> error text when asked for its inpoints
		want result
	}{
		{
			name: "confirmed duplicate below the failing group wins",
			// dup of in(1) between 5 and 20; group in(2) = txs 8,30 fails
			txs:  pvTxs(40, map[int][]subtreepkg.Inpoint{5: {in(1, 0)}, 20: {in(1, 0)}, 8: {in(2, 0)}, 30: {in(2, 0)}}),
			fail: map[uint64]string{8: "e8", 30: "e30"},
			want: result{tx: 20, found: true},
		},
		{
			name: "failing group below the duplicate wins",
			txs:  pvTxs(60, map[int][]subtreepkg.Inpoint{5: {in(1, 0)}, 40: {in(1, 0)}, 8: {in(2, 0)}, 30: {in(2, 0)}}),
			fail: map[uint64]string{8: "e8", 30: "e30"},
			want: result{err: "e8"},
		},
		{
			name: "duplicate equal to the failing group's lowest tx wins",
			txs:  pvTxs(200, map[int][]subtreepkg.Inpoint{5: {in(1, 0)}, 30: {in(1, 0)}, 8: {in(2, 0)}, 30 + 100: {in(2, 0)}}),
			fail: map[uint64]string{8: "e8"},
			want: result{tx: 30, found: true},
		},
		{
			name: "of several failing groups the lowest one is reported",
			txs: pvTxs(90, map[int][]subtreepkg.Inpoint{
				4: {in(1, 0)}, 70: {in(1, 0)},
				10: {in(2, 0)}, 50: {in(2, 0)},
				12: {in(3, 0)}, 25: {in(3, 0)},
				14: {in(4, 0)}, 80: {in(4, 0)},
			}),
			fail: map[uint64]string{4: "g1", 10: "g2", 12: "g3", 14: "g4"},
			want: result{err: "g3"},
		},
		{
			name: "no duplicate, failing group",
			txs:  pvTxs(40, map[int][]subtreepkg.Inpoint{8: {in(2, 0)}, 30: {in(2, 0)}}),
			fail: map[uint64]string{30: "e30"},
			want: result{err: "e30"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(9))

			for rep := 0; rep < 60; rep++ {
				workers := []int{1, 2, 3, 8, 32}[rep%5]
				runs := 1 + rng.Intn(9)

				d := newInpointDedup(runs, uint64(rng.Intn(1<<22)), uint64(len(tc.txs)), nil)

				bounds := make([]int, runs+1)
				for r := 0; r <= runs; r++ {
					bounds[r] = r * len(tc.txs) / runs
				}

				order := rng.Perm(runs)

				var wg sync.WaitGroup

				for _, r := range order {
					wg.Add(1)

					go func() {
						defer wg.Done()

						rb := d.newRun(r, 0)
						for i := bounds[r]; i < bounds[r+1]; i++ {
							rb.addTx(uint64(i), tc.txs[i])
						}

						rb.finish()
					}()
				}

				wg.Wait()

				tx, found, err := d.detect(context.Background(), workers, func() func(uint64) ([]subtreepkg.Inpoint, error) {
					return func(tx uint64) ([]subtreepkg.Inpoint, error) {
						if msg, ok := tc.fail[tx]; ok {
							return nil, tererrors.NewProcessingError(msg)
						}

						return tc.txs[tx], nil
					}
				})

				got := result{tx: tx, found: found}
				if err != nil {
					got.err = err.Error()
				}

				if tc.want.err != "" {
					require.Errorf(t, err, "rep %d workers %d", rep, workers)
					require.Containsf(t, err.Error(), tc.want.err, "rep %d workers %d", rep, workers)
					require.False(t, found)

					continue
				}

				require.NoErrorf(t, err, "rep %d workers %d", rep, workers)
				require.Equalf(t, tc.want, got, "rep %d workers %d", rep, workers)
			}
		})
	}
}

// pvTxs builds n txs, tx i spending a unique inpoint, with the given overrides
// (replacing the tx's inputs).
func pvTxs(n int, override map[int][]subtreepkg.Inpoint) [][]subtreepkg.Inpoint {
	txs := uniqueTxs(n, 1)
	for i := range txs {
		for j := range txs[i] {
			txs[i][j] = dedupInpoint(1_000_000+uint32(i), 0) //nolint:gosec // test
		}
	}

	for i, ins := range override {
		txs[i] = ins
	}

	return txs
}

func BenchmarkInpointDedup_TinyRuns(b *testing.B) {
	const runs, perRun = 100_000, 2

	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		d := newInpointDedup(runs, runs*perRun, runs, nil)

		for r := 0; r < runs; r++ {
			rb := d.newRun(r, perRun)
			rb.addTx(uint64(r), []subtreepkg.Inpoint{dedupInpoint(uint32(r), 0), dedupInpoint(uint32(r), 1)})
			rb.finish()
		}

		if _, found, err := d.detect(context.Background(), 8, func() func(uint64) ([]subtreepkg.Inpoint, error) { return nil }); err != nil || found {
			b.Fatal("unexpected result")
		}
	}
}
