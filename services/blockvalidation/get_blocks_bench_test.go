package blockvalidation

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/adaptivefetch"
	"github.com/bsv-blockchain/teranode/stores/blob/file"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/blob/storetypes"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
)

// BenchmarkCatchupSubtreeDataPrefetch measures the catch-up subtree-data prewarm so the
// default of blockvalidation_catchup_prefetch_budget_bytes is chosen from a number rather
// than asserted (bsv-blockchain/teranode#1139).
//
// It reports, per cell:
//   - incrHeapMB    - peak sampled HeapAlloc minus the post-GC baseline taken just before
//     the measured region.
//   - baselineHeapMB - that baseline, so the subtraction is auditable.
//   - MB/s          - declared subtree_data payload bytes per second, the throughput cost
//     of the bound.
//
// incrHeapMB is a SAMPLED INCREMENTAL HEAP ESTIMATE, not the prewarm's isolated
// contribution and not an established heap multiplier. It includes allocations awaiting
// collection, it misses anything between 10 ms samples, and it assumes fixture retention is
// constant across the measured region. These are indicative numbers for choosing a default,
// not a memory proof.
//
// It is a benchmark, not a gating test: heap assertions are GC-noisy and would flake, so
// nothing here requires a heap figure. Run it manually:
//
//	set -o pipefail
//	export SETTINGS_CONTEXT=test
//	go test -tags testtxmetacache -run '^$' -bench BenchmarkCatchupSubtreeDataPrefetch \
//	  -benchtime 1x -benchmem -timeout 30m ./services/blockvalidation/ 2>&1 | tee /tmp/bench.log
//
// The fixtures are large by design and are materialised eagerly, one shape at a time:
// 200 MiB for even, 205 MiB for skewed, 550 MiB for oversized. The worst cell is
// oversized/budget=off, where nothing bounds the prewarm and all 550 MiB is parsed at once;
// that is the cell whose incrHeapMB figure the budget exists to reduce. See the constant
// block below for the sizing rationale and for how to scale up.
//
// If the set is still too large for the machine, shrink benchPrefetchBlocks — do NOT make
// the responder bodies lazy. Bodies generated inside a responder allocate inside the measured
// region and invalidate the constant-fixture assumption the subtraction rests on.
func BenchmarkCatchupSubtreeDataPrefetch(b *testing.B) {
	for _, shape := range benchPrefetchShapes() {
		// Scoped so this shape's fixtures become unreachable before the next shape is
		// built: holding all three sets at once would put gigabytes of unrelated bodies
		// inside the next shape's baseline.
		func() {
			fixtures, payloadBytes := buildBenchPrefetchBlocks(b, shape)

			for _, budget := range benchPrefetchBudgets() {
				b.Run(shape.name+"/budget="+budget.name, func(b *testing.B) {
					benchmarkCatchupPrefetchCell(b, fixtures, budget.bytes, payloadBytes)
				})
			}
		}()

		runtime.GC()
		runtime.GC()
	}
}

// Fixture dimensions.
//
// These are sized so the DEFAULT `go test -bench` invocation fits in roughly 4 GiB of peak
// process memory, while keeping every property section 4.4 asks the benchmark to exercise:
// three shapes, three budgets, and an `oversized` shape whose blocks genuinely exceed the
// 256 MiB default so the serial oversized-block policy is actually measured.
//
// The binding structure that the sizes below are chosen to produce:
//
//	shape      | block size | 256 MiB budget                  | 128 MiB budget
//	-----------|------------|---------------------------------|----------------
//	even       | 100.03 MiB | not binding (both blocks fit)   | binding
//	skewed     | 102.38 MiB | not binding (both blocks fit)   | binding
//	oversized  | 275.09 MiB | BINDING: admitted alone, and    | same
//	           |            | subtree concurrency drops to 1  |
//
// FOR A FULL-SCALE RUN on a machine with the headroom (~16 GiB+), raise
// benchPrefetchBlocks to 8 and the per-shape transaction counts in benchPrefetchShapes to
// 16x64 / 2x512+14x4 / 16x128. That restores the ~200 MB and ~400 MB per-block payloads the
// plan describes. Raise benchPrefetchBlocks FIRST — it scales peak memory linearly and is
// the only knob that does not change which cells bind.
//
// Do NOT recover memory by making the responder bodies lazy; see the note on the
// benchmark function above for why that invalidates the measurement rather than shrinking it.
const (
	// benchPrefetchScriptBytes is the size of each transaction's input AND output script.
	// The incident's heap profile was 67% input scripts / 33% output scripts, so both
	// sides are loaded. Held at 100 KiB even when scaling down: "a large-script region" is
	// the condition being reproduced, so this is the last constant that should shrink.
	benchPrefetchScriptBytes = 100 * 1024

	// benchPrefetchBlocks is the number of blocks pushed through the pipeline per cell.
	// Two is the floor that still exercises block-level admission: the `oversized` shape
	// declares more than the whole 256 MiB budget per block, so N blocks cost at least
	// N x 256 MiB of eagerly-materialised fixtures whatever else is shrunk.
	benchPrefetchBlocks = 2

	// benchPrefetchWorkers and benchPrefetchSubtreeConcurrency are the shipped defaults,
	// so the measurement describes the shipped configuration. Neither is a memory lever
	// here: with the budget disabled every block is in flight at once regardless. Note
	// when reading the results that at these scaled-down sizes every shape has FEWER
	// subtrees per block than benchPrefetchSubtreeConcurrency, so the effective per-block
	// concurrency is the subtree count (8 / 4 / 11), not 32. What the oversized cells
	// measure is therefore 11 -> 1, which is the policy; the shipped 16 x 32 product is
	// only reached at full scale.
	benchPrefetchWorkers            = 16
	benchPrefetchSubtreeConcurrency = 32

	benchPrefetchBaseURL = "http://bench-peer:8080"
	benchPrefetchPeerID  = "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ"
)

// benchPrefetchShape describes one block shape. txsPerSubtree has one entry per subtree,
// holding that subtree's transaction count. Powers of two are used throughout: the receive
// path rebuilds the subtree with NewIncompleteTreeByLeafCount(nodeCount), which rounds the
// height up, so a power of two keeps the served tree complete and its root unambiguous.
type benchPrefetchShape struct {
	name          string
	txsPerSubtree []int
}

// benchPrefetchShapes returns the three configurations: a fitting block, a fitting block
// with a wildly skewed subtree size distribution, and a block above the 256 MiB default.
// The oversized shape is what exercises the serial oversized-block policy — without it the
// rule the budget exists to enforce is never measured.
//
// One transaction serializes to 204,868 bytes (two 100 KiB scripts plus headers), which is
// where the per-block figures below come from.
func benchPrefetchShapes() []benchPrefetchShape {
	// 8 x 64 = 512 txs = 100.03 MiB per block. Two blocks sum to 200.06 MiB: under the
	// 256 MiB budget (so it never binds) and over the 128 MiB one (so it does).
	even := make([]int, 8)
	for i := range even {
		even[i] = 64
	}

	// Same total payload, one fat subtree and three tiny ones: the largest subtree is
	// 100.03 MiB against a 25.59 MiB average, a ~3.9x skew. This is the distribution an
	// average-based divisor would misjudge, so the measured cost of NOT using one is
	// visible here rather than only in the unit tests.
	skewed := []int{512, 4, 4, 4}

	// 11 x 128 = 1408 txs = 275.09 MiB per block — deliberately just over the 256 MiB
	// default rather than far over, because every extra byte here is an eagerly
	// materialised fixture. Eleven subtrees rather than a handful so the drop from
	// min(32, 11)-way concurrency to 1 is a real change in the measured shape.
	oversized := make([]int, 11)
	for i := range oversized {
		oversized[i] = 128
	}

	return []benchPrefetchShape{
		{name: "even", txsPerSubtree: even},
		{name: "skewed", txsPerSubtree: skewed},
		{name: "oversized", txsPerSubtree: oversized},
	}
}

type benchPrefetchBudget struct {
	name  string
	bytes int64
}

// benchPrefetchBudgets is the budget sweep. Run A (0) isolates F1 only: the streaming store
// write is present in every run, so A is NOT "today's behaviour" and cannot be used to
// quantify the streaming change.
func benchPrefetchBudgets() []benchPrefetchBudget {
	return []benchPrefetchBudget{
		{name: "off", bytes: 0},
		{name: "256Mi", bytes: 256 << 20},
		{name: "128Mi", bytes: 128 << 20},
	}
}

// noopBlobDeletionScheduler satisfies options.BlobDeletionScheduler without doing anything.
//
// Both production store calls on this path pass options.WithDeleteAt(dah), and the file
// store refuses a non-zero DAH when no scheduler is configured
// ("cannot schedule blob deletion: blob deletion scheduler not configured"). In a real node
// that scheduler is the blockchain client; here the retention behaviour is not what is being
// measured, so the benchmark supplies a stub rather than the benchmark's needs being allowed
// to reach back into production code. Deliberately stateless: a counter would put shared
// atomic contention on a path that up to SubtreeFetchConcurrency x FetchNumWorkers goroutines
// traverse inside the timed region.
type noopBlobDeletionScheduler struct{}

func (noopBlobDeletionScheduler) ScheduleBlobDeletion(_ context.Context, _ []byte, _ string,
	_ storetypes.BlobStoreType, _ uint32) (int64, bool, error) {
	return 0, true, nil
}

func (noopBlobDeletionScheduler) CancelBlobDeletion(_ context.Context, _ []byte, _ string,
	_ storetypes.BlobStoreType) (bool, error) {
	return true, nil
}

type benchPrefetchSubtree struct {
	hash      *chainhash.Hash
	nodeBytes []byte
	dataBytes []byte
}

type benchPrefetchBlock struct {
	block    *model.Block
	subtrees []benchPrefetchSubtree
}

// buildBenchPrefetchTx builds one syntactically valid, NON-extended transaction with a large
// input script and a large output script. It is never signed: nothing on the subtree_data
// receive path verifies signatures, only that each transaction hashes to the subtree node it
// sits under. scriptBytes is a parameter rather than the constant because the latency sweep
// needs a many-subtree shape with small payloads.
func buildBenchPrefetchTx(b *testing.B, shapeName string, blockIdx, subtreeIdx, txIdx, scriptBytes int) *bt.Tx {
	b.Helper()

	tx := bt.NewTx()

	// A real (non-zero) previous txid, so the transaction is never mistaken for a coinbase.
	previous := chainhash.DoubleHashH([]byte(fmt.Sprintf("%s/%d/%d/%d", shapeName, blockIdx, subtreeIdx, txIdx)))

	unlocking := bscript.Script(bytes.Repeat([]byte{0x51}, scriptBytes))

	input := &bt.Input{
		PreviousTxOutIndex: 0,
		UnlockingScript:    &unlocking,
		SequenceNumber:     0xfffffffe,
	}

	if err := input.PreviousTxIDAdd(&previous); err != nil {
		b.Fatalf("failed to set previous txid: %v", err)
	}

	tx.Inputs = append(tx.Inputs, input)

	locking := bscript.Script(bytes.Repeat([]byte{0x52}, scriptBytes))
	tx.AddOutput(&bt.Output{Satoshis: 1000, LockingScript: &locking})

	return tx
}

// buildBenchPrefetchSubtree materialises one subtree's /subtree node list and its
// /subtree_data body. The body is the concatenation of each transaction's STANDARD
// serialization, which is exactly what Data.Serialize would emit for a subtree with no
// coinbase placeholder.
func buildBenchPrefetchSubtree(b *testing.B, shapeName string, blockIdx, subtreeIdx, txCount, scriptBytes int) benchPrefetchSubtree {
	b.Helper()

	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(txCount)
	if err != nil {
		b.Fatalf("failed to create subtree with %d leaves: %v", txCount, err)
	}

	nodeBytes := make([]byte, 0, txCount*chainhash.HashSize)
	dataBytes := make([]byte, 0, txCount*(2*scriptBytes+256))

	for i := 0; i < txCount; i++ {
		tx := buildBenchPrefetchTx(b, shapeName, blockIdx, subtreeIdx, i, scriptBytes)
		hash := tx.TxIDChainHash()

		if err = subtree.AddNode(*hash, uint64(i+1), uint64(tx.Size())); err != nil {
			b.Fatalf("failed to add node %d: %v", i, err)
		}

		nodeBytes = append(nodeBytes, hash[:]...)
		dataBytes = append(dataBytes, tx.Bytes()...)
	}

	return benchPrefetchSubtree{hash: subtree.RootHash(), nodeBytes: nodeBytes, dataBytes: dataBytes}
}

// buildBenchPrefetchBlocks builds every fixture body eagerly, up front, as fully
// materialised []byte. It returns the blocks and the total declared payload the throughput
// figure is computed from.
func buildBenchPrefetchBlocks(b *testing.B, shape benchPrefetchShape) ([]benchPrefetchBlock, int64) {
	b.Helper()

	fixtures := make([]benchPrefetchBlock, 0, benchPrefetchBlocks)

	var totalPayload int64

	for blockIdx := 0; blockIdx < benchPrefetchBlocks; blockIdx++ {
		subtrees := make([]benchPrefetchSubtree, 0, len(shape.txsPerSubtree))
		hashes := make([]*chainhash.Hash, 0, len(shape.txsPerSubtree))

		var (
			declared uint64
			txCount  uint64
		)

		for subtreeIdx, subtreeTxs := range shape.txsPerSubtree {
			fixture := buildBenchPrefetchSubtree(b, shape.name, blockIdx, subtreeIdx, subtreeTxs, benchPrefetchScriptBytes)

			declared += uint64(len(fixture.dataBytes))
			txCount += uint64(subtreeTxs)
			totalPayload += int64(len(fixture.dataBytes))

			subtrees = append(subtrees, fixture)
			hashes = append(hashes, fixture.hash)
		}

		fixtures = append(fixtures, benchPrefetchBlock{
			block: &model.Block{
				Height:           uint32(1000 + blockIdx),
				SizeInBytes:      declared,
				TransactionCount: txCount,
				Subtrees:         hashes,
			},
			subtrees: subtrees,
		})
	}

	return fixtures, totalPayload
}

// benchmarkCatchupPrefetchCell runs one (shape, budget) cell.
func benchmarkCatchupPrefetchCell(b *testing.B, fixtures []benchPrefetchBlock, budgetBytes, payloadBytes int64) {
	logger := ulogger.TestLogger{}

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	// Responders serve the pre-built bodies. Nothing is generated here.
	for _, fixture := range fixtures {
		for _, subtree := range fixture.subtrees {
			httpmock.RegisterResponder("GET",
				fmt.Sprintf("%s/subtree/%s", benchPrefetchBaseURL, subtree.hash.String()),
				httpmock.NewBytesResponder(200, subtree.nodeBytes))
			httpmock.RegisterResponder("GET",
				fmt.Sprintf("%s/subtree_data/%s", benchPrefetchBaseURL, subtree.hash.String()),
				httpmock.NewBytesResponder(200, subtree.dataBytes))
		}
	}

	var (
		peakIncrement uint64
		baselineHeap  uint64
		elapsed       time.Duration
	)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		// Fresh, EMPTY output store per iteration. fetchAndStoreSubtreeData returns early
		// when the blob already exists, so a store reused across iterations would turn
		// every iteration after the first into cache hits that measure nothing. It is a
		// FILE store, not the memory one: blobmemory.SetFromReader does io.ReadAll and
		// retains the payload in its map, so the stored output would accumulate
		// independently of the prewarm and the streaming write's benefit would be
		// invisible.
		storeURL, err := url.Parse("file://" + b.TempDir())
		if err != nil {
			b.Fatalf("failed to parse store url: %v", err)
		}

		// The scheduler is required, not optional: fetchAndStoreSubtree and
		// fetchAndStoreSubtreeData both store with options.WithDeleteAt(dah), and the file
		// store rejects a non-zero DAH outright when none is configured.
		store, err := file.New(logger, storeURL,
			options.WithBlobDeletionScheduler(noopBlobDeletionScheduler{}))
		if err != nil {
			b.Fatalf("failed to create file store: %v", err)
		}

		tSettings := test.CreateBaseTestSettings(b)
		tSettings.BlockValidation.FetchNumWorkers = benchPrefetchWorkers
		tSettings.BlockValidation.SubtreeFetchConcurrency = benchPrefetchSubtreeConcurrency
		tSettings.BlockValidation.CatchupParallelFetchEnabled = false
		tSettings.BlockValidation.CatchupPrefetchBudgetBytes = budgetBytes

		afCfg := adaptivefetch.DefaultConfig()
		afCfg.BootstrapMode = adaptivefetch.ModePessimistic

		af, err := adaptivefetch.New(afCfg, "bench-catchup-prefetch", prometheus.NewRegistry())
		if err != nil {
			b.Fatalf("failed to create adaptive fetch state: %v", err)
		}

		server := &Server{
			logger:        logger,
			settings:      tSettings,
			subtreeStore:  store,
			stats:         gocore.NewStat("bench-catchup-prefetch"),
			adaptiveFetch: af,
		}
		server.fetchSubtreeDataForBlockFn = server.fetchSubtreeDataForBlock

		if budgetBytes > 0 {
			server.catchupPrefetchBudgetBytes = budgetBytes
			server.catchupPrefetchBudget = semaphore.NewWeighted(budgetBytes)
		}

		runtime.GC()
		runtime.GC()

		var before runtime.MemStats

		runtime.ReadMemStats(&before)
		baselineHeap = before.HeapAlloc

		stopSampler := make(chan struct{})
		sampledPeak := make(chan uint64, 1)

		go func() {
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()

			var (
				peak uint64
				ms   runtime.MemStats
			)

			for {
				select {
				case <-ticker.C:
					runtime.ReadMemStats(&ms)

					if ms.HeapAlloc > peak {
						peak = ms.HeapAlloc
					}
				case <-stopSampler:
					sampledPeak <- peak
					return
				}
			}
		}()

		b.StartTimer()

		start := time.Now()
		runBenchPrefetch(b, server, fixtures)
		elapsed += time.Since(start)

		b.StopTimer()

		close(stopSampler)

		if peak := <-sampledPeak; peak > baselineHeap && peak-baselineHeap > peakIncrement {
			peakIncrement = peak - baselineHeap
		}

		if err = store.Close(context.Background()); err != nil {
			b.Fatalf("failed to close store: %v", err)
		}

		// Hold the fixtures across the whole measured region so their retention sits
		// inside the baseline rather than drifting in and out of it.
		runtime.KeepAlive(fixtures)

		b.StartTimer()
	}

	b.StopTimer()

	b.ReportMetric(float64(peakIncrement)/(1<<20), "incrHeapMB")
	b.ReportMetric(float64(baselineHeap)/(1<<20), "baselineHeapMB")

	if elapsed > 0 {
		b.ReportMetric(float64(payloadBytes*int64(b.N))/(1<<20)/elapsed.Seconds(), "MB/s")
	}
}

// runBenchPrefetch pushes every block through the real catch-up worker pool, so the budget
// acquire/release in blockWorker and the per-block subtree concurrency rule are both on the
// measured path.
func runBenchPrefetch(b *testing.B, server *Server, fixtures []benchPrefetchBlock) {
	b.Helper()

	ctx := context.Background()

	workQueue := make(chan workItem, len(fixtures))
	resultQueue := make(chan resultItem, len(fixtures))

	for i, fixture := range fixtures {
		workQueue <- workItem{block: fixture.block, index: i}
	}

	close(workQueue)

	var wg sync.WaitGroup

	for w := 0; w < server.settings.BlockValidation.FetchNumWorkers; w++ {
		workerID := w

		wg.Add(1)

		go func() {
			defer wg.Done()

			if err := server.blockWorker(ctx, workerID, workQueue, resultQueue, benchPrefetchPeerID,
				benchPrefetchBaseURL, fixtures[0].block); err != nil {
				b.Errorf("blockWorker %d failed: %v", workerID, err)
			}
		}()
	}

	wg.Wait()
	close(resultQueue)

	for result := range resultQueue {
		if result.err != nil {
			b.Errorf("prewarm failed for block %d: %v", result.index, result.err)
		}
	}
}

// BenchmarkCatchupSubtreeDataPrefetchLatency is SUPPLEMENTAL evidence about the
// oversized-block subtree-concurrency rule, and nothing more.
//
// It exists because BenchmarkCatchupSubtreeDataPrefetch understates that rule's cost: its
// oversized shape has 11 subtrees and no network latency, while a scale-size block is roughly
// 60 to 130 subtrees and every fetch crosses a real network. Serialising subtree_data fetches
// serialises round trips, not just parsing, so latency is the term that matters and the
// existing fixture has none of it.
//
// SCOPE, and it travels with every number this produces: ONE block per cell, reduced payloads,
// httpmock responders with an injected sleep, and a per-cell budget lowered until the block is
// oversized, on one workstation. Absolute figures are not transferable. The only meaningful
// quantity is R, the within-cell ratio of budget-on to budget-off wall clock at a FIXED
// injected latency, and it says something only about the subtree-concurrency rule.
//
// This is NOT a catch-up measurement on a scale-size block against a real peer. It does not
// establish that the rule's production cost is bounded or acceptable, it carries no threshold,
// and no default is chosen from it (bsv-blockchain/teranode#1139).
//
// One block per cell is deliberate. With two or more, an oversized block is clamped to the
// whole budget and admitted alone, so the others park in acquireCatchupPrefetch and the ratio
// would mix block-level admission with the per-block subtree rule rather than isolating the
// rule under discussion.
//
//	set -o pipefail
//	export SETTINGS_CONTEXT=test
//	go test -tags testtxmetacache -run '^$' -bench BenchmarkCatchupSubtreeDataPrefetchLatency \
//	  -benchtime 1x -benchmem -timeout 60m ./services/blockvalidation/ 2>&1 | tee /tmp/bench-latency.log
func BenchmarkCatchupSubtreeDataPrefetchLatency(b *testing.B) {
	fixture := buildBenchLatencyBlock(b)

	// Half the declared size: enough to make this single block oversized whatever the fixture
	// serializes to, while staying far above the 64 KiB reservation floor.
	budgetBytes := int64(fixture.block.SizeInBytes / 2) //nolint:gosec

	// The workload travels with the numbers, so a pasted result cannot be read as more than
	// it is.
	b.Logf("supplemental: 1 block, %d subtrees, %d txs each, %d declared bytes, budget-on=%d bytes (oversized), budget-off=0, httpmock responders with an injected sleep",
		benchLatencySubtrees, benchLatencyTxsPerSubtree, fixture.block.SizeInBytes, budgetBytes)

	for _, latency := range benchLatencySweep() {
		b.Run("latency="+latency.String(), func(b *testing.B) {
			var off, on time.Duration

			for i := 0; i < b.N; i++ {
				off += runBenchLatencyCell(b, fixture, 0, latency)
				on += runBenchLatencyCell(b, fixture, budgetBytes, latency)
			}

			b.ReportMetric(off.Seconds()*1000/float64(b.N), "ms_budget_off")
			b.ReportMetric(on.Seconds()*1000/float64(b.N), "ms_budget_on")

			if off > 0 {
				b.ReportMetric(float64(on)/float64(off), "R")
			}
		})
	}
}

const (
	// benchLatencySubtrees is a realistic subtree count for a scale-size block, which is
	// roughly 60 to 130 subtrees. The per-subtree payload is small instead: the serial rule's
	// latency cost scales with the NUMBER of subtrees, and materialising a genuinely
	// scale-size fixture is neither possible here nor what this measures.
	benchLatencySubtrees      = 64
	benchLatencyTxsPerSubtree = 4

	// benchLatencyScriptBytes keeps each transaction small, so the whole fixture is a couple
	// of MiB and transfer cost stays well below the injected latency.
	benchLatencyScriptBytes = 4 * 1024

	// benchLatencySubtreeConcurrency is the shipped default, so budget-off runs the fan-out a
	// real node would run.
	benchLatencySubtreeConcurrency = 32
)

// benchLatencySweep is the injected per-response latency sweep: none, a same-region round
// trip, and a wide-area one.
func benchLatencySweep() []time.Duration {
	return []time.Duration{0, 20 * time.Millisecond, 200 * time.Millisecond}
}

// buildBenchLatencyBlock materialises the single many-subtree block used by every latency cell.
func buildBenchLatencyBlock(b *testing.B) benchPrefetchBlock {
	b.Helper()

	subtrees := make([]benchPrefetchSubtree, 0, benchLatencySubtrees)
	hashes := make([]*chainhash.Hash, 0, benchLatencySubtrees)

	var (
		declared uint64
		txCount  uint64
	)

	for subtreeIdx := 0; subtreeIdx < benchLatencySubtrees; subtreeIdx++ {
		subtree := buildBenchPrefetchSubtree(b, "latency", 0, subtreeIdx, benchLatencyTxsPerSubtree,
			benchLatencyScriptBytes)

		declared += uint64(len(subtree.dataBytes))
		txCount += benchLatencyTxsPerSubtree

		subtrees = append(subtrees, subtree)
		hashes = append(hashes, subtree.hash)
	}

	return benchPrefetchBlock{
		block: &model.Block{
			Height:           2000,
			SizeInBytes:      declared,
			TransactionCount: txCount,
			Subtrees:         hashes,
		},
		subtrees: subtrees,
	}
}

// benchLatencyResponder serves a pre-built body after an injected delay. Nothing is generated
// inside the responder, so no allocation of the measured workload happens in the timed region.
func benchLatencyResponder(body []byte, latency time.Duration) httpmock.Responder {
	return func(_ *http.Request) (*http.Response, error) {
		if latency > 0 {
			time.Sleep(latency)
		}

		return httpmock.NewBytesResponse(200, body), nil
	}
}

// runBenchLatencyCell runs the one block through the real catch-up worker once and returns the
// wall clock it took. Both /subtree and /subtree_data sleep: a block parsing its subtrees one
// at a time pays both round trips per subtree.
func runBenchLatencyCell(b *testing.B, fixture benchPrefetchBlock, budgetBytes int64,
	latency time.Duration) time.Duration {
	b.Helper()

	b.StopTimer()

	logger := ulogger.TestLogger{}

	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	for _, subtree := range fixture.subtrees {
		httpmock.RegisterResponder("GET",
			fmt.Sprintf("%s/subtree/%s", benchPrefetchBaseURL, subtree.hash.String()),
			benchLatencyResponder(subtree.nodeBytes, latency))
		httpmock.RegisterResponder("GET",
			fmt.Sprintf("%s/subtree_data/%s", benchPrefetchBaseURL, subtree.hash.String()),
			benchLatencyResponder(subtree.dataBytes, latency))
	}

	// Fresh, empty file store per cell: fetchAndStoreSubtreeData returns early when the blob
	// already exists, so a reused store would turn the second cell into cache hits.
	storeURL, err := url.Parse("file://" + b.TempDir())
	if err != nil {
		b.Fatalf("failed to parse store url: %v", err)
	}

	store, err := file.New(logger, storeURL, options.WithBlobDeletionScheduler(noopBlobDeletionScheduler{}))
	if err != nil {
		b.Fatalf("failed to create file store: %v", err)
	}

	tSettings := test.CreateBaseTestSettings(b)
	// One worker for one block: block-level admission is deliberately out of scope here.
	tSettings.BlockValidation.FetchNumWorkers = 1
	tSettings.BlockValidation.SubtreeFetchConcurrency = benchLatencySubtreeConcurrency
	tSettings.BlockValidation.CatchupParallelFetchEnabled = false
	tSettings.BlockValidation.CatchupPrefetchBudgetBytes = budgetBytes

	afCfg := adaptivefetch.DefaultConfig()
	afCfg.BootstrapMode = adaptivefetch.ModePessimistic

	af, err := adaptivefetch.New(afCfg, "bench-catchup-prefetch-latency", prometheus.NewRegistry())
	if err != nil {
		b.Fatalf("failed to create adaptive fetch state: %v", err)
	}

	server := &Server{
		logger:        logger,
		settings:      tSettings,
		subtreeStore:  store,
		stats:         gocore.NewStat("bench-catchup-prefetch-latency"),
		adaptiveFetch: af,
	}
	server.fetchSubtreeDataForBlockFn = server.fetchSubtreeDataForBlock

	if budgetBytes > 0 {
		server.catchupPrefetchBudgetBytes = budgetBytes
		server.catchupPrefetchBudget = semaphore.NewWeighted(budgetBytes)
	}

	b.StartTimer()

	start := time.Now()
	runBenchPrefetch(b, server, []benchPrefetchBlock{fixture})
	elapsed := time.Since(start)

	b.StopTimer()

	if err = store.Close(context.Background()); err != nil {
		b.Fatalf("failed to close store: %v", err)
	}

	return elapsed
}
