//go:build reorgscale

package subtreeprocessor

// The reorgscale suite times a fork reorg at the sizes of issue 2269 (tens to hundreds of millions
// of pending transactions) on a large machine. It is not part of any CI run: run it with
//
//	make reorg-scale REORG_SCALE_P=125000000
//
// and keep the JSON report next to the build it measured. Settings, all optional:
//
//	REORG_SCALE_CASE        reorg (default), reset or delta: what is timed after the scenario is
//	                        built. reset times the processor's part of a reset; the reload of the
//	                        unmined transactions that follows it is BlockAssembler.loadUnminedTransactions,
//	                        timed by BenchmarkLoadUnminedTransactions. delta builds no scenario: it times
//	                        the union set of the forward blocks and computeReorgDelta (issue 2269, PR 5)
//	                        on blocks of real subtree files, where P is the winners' hashes, the lost
//	                        block holds P times LOST_RATIO nodes, LOST_IN_F of them in the winners, and
//	                        both sides round up to whole subtrees (the report has the real counts)
//	                        store (a test of services/blockassembly, which has the store workers;
//	                        this one skips) times the store step of a forward block, see
//	                        reorg_store_stage_scale_test.go there
//	REORG_SCALE_P           pending transactions (default 10M)
//	REORG_SCALE_LOST_RATIO  lost block size as a multiple of P (default 1)
//	REORG_SCALE_LOST_IN_F   fraction of the lost block in the winners (default 0.995)
//	REORG_SCALE_HITS        fraction of P in the winners, at the front of P (default 0.3)
//	REORG_SCALE_FWD         forward blocks (default 2)
//	REORG_SCALE_SUBTREE     subtree size (default 1M)
//	REORG_SCALE_MAP         memory (default) or disk; the disk map writes under TMPDIR
//	REORG_SCALE_INTAKE_TPS  live intake during the timed run, tx/s (default 0)
//	REORG_SCALE_FAST        1 to run the reorg case with blockassembly_reorgFastPath on (default off);
//	                        the report says whether the fast path applied or declined, and why
//	REORG_SCALE_K0          set (0 = every subtree) to run the reorg case with blockassembly_storedPrefixTemplate on
//	                        and blockassembly_reorgAwaitedSubtrees = n, and to report the template over time:
//	                        tip change to the first non-empty template and to 50, 90 and 100% of the template's
//	                        bytes. Four workers write each subtree's file (no meta) into a file blob store under
//	                        REORG_SCALE_BLOB (default TMPDIR)
//	REORG_SCALE_WRITE_DELAY a duration (default 0) each subtree write waits first, to stand in for a slower
//	                        store than the local disk, where the stores finish inside the rebuild
//	REORG_SCALE_BLOB        file:// path the K0 case writes the subtree files under (default: TMPDIR)
//	REORG_SCALE_VERIFY      1 to check the order oracle after the run (slow, needs the hash sets)
//	REORG_SCALE_OUT         path of the JSON report (default: logged only)
//
// Deviation from the repository's test rules, on purpose: the marks are counted, not sent to the
// UTXO store (skipStoreMarks), so the timed phases are the processor's own. The marks are measured
// on their own against Aerospike (issue 2269, plan PR 1 item 7).

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

type reorgScaleReport struct {
	Case           string  `json:"case"`
	Pending        int     `json:"pending"`
	Lost           int     `json:"lost"`
	LostInWinners  float64 `json:"lost_in_winners"`
	Hits           float64 `json:"hits"`
	ForwardBlocks  int     `json:"forward_blocks"`
	SubtreeSize    int     `json:"subtree_size"`
	Map            string  `json:"map"`
	IntakeTPS      int     `json:"intake_tps"`
	FastPath       bool    `json:"fast_path"`
	FastApplied    bool    `json:"fast_applied"`
	FastDeclined   string  `json:"fast_declined,omitempty"`
	GOMAXPROCS     int     `json:"gomaxprocs"`
	BuildSeconds   float64 `json:"build_seconds"`
	RunSeconds     float64 `json:"run_seconds"`
	PeakHeapBytes  uint64  `json:"peak_heap_bytes"`
	GCPauseSeconds float64 `json:"gc_pause_seconds"`
	NumGC          uint32  `json:"num_gc"`
	MarkedTrue     int     `json:"marked_true"`
	MarkedFalse    int     `json:"marked_false"`
	IntakeSent     int     `json:"intake_sent"`
	AssemblyLength int     `json:"assembly_length"`
	OrderVerified  bool    `json:"order_verified"`

	// K0 case: the template over time, from the start of the reorg (the tip change). Bytes are the
	// subtrees' sizes in the template, against the whole chain when every store is done.
	K0                *int    `json:"k0,omitempty"`
	TemplateFirstSecs float64 `json:"template_first_seconds,omitempty"`
	Template50Secs    float64 `json:"template_50pct_seconds,omitempty"`
	Template90Secs    float64 `json:"template_90pct_seconds,omitempty"`
	Template100Secs   float64 `json:"template_100pct_seconds,omitempty"`
	TemplateBytes     uint64  `json:"template_bytes,omitempty"`
	TemplateSteps     int     `json:"template_steps,omitempty"`

	// delta case: the real sizes after rounding up to whole subtrees, the two timings, the probe
	// rate of the delta and the sizes of its answer
	LostNodes       int     `json:"lost_nodes,omitempty"`
	WinnerHashes    int     `json:"winner_hashes,omitempty"`
	SetBuildSeconds float64 `json:"set_build_seconds,omitempty"`
	DeltaSeconds    float64 `json:"delta_seconds,omitempty"`
	ProbesPerSecond float64 `json:"probes_per_second,omitempty"`
	DeltaHits       uint64  `json:"delta_hits,omitempty"`
	D1Nodes         int     `json:"d1_nodes,omitempty"`
	D2Hashes        int     `json:"d2_hashes,omitempty"`

	// PhaseSeconds is the time of each phase of the timed run (reorg_phase_seconds), summed over
	// repeats such as the forward steps.
	PhaseSeconds map[string]float64 `json:"phase_seconds"`
	Error        string             `json:"error,omitempty"`
}

func scaleEnvInt(t *testing.T, key string, def int) int {
	t.Helper()

	v := os.Getenv(key)
	if v == "" {
		return def
	}

	n, err := strconv.Atoi(v)
	require.NoError(t, err, key)

	return n
}

func scaleEnvFloat(t *testing.T, key string, def float64) float64 {
	t.Helper()

	v := os.Getenv(key)
	if v == "" {
		return def
	}

	f, err := strconv.ParseFloat(v, 64)
	require.NoError(t, err, key)

	return f
}

// sampleHeap samples the live heap every 50 ms until stop is called and returns the peak.
func sampleHeap() (stop func() uint64) {
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	done := make(chan struct{})

	var peak atomic.Uint64

	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()

		for {
			metrics.Read(sample)

			if v := sample[0].Value.Uint64(); v > peak.Load() {
				peak.Store(v)
			}

			select {
			case <-done:
				return
			case <-ticker.C:
			}
		}
	}()

	return func() uint64 {
		close(done)
		return peak.Load()
	}
}

func TestReorgScale(t *testing.T) {
	pending := scaleEnvInt(t, "REORG_SCALE_P", 10_000_000)
	report := reorgScaleReport{
		Case:          os.Getenv("REORG_SCALE_CASE"),
		Pending:       pending,
		Lost:          int(float64(pending) * scaleEnvFloat(t, "REORG_SCALE_LOST_RATIO", 1)),
		LostInWinners: scaleEnvFloat(t, "REORG_SCALE_LOST_IN_F", 0.995),
		Hits:          scaleEnvFloat(t, "REORG_SCALE_HITS", 0.3),
		ForwardBlocks: scaleEnvInt(t, "REORG_SCALE_FWD", 2),
		SubtreeSize:   scaleEnvInt(t, "REORG_SCALE_SUBTREE", 1<<20),
		Map:           os.Getenv("REORG_SCALE_MAP"),
		IntakeTPS:     scaleEnvInt(t, "REORG_SCALE_INTAKE_TPS", 0),
		FastPath:      os.Getenv("REORG_SCALE_FAST") == "1",
		GOMAXPROCS:    runtime.GOMAXPROCS(0),
	}

	if report.Case == "" {
		report.Case = "reorg"
	}

	if report.Map == "" {
		report.Map = "memory"
	}

	if report.Case == "store" {
		t.Skip("REORG_SCALE_CASE=store runs in services/blockassembly")
	}

	require.Contains(t, []string{"reorg", "reset", "delta"}, report.Case, "REORG_SCALE_CASE")
	require.Contains(t, []string{"memory", "disk"}, report.Map, "REORG_SCALE_MAP")

	if report.Case == "delta" {
		runReorgScaleDelta(t, &report)
		writeReorgScaleReport(t, &report)

		return
	}

	var k0 *templateTimeline
	if v, ok := os.LookupEnv("REORG_SCALE_K0"); ok && report.Case == "reorg" {
		n, err := strconv.Atoi(v)
		require.NoError(t, err, "REORG_SCALE_K0")

		k0 = newTemplateTimeline(n)
		report.K0 = &n
	}

	start := time.Now()
	cfg := reorgScenarioConfig{
		seed:             2269,
		pending:          report.Pending,
		lost:             report.Lost,
		lostInWinners:    report.LostInWinners,
		pendingInWinners: report.Hits,
		hitsAtFront:      true,
		winnerOnly:       report.Pending / 1000,
		forwardBlocks:    report.ForwardBlocks,
		subtreeSize:      report.SubtreeSize,
		diskTxMap:        report.Map == "disk",
		skipStoreMarks:   true,
	}

	if k0 != nil {
		cfg.storeQueue, cfg.storedSet, cfg.reorgAwaitedSubtrees = k0.q, k0.set, k0.k0
	}

	sc := newReorgScenario(t, cfg)
	report.BuildSeconds = time.Since(start).Seconds()

	runtime.GC()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	var stopIntake func() int
	if report.IntakeTPS > 0 {
		stopIntake = sc.startIntake(report.IntakeTPS)
	}

	phaseOp := map[string]string{"reorg": "reorgBlocks", "reset": "reset"}[report.Case]
	phasesBefore := reorgPhaseSeconds(phaseOp)

	stopHeap := sampleHeap()
	start = time.Now()

	if k0 != nil {
		k0.start(t, sc, start)
	}

	var err error

	switch report.Case {
	case "reorg":
		var rec fastPathRun

		if report.FastPath {
			err = recordingFastPath(&rec)(t, sc)
			report.FastApplied = rec.applied

			if len(rec.reasons) > 0 {
				report.FastDeclined = rec.reasons[0]
			}
		} else {
			err = sc.run(t)
		}
	case "reset":
		err = sc.stp.reset(sc.genesis, []*model.Block{sc.lost}, sc.forward, false, func() error { return nil })
	}

	if k0 != nil {
		k0.committed.Store(true)
	}

	report.RunSeconds = time.Since(start).Seconds()
	report.PeakHeapBytes = stopHeap()

	if k0 != nil {
		k0.finish(t, sc, &report)
	}

	report.PhaseSeconds = make(map[string]float64)
	for phase, seconds := range reorgPhaseSeconds(phaseOp) {
		report.PhaseSeconds[phase] = seconds - phasesBefore[phase]
	}

	if stopIntake != nil {
		report.IntakeSent = stopIntake()
	}

	runtime.ReadMemStats(&after)
	report.GCPauseSeconds = float64(after.PauseTotalNs-before.PauseTotalNs) / 1e9
	report.NumGC = after.NumGC - before.NumGC
	report.MarkedTrue = sc.utxo.markCount(true)
	report.MarkedFalse = sc.utxo.markCount(false)
	report.AssemblyLength = len(assemblyNodes(sc.stp))

	if err != nil {
		report.Error = err.Error()
	}

	if err == nil && os.Getenv("REORG_SCALE_VERIFY") == "1" {
		requireAssemblyValid(t, sc.stp, sc.onChainAfter)
		report.OrderVerified = true
	}

	writeReorgScaleReport(t, &report)

	require.NoError(t, err)
}

func writeReorgScaleReport(t *testing.T, report *reorgScaleReport) {
	t.Helper()

	out, jsonErr := json.MarshalIndent(report, "", "  ")
	require.NoError(t, jsonErr)
	t.Logf("reorgscale report:\n%s", out)

	if path := os.Getenv("REORG_SCALE_OUT"); path != "" {
		require.NoError(t, os.WriteFile(path, out, 0o644))
	}
}

// runReorgScaleDelta times the two steps of the reorg delta on real subtree files in the memory
// blob store: building the union set of the forward blocks, and probing the lost block against it.
// The fixture's hash lists are dropped and collected before the timed steps, so the peak heap is
// the blob store, the set and the delta, which is what a reorg holds.
func runReorgScaleDelta(t *testing.T, report *reorgScaleReport) {
	t.Helper()

	size := report.SubtreeSize
	perBlock := report.Pending / report.ForwardBlocks
	subtreesPerWinner := max(1, (perBlock+size-1)/size)
	subtreesLost := max(1, (report.Lost+size-1)/size)

	stp, store := newTxMapTestProcessor(t)

	start := time.Now()
	d := newDeltaBlocks(t, store, deltaCase{
		lostBlocks: 1, winBlocks: report.ForwardBlocks,
		subtrees: subtreesLost, subtreeSize: size,
		overlap: report.LostInWinners, seed: 2269,
		winnerSubtrees: subtreesPerWinner,
	})
	report.BuildSeconds = time.Since(start).Seconds()

	for _, b := range d.lostLeaves {
		report.LostNodes-- // the placeholder is not a transaction
		for _, st := range b {
			report.LostNodes += len(st)
		}
	}

	for _, b := range d.winnerLeaves {
		for _, st := range b {
			report.WinnerHashes += len(st)
		}
	}

	d.lostLeaves, d.winnerLeaves = nil, nil

	ctx := context.Background()
	workers := stp.settings.BlockAssembly.SubtreeProcessorConcurrentReads

	runtime.GC()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	stopHeap := sampleHeap()
	start = time.Now()
	winners, _, err := stp.buildUnionSet(ctx, d.winners)
	report.SetBuildSeconds = time.Since(start).Seconds()
	require.NoError(t, err)

	start = time.Now()
	delta, err := stp.computeReorgDelta(ctx, d.lost, winners, workers)
	report.DeltaSeconds = time.Since(start).Seconds()
	report.PeakHeapBytes = stopHeap()

	require.NoError(t, err)

	report.RunSeconds = report.SetBuildSeconds + report.DeltaSeconds
	report.ProbesPerSecond = float64(report.LostNodes) / report.DeltaSeconds
	report.DeltaHits = delta.Hits
	report.D1Nodes = len(delta.D1)
	report.D2Hashes = len(delta.D2)

	runtime.ReadMemStats(&after)
	report.GCPauseSeconds = float64(after.PauseTotalNs-before.PauseTotalNs) / 1e9
	report.NumGC = after.NumGC - before.NumGC

	require.Equal(t, report.LostNodes, int(delta.Hits)+len(delta.D1), "every lost node is a hit or in D1")
}

// templateTimeline is the K0 case: a store queue with workers that write each subtree's file into a
// file blob store, and a sampler that watches the template the processor serves. It measures how long
// after the tip change a miner gets a first template and the rest of it, which the wait for the stores
// before the marks decides.
type templateTimeline struct {
	k0  int
	q   *StoreQueue
	set *StoredSet

	// committed is set when the reorg returns. The block assembler serves a template of the new tip
	// only once its best block header has moved, which is after the reorg returned, so the sampler
	// counts nothing before it (the processor publishes the data at every forward step).
	committed atomic.Bool

	mu      sync.Mutex
	steps   []templateStep
	stopped chan struct{}
	done    sync.WaitGroup
	cancel  context.CancelFunc
}

type templateStep struct {
	at    time.Duration
	bytes uint64
}

func newTemplateTimeline(k0 int) *templateTimeline {
	return &templateTimeline{k0: k0, q: NewStoreQueue(1 << 40), set: NewStoredSet(), stopped: make(chan struct{})}
}

// start runs the workers and the sampler from the tip change at t0. The sampler counts a template
// only once it is on the new tip and the reorg has returned.
func (tl *templateTimeline) start(t *testing.T, sc *reorgScenario, t0 time.Time) {
	t.Helper()

	base := strings.TrimPrefix(os.Getenv("REORG_SCALE_BLOB"), "file://")
	if base == "" {
		base = os.TempDir()
	}

	dir, err := os.MkdirTemp(base, "reorgscale-k0-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	storeURL, err := url.Parse("file://" + dir)
	require.NoError(t, err)

	store, err := blob.NewStore(ulogger.TestLogger{}, storeURL)
	require.NoError(t, err)

	delay, err := time.ParseDuration(os.Getenv("REORG_SCALE_WRITE_DELAY"))
	if os.Getenv("REORG_SCALE_WRITE_DELAY") == "" {
		delay, err = 0, nil
	}

	require.NoError(t, err, "REORG_SCALE_WRITE_DELAY")

	ctx, cancel := context.WithCancel(context.Background())
	tl.cancel = cancel

	for range 4 {
		tl.done.Add(1)

		go func() {
			defer tl.done.Done()

			for {
				req, _, ok := tl.q.Take(ctx)
				if !ok {
					return
				}

				res := StoreResult{SubtreeStored: true, MetaNotApplicable: true}

				if !req.AnnounceOnly {
					time.Sleep(delay)

					b, err := req.Subtree.Serialize()
					if err == nil {
						err = store.Set(ctx, req.Subtree.RootHash()[:], fileformat.FileTypeSubtree, b)
					}

					if err != nil {
						res = StoreResult{Err: err}
					}
				}

				answerFromQueue(req, tl.set, res)
			}
		}()
	}

	tip := sc.forward[len(sc.forward)-1].Header.Hash()

	tl.done.Add(1)

	go func() {
		defer tl.done.Done()

		var seen int

		for {
			if data := sc.stp.GetPrecomputedMiningData(); tl.committed.Load() && data != nil && data.PreviousHeader != nil && *data.PreviousHeader.Hash() == *tip && len(data.Subtrees) != seen {
				seen = len(data.Subtrees)

				var bytes uint64
				for _, st := range data.Subtrees {
					bytes += st.SizeInBytes
				}

				tl.mu.Lock()
				tl.steps = append(tl.steps, templateStep{at: time.Since(t0), bytes: bytes})
				tl.mu.Unlock()
			}

			select {
			case <-tl.stopped:
				return
			case <-time.After(time.Millisecond):
			}
		}
	}()

	t.Cleanup(func() {
		tl.cancel()
		tl.done.Wait()
	})
}

// finish waits for every store of the chain, stops the sampler and fills the report.
func (tl *templateTimeline) finish(t *testing.T, sc *reorgScenario, report *reorgScaleReport) {
	t.Helper()

	chain := sc.stp.chainedSubtrees
	require.Eventually(t, func() bool { return tl.set.PrefixLen(chain) == len(chain) }, 4*time.Hour, 20*time.Millisecond, "every subtree stored")
	time.Sleep(20 * time.Millisecond) // the sampler sees the last step
	close(tl.stopped)

	var total uint64
	for _, st := range chain {
		total += st.SizeInBytes
	}

	tl.mu.Lock()
	defer tl.mu.Unlock()

	report.TemplateBytes, report.TemplateSteps = total, len(tl.steps)

	at := func(pct float64) float64 {
		for _, step := range tl.steps {
			if float64(step.bytes) >= pct*float64(total) {
				return step.at.Seconds()
			}
		}

		return 0
	}

	if len(tl.steps) > 0 {
		report.TemplateFirstSecs = tl.steps[0].at.Seconds()
	}

	report.Template50Secs, report.Template90Secs, report.Template100Secs = at(0.5), at(0.9), at(1)
}
