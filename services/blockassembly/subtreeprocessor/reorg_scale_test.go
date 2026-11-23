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
//	REORG_SCALE_P           pending transactions (default 10M)
//	REORG_SCALE_LOST_RATIO  lost block size as a multiple of P (default 1)
//	REORG_SCALE_LOST_IN_F   fraction of the lost block in the winners (default 0.995)
//	REORG_SCALE_HITS        fraction of P in the winners, at the front of P (default 0.3)
//	REORG_SCALE_FWD         forward blocks (default 2)
//	REORG_SCALE_SUBTREE     subtree size (default 1M)
//	REORG_SCALE_MAP         memory (default) or disk; the disk map writes under TMPDIR
//	REORG_SCALE_INTAKE_TPS  live intake during the timed run, tx/s (default 0)
//	REORG_SCALE_VERIFY      1 to check the order oracle after the run (slow, needs the hash sets)
//	REORG_SCALE_OUT         path of the JSON report (default: logged only)
//
// Deviation from the repository's test rules, on purpose: the marks are counted, not sent to the
// UTXO store (skipStoreMarks), so the timed phases are the processor's own. The marks are measured
// on their own against Aerospike (issue 2269, plan PR 1 item 7).

import (
	"context"
	"encoding/json"
	"os"
	"runtime"
	"runtime/metrics"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/model"
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
		GOMAXPROCS:    runtime.GOMAXPROCS(0),
	}

	if report.Case == "" {
		report.Case = "reorg"
	}

	if report.Map == "" {
		report.Map = "memory"
	}

	require.Contains(t, []string{"reorg", "reset", "delta"}, report.Case, "REORG_SCALE_CASE")
	require.Contains(t, []string{"memory", "disk"}, report.Map, "REORG_SCALE_MAP")

	if report.Case == "delta" {
		runReorgScaleDelta(t, &report)
		writeReorgScaleReport(t, &report)

		return
	}

	start := time.Now()
	sc := newReorgScenario(t, reorgScenarioConfig{
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
	})
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

	var err error

	switch report.Case {
	case "reorg":
		err = sc.run(t)
	case "reset":
		err = sc.stp.reset(sc.genesis, []*model.Block{sc.lost}, sc.forward, false, func() error { return nil })
	}

	report.RunSeconds = time.Since(start).Seconds()
	report.PeakHeapBytes = stopHeap()

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
