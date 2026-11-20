//go:build reorgscale

package subtreeprocessor

// The reorgscale suite times a fork reorg at the sizes of issue 2269 (tens to hundreds of millions
// of pending transactions) on a large machine. It is not part of any CI run: run it with
//
//	make reorg-scale REORG_SCALE_P=125000000
//
// and keep the JSON report next to the build it measured. Settings, all optional:
//
//	REORG_SCALE_CASE        reorg (default) or reset: what is timed after the scenario is built
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
	Error          string  `json:"error,omitempty"`
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

	require.Contains(t, []string{"reorg", "reset"}, report.Case, "REORG_SCALE_CASE")
	require.Contains(t, []string{"memory", "disk"}, report.Map, "REORG_SCALE_MAP")

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

	out, jsonErr := json.MarshalIndent(report, "", "  ")
	require.NoError(t, jsonErr)
	t.Logf("reorgscale report:\n%s", out)

	if path := os.Getenv("REORG_SCALE_OUT"); path != "" {
		require.NoError(t, os.WriteFile(path, out, 0o644))
	}

	require.NoError(t, err)
}
