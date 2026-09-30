//go:build soak

// Package soaktest holds the endurance (soak) test: it runs the in-process daemon under steady transaction load for
// a long wall-clock duration and fails if heap or goroutine counts trend upward. It is behind the soak build tag so it
// never runs in the normal unit, smoke or nightly daemon suites; run it with `make soaktest`.
package soaktest

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/internal/soak"
	"github.com/bsv-blockchain/teranode/test"
	inmemorykafka "github.com/bsv-blockchain/teranode/util/kafka/in_memory_kafka"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"golang.org/x/sync/errgroup"
)

const (
	// txFee is the fee each load transaction pays; a 1-in/1-out P2PKH tx at 1000 sats clears the test fee policy.
	txFee = 1000

	// sendConcurrency bounds the goroutines submitting a cycle's transactions, so the load generator itself stays
	// constant and does not show up as goroutine growth.
	sendConcurrency = 16

	// maxMineAttempts bounds how many blocks a cycle may mine while waiting for all its transactions to confirm.
	maxMineAttempts = 5

	// goleakLogLimit truncates the post-stop goroutine report so a noisy shutdown does not flood the log.
	goleakLogLimit = 8192
)

type config struct {
	duration       time.Duration
	warmup         time.Duration
	sampleInterval time.Duration
	txsPerCycle    int
	utxoStoreType  string
	outputDir      string
	injectLeak     bool
	tolerance      soak.Tolerance
}

// loadConfig reads the soak settings from the environment. Defaults suit a local 30 minute run; the nightly job
// passes a multi-hour SOAK_DURATION.
func loadConfig(t *testing.T) config {
	t.Helper()

	cfg := config{
		duration:       envDuration(t, "SOAK_DURATION", 30*time.Minute),
		sampleInterval: envDuration(t, "SOAK_SAMPLE_INTERVAL", 30*time.Second),
		txsPerCycle:    envInt(t, "SOAK_TXS_PER_CYCLE", 500),
		utxoStoreType:  os.Getenv("SOAK_UTXO_STORE"),
		outputDir:      os.Getenv("SOAK_OUTPUT_DIR"),
		injectLeak:     envBool(t, "SOAK_INJECT_LEAK"),
		tolerance:      soak.DefaultTolerance(),
	}

	// Default warm-up: the larger of 5 minutes and a fifth of the run, but never more than half of it.
	defaultWarmup := time.Duration(math.Min(
		math.Max(float64(5*time.Minute), float64(cfg.duration)/5),
		float64(cfg.duration)/2,
	))
	cfg.warmup = envDuration(t, "SOAK_WARMUP", defaultWarmup)

	cfg.tolerance.HeapRel = envFloat(t, "SOAK_HEAP_TOLERANCE", cfg.tolerance.HeapRel)
	cfg.tolerance.GoroutineRel = envFloat(t, "SOAK_GOROUTINE_TOLERANCE", cfg.tolerance.GoroutineRel)

	require.Positive(t, cfg.txsPerCycle, "SOAK_TXS_PER_CYCLE must be positive")
	require.Less(t, cfg.warmup, cfg.duration, "SOAK_WARMUP must be shorter than SOAK_DURATION")

	postWarmupSamples := int((cfg.duration - cfg.warmup) / cfg.sampleInterval)
	require.GreaterOrEqual(t, postWarmupSamples, soak.MinAnalysisSamples,
		"SOAK_DURATION - SOAK_WARMUP must allow at least %d samples at SOAK_SAMPLE_INTERVAL", soak.MinAnalysisSamples)

	if cfg.outputDir == "" {
		cfg.outputDir = t.TempDir()
	} else {
		require.NoError(t, os.MkdirAll(cfg.outputDir, 0o750))
	}

	return cfg
}

// TestSoakSteadyLoad runs the real in-process daemon (propagation, validator, block assembly, block and subtree
// validation, blockchain) under a constant transaction rate for SOAK_DURATION, samples heap and goroutine counts every
// SOAK_SAMPLE_INTERVAL, and fails if, after SOAK_WARMUP, either trends upward beyond the tolerance.
//
// The load is steady by construction: a fixed pool of SOAK_TXS_PER_CYCLE UTXOs is spent 1-in/1-out each cycle, the
// outputs become the next cycle's pool, and every cycle ends with the transactions mined. The UTXO set and the
// per-block transaction count therefore stay constant, so a rising trend points at retained state, not more work.
//
// Set SOAK_INJECT_LEAK=1 to add a deliberate per-cycle leak and confirm the test fails.
func TestSoakSteadyLoad(t *testing.T) {
	cfg := loadConfig(t)

	if deadline, ok := t.Deadline(); ok {
		require.Greater(t, time.Until(deadline), cfg.duration+10*time.Minute,
			"go test -timeout must exceed SOAK_DURATION (%s) by at least 10m for setup and teardown", cfg.duration)
	}

	t.Logf("soak config: duration=%s warmup=%s sample_interval=%s txs_per_cycle=%d utxo_store=%q inject_leak=%v output_dir=%s",
		cfg.duration, cfg.warmup, cfg.sampleInterval, cfg.txsPerCycle, cfg.utxoStoreType, cfg.injectLeak, cfg.outputDir)

	// Snapshot the goroutines that predate the daemon, so the post-stop report lists only what the daemon left behind.
	preexisting := goleak.IgnoreCurrent()

	leak := newLeakInjector()
	defer leak.release()

	td := daemon.NewTestDaemon(t, daemon.TestOptions{
		EnableRPC:            true,
		EnableValidator:      true,
		UTXOStoreType:        cfg.utxoStoreType,
		SettingsOverrideFunc: test.ComposeSettings(test.SystemTestSettings()),
	})

	stopped := false

	defer func() {
		if !stopped {
			td.Stop(t, true)
		}
	}()

	require.NoError(t, td.BlockchainClient.Run(td.Ctx, "test"))

	privKey := td.GetPrivateKey(t)

	coinbaseTx := td.MineToMaturityAndGetSpendableCoinbaseTx(t, td.Ctx)

	parentTx, err := td.CreateParentTransactionWithNOutputs(t, coinbaseTx, cfg.txsPerCycle)
	require.NoError(t, err)

	td.MineAndWait(t, 1)

	pool := outputsOf(parentTx)

	sampler := soak.NewSampler(cfg.sampleInterval)
	sampler.Start(td.Ctx)

	start := time.Now()
	cycles := 0
	baselineWritten := false

	for time.Since(start) < cfg.duration {
		pool = runCycle(t, td, privKey, pool)
		cycles++

		// The test daemon's in-memory Kafka broker keeps every message ever produced. Production Kafka does not,
		// and by now the cycle is mined, so every consumer has its messages; drop the history so harness retention
		// is not reported as a daemon leak.
		truncateInMemoryKafka()

		if cfg.injectLeak {
			leak.inject()
		}

		if !baselineWritten && time.Since(start) >= cfg.warmup {
			// A heap profile at the start of the analysis window lets a failure be diagnosed with
			// go tool pprof -diff_base, rather than guessed at from a single end-of-run snapshot.
			writeProfile(t, cfg.outputDir, "heap", "soak-heap-baseline.pprof", 0)

			baselineWritten = true
		}
	}

	samples := sampler.Stop()

	t.Logf("soak load done: cycles=%d txs=%d elapsed=%s", cycles, cycles*cfg.txsPerCycle, time.Since(start).Round(time.Second))

	csvPath := filepath.Join(cfg.outputDir, "soak-samples.csv")
	require.NoError(t, soak.WriteCSV(csvPath, samples))
	t.Logf("soak samples written to %s", csvPath)

	result, err := soak.Analyze(samples, cfg.warmup, cfg.tolerance)
	require.NoError(t, err)

	t.Logf("soak result: %s", result)

	if !result.Flat() {
		// Capture profiles while the daemon is still running, so they show what the services are holding on to.
		writeProfile(t, cfg.outputDir, "heap", "soak-heap.pprof", 0)
		writeProfile(t, cfg.outputDir, "goroutine", "soak-goroutine.pprof", 1)

		t.Logf("diagnose heap growth with: go tool pprof -sample_index=inuse_space -diff_base %s %s",
			filepath.Join(cfg.outputDir, "soak-heap-baseline.pprof"), filepath.Join(cfg.outputDir, "soak-heap.pprof"))
	}

	td.Stop(t, true)
	stopped = true

	leak.release()
	logRemainingGoroutines(t, preexisting)

	require.True(t, result.Flat(), "heap or goroutine count grew under steady load: %v", result.Leaking())
}

// runCycle spends every UTXO in pool with a 1-in/1-out transaction, waits until they are all mined, and returns the
// new outputs as the next pool.
func runCycle(t *testing.T, td *daemon.TestDaemon, privKey *bec.PrivateKey, pool []*bt.UTXO) []*bt.UTXO {
	t.Helper()

	txs := make([]*bt.Tx, len(pool))

	g, ctx := errgroup.WithContext(td.Ctx)
	g.SetLimit(sendConcurrency)

	for i, utxo := range pool {
		g.Go(func() error {
			tx, err := spend(ctx, privKey, utxo)
			if err != nil {
				return err
			}

			if err = td.PropagationClient.ProcessTransaction(ctx, tx); err != nil {
				return errors.NewProcessingError("failed to send tx %s", tx.TxID(), err)
			}

			txs[i] = tx

			return nil
		})
	}

	require.NoError(t, g.Wait())

	waitForMiningCandidate(t, td, len(txs))
	mineUntilConfirmed(t, td, len(txs))

	next := make([]*bt.UTXO, len(txs))
	for i, tx := range txs {
		next[i] = outputsOf(tx)[0]
	}

	return next
}

// spend builds and signs a 1-in/1-out transaction paying utxo, less txFee, back to privKey.
func spend(ctx context.Context, privKey *bec.PrivateKey, utxo *bt.UTXO) (*bt.Tx, error) {
	if utxo.Satoshis <= txFee+1 {
		return nil, errors.NewProcessingError("utxo %s:%d has %d sats, too few to pay the %d sat fee; lower SOAK_DURATION or SOAK_TXS_PER_CYCLE",
			utxo.TxIDHash, utxo.Vout, utxo.Satoshis, txFee)
	}

	tx := bt.NewTx()
	if err := tx.FromUTXOs(utxo); err != nil {
		return nil, errors.NewProcessingError("failed to add input", err)
	}

	if err := tx.AddP2PKHOutputFromPubKeyBytes(privKey.PubKey().Compressed(), utxo.Satoshis-txFee); err != nil {
		return nil, errors.NewProcessingError("failed to add output", err)
	}

	if err := tx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privKey}); err != nil {
		return nil, errors.NewProcessingError("failed to sign tx", err)
	}

	return tx, nil
}

// outputsOf returns every output of tx as a spendable UTXO.
func outputsOf(tx *bt.Tx) []*bt.UTXO {
	out := make([]*bt.UTXO, len(tx.Outputs))
	for i, o := range tx.Outputs {
		out[i] = &bt.UTXO{
			TxIDHash:      tx.TxIDChainHash(),
			Vout:          uint32(i), //nolint:gosec // output index is bounded by the tx's output count
			LockingScript: o.LockingScript,
			Satoshis:      o.Satoshis,
		}
	}

	return out
}

// waitForMiningCandidate polls block assembly until the mining candidate holds n transactions, so the next block
// normally confirms the whole cycle. On timeout it returns anyway; mineUntilConfirmed is the authoritative check.
func waitForMiningCandidate(t *testing.T, td *daemon.TestDaemon, n int) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)

	for time.Now().Before(deadline) {
		candidate, err := td.BlockAssemblyClient.GetMiningCandidate(td.Ctx)
		if err == nil && int(candidate.NumTxs) >= n {
			return
		}

		time.Sleep(100 * time.Millisecond)
	}

	t.Logf("mining candidate did not reach %d txs within 30s; mining anyway", n)
}

// mineUntilConfirmed mines blocks until n non-coinbase transactions have been confirmed, failing after
// maxMineAttempts blocks. Exactly n must confirm: nothing else is submitted, so more would mean double counting.
func mineUntilConfirmed(t *testing.T, td *daemon.TestDaemon, n int) {
	t.Helper()

	confirmed := 0

	for attempt := 1; attempt <= maxMineAttempts; attempt++ {
		block := td.MineAndWait(t, 1)
		confirmed += int(block.TransactionCount) - 1 //nolint:gosec // per-block tx count fits in int

		if confirmed >= n {
			require.Equal(t, n, confirmed, "confirmed more transactions than the cycle sent")
			return
		}

		time.Sleep(500 * time.Millisecond)
	}

	t.Fatalf("only %d of %d cycle transactions confirmed after %d blocks", confirmed, n, maxMineAttempts)
}

// injectedLeak is the memory retained by SOAK_INJECT_LEAK. It is package-level on purpose: that is the leak shape
// the soak test must catch.
var injectedLeak [][]byte

// leakInjector implements SOAK_INJECT_LEAK: each inject retains 1 MiB and parks a goroutine until release.
type leakInjector struct {
	done chan struct{}
	once sync.Once
}

func newLeakInjector() *leakInjector {
	return &leakInjector{done: make(chan struct{})}
}

func (l *leakInjector) inject() {
	injectedLeak = append(injectedLeak, make([]byte, 1024*1024))

	go func() { <-l.done }()
}

// release frees the injected memory and goroutines; it is safe to call more than once.
func (l *leakInjector) release() {
	l.once.Do(func() {
		close(l.done)

		injectedLeak = nil
	})
}

// truncateInMemoryKafka drops the retained-message history of every topic on the shared in-memory broker. Only the
// history buffer is cleared; per-consumer delivery channels are untouched.
func truncateInMemoryKafka() {
	broker := inmemorykafka.GetSharedBroker()
	for _, topic := range broker.Topics() {
		broker.TruncateTopic(topic)
	}
}

// writeProfile saves the named runtime profile to dir/file. Failures are logged, not fatal: profiles are diagnostics.
func writeProfile(t *testing.T, dir, name, file string, debug int) {
	t.Helper()

	path := filepath.Join(dir, file)

	f, err := os.Create(path)
	if err != nil {
		t.Logf("failed to create %s profile: %v", name, err)
		return
	}

	if err = pprof.Lookup(name).WriteTo(f, debug); err != nil {
		t.Logf("failed to write %s profile: %v", name, err)
	}

	_ = f.Close()

	t.Logf("%s profile written to %s", name, path)
}

// logRemainingGoroutines reports goroutines still running after the daemon stopped. It only logs: the daemon leaves
// process-global background goroutines behind by design, so a hard assertion here would fail on every run, while the
// trend assertion above is what catches a leak under load.
func logRemainingGoroutines(t *testing.T, preexisting goleak.Option) {
	t.Helper()

	err := goleak.Find(preexisting)
	if err == nil {
		t.Log("goleak: no goroutines left after daemon stop")
		return
	}

	report := err.Error()
	if len(report) > goleakLogLimit {
		report = report[:goleakLogLimit] + "\n... (truncated)"
	}

	t.Logf("goleak: goroutines left after daemon stop (informational):\n%s", report)
}

func envDuration(t *testing.T, key string, def time.Duration) time.Duration {
	t.Helper()

	v := os.Getenv(key)
	if v == "" {
		return def
	}

	d, err := time.ParseDuration(v)
	require.NoError(t, err, "invalid %s", key)
	require.Positive(t, d, "%s must be positive", key)

	return d
}

func envInt(t *testing.T, key string, def int) int {
	t.Helper()

	v := os.Getenv(key)
	if v == "" {
		return def
	}

	n, err := strconv.Atoi(v)
	require.NoError(t, err, "invalid %s", key)

	return n
}

func envFloat(t *testing.T, key string, def float64) float64 {
	t.Helper()

	v := os.Getenv(key)
	if v == "" {
		return def
	}

	f, err := strconv.ParseFloat(v, 64)
	require.NoError(t, err, "invalid %s", key)

	return f
}

func envBool(t *testing.T, key string) bool {
	t.Helper()

	v := os.Getenv(key)
	if v == "" {
		return false
	}

	b, err := strconv.ParseBool(v)
	require.NoError(t, err, "invalid %s", key)

	return b
}
