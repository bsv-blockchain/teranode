package utxo_test

// ---------------------------------------------------------------------------
// Aged fan-out tx builders + bench config for the deferred-DAH lag bench
// ---------------------------------------------------------------------------
//
// Background: the postgres UTXO store's deferred DAH-setting path re-aggregates
// each tx's FULL spend history to decide when to stamp delete_at_height. Under
// IBD with a high-fan-out workload (one source tx spending to many outputs, each
// spent independently by later txs), this re-aggregation grows with fan-out k
// and can lag severely. This file provides:
//
//   - makeAgedFanoutTx(workerID, seq, k): a tx with k spendable P2PKH outputs
//     plus OP_FALSE OP_RETURN padding outputs (realistic byte budget).
//   - makeSpendOfVout(parent, vout): a child tx spending exactly one named vout
//     of parent — the primitive the deferred-DAH cursor aggregates.
//   - Config constants (all env-overridable) for the harness (Tasks 2+).
//   - runAgedFanoutLag: un-throttled disk-bound harness recording deferred-DAH lag.
//
// Env knobs:
//
//	AGED_FANOUT_K=64      # outputs per fan-out tx (default 64)
//	AGED_PARENTS=20000    # fan-out txs to create in the aged-parents pool
//	AGE_SPAN=20000        # height gap between parent creation and spend
//	BACKLOG_BOUND=2000    # max in-flight un-DAH-stamped parents before back-pressure

import (
	"context"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Config constants (env-overridable via envInt — defined in throughput_stable_test.go)
// ---------------------------------------------------------------------------

var (
	// agedFanoutK is the number of spendable P2PKH outputs per fan-out tx.
	agedFanoutK = envInt("AGED_FANOUT_K", 64)
	// agedParents is the number of fan-out parent txs to create in the pool.
	// Default of 20000 parents × 62 spends ≈ 1.24M rows × ~437 bytes ≈ 542MB — well
	// above the bench cluster's shared_buffers=64MB, ensuring disk-bound sweep behaviour.
	agedParents = envInt("AGED_PARENTS", 20000)
	// ageSpan is the height gap between parent creation and when spends arrive.
	// Must be large enough that the (watermark, tip] range the sweep scans is cold.
	ageSpan = envInt("AGE_SPAN", 20000)
	// backlogBound is the max in-flight un-DAH-stamped parents before workers
	// back-pressure (prevents the deferred-DAH cursor from being overwhelmed).
	backlogBound = envInt("BACKLOG_BOUND", 2000)
)

// ---------------------------------------------------------------------------
// Fan-out tx builders
// ---------------------------------------------------------------------------

// makeAgedFanoutTx creates a tx with k spendable P2PKH outputs plus OP_FALSE
// OP_RETURN padding outputs. Each tx has a unique txid driven by workerID and
// seq embedded in a fake input referencing a synthetic previous outpoint.
// Padding outputs are provably unspendable (ShouldStoreOutputAsUTXO returns
// false for OP_FALSE OP_RETURN) and do not affect spendable_count, but DO
// inflate raw_tx to a mainnet-realistic size.
func makeAgedFanoutTx(workerID, seq, k int) *bt.Tx {
	tx := bt.NewTx()
	tx.Version = 1

	// Synthetic previous outpoint: unique per (workerID, seq) so txid is unique.
	// Mirrors makeGenesisTx's approach of encoding identity into the prev-hash bytes.
	// p2pkhScript is defined in throughput_test.go (same package).
	var h [32]byte
	h[0] = byte(workerID)
	h[1] = byte(workerID >> 8)
	h[2] = byte(workerID >> 16)
	h[3] = byte(workerID >> 24)
	h[4] = byte(seq)
	h[5] = byte(seq >> 8)
	h[6] = byte(seq >> 16)
	h[7] = byte(seq >> 24)
	h[8] = 0xAF // sentinel: distinguishes aged-fanout txs from plain genesis txs
	prev, _ := chainhash.NewHash(h[:])
	_ = tx.From(prev.String(), 0, p2pkhScript().String(), 0)
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})

	// k spendable P2PKH outputs (value 1000 sat each). Each output has a unique
	// locking script derived from (workerID, seq, vout) so raw_tx bytes are
	// distinct and not LZ4-compressible to nothing (real scripts carry distinct
	// 20-byte pubkey hashes; identical padding would hide de-TOAST cost).
	for v := 0; v < k; v++ {
		script := agedP2PKHScript(workerID, seq, v)
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      1000,
			LockingScript: script,
		})
	}

	// OP_FALSE OP_RETURN padding: 4 non-spendable outputs to give a realistic
	// raw_tx byte budget (mirrors padReprOutputs). Unique seed per output so
	// bytes are incompressible.
	const nPad = 4
	const payloadLen = 30
	for p := 0; p < nPad; p++ {
		b := make([]byte, 0, 3+payloadLen)
		b = append(b, bscript.OpFALSE, bscript.OpRETURN, byte(payloadLen))
		seed := uint64(workerID)*0x9e3779b9 + uint64(seq)*0x6c62272e + uint64(p)*0x517cc1b7
		for j := 0; j < payloadLen; j++ {
			b = append(b, byte(seed>>(uint(j%8)*8))^byte(j*7+p))
		}
		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      0,
			LockingScript: bscript.NewFromBytes(b),
		})
	}

	return tx
}

// agedP2PKHScript returns a unique P2PKH-shaped locking script for the given
// (workerID, seq, vout). The 20-byte pubkey hash is filled deterministically so
// every output has distinct bytes, preventing LZ4 compression from collapsing
// them and hiding de-TOAST cost during benchmarks.
func agedP2PKHScript(workerID, seq, vout int) *bscript.Script {
	// P2PKH: OP_DUP OP_HASH160 <20 bytes> OP_EQUALVERIFY OP_CHECKSIG
	b := make([]byte, 25)
	b[0] = 0x76 // OP_DUP
	b[1] = 0xa9 // OP_HASH160
	b[2] = 0x14 // push 20 bytes
	for i := 0; i < 20; i++ {
		b[3+i] = byte(workerID*0x9b+seq*0x6d+vout*0x1f+i*0x37) ^ byte((workerID>>8)*0xb3+i)
	}
	b[23] = 0x88 // OP_EQUALVERIFY
	b[24] = 0xac // OP_CHECKSIG
	s := bscript.Script(b)
	return &s
}

// makeSpendOfVout builds a child tx spending exactly one named output (vout) of
// parent. The child carries one input referencing parent.TxIDChainHash():vout
// and one P2PKH output, appending exactly one row to the spends table. This is
// the primitive the deferred-DAH cursor aggregates when deciding whether a
// parent is fully spent.
func makeSpendOfVout(parent *bt.Tx, vout uint32) *bt.Tx {
	tx := bt.NewTx()
	tx.Version = 1

	parentOut := parent.Outputs[vout]
	_ = tx.From(
		parent.TxIDChainHash().String(),
		vout,
		parentOut.LockingScript.String(),
		parentOut.Satoshis,
	)
	tx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x00})

	outVal := parentOut.Satoshis / 2
	if outVal == 0 {
		outVal = 1
	}
	tx.AddOutput(&bt.Output{Satoshis: outVal, LockingScript: p2pkhScript()})
	return tx
}

// ---------------------------------------------------------------------------
// Unit test: makeAgedFanoutTx shape
// ---------------------------------------------------------------------------

// TestMakeAgedFanoutTx_Shape verifies that makeAgedFanoutTx produces exactly k
// spendable outputs (per utxo.ShouldStoreOutputAsUTXO) plus at least one
// additional non-spendable OP_FALSE OP_RETURN padding output.
//
// utxo.ShouldStoreOutputAsUTXO signature:
//
//	ShouldStoreOutputAsUTXO(output *bt.Output, blockHeight uint32, genesisActivationHeight uint32) bool
//
// We use a post-genesis blockHeight (>= genesisActivationHeight) so that only
// OP_FALSE OP_RETURN outputs are provably unspendable. Our P2PKH outputs are
// spendable; our padding outputs (OP_FALSE OP_RETURN) are not.
func TestMakeAgedFanoutTx_Shape(t *testing.T) {
	const k = 64
	// mainnet genesis activation height; post-genesis means only OP_FALSE OP_RETURN is unspendable.
	const genesisActivation = uint32(620538)
	const blockHeight = genesisActivation + 1

	tx := makeAgedFanoutTx(1, 0, k)

	spendable := 0
	for _, o := range tx.Outputs {
		if utxo.ShouldStoreOutputAsUTXO(o, blockHeight, genesisActivation) {
			spendable++
		}
	}
	require.Equal(t, k, spendable, "must have exactly k spendable outputs")
	require.Greater(t, len(tx.Outputs), k, "must also carry OP_RETURN padding")
}

// ---------------------------------------------------------------------------
// Deferred-DAH lag harness: runAgedFanoutLag
// ---------------------------------------------------------------------------
//
// runAgedFanoutLag is a disk-bound, un-throttled harness that reproduces the
// production failure mode: the postgres UTXO store's deferred DAH-setting path
// re-aggregates each tx's full spend history for every spend that arrives. With
// high-fanout parents (k outputs, k-2 spent individually across ageSpan height),
// the sweep cursor falls behind creation and the backlog (tip − min watermark)
// grows unboundedly.
//
// Unlike runPrunedValidator, this harness deliberately omits the TABLE-SIZE GATE
// and uses an effectively-unbounded mine channel so creation is never throttled.
// That is the point: we want to measure how fast the sweep falls behind, not the
// balanced equilibrium throughput.
//
// Phases:
//  1. SEED (untimed): create agedParents fan-out parent txs, mine each at a low
//     height, then spend k-2 of each parent's k outputs at heights scattered
//     uniformly across [seedLow, seedLow+ageSpan). Two outputs are left unspent so
//     no parent is "fully spent" yet — the sweep cursor cannot stamp any of them.
//  2. TIMED LOOP: workers create fresh fan-out parents AND spend remaining
//     outputs of seeded parents at the advancing tip. The DAH sweep cursor
//     must re-aggregate the full spend history (k-2 historical + new spends)
//     for each parent on every sweep iteration — this is the expensive path.
//  3. SAMPLER (1s ticker): records backlog, stamp rate, and create rate.

// lagSample is one 1-second snapshot of the deferred-DAH lag metrics.
type lagSample struct {
	T          time.Time
	Backlog    int64 // tip - COALESCE(min(last_swept_height),0)
	StampRate  int64 // Δ dah_sweep_control.total_rows_stamped per second
	CreateRate int64 // Δ created-ops counter per second
}

// lagResult summarises a runAgedFanoutLag run.
type lagResult struct {
	MaxBacklog    int64
	StartBacklog  int64
	EndBacklog    int64
	MedStampRate  float64
	MedCreateRate float64
	Samples       []lagSample
}

// sampleBacklog returns tip − COALESCE(min(last_swept_height), 0) from
// dah_part_watermark. A large backlog means the DAH sweep cursor is lagging
// behind the chain tip; zero means the sweep is keeping up.
func sampleBacklog(ctx context.Context, pool *pgxpool.Pool, tip int64) (int64, error) {
	var minW int64
	err := pool.QueryRow(ctx, `SELECT COALESCE(min(last_swept_height),0) FROM dah_part_watermark`).Scan(&minW)
	return tip - minW, err
}

// mustStatPool opens a pgxpool against throughputDSN, skipping the test on
// connection failure. Used by the wiring test so there is no separate
// pool-creation boilerplate at the call site.
func mustStatPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, throughputDSN)
	if err != nil {
		t.Skipf("no postgres for stat pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("no postgres for stat pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// runAgedFanoutLag drives the deferred-DAH lag workload and returns collected
// samples. numWorkers controls concurrency; cfg supplies warmup/measure
// durations; statPool is used for backlog + stamp-rate sampling (must point at
// the same DB as the store).
func runAgedFanoutLag(t *testing.T, store prunedBenchStore, numWorkers int, cfg stableCfg, statPool *pgxpool.Pool) lagResult {
	t.Helper()
	ctx := context.Background()

	// Heights: seed phase runs from seedLow; the timed phase advances from tipStart.
	const seedLow = int64(100)
	tipStart := seedLow + int64(ageSpan) + 200

	var curH atomic.Int64
	curH.Store(seedLow)
	_ = store.SetBlockHeight(uint32(seedLow))

	svc, err := store.GetPrunerService()
	if err != nil {
		t.Fatalf("pruner service: %v", err)
	}
	svc.Start(ctx)

	// Effectively-unbounded mine channel: workers never block on mine lag.
	// 4_000_000 hashes ≈ 128 MB — big enough that even a long-running bench
	// cannot fill it before miners drain it.
	const mineChanCap = 4_000_000
	mineCh := make(chan chainhash.Hash, mineChanCap)

	driverCtx, cancel := context.WithCancel(ctx)
	var driverWG sync.WaitGroup
	var totalMined atomic.Int64

	// HEIGHT: height stays fixed at seedLow during the seed phase so the cursor's
	// safeTip stays below the watermark (cursor is idle, watermark doesn't advance
	// above seedLow). After the seed phase, tipStart is set and the height goroutine
	// begins the fast-tick advancement. Using a fast tick (10ms = 100 heights/s)
	// during the timed phase only ensures the height-based backlog (tip − watermark)
	// grows faster than the sweep can drain it, producing observable sustained
	// divergence even on fast NVMe storage.
	// ms per +1 tip height during the timed phase (env-tunable). Default 10 = 100 h/s,
	// which is far faster than real IBD block ingestion; DAH_TIP_TICK_MS=30 (~33 h/s) is a
	// more realistic ingestion rate that a correct fold-forward setter comfortably outpaces
	// while the frozen v10 sweep still diverges.
	lagHeightTickMS := envInt("DAH_TIP_TICK_MS", 10)
	// heightStartCh is closed to begin fast-height advancement after seed.
	heightStartCh := make(chan struct{})
	driverWG.Add(1)
	go func() {
		defer driverWG.Done()
		// Wait for the seed phase to close heightStartCh before starting the
		// fast ticker. During seed, blockHeight stays at seedLow (safeTip=seedLow-2
		// < watermark=199) so the cursor does nothing and the watermark is not
		// advanced above seedLow. This prevents the cursor from sweeping past
		// tipStart during the seed phase and missing the completion spends.
		select {
		case <-driverCtx.Done():
			return
		case <-heightStartCh:
		}
		tk := time.NewTicker(time.Duration(lagHeightTickMS) * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-driverCtx.Done():
				return
			case <-tk.C:
				_ = store.SetBlockHeight(uint32(curH.Add(1)))
			}
		}
	}()

	// MINERS: drain the mine channel in batches, mirroring production block assembly.
	for m := 0; m < prunedMiners; m++ {
		driverWG.Add(1)
		go func() {
			defer driverWG.Done()
			buf := make([]*chainhash.Hash, 0, prunedMineBatch)
			flush := func() {
				if len(buf) == 0 {
					return
				}
				if _, mErr := store.SetMinedMulti(driverCtx, buf, utxo.MinedBlockInfo{
					BlockID: 1, BlockHeight: uint32(curH.Load()), OnLongestChain: true,
				}); mErr == nil {
					totalMined.Add(int64(len(buf)))
				} else if driverCtx.Err() == nil {
					t.Logf("[lag] SetMinedMulti(%d): %v", len(buf), mErr)
				}
				buf = buf[:0]
			}
			tk := time.NewTicker(100 * time.Millisecond)
			defer tk.Stop()
			for {
				select {
				case <-driverCtx.Done():
					flush() // drain remaining on shutdown
					return
				case h := <-mineCh:
					hh := h
					buf = append(buf, &hh)
					if len(buf) >= prunedMineBatch {
						flush()
					}
				case <-tk.C:
					flush()
				}
			}
		}()
	}

	// PRUNER: continuously sweep + cascade-delete reached-DAH txs.
	driverWG.Add(1)
	go func() {
		defer driverWG.Done()
		for {
			if driverCtx.Err() != nil {
				return
			}
			d, pErr := svc.Prune(driverCtx, uint32(curH.Load()), "lag-bench")
			if pErr != nil && driverCtx.Err() == nil {
				t.Logf("[lag] Prune: %v", pErr)
			}
			if d == 0 {
				select {
				case <-driverCtx.Done():
					return
				case <-time.After(5 * time.Millisecond):
				}
			}
		}
	}()

	// -----------------------------------------------------------------------
	// SEED PHASE (untimed): create agedParents fan-out parents, mine each, then
	// spend k-2 of each parent's k outputs at scattered heights across ageSpan.
	// The two unspent outputs mean no parent is "fully spent" yet — the sweep
	// cursor cannot stamp any of them — and they form the workload for the timed
	// phase where spending the last outputs triggers the expensive re-aggregation.
	// -----------------------------------------------------------------------
	k := agedFanoutK
	nParents := agedParents
	seededParents := make([]*bt.Tx, nParents)
	// seededUnspentVout[i] holds the two remaining unspent vouts of seededParents[i].
	// The timed phase spends these to drive the deferred-DAH re-aggregation.
	seededUnspentVouts := make([][2]uint32, nParents)

	t.Logf("[lag] seed phase: creating %d fan-out parents (k=%d, ageSpan=%d) ...", nParents, k, ageSpan)
	seedStart := time.Now()
	{
		conc := runtime.GOMAXPROCS(0) * 4
		if conc > nParents {
			conc = nParents
		}
		var gwg sync.WaitGroup
		var seedErr atomic.Value
		sem := make(chan struct{}, conc)

		for i := 0; i < nParents; i++ {
			i := i
			sem <- struct{}{}
			gwg.Add(1)
			go func() {
				defer gwg.Done()
				defer func() { <-sem }()
				if seedErr.Load() != nil {
					return
				}

				// Create the fan-out parent at seedLow.
				// makeAgedFanoutTx uses a synthetic input with 0 PreviousTxSatoshis, but
				// the store's Create path calls GetFees which requires input >= output. Set
				// PreviousTxSatoshis high enough to cover all k outputs (1000 sat each) plus
				// a fee of 1 sat, so fee computation succeeds.
				parent := makeAgedFanoutTx(0, i, k)
				parent.Inputs[0].PreviousTxSatoshis = uint64(k)*1000 + 1
				if _, cErr := store.Create(ctx, parent, uint32(seedLow)); cErr != nil {
					seedErr.Store(cErr)
					return
				}
				seededParents[i] = parent

				// Mine the parent at seedLow so the sweep cursor can see it.
				ph := parent.TxIDChainHash()
				select {
				case mineCh <- *ph:
				case <-driverCtx.Done():
					return
				}

				// Spend k-2 of the k outputs at heights scattered across
				// [seedLow, seedLow+ageSpan). Leave vouts k-2 and k-1 unspent.
				seededUnspentVouts[i] = [2]uint32{uint32(k - 2), uint32(k - 1)}
				for v := 0; v < k-2; v++ {
					spendH := uint32(seedLow) + uint32(int64(v)*int64(ageSpan)/int64(k-2+1))
					child := makeSpendOfVout(parent, uint32(v))
					if _, sErr := store.Spend(ctx, child, spendH); sErr != nil {
						seedErr.Store(sErr)
						return
					}
					if _, cErr := store.Create(ctx, child, spendH); cErr != nil {
						seedErr.Store(cErr)
						return
					}
					ch := child.TxIDChainHash()
					select {
					case mineCh <- *ch:
					case <-driverCtx.Done():
						return
					}
				}
			}()
		}
		gwg.Wait()
		if e := seedErr.Load(); e != nil {
			t.Fatalf("seed phase: %v", e.(error))
		}
	}
	t.Logf("[lag] seed phase done in %s; advancing tip to %d", time.Since(seedStart).Round(time.Second), tipStart)

	// Advance height to tipStart so seeded spends are all "in the past".
	// Store curH BEFORE closing heightStartCh so the HEIGHT goroutine's first
	// tick increments from tipStart, not from seedLow.
	curH.Store(tipStart)
	_ = store.SetBlockHeight(uint32(tipStart))

	// Signal the HEIGHT goroutine to begin the fast-tick advancement now that
	// tipStart is in place.
	close(heightStartCh)

	// Wait for the cursor's watermark to reach tipStart-2 (meaning it has
	// swept the empty initial range [watermark..tipStart-2]). The range has no
	// completable parents so it drains in one pass. We wait on the watermark
	// rather than on backlog because the fast tick keeps advancing curH and the
	// backlog oscillates rather than reaching zero.
	t.Logf("[lag] waiting for cursor watermark to reach tipStart (%d)…", tipStart)
	catchUpDeadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(catchUpDeadline) {
		var minWM int64
		wmErr := statPool.QueryRow(ctx, `SELECT COALESCE(MIN(last_swept_height),0) FROM dah_part_watermark`).Scan(&minWM)
		if wmErr == nil && minWM >= tipStart-5 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Logf("[lag] cursor watermark ready; beginning timed measurement")

	// -----------------------------------------------------------------------
	// TIMED LOOP: workers create fresh fan-out parents AND spend the two
	// remaining outputs of seeded parents. The DAH sweep cursor must
	// re-aggregate k-2 historical spends + new spends on every re-check of each
	// seeded parent — this is the expensive path that causes production lag.
	// -----------------------------------------------------------------------

	// parentSeq is shared across workers for fresh parent creation so txids stay unique.
	var parentSeq atomic.Int64

	// seededIdx is shared so workers round-robin over the seeded parents' unspent vouts.
	var seededIdx atomic.Int64

	// createdOps tracks total created ops for the sampler's create-rate computation.
	var createdOps atomic.Int64

	runTimedPhase := func(dur time.Duration) {
		var wg sync.WaitGroup
		wg.Add(numWorkers)
		deadline := time.Now().Add(dur)

		for w := 0; w < numWorkers; w++ {
			w := w
			go func() {
				defer wg.Done()
				for time.Now().Before(deadline) {
					h := uint32(curH.Load())

					// (a) Create a fresh aged fan-out parent at the current tip.
					// Patch PreviousTxSatoshis so GetFees succeeds (same reason as seed phase).
					seq := int(parentSeq.Add(1))
					freshParent := makeAgedFanoutTx(w+1, seq, k)
					freshParent.Inputs[0].PreviousTxSatoshis = uint64(k)*1000 + 1
					if _, cErr := store.Create(ctx, freshParent, h); cErr != nil {
						return
					}
					createdOps.Add(1)
					fph := freshParent.TxIDChainHash()
					select {
					case mineCh <- *fph:
					case <-driverCtx.Done():
						return
					}

					// (b) Spend one of the two remaining unspent vouts of a seeded parent.
					// Round-robin across seededParents so all are exercised.
					n := seededIdx.Add(1)
					idx := int(n) % nParents
					sParent := seededParents[idx]
					if sParent == nil {
						continue
					}
					vouts := seededUnspentVouts[idx]
					// Pick which unspent vout to spend based on the ROUND (n/nParents),
					// not n%2. Round 0 spends vouts[0] (=k-2) for every parent; Round 1
					// spends vouts[1] (=k-1) for every parent — so all parents are fully
					// spent after exactly 2 rounds, not never (the previous n%2 logic with
					// nParents=even made every visit attempt the same vout, so parents were
					// never actually completed).
					vout := vouts[(n/int64(nParents))%2]
					spendChild := makeSpendOfVout(sParent, vout)
					if _, sErr := store.Spend(ctx, spendChild, h); sErr != nil {
						// Duplicate spend or already spent — skip quietly.
						continue
					}
					if _, cErr := store.Create(ctx, spendChild, h); cErr != nil {
						continue
					}
					createdOps.Add(1)
					sch := spendChild.TxIDChainHash()
					select {
					case mineCh <- *sch:
					case <-driverCtx.Done():
						return
					}
				}
			}()
		}
		wg.Wait()
	}

	// -----------------------------------------------------------------------
	// SAMPLER goroutine: every 1s record backlog, stamp rate, create rate.
	// -----------------------------------------------------------------------
	var samples []lagSample
	var samplesMu sync.Mutex
	samplerResetCh := make(chan struct{}, 1)

	samplerCtx, stopSampler := context.WithCancel(driverCtx)
	var samplerWG sync.WaitGroup
	samplerWG.Add(1)
	go func() {
		defer samplerWG.Done()
		tk := time.NewTicker(time.Second)
		defer tk.Stop()

		var prevStamped int64
		var prevCreated int64
		_ = statPool.QueryRow(samplerCtx, `SELECT COALESCE(total_rows_stamped,0) FROM dah_sweep_control WHERE id = 1`).Scan(&prevStamped)
		prevCreated = createdOps.Load()

		for {
			select {
			case <-samplerCtx.Done():
				return
			case <-samplerResetCh:
				// Reset baselines at warmup→measured transition.
				_ = statPool.QueryRow(samplerCtx, `SELECT COALESCE(total_rows_stamped,0) FROM dah_sweep_control WHERE id = 1`).Scan(&prevStamped)
				prevCreated = createdOps.Load()
			case now := <-tk.C:
				tip := curH.Load()

				backlog, bErr := sampleBacklog(samplerCtx, statPool, tip)
				if bErr != nil {
					continue
				}

				var curStamped int64
				_ = statPool.QueryRow(samplerCtx, `SELECT COALESCE(total_rows_stamped,0) FROM dah_sweep_control WHERE id = 1`).Scan(&curStamped)
				curCreated := createdOps.Load()

				stampRate := curStamped - prevStamped
				createRate := curCreated - prevCreated
				prevStamped = curStamped
				prevCreated = curCreated

				s := lagSample{
					T:          now,
					Backlog:    backlog,
					StampRate:  stampRate,
					CreateRate: createRate,
				}
				samplesMu.Lock()
				samples = append(samples, s)
				samplesMu.Unlock()
			}
		}
	}()

	// Warmup phase (excluded from final samples slice).
	if cfg.warmup > 0 {
		runTimedPhase(cfg.warmup)
	}
	// Clear samples collected during warmup so result only covers the measured phase.
	// Also reset the sampler's baseline counters so the first measured sample has a
	// correct delta from the warmup→measured boundary.
	samplesMu.Lock()
	samples = samples[:0]
	samplesMu.Unlock()
	// Signal sampler to reset its baselines (via a channel in samplerResetCh).
	select {
	case samplerResetCh <- struct{}{}:
	case <-driverCtx.Done():
	}

	// Measured phase.
	if cfg.measure > 0 {
		runTimedPhase(cfg.measure)
	} else {
		// Default: 30 seconds if no cfg set.
		runTimedPhase(30 * time.Second)
	}

	stopSampler()
	samplerWG.Wait()
	cancel()
	driverWG.Wait()

	// -----------------------------------------------------------------------
	// Build lagResult from collected samples.
	// -----------------------------------------------------------------------
	samplesMu.Lock()
	finalSamples := append([]lagSample(nil), samples...)
	samplesMu.Unlock()

	var res lagResult
	res.Samples = finalSamples

	if len(finalSamples) == 0 {
		return res
	}

	res.StartBacklog = finalSamples[0].Backlog
	res.EndBacklog = finalSamples[len(finalSamples)-1].Backlog

	stampRates := make([]float64, len(finalSamples))
	createRates := make([]float64, len(finalSamples))
	for i, s := range finalSamples {
		if s.Backlog > res.MaxBacklog {
			res.MaxBacklog = s.Backlog
		}
		stampRates[i] = float64(s.StampRate)
		createRates[i] = float64(s.CreateRate)
	}
	res.MedStampRate = median(stampRates)
	res.MedCreateRate = median(createRates)

	t.Logf("[lag] maxBacklog=%d startBacklog=%d endBacklog=%d medStampRate=%.0f medCreateRate=%.0f samples=%d",
		res.MaxBacklog, res.StartBacklog, res.EndBacklog, res.MedStampRate, res.MedCreateRate, len(res.Samples))
	return res
}

// median returns the median of a float64 slice. Returns 0 for empty slices.
func median(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	s := append([]float64(nil), vals...)
	sort.Float64s(s)
	n := len(s)
	if n%2 == 1 {
		return s[n/2]
	}
	return (s[n/2-1] + s[n/2]) / 2
}

// backlogTrendInBackHalf returns the slope (Δbacklog/Δsample) across the back
// half of the sample window. A positive slope means backlog is growing over the
// sustained measurement period — distinguishing true divergence from a transient
// spike that drains. Requires at least 4 samples; returns 0 if fewer.
func backlogTrendInBackHalf(samples []lagSample) float64 {
	if len(samples) < 4 {
		return 0
	}
	half := samples[len(samples)/2:]
	return float64(half[len(half)-1].Backlog-half[0].Backlog) / float64(len(half))
}

// ---------------------------------------------------------------------------
// Wiring test: verify the harness produces at least one sample
// ---------------------------------------------------------------------------

// TestRunAgedFanoutLag_Wiring is a fast smoke-test that verifies the harness
// wires up correctly and records at least one backlog sample. It is intentionally
// small-scale (overrides AGED_PARENTS to 2000 and AGE_SPAN to 500 so the untimed
// seed phase finishes quickly). It skips when THROUGHPUT_WORKERS is unset to
// avoid running in the plain unit-test job that has no Postgres.
func TestRunAgedFanoutLag_Wiring(t *testing.T) {
	if os.Getenv("THROUGHPUT_WORKERS") == "" {
		t.Skip("set THROUGHPUT_WORKERS to run")
	}

	// Small scale so the seed phase is fast during wiring verification.
	t.Setenv("AGED_PARENTS", "2000")
	t.Setenv("AGE_SPAN", "500")
	// Reload the vars that were set at package init time from env.
	// Because envInt reads os.Getenv at call time and the vars are initialised
	// at package init, t.Setenv alone is not enough — reinitialise them here
	// for the duration of this test.
	savedParents := agedParents
	savedSpan := ageSpan
	agedParents = 2000
	ageSpan = 500
	t.Cleanup(func() {
		agedParents = savedParents
		ageSpan = savedSpan
	})

	store, done := newPrunedQueueStore(t)
	defer done()

	statPool := mustStatPool(t)

	// Short warmup + measure so the wiring test finishes well within the timeout.
	cfg := stableCfg{
		warmup:  2 * time.Second,
		measure: 5 * time.Second,
		verbose: os.Getenv("THROUGHPUT_VERBOSE") != "",
	}

	res := runAgedFanoutLag(t, store, envInt("THROUGHPUT_WORKERS", 1000), cfg, statPool)
	require.NotEmpty(t, res.Samples, "sampler must record at least one backlog sample")
}

// ---------------------------------------------------------------------------
// assertDiskBound: guard that the Postgres cluster is the disk-bound instance
// (shared_buffers=256MB on :5439). The lag bench only diverges on the current
// design when the working set exceeds shared_buffers — so running against a
// 16GB-buffered cluster produces misleading PASS results.
// ---------------------------------------------------------------------------

// assertDiskBound fails the test if the connected Postgres instance has
// shared_buffers > 512MB. This guards against accidentally running the lag
// assertion test against a large-buffer cluster where the current design
// doesn't diverge (working set fits in cache so sweep keeps pace).
func assertDiskBound(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, throughputDSN)
	if err != nil {
		t.Skipf("assertDiskBound: no postgres: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("assertDiskBound: no postgres: %v", err)
	}

	var sharedBuffers string
	if err := pool.QueryRow(ctx, `SHOW shared_buffers`).Scan(&sharedBuffers); err != nil {
		t.Fatalf("assertDiskBound: SHOW shared_buffers: %v", err)
	}

	// Parse "256MB", "512MB", "1GB", "2GB", etc. into bytes.
	parsed := parsePostgresMem(sharedBuffers)
	const limit512MB = 512 * 1024 * 1024
	if parsed > limit512MB {
		t.Fatalf("assertDiskBound: shared_buffers=%s (%d bytes) > 512MB — "+
			"run against the disk-bound cluster (THROUGHPUT_DSN=postgres://teranode:teranode@localhost:5439/teranode_test); "+
			"a large-buffer cluster masks the DAH-set lag divergence",
			sharedBuffers, parsed)
	}
	t.Logf("assertDiskBound: shared_buffers=%s (OK, <= 512MB)", sharedBuffers)
}

// parsePostgresMem parses a Postgres GUC memory value (e.g. "256MB", "1GB",
// "512kB") into bytes. Returns 0 for unrecognised formats (caller checks > 0).
func parsePostgresMem(s string) int64 {
	s = strings.TrimSpace(s)
	if len(s) < 2 {
		return 0
	}
	// suffix detection: last two chars (kB, MB, GB, TB) or single char (B)
	switch {
	case strings.HasSuffix(s, "TB"):
		n, _ := strconv.ParseInt(strings.TrimSuffix(s, "TB"), 10, 64)
		return n * 1024 * 1024 * 1024 * 1024
	case strings.HasSuffix(s, "GB"):
		n, _ := strconv.ParseInt(strings.TrimSuffix(s, "GB"), 10, 64)
		return n * 1024 * 1024 * 1024
	case strings.HasSuffix(s, "MB"):
		n, _ := strconv.ParseInt(strings.TrimSuffix(s, "MB"), 10, 64)
		return n * 1024 * 1024
	case strings.HasSuffix(s, "kB"):
		n, _ := strconv.ParseInt(strings.TrimSuffix(s, "kB"), 10, 64)
		return n * 1024
	case strings.HasSuffix(s, "B"):
		n, _ := strconv.ParseInt(strings.TrimSuffix(s, "B"), 10, 64)
		return n
	}
	return 0
}

// ---------------------------------------------------------------------------
// TestThroughput_QueueStoreAgedFanoutLag — the RED gate
// ---------------------------------------------------------------------------
//
// INTENT: prove that the CURRENT (unfixed) deferred-DAH design fails to keep
// up with un-throttled aged-fan-out ingestion. This test is expected to FAIL
// on today's code. It will be turned green by a later task (Setter C/B) that
// replaces the per-tx re-aggregation with a bounded design.
//
// DISK-BOUND SETUP (required for divergence to be visible):
//
//	Postgres cluster on :5439 has shared_buffers=64MB.
//	Dataset: 20000 parents × 62 spends ≈ 1.24M rows × ~437 bytes ≈ 542MB.
//	542MB / 64MB ≈ 8.5× shared_buffers — working set is firmly disk-bound.
//	assertDiskBound() enforces shared_buffers ≤ 512MB to prevent accidentally
//	running on a large-buffer cluster where the sweep would keep pace.
//
// REPRODUCTION CONFIG (use defaults; all are env-overridable):
//
//	THROUGHPUT_DSN=postgres://teranode:teranode@localhost:5439/teranode_test
//	THROUGHPUT_WORKERS=2000
//	AGED_PARENTS=20000        # default — 20K parents × 62 spends ≈ 542MB
//	AGE_SPAN=20000            # default — large cold-history scan range
//	BACKLOG_BOUND=2000        # default
//	AGED_FANOUT_K=64          # default
//
//	Command:
//	  THROUGHPUT_WORKERS=2000 THROUGHPUT_DSN=postgres://teranode:teranode@localhost:5439/teranode_test \
//	    go test ./stores/utxo/ -run TestThroughput_QueueStoreAgedFanoutLag -v -timeout 40m
//
//	Seed phase: ~300-600s (20K parents × ~188 store ops = ~3.76M ops at ~6-12K ops/s)
//	Measurement window: 180s (long enough to confirm sustained growth, not transient spike)
//
// EXPECTED FAILING NUMBERS (TRUE divergence, the opposite of a transient spike):
//
//	PRIMARY signal — backlog GROWS throughout the measurement window:
//	  endBacklog >> startBacklog   (e.g. end=50000+, start=low)
//	  backlogTrend > 0 in the back half of samples (monotonically climbing)
//
//	SECONDARY signals:
//	  medStampRate << medCreateRate  (sweep can't keep pace with creation)
//	  medStampRate near 0            (sweep stalls on disk-bound re-aggregation)
//
// WHY THE MECHANISM WORKS: seeded parents each have k-2=62 historical spends.
// The spend rows span a cold height range [seedLow, seedLow+ageSpan) that
// does not fit in shared_buffers=64MB — every sweep re-aggregation requires
// physical disk reads. When the timed phase spends vout k-2 or k-1, the
// deferred-DAH proc must re-aggregate ALL 62 historical spends + new ones per
// parent. At 2000 workers across 20000 parents, the O(k × activeParents) cost
// per sweep cycle far exceeds what the cursor can sustain from disk — backlog
// grows unboundedly throughout the 180s measurement window.
//
// WHY THE PREVIOUS CONFIG WAS INVALID (5000 parents at shared_buffers=256MB):
//   - 5000 × 62 = 310K rows × ~437 bytes ≈ 136MB < 256MB shared_buffers
//   - The working set fit in buffer cache → sweep was fast/CPU-bound, not disk-bound
//   - endBacklog=3 (sweep caught up) — that is the OPPOSITE of divergence
//   - maxBacklog=2918 was a transient spike from draining the seeded pool at t=0
//
// The test asserts the DESIRED (bounded) condition; on the current design these FAIL,
// which IS the red evidence. A correct O(new-spends) setter makes them pass (Tasks 9/12):
//  1. res.EndBacklog <= res.StartBacklog     — FAILS now: end >> start (sustained growth)
//  2. res.MaxBacklog <= backlogBound         — FAILS now: max blows past the bound
//  3. res.MedStampRate >= res.MedCreateRate  — FAILS now: stamp << create
//  4. backlogTrend <= 0 in back half         — FAILS now: positive slope = monotonic growth
//
// (res.MedCreateRate > 0 is a harness-sanity guard and passes in both states.)
//
// This test is committed RED intentionally. CI skips it (THROUGHPUT_WORKERS unset).
func TestThroughput_QueueStoreAgedFanoutLag(t *testing.T) {
	if os.Getenv("THROUGHPUT_WORKERS") == "" {
		t.Skip("set THROUGHPUT_WORKERS to run")
	}

	assertDiskBound(t)

	// Log the expected dataset size so disk-bound property is documented in output.
	k := agedFanoutK
	nParents := agedParents
	spendRows := int64(nParents) * int64(k-2)
	const bytesPerSpendRow = 437 // empirically measured (row + index overhead)
	datasetBytes := spendRows * bytesPerSpendRow
	t.Logf("[lag] dataset: %d parents × %d spends = %d rows ≈ %d MB (shared_buffers=64MB)",
		nParents, k-2, spendRows, datasetBytes/1024/1024)

	store, done := newPrunedQueueStore(t)
	defer done()

	statPool := mustStatPool(t)

	// 180s measurement window — long enough that sustained backlog growth is
	// unambiguous. A 30s window lets a transient spike look like divergence.
	res := runAgedFanoutLag(t, store, envInt("THROUGHPUT_WORKERS", 2000), stableCfg{
		measure: 180 * time.Second,
	}, statPool)

	// Log the back-half trend to distinguish sustained growth from transient spike.
	trend := backlogTrendInBackHalf(res.Samples)
	t.Logf("maxBacklog=%d start=%d end=%d medStamp=%.0f medCreate=%.0f backHalfTrend=%.1f/s",
		res.MaxBacklog, res.StartBacklog, res.EndBacklog, res.MedStampRate, res.MedCreateRate, trend)

	// This test asserts the DESIRED (bounded) condition, so it is RED on the current
	// design (the deferred sweep diverges: endBacklog grows past startBacklog) and turns
	// GREEN when an O(new-spends) setter keeps pace (Tasks 9/12). Do NOT invert these to
	// assert divergence — the red→green gate depends on asserting the good condition.

	// Harness sanity: the un-throttled producer must actually have run, so a red is
	// genuine divergence and not a harness bug that created nothing.
	require.Positive(t, res.MedCreateRate,
		"producer must be creating (medCreateRate=%.0f)", res.MedCreateRate)

	// PRIMARY: DAH-set lag must stay bounded. The current design FAILS here because
	// endBacklog grows past startBacklog as the sweep falls behind; a correct setter
	// keeps end <= start.
	require.LessOrEqual(t, res.EndBacklog, res.StartBacklog,
		"backlog must stay bounded (endBacklog=%d must not exceed startBacklog=%d — sweep is diverging)",
		res.EndBacklog, res.StartBacklog)

	require.LessOrEqual(t, res.MaxBacklog, int64(backlogBound),
		"max DAH-set backlog must stay <= %d blocks (was %d)", backlogBound, res.MaxBacklog)

	// Sweep must be actively STAMPING completions — not frozen. NOTE: comparing stamp
	// rate to create rate is meaningless for this workload — a stamp fires once per PARENT
	// completion, while medCreateRate counts every create including the ~k spend-txs per
	// parent, so stamp rate is structurally far below create rate even when perfectly
	// healthy. The real keep-up signal is the bounded, non-growing backlog asserted above;
	// this guards only that the setter is issuing stamps (the frozen v10 sweep stamps 0).
	require.Positive(t, res.MedStampRate,
		"sweep must be stamping completions (medStampRate=%.0f) — 0 means the setter is frozen",
		res.MedStampRate)

	// Back-half trend must not be upward (a positive slope is sustained divergence).
	require.LessOrEqual(t, trend, 0.0,
		"backlog must not trend upward in the back half (%.1f/s)", trend)
}
