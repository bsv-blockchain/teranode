package subtreeprocessor

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

// Issue 2269, plan PR 6: with blockassembly_reorgFastPath on, a fork reorg that passes every gate is
// applied to the pending set in place, and one that fails a gate is declined before anything changes
// and runs on the slow path. These tests run the two paths on the same scenario and compare what they
// leave behind (the order, the inpoints, both maps, the marks); the order oracle runs on every
// successful result.

// fastPathCounts is the fast path's counters at a moment.
type fastPathCounts struct {
	applied  float64
	declined map[string]float64
}

func readFastPathCounts() fastPathCounts {
	initPrometheusMetrics()

	counts := fastPathCounts{declined: make(map[string]float64)}

	ch := make(chan prometheus.Metric, 64)

	go func() {
		prometheusSubtreeProcessorReorgFastPathDeclined.Collect(ch)
		close(ch)
	}()

	for metric := range ch {
		m := &dto.Metric{}
		if err := metric.Write(m); err != nil {
			continue
		}

		for _, l := range m.GetLabel() {
			if l.GetName() == "reason" {
				counts.declined[l.GetValue()] = m.GetCounter().GetValue()
			}
		}
	}

	m := &dto.Metric{}
	if err := prometheusSubtreeProcessorReorgFastPathApplied.Write(m); err == nil {
		counts.applied = m.GetCounter().GetValue()
	}

	return counts
}

// declinedSince lists the reasons whose counter grew since before, one entry per decline.
func (c fastPathCounts) declinedSince(before fastPathCounts) []string {
	var reasons []string

	for reason, n := range c.declined {
		for range int(n - before.declined[reason]) {
			reasons = append(reasons, reason)
		}
	}

	slices.Sort(reasons)

	return reasons
}

// fastPathRun is what the fast path did on a scenario run through fastReorgPath.
type fastPathRun struct {
	applied bool
	reasons []string
}

// fastReorgPath is the fast path: the scenario's processor with the setting on, run through reorgBlocks.
func fastReorgPath(t testing.TB, sc *reorgScenario) error {
	t.Helper()

	sc.stp.settings.BlockAssembly.ReorgFastPath = true

	return sc.run(t)
}

// recordingFastPath is fastReorgPath that notes into rec whether the fast path applied or declined.
func recordingFastPath(rec *fastPathRun) reorgPath {
	return func(t testing.TB, sc *reorgScenario) error {
		before := readFastPathCounts()
		err := fastReorgPath(t, sc)
		after := readFastPathCounts()

		rec.applied = after.applied > before.applied
		rec.reasons = after.declinedSince(before)

		for _, reason := range rec.reasons {
			t.Logf("fast path declined: %s", reason)
		}

		return err
	}
}

// fastPathFirstBeat is the number of the progress beat, counted from the start of the reorg, that the
// fast path gives right after it removed the lost block's coinbase. The beats of the fast path follow it:
// +1 after effective D1 is in the map, +2 after the build, +3 before the first coinbase of a winning
// block is applied. Earlier beats belong to the reads before it.
func fastPathFirstBeat(t *testing.T, cfg reorgScenarioConfig) int {
	t.Helper()

	sc := newReorgScenario(t, cfg)
	sc.stp.settings.BlockAssembly.ReorgFastPath = true

	beats, first := 0, 0

	sc.stp.SetProgressHook(func() {
		if beats++; first == 0 && sc.stp.reorgPendingD1 != nil {
			first = beats
		}
	})

	require.NoError(t, sc.run(t))
	require.NotZero(t, first, "the fast path applied")

	return first
}

// requireFastPathMatchesSlow requires the fast path to apply on cfg and to leave what the slow path leaves.
func requireFastPathMatchesSlow(t *testing.T, cfg reorgScenarioConfig, divergences ...reorgDivergence) {
	t.Helper()

	var rec fastPathRun

	requireReorgEquivalent(t, cfg, slowReorgPath, recordingFastPath(&rec), divergences...)
	require.True(t, rec.applied, "the fast path must apply, it declined: %v", rec.reasons)
	require.Empty(t, rec.reasons)
}

// requireFastPathDeclines requires the fast path to decline cfg for exactly reason, and the reorg that
// follows on the slow path to leave what a run without the fast path leaves.
func requireFastPathDeclines(t *testing.T, cfg reorgScenarioConfig, reason string) {
	t.Helper()

	var rec fastPathRun

	requireReorgEquivalent(t, cfg, slowReorgPath, recordingFastPath(&rec))
	require.False(t, rec.applied, "the fast path must decline")
	require.Equal(t, []string{reason}, rec.reasons)
}

// requireDeclinedLikeSlow runs the fast path and the slow path on scenarios that setup has changed, and
// requires the fast path to decline for reason and the two to end in the same error, or both in success.
// A scenario setup has made inconsistent can make the slow path fail; the fast path must not hide that.
func requireDeclinedLikeSlow(t *testing.T, cfg reorgScenarioConfig, setup func(sc *reorgScenario), reason string) {
	t.Helper()

	slow := newReorgScenario(t, cfg)
	setup(slow)
	slowErr := slow.run(t)

	fast := newReorgScenario(t, cfg)
	setup(fast)

	var rec fastPathRun

	fastErr := recordingFastPath(&rec)(t, fast)

	require.Equal(t, []string{reason}, rec.reasons)
	require.False(t, fast.stp.reorgPendingD1 != nil, "a decline records nothing for the reset")
	require.Equal(t, slow.stp.failedForkReorgNeedsReset.Load(), fast.stp.failedForkReorgNeedsReset.Load())

	if slowErr == nil {
		require.NoError(t, fastErr)
		require.Equal(t, txHashes(assemblyNodes(slow.stp)), txHashes(assemblyNodes(fast.stp)))

		return
	}

	require.Error(t, fastErr)
	require.Equal(t, slowErr.Error(), fastErr.Error())
}

// A decline falls through to the move-back loop: the reset-flag defer of reorgBlocks is armed before
// the hook, so a decline that returned would mark the processor as needing a reset. Here the removeMap
// holds a lost-block transaction no winner has, which the slow path drops and the fast path cannot
// reproduce.
func TestReorgScenario_FastPathDeclineFallsThrough(t *testing.T) {
	cfg := smallScenarioConfig()
	cfg.removeMapD1 = 5

	var rec fastPathRun

	sc := newReorgScenario(t, cfg)
	require.NoError(t, recordingFastPath(&rec)(t, sc))

	require.False(t, rec.applied)
	require.Equal(t, []string{"remove_map_overlap"}, rec.reasons)
	require.False(t, sc.stp.failedForkReorgNeedsReset.Load(), "a decline is not a failure")
	require.Empty(t, sc.stp.reorgPendingD1, "a decline records nothing for the reset")
	requireAssemblyValid(t, sc.stp, sc.onChainAfter)
	require.Equal(t, sc.expectedOrderAfter(), txHashes(assemblyNodes(sc.stp)))

	requireFastPathDeclines(t, cfg, "remove_map_overlap")
}

// With the setting off the fast path is never consulted: no counter moves.
func TestReorgScenario_FastPathOffDoesNotRun(t *testing.T) {
	before := readFastPathCounts()

	sc := newReorgScenario(t, smallScenarioConfig())
	require.NoError(t, sc.run(t))

	after := readFastPathCounts()
	require.Equal(t, before.applied, after.applied)
	require.Empty(t, after.declinedSince(before))
}

// fastPathMatrix is the reorgs the fast path must apply and leave exactly as the slow path does.
func fastPathMatrix() map[string]func(cfg *reorgScenarioConfig) {
	return map[string]func(cfg *reorgScenarioConfig){
		"two forward blocks":                   func(cfg *reorgScenarioConfig) {},
		"one forward block":                    func(cfg *reorgScenarioConfig) { cfg.forwardBlocks = 1 },
		"three forward blocks":                 func(cfg *reorgScenarioConfig) { cfg.forwardBlocks = 3 },
		"hits at the front":                    func(cfg *reorgScenarioConfig) { cfg.hitsAtFront = true },
		"hits at the front, three blocks":      func(cfg *reorgScenarioConfig) { cfg.hitsAtFront, cfg.forwardBlocks = true, 3 },
		"no hits":                              func(cfg *reorgScenarioConfig) { cfg.pendingInWinners = 0 },
		"hits just below survivors":            func(cfg *reorgScenarioConfig) { cfg.pendingInWinners = 0.45 },
		"lost block not in the winners at all": func(cfg *reorgScenarioConfig) { cfg.lostInWinners = 0 },
		"half of the lost block in winners":    func(cfg *reorgScenarioConfig) { cfg.lostInWinners = 0.5 },
		"all of the lost block in winners": func(cfg *reorgScenarioConfig) {
			cfg.lostInWinners, cfg.lostAlsoPending, cfg.pendingSpendsLost = 1, 0, 0
		},
		"no lost transaction is also pending": func(cfg *reorgScenarioConfig) { cfg.lostAlsoPending, cfg.pendingSpendsLost = 0, 0 },
		"many lost transactions also pending": func(cfg *reorgScenarioConfig) { cfg.lostAlsoPending = 60 },
		"no winner-only transaction":          func(cfg *reorgScenarioConfig) { cfg.winnerOnly = 0 },
		"removeMap holds pending transactions": func(cfg *reorgScenarioConfig) {
			cfg.removeMapPending = 20
		},
		"removeMap pending, hits at the front": func(cfg *reorgScenarioConfig) {
			cfg.removeMapPending, cfg.hitsAtFront = 20, true
		},
		"parallel remainder threshold of one": func(cfg *reorgScenarioConfig) { cfg.parallelThreshold = 1 },
		"pending set in the open subtree":     func(cfg *reorgScenarioConfig) { cfg.subtreeSize = 4096 },
		"large subtrees":                      func(cfg *reorgScenarioConfig) { cfg.subtreeSize = 512 },
		"pending chains only":                 func(cfg *reorgScenarioConfig) { cfg.pendingChains, cfg.pendingSpendsLost = 200, 0 },
	}
}

func TestReorgDifferential_FastPathMatchesSlowPath(t *testing.T) {
	for name, tweak := range fastPathMatrix() {
		for _, disk := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/disk=%v", name, disk), func(t *testing.T) {
				cfg := smallScenarioConfig()
				cfg.diskTxMap = disk
				tweak(&cfg)

				requireFastPathMatchesSlow(t, cfg)
			})
		}
	}
}

// Random forks over every knob the fast path accepts: overlap 0 to 100 percent, one to three winning
// blocks, scattered or front-clustered hits.
func TestReorgDifferential_FastPathMatchesSlowPathOnRandomForks(t *testing.T) {
	for seed := range uint64(24) {
		rng := rand.New(rand.NewPCG(seed, 0x6))

		cfg := smallScenarioConfig()
		cfg.seed = seed + 100
		cfg.pending = 500 + rng.IntN(2500)
		cfg.lost = 200 + rng.IntN(1500)
		cfg.lostInWinners = float64(rng.IntN(5)) / 4
		cfg.pendingInWinners = float64(rng.IntN(46)) / 100
		cfg.hitsAtFront = rng.IntN(2) == 0
		cfg.winnerOnly = rng.IntN(100)
		cfg.forwardBlocks = 1 + rng.IntN(3)
		cfg.subtreeSize = 32 << rng.IntN(4)
		cfg.diskTxMap = rng.IntN(2) == 0
		cfg.parallelThreshold = []int{0, 1}[rng.IntN(2)]

		rawD1 := cfg.lost - int(math.Round(float64(cfg.lost)*cfg.lostInWinners))
		cfg.lostAlsoPending = min(rng.IntN(20), rawD1/3)
		cfg.pendingSpendsLost = min(rng.IntN(20), rawD1/3)
		cfg.pendingChains = rng.IntN(60)

		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			requireFastPathMatchesSlow(t, cfg)
		})
	}
}

// fastPathDeclineCases is every gate the fast path checks before it changes anything, with the
// config that trips it and the reason the counter reports.
func fastPathDeclineCases() map[string]struct {
	reason string
	tweak  func(cfg *reorgScenarioConfig)
} {
	return map[string]struct {
		reason string
		tweak  func(cfg *reorgScenarioConfig)
	}{
		"removeMap holds a hit":                           {"remove_map_overlap", func(cfg *reorgScenarioConfig) { cfg.removeMapHits = 10 }},
		"removeMap holds a lost transaction also pending": {"remove_map_overlap", func(cfg *reorgScenarioConfig) { cfg.removeMapLost = 3 }},
		"removeMap holds raw D1":                          {"remove_map_overlap", func(cfg *reorgScenarioConfig) { cfg.removeMapD1 = 5 }},
		"pending children of the coinbase":                {"coinbase_children", func(cfg *reorgScenarioConfig) { cfg.coinbaseChildren = 3 }},
		"a duplicate pending hash":                        {"tx_map_length", func(cfg *reorgScenarioConfig) { cfg.duplicateInPending = 4 }},
		"empty first forward block":                       {"empty_forward_block", func(cfg *reorgScenarioConfig) { cfg.emptyForwardFirst = true }},
		"empty last forward block":                        {"empty_forward_block", func(cfg *reorgScenarioConfig) { cfg.emptyForwardLast = true }},
		"conflict in the lost block":                      {"conflicting_back", func(cfg *reorgScenarioConfig) { cfg.conflictsInLost = 2 }},
		"conflict in a winning block":                     {"conflicting_forward", func(cfg *reorgScenarioConfig) { cfg.conflictsInWinner = 2 }},
		"lost child of a pending parent":                  {"d1_spends_pending", func(cfg *reorgScenarioConfig) { cfg.lostChildOfPending = 3 }},
		"chain of lost children of a pending parent":      {"d1_spends_pending", func(cfg *reorgScenarioConfig) { cfg.lostChildOfPending, cfg.lostChildChain = 3, true }},
		"the winners mine most of the pending set":        {"hits_exceed_survivors", func(cfg *reorgScenarioConfig) { cfg.pendingInWinners = 0.9 }},
		"the winners mine half the pending set":           {"hits_exceed_survivors", func(cfg *reorgScenarioConfig) { cfg.pendingInWinners = 0.55 }},
	}
}

func TestReorgDifferential_FastPathDeclinesWhatItCannotProve(t *testing.T) {
	for name, c := range fastPathDeclineCases() {
		for _, disk := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/disk=%v", name, disk), func(t *testing.T) {
				cfg := smallScenarioConfig()
				cfg.diskTxMap = disk
				c.tweak(&cfg)

				requireFastPathDeclines(t, cfg, c.reason)
			})
		}
	}
}

// A reorg with an invalid lost block is followed by a reset in block assembly, so the fast path
// leaves it to the slow path.
func TestReorgScenario_FastPathDeclinesAnInvalidLostBlock(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	_, err := sc.client.InvalidateBlock(t.Context(), sc.lost.Hash())
	require.NoError(t, err)

	var rec fastPathRun

	require.NoError(t, recordingFastPath(&rec)(t, sc))
	require.Equal(t, []string{"invalid_block"}, rec.reasons)
}

// A saved list of txs deleted with the lost block or a winning block is a sign of an interrupted
// earlier removal; the locked removal would merge it and change what the fast path flagged.
func TestReorgScenario_FastPathDeclinesASavedReorgDeletedTxsFile(t *testing.T) {
	for name, pick := range map[string]func(sc *reorgScenario) *model.Block{
		"lost block":    func(sc *reorgScenario) *model.Block { return sc.lost },
		"winning block": func(sc *reorgScenario) *model.Block { return sc.forward[1] },
	} {
		t.Run(name, func(t *testing.T) {
			requireDeclinedLikeSlow(t, smallScenarioConfig(), func(sc *reorgScenario) {
				saved := encodeReorgDeletedTxs([]*reorgDeletedTx{{tx: sc.rawD1Tx(t, 0), unmined: true}})
				require.NoError(t, sc.blobStore.Set(t.Context(), pick(sc).Hash()[:], fileformat.FileTypeReorgDeletedTxs, saved))
			}, "journal")
		})
	}
}

// A tx map with an entry no subtree has cannot be told from the real thing: the length check declines.
func TestReorgScenario_FastPathDeclinesATxMapLongerThanTheAssembly(t *testing.T) {
	requireDeclinedLikeSlow(t, smallScenarioConfig(), func(sc *reorgScenario) {
		orphan := voutZeroInpoints(chainhash.Hash{9})
		sc.stp.currentTxMap.Set(chainhash.Hash{0xaa}, &orphan)
	}, "tx_map_length")
}

// One orphan entry plus one missing entry leaves the count right. An orphan equal to a raw D1 hash
// would silently drop that transaction in the move-back's dedup, so the fast path checks that every raw
// D1 hash the map suppresses sits at a pending position.
func TestReorgScenario_FastPathDeclinesASuppressingMapEntryNoSubtreeHolds(t *testing.T) {
	requireDeclinedLikeSlow(t, smallScenarioConfig(), func(sc *reorgScenario) {
		var d1 chainhash.Hash

		for _, h := range sc.rawD1() {
			if !sc.inPending(h) {
				d1 = h

				break
			}
		}

		pending := sc.pendingOrder()
		require.True(t, sc.stp.currentTxMap.Delete(pending[len(pending)-1]), "one pending entry gone, so the lengths agree")

		orphan := voutZeroInpoints(chainhash.Hash{9})
		sc.stp.currentTxMap.Set(d1, &orphan)
	}, "suppressed_not_pending")
}

// The memory gate: live heap plus the set plus the pending set at 48 bytes each must stay within three
// quarters of GOMEMLIMIT, when one is set.
func TestReorgScenario_FastPathMemoryGate(t *testing.T) {
	defer func(prev func() (uint64, int64)) { reorgMemoryStats = prev }(reorgMemoryStats)

	t.Run("no limit set", func(t *testing.T) {
		reorgMemoryStats = func() (uint64, int64) { return 1 << 60, math.MaxInt64 }

		requireFastPathMatchesSlow(t, smallScenarioConfig())
	})

	t.Run("within the limit", func(t *testing.T) {
		reorgMemoryStats = func() (uint64, int64) { return 1 << 20, 1 << 30 }

		requireFastPathMatchesSlow(t, smallScenarioConfig())
	})

	t.Run("over three quarters of the limit", func(t *testing.T) {
		reorgMemoryStats = func() (uint64, int64) { return 800 << 20, 1 << 30 }

		requireFastPathDeclines(t, smallScenarioConfig(), "memory")
	})

	t.Run("the pending set tips it over", func(t *testing.T) {
		cfg := smallScenarioConfig()
		// the set of about 1550 hashes x 33 B fits in the 120 kB of headroom, 2000 pending x 48 B on top does not
		limit := int64(1 << 20)
		reorgMemoryStats = func() (uint64, int64) { return uint64(limit*3/4) - 120_000, limit }

		requireFastPathDeclines(t, cfg, "memory")
	})
}

// A fast path declines a reorg of more than one lost block: the coinbase interplay of several is not
// covered. It leaves everything as it found it.
func TestReorgScenario_FastPathDeclinesMoreThanOneBackBlock(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())
	sc.stp.settings.BlockAssembly.ReorgFastPath = true

	before := readFastPathCounts()
	res, err := sc.stp.reorgInPlace(t.Context(), []*model.Block{sc.lost, sc.lost}, sc.forward, [][]chainhash.Hash{nil, nil})

	require.NoError(t, err)
	require.Nil(t, res)
	require.Equal(t, []string{"back_blocks"}, readFastPathCounts().declinedSince(before))
	require.Equal(t, sc.pendingOrder(), txHashes(assemblyNodes(sc.stp)), "nothing changed")
}

// The union-filtered final drain (design section 5 item 6) is the one named divergence. The slow path
// drains after each forward step against that step's block only, so a transaction the first block
// mined, arriving between the two steps, is kept; the fast path drains once against both blocks and
// drops it. The arrival is injected at the same point of each path, and only those hashes are left out of
// the comparison.
func TestReorgDifferential_FastPathUnionFilteredFinalDrain(t *testing.T) {
	cfg := smallScenarioConfig()

	// A winner-only transaction of the first block, queued while the reorg runs.
	probe := newReorgScenario(t, cfg)
	firstBeat := fastPathFirstBeat(t, cfg)
	firstBlock := probe.winnerIDs[:len(probe.winnerIDs)/2]

	var arrivals []scenarioID

	for _, id := range firstBlock {
		if id.kind() == kindWinnerOnly && len(arrivals) < 5 {
			arrivals = append(arrivals, id)
		}
	}

	require.Len(t, arrivals, 5)

	inject := func(sc *reorgScenario) func() {
		return func() {
			nodes := make([]subtreepkg.Node, len(arrivals))
			inpoints := make([]*subtreepkg.TxInpoints, len(arrivals))

			for i, id := range arrivals {
				tx := sc.tx(t, id)
				nodes[i], inpoints[i] = tx.node, &tx.inpoints
			}

			sc.stp.AddBatch(nodes, inpoints)
		}
	}

	prepare := func(sc *reorgScenario) {
		enqueueAt := time.Now()
		sc.stp.queue.clock = fixedClock{t: enqueueAt}
		sc.stp.clock = fixedClock{t: enqueueAt.Add(time.Millisecond)}
	}

	slowWithArrival := func(t testing.TB, sc *reorgScenario) error {
		prepare(sc)
		fired := beforeForwardBlockOn(sc.stp, sc.forward[0].Header, inject(sc))
		err := sc.run(t)
		require.True(t, *fired)

		return err
	}

	fastWithArrival := func(t testing.TB, sc *reorgScenario) error {
		prepare(sc)

		beats, afterBuild := 0, firstBeat+2

		sc.stp.SetProgressHook(func() {
			if beats++; beats == afterBuild {
				inject(sc)()
			}
		})

		err := fastReorgPath(t, sc)
		require.GreaterOrEqual(t, beats, afterBuild)

		return err
	}

	hashes := make(map[chainhash.Hash]struct{})
	for _, id := range arrivals {
		hashes[probe.hash(id)] = struct{}{}
	}

	forgetArrivals := reorgDivergence{
		name: "union-filtered final drain",
		normalize: func(o *reorgOutcome) {
			kept := o.order[:0]

			for _, h := range o.order {
				if _, ok := hashes[h]; ok {
					o.txMapLen--
					delete(o.inpoints, h)

					continue
				}

				kept = append(kept, h)
			}

			o.order = kept
		},
	}

	// The slow path's own result holds a transaction the first block mined, so the order oracle would
	// reject it: that is the difference, and only the fast path's result is checked by it.
	outcome := func(path reorgPath, oracle bool) reorgOutcome {
		sc := newReorgScenario(t, cfg)
		err := path(t, sc)
		require.NoError(t, err)

		if oracle {
			requireAssemblyValid(t, sc.stp, sc.onChainAfter)
		}

		return captureReorgOutcome(sc, err)
	}

	slow, fast := outcome(slowWithArrival, false), outcome(fastWithArrival, true)

	require.NotEmpty(t, compareReorgOutcomes(slow, fast, nil),
		"without the divergence the two paths differ: the slow path keeps what the first block mined")
	require.Empty(t, compareReorgOutcomes(slow, fast, []reorgDivergence{forgetArrivals}))

	for h := range hashes {
		require.Contains(t, slow.order, h, "the slow path kept the arrival")
		require.NotContains(t, fast.order, h, "the fast path dropped it")
	}
}
