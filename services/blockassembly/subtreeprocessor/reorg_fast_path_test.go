package subtreeprocessor

import (
	"context"
	"fmt"
	"io"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
	bloboptions "github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
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

// slowReference is the slow path whatever blockassembly_reorgFastPath the environment sets.
func slowReference(t testing.TB, sc *reorgScenario) error {
	t.Helper()

	sc.stp.settings.BlockAssembly.ReorgFastPath = false

	return sc.run(t)
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

	requireReorgEquivalent(t, cfg, slowReference, recordingFastPath(&rec), divergences...)
	require.True(t, rec.applied, "the fast path must apply, it declined: %v", rec.reasons)
	require.Empty(t, rec.reasons)
}

// requireFastPathDeclines requires the fast path to decline cfg for exactly reason, and the reorg that
// follows on the slow path to leave what a run without the fast path leaves.
func requireFastPathDeclines(t *testing.T, cfg reorgScenarioConfig, reason string) {
	t.Helper()

	var rec fastPathRun

	requireReorgEquivalent(t, cfg, slowReference, recordingFastPath(&rec))
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
	slowErr := slowReference(t, slow)

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

	// Which subtree of a block fails first is not fixed, so only the outer layers of the error are compared.
	require.Error(t, fastErr)
	require.Equal(t, outerErrorLayers(slowErr), outerErrorLayers(fastErr))
}

// outerErrorLayers is the first two layers of err's chain, before the part that names a subtree or a hash.
func outerErrorLayers(err error) string {
	layers := strings.SplitN(err.Error(), " -> ", 3)

	return strings.Join(layers[:min(2, len(layers))], " -> ")
}

// A decline falls through to the move-back loop: a decline that returned would arm the reset flag of
// reorgBlocks. The removeMap holds a lost-block transaction no winner has, which only the slow path drops.
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
	require.NoError(t, slowReference(t, sc))

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

// differentialTxMaps is the tx maps the fast path's differential matrices run on: the in-memory map
// under `make test`, the disk map too under heavy tests (`make test-heavy`, the mutant script). No
// decision of the fast path depends on the map type; the disk map doubles a matrix's time.
func differentialTxMaps() []bool {
	if heavyTests() {
		return []bool{false, true}
	}

	return []bool{false}
}

// laneShapes is the shapes of fastPathMatrix the lane-position differential runs under `make test`:
// the ones whose positions or bitmaps differed before the fast path kept them. Heavy tests run all.
var laneShapes = map[string]struct{}{
	"two forward blocks":              {},
	"three forward blocks":            {},
	"hits just below survivors":       {},
	"pending set in the open subtree": {},
	"pending chains only":             {},
}

func TestReorgDifferential_FastPathMatchesSlowPath(t *testing.T) {
	for name, tweak := range fastPathMatrix() {
		for _, disk := range differentialTxMaps() {
			t.Run(fmt.Sprintf("%s/disk=%v", name, disk), func(t *testing.T) {
				cfg := smallScenarioConfig()
				cfg.diskTxMap = disk
				tweak(&cfg)

				requireFastPathMatchesSlow(t, cfg)
			})
		}
	}
}

// With lanes at shadow, the fast path leaves the tx map with the lane position of every transaction in
// assembly, the effective D1 ones read from the UTXO store, and every chained subtree with the lane
// bitmaps the slow path builds for it.
func TestReorgDifferential_FastPathKeepsLanePositions(t *testing.T) {
	for name, tweak := range fastPathMatrix() {
		if _, ok := laneShapes[name]; !ok && !heavyTests() {
			continue
		}

		for _, disk := range differentialTxMaps() {
			t.Run(fmt.Sprintf("%s/disk=%v", name, disk), func(t *testing.T) {
				cfg := smallScenarioConfig()
				cfg.diskTxMap = disk
				cfg.lanePositions = true
				tweak(&cfg)

				requireFastPathMatchesSlow(t, cfg)

				sc := newReorgScenario(t, cfg)
				require.NoError(t, fastReorgPath(t, sc))

				out := captureReorgOutcome(sc, nil)
				require.NotEmpty(t, out.order)

				for _, h := range out.order {
					require.Equal(t, scenarioLanePosition(h), out.positions[h], "lane position of %s", h)
				}

				for root, kept := range out.laneBitmaps {
					require.NotEqual(t, "none", kept, "lane bitmaps of chained subtree %s", root)
				}
			})
		}
	}
}

// Random forks over every knob the fast path accepts: overlap 0 to 100 percent, one to three winning
// blocks, scattered or front-clustered hits. Six seeds under `make test`; all 24 under heavy tests
// (`make test-heavy`, the mutant script).
func TestReorgDifferential_FastPathMatchesSlowPathOnRandomForks(t *testing.T) {
	seeds := uint64(6)
	if heavyTests() {
		seeds = 24
	}

	for seed := range seeds {
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

// No decline reason depends on the map type, so the disk map runs only under heavy tests.
func TestReorgDifferential_FastPathDeclinesWhatItCannotProve(t *testing.T) {
	disks := []bool{false}
	if heavyTests() {
		disks = append(disks, true)
	}

	for name, c := range fastPathDeclineCases() {
		for _, disk := range disks {
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

// A lost block whose subtree holds effective D1 but has no meta file cannot be read: the fast path
// declines, and the slow path then fails on the same file.
func TestReorgScenario_FastPathDeclinesAnUnreadableLostSubtree(t *testing.T) {
	requireDeclinedLikeSlow(t, smallScenarioConfig(), func(sc *reorgScenario) {
		for _, st := range sc.lost.Subtrees {
			require.NoError(t, sc.blobStore.Del(t.Context(), st[:], fileformat.FileTypeSubtreeMeta))
		}
	}, "read_failed")
}

// A file stored under a lost subtree's key that is another subtree (here a winning block's) has leaves the
// delta cannot tell from the real ones: the lost block's own transactions in that subtree would be dropped
// without a trace. The stored root in the file header must match the key.
func TestReorgScenario_FastPathDeclinesAForeignFileUnderALostSubtreeKey(t *testing.T) {
	requireDeclinedLikeSlow(t, smallScenarioConfig(), func(sc *reorgScenario) {
		require.Greater(t, len(sc.lost.Subtrees), 1)

		foreign, err := sc.blobStore.Get(t.Context(), sc.forward[0].Subtrees[0][:], fileformat.FileTypeSubtree)
		require.NoError(t, err)

		key := sc.lost.Subtrees[1][:]
		require.NoError(t, sc.blobStore.Del(t.Context(), key, fileformat.FileTypeSubtree))
		require.NoError(t, sc.blobStore.Set(t.Context(), key, fileformat.FileTypeSubtree, foreign))
	}, "read_failed")
}

// The disk tx map keeps a hint of the chained subtree each transaction is in, and a removal reads it. The
// rebuild gives every chained node the hint of its new subtree, so the first removal after the reorg is
// as cheap as before it.
func TestReorgScenario_FastPathLeavesEveryChainedNodeWithItsSubtreeHint(t *testing.T) {
	cfg := smallScenarioConfig()
	cfg.diskTxMap = true

	sc := newReorgScenario(t, cfg)

	var rec fastPathRun

	require.NoError(t, recordingFastPath(&rec)(t, sc))
	require.True(t, rec.applied, "declined: %v", rec.reasons)
	require.NotEmpty(t, sc.stp.chainedSubtrees)

	for k, st := range sc.stp.chainedSubtrees {
		for _, node := range st.Nodes {
			if node.Hash == subtreepkg.CoinbasePlaceholderHashValue {
				continue
			}

			p, ok := sc.stp.currentTxMap.Get(node.Hash)
			require.True(t, ok)
			require.Equal(t, int16(k+1), p.SubtreeIndex, "node %s of chained subtree %d", node.Hash, k)
		}
	}
}

// The memory gate: live heap plus the lost block and the winners' set plus the pending set twice (the flat copy of the survivors and the new subtrees) at 48 bytes each must stay within three
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
		// the set of about 1550 hashes x 33 B fits in the 200 kB of headroom, 2000 pending x 48 B once (96 kB) still fits, twice (192 kB) does not
		limit := int64(1 << 20)
		reorgMemoryStats = func() (uint64, int64) { return uint64(limit*3/4) - 200_000, limit }

		requireFastPathDeclines(t, cfg, "memory")
	})

	t.Run("the lost block tips it over", func(t *testing.T) {
		// The winners' set and the pending set twice fit in the headroom; the lost block's 1000 hashes x 33 B do not.
		limit := int64(1 << 20)
		reorgMemoryStats = func() (uint64, int64) { return uint64(limit*3/4) - 260_000, limit }

		requireFastPathDeclines(t, smallScenarioConfig(), "memory")
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

// The union-filtered final drain (design section 5 item 6): the slow path drains per forward step, the
// fast path once against both blocks. With kept mined sets on the paths agree; with retention 0 only the
// diverging hashes are left out of the comparison.
func TestReorgDifferential_FastPathUnionFilteredFinalDrain(t *testing.T) {
	for _, tc := range []struct {
		name      string
		retention time.Duration
		diverges  bool
	}{
		{name: "kept mined sets on", retention: 10 * time.Second},
		{name: "retention 0", retention: 0, diverges: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireUnionFilteredFinalDrain(t, tc.retention, tc.diverges)
		})
	}
}

func requireUnionFilteredFinalDrain(t *testing.T, retention time.Duration, diverges bool) {
	t.Helper()

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
		err := slowReference(t, sc)
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

	// With retention 0 the slow path's own result holds a transaction the first block mined, so the
	// order oracle would reject it: that is the difference, and only the fast path's result is checked.
	outcome := func(path reorgPath, oracle bool) reorgOutcome {
		sc := newReorgScenario(t, cfg)
		sc.stp.settings.BlockAssembly.MinedTxSetRetention = retention
		err := path(t, sc)
		require.NoError(t, err)

		if oracle {
			requireAssemblyValid(t, sc.stp, sc.onChainAfter)
		}

		return captureReorgOutcome(sc, err)
	}

	slow, fast := outcome(slowWithArrival, !diverges), outcome(fastWithArrival, true)

	if !diverges {
		require.Empty(t, compareReorgOutcomes(slow, fast, nil), "the kept sets make the slow path drop what the first block mined")

		for h := range hashes {
			require.NotContains(t, slow.order, h)
			require.NotContains(t, fast.order, h)
		}

		return
	}

	require.NotEmpty(t, compareReorgOutcomes(slow, fast, nil),
		"without the divergence the two paths differ: the slow path keeps what the first block mined")
	require.Empty(t, compareReorgOutcomes(slow, fast, []reorgDivergence{forgetArrivals}))

	for h := range hashes {
		require.Contains(t, slow.order, h, "the slow path kept the arrival")
		require.NotContains(t, fast.order, h, "the fast path dropped it")
	}
}

// keyLogStore notes how many bytes are read from each key of the subtree store. Opening a file is not
// reading it: checkMoveForwardSubtreesOpen opens every winning subtree up front and closes it.
type keyLogStore struct {
	blob.Store
	mu   sync.Mutex
	read map[chainhash.Hash]int
}

type loggedReader struct {
	io.ReadCloser
	store *keyLogStore
	key   chainhash.Hash
}

func (r *loggedReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)

	r.store.mu.Lock()
	r.store.read[r.key] += n
	r.store.mu.Unlock()

	return n, err
}

func (s *keyLogStore) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...bloboptions.FileOption) (io.ReadCloser, error) {
	reader, err := s.Store.GetIoReader(ctx, key, fileType, opts...)
	if err != nil {
		return nil, err
	}

	return &loggedReader{ReadCloser: reader, store: s, key: chainhash.Hash(key)}, nil
}

// forwardBytesRead is how many bytes have been read from the winning blocks' subtrees.
func (s *keyLogStore) forwardBytesRead(sc *reorgScenario) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	n := 0

	for _, block := range sc.forward {
		for _, st := range block.Subtrees {
			n += s.read[*st]
		}
	}

	return n
}

// logSubtreeReads starts noting the subtree reads of the scenario's processor.
func logSubtreeReads(sc *reorgScenario) *keyLogStore {
	log := &keyLogStore{Store: sc.stp.subtreeStore, read: make(map[chainhash.Hash]int)}
	sc.stp.subtreeStore = log

	return log
}

// prepareForwardSet puts into the processor's fork slot the set that PrepareBlock builds for the
// scenario's winning blocks, from blocks[0] to blocks[n-1], built as it builds them. The scenario's
// headers do not commit to their subtrees, so the bind in PrepareBlock and the memory gate are left out.
func (sc *reorgScenario) prepareForwardSet(t testing.TB, n int) {
	t.Helper()

	blocks := sc.forward[:n]
	e := &forkPrepared{tip: *sc.lost.Hash(), done: make(chan struct{}), cancel: func() {}}

	for _, block := range blocks {
		list := make([]chainhash.Hash, len(block.Subtrees))
		for i, st := range block.Subtrees {
			list[i] = *st
		}

		e.blocks = append(e.blocks, *block.Hash())
		e.subtrees = append(e.subtrees, list)
	}

	for _, block := range blocks {
		e.txCount += block.TransactionCount
	}

	// built as PrepareBlock builds it: on the bounded workers, verifying what is still to be checked
	require.NoError(t, sc.stp.buildForkTxMap(context.Background(), e))
	close(e.done)

	sc.stp.preparedMu.Lock()
	sc.stp.preparedFork = append(sc.stp.preparedFork, e)
	sc.stp.preparedMu.Unlock()
}

// With the winning blocks' set prepared, the fast path uses it: the winners' subtrees are not read
// again, and the reorg ends as it does without the set.
func TestReorgFastPath_UsesTheSetPreparedForTheWinners(t *testing.T) {
	cfg := smallScenarioConfig()

	var (
		reads    int
		slotLeft int
	)

	prepared := func(t testing.TB, sc *reorgScenario) error {
		sc.prepareForwardSet(t, len(sc.forward))

		log := logSubtreeReads(sc)
		err := fastReorgPath(t, sc)
		reads, slotLeft = log.forwardBytesRead(sc), len(forkEntries(sc.stp))

		return err
	}

	requireReorgEquivalent(t, cfg, slowReference, prepared)
	require.Zero(t, reads, "the winners' subtrees are in the prepared set")
	require.Zero(t, slotLeft, "the slot is let go")

	// the control: the same reorg without the set reads them
	requireReorgEquivalent(t, cfg, slowReference, func(t testing.TB, sc *reorgScenario) error {
		log := logSubtreeReads(sc)
		err := fastReorgPath(t, sc)
		reads = log.forwardBytesRead(sc)

		return err
	})
	require.Positive(t, reads)
}

// A set prepared for other blocks than the ones the reorg moves onto is not used.
func TestReorgFastPath_IgnoresASetPreparedForOtherBlocks(t *testing.T) {
	reads := 0

	requireReorgEquivalent(t, smallScenarioConfig(), slowReference, func(t testing.TB, sc *reorgScenario) error {
		sc.prepareForwardSet(t, 1)

		log := logSubtreeReads(sc)
		err := fastReorgPath(t, sc)
		reads = log.forwardBytesRead(sc)

		return err
	})
	require.Positive(t, reads, "the set is of the first winner only, so the winners are read")
}

// The sets are let go whenever the fast path declines, before the slow path allocates: at a gate before
// the set is taken, and at one after. reorgInPlace is called on its own, because the slow path that runs
// after a decline lets go of the slot too, once it moves a block forward.
func TestReorgFastPath_LetsGoOfThePreparedSetsWhenItDeclines(t *testing.T) {
	for name, c := range map[string]struct {
		reason string
		tweak  func(cfg *reorgScenarioConfig)
	}{
		"before the set is taken": {"tx_map_length", func(cfg *reorgScenarioConfig) { cfg.duplicateInPending = 4 }},
		"after the set is taken":  {"hits_exceed_survivors", func(cfg *reorgScenarioConfig) { cfg.pendingInWinners = 0.9 }},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := smallScenarioConfig()
			c.tweak(&cfg)

			sc := newReorgScenario(t, cfg)
			sc.prepareForwardSet(t, 1)
			sc.prepareForwardSet(t, 2)
			require.Len(t, forkEntries(sc.stp), 2)

			before := readFastPathCounts()

			result, err := sc.stp.reorgInPlace(t.Context(), []*model.Block{sc.lost}, sc.forward, nil)
			require.NoError(t, err)
			require.Nil(t, result, "declined")
			require.Equal(t, []string{c.reason}, readFastPathCounts().declinedSince(before))
			require.Empty(t, forkEntries(sc.stp))
		})
	}
}

// The memory gate charges the winners' set whether the fast path builds it or takes a prepared one: the
// live figure is the heap the last GC found, which need not include a set built since.
func TestReorgFastPath_MemoryGateChargesAPreparedSetToo(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	var setBytes uint64
	for _, block := range sc.forward {
		setBytes += block.TransactionCount * fastPathSetBytesPerHash
	}

	pending := uint64(sc.stp.currentTxMap.Length())
	live := uint64(1 << 20)
	// three quarters of the limit sit between the two sums
	limit := int64((live + 2*pending*fastPathBytesPerPending + setBytes/2) / 3 * 4)

	old := reorgMemoryStats
	reorgMemoryStats = func() (uint64, int64) { return live, limit }

	defer func() { reorgMemoryStats = old }()

	var rec fastPathRun

	require.NoError(t, recordingFastPath(&rec)(t, sc))
	require.Equal(t, []string{"memory"}, rec.reasons, "built from the store, the set is charged")

	prepared := newReorgScenario(t, smallScenarioConfig())
	prepared.prepareForwardSet(t, len(prepared.forward))

	rec = fastPathRun{}

	require.NoError(t, recordingFastPath(&rec)(t, prepared))
	require.Equal(t, []string{"memory"}, rec.reasons, "prepared, the set is charged all the same")
}

// The differential harness still matches the slow path on prepared sets, over what the fast path's own
// differential cases run.
func TestReorgDifferential_FastPathWithPreparedSetsMatchesSlow(t *testing.T) {
	configs := map[string]func(cfg *reorgScenarioConfig){
		"small":         func(cfg *reorgScenarioConfig) {},
		"one winner":    func(cfg *reorgScenarioConfig) { cfg.forwardBlocks = 1 },
		"hits at front": func(cfg *reorgScenarioConfig) { cfg.hitsAtFront = true },
		"disk map":      func(cfg *reorgScenarioConfig) { cfg.diskTxMap = true },
	}

	for name, tweak := range configs {
		t.Run(name, func(t *testing.T) {
			cfg := smallScenarioConfig()
			tweak(&cfg)

			var rec fastPathRun

			requireReorgEquivalent(t, cfg, slowReference, func(t testing.TB, sc *reorgScenario) error {
				sc.prepareForwardSet(t, len(sc.forward))

				return recordingFastPath(&rec)(t, sc)
			})
			require.True(t, rec.applied, "declined: %v", rec.reasons)
		})
	}
}

// handOff queues one transaction through AddBatch, which checks it against the kept mined sets, and
// reports whether it was queued.
func handOff(sc *reorgScenario, hash chainhash.Hash) bool {
	before := sc.stp.QueueLength()

	sc.stp.AddBatch(
		[]subtreepkg.Node{{Hash: hash, Fee: 1, SizeInBytes: 1}},
		[]*subtreepkg.TxInpoints{{}},
	)

	return sc.stp.QueueLength() > before
}

// A late hand-off of a transaction a winning block mined stays out of the template after a fast-path
// reorg, as it does after the slow path's forward steps (kept mined sets). The one union set sits under
// F2, so a later move-back of only F2 also forgets F1's: a late F1 hand-off is placed (documented fallback).
func TestReorgDifferential_FastPathKeepsTheWinnersMinedSet(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	var rec fastPathRun

	require.NoError(t, recordingFastPath(&rec)(t, sc))
	require.True(t, rec.applied, "the fast path must apply, it declined: %v", rec.reasons)

	// forwardBlocks is 2: the first half of the winners is F1, the second F2.
	f1 := sc.hash(sc.winnerIDs[0])
	f2 := sc.hash(sc.winnerIDs[len(sc.winnerIDs)-1])
	neither := chainhash.HashH([]byte("mined-by-neither-winner"))

	require.False(t, handOff(sc, f1), "a transaction F1 mined must stay out")
	require.False(t, handOff(sc, f2), "a transaction F2 mined must stay out")
	require.True(t, handOff(sc, neither), "a transaction no winner mined is placed")

	// What the head of a later reorgBlocks that moves the tip back does. The scenario has no block to
	// move forward instead, and reorgBlocks rejects a move with none.
	sc.stp.forgetMinedSets([]*model.Block{sc.forward[len(sc.forward)-1]})
	require.Zero(t, sc.stp.keptMinedSetCount())

	require.True(t, handOff(sc, f2), "after the tip is moved back its transaction is placed")
	require.True(t, handOff(sc, f1), "the union goes with the tip, which only falls back to no kept set")
}

// A transaction the winners mine is confirmed, so its drop is forgotten as each slow-path step does,
// and the drop of one nobody mined is kept. A dropped tx makes the fast path decline, so the slow path
// does it.
func TestReorgDifferential_DroppedTxsForgottenWhenTheWinnersMineThem(t *testing.T) {
	cfg := smallScenarioConfig()

	var dropped []chainhash.Hash

	seeded := func(path reorgPath, record bool) reorgPath {
		return func(t testing.TB, sc *reorgScenario) error {
			for _, h := range []chainhash.Hash{
				sc.hash(sc.winnerIDs[0]),
				sc.hash(sc.winnerIDs[len(sc.winnerIDs)-1]),
				chainhash.HashH([]byte("dropped-and-never-mined")),
			} {
				sc.stp.droppedMu.Lock()
				sc.stp.recordDroppedLocked(h)
				sc.stp.droppedMu.Unlock()
			}

			err := path(t, sc)

			if record {
				sc.stp.droppedMu.Lock()
				dropped = slices.Collect(maps.Keys(sc.stp.droppedTxs))
				sc.stp.droppedMu.Unlock()
			}

			return err
		}
	}

	var rec fastPathRun

	requireReorgEquivalent(t, cfg, seeded(slowReference, false), seeded(recordingFastPath(&rec), true))
	require.Equal(t, []string{"dropped_txs"}, rec.reasons)
	require.Equal(t, []chainhash.Hash{chainhash.HashH([]byte("dropped-and-never-mined"))}, dropped)
}

// A declined fast path leaves the slow path to keep a set per forward block, exactly as with the fast
// path off: one entry for each block that mined something, none for an empty one.
func TestReorgDifferential_FastPathDeclineKeepsPerBlockSets(t *testing.T) {
	cfg := smallScenarioConfig()
	cfg.emptyForwardLast = true

	keptBlocks := func(sc *reorgScenario) []chainhash.Hash {
		var hashes []chainhash.Hash

		for _, set := range sc.stp.recentMined.load() {
			hashes = append(hashes, set.blockHash)
		}

		return sortedHashes(hashes)
	}

	flagOff := newReorgScenario(t, cfg)
	require.NoError(t, slowReference(t, flagOff))

	var rec fastPathRun

	declined := newReorgScenario(t, cfg)
	require.NoError(t, recordingFastPath(&rec)(t, declined))
	require.Equal(t, []string{"empty_forward_block"}, rec.reasons)

	want := sortedHashes([]chainhash.Hash{*declined.forward[0].Hash(), *declined.forward[1].Hash()})
	require.Equal(t, want, keptBlocks(declined))
	require.Equal(t, keptBlocks(flagOff), keptBlocks(declined))
}

// withSetup runs setup on the scenario before path does.
func withSetup(path reorgPath, setup func(sc *reorgScenario)) reorgPath {
	return func(t testing.TB, sc *reorgScenario) error {
		setup(sc)

		return path(t, sc)
	}
}

// The admission of a moved-back transaction (the tx era and the dropped set) and the drop of its pending
// descendants live in the slow path. The fast path declines when the era could refuse the lost block, or
// when a drop exists, rather than duplicate them.
func TestReorgDifferential_FastPathDeclinesWhenTheTxEraRefusesTheLostBlock(t *testing.T) {
	cfg := smallScenarioConfig()

	// the era is 100..199 and the lost block is at height 1
	setEra := func(sc *reorgScenario) { sc.stp.SetTxEra(150) }

	var rec fastPathRun

	requireReorgEquivalent(t, cfg, withSetup(slowReference, setEra), withSetup(recordingFastPath(&rec), setEra))
	require.False(t, rec.applied, "the fast path must decline")
	require.Equal(t, []string{"tx_era"}, rec.reasons)
}

func TestReorgDifferential_FastPathDeclinesWhenADroppedTxExists(t *testing.T) {
	cfg := smallScenarioConfig()

	// the parent of every lost-block transaction is dropped, so the D1 ones are refused
	seed := func(sc *reorgScenario) {
		sc.stp.droppedMu.Lock()
		defer sc.stp.droppedMu.Unlock()

		for _, id := range sc.lostIDs {
			sc.stp.recordDroppedLocked(sc.confirmedParent(id))
		}
	}

	var rec fastPathRun

	requireReorgEquivalent(t, cfg, withSetup(slowReference, seed), withSetup(recordingFastPath(&rec), seed))
	require.False(t, rec.applied, "the fast path must decline")
	require.Equal(t, []string{"dropped_txs"}, rec.reasons)
}

func TestReorgDifferential_FastPathStaysFastWhenTheEraHoldsTheLostBlock(t *testing.T) {
	// an era that includes the lost block's height, and no drops: the common case stays fast
	setEra := func(sc *reorgScenario) {
		sc.stp.SetTxEra(sc.lost.Height)

		lo, hi, ok := sc.stp.TxEra()
		require.True(t, ok, "the era is set")
		require.LessOrEqual(t, lo, sc.lost.Height)
		require.GreaterOrEqual(t, hi, sc.lost.Height)
	}

	var rec fastPathRun

	requireReorgEquivalent(t, smallScenarioConfig(), withSetup(slowReference, setEra), withSetup(recordingFastPath(&rec), setEra))
	require.True(t, rec.applied, "the fast path must apply, it declined: %v", rec.reasons)
}

// The store requests the old subtrees still have in flight read tx map entries the fast path deletes,
// so a build that misses a parent then must read as cancelled, not failed: the fast path supersedes
// them, as reset does, and the requests made from then on are live.
func TestReorgScenario_FastPathSupersedesTheStoreRequestsOfTheOldSubtrees(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())

	old := sc.stp.supersedeToken()
	oldRequest := NewSubtreeRequest{superseded: old}

	require.False(t, oldRequest.Superseded(), "nothing has superseded the request yet")

	var rec fastPathRun

	require.NoError(t, recordingFastPath(&rec)(t, sc))
	require.True(t, rec.applied, "declined: %v", rec.reasons)

	require.True(t, oldRequest.Superseded(), "a request made before the fast path is superseded")
	require.False(t, NewSubtreeRequest{superseded: sc.stp.supersedeToken()}.Superseded(), "a request made after it is not")
}

// Effective D1 is read from the lost block's subtrees a second time, after the delta was computed. A
// subtree that no longer holds the node the delta recorded cannot be installed: the fast path declines.
func TestReorgScenario_FastPathDeclinesWhenALostSubtreeChangedBetweenTheTwoReads(t *testing.T) {
	cfg := smallScenarioConfig()
	cfg.pendingSpendsLost, cfg.lostChildOfPending = 0, 0

	sc := newReorgScenario(t, cfg)

	winners, _, err := sc.stp.buildUnionSet(t.Context(), sc.forward)
	require.NoError(t, err)

	delta, err := sc.stp.computeReorgDelta(t.Context(), []*model.Block{sc.lost}, winners, 2)
	require.NoError(t, err)
	require.Greater(t, len(delta.D1), 2)

	read := func(effective []reorgDeltaNode) (*fastReorgPlan, string) {
		plan := &fastReorgPlan{}

		return plan, sc.stp.readEffectiveD1(t.Context(), plan, sc.lost, effective)
	}

	plan, reason := read(slices.Clone(delta.D1))
	require.Empty(t, reason, "the unchanged subtrees are read")
	require.Len(t, plan.d1Nodes, len(delta.D1))

	moved := slices.Clone(delta.D1)
	moved[1].Hash = chainhash.HashH([]byte("not the recorded node"))

	_, reason = read(moved)
	require.Equal(t, "read_failed", reason, "a node with another hash")

	moved = slices.Clone(delta.D1)
	moved[1].Node = 1 << 20

	_, reason = read(moved)
	require.Equal(t, "read_failed", reason, "a node past the end of the subtree")
}

// Issue 2269, plan PR 6: once the fast path has removed the lost block's coinbase there is no rollback,
// only a reset. The reset sweeps the installed subtrees for unmined transactions, so whatever is not in
// them at the moment of a failure must be swept some other way: effective D1 is flagged mined in the
// store from the coinbase removal until it is in the installed subtrees.

// requireResetRecoversEveryUnminedTx resets after a failed fast path and requires the sweep to have
// marked every transaction that is unmined after the reorg (the pending ones the winners did not mine
// and the lost ones no winner has) as not on the longest chain, which is what lets the reload find them.
func requireResetRecoversEveryUnminedTx(t *testing.T, sc *reorgScenario) {
	t.Helper()

	require.True(t, sc.stp.failedForkReorgNeedsReset.Load(), "a failed fork reorg waits for a reset")

	// The lost block's transactions are mined in the store, which is the state a failed fast path leaves them in.
	require.NoError(t, sc.utxo.Store.MarkTransactionsOnLongestChain(t.Context(), sc.rawD1(), true))

	require.NoError(t, sc.stp.reset(sc.genesis, []*model.Block{sc.lost}, sc.forward, false, func() error { return nil }))

	// The end state, read from the store: the lost block's transactions are unmined again. The pending
	// ones are not in the scenario's store, so only the spy sees their marks.
	for _, h := range sc.rawD1() {
		data, err := sc.utxo.Get(t.Context(), &h, fields.UnminedSince)
		require.NoError(t, err)
		require.NotZero(t, data.UnminedSince, "lost transaction %s no winner has is unmined in the store after the reset", h)
	}

	require.False(t, sc.stp.failedForkReorgNeedsReset.Load())
	require.Empty(t, sc.stp.reorgPendingD1, "the reset that swept the record clears it")
}

// A failure between the coinbase removal and the end of the build: the old subtrees are put back and
// hold the pending set, and effective D1 is in no subtree at all.
func TestReorgScenario_FastPathFailureBeforeTheBuildEndsInAResetThatSweepsEffectiveD1(t *testing.T) {
	first := fastPathFirstBeat(t, smallScenarioConfig())

	for _, c := range []struct {
		name string
		beat int
	}{{"after the coinbase removal", first}, {"after the D1 insert", first + 1}} {
		t.Run(c.name, func(t *testing.T) {
			sc := newReorgScenario(t, smallScenarioConfig())
			sc.stp.settings.BlockAssembly.ReorgFastPath = true

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			beats := 0

			sc.stp.SetProgressHook(func() {
				if beats++; beats == c.beat {
					cancel()
				}
			})

			err := sc.stp.reorgBlocks(ctx, []*model.Block{sc.lost}, sc.forward)

			require.Error(t, err)
			require.NotEmpty(t, sc.stp.reorgPendingD1, "effective D1 is recorded for the reset")
			require.Equal(t, sc.pendingOrder(), txHashes(assemblyNodes(sc.stp)), "the pending set is back in the subtrees")
			require.Empty(t, sc.utxo.marked(true))

			requireResetRecoversEveryUnminedTx(t, sc)
		})
	}
}

// A failure after the build, in the awaited stores before the marks (PR 2b): effective D1 is installed,
// nothing is marked, and the reset recovers everything.
func TestReorgScenario_FastPathFailedStoreEndsInAResetThatSweepsTheInstalledSubtrees(t *testing.T) {
	cfg := smallScenarioConfig()
	cfg.reorgStoreDeadline = 10 * time.Second
	cfg.storeConsumer = failAwaited

	sc := newReorgScenario(t, cfg)

	var rec fastPathRun

	err := recordingFastPath(&rec)(t, sc)

	require.Error(t, err)
	require.True(t, rec.applied, "the failure is after the fast path applied")
	require.Empty(t, sc.stp.reorgPendingD1, "effective D1 is installed, nothing to record")
	require.Empty(t, sc.utxo.marked(true))
	require.Empty(t, sc.utxo.marked(false), "nothing is marked before the stores are stored")

	requireResetRecoversEveryUnminedTx(t, sc)
}

// Failures inside the in-place delete and the drain can only come from the map or the store; the
// reorg returns them and the reset recovers, as above. A failing coinbase application of a winning block
// is the one a test can reach: the first winning block's coinbase is already in the store.
func TestReorgScenario_FastPathFailedCoinbaseApplicationEndsInAResetThatSweepsTheInstalledSubtrees(t *testing.T) {
	first := fastPathFirstBeat(t, smallScenarioConfig())

	sc := newReorgScenario(t, smallScenarioConfig())
	sc.stp.settings.BlockAssembly.ReorgFastPath = true

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	beats := 0

	sc.stp.SetProgressHook(func() {
		// the beat before the first winning block's coinbase is applied
		if beats++; beats == first+3 {
			cancel()
		}
	})

	require.Error(t, sc.stp.reorgBlocks(ctx, []*model.Block{sc.lost}, sc.forward), "the cancelled context fails the coinbase application")
	require.Empty(t, sc.stp.reorgPendingD1)
	requireResetRecoversEveryUnminedTx(t, sc)
}

// Map writes of the fast path: effective D1 and the drained arrivals are the only inserts, and the delete
// set (the hits) is the only set of deletes, whatever the size of the pending set. The slow path inserts
// every survivor into a fresh map, which grows with the pending set.
func TestReorgScenario_FastPathMapWritesDoNotGrowWithThePendingSet(t *testing.T) {
	const (
		hits     = 300
		arrivals = 40
	)

	type counted struct {
		inserted, setIfNotExists, set, deleted int64
		effectiveD1                            int
	}

	run := func(t *testing.T, pending int) counted {
		cfg := smallScenarioConfig()
		cfg.pending = pending
		cfg.pendingInWinners = float64(hits) / float64(pending)
		cfg.hitsAtFront = true
		cfg.pendingChains, cfg.removeMapPending = 0, 20
		cfg.subtreeSize = 1024

		sc := newReorgScenario(t, cfg)
		sc.stp.settings.BlockAssembly.ReorgFastPath = true
		require.Zero(t, sc.stp.settings.BlockAssembly.DoubleSpendWindow)

		counting := newCountingTxMap(sc.stp.currentTxMap)
		sc.stp.currentTxMap = counting

		// arrivals queued before the reorg are taken by the head drain
		enqueueAt := time.Now()
		sc.stp.queue.clock = fixedClock{t: enqueueAt}
		sc.stp.clock = fixedClock{t: enqueueAt.Add(time.Millisecond)}

		nodes := make([]subtreepkg.Node, arrivals)
		inpoints := make([]*subtreepkg.TxInpoints, arrivals)

		for k := range arrivals {
			id := kindIntake | scenarioID(k)
			p := voutZeroInpoints(sc.confirmedParent(id))
			nodes[k], inpoints[k] = subtreepkg.Node{Hash: sc.hash(id), Fee: 1, SizeInBytes: 250}, &p
		}

		sc.stp.AddBatch(nodes, inpoints)

		var rec fastPathRun

		require.NoError(t, recordingFastPath(&rec)(t, sc))
		require.True(t, rec.applied, "declined: %v", rec.reasons)

		c := counting.counts()

		effective := 0

		for _, h := range sc.rawD1() {
			if !sc.inPending(h) {
				effective++
			}
		}

		return counted{c.inserted, c.setIfNotExists, c.set, c.delete, effective}
	}

	small, large := run(t, 4_000), run(t, 16_000)

	for _, c := range []counted{small, large} {
		require.Equal(t, int64(c.effectiveD1+arrivals), c.inserted, "effective D1 plus the drained arrivals")
		require.Equal(t, c.inserted, c.setIfNotExists, "nothing is inserted twice")
		require.Zero(t, c.set)
		require.Equal(t, int64(hits+20), c.deleted, "the hits and the removeMap entries, once each")
	}

	require.Equal(t, small.inserted, large.inserted)
	require.Equal(t, small.deleted, large.deleted)
}

// The awaited stores of PR 2b hold for the fast path: its subtrees are stored before the first
// longest-chain mark and announced after it, and a failed store fails the reorg before any mark.
func TestReorgScenario_FastPathStoresBeforeTheMarksAndAnnouncesAfter(t *testing.T) {
	for _, c := range []struct {
		name   string
		answer func(req NewSubtreeRequest)
		fails  bool
	}{
		{"stored", answerStored, false},
		{"failed", failAwaited, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			sc, r, logger := newAwaitedStoreScenario(t, 5*time.Second, c.answer)

			var rec fastPathRun

			err := recordingFastPath(&rec)(t, sc)

			require.True(t, rec.applied, "declined: %v", rec.reasons)

			r.mu.Lock()
			require.NotEmpty(t, r.awaited, "the reorg waits for the stores of the subtrees it built")

			for _, marks := range r.awaited {
				require.Zero(t, marks, "every awaited store is sent before the first longest-chain mark")
			}

			r.mu.Unlock()

			if c.fails {
				require.Error(t, err)
				require.Zero(t, sc.utxo.markCount(true)+sc.utxo.markCount(false), "no longest-chain mark is written")
				require.True(t, sc.stp.failedForkReorgNeedsReset.Load(), "only a reset may follow")
				require.NotContains(t, logger.phases("reorgBlocks"), "processed_at", "no forward block is marked processed")

				r.mu.Lock()
				defer r.mu.Unlock()

				require.Empty(t, r.announced, "nothing is announced")

				return
			}

			require.NoError(t, err)

			// nothing waits for the announcing send, so the listener may take it after the reorg returns
			require.Eventually(t, func() bool {
				r.mu.Lock()
				defer r.mu.Unlock()

				return len(r.announced) > 0
			}, 5*time.Second, 10*time.Millisecond, "the reorg announces its subtrees")

			r.mu.Lock()
			defer r.mu.Unlock()

			for _, marks := range r.announced {
				require.Positive(t, marks, "nothing is announced before the marks")
			}
		})
	}
}

// Design section 3.2: every node of the installed subtrees has an entry in the tx map at every instant.
// The meta of a subtree that holds an effective D1 transaction reads its inpoints from the map, and a
// reset or a removal between two steps would find the node without one.
func TestReorgScenario_FastPathEveryInstalledNodeIsInTheMapAtEveryBeat(t *testing.T) {
	for _, disk := range []bool{false, true} {
		cfg := smallScenarioConfig()
		cfg.diskTxMap = disk

		sc := newReorgScenario(t, cfg)
		sc.stp.settings.BlockAssembly.ReorgFastPath = true

		beats, checked := 0, 0

		sc.stp.SetProgressHook(func() {
			beats++

			if sc.stp.reorgPendingD1 == nil && checked == 0 {
				return // before the fast path
			}

			missing := 0

			for _, node := range assemblyNodes(sc.stp) {
				if node.Hash != subtreepkg.CoinbasePlaceholderHashValue && !sc.stp.currentTxMap.Exists(node.Hash) {
					missing++
				}
			}

			require.Zero(t, missing, "beat %d: installed nodes without a tx map entry", beats)

			checked++
		})

		require.NoError(t, sc.run(t))
		require.GreaterOrEqual(t, checked, 4, "the fast path beats at least four times")
	}
}

// coinbaseDeleteStore runs onDelete when the coinbase of the lost block is deleted, the last step of
// removeCoinbaseUtxos, and returns what it returns.
type coinbaseDeleteStore struct {
	utxo.Store

	onDelete func() error
}

func (s *coinbaseDeleteStore) Delete(ctx context.Context, hash *chainhash.Hash) error {
	if err := s.onDelete(); err != nil {
		return err
	}

	return s.Store.Delete(ctx, hash)
}

// The coinbase removal is the first change the fast path makes to the store, and effective D1 is flagged
// mined from there on, in no installed subtree: a failure inside the removal, or the length check right
// after it, must still leave effective D1 recorded for the reset.
func TestReorgScenario_FastPathFailureInsideTheCoinbaseRemovalEndsInAResetThatSweepsEffectiveD1(t *testing.T) {
	for name, inject := range map[string]func(sc *reorgScenario) error{
		"the removal fails": func(*reorgScenario) error { return errors.NewStorageError("injected coinbase delete failure") },
		"the removal takes a transaction out of the assembly": func(sc *reorgScenario) error {
			sc.stp.currentTxMap.Delete(sc.pendingOrder()[0])

			return nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			sc := newReorgScenario(t, smallScenarioConfig())
			real := sc.stp.utxoStore
			failing := true

			sc.stp.utxoStore = &coinbaseDeleteStore{Store: real, onDelete: func() error {
				if failing {
					return inject(sc)
				}

				return nil
			}}

			require.Error(t, recordingFastPath(&fastPathRun{})(t, sc))
			require.NotEmpty(t, sc.stp.reorgPendingD1, "effective D1 is recorded before the coinbase is touched")

			failing = false

			requireResetRecoversEveryUnminedTx(t, sc)
		})
	}
}

// Design section 5 item 10: a removal accepted after the flag pass is not in the flags. The first
// template may contain the transaction, as it can after any Remove; the processor applies the removal
// when it resumes its removal channel.
func TestReorgScenario_FastPathRemovalAcceptedAfterTheFlagPassIsAppliedOnTheNextDequeue(t *testing.T) {
	sc := newReorgScenario(t, smallScenarioConfig())
	sc.stp.settings.BlockAssembly.ReorgFastPath = true

	var removed chainhash.Hash

	for _, h := range sc.pendingOrder() {
		if !sc.inWinners(h) && !sc.inLost(h) {
			removed = h

			break
		}
	}

	require.NotZero(t, removed)

	fired := false

	sc.stp.SetProgressHook(func() {
		// the first beat of the fast path is after the flag pass
		if !fired && sc.stp.reorgPendingD1 != nil {
			fired = true

			require.NoError(t, sc.stp.Remove(t.Context(), removed))
		}
	})

	var rec fastPathRun

	require.NoError(t, recordingFastPath(&rec)(t, sc))
	require.True(t, rec.applied, "declined: %v", rec.reasons)
	require.True(t, fired)

	require.Contains(t, txHashes(assemblyNodes(sc.stp)), removed, "the first template may hold the transaction")

	select {
	case h := <-sc.stp.removeTxCh:
		require.Equal(t, removed, h)
		require.NoError(t, sc.stp.removeTxFromSubtrees(t.Context(), h))
	case <-time.After(5 * time.Second):
		t.Fatal("the removal reached no channel")
	}

	require.NotContains(t, txHashes(assemblyNodes(sc.stp)), removed)
	require.False(t, sc.stp.currentTxMap.Exists(removed))
	requireAssemblyValid(t, sc.stp, sc.onChainAfter)
}
