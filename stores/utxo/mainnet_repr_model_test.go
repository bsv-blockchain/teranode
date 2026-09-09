package utxo_test

import (
	"container/heap"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOutCountSampler_MatchesMainnetShape(t *testing.T) {
	s := newOutCountSampler(42)
	const n = 200000
	vals := make([]int, n)
	var sum int
	for i := range vals {
		v := s.sample()
		require.GreaterOrEqual(t, v, 1)
		require.LessOrEqual(t, v, 1024) // capped
		vals[i] = v
		sum += v
	}
	sort.Ints(vals)
	median := vals[n/2]
	p90 := vals[n*90/100]
	p99 := vals[n*99/100]
	mean := float64(sum) / n
	require.Equal(t, 2, median, "median outputs")
	require.LessOrEqual(t, p90, 15, "p90")
	require.InDelta(t, 139, p99, 30, "p99")
	require.InDelta(t, 10.0, mean, 4.0, "mean ~10")
}

// outCountSampler draws outputs-per-tx calibrated to mainnet txs_p07
// (median 2, mean ~10, p90 13, p99 139), capped at 1024 to bound memory.
type outCountSampler struct{ rng *rand.Rand }

func newOutCountSampler(seed int64) *outCountSampler {
	return &outCountSampler{rng: rand.New(rand.NewSource(seed))}
}

func (s *outCountSampler) sample() int {
	u := s.rng.Float64()
	switch {
	case u < 0.75:
		return 1 + s.rng.Intn(2) // 1-2  (median 2)
	case u < 0.92:
		return 3 + s.rng.Intn(8) // 3-10 (p90 ~10)
	case u < 0.99:
		return 11 + s.rng.Intn(129) // 11-139 (p99 ~139)
	default:
		return 140 + s.rng.Intn(885) // 140-1024 tail
	}
}

func TestSpendAgeSampler_MatchesMainnetShape(t *testing.T) {
	s := newSpendAgeSampler(7)
	const n = 200000
	vals := make([]int, n)
	var sameBlock, within6 int
	for i := range vals {
		v := s.sample()
		require.GreaterOrEqual(t, v, 0)
		if v == 0 {
			sameBlock++
		}
		if v <= 6 {
			within6++
		}
		vals[i] = v
	}
	sort.Ints(vals)
	p50, p90 := vals[n/2], vals[n*90/100]
	require.InDelta(t, 0.10, float64(sameBlock)/n, 0.03, "same-block frac")
	require.InDelta(t, 0.25, float64(within6)/n, 0.04, "<=6 frac")
	require.InDelta(t, 215, p50, 60, "p50")
	require.InDelta(t, 6273, p90, 1200, "p90")
}

// spendAgeSampler draws heights-until-spent calibrated to mainnet
// (same-block 10%, <=6 25%, p50 215, p90 6273, p99 47872, max ~154570).
type spendAgeSampler struct{ rng *rand.Rand }

func newSpendAgeSampler(seed int64) *spendAgeSampler {
	return &spendAgeSampler{rng: rand.New(rand.NewSource(seed))}
}

func (s *spendAgeSampler) sample() int {
	u := s.rng.Float64()
	switch {
	case u < 0.10:
		return 0
	case u < 0.25:
		return 1 + s.rng.Intn(6) // 1-6
	case u < 0.50:
		return 7 + s.rng.Intn(209) // 7-215
	case u < 0.90:
		return 216 + s.rng.Intn(6058) // 216-6273
	case u < 0.99:
		return 6274 + s.rng.Intn(41599) // 6274-47872
	default:
		return 47873 + s.rng.Intn(106698) // tail to ~154570
	}
}

func TestUTXOScheduler_SameBlockAndFutureSpends(t *testing.T) {
	s := newUTXOScheduler(1, 0.0, 8) // no survivors, up to 8 inputs/tx
	// First tx at height 0: nothing created yet => no inputs.
	_, _, in0 := s.createTx(0)
	require.Empty(t, in0, "first tx at height 0 has nothing to spend")
	// More txs at height 0: age-0 (same-block, ~10%) outputs from earlier txs
	// become due at height 0 under <= semantics and get consumed.
	sameBlock := 0
	for i := 0; i < 2000; i++ {
		_, _, in := s.createTx(0)
		sameBlock += len(in)
	}
	require.Positive(t, sameBlock, "same-block (age 0) spends must be consumable at creation height (<= semantics)")
	// Advancing ahead, the longer-age cohort comes due and is consumed too.
	future := 0
	for h := 1; h <= 7000; h++ {
		_, _, in := s.createTx(h)
		future += len(in)
	}
	require.Positive(t, future, "scheduled outputs come due at later heights")
}

func TestUTXOScheduler_SurvivorsNeverComplete(t *testing.T) {
	// survivorProb 1.0: every output is a permanent survivor; no tx ever completes.
	s := newUTXOScheduler(2, 1.0, 2)
	for i := 0; i < 100; i++ {
		s.createTx(0)
	}
	for h := 1; h <= 200000; h += 5000 {
		s.createTx(h)
	}
	require.Empty(t, s.completedSince(), "survivor-only txs never become prunable")
}

type outpoint struct {
	tx   uint64
	vout uint32
}

// pendingSpend: an outpoint scheduled to be consumed at dueHeight.
type pendingSpend struct {
	op        outpoint
	dueHeight int
}

type spendHeap []pendingSpend

func (h spendHeap) Len() int           { return len(h) }
func (h spendHeap) Less(i, j int) bool { return h[i].dueHeight < h[j].dueHeight }
func (h spendHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *spendHeap) Push(x any)        { *h = append(*h, x.(pendingSpend)) }
func (h *spendHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[:n-1]
	return it
}

type utxoScheduler struct {
	rng          *rand.Rand
	outc         *outCountSampler
	agec         *spendAgeSampler
	survivorProb float64
	inputsPerTx  int
	nextTxID     uint64
	pending      spendHeap
	remaining    map[uint64]int // tx id -> scheduled (non-survivor) outputs not yet spent
	hasSurvivor  map[uint64]bool
	completed    []uint64
}

func newUTXOScheduler(seed int64, survivorProb float64, inputsPerTx int) *utxoScheduler {
	r := rand.New(rand.NewSource(seed))
	s := &utxoScheduler{
		rng:          r,
		outc:         &outCountSampler{rng: rand.New(rand.NewSource(seed + 1))},
		agec:         &spendAgeSampler{rng: rand.New(rand.NewSource(seed + 2))},
		survivorProb: survivorProb,
		inputsPerTx:  inputsPerTx,
		remaining:    make(map[uint64]int),
		hasSurvivor:  make(map[uint64]bool),
	}
	heap.Init(&s.pending)
	return s
}

func (s *utxoScheduler) createTx(height int) (uint64, int, []outpoint) {
	// Consume up to inputsPerTx already-due outpoints as this tx's inputs.
	var inputs []outpoint
	for len(inputs) < s.inputsPerTx && s.pending.Len() > 0 && s.pending[0].dueHeight <= height {
		ps := heap.Pop(&s.pending).(pendingSpend)
		inputs = append(inputs, ps.op)
		if r := s.remaining[ps.op.tx]; r > 0 {
			s.remaining[ps.op.tx] = r - 1
			if s.remaining[ps.op.tx] == 0 && !s.hasSurvivor[ps.op.tx] {
				s.completed = append(s.completed, ps.op.tx)
				delete(s.remaining, ps.op.tx)
			}
		}
	}
	// Allocate this tx and schedule its outputs.
	id := s.nextTxID
	s.nextTxID++
	oc := s.outc.sample()
	scheduled := 0
	for v := 0; v < oc; v++ {
		if s.rng.Float64() < s.survivorProb {
			s.hasSurvivor[id] = true
			continue
		}
		age := s.agec.sample()
		heap.Push(&s.pending, pendingSpend{op: outpoint{tx: id, vout: uint32(v)}, dueHeight: height + age})
		scheduled++
	}
	if scheduled > 0 {
		s.remaining[id] = scheduled
	} else if !s.hasSurvivor[id] {
		// zero spendable outputs scheduled and no survivor => immediately complete
		s.completed = append(s.completed, id)
	}
	return id, oc, inputs
}

func (s *utxoScheduler) completedSince() []uint64 {
	out := s.completed
	s.completed = nil
	return out
}

func (s *utxoScheduler) liveTxCount() int { return len(s.remaining) + countSurvivors(s.hasSurvivor) }

func countSurvivors(m map[uint64]bool) int {
	n := 0
	for _, v := range m {
		if v {
			n++
		}
	}
	return n
}
