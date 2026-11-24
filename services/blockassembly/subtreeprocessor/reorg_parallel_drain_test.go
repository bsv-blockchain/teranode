package subtreeprocessor

import (
	"fmt"
	"testing"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// queueIntake queues n transactions of the scenario's intake kind, ids from first, which no set holds.
func queueIntake(sc *reorgScenario, first, n int) {
	nodes := make([]subtreepkg.Node, n)
	inpoints := make([]*subtreepkg.TxInpoints, n)

	for i := range nodes {
		id := kindIntake | scenarioID(first+i)
		p := voutZeroInpoints(sc.confirmedParent(id))
		nodes[i] = subtreepkg.Node{Hash: sc.hash(id), Fee: 200, SizeInBytes: 250}
		inpoints[i] = &p
	}

	sc.stp.AddBatch(nodes, inpoints)
}

// queuedDuringReorg is a reorg path that has 350 transactions queued before the reorg and 100 more each
// time a drain starts, so every drain finds something. Fifty of the first are queued twice in the same
// drain, and each later batch repeats the last ten of the one before, which the drain before it placed;
// the first copy wins. It records whether each drain ran in parallel, or, with parallel off, runs them
// all on one goroutine.
func queuedDuringReorg(t testing.TB, parallel bool, modes *[]bool) reorgPath {
	return func(t testing.TB, sc *reorgScenario) error {
		prevHook, prevParallel := reorgDrainHook, reorgParallelDrain
		defer func() { reorgDrainHook, reorgParallelDrain = prevHook, prevParallel }()

		reorgParallelDrain = parallel
		queued := 350

		queueIntake(sc, 0, 300)
		queueIntake(sc, 250, 100)

		reorgDrainHook = func(p bool) {
			*modes = append(*modes, p)

			queueIntake(sc, queued-10, 110)
			queued += 100
		}

		return sc.run(t)
	}
}

func TestReorgParallelDrain_RunsOnEveryCoreWhenNothingConflicts(t *testing.T) {
	cfg := smallScenarioConfig()
	cfg.parallelThreshold = 1

	var modes []bool

	// the reference drains every queued transaction one at a time
	var sequential []bool

	requireReorgEquivalent(t, cfg, queuedDuringReorg(t, false, &sequential), queuedDuringReorg(t, true, &modes))

	// the head drain, then the drain of each of the two forward steps
	require.Equal(t, []bool{true, true, true}, modes)
	require.Equal(t, []bool{false, false, false}, sequential)
}

// A drain is skipped by a conflict in the order of the queue: a tx is dropped when an earlier queued
// tx was, which the parallel steps do not keep. The head drain has no filter, so it still runs on
// every core; a step that has conflicting or losing hashes does not.
func TestReorgParallelDrain_StaysSequentialInAStepWithConflicts(t *testing.T) {
	cfg := smallScenarioConfig()
	cfg.parallelThreshold = 1
	cfg.conflictsInWinner = 3

	var modes, sequential []bool

	requireReorgEquivalent(t, cfg, queuedDuringReorg(t, false, &sequential), queuedDuringReorg(t, true, &modes))

	require.Len(t, modes, 3)
	require.True(t, modes[0], "the head drain has no filter")
	require.Contains(t, modes[1:], false, "the step that applies the conflicting nodes drains sequentially: %v", modes)
}

// A conflict the move-back reversed reaches a forward step only through that step's own conflicting
// nodes. A reorg whose winners have none has nothing in play in any step, whatever the lost block did.
func TestReorgParallelDrain_ARevertedLostConflictAloneDoesNotBlockIt(t *testing.T) {
	cfg := smallScenarioConfig()
	cfg.parallelThreshold = 1
	cfg.conflictsInLost = 2

	var modes, sequential []bool

	requireReorgEquivalent(t, cfg, queuedDuringReorg(t, false, &sequential), queuedDuringReorg(t, true, &modes))

	require.Equal(t, []bool{true, true, true}, modes)
}

// BenchmarkReorgDrain times a fork reorg with half a million transactions queued when it starts (issue
// 2269, PR 9), with the drains on one goroutine and on every core. head_drain_ms in the report is the
// drain of the queue before the move-back; the queueing itself is in the iteration's time, not in the
// phase. Without a queue the drains cost nothing and the two runs are the same.
func BenchmarkReorgDrain(b *testing.B) {
	for _, parallel := range []bool{false, true} {
		b.Run(fmt.Sprintf("parallel=%v", parallel), func(b *testing.B) {
			benchmarkReorgPath(b, reorgPathBenchConfig(256<<10, 0.3, false), func(t testing.TB, sc *reorgScenario) error {
				prev := reorgParallelDrain
				defer func() { reorgParallelDrain = prev }()

				reorgParallelDrain = parallel

				queueIntake(sc, 0, 512<<10)

				return sc.run(t)
			})
		})
	}
}
