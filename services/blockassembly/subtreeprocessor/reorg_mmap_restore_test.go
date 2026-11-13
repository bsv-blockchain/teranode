package subtreeprocessor

// With mmap-backed subtrees (blockassembly_subtreeMmapDir), a failed block
// movement must never put back a subtree it has already closed: closing unmaps
// the nodes, and the next read of them faults and kills the process. The
// failing movement therefore runs in a child process, which reads every node of
// the subtrees it is left with, and the parent checks that the child survived.

import (
	"context"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/require"
)

const (
	mmapRestoreChildEnv = "REORG_MMAP_RESTORE_CHILD"
	mmapRestoreSurvived = "MMAP RESTORE CHILD SURVIVED"
)

// mmapRestoreCases each fail a block movement on an mmap-backed processor.
var mmapRestoreCases = map[string]func(t *testing.T, f *stepRestoreFixture) error{
	// The move-back replaced the pre-reorg subtrees, then the first forward step
	// fails before its reset.
	"fork_step_boundary": func(t *testing.T, f *stepRestoreFixture) error {
		f.atStep(f.parent, func(_ *SubtreeProcessor) {
			require.NoError(t, f.blob.Del(context.Background(), f.fSubtreeKey[:], fileformat.FileTypeSubtree))
		})

		return f.stp.Reorg([]*model.Block{f.blockA}, []*model.Block{f.blockF, f.blockF2})
	},
	// The first forward step's reset replaced the move-back's subtrees, and the
	// step puts them back.
	"fork_mid_step": func(t *testing.T, f *stepRestoreFixture) error {
		f.atStep(f.parent, func(stp *SubtreeProcessor) { failSequentialRemainderOn(t, stp, f.a1) })

		return f.stp.Reorg([]*model.Block{f.blockA}, []*model.Block{f.blockF, f.blockF2})
	},
	// The move-back's bulk build fails after replacing the pre-reorg subtrees,
	// and the move-back puts them back.
	"fork_move_back_build": func(_ *testing.T, f *stepRestoreFixture) error {
		return f.stp.Reorg([]*model.Block{f.blockABadBuild}, []*model.Block{f.blockF, f.blockF2})
	},
	// A single block outside a reorg fails after its reset replaced the subtrees,
	// and its dispatcher rollback puts them back. m1 is the only transaction in
	// the remainder, so its missing map entry fails the remainder pass.
	"single_block_mid_step": func(t *testing.T, f *stepRestoreFixture) error {
		require.True(t, f.stp.GetCurrentTxMap().Delete(f.m1))

		return f.stp.MoveForwardBlock(f.blockA2)
	},
	// A2 commits, A3 fails, and the catch-up's rollback puts back the subtrees
	// from before the catch-up.
	"catch_up_second_block": func(t *testing.T, f *stepRestoreFixture) error {
		f.atStep(f.childA2, func(_ *SubtreeProcessor) {
			require.NoError(t, f.blob.Del(context.Background(), f.a3SubtreeKey[:], fileformat.FileTypeSubtree))
		})

		return f.stp.Reorg([]*model.Block{}, []*model.Block{f.blockA2, f.blockA3})
	},
}

func TestFailedBlockMovement_NeverReinstallsAnUnmappedSubtree(t *testing.T) {
	if name := os.Getenv(mmapRestoreChildEnv); name != "" {
		runMmapRestoreChild(t, name)
		return
	}

	for name := range mmapRestoreCases {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestFailedBlockMovement_NeverReinstallsAnUnmappedSubtree$", "-test.v", "-test.count=1")
			cmd.Env = append(os.Environ(), mmapRestoreChildEnv+"="+name)

			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "the child died reading the subtrees the failed movement left:\n%s", lastLines(string(out), 30))
			require.Contains(t, string(out), mmapRestoreSurvived)
		})
	}
}

func runMmapRestoreChild(t *testing.T, name string) {
	run, ok := mmapRestoreCases[name]
	require.True(t, ok, "unknown case %s", name)

	f := newStepRestoreFixture(t, stepRestoreOptions{opts: []Options{WithMmapDir(t.TempDir())}})
	require.True(t, f.stp.currentSubtree.Load().IsMmapBacked(), "the fixture must use mmap subtrees")

	require.Error(t, run(t, f), "the movement must fail")

	// Read every node of the installed subtrees, as the reset's sweep and the
	// dequeue path would, more than once to give a fault every chance to land.
	var sum byte

	for range 3 {
		for _, h := range f.stp.GetTransactionHashes(context.Background()) {
			sum ^= h[0]
		}
	}

	held := f.assemblyHashes(t)
	require.Contains(t, held, f.m1)

	if strings.HasPrefix(name, "fork_") {
		// the reset that follows a failed fork reorg sweeps the same subtrees
		swept := f.sweepAfterFailure(t)
		require.Contains(t, swept, f.m1)
	}

	t.Logf("%s %s (checksum %d, held %d)", mmapRestoreSurvived, name, sum, len(held))
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}

	return strings.Join(lines, "\n")
}

// installedMmapSubtrees returns the chained and current subtrees the processor
// holds. Called between movements, while the processor goroutine is idle.
func installedMmapSubtrees(stp *SubtreeProcessor) []*subtreepkg.Subtree {
	installed := slices.Clone(stp.chainedSubtrees)
	return append(installed, stp.currentSubtree.Load())
}

func requireRetiredOnce(t *testing.T, stp *SubtreeProcessor) {
	t.Helper()

	seen := make(map[*subtreepkg.Subtree]struct{}, len(stp.retiredSubtrees))
	for _, st := range stp.retiredSubtrees {
		_, dup := seen[st]
		require.False(t, dup, "a retired subtree is listed twice, so closeRetiredSubtrees would close it twice")
		seen[st] = struct{}{}
	}
}

// A failed block movement puts back subtrees that are still retired. The next
// movement retires them again, and the list must still hold each one once, or
// the commit that follows closes the same subtree twice. Failing the same block
// twice stops before any commit closes the list, so the list can be read.
func TestFailedBlockMovement_RetiresAPutBackSubtreeOnce(t *testing.T) {
	f := newStepRestoreFixture(t, stepRestoreOptions{opts: []Options{WithMmapDir(t.TempDir())}})
	require.True(t, f.stp.currentSubtree.Load().IsMmapBacked(), "the fixture must use mmap subtrees")

	// m1 is the only transaction in the remainder, so its missing map entry fails
	// the remainder pass after the block's subtree reset, every time.
	require.True(t, f.stp.GetCurrentTxMap().Delete(f.m1))

	require.Error(t, f.stp.MoveForwardBlock(f.blockA2))

	putBack := installedMmapSubtrees(f.stp)
	require.True(t, slices.Contains(f.stp.retiredSubtrees, putBack[len(putBack)-1]), "precondition: the put-back current subtree is still retired")

	require.Error(t, f.stp.MoveForwardBlock(f.blockA2))
	requireRetiredOnce(t, f.stp)
}

// The reset that must follow a failed fork reorg replaces subtrees the failed
// step put back while they were still retired. It must close them through the
// retired list, not directly, so no closed subtree is left on the list for a
// later closeRetiredSubtrees to close again.
func TestFailedForkReorg_ResetLeavesNoReplacedSubtreeRetired(t *testing.T) {
	f := newStepRestoreFixture(t, stepRestoreOptions{opts: []Options{WithMmapDir(t.TempDir())}})
	require.True(t, f.stp.currentSubtree.Load().IsMmapBacked(), "the fixture must use mmap subtrees")

	f.atStep(f.parent, func(stp *SubtreeProcessor) { failSequentialRemainderOn(t, stp, f.a1) })
	require.Error(t, f.stp.Reorg([]*model.Block{f.blockA}, []*model.Block{f.blockF, f.blockF2}))

	putBack := installedMmapSubtrees(f.stp)
	require.True(t, slices.Contains(f.stp.retiredSubtrees, putBack[len(putBack)-1]), "precondition: the put-back current subtree is still retired")

	f.sweepAfterFailure(t)

	requireRetiredOnce(t, f.stp)

	for _, st := range putBack {
		// compared by pointer: require.NotContains compares contents, and reading a
		// closed subtree's nodes faults
		require.False(t, slices.Contains(f.stp.retiredSubtrees, st), "the reset closed this subtree, so it must not stay retired")
	}
}

// unheldRetiredAtEveryStep records, at the top of every block movement step, how
// many retired subtrees nothing holds: not installed, not in the mining snapshot
// and not in keep. Those are the ones the previous step should already have
// closed, so with mmap they sit on the mmap volume for no reader.
func unheldRetiredAtEveryStep(f *stepRestoreFixture, keep []*subtreepkg.Subtree) *[]int {
	counts := &[]int{}

	hook := func(_ *model.BlockHeader) {
		stp := f.stp

		held := make(map[*subtreepkg.Subtree]struct{})
		for _, st := range append(installedMmapSubtrees(stp), keep...) {
			held[st] = struct{}{}
		}

		if data := stp.precomputedMiningData.Load(); data != nil {
			for _, st := range data.Subtrees {
				held[st] = struct{}{}
			}
		}

		unheld := 0

		for _, st := range stp.retiredSubtrees {
			if _, ok := held[st]; !ok {
				unheld++
			}
		}

		*counts = append(*counts, unheld)
	}

	f.hook.Store(&hook)

	return counts
}

// A multi-block movement closes the subtrees each step replaced once nothing can
// put them back, as a single block does at its commit, so the mmap volume holds
// one step's worth of replaced subtrees and not one per block. A catch-up keeps
// only the subtrees from before it, which its rollback puts back.
func TestBlockMovement_ClosesEachStepsReplacedSubtrees(t *testing.T) {
	t.Run("catch-up", func(t *testing.T) {
		f := newStepRestoreFixture(t, stepRestoreOptions{opts: []Options{WithMmapDir(t.TempDir())}})
		require.True(t, f.stp.currentSubtree.Load().IsMmapBacked(), "the fixture must use mmap subtrees")

		counts := unheldRetiredAtEveryStep(f, installedMmapSubtrees(f.stp))

		require.NoError(t, f.stp.Reorg([]*model.Block{}, []*model.Block{f.blockA2, f.blockA3, f.blockA4}))
		require.Equal(t, f.childA4.Hash(), f.stp.GetCurrentBlockHeader().Hash())

		require.GreaterOrEqual(t, len(*counts), 3, "precondition: the hook ran at each catch-up block")
		require.Equal(t, make([]int, len(*counts)), *counts, "each catch-up block closes the subtrees it replaced at its commit")
		require.Empty(t, f.stp.retiredSubtrees, "the catch-up's end closes the subtrees from before it")
	})

	t.Run("move-back", func(t *testing.T) {
		f := newStepRestoreFixture(t, stepRestoreOptions{opts: []Options{WithMmapDir(t.TempDir())}})
		require.True(t, f.stp.currentSubtree.Load().IsMmapBacked(), "the fixture must use mmap subtrees")

		require.NoError(t, f.stp.Reorg([]*model.Block{}, []*model.Block{f.blockA2, f.blockA3}))

		counts := unheldRetiredAtEveryStep(f, nil)

		require.NoError(t, f.stp.Reorg([]*model.Block{f.blockA3, f.blockA2, f.blockA}, []*model.Block{f.blockF, f.blockF2}))
		require.Equal(t, f.forkF2.Hash(), f.stp.GetCurrentBlockHeader().Hash())

		require.GreaterOrEqual(t, len(*counts), 5, "precondition: the hook ran at three move-back and two forward steps")
		require.Equal(t, make([]int, len(*counts)), *counts, "each move-back step closes the subtrees it replaced")
	})
}

// newChainedMmapFixture is the step-restore fixture with mmap subtrees and two
// complete chained subtrees, which the mining snapshot holds.
func newChainedMmapFixture(t *testing.T) (*stepRestoreFixture, []*subtreepkg.Subtree) {
	t.Helper()

	f := newStepRestoreFixture(t, stepRestoreOptions{
		fillers:  6,
		opts:     []Options{WithMmapDir(t.TempDir())},
		settings: func(s *settings.Settings) { s.BlockAssembly.InitialMerkleItemsPerSubtree = 4 },
	})
	require.True(t, f.stp.currentSubtree.Load().IsMmapBacked(), "the fixture must use mmap subtrees")

	// AddDirectly does not refresh the snapshot; a move-forward-free reorg is not
	// available, so refresh it the way the processor does after a block.
	f.stp.updatePrecomputedMiningData()

	snapshot := f.stp.precomputedMiningData.Load()
	require.NotNil(t, snapshot)
	require.Len(t, snapshot.Subtrees, 2, "precondition: the mining snapshot holds the two chained subtrees")

	return f, slices.Clone(snapshot.Subtrees)
}

// A reset replaces the subtrees the mining snapshot points at and closes the
// retired ones before it refreshes the snapshot, while a mining candidate may
// still read them. The snapshot's subtrees must stay mapped until that refresh.
func TestReset_KeepsTheSnapshotsSubtreesMappedUntilItRefreshesIt(t *testing.T) {
	f, snapshotSubtrees := newChainedMmapFixture(t)

	var (
		fired     bool
		stillHeld []bool
	)

	hook := func(_ *model.BlockHeader) {
		if fired {
			return
		}

		fired = true

		for _, st := range snapshotSubtrees {
			stillHeld = append(stillHeld, slices.Contains(f.stp.retiredSubtrees, st))
		}
	}
	f.hook.Store(&hook)

	resp := f.stp.Reset(f.childA2, []*model.Block{}, []*model.Block{f.blockA2}, false, nil)
	require.NoError(t, resp.Err)

	require.True(t, fired, "precondition: the hook ran inside the reset, after its first close")
	require.Equal(t, []bool{true, true}, stillHeld, "the snapshot's subtrees stay retired, not closed, until the snapshot moves off them")
}

// Stop closes every retired subtree, including those the mining snapshot still
// points at, so no mmap file is left behind.
func TestStop_ClosesRetiredSubtreesTheSnapshotHolds(t *testing.T) {
	mmapDir := t.TempDir()

	f := newStepRestoreFixture(t, stepRestoreOptions{
		fillers:  6,
		opts:     []Options{WithMmapDir(mmapDir)},
		settings: func(s *settings.Settings) { s.BlockAssembly.InitialMerkleItemsPerSubtree = 4 },
	})
	f.stp.updatePrecomputedMiningData()

	snapshot := f.stp.precomputedMiningData.Load()
	require.NotNil(t, snapshot)
	require.Len(t, snapshot.Subtrees, 2, "precondition: the mining snapshot holds the two chained subtrees")

	// The state a block's reset leaves before the snapshot is refreshed: the
	// chained subtrees replaced and retired, and the snapshot still pointing at
	// them, so closeRetiredSubtrees keeps them. Set directly while the processor
	// goroutine is idle.
	f.stp.retireChainedSubtrees()
	f.stp.closeRetiredSubtrees()

	for _, st := range snapshot.Subtrees {
		require.True(t, slices.Contains(f.stp.retiredSubtrees, st), "precondition: a snapshot subtree is retired and was not closed")
	}

	entries, err := os.ReadDir(mmapDir)
	require.NoError(t, err)
	require.NotEmpty(t, entries, "precondition: the subtrees have mmap files")

	f.stp.Stop(context.Background())

	entries, err = os.ReadDir(mmapDir)
	require.NoError(t, err)
	require.Empty(t, entries, "Stop removes every mmap file, the snapshot's included")
}
