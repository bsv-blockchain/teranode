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
