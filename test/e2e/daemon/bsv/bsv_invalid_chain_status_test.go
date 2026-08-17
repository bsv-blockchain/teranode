package bsv

import (
	"testing"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

const (
	// invalidChainActiveLen and invalidChainForkLen shape the chain: an active
	// chain long enough that the fork never overtakes it, and a fork long enough
	// that invalidity has somewhere to propagate to. Upstream's pre-generated
	// chain is 12 blocks with forks of 5 and 20; the lengths do not matter here,
	// only that the fork stays a fork and has a tip distinct from the block that
	// is invalidated.
	invalidChainActiveLen = 6
	invalidChainForkFrom  = 2
	invalidChainForkLen   = 3
)

// TestBSVUpdateInvalidChainStatus ports
// bsv-update-invalid-chain-status-at-startup.py.
//
// Upstream copies a pre-generated bitcoind data directory into place - a chain
// built by node version 1.0.0, whose stored block statuses are wrong because of a
// historical bug that failed to mark fork blocks withFailedParent - starts the
// node, and checks that a repair pass at startup has turned two header-only fork
// tips into invalid ones. None of that can be set up here: Teranode stores its
// chain in SQLite and file-backed block and subtree stores, and cannot read a
// bitcoind data directory at all, so there is no legacy state to repair and no
// repair pass to test.
//
// What is portable is the invariant the repair exists to restore, and it is worth a
// test on its own: a fork descending from an invalid block must be reported as
// invalid, not as a merely-unfollowed branch. Teranode maintains it directly -
// invalidating a fork's base block flips the fork's TIP, three blocks above it, from
// valid-headers to invalid - so it never needs the repair upstream is checking for.
//
// Deliberately distinct from TestBSVGetChainTips, which covers the valid-fork case
// and asserts a non-active branch is not reported invalid. This is the other side:
// that a branch which SHOULD be invalid is reported so, and that invalidity reaches
// descendants rather than stopping at the block named.
func TestBSVUpdateInvalidChainStatus(t *testing.T) {
	// P2P is enabled for RPC speed; see the getpeerinfo-stalls-without-p2p-service
	// gap, which this exercise has now measured on two different RPCs.
	td := wirepeer.NewLegacyDaemonWithP2P(t)
	defer td.Stop(t)

	td.MineBlocks(t, invalidChainActiveLen)

	activeHash := bestBlockHash(t, td)

	fork := buildFork(t, td, invalidChainForkFrom, invalidChainForkLen)
	forkBase := fork[0]
	forkTip := fork[len(fork)-1]

	t.Run("a fork with no invalid ancestor is not reported invalid", func(t *testing.T) {
		// The precondition, and it is worth asserting rather than assuming: if the
		// fork were already reported invalid, the assertion after invalidation would
		// hold without invalidation having done anything.
		tip := tipWithHash(t, chainTips(t, td), forkTip.Header.Hash().String())

		require.NotEqual(t, "invalid", tip.Status,
			"a fork built from valid blocks must not be reported invalid before anything is invalidated")
		require.EqualValues(t, invalidChainForkLen, tip.BranchLen,
			"the fork's branchlen should be its divergence from the active chain")
	})

	t.Run("invalidating the fork's base makes the fork's tip invalid", func(t *testing.T) {
		// Upstream's T3 and T4: fork tips whose ancestor is invalid must read
		// invalid. Upstream gets there by loading a chain where that had been
		// recorded wrongly; this gets there by invalidating the ancestor directly.
		requireRPCOK(t, td, "invalidateblock", forkBase.Header.Hash().String())

		tip := tipWithHash(t, chainTips(t, td), forkTip.Header.Hash().String())

		require.Equal(t, "invalid", tip.Status,
			"invalidity must reach the fork's tip, %d blocks above the block invalidated",
			invalidChainForkLen-1)
	})

	t.Run("the active chain is untouched", func(t *testing.T) {
		// Upstream's T1: the active tip is unaffected by what happens on the forks.
		require.Equal(t, activeHash, bestBlockHash(t, td),
			"invalidating a block on a fork must not disturb the active chain")

		require.Equal(t, activeHash, activeTip(t, chainTips(t, td)).Hash,
			"getchaintips should still report the same active tip")
	})
}

// buildFork builds a chain of length blocks descending from the block at height
// from, and returns them in order.
//
// Submitted through the block validation client rather than over the wire because
// the fork must be built block by block on a parent of our choosing, which mining
// cannot do and which announcing cannot do either - see the
// headers-announcement-disconnects gap.
func buildFork(t *testing.T, td *daemon.TestDaemon, from uint32, length int) []*model.Block {
	t.Helper()

	parent, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, from)
	require.NoError(t, err, "fetch the fork point at height %d", from)

	blocks := make([]*model.Block, 0, length)

	for i := range length {
		// A distinct nonce prefix so these cannot collide with the active chain's
		// blocks at the same heights.
		_, block := td.CreateTestBlock(t, parent, uint32(0xfa0e0000+i))

		require.NoError(t, td.BlockValidationClient.ProcessBlock(td.Ctx, block, block.Height, "", "legacy", 0),
			"fork block %d at height %d should be accepted as a side chain", i, block.Height)

		blocks = append(blocks, block)
		parent = block
	}

	return blocks
}

// tipWithHash finds one tip by hash, failing if it is absent.
func tipWithHash(t *testing.T, tips []chainTip, hash string) chainTip {
	t.Helper()

	for _, tip := range tips {
		if tip.Hash == hash {
			return tip
		}
	}

	require.FailNow(t, "no tip with the expected hash", "wanted %s, got %+v", hash, tips)

	return chainTip{}
}
