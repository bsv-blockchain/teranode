package bsv

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// TestBSVInvalidateBlock is the Teranode port of bitcoin-sv's invalidateblock.py.
//
// Upstream uses three nodes: node 0 mines 4 blocks, node 1 mines a competing 6,
// they are connected so node 0 reorgs onto the longer chain, and invalidating an
// early block of that chain must send node 0 back to its original tip. The
// three-node setup exists only to manufacture two competing chains; this port
// builds the competing chain locally with CreateTestBlock, which reproduces the
// assertions without depending on multi-node P2P sync (see the
// invalidblockrequest-port-red gap for why that matters).
//
// Reproduced from upstream:
//   - a longer competing chain causes a reorg onto it
//   - invalidating an early block of the active chain reorgs back to the
//     previously-best chain, at its original height and hash
//   - the invalidated branch is not reinstated afterwards
func TestBSVInvalidateBlock(t *testing.T) {
	td := wirepeer.NewLegacyDaemon(t)
	defer td.Stop(t)

	const (
		originalLen  = 4
		competingLen = 6
	)

	// The original chain: 4 blocks, whose tip we must be able to return to.
	td.MineBlocks(t, originalLen)

	originalHeight := bestHeight(t, td)
	originalHash := bestBlockHash(t, td)
	require.Equal(t, uint32(originalLen), originalHeight, "should have mined %d blocks", originalLen)

	// The competing chain: 6 blocks from genesis, so it outweighs the original
	// and the node must reorg onto it. Built one at a time because each block
	// needs its predecessor as parent.
	genesis, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, 0)
	require.NoError(t, err, "fetch genesis")

	competing := make([]*model.Block, 0, competingLen)
	parent := genesis

	for i := range competingLen {
		// Distinct nonces so the competing blocks cannot collide with the
		// original chain's blocks at the same heights.
		_, block := td.CreateTestBlock(t, parent, uint32(0xc0de0000+i))

		err = td.BlockValidationClient.ProcessBlock(td.Ctx, block, block.Height, "", "legacy", 0)
		require.NoError(t, err, "competing block at height %d should be accepted", block.Height)

		competing = append(competing, block)
		parent = block
	}

	// Upstream: after connecting, node 0's count is 6 - the reorg happened.
	require.Eventually(t, func() bool {
		return tryBestHeight(td) == uint32(len(competing))
	}, 30*time.Second, 250*time.Millisecond,
		"the node should reorg onto the longer competing chain; height is %d", bestHeight(t, td))

	competingTipHash := bestBlockHash(t, td)
	require.Equal(t, competing[len(competing)-1].Header.Hash().String(), competingTipHash,
		"the tip should be the competing chain's last block")

	// Upstream: invalidate block 2 of the competing chain and verify we reorg back
	// to the original chain's tip, at its original height and hash.
	badHash := competing[1].Header.Hash().String()

	_, err = td.CallRPC(td.Ctx, "invalidateblock", []any{badHash})
	require.NoError(t, err, "invalidateblock %s", badHash)

	require.Eventually(t, func() bool {
		return tryBestBlockHash(td) == originalHash
	}, 30*time.Second, 250*time.Millisecond,
		"invalidating an early block of the active chain should reorg back to the original tip; tip is %s at height %d",
		bestBlockHash(t, td), bestHeight(t, td))

	require.Equal(t, originalHeight, bestHeight(t, td),
		"the restored tip should be at the original height")

	// Upstream's second half checks the node does not later drift back onto the
	// invalidated chain. Assert it stays put rather than that it has not moved yet.
	require.Never(t, func() bool {
		return tryBestBlockHash(td) == competingTipHash
	}, 5*time.Second, 500*time.Millisecond, "the invalidated branch must not be reinstated")

	t.Logf("reorged to competing tip at height %d, then back to %s at height %d",
		len(competing), originalHash, originalHeight)
}
