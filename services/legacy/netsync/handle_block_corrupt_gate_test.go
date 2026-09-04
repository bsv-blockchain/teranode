package netsync

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/stretchr/testify/require"
)

// TestHandleBlockDirect_MerkleGatePremise guards the shared CheckMerkleRoot classification CONTRACT
// that the bitcoin-sv/teranode#4692 fix relies on (bitcoin-sv/teranode#4692): a body-derived merkle mismatch
// is BlockCorrupt, while an infrastructure failure (here a subtree-count mismatch) is a NON-corrupt
// storage error. Both the HandleBlockDirect gate and its already-tested sibling in
// quick_validate.go's validateSubtrees branch on exactly this classification; if it ever regressed
// (e.g. a count mismatch started returning corrupt) both gates would silently start blaming the peer
// for our own failures.
//
// NOT a revert guard for the HandleBlockDirect gate — and why (verified obstacle, see the report):
// the gate's non-corrupt else-branch is UNREACHABLE through the real unified route, so a test that
// fails when handle_block.go reverts to wrapping every error corrupt cannot be written without
// restructuring production. Concretely:
//   - the gate only runs when preparedSubtreeSlices != nil, i.e. the unified route, on the
//     in-memory slices createSubtrees built from the wire block;
//   - prepareSubtrees always returns len(subtrees) == len(subtreeSlices) (both = numSubtrees,
//     handle_block.go:566-575), so CheckMerkleRoot's count-mismatch StorageError (Block.go:1721) is
//     unreachable, and the slices are well-formed non-nil trees with a coinbase node at index 0, so
//     the ProcessingError paths (Block.go:1735/1742/1815/1823/1837) are unreachable too;
//   - with well-formed slices CheckMerkleRoot returns only success or a corrupt verdict, so the
//     gate's `if IsBlockCorrupt` is always true on error and the else-branch never executes;
//   - a failing subtree store fails earlier, inside prepareSubtrees.writeSubtree
//     (handle_block.go:551 -> HandleBlockDirect:217), BEFORE the gate, so it never exercises it.
//
// Injecting the malformed block CheckMerkleRoot needs to fail non-corruptly therefore requires an
// injection seam that HandleBlockDirect does not expose. The fix is still correct and worth keeping
// (defensive, and it makes this gate consistent with the reachable sibling in quick_validate.go);
// the gap is documented rather than papered over with a decorative test.
func TestHandleBlockDirect_MerkleGatePremise(t *testing.T) {
	ctx := context.Background()

	t.Run("body-derived merkle mismatch is corrupt (gate wraps corrupt, strikes peer)", func(t *testing.T) {
		st, err := subtreepkg.NewTreeByLeafCount(2)
		require.NoError(t, err)
		require.NoError(t, st.AddCoinbaseNode())
		require.NoError(t, st.AddNode(chainhash.Hash{0xAA}, 1, 100))

		coinbase, err := bt.NewTxFromString(model.CoinbaseHex)
		require.NoError(t, err)

		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{},
				HashMerkleRoot: &chainhash.Hash{}, // zeroed: cannot match the computed root -> corrupt
				Timestamp:      1,
			},
			CoinbaseTx:       coinbase,
			SubtreeSlices:    []*subtreepkg.Subtree{st},
			Subtrees:         []*chainhash.Hash{st.RootHash()},
			TransactionCount: 2,
		}

		err = block.CheckMerkleRoot(ctx)
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err),
			"a merkle mismatch is body-derived corrupt -> the gate wraps corrupt and strikes the serving peer")
	})

	t.Run("infrastructure error is NOT corrupt (gate returns it unwrapped, no strike)", func(t *testing.T) {
		mk := func(seed byte) *subtreepkg.Subtree {
			st, err := subtreepkg.NewTreeByLeafCount(2)
			require.NoError(t, err)
			require.NoError(t, st.AddNode(chainhash.Hash{seed}, 1, 0))

			return st
		}

		block := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				HashPrevBlock:  &chainhash.Hash{},
				HashMerkleRoot: &chainhash.Hash{},
				Timestamp:      1,
			},
			// Two loaded slices but only one Subtrees hash: a count mismatch CheckMerkleRoot treats as
			// a NON-corrupt storage error, not a peer fault.
			SubtreeSlices:    []*subtreepkg.Subtree{mk(0x01), mk(0x02)},
			Subtrees:         []*chainhash.Hash{{0x01}},
			TransactionCount: 2,
		}

		err := block.CheckMerkleRoot(ctx)
		require.Error(t, err)
		require.False(t, errors.IsBlockCorrupt(err),
			"an infrastructure error must NOT be corrupt -> the gate returns it unwrapped, never striking the peer for our failure")
	})
}
