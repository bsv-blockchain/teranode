package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const truncatedBodyHeight = uint32(100)

// buildTruncatedBody returns a DECODED block whose header commits an honest two-transaction body but
// whose delivered body carries no subtrees and claims one transaction. It is round-tripped through
// the real encoder and decoder on purpose: the transaction count, the size and the subtree count are
// three independent untrusted varints on the wire, so an in-memory-only fixture would prove the
// check but not that it covers the actual ingress shape.
//
// txCount is the forged count the tampered body carries. Pass 1 for the primary case: the evasion a
// TransactionCount-based rejection would miss, so the merkle bind is the only thing that can catch
// it. Any other value is rejected by the SAME merkle clause, not by the transaction-count one —
// this fixture's header still commits the honest two-transaction body, so
// CheckCoinbaseOnlyBodyBound returns on the root mismatch before the count is ever read. The
// TransactionCount > 1 branch is unreachable through this shape and is pinned directly in
// model/block_coinbase_only_binding_test.go, which supplies a root that matches the coinbase txid.
func buildTruncatedBody(t *testing.T, tSettings *settings.Settings, txCount uint64) *model.Block {
	t.Helper()

	coinbaseTx := coinbaseAtHeight(t, truncatedBodyHeight)

	// The honest body: one subtree holding the coinbase placeholder plus one other transaction. The
	// header commits that body, so its merkle root is NOT the coinbase txid.
	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(chainhash.Hash{0xAB}, 100, 0))

	replicated := subtree.Duplicate()
	replicated.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec
	merkleRoot := replicated.RootHash()

	hdr := minedBIP34Header(t, 4, tSettings.ChainCfgParams.GenesisHash, merkleRoot)

	honest, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{subtree.RootHash()},
		uint64(subtree.Length()), uint64(coinbaseTx.Size()), truncatedBodyHeight, 0) //nolint:gosec
	require.NoError(t, err)

	// The tampered delivery: same header, so the same hash and the same proof of work, with the
	// subtree list emptied and the transaction count forged.
	tampered, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, txCount, honest.SizeInBytes, honest.Height, 0)
	require.NoError(t, err)

	raw, err := tampered.Bytes()
	require.NoError(t, err)

	decoded, err := model.NewBlockFromBytes(raw)
	require.NoError(t, err, "the wire decoder accepts an emptied subtree list — that is the premise of this test")
	require.Empty(t, decoded.Subtrees, "the delivered body carries no subtrees")
	require.True(t, decoded.Header.HashMerkleRoot.IsEqual(honest.Header.HashMerkleRoot),
		"the header still commits the honest two-transaction body")
	require.True(t, decoded.Hash().IsEqual(honest.Hash()), "the tampering does not change the block hash")

	// The height survives the round trip (Block.Bytes writes it, readBlockFromReader reads it back),
	// and quick validation reads it for the version floor and the below-checkpoint decision — so
	// assert the premise rather than re-assigning it. If the wire format ever stops carrying the
	// height, this reddens here instead of silently sending a height-0 block down the genesis
	// exemption.
	require.Equal(t, truncatedBodyHeight, decoded.Height, "the decoded body must carry the honest height")

	return decoded
}

// TestQuickValidate_CoinbaseOnlyBodyBinding pins the bitcoin-sv/teranode#4692 gap on the quick path:
// model.Block.Valid binds a no-subtrees body to its header — for a single-transaction block the
// merkle root IS the coinbase txid — but quick validation never calls Valid, and its whole subtree
// stage is inside `if len(block.Subtrees) > 0`. So an honest multi-transaction header hash served
// with its subtree list emptied and any coinbase the peer likes used to reach commitBlock and be
// AddBlock'd with subtrees_set and mined_set, with nothing to revisit it — the failure this work
// exists to prevent, arrived at from the other side.
//
// Both entry points are covered deliberately. quickValidateBlockAsync is the catch-up path and is on
// by default (blockvalidation_catchup_allow_quick_validation); quickValidateBlock is a separate
// wired call site (Server.go, the unified below-checkpoint route). Patching one would leave the
// other open.
//
// Mutation proof: removing either CheckCoinbaseOnlyBodyBound call makes that entry point commit the
// tampered body, so that subtest's corrupt assertion and its "AddBlock not called" assertion both
// redden.
func TestQuickValidate_CoinbaseOnlyBodyBinding(t *testing.T) {
	t.Run("quickValidateBlock: emptied subtree list is corrupt, never committed", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		// Permissive, so a failure to reject would show up as a COMMIT rather than as a mock panic.
		suite.MockBlockchain.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(1), nil).Maybe()
		suite.MockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

		block := buildTruncatedBody(t, suite.Server.settings, 1)

		err := suite.Server.blockValidation.quickValidateBlock(suite.Ctx, block, "test", "")
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err), "an emptied subtree list is a corrupt body, got: %v", err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid), "the honest hash must never be condemned on an unbound body")
		require.Contains(t, err.Error(), "the header merkle root is not the coinbase txid",
			"the verdict must come from the binding, not from an incidental later check")

		suite.MockBlockchain.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("quickValidateBlockAsync: same body, same verdict", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()

		suite.MockBlockchain.On("AssignBlockID", mock.Anything, mock.Anything).Return(uint64(1), nil).Maybe()
		suite.MockBlockchain.On("AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

		block := buildTruncatedBody(t, suite.Server.settings, 1)

		// Buffered so the async path never blocks queuing write jobs; this body queues none.
		writeJobsChan := make(chan *SubtreeWriteJob, 16)

		_, _, err := suite.Server.blockValidation.quickValidateBlockAsync(suite.Ctx, block, "test", "", writeJobsChan)
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err), "the catch-up entry point must reject the same body, got: %v", err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid), "the honest hash must never be condemned on an unbound body")

		suite.MockBlockchain.AssertNotCalled(t, "AddBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

}
