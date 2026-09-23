package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// parentInvalidFixture is a harness whose store holds a parent that was accepted and then
// condemned, plus the difficulty bits and timestamp a child of it must carry to clear the hoisted
// expected-nBits gate and genuinely reach the parent-invalid check.
type parentInvalidFixture struct {
	bv        *BlockValidation
	client    blockchain.ClientI
	subtrees  *countingSubtreeValidationClient
	parent    *model.Block
	childBits model.NBit
	childTime uint32
}

func newParentInvalidFixture(ctx context.Context, t *testing.T) *parentInvalidFixture {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false
	tSettings.ChainCfgParams.Checkpoints = nil

	bv, client, subtrees := newNoPersistHarnessWithSubtreeClient(ctx, t, tSettings)

	parentCoinbase := coinbaseAtHeight(t, 1)
	parentHdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, parentCoinbase.TxIDChainHash(),
		nBitsFrom(t, "207fffff"), uint32(time.Now().Unix())) //nolint:gosec

	parent, err := model.NewBlock(parentHdr, parentCoinbase, []*chainhash.Hash{}, 1, uint64(parentCoinbase.Size()), 1, 0)
	require.NoError(t, err)

	require.NoError(t, client.AddBlock(ctx, parent, "test",
		blockchainoptions.WithMinedSet(true), blockchainoptions.WithSubtreesSet(true)))

	_, err = client.InvalidateBlock(ctx, parent.Hash())
	require.NoError(t, err)

	childTime := uint32(time.Now().Unix()) //nolint:gosec

	expected, err := client.GetNextWorkRequired(ctx, parent.Hash(), int64(childTime))
	require.NoError(t, err)
	require.NotNil(t, expected)

	return &parentInvalidFixture{
		bv:        bv,
		client:    client,
		subtrees:  subtrees,
		parent:    parent,
		childBits: *expected,
		childTime: childTime,
	}
}

// unboundChild builds a child of the condemned parent whose header merkle root is NOT its coinbase
// txid, carrying the given subtree list, so nothing binds the body to the header.
func (f *parentInvalidFixture) unboundChild(t *testing.T, subtrees []*chainhash.Hash, txCount uint64) *model.Block {
	t.Helper()

	coinbaseTx := canaryCoinbaseAtHeight(t, 2)
	unboundRoot := chainhash.Hash{0xCD}
	hdr := minedHeaderWithBits(t, f.parent.Hash(), &unboundRoot, f.childBits, f.childTime)

	child, err := model.NewBlock(hdr, coinbaseTx, subtrees, txCount, uint64(coinbaseTx.Size()), 2, 0)
	require.NoError(t, err)

	return child
}

// requireParentInvalidWithoutRow asserts the child got the parent-invalid verdict and left no row.
func requireParentInvalidWithoutRow(ctx context.Context, t *testing.T, client blockchain.ClientI, child *model.Block, err error) {
	t.Helper()

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid))
	require.ErrorContains(t, err, "parent block is invalid")

	exists, existsErr := client.GetBlockExists(ctx, child.Hash())
	require.NoError(t, existsErr)
	require.False(t, exists, "a parent-invalid verdict on a body not bound to its header must persist nothing")
}

// TestValidateBlock_ParentInvalid_UnboundBodyPersistsNothing is a regression for
// bitcoin-sv/teranode#4844: the parent-invalid branch used to store whatever body the peer
// delivered, so replaying an honest header under a condemned parent let the peer persist its own
// coinbase without any proof of work. The record is now written only for a body bound to its
// header (a coinbase-only body whose merkle root is its coinbase txid); the verdict itself is
// unchanged.
func TestValidateBlock_ParentInvalid_UnboundBodyPersistsNothing(t *testing.T) {
	initPrometheusMetrics()

	t.Run("subtree-less body whose merkle root is not its coinbase txid", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		f := newParentInvalidFixture(ctx, t)
		child := f.unboundChild(t, []*chainhash.Hash{}, 1)

		err := f.bv.ValidateBlockWithOptions(ctx, child, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		requireParentInvalidWithoutRow(ctx, t, f.client, child, err)
	})

	t.Run("body carrying a subtree list", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		f := newParentInvalidFixture(ctx, t)
		child := f.unboundChild(t, []*chainhash.Hash{{0x01}}, 2)

		err := f.bv.ValidateBlockWithOptions(ctx, child, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		requireParentInvalidWithoutRow(ctx, t, f.client, child, err)

		require.Equal(t, int32(0), f.subtrees.checkBlockSubtreesCalls.Load(),
			"the parent-invalid verdict precedes subtree validation, so no subtree may be fetched for it")
	})

	// Pins the tradeoff of writing no row for the child: its descendants do not get the cheap
	// parent-invalid verdict, because checkParentInvalid sees only the immediate parent's metadata
	// and that parent is now unknown. The validator itself does not reject the grandchild as
	// invalid; the chain-level rejection is catch-up's invalid-common-ancestor check, which the
	// direct path reaches by queueing catch-up for a block whose parent is missing.
	t.Run("grandchild of an unstored parent-invalid child", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		f := newParentInvalidFixture(ctx, t)
		child := f.unboundChild(t, []*chainhash.Hash{{0x01}}, 2)

		err := f.bv.ValidateBlockWithOptions(ctx, child, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		requireParentInvalidWithoutRow(ctx, t, f.client, child, err)

		grandchildCoinbase := coinbaseAtHeight(t, 3)
		grandchildHdr := minedHeaderWithBits(t, child.Hash(), grandchildCoinbase.TxIDChainHash(), f.childBits, f.childTime)

		grandchild, err := model.NewBlock(grandchildHdr, grandchildCoinbase, []*chainhash.Hash{}, 1, uint64(grandchildCoinbase.Size()), 3, 0)
		require.NoError(t, err)

		err = f.bv.ValidateBlockWithOptions(ctx, grandchild, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid),
			"the grandchild's parent is unknown, so it cannot inherit the parent-invalid verdict here")
		require.True(t, errors.Is(err, errors.ErrServiceError),
			"with the parent unknown, the expected-difficulty lookup fails before any verdict: %v", err)
		require.ErrorContains(t, err, "failed to get expected work required")

		for _, hash := range []*chainhash.Hash{grandchild.Hash(), child.Hash()} {
			exists, existsErr := f.client.GetBlockExists(ctx, hash)
			require.NoError(t, existsErr)
			require.False(t, exists, "neither the child nor its descendant may leave a row")
		}
	})
}
