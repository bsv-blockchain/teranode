package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestValidateBlock_ParentInvalid_ChildKeepsFullBodyAndReconsiders is a POSITIVE CONTROL, not a
// regression test: it passes on the base revision, because it asserts the behaviour this change set
// out to preserve (bitcoin-sv/teranode#4844).
//
// It is kept because it guards a branch this change rewrote twice: the parent-invalid site was
// moved below the hoisted expected-nBits check and merged from two call sites into one, while the
// rule the rest of this change applies — never persist a body that is not bound to its header —
// would, over-applied, remove exactly this write.
//
// Parent-invalid is an inherited and REVERSIBLE verdict: reconsidering the parent can make the
// child valid again, and RevalidateBlock reloads the child from the store. So this one site keeps
// writing the child's REAL body when that body is bound to its header (here a coinbase-only body,
// bound by the coinbase-txid rule) — a header-only or placeholder record could never be
// reconsidered, which is why the quarantine-representation alternative was rejected. An unbound
// body gets the same verdict and no record (parent_invalid_unbound_body_test.go).
//
// The child is given the correct expected difficulty bits on purpose, so it clears the hoisted
// expected-nBits gate and genuinely reaches the parent-invalid check rather than being rejected
// above it.
func TestValidateBlock_ParentInvalid_ChildKeepsFullBodyAndReconsiders(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false
	tSettings.ChainCfgParams.Checkpoints = nil

	bv, client := newNoPersistHarness(ctx, t, tSettings)

	// A parent accepted on the chain, then condemned. Stored mined/subtrees-set because that is the
	// state a block reaches on the accept path, and the child's wait for previous blocks reads it.
	parentCoinbase := coinbaseAtHeight(t, 1)
	parentHdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, parentCoinbase.TxIDChainHash(),
		nBitsFrom(t, "207fffff"), uint32(time.Now().Unix())) //nolint:gosec

	parent, err := model.NewBlock(parentHdr, parentCoinbase, []*chainhash.Hash{}, 1, uint64(parentCoinbase.Size()), 1, 0)
	require.NoError(t, err)

	require.NoError(t, client.AddBlock(ctx, parent, "test",
		blockchainoptions.WithMinedSet(true), blockchainoptions.WithSubtreesSet(true)))

	_, err = client.InvalidateBlock(ctx, parent.Hash())
	require.NoError(t, err)

	// The child: a real body carrying the markup payload, with the difficulty bits the chain
	// expects, so the only thing wrong with it is its parent.
	childTimestamp := uint32(time.Now().Unix()) //nolint:gosec

	expected, err := client.GetNextWorkRequired(ctx, parent.Hash(), int64(childTimestamp))
	require.NoError(t, err)
	require.NotNil(t, expected)

	childCoinbase := canaryCoinbaseAtHeight(t, 2)
	childHdr := minedHeaderWithBits(t, parent.Hash(), childCoinbase.TxIDChainHash(), *expected, childTimestamp)

	child, err := model.NewBlock(childHdr, childCoinbase, []*chainhash.Hash{}, 1, uint64(childCoinbase.Size()), 2, 0)
	require.NoError(t, err)

	err = bv.ValidateBlockWithOptions(ctx, child, "http://localhost", &ValidateBlockOptions{PeerID: "peer-serving"})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid))
	require.ErrorContains(t, err, "parent block is invalid")

	// The record exists, carries the invalid verdict, and holds the REAL body.
	exists, err := client.GetBlockExists(ctx, child.Hash())
	require.NoError(t, err)
	require.True(t, exists, "a reversible parent-invalid verdict must be remembered")

	_, childMeta, err := client.GetBlockHeader(ctx, child.Hash())
	require.NoError(t, err)
	require.True(t, childMeta.Invalid)

	storedChild, err := client.GetBlock(ctx, child.Hash())
	require.NoError(t, err)
	require.NotNil(t, storedChild.CoinbaseTx)
	require.Equal(t, childCoinbase.TxIDChainHash().String(), storedChild.CoinbaseTx.TxIDChainHash().String(),
		"the stored record must hold the block's real coinbase, not a synthetic placeholder — RevalidateBlock reloads it from here")

	storedMiner, err := util.ExtractCoinbaseMinerRaw(storedChild.CoinbaseTx, false)
	require.NoError(t, err)
	require.Contains(t, storedMiner, minerMarkupCanary,
		"the real body is retained by design here, which is why the dashboard sink must escape it")

	// Reconsider the parent. RevalidateBlock clears mined_set, so re-stamp it the way the setMined
	// worker would; the child's validation waits on the parent's mined state.
	require.NoError(t, client.RevalidateBlock(ctx, parent.Hash()))
	require.NoError(t, client.SetBlockMinedSet(ctx, parent.Hash()))

	_, parentMeta, err := client.GetBlockHeader(ctx, parent.Hash())
	require.NoError(t, err)
	require.False(t, parentMeta.Invalid, "fixture precondition: the parent is reconsidered")

	// The reconsideration must be a fresh validation, not the earlier parent-invalid result replayed
	// from the once-per-block grace window.
	time.Sleep(2 * validationResultGrace)

	// And the child can now be reconsidered too — the end-to-end proof that keeping the real body
	// keeps the record recoverable.
	err = bv.ValidateBlockWithOptions(ctx, child, "http://localhost", &ValidateBlockOptions{
		IsRevalidation:          true,
		DisableOptimisticMining: true,
	})
	require.NoError(t, err, "the child must revalidate once its parent is reconsidered")

	_, childMeta, err = client.GetBlockHeader(ctx, child.Hash())
	require.NoError(t, err)
	require.False(t, childMeta.Invalid, "the reconsidered child must no longer be marked invalid")
}
