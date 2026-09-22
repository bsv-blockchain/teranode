package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/settings"
	blockchainoptions "github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// storeCachedHeadersParent writes a coinbase-only parent at height 1 and returns it, either accepted
// or condemned. An accepted parent is stored mined/subtrees-set, which is the state the accept path
// leaves behind and what the child's wait for previous blocks reads.
func storeCachedHeadersParent(ctx context.Context, t *testing.T, client blockchain.ClientI, tSettings *settings.Settings, invalid bool) *model.Block {
	t.Helper()

	coinbaseTx := coinbaseAtHeight(t, 1)
	hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(),
		nBitsFrom(t, "207fffff"), uint32(time.Now().Unix())) //nolint:gosec

	parent, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), 1, 0)
	require.NoError(t, err)

	opts := []blockchainoptions.StoreBlockOption{
		blockchainoptions.WithMinedSet(true),
		blockchainoptions.WithSubtreesSet(true),
	}
	if invalid {
		opts = append(opts, blockchainoptions.WithInvalid(true))
	}

	require.NoError(t, client.AddBlock(ctx, parent, "test", opts...))

	_, meta, err := client.GetBlockHeader(ctx, parent.Hash())
	require.NoError(t, err)
	require.Equal(t, invalid, meta.Invalid, "fixture precondition: the parent's stored verdict")

	return parent
}

// TestValidateBlock_CachedHeaders_ParentStoredBeforeHoistedNBitsCheck is the condition the second
// hoist rests on (bitcoin-sv/teranode#4844).
//
// Hoisting the expected-nBits check above the parent-invalid check also places its store-backed
// GetNextWorkRequired ahead of the wait for previous blocks, on the catch-up branch whose headers
// come from opts.CachedHeaders rather than from the store. That is only safe because sequential
// catch-up has already stored the candidate's parent header row by the time the check runs. The
// other tests in this package exercise the fetched-headers branch and would not catch a regression
// here, so this locks the cross-component ordering contract.
func TestValidateBlock_CachedHeaders_ParentStoredBeforeHoistedNBitsCheck(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false
	tSettings.ChainCfgParams.Checkpoints = nil

	bv, client := newNoPersistHarness(ctx, t, tSettings)

	parent := storeCachedHeadersParent(ctx, t, client, tSettings, false)

	timestamp := uint32(time.Now().Unix()) //nolint:gosec

	expected, err := client.GetNextWorkRequired(ctx, parent.Hash(), int64(timestamp))
	require.NoError(t, err)
	require.NotNil(t, expected)

	coinbaseTx := coinbaseAtHeight(t, 2)
	hdr := minedHeaderWithBits(t, parent.Hash(), coinbaseTx.TxIDChainHash(), *expected, timestamp)

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), 2, 0)
	require.NoError(t, err)

	// Mirrors catchup's validateBlocksOnChannel: cached headers plus optimistic mining forced off.
	err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{
		CachedHeaders:           []*model.BlockHeader{parent.Header},
		DisableOptimisticMining: true,
		IsCatchupMode:           true,
	})
	require.NoError(t, err, "the hoisted expected-nBits lookup must find the parent's stored row on the cached-headers branch")
}

// TestValidateBlock_CachedHeaders_CorrectNBitsChildOfInvalidParent_StillParentInvalid proves the
// second hoist did not swallow the verdict it reorders around: a child whose difficulty bits are
// correct still reaches the parent-invalid check on the cached-headers branch, and still keeps its
// real body there (bitcoin-sv/teranode#4844).
func TestValidateBlock_CachedHeaders_CorrectNBitsChildOfInvalidParent_StillParentInvalid(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false
	tSettings.ChainCfgParams.Checkpoints = nil

	bv, client := newNoPersistHarness(ctx, t, tSettings)

	parent := storeCachedHeadersParent(ctx, t, client, tSettings, true)

	timestamp := uint32(time.Now().Unix()) //nolint:gosec

	expected, err := client.GetNextWorkRequired(ctx, parent.Hash(), int64(timestamp))
	require.NoError(t, err)
	require.NotNil(t, expected)

	coinbaseTx := coinbaseAtHeight(t, 2)
	hdr := minedHeaderWithBits(t, parent.Hash(), coinbaseTx.TxIDChainHash(), *expected, timestamp)

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), 2, 0)
	require.NoError(t, err)

	err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{
		CachedHeaders:           []*model.BlockHeader{parent.Header},
		DisableOptimisticMining: true,
		IsCatchupMode:           true,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid))
	require.ErrorContains(t, err, "parent block is invalid")

	exists, err := client.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)
	require.True(t, exists, "the reversible parent-invalid verdict keeps its record")

	storedChild, err := client.GetBlock(ctx, block.Hash())
	require.NoError(t, err)
	require.Equal(t, coinbaseTx.TxIDChainHash().String(), storedChild.CoinbaseTx.TxIDChainHash().String(),
		"the record must hold the real body so it can be reconsidered")
}

// TestValidateBlock_CachedHeaders_WrongNBitsChild_RejectedOnNBitsNotPersisted is the difficulty-1
// spam case on the cached-headers branch: the hoisted expected-nBits gate rejects it and nothing is
// written, so the parent-invalid site is not reachable at that price
// (bitcoin-sv/teranode#4844).
func TestValidateBlock_CachedHeaders_WrongNBitsChild_RejectedOnNBitsNotPersisted(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false
	tSettings.ChainCfgParams.Checkpoints = nil

	bv, client := newNoPersistHarness(ctx, t, tSettings)

	parent := storeCachedHeadersParent(ctx, t, client, tSettings, true)

	timestamp := uint32(time.Now().Unix()) //nolint:gosec

	expected, err := client.GetNextWorkRequired(ctx, parent.Hash(), int64(timestamp))
	require.NoError(t, err)
	require.NotNil(t, expected)

	wrongBits := nBitsFrom(t, "1f7fffff")
	require.NotEqual(t, expected.String(), wrongBits.String(),
		"fixture precondition: the declared bits must differ from the expected bits")

	coinbaseTx := canaryCoinbaseAtHeight(t, 2)
	hdr := minedHeaderWithBits(t, parent.Hash(), coinbaseTx.TxIDChainHash(), wrongBits, timestamp)

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), 2, 0)
	require.NoError(t, err)

	err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{
		CachedHeaders:           []*model.BlockHeader{parent.Header},
		DisableOptimisticMining: true,
		IsCatchupMode:           true,
	})
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrBlockInvalid))
	require.ErrorContains(t, err, "incorrect difficulty bits")
	require.NotContains(t, err.Error(), "parent block is invalid")

	requireNothingPersisted(ctx, t, client, block.Hash())
}
