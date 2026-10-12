package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestValidateBlock_OptimisticPeerBlocks_SubtreeLessUnboundBodyRejectedBeforeAdd is a regression for
// bitcoin-sv/teranode#4844. When an operator opts into optimistic mining for peer-served blocks, the
// received body is added to the chain BEFORE block.Valid runs. A body carrying no subtrees used to
// be added unbound — transiently visible as a valid chain tip — and then persisted as invalid with
// the peer's chosen coinbase. The coinbase-only binding is pure (header plus coinbase), so it now
// runs before the add: the body is rejected as corrupt, the serving peer is struck, and nothing is
// written.
//
// The remaining exposure under the opt-in is a body CARRYING subtrees, which cannot be bound until
// block.Valid is split so its integrity floor runs before the optimistic AddBlock. It is documented
// on the setting (settings/blockvalidation_settings.go, blockvalidation_optimistic_mining_peer_blocks,
// which defaults to false) and has no fixture here.
func TestValidateBlock_OptimisticPeerBlocks_SubtreeLessUnboundBodyRejectedBeforeAdd(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = true
	tSettings.BlockValidation.OptimisticMiningPeerBlocks = true
	tSettings.ChainCfgParams.Checkpoints = nil

	bv, client := newNoPersistHarness(ctx, t, tSettings)

	fake := &corruptStrikeP2PClient{}
	bv.p2pClient = fake

	const blockHeight = uint32(1)

	timestamp := uint32(time.Now().Unix()) //nolint:gosec

	expected, err := client.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
	require.NoError(t, err)
	require.NotNil(t, expected)

	// An unbound body: a single-transaction block whose header merkle root is NOT its coinbase
	// txid, so nothing reconciles the body to the header.
	coinbaseTx := canaryCoinbaseAtHeight(t, blockHeight)
	unboundRoot := chainhash.Hash{0xCD}
	hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, &unboundRoot, *expected, timestamp)

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), blockHeight, 0)
	require.NoError(t, err)

	err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{
		PeerID:                  "peer-serving",
		DisableOptimisticMining: optimisticMiningDisabledForPeerPath(tSettings, "http://localhost"),
	})
	require.Error(t, err)
	require.True(t, errors.IsBlockCorrupt(err), "an unbound subtree-less body must be rejected as corrupt, got: %v", err)

	exists, err := client.GetBlockExists(ctx, block.Hash())
	require.NoError(t, err)
	require.False(t, exists, "the unbound body must not be added to the chain")

	calls := fake.recorded()
	require.Len(t, calls, 1, "the serving peer must be struck exactly once")
	require.Equal(t, "peer-serving", calls[0].peerID)
}

// TestValidateBlockWithOptions_GlobalFlagAloneIsNotOptimistic is a regression for
// bitcoin-sv/teranode#4844: the optimistic seed in ValidateBlockWithOptions used to read the global
// flag alone, so a caller passing no override (the ValidateBlock wrapper, the revalidation worker)
// went optimistic on the default-true global flag even with the peer-blocks opt-in off. The seed now
// requires both flags.
//
// The block is bound (coinbase-only, merkle root is its coinbase txid) and its coinbase pays twice
// the subsidy, so it fails consensus after the binding. A synchronous ErrBlockInvalid is only
// possible on the non-optimistic branch; the optimistic branch adds it first and returns nil.
func TestValidateBlockWithOptions_GlobalFlagAloneIsNotOptimistic(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = true
	tSettings.BlockValidation.OptimisticMiningPeerBlocks = false
	tSettings.ChainCfgParams.Checkpoints = nil

	bv, client := newNoPersistHarness(ctx, t, tSettings)

	const blockHeight = uint32(1)

	timestamp := uint32(time.Now().Unix()) //nolint:gosec

	expected, err := client.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
	require.NoError(t, err)
	require.NotNil(t, expected)

	coinbaseTx := coinbaseAtHeight(t, blockHeight)
	// 50 BTC is the regtest subsidy at this height; pay twice that so the no-inflation check fails.
	coinbaseTx.Outputs[0].Satoshis = 2 * 50 * 100000000

	hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash(), *expected, timestamp)

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), blockHeight, 0)
	require.NoError(t, err)
	require.NoError(t, block.CheckCoinbaseOnlyBodyBound(), "fixture precondition: the body is bound to its header")

	err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{
		PeerID:                  "peer-serving",
		DisableOptimisticMining: false,
	})
	require.Error(t, err, "the non-optimistic branch validates before adding, so the verdict is synchronous")
	require.True(t, errors.Is(err, errors.ErrBlockInvalid), "got: %v", err)
}
