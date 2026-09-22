package blockvalidation

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestValidateBlock_OptimisticPeerBlocks_UnboundBodyTakesInvalidateRoute is a CHARACTERIZATION test.
// It pins a DEFERRED exposure, it does not assert that anything is safe.
//
// When an operator opts into optimistic mining for peer-served blocks, the received body is added to
// the chain BEFORE block.Valid runs, so an attacker-chosen unbound body is transiently visible as a
// VALID tip and is then persisted as invalid, carrying the peer's coinbase — the one place the
// "never persist an unbound body" rule does not hold (bitcoin-sv/teranode#4844). Fixing it means
// splitting block.Valid so its integrity floor runs before the optimistic AddBlock, which is
// recorded as the prerequisite on the setting itself
// (settings/blockvalidation_settings.go, blockvalidation_optimistic_mining_peer_blocks).
//
// DELETE this test when that split lands. Operators should keep the flag off until then.
//
// The second subtest is the one protecting the shipped default: with the opt-in off, the same
// delivery persists nothing.
func TestValidateBlock_OptimisticPeerBlocks_UnboundBodyTakesInvalidateRoute(t *testing.T) {
	initPrometheusMetrics()

	// Both subtests deliver the same unbound body: a single-transaction block whose header merkle
	// root is NOT its coinbase txid, so nothing reconciles the body to the header.
	t.Run("opted in: the body is added, then invalidated, and the record keeps the peer's coinbase", func(t *testing.T) {
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

		coinbaseTx := canaryCoinbaseAtHeight(t, blockHeight)
		unboundRoot := chainhash.Hash{0xCD}
		hdr := minedHeaderWithBits(t, tSettings.ChainCfgParams.GenesisHash, &unboundRoot, *expected, timestamp)

		block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), blockHeight, 0)
		require.NoError(t, err)

		err = bv.ValidateBlockWithOptions(ctx, block, "http://localhost", &ValidateBlockOptions{
			PeerID:                  "peer-serving",
			DisableOptimisticMining: optimisticMiningDisabledForPeerPath(tSettings, "http://localhost"),
		})
		require.NoError(t, err, "the opt-in path accepts the body before validating it — that IS the exposure")

		exists, err := client.GetBlockExists(ctx, block.Hash())
		require.NoError(t, err)
		require.True(t, exists, "the unbound body was published to the chain")

		// The background validation then takes the invalidate route.
		require.Eventually(t, func() bool {
			_, meta, metaErr := client.GetBlockHeader(ctx, block.Hash())
			return metaErr == nil && meta != nil && meta.Invalid
		}, 15*time.Second, 50*time.Millisecond, "the background validation must invalidate the accepted body")

		stored, err := client.GetBlock(ctx, block.Hash())
		require.NoError(t, err)

		storedMiner, err := util.ExtractCoinbaseMinerRaw(stored.CoinbaseTx, false)
		require.NoError(t, err)
		require.Contains(t, storedMiner, minerMarkupCanary,
			"the persisted record carries the peer's chosen coinbase — the residual this test pins")
	})

	t.Run("default configuration: nothing is persisted", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = true
		// The shipped default for the peer path.
		tSettings.BlockValidation.OptimisticMiningPeerBlocks = false
		tSettings.ChainCfgParams.Checkpoints = nil

		require.True(t, optimisticMiningDisabledForPeerPath(tSettings, "http://localhost"),
			"the peer path needs BOTH flags, so the default keeps it non-optimistic")

		bv, client := newNoPersistHarness(ctx, t, tSettings)

		fake := &corruptStrikeP2PClient{}
		bv.p2pClient = fake

		const blockHeight = uint32(1)

		timestamp := uint32(time.Now().Unix()) //nolint:gosec

		expected, err := client.GetNextWorkRequired(ctx, tSettings.ChainCfgParams.GenesisHash, int64(timestamp))
		require.NoError(t, err)
		require.NotNil(t, expected)

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
		require.True(t, errors.IsBlockCorrupt(err), "an unbound body is a verdict on the delivery, got: %v", err)

		requireNothingPersisted(ctx, t, client, block.Hash())
	})
}
