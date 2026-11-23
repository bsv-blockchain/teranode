package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestUnsetMinedBackdatesUnminedSince: when the last block of a tx is invalidated the tx goes
// mined -> unmined and must be stamped with utxo.BackdatedUnminedSince (issue 1768), as
// MarkTransactionsOnLongestChain(false) does. An unmined_since the tx already carries is kept.
func TestUnsetMinedBackdatesUnminedSince(t *testing.T) {
	const createHeight uint32 = 50
	const tip uint32 = 5000

	t.Run("absent unmined_since is stamped backdated", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		store, parent := setup(ctx, t)
		retention := store.settings.UtxoStore.UnminedTxRetention
		require.Greater(t, retention, uint32(0))

		parentHash := parent.TxIDChainHash()
		_, err := store.Create(ctx, parent, createHeight)
		require.NoError(t, err)

		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
			BlockID: 100, BlockHeight: createHeight, OnLongestChain: true,
		})
		require.NoError(t, err)

		require.NoError(t, store.SetBlockHeight(tip))

		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
			BlockID: 100, BlockHeight: createHeight, UnsetMined: true,
		})
		require.NoError(t, err)

		_, unminedSince := readDAHAndUnmined(ctx, t, store, parentHash)
		require.NotNil(t, unminedSince)
		require.Equal(t, int64(utxo.BackdatedUnminedSince(tip, retention)), *unminedSince)
	})

	t.Run("existing unmined_since is kept", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		store, parent := setup(ctx, t)
		parentHash := parent.TxIDChainHash()

		// Create stamps unmined_since = createHeight.
		_, err := store.Create(ctx, parent, createHeight)
		require.NoError(t, err)

		_, unminedSince := readDAHAndUnmined(ctx, t, store, parentHash)
		require.NotNil(t, unminedSince)
		require.Equal(t, int64(createHeight), *unminedSince)

		// Mined only on a fork block: unmined_since untouched.
		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
			BlockID: 200, BlockHeight: createHeight, OnLongestChain: false,
		})
		require.NoError(t, err)

		require.NoError(t, store.SetBlockHeight(tip))

		// The fork block is invalidated: the tx never left the unmined set, keep its stamp.
		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
			BlockID: 200, BlockHeight: createHeight, UnsetMined: true,
		})
		require.NoError(t, err)

		_, unminedSince = readDAHAndUnmined(ctx, t, store, parentHash)
		require.NotNil(t, unminedSince)
		require.Equal(t, int64(createHeight), *unminedSince)
	})
}
