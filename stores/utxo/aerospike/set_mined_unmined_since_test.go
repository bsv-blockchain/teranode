package aerospike_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestUnsetMinedBackdatesUnminedSince is the Aerospike counterpart of the SQL test of the same
// name: mined -> unmined via unsetMined stamps utxo.BackdatedUnminedSince (issue 1768), and an
// existing unmined_since is kept.
func TestUnsetMinedBackdatesUnminedSince(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Aerospike integration test in short mode")
	}

	const createHeight uint32 = 50
	const tip uint32 = 5000

	t.Run("absent unmined_since is stamped backdated", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.NewErrorTestLogger(t)
		tSettings := test.CreateBaseTestSettings(t)
		retention := tSettings.UtxoStore.UnminedTxRetention
		require.Greater(t, retention, uint32(0))

		client, store, _, cleanup := initAerospike(t, tSettings, logger)
		defer cleanup()
		cleanDB(t, client)

		parent := newParent(t)
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

		_, unminedSince := readMasterBins(t, client, store.GetNamespace(), store.GetName(), parentHash)
		require.Equal(t, int(utxo.BackdatedUnminedSince(tip, retention)), unminedSince)
	})

	t.Run("existing unmined_since is kept", func(t *testing.T) {
		ctx := context.Background()
		logger := ulogger.NewErrorTestLogger(t)
		tSettings := test.CreateBaseTestSettings(t)

		client, store, _, cleanup := initAerospike(t, tSettings, logger)
		defer cleanup()
		cleanDB(t, client)

		parent := newParent(t)
		parentHash := parent.TxIDChainHash()
		_, err := store.Create(ctx, parent, createHeight)
		require.NoError(t, err)

		_, unminedSince := readMasterBins(t, client, store.GetNamespace(), store.GetName(), parentHash)
		require.Equal(t, int(createHeight), unminedSince)

		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
			BlockID: 200, BlockHeight: createHeight, OnLongestChain: false,
		})
		require.NoError(t, err)

		require.NoError(t, store.SetBlockHeight(tip))

		_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{parentHash}, utxo.MinedBlockInfo{
			BlockID: 200, BlockHeight: createHeight, UnsetMined: true,
		})
		require.NoError(t, err)

		_, unminedSince = readMasterBins(t, client, store.GetNamespace(), store.GetName(), parentHash)
		require.Equal(t, int(createHeight), unminedSince)
	})
}
