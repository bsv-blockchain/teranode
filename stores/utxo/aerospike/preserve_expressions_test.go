package aerospike_test

import (
	"context"
	"testing"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestPreserveTransactionsWithExpressions(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Aerospike.EnablePreserveFilterExpressions = true

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	defer deferFn()

	t.Run("empty txIDs returns nil", func(t *testing.T) {
		err := store.PreserveTransactions(ctx, nil, 100)
		require.NoError(t, err)

		err = store.PreserveTransactions(ctx, []chainhash.Hash{}, 100)
		require.NoError(t, err)
	})

	t.Run("missing record is a no-op", func(t *testing.T) {
		cleanDB(t, client)

		missingHash := chainhash.HashH([]byte("does-not-exist"))
		err := store.PreserveTransactions(ctx, []chainhash.Hash{missingHash}, 500)
		require.NoError(t, err)
	})

	t.Run("sets preserveUntil and clears deleteAtHeight", func(t *testing.T) {
		cleanDB(t, client)

		_, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)

		txHash := *tx.TxIDChainHash()

		// Set a deleteAtHeight on the record first via raw client
		txKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), txHash[:])
		require.NoError(t, err)

		writePolicy := util.GetAerospikeWritePolicy(tSettings, 0)
		writePolicy.RecordExistsAction = aerospike.UPDATE
		err = client.PutBins(writePolicy, txKey,
			aerospike.NewBin(fields.DeleteAtHeight.String(), 200),
		)
		require.NoError(t, err)

		// Verify deleteAtHeight is set
		record, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.NotNil(t, record.Bins[fields.DeleteAtHeight.String()])

		// Now preserve the transaction
		err = store.PreserveTransactions(ctx, []chainhash.Hash{txHash}, 1000)
		require.NoError(t, err)

		// Verify preserveUntil is set and deleteAtHeight is cleared
		record, err = client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.Equal(t, 1000, record.Bins[fields.PreserveUntil.String()])
		require.Nil(t, record.Bins[fields.DeleteAtHeight.String()])
	})

	t.Run("idempotent - preserving same tx twice is ok", func(t *testing.T) {
		cleanDB(t, client)

		_, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)

		txHash := *tx.TxIDChainHash()

		err = store.PreserveTransactions(ctx, []chainhash.Hash{txHash}, 500)
		require.NoError(t, err)

		err = store.PreserveTransactions(ctx, []chainhash.Hash{txHash}, 500)
		require.NoError(t, err)

		// Verify the value is still correct
		txKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), txHash[:])
		require.NoError(t, err)
		record, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.Equal(t, 500, record.Bins[fields.PreserveUntil.String()])
	})

	t.Run("batch with mix of existing and missing records", func(t *testing.T) {
		cleanDB(t, client)

		_, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)

		txHash := *tx.TxIDChainHash()
		missingHash := chainhash.HashH([]byte("missing-tx"))

		err = store.PreserveTransactions(ctx, []chainhash.Hash{txHash, missingHash}, 750)
		require.NoError(t, err)

		// Existing record should be preserved
		txKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), txHash[:])
		require.NoError(t, err)
		record, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.Equal(t, 750, record.Bins[fields.PreserveUntil.String()])
	})

	t.Run("updates preserveUntil to new height", func(t *testing.T) {
		cleanDB(t, client)

		_, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)

		txHash := *tx.TxIDChainHash()

		err = store.PreserveTransactions(ctx, []chainhash.Hash{txHash}, 500)
		require.NoError(t, err)

		// Update to a higher height
		err = store.PreserveTransactions(ctx, []chainhash.Hash{txHash}, 1000)
		require.NoError(t, err)

		txKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), txHash[:])
		require.NoError(t, err)
		record, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
		require.NoError(t, err)
		require.Equal(t, 1000, record.Bins[fields.PreserveUntil.String()])
		require.Nil(t, record.Bins[fields.DeleteAtHeight.String()])
	})
}

func TestPreserveTransactionsWithExpressions_LuaParity(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)

	t.Run("expression result matches lua result", func(t *testing.T) {
		for _, useExpressions := range []bool{false, true} {
			name := "lua"
			if useExpressions {
				name = "expressions"
			}

			t.Run(name, func(t *testing.T) {
				tSettings := test.CreateBaseTestSettings(t)
				tSettings.Aerospike.EnablePreserveFilterExpressions = useExpressions

				client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
				defer deferFn()
				cleanDB(t, client)

				_, err := store.Create(ctx, tx, 0)
				require.NoError(t, err)

				txHash := *tx.TxIDChainHash()

				// Set deleteAtHeight first
				txKey, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), txHash[:])
				require.NoError(t, err)
				writePolicy := util.GetAerospikeWritePolicy(tSettings, 0)
				writePolicy.RecordExistsAction = aerospike.UPDATE
				err = client.PutBins(writePolicy, txKey,
					aerospike.NewBin(fields.DeleteAtHeight.String(), 200),
				)
				require.NoError(t, err)

				// Preserve
				err = store.PreserveTransactions(context.Background(), []chainhash.Hash{txHash}, 999)
				require.NoError(t, err)

				// Check bins
				record, err := client.Get(util.GetAerospikeReadPolicy(tSettings), txKey)
				require.NoError(t, err)
				require.Equal(t, 999, record.Bins[fields.PreserveUntil.String()], "preserveUntil should be set")
				require.Nil(t, record.Bins[fields.DeleteAtHeight.String()], "deleteAtHeight should be cleared")
			})
		}
	})
}
