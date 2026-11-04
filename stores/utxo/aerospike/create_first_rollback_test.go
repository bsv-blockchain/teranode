package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// recordBins reads the named bins of record idx of a transaction straight off the master or a
// pagination child with the raw client, which is the only way to see a bin the store does not
// expose. It returns nil when the record does not exist.
func recordBins(t *testing.T, client *uaerospike.Client, store *teranode_aerospike.Store, hash *chainhash.Hash, idx uint32, names ...fields.FieldName) aerospike.BinMap {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), uaerospike.CalculateKeySourceInternal(hash, idx))
	require.NoError(t, err)

	binNames := make([]string, len(names))
	for i, n := range names {
		binNames[i] = n.String()
	}

	rec, aErr := client.Get(util.GetAerospikeReadPolicy(tSettings), key, binNames...)
	if aErr != nil {
		require.ErrorIs(t, aErr, aerospike.ErrKeyNotFound)

		return nil
	}

	return rec.Bins
}

func TestBeginCreateFirstRollback(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	_, err := store.Create(ctx, tests.ParentTx, 100)
	require.NoError(t, err)

	t.Run("a creating record is marked and stays creating", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 0)
		hash := child.TxIDChainHash()

		_, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
		require.NoError(t, err)

		began, err := store.BeginCreateFirstRollback(ctx, hash)
		require.NoError(t, err)
		require.True(t, began)

		bins := recordBins(t, client, store, hash, 0, fields.Creating, fields.RollingBack)
		require.Equal(t, true, bins[fields.RollingBack.String()], "the rollingBack bin is set")
		require.Equal(t, true, bins[fields.Creating.String()], "the creating bin stays, so the Lua spend gate still rejects the outputs")

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, hash, got))
		require.True(t, got.Creating)
		require.True(t, got.RollingBack, "GetMeta must report the marker")
	})

	t.Run("an already rolling-back record reports true", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 1)
		hash := child.TxIDChainHash()

		_, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
		require.NoError(t, err)

		for range 2 {
			began, err := store.BeginCreateFirstRollback(ctx, hash)
			require.NoError(t, err)
			require.True(t, began, "a repeat of the marker call reports true")
		}
	})

	t.Run("a finalized record is not marked", func(t *testing.T) {
		child := spendableChildTx(t, tests.ParentTx, 2)
		hash := child.TxIDChainHash()

		_, err := store.Create(ctx, child, 101, utxo.WithCreating(true))
		require.NoError(t, err)
		require.NoError(t, store.FinalizeTransaction(ctx, child))

		began, err := store.BeginCreateFirstRollback(ctx, hash)
		require.NoError(t, err)
		require.False(t, began)

		bins := recordBins(t, client, store, hash, 0, fields.Creating, fields.RollingBack)
		require.NotContains(t, bins, fields.RollingBack.String(), "nothing is written to a finalized record")

		got := &meta.Data{}
		require.NoError(t, store.GetMeta(ctx, hash, got))
		require.False(t, got.Creating)
		require.False(t, got.RollingBack)
	})

	t.Run("an absent record reports false without creating one", func(t *testing.T) {
		absent := spendableChildTx(t, tests.ParentTx, 0)
		absent.Version = 77

		began, err := store.BeginCreateFirstRollback(ctx, absent.TxIDChainHash())
		require.NoError(t, err)
		require.False(t, began)
		require.Nil(t, recordBins(t, client, store, absent.TxIDChainHash(), 0, fields.RollingBack), "UPDATE_ONLY must not create the record")
	})
}
