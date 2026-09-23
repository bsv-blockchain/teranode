package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// TestFreezeRecordBins pins the on-disk shape of the alert system's freeze record
// (issue #1422): the utxoFreezeFrom entry is always written (its presence IS the
// record), utxoFreezeUntil and utxoFreezeExp only when set, and an unfreeze removes the
// entries and drops every bin that empties — so a record that was once frozen pays no
// permanent Lua-fallback tax on the expression-based spend path, whose guard keys on bin
// existence alone.
func TestFreezeRecordBins(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, ctx, deferFn := initAerospike(t, tSettings, logger)

	t.Cleanup(deferFn)

	keySource := uaerospike.CalculateKeySource(tx.TxIDChainHash(), 0, store.GetUtxoBatchSize()) //nolint:gosec
	key, aErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource)
	require.NoError(t, aErr)

	require.NoError(t, client.Put(nil, key, aerospike.BinMap{
		fields.Utxos.String():      []interface{}{utxoHash0[:]},
		fields.TotalUtxos.String(): 1,
	}))

	readBins := func() aerospike.BinMap {
		rec, err := client.Get(nil, key)
		require.NoError(t, err)
		require.NotNil(t, rec)

		return rec.Bins
	}

	entry := func(bins aerospike.BinMap, bin fields.FieldName) (interface{}, bool) {
		raw, found := bins[bin.String()]
		if !found {
			return nil, false
		}

		m, ok := raw.(map[interface{}]interface{})
		require.True(t, ok, "%s must be a map bin", bin)

		v, found := m[0]

		return v, found
	}

	t.Run("an unqualified freeze writes the from entry and nothing else", func(t *testing.T) {
		require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{{TxID: tx.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash0}}, tSettings))

		bins := readBins()

		from, found := entry(bins, fields.UtxoFreezeFrom)
		require.True(t, found, "the from entry is the record and must always be written")
		require.Equal(t, 0, from)

		_, found = entry(bins, fields.UtxoFreezeUntil)
		require.False(t, found, "a zero end is not stored")

		_, found = entry(bins, fields.UtxoFreezeExp)
		require.False(t, found, "a clear flag is not stored")
	})

	t.Run("a windowed freeze with the flag writes all three", func(t *testing.T) {
		require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{{
			TxID: tx.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash0,
			FreezeFrom: 500, FreezeUntil: 600, FreezePolicyExpires: true,
		}}, tSettings))

		bins := readBins()

		from, found := entry(bins, fields.UtxoFreezeFrom)
		require.True(t, found)
		require.Equal(t, 500, from)

		until, found := entry(bins, fields.UtxoFreezeUntil)
		require.True(t, found)
		require.Equal(t, 600, until)

		exp, found := entry(bins, fields.UtxoFreezeExp)
		require.True(t, found)
		require.Equal(t, true, exp)
	})

	t.Run("narrowing the record removes the entries it no longer needs", func(t *testing.T) {
		require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{{
			TxID: tx.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash0,
			FreezeFrom: 500,
		}}, tSettings))

		bins := readBins()

		_, found := entry(bins, fields.UtxoFreezeFrom)
		require.True(t, found)

		_, found = bins[fields.UtxoFreezeUntil.String()]
		require.False(t, found, "an emptied map bin must be dropped, not left holding a zero")

		_, found = bins[fields.UtxoFreezeExp.String()]
		require.False(t, found, "an emptied map bin must be dropped, not left holding a zero")
	})

	t.Run("unfreeze leaves no freeze bins behind", func(t *testing.T) {
		require.NoError(t, store.UnFreezeUTXOs(ctx, []*utxo.Spend{{TxID: tx.TxIDChainHash(), Vout: 0, UTXOHash: utxoHash0}}, tSettings))

		bins := readBins()

		for _, bin := range []fields.FieldName{fields.UtxoFreezeFrom, fields.UtxoFreezeUntil, fields.UtxoFreezeExp} {
			_, found := bins[bin.String()]
			require.False(t, found, "%s must be gone after unfreeze", bin)
		}

		utxos, ok := bins[fields.Utxos.String()].([]interface{})
		require.True(t, ok)
		require.Len(t, utxos, 1)
		require.Len(t, utxos[0].([]byte), 32, "the output must be a bare unspent hash again")
	})
}
