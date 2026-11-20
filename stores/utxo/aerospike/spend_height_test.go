package aerospike_test

import (
	"encoding/binary"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranode_aerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// spendHeightRetention is the retention the direct UDF calls below pass. The UDF only uses it to
// set a delete-at-height, so it does not change what these tests pin.
const spendHeightRetention = 288

func spendHeightKey(t *testing.T, store *teranode_aerospike.Store, hash *chainhash.Hash) *aerospike.Key {
	t.Helper()

	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), hash.CloneBytes())
	require.NoError(t, err)

	return key
}

// spendHeightEntry reads the raw utxos-bin entry at offset of the master record.
func spendHeightEntry(t *testing.T, store *teranode_aerospike.Store, hash *chainhash.Hash, offset int) []byte {
	t.Helper()

	rec, err := store.GetClient().Get(util.GetAerospikeReadPolicy(test.CreateBaseTestSettings(t)), spendHeightKey(t, store, hash), "utxos")
	require.NoError(t, err)

	utxos, ok := rec.Bins["utxos"].([]interface{})
	require.True(t, ok)

	entry, ok := utxos[offset].([]byte)
	require.True(t, ok)

	return entry
}

// spendHeightSetEntry replaces the entry at offset, to stage an entry written by older code.
func spendHeightSetEntry(t *testing.T, store *teranode_aerospike.Store, hash *chainhash.Hash, offset int, entry []byte) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	key := spendHeightKey(t, store, hash)

	rec, err := store.GetClient().Get(util.GetAerospikeReadPolicy(tSettings), key, "utxos")
	require.NoError(t, err)

	utxos, ok := rec.Bins["utxos"].([]interface{})
	require.True(t, ok)

	utxos[offset] = entry

	require.NoError(t, store.GetClient().Put(util.GetAerospikeWritePolicy(tSettings, 0), key, aerospike.BinMap{"utxos": utxos}))
}

// spendHeightUDF calls the spend UDF directly and parses its answer, so a test sees what the UDF
// reports without the Go spend path's own handling of it.
func spendHeightUDF(t *testing.T, store *teranode_aerospike.Store, parent *bt.Tx, spend *utxo.Spend, height uint32) *teranode_aerospike.LuaMapResponse {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	ret, aErr := store.GetClient().Execute(util.GetAerospikeWritePolicy(tSettings, 0), spendHeightKey(t, store, parent.TxIDChainHash()), teranode_aerospike.LuaPackage, "spend",
		aerospike.NewIntegerValue(int(spend.Vout)),
		aerospike.NewValue(spend.UTXOHash[:]),
		aerospike.NewValue(spend.SpendingData.Bytes()),
		aerospike.NewValue(false),
		aerospike.NewValue(false),
		aerospike.NewValue(int(height)),
		aerospike.NewValue(spendHeightRetention),
	)
	require.NoError(t, aErr)

	res, err := store.ParseLuaMapResponse(ret)
	require.NoError(t, err)

	return res
}

func spendHeightSpendsOf(t *testing.T, child *bt.Tx) []*utxo.Spend {
	t.Helper()

	spends, err := utxo.GetSpends(child)
	require.NoError(t, err)

	return spends
}

// TestSpendRecordsItsHeight_Aerospike pins the entry layout: a fresh spend writes the 72-byte entry
// with its block height, a match reports that height, an unspend leaves 32 bytes, and a frozen
// output keeps its 68-byte form and is never read as a historical spend.
func TestSpendRecordsItsHeight_Aerospike(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, ctx, cleanup := initAerospike(t, tSettings, logger)
	t.Cleanup(cleanup)

	parent := conflictingIndexParent(t, ctx, store, 7001, 3)
	child := conflictingIndexTx(t, parent, []uint32{0}, 1)

	t.Run("a spend writes a 72-byte entry carrying its height", func(t *testing.T) {
		spends, err := store.Spend(ctx, child, 1200)
		require.NoError(t, err)

		entry := spendHeightEntry(t, store, parent.TxIDChainHash(), 0)
		require.Len(t, entry, 72)
		require.Equal(t, uint32(1200), binary.LittleEndian.Uint32(entry[68:72]))
		require.Equal(t, spends[0].SpendingData.Bytes(), entry[32:68])
	})

	t.Run("an idempotent match reports the recorded height", func(t *testing.T) {
		spends := spendHeightSpendsOf(t, child)

		res := spendHeightUDF(t, store, parent, spends[0], 2001)
		require.Equal(t, teranode_aerospike.LuaStatusOK, res.Status)
		require.Equal(t, []int{0}, res.Idempotent)
		require.Equal(t, map[int]uint32{0: 1200}, res.IdempotentHeights)

		// a match writes nothing: the recorded height stays the first spend's
		entry := spendHeightEntry(t, store, parent.TxIDChainHash(), 0)
		require.Len(t, entry, 72)
		require.Equal(t, uint32(1200), binary.LittleEndian.Uint32(entry[68:72]))
	})

	t.Run("an unspend leaves a 32-byte entry that can be spent again", func(t *testing.T) {
		spends := spendHeightSpendsOf(t, child)

		require.NoError(t, store.Unspend(ctx, spends))
		require.Len(t, spendHeightEntry(t, store, parent.TxIDChainHash(), 0), 32)

		other := conflictingIndexTx(t, parent, []uint32{0}, 2)
		_, err := store.Spend(ctx, other, 1300)
		require.NoError(t, err)

		entry := spendHeightEntry(t, store, parent.TxIDChainHash(), 0)
		require.Len(t, entry, 72)
		require.Equal(t, uint32(1300), binary.LittleEndian.Uint32(entry[68:72]))
	})

	t.Run("a frozen output keeps its 68-byte form and is not a historical spend", func(t *testing.T) {
		frozen := conflictingIndexTx(t, parent, []uint32{1}, 1)
		frozenSpends := spendHeightSpendsOf(t, frozen)
		freeze := []*utxo.Spend{{TxID: parent.TxIDChainHash(), Vout: 1, UTXOHash: frozenSpends[0].UTXOHash}}

		require.NoError(t, store.FreezeUTXOs(ctx, freeze, tSettings))
		require.Len(t, spendHeightEntry(t, store, parent.TxIDChainHash(), 1), 68)

		res := spendHeightUDF(t, store, parent, frozenSpends[0], 5000)
		require.Equal(t, teranode_aerospike.LuaStatusError, res.Status)
		require.Empty(t, res.Idempotent, "a frozen output is never an idempotent match")
		require.Nil(t, res.IdempotentHeights)

		require.NoError(t, store.UnFreezeUTXOs(ctx, freeze, tSettings))
		require.Len(t, spendHeightEntry(t, store, parent.TxIDChainHash(), 1), 32)

		_, err := store.Spend(ctx, frozen, 5001)
		require.NoError(t, err)
		require.Len(t, spendHeightEntry(t, store, parent.TxIDChainHash(), 1), 72)
	})

	t.Run("an entry written without a height reports no height", func(t *testing.T) {
		old := conflictingIndexTx(t, parent, []uint32{2}, 1)
		oldSpends := spendHeightSpendsOf(t, old)

		// stage what code before the height wrote: hash + spending data, 68 bytes
		entry := append(append([]byte{}, oldSpends[0].UTXOHash[:]...), oldSpends[0].SpendingData.Bytes()...)
		require.Len(t, entry, 68)
		spendHeightSetEntry(t, store, parent.TxIDChainHash(), 2, entry)

		res := spendHeightUDF(t, store, parent, oldSpends[0], 9000)
		require.Equal(t, teranode_aerospike.LuaStatusOK, res.Status)
		require.Equal(t, []int{0}, res.Idempotent)
		require.Empty(t, res.IdempotentHeights, "a 68-byte entry has no height to report")

		// and the match did not upgrade the entry: it wrote nothing
		require.Len(t, spendHeightEntry(t, store, parent.TxIDChainHash(), 2), 68)
	})
}

// TestGetReadsBothSpentEntrySizes_Aerospike: every reader decodes the spending data of a 68-byte
// entry (spent before the height was recorded) and of a 72-byte one.
func TestGetReadsBothSpentEntrySizes_Aerospike(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, ctx, cleanup := initAerospike(t, tSettings, logger)
	t.Cleanup(cleanup)

	parent := conflictingIndexParent(t, ctx, store, 7002, 2)
	child := conflictingIndexTx(t, parent, []uint32{0, 1}, 1)

	spends, err := store.Spend(ctx, child, 1200)
	require.NoError(t, err)

	// output 0 stays a 72-byte entry; output 1 is rewritten as a 68-byte one
	require.Len(t, spendHeightEntry(t, store, parent.TxIDChainHash(), 0), 72)
	spendHeightSetEntry(t, store, parent.TxIDChainHash(), 1, spendHeightEntry(t, store, parent.TxIDChainHash(), 1)[:68])

	for _, spend := range spends {
		resp, getErr := store.GetSpend(ctx, spend)
		require.NoError(t, getErr)
		require.Equal(t, int(utxo.Status_SPENT), resp.Status, "output %d", spend.Vout)
		require.Equal(t, child.TxIDChainHash(), resp.SpendingData.TxID, "output %d", spend.Vout)
		require.Equal(t, int(spend.Vout), resp.SpendingData.Vin, "output %d", spend.Vout)
	}

	data, err := store.Get(ctx, parent.TxIDChainHash(), fields.Utxos)
	require.NoError(t, err)
	require.Len(t, data.SpendingDatas, 2)

	for i, spendingData := range data.SpendingDatas {
		require.NotNil(t, spendingData, "output %d", i)
		require.Equal(t, child.TxIDChainHash(), spendingData.TxID, "output %d", i)
	}
}
