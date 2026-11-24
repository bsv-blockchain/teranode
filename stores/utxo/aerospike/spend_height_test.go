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
	"github.com/bsv-blockchain/teranode/util/uaerospike"
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

// spendHeightAt reads the spend height recorded for the output at offset of the record at key:
// the 4 little-endian bytes at offset*4 of its spendHeights bin. ok is false when the bin does not
// cover the output or holds zero there, which is how an output spent by older code reads.
func spendHeightAt(t *testing.T, client *uaerospike.Client, key *aerospike.Key, offset int) (height uint32, ok bool) {
	t.Helper()

	rec, err := client.Get(nil, key, fields.SpendHeights.String())
	require.NoError(t, err)

	heights, _ := rec.Bins[fields.SpendHeights.String()].([]byte)
	if len(heights) < (offset+1)*4 {
		return 0, false
	}

	height = binary.LittleEndian.Uint32(heights[offset*4:])

	return height, height != 0
}

// spendHeightClear zeroes the spend height of the output at offset, to stage an output spent by
// code that recorded none.
func spendHeightClear(t *testing.T, client *uaerospike.Client, key *aerospike.Key, offset int) {
	t.Helper()

	_, err := client.Operate(nil, key, aerospike.BitSetOp(aerospike.DefaultBitPolicy(), fields.SpendHeights.String(), offset*32, 32, make([]byte, 4)))
	require.NoError(t, err)
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

// TestSpendRecordsItsHeight_Aerospike pins the layout: a fresh spend writes the 68-byte entry older
// code reads and records its block height in the spendHeights bin, a match reports that height, an
// unspend clears it, a re-spend records its own, and a frozen output is never read as a historical
// spend.
func TestSpendRecordsItsHeight_Aerospike(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, ctx, cleanup := initAerospike(t, tSettings, logger)
	t.Cleanup(cleanup)

	parent := conflictingIndexParent(t, ctx, store, 7001, 3)
	child := conflictingIndexTx(t, parent, []uint32{0}, 1)
	parentKey := spendHeightKey(t, store, parent.TxIDChainHash())

	t.Run("a spend keeps the 68-byte entry and records its height in its own bin", func(t *testing.T) {
		spends, err := store.Spend(ctx, child, 1200)
		require.NoError(t, err)

		entry := spendHeightEntry(t, store, parent.TxIDChainHash(), 0)
		require.Len(t, entry, 68, "an older teranode.lua reads only a 68-byte entry as spent")
		require.Equal(t, spends[0].SpendingData.Bytes(), entry[32:68])

		height, ok := spendHeightAt(t, store.GetClient(), parentKey, 0)
		require.True(t, ok)
		require.Equal(t, uint32(1200), height)
	})

	t.Run("an idempotent match reports the recorded height", func(t *testing.T) {
		spends := spendHeightSpendsOf(t, child)

		res := spendHeightUDF(t, store, parent, spends[0], 2001)
		require.Equal(t, teranode_aerospike.LuaStatusOK, res.Status)
		require.Equal(t, []int{0}, res.Idempotent)
		require.Equal(t, map[int]uint32{0: 1200}, res.IdempotentHeights)

		// a match writes nothing: the recorded height stays the first spend's
		height, ok := spendHeightAt(t, store.GetClient(), parentKey, 0)
		require.True(t, ok)
		require.Equal(t, uint32(1200), height)
	})

	t.Run("an unspend clears the height and a later spend records its own", func(t *testing.T) {
		spends := spendHeightSpendsOf(t, child)

		require.NoError(t, store.Unspend(ctx, spends))
		require.Len(t, spendHeightEntry(t, store, parent.TxIDChainHash(), 0), 32)

		// the unspend clears the height, so an older package's later spend, which writes
		// none, is not read as one made at 1200
		_, ok := spendHeightAt(t, store.GetClient(), parentKey, 0)
		require.False(t, ok, "unspend zeroes the output's height")

		other := conflictingIndexTx(t, parent, []uint32{0}, 2)
		_, err := store.Spend(ctx, other, 1300)
		require.NoError(t, err)

		height, ok := spendHeightAt(t, store.GetClient(), parentKey, 0)
		require.True(t, ok)
		require.Equal(t, uint32(1300), height)
	})

	t.Run("a frozen output is not a historical spend", func(t *testing.T) {
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

		height, ok := spendHeightAt(t, store.GetClient(), parentKey, 1)
		require.True(t, ok)
		require.Equal(t, uint32(5001), height)
	})

	t.Run("an output spent without a height reports none", func(t *testing.T) {
		old := conflictingIndexTx(t, parent, []uint32{2}, 1)
		oldSpends := spendHeightSpendsOf(t, old)

		_, err := store.Spend(ctx, old, 6000)
		require.NoError(t, err)

		// stage what code before the height wrote: the same entry, no height
		spendHeightClear(t, store.GetClient(), parentKey, 2)

		res := spendHeightUDF(t, store, parent, oldSpends[0], 9000)
		require.Equal(t, teranode_aerospike.LuaStatusOK, res.Status)
		require.Equal(t, []int{0}, res.Idempotent)
		require.Empty(t, res.IdempotentHeights, "an output with no height recorded has none to report")

		// and the match did not record one: it wrote nothing
		_, ok := spendHeightAt(t, store.GetClient(), parentKey, 2)
		require.False(t, ok)
	})
}

// TestSpendHeightsBinGrowsToCoverTheOutput_Aerospike: the bin holds 4 bytes per output in output
// order and grows only as far as the highest output spent. A record spent only by older code has
// no bin at all, and a match on it reports no height.
func TestSpendHeightsBinGrowsToCoverTheOutput_Aerospike(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, ctx, cleanup := initAerospike(t, tSettings, logger)
	t.Cleanup(cleanup)

	parent := conflictingIndexParent(t, ctx, store, 7002, 4)
	parentKey := spendHeightKey(t, store, parent.TxIDChainHash())

	readBin := func() []byte {
		rec, err := store.GetClient().Get(nil, parentKey, fields.SpendHeights.String())
		require.NoError(t, err)

		heights, _ := rec.Bins[fields.SpendHeights.String()].([]byte)

		return heights
	}

	t.Run("a record with no spend has no bin", func(t *testing.T) {
		require.Nil(t, readBin())
	})

	t.Run("spending output 2 first writes 12 bytes", func(t *testing.T) {
		_, err := store.Spend(ctx, conflictingIndexTx(t, parent, []uint32{2}, 1), 1500)
		require.NoError(t, err)

		heights := readBin()
		require.Len(t, heights, 12)
		require.Equal(t, make([]byte, 8), heights[:8], "outputs 0 and 1 are not spent and read as no height")
		require.Equal(t, uint32(1500), binary.LittleEndian.Uint32(heights[8:12]))
	})

	t.Run("spending output 0 fills its slot without growing the bin", func(t *testing.T) {
		_, err := store.Spend(ctx, conflictingIndexTx(t, parent, []uint32{0}, 1), 1501)
		require.NoError(t, err)

		heights := readBin()
		require.Len(t, heights, 12)
		require.Equal(t, uint32(1501), binary.LittleEndian.Uint32(heights[0:4]))
		require.Equal(t, uint32(1500), binary.LittleEndian.Uint32(heights[8:12]))
	})

	t.Run("spending output 3 grows the bin and keeps the others", func(t *testing.T) {
		_, err := store.Spend(ctx, conflictingIndexTx(t, parent, []uint32{3}, 1), 1502)
		require.NoError(t, err)

		heights := readBin()
		require.Len(t, heights, 16)
		require.Equal(t, uint32(1501), binary.LittleEndian.Uint32(heights[0:4]))
		require.Equal(t, uint32(0), binary.LittleEndian.Uint32(heights[4:8]))
		require.Equal(t, uint32(1500), binary.LittleEndian.Uint32(heights[8:12]))
		require.Equal(t, uint32(1502), binary.LittleEndian.Uint32(heights[12:16]))
	})

	t.Run("a match on a record without the bin reports no height", func(t *testing.T) {
		other := conflictingIndexParent(t, ctx, store, 7003, 1)
		child := conflictingIndexTx(t, other, []uint32{0}, 1)

		_, err := store.Spend(ctx, child, 1600)
		require.NoError(t, err)

		// stage a record spent only by older code: drop the bin
		otherKey := spendHeightKey(t, store, other.TxIDChainHash())
		require.NoError(t, store.GetClient().Put(nil, otherKey, aerospike.BinMap{fields.SpendHeights.String(): nil}))

		res := spendHeightUDF(t, store, other, spendHeightSpendsOf(t, child)[0], 1601)
		require.Equal(t, teranode_aerospike.LuaStatusOK, res.Status)
		require.Equal(t, []int{0}, res.Idempotent)
		require.Empty(t, res.IdempotentHeights)
	})
}

// unspendUDF calls the unspend UDF directly and parses its answer. keepBelow is the optional
// keepBelowHeight argument: none sends the arguments an older caller sent.
func unspendUDF(t *testing.T, store *teranode_aerospike.Store, parent *bt.Tx, spend *utxo.Spend, keepBelow ...int) *teranode_aerospike.LuaMapResponse {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	args := make([]aerospike.Value, 0, 5+len(keepBelow))
	args = append(args,
		aerospike.NewIntegerValue(int(spend.Vout)),
		aerospike.NewValue(spend.UTXOHash[:]),
		aerospike.NewValue(spend.SpendingData.Bytes()),
		aerospike.NewValue(1300),
		aerospike.NewValue(spendHeightRetention),
	)

	for _, keep := range keepBelow {
		args = append(args, aerospike.NewIntegerValue(keep))
	}

	ret, aErr := store.GetClient().Execute(util.GetAerospikeWritePolicy(tSettings, 0), spendHeightKey(t, store, parent.TxIDChainHash()), teranode_aerospike.LuaPackage, "unspend", args...)
	require.NoError(t, aErr)

	res, err := store.ParseLuaMapResponse(ret)
	require.NoError(t, err)

	return res
}

// TestUnspendKeepBelowHeight_Aerospike pins the optional keepBelowHeight argument of the unspend
// UDF: a spend recorded below it stays and the answer says so, a spend recorded at or above it, a
// spend with no recorded height and a call with no argument are cleared as before.
func TestUnspendKeepBelowHeight_Aerospike(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, ctx, cleanup := initAerospike(t, tSettings, logger)
	t.Cleanup(cleanup)

	const spentAt = uint32(1200)

	parent := conflictingIndexParent(t, ctx, store, 7101, 7)
	child := conflictingIndexTx(t, parent, []uint32{0, 1, 2, 3, 4, 5, 6}, 1)
	parentKey := spendHeightKey(t, store, parent.TxIDChainHash())
	spends := spendHeightSpendsOf(t, child)

	for _, spend := range spends {
		res := spendHeightUDF(t, store, parent, spend, spentAt)
		require.Equal(t, teranode_aerospike.LuaStatusOK, res.Status)
	}

	// the output spent by older code: spent, with no height
	spendHeightClear(t, store.GetClient(), parentKey, 5)

	for _, tc := range []struct {
		name      string
		vout      int
		keepBelow []int
		wantKept  bool
	}{
		{name: "no argument clears the spend as before", vout: 0},
		{name: "recorded height below the argument: kept", vout: 1, keepBelow: []int{int(spentAt) + 1}, wantKept: true},
		{name: "recorded height at the argument: cleared", vout: 2, keepBelow: []int{int(spentAt)}},
		{name: "recorded height above the argument: cleared", vout: 3, keepBelow: []int{int(spentAt) - 1}},
		{name: "argument far above: kept", vout: 4, keepBelow: []int{4000000000}, wantKept: true},
		{name: "no recorded height: cleared whatever the argument", vout: 5, keepBelow: []int{4000000000}},
		{name: "argument zero keeps nothing", vout: 6, keepBelow: []int{0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := unspendUDF(t, store, parent, spends[tc.vout], tc.keepBelow...)
			require.Equal(t, teranode_aerospike.LuaStatusOK, res.Status, "a kept spend is not an error")
			require.Equal(t, tc.wantKept, res.Kept)

			entry := spendHeightEntry(t, store, parent.TxIDChainHash(), tc.vout)
			height, recorded := spendHeightAt(t, store.GetClient(), parentKey, tc.vout)

			if tc.wantKept {
				require.Len(t, entry, 68, "the output is still spent")
				require.Equal(t, spends[tc.vout].SpendingData.Bytes(), entry[32:68])
				require.True(t, recorded)
				require.Equal(t, spentAt, height, "the recorded height stays with the spend")

				return
			}

			require.Len(t, entry, 32, "the output is free")
			require.False(t, recorded, "unspend zeroes the height with the spend")
		})
	}

	t.Run("a spend the caller does not own is left alone whatever the argument", func(t *testing.T) {
		other := conflictingIndexTx(t, parent, []uint32{3}, 2)
		otherSpends := spendHeightSpendsOf(t, other)

		res := spendHeightUDF(t, store, parent, otherSpends[0], spentAt+10)
		require.Equal(t, teranode_aerospike.LuaStatusOK, res.Status)

		stranger := spendHeightSpendsOf(t, child)[3]

		res = unspendUDF(t, store, parent, stranger, int(spentAt)+500)
		require.Equal(t, teranode_aerospike.LuaStatusOK, res.Status)
		require.False(t, res.Kept, "a spend by another transaction is not the caller's to keep or clear")
		require.Len(t, spendHeightEntry(t, store, parent.TxIDChainHash(), 3), 68)
	})
}

// TestSpenderOwnsSpend pins when the record of a spender stands behind a spend that matches a
// historical one. A final record owns it. A tentative record owns the spends recorded at or after
// its own height, and none when it carries no height, because nothing then separates its spends
// from those of a pruned record. An absent record, a hold and a rollback own nothing.
func TestSpenderOwnsSpend(t *testing.T) {
	for _, tc := range []struct {
		name     string
		state    teranode_aerospike.SpenderState
		recorded uint32
		hasRec   bool
		want     bool
	}{
		{name: "a final record owns it", state: teranode_aerospike.SpenderState{}, recorded: 10, hasRec: true, want: true},
		{name: "a final record owns it even when the height is not known", state: teranode_aerospike.SpenderState{}, want: true},
		{name: "an absent record owns nothing", state: teranode_aerospike.SpenderState{Absent: true}, recorded: 1000, hasRec: true, want: false},
		{name: "a hold owns nothing", state: teranode_aerospike.SpenderState{Creating: true, Hold: true, HasUnminedSince: true, UnminedSince: 100}, recorded: 1000, hasRec: true, want: false},
		{name: "a rollback owns nothing", state: teranode_aerospike.SpenderState{Creating: true, RollingBack: true, HasUnminedSince: true, UnminedSince: 100}, recorded: 1000, hasRec: true, want: false},
		{name: "a tentative record owns a spend recorded after it", state: teranode_aerospike.SpenderState{Creating: true, HasUnminedSince: true, UnminedSince: 100}, recorded: 101, hasRec: true, want: true},
		{name: "a tentative record owns a spend recorded at its height", state: teranode_aerospike.SpenderState{Creating: true, HasUnminedSince: true, UnminedSince: 100}, recorded: 100, hasRec: true, want: true},
		{name: "a tentative record does not own a spend recorded before it", state: teranode_aerospike.SpenderState{Creating: true, HasUnminedSince: true, UnminedSince: 100}, recorded: 99, hasRec: true, want: false},
		{name: "a tentative record owns no spend whose height is not known", state: teranode_aerospike.SpenderState{Creating: true, HasUnminedSince: true, UnminedSince: 100}, want: false},
		{name: "a tentative record at height 0 owns no spend whose height is not known", state: teranode_aerospike.SpenderState{Creating: true, HasUnminedSince: true, UnminedSince: 0}, want: false},
		{name: "a tentative record that carries no height owns none", state: teranode_aerospike.SpenderState{Creating: true}, recorded: 1000, hasRec: true, want: false},
		{name: "a tentative record whose height is 0 owns every spend that has a height", state: teranode_aerospike.SpenderState{Creating: true, HasUnminedSince: true, UnminedSince: 0}, recorded: 5, hasRec: true, want: true},
		{name: "a tentative record with a negative height owns none", state: teranode_aerospike.SpenderState{Creating: true, HasUnminedSince: true, UnminedSince: -1}, recorded: 5, hasRec: true, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, teranode_aerospike.SpenderOwnsSpend(tc.state, tc.recorded, tc.hasRec))
		})
	}
}
