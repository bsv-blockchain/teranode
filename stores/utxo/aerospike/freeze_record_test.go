package aerospike

import (
	"math"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// TestReadFreezeRecordShape pins the reader's three answers — no record, a record, and
// storage damage — for the freeze bins block validation judges blocks by. Absent data is
// "no record"; data of the wrong shape must never be.
func TestReadFreezeRecordShape(t *testing.T) {
	fromBin, untilBin, expBin := fields.UtxoFreezeFrom.String(), fields.UtxoFreezeUntil.String(), fields.UtxoFreezeExp.String()

	t.Run("absent bins read as no record", func(t *testing.T) {
		rec, err := readFreezeRecord(aerospike.BinMap{}, 5)
		require.NoError(t, err)
		require.False(t, rec.present)
	})

	t.Run("an absent entry reads as no record", func(t *testing.T) {
		rec, err := readFreezeRecord(aerospike.BinMap{fromBin: map[interface{}]interface{}{3: 10}}, 5)
		require.NoError(t, err)
		require.False(t, rec.present)
	})

	t.Run("a well-formed record", func(t *testing.T) {
		rec, err := readFreezeRecord(aerospike.BinMap{
			fromBin:  map[interface{}]interface{}{5: 100},
			untilBin: map[interface{}]interface{}{5: 200},
			expBin:   map[interface{}]interface{}{5: true},
		}, 5)
		require.NoError(t, err)
		require.Equal(t, freezeRecord{present: true, from: 100, until: 200, policyExpires: true}, rec)
	})

	t.Run("a false policy-expiry entry reads as not expiring", func(t *testing.T) {
		rec, err := readFreezeRecord(aerospike.BinMap{
			fromBin: map[interface{}]interface{}{5: 100},
			expBin:  map[interface{}]interface{}{5: false},
		}, 5)
		require.NoError(t, err)
		require.Equal(t, freezeRecord{present: true, from: 100}, rec)
	})

	t.Run("a policy-expiry entry that is not a bool is storage damage, never true", func(t *testing.T) {
		for name, malformed := range map[string]interface{}{"int": 1, "string": "true", "bytes": []byte{1}} {
			_, err := readFreezeRecord(aerospike.BinMap{
				fromBin: map[interface{}]interface{}{5: 100},
				expBin:  map[interface{}]interface{}{5: malformed},
			}, 5)
			require.ErrorIs(t, err, errors.ErrStorageError, "a %s entry must be storage damage", name)
		}
	})

	t.Run("a from entry alone is an open-ended record", func(t *testing.T) {
		rec, err := readFreezeRecord(aerospike.BinMap{fromBin: map[interface{}]interface{}{5: 100}}, 5)
		require.NoError(t, err)
		require.Equal(t, freezeRecord{present: true, from: 100}, rec)
	})

	t.Run("a from bin that is not a map is storage damage", func(t *testing.T) {
		_, err := readFreezeRecord(aerospike.BinMap{fromBin: "garbage"}, 5)
		require.ErrorIs(t, err, errors.ErrStorageError)
	})

	t.Run("a from entry that is not an int is storage damage", func(t *testing.T) {
		_, err := readFreezeRecord(aerospike.BinMap{fromBin: map[interface{}]interface{}{5: "100"}}, 5)
		require.ErrorIs(t, err, errors.ErrStorageError)
	})

	t.Run("an until entry that is not an int is storage damage", func(t *testing.T) {
		_, err := readFreezeRecord(aerospike.BinMap{
			fromBin:  map[interface{}]interface{}{5: 100},
			untilBin: map[interface{}]interface{}{5: []byte{1}},
		}, 5)
		require.ErrorIs(t, err, errors.ErrStorageError)
	})

	t.Run("a height outside the uint32 range is storage damage, never a wrapped height", func(t *testing.T) {
		for name, bins := range map[string]aerospike.BinMap{
			"negative from":         {fromBin: map[interface{}]interface{}{5: -1}},
			"from above MaxUint32":  {fromBin: map[interface{}]interface{}{5: math.MaxUint32 + 1}},
			"negative until":        {fromBin: map[interface{}]interface{}{5: 100}, untilBin: map[interface{}]interface{}{5: -1}},
			"until above MaxUint32": {fromBin: map[interface{}]interface{}{5: 100}, untilBin: map[interface{}]interface{}{5: math.MaxUint32 + 1}},
		} {
			_, err := readFreezeRecord(bins, 5)
			require.ErrorIs(t, err, errors.ErrStorageError, name)
		}
	})

	t.Run("the range bounds themselves are heights", func(t *testing.T) {
		rec, err := readFreezeRecord(aerospike.BinMap{
			fromBin:  map[interface{}]interface{}{5: 0},
			untilBin: map[interface{}]interface{}{5: math.MaxUint32},
		}, 5)
		require.NoError(t, err)
		require.Equal(t, freezeRecord{present: true, from: 0, until: math.MaxUint32}, rec)
	})
}

func TestCollectFreezeRecords(t *testing.T) {
	fromBin, untilBin := fields.UtxoFreezeFrom.String(), fields.UtxoFreezeUntil.String()

	t.Run("records are keyed by absolute output index", func(t *testing.T) {
		records, err := collectFreezeRecords(aerospike.BinMap{
			fromBin:  map[interface{}]interface{}{2: 100, 7: 0},
			untilBin: map[interface{}]interface{}{2: 200},
		}, 128, nil)
		require.NoError(t, err)
		require.Len(t, records, 2)
		require.Equal(t, uint32(100), records[130].From)
		require.Equal(t, uint32(200), records[130].Until)
		require.Equal(t, uint32(0), records[135].Until, "no until entry means no end")
	})

	t.Run("no from bin adds nothing", func(t *testing.T) {
		records, err := collectFreezeRecords(aerospike.BinMap{}, 0, nil)
		require.NoError(t, err)
		require.Nil(t, records)
	})

	t.Run("a key that is not an offset is storage damage", func(t *testing.T) {
		_, err := collectFreezeRecords(aerospike.BinMap{fromBin: map[interface{}]interface{}{"x": 100}}, 0, nil)
		require.ErrorIs(t, err, errors.ErrStorageError)
	})
}

func TestMarkedFreezeExtraRecords(t *testing.T) {
	recsBin := fields.UtxoFreezeRecs.String()

	t.Run("no marker names no extra records", func(t *testing.T) {
		nums, err := markedFreezeExtraRecords(aerospike.BinMap{})
		require.NoError(t, err)
		require.Empty(t, nums)
	})

	t.Run("record numbers come back sorted", func(t *testing.T) {
		nums, err := markedFreezeExtraRecords(aerospike.BinMap{recsBin: map[interface{}]interface{}{7: 1, 2: 1, 3: 1}})
		require.NoError(t, err)
		require.Equal(t, []int{2, 3, 7}, nums)
	})

	t.Run("a marker that is not a map is storage damage", func(t *testing.T) {
		_, err := markedFreezeExtraRecords(aerospike.BinMap{recsBin: "garbage"})
		require.ErrorIs(t, err, errors.ErrStorageError)
	})

	t.Run("a record number that is not positive is storage damage", func(t *testing.T) {
		_, err := markedFreezeExtraRecords(aerospike.BinMap{recsBin: map[interface{}]interface{}{0: 1}})
		require.ErrorIs(t, err, errors.ErrStorageError)
	})
}
