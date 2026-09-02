package aerospike

import (
	"fmt"
	"strings"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

func TestDescribeAerospikeBatchRecordIncludesResponseShape(t *testing.T) {
	batchRecord := &aerospike.BatchWrite{}
	batchRecord.ResultCode = types.OK
	batchRecord.Record = &aerospike.Record{
		Generation: 7,
		Bins: aerospike.BinMap{
			LuaSuccess.String(): nil,
			"other":             []byte{1, 2, 3},
		},
	}

	got := describeAerospikeBatchRecord(batchRecord)

	for _, want := range []string{
		"resultCode=OK",
		"inDoubt=false",
		"generation=7",
		"SUCCESS:<nil>",
		"other:[]uint8(len=3)",
	} {
		require.Contains(t, got, want)
	}
}

func TestDescribeAerospikeBatchRecordIncludesBatchError(t *testing.T) {
	batchRecord := &aerospike.BatchWrite{}
	batchRecord.ResultCode = types.PARAMETER_ERROR
	batchRecord.Err = aerospike.ErrInvalidParam

	got := describeAerospikeBatchRecord(batchRecord)

	for _, want := range []string{
		"resultCode=PARAMETER_ERROR",
		"recordErr=ResultCode: PARAMETER_ERROR",
		"record=<nil>",
	} {
		require.Contains(t, got, want)
	}
}

func TestDiagnosticHelpersHandleNilInputs(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "nil batch record", got: describeAerospikeBatchRecord(nil), want: "batchRecord=<nil>"},
		{name: "nil record", got: describeAerospikeRecord(nil), want: "record=<nil>"},
		{name: "nil hash", got: describeChainHash(nil), want: "<nil>"},
		{name: "nil spend", got: describeUTXOSpend(nil), want: "<nil>"},
		{name: "nil txid spend", got: describeUTXOSpend(&utxo.Spend{}), want: "<nil>:0"},
		{name: "nil bins", got: describeAerospikeRecord(&aerospike.Record{}), want: "generation=0, bins=<nil>"},
		{name: "empty bins", got: describeAerospikeBins(aerospike.BinMap{}), want: "{}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.got)
		})
	}
}

// TestDescribeAerospikeValueRendersScalarsNotTypes covers the arms that decide
// whether a log line is useful: the counters and flags that explain a batch
// failure must render their value, while bulk containers stay summarised by
// length so one record cannot flood the line.
func TestDescribeAerospikeValueRendersScalarsNotTypes(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  string
	}{
		{name: "nil", value: nil, want: "<nil>"},
		{name: "int counter", value: 42, want: "42"},
		{name: "int64 counter", value: int64(-7), want: "-7"},
		{name: "uint32 height", value: uint32(910000), want: "910000"},
		{name: "float", value: 1.5, want: "1.5"},
		{name: "bool true", value: true, want: "true"},
		{name: "bool false", value: false, want: "false"},
		{name: "short string", value: "abc", want: `"abc"`},
		{name: "byte slice", value: []byte{1, 2, 3, 4}, want: "[]uint8(len=4)"},
		{name: "interface slice", value: []interface{}{1, 2}, want: "[]interface {}(len=2)"},
		{name: "iface map", value: map[interface{}]interface{}{1: 2}, want: "map[interface {}]interface {}(len=1)"},
		{name: "string map", value: map[string]interface{}{"a": 1}, want: "map[string]interface {}(len=1)"},
		{name: "op results", value: aerospike.OpResults{1, 2, 3}, want: "aerospike.OpResults(len=3)"},
		{name: "unhandled type falls back to type", value: struct{ A int }{A: 1}, want: "struct { A int }"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, describeAerospikeValue(tt.value))
		})
	}
}

func TestDescribeAerospikeValueCapsLongStrings(t *testing.T) {
	long := strings.Repeat("x", maxStringValueLen+50)

	got := describeAerospikeValue(long)

	require.Equal(t,
		fmt.Sprintf("%q...(len=%d)", strings.Repeat("x", maxStringValueLen), len(long)),
		got)
	require.Less(t, len(got), len(long),
		"rendered value must be shorter than the raw string")
}

// TestDescribeAerospikeBinsPrioritisesDiagnosticBins is the regression test for
// the truncation defect: with plain alphabetical ordering and a cap of 8, a full
// 33-bin record always rendered blockHeights…deletedChildren and always dropped
// the UTXO accounting counters that explain the failure.
func TestDescribeAerospikeBinsPrioritisesDiagnosticBins(t *testing.T) {
	bins := aerospike.BinMap{}

	// Every bin the store defines, so the truncation path is exercised for real.
	for _, name := range []fields.FieldName{
		fields.Tx, fields.TxID, fields.Inputs, fields.Outputs, fields.External,
		fields.LockTime, fields.Version, fields.Fee, fields.SizeInBytes,
		fields.ExtendedSize, fields.TxInpoints, fields.IsCoinbase,
		fields.Conflicting, fields.ConflictingChildren, fields.Locked,
		fields.Creating, fields.UtxoSpendableIn, fields.SpendingHeight,
		fields.Utxos, fields.TotalUtxos, fields.RecordUtxos, fields.SpentUtxos,
		fields.TotalExtraRecs, fields.SpentExtraRecs, fields.BlockIDs,
		fields.BlockHeights, fields.SubtreeIdxs, fields.Reassignments,
		fields.DeleteAtHeight, fields.CreatedAt, fields.UnminedSince,
		fields.PreserveUntil, fields.DeletedChildren,
	} {
		bins[name.String()] = 1
	}

	require.Len(t, bins, 33, "guard against fields.go drifting from this fixture")

	got := describeAerospikeBins(bins)

	// The bins that explain a BatchOperate failure must survive truncation.
	for _, name := range []fields.FieldName{
		fields.SpentUtxos, fields.TotalUtxos, fields.RecordUtxos, fields.Utxos,
		fields.UtxoSpendableIn, fields.SpendingHeight, fields.Conflicting,
		fields.Locked, fields.DeleteAtHeight,
	} {
		require.Contains(t, got, name.String()+":1",
			"diagnostic bin %s must survive truncation", name)
	}

	require.Contains(t, got, "more", "truncation marker expected for a 33-bin record")

	// spentUtxos is the highest-priority bin, so it must lead the rendering —
	// ahead of the alphabetically-first bin that previously crowded it out.
	require.Less(t, strings.Index(got, fields.SpentUtxos.String()),
		strings.Index(got, fields.BlockHeights.String()),
		"priority bins must precede the alphabetical tail")
}

func TestDescribeAerospikeBinsIsDeterministic(t *testing.T) {
	bins := aerospike.BinMap{
		"zeta":                      1,
		"alpha":                     2,
		fields.SpentUtxos.String():  3,
		fields.TotalUtxos.String():  4,
		fields.Conflicting.String(): true,
	}

	first := describeAerospikeBins(bins)

	for i := 0; i < 20; i++ {
		require.Equal(t, first, describeAerospikeBins(bins),
			"bin rendering must not depend on map iteration order")
	}

	// Priority order, then alphabetical tail.
	require.Equal(t, "{spentUtxos:3, totalUtxos:4, conflicting:true, alpha:2, zeta:1}", first)
}
