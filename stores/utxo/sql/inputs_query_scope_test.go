package sql

import (
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// The four columns that exist only so a whole transaction can be rebuilt. No
// fields.TxInpoints or fields.Utxos consumer reads any of them.
var bodyOnlyInputColumns = []string{
	"previous_tx_satoshis",
	"previous_tx_script",
	"unlocking_script",
	"sequence_number",
}

func TestInputsScopeFor(t *testing.T) {
	tests := []struct {
		name     string
		bins     []fields.FieldName
		expected inputsQueryScope
	}{
		{"no fields at all", nil, inputsQueryNone},
		{"scalars only", []fields.FieldName{fields.Fee, fields.SizeInBytes}, inputsQueryNone},
		{"block ids only", []fields.FieldName{fields.BlockIDs}, inputsQueryNone},
		// The two that used to drag the whole inputs row and no longer do.
		{"tx inpoints only", []fields.FieldName{fields.TxInpoints}, inputsQueryOutpoints},
		{"utxos only", []fields.FieldName{fields.Utxos}, inputsQueryNone},
		// The two that genuinely rebuild a transaction.
		{"tx", []fields.FieldName{fields.Tx}, inputsQueryFull},
		{"inputs", []fields.FieldName{fields.Inputs}, inputsQueryFull},
		// A wide request wins over a narrow one in the same set.
		{"tx and tx inpoints", []fields.FieldName{fields.TxInpoints, fields.Tx}, inputsQueryFull},
		{"inputs and utxos", []fields.FieldName{fields.Utxos, fields.Inputs}, inputsQueryFull},
		// The standard metadata sets, which is what most callers pass.
		{"MetaFields", utxo.MetaFields, inputsQueryOutpoints},
		{"MetaFieldsWithTx", utxo.MetaFieldsWithTx, inputsQueryFull},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, inputsScopeFor(tc.bins))
		})
	}
}

func TestNeedsOutputsQuery(t *testing.T) {
	require.True(t, needsOutputsQuery([]fields.FieldName{fields.Tx}))
	require.True(t, needsOutputsQuery([]fields.FieldName{fields.Outputs}))

	// fields.Utxos runs its own query over outputs and used the outputs read only
	// for a slice length, which the vout-indexed sizing no longer needs.
	require.False(t, needsOutputsQuery([]fields.FieldName{fields.Utxos}))
	require.False(t, needsOutputsQuery([]fields.FieldName{fields.TxInpoints}))
	require.False(t, needsOutputsQuery(utxo.MetaFields))
	require.True(t, needsOutputsQuery(utxo.MetaFieldsWithTx))
}

// TestInputsQuerySQLOmitsBodyColumnsForOutpointScope is the regression guard. The
// point of the scope is the emitted column list, so assert on the SQL itself: a
// future edit that widens the outpoint projection back out fails here.
func TestInputsQuerySQLOmitsBodyColumnsForOutpointScope(t *testing.T) {
	outpoints := inputsQuerySQL(inputsQueryOutpoints, "transaction_id = $1")

	require.Contains(t, outpoints, "previous_transaction_hash")
	require.Contains(t, outpoints, "previous_tx_idx")

	for _, col := range bodyOnlyInputColumns {
		require.NotContains(t, outpoints, col,
			"outpoint-only scope must not select %s", col)
	}

	// Ordering is load bearing: TxInpoints ordering has to match input order.
	require.Contains(t, outpoints, "ORDER BY idx")

	full := inputsQuerySQL(inputsQueryFull, "transaction_id = $1")
	for _, col := range bodyOnlyInputColumns {
		require.Contains(t, full, col, "full scope must still select %s", col)
	}
}

// TestScanTargetsForInputScopeMatchesColumnCount pins the two halves together. A
// column list and a Scan target list that disagree is a runtime error on every
// row, so count them against each other here instead.
func TestScanTargetsForInputScopeMatchesColumnCount(t *testing.T) {
	for _, tc := range []struct {
		scope inputsQueryScope
		cols  int
	}{
		{inputsQueryOutpoints, 2},
		{inputsQueryFull, 6},
	} {
		var (
			hashBytes  []byte
			prevTxIdx  int64
			input      = &bt.Input{}
			selectPart = strings.SplitN(inputsQuerySQL(tc.scope, "transaction_id = $1"), " FROM ", 2)[0]
		)

		targets := scanTargetsForInputScope(tc.scope, &hashBytes, &prevTxIdx, input)
		require.Len(t, targets, tc.cols)
		require.Len(t, strings.Split(strings.TrimPrefix(selectPart, "SELECT "), ","), tc.cols)
	}
}
