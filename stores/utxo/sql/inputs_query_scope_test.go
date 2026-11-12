package sql

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
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

// TestInputScanRowTargetsMatchColumnCount pins the two halves together. A
// column list and a Scan target list that disagree is a runtime error on every
// row, so count them against each other here instead, with and without the
// leading transaction_id target the batch read adds.
func TestInputScanRowTargetsMatchColumnCount(t *testing.T) {
	for _, tc := range []struct {
		scope inputsQueryScope
		cols  int
	}{
		{inputsQueryOutpoints, 2},
		{inputsQueryFull, 6},
	} {
		var row inputScanRow

		selectPart := strings.SplitN(inputsQuerySQL(tc.scope, "transaction_id = $1"), " FROM ", 2)[0]
		require.Len(t, strings.Split(strings.TrimPrefix(selectPart, "SELECT "), ","), tc.cols)

		require.Len(t, row.scanTargets(tc.scope), tc.cols)

		var txID int

		targets := row.scanTargets(tc.scope, &txID)
		require.Len(t, targets, tc.cols+1)
		require.Same(t, &txID, targets[0])
	}
}

// TestInputScanRowReuseKeepsEveryInput reads a multi-input transaction back
// through both read paths and checks every input field by field. Both paths
// scan all rows into one reused inputScanRow, so this is what shows that no
// row's scripts, sequence number or outpoint leak into another on the real
// driver.
func TestInputScanRowReuseKeepsEveryInput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _ := setup(ctx, t)

	tx := bt.NewTx()

	for i := 0; i < 3; i++ {
		parent := chainhash.HashH([]byte{byte(i)})

		input := &bt.Input{
			PreviousTxOutIndex: uint32(i + 1),
			PreviousTxSatoshis: uint64(1000 * (i + 1)),
			PreviousTxScript:   bscript.NewFromBytes(bytes.Repeat([]byte{byte(0x70 + i)}, 20+i)),
			UnlockingScript:    bscript.NewFromBytes(bytes.Repeat([]byte{byte(0x51 + i)}, 10+i)),
			SequenceNumber:     uint32(0xfffffff0 + i),
		}
		require.NoError(t, input.PreviousTxIDAdd(&parent))

		tx.Inputs = append(tx.Inputs, input)
	}

	tx.Outputs = append(tx.Outputs, &bt.Output{
		Satoshis:      500,
		LockingScript: bscript.NewFromBytes(bytes.Repeat([]byte{0xab}, 25)),
	})

	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	hash := tx.TxIDChainHash()

	check := func(t *testing.T, got *bt.Tx) {
		t.Helper()

		require.NotNil(t, got)
		require.Len(t, got.Inputs, len(tx.Inputs))

		for i, want := range tx.Inputs {
			in := got.Inputs[i]
			require.Equal(t, want.PreviousTxID(), in.PreviousTxID(), "input %d parent", i)
			require.Equal(t, want.PreviousTxOutIndex, in.PreviousTxOutIndex, "input %d vout", i)
			require.Equal(t, want.PreviousTxSatoshis, in.PreviousTxSatoshis, "input %d satoshis", i)
			require.Equal(t, want.PreviousTxScript.Bytes(), in.PreviousTxScript.Bytes(), "input %d previous script", i)
			require.Equal(t, want.UnlockingScript.Bytes(), in.UnlockingScript.Bytes(), "input %d unlocking script", i)
			require.Equal(t, want.SequenceNumber, in.SequenceNumber, "input %d sequence", i)
		}

		for i := 1; i < len(got.Inputs); i++ {
			require.NotSame(t, got.Inputs[0].UnlockingScript, got.Inputs[i].UnlockingScript)
			require.NotSame(t, got.Inputs[0].PreviousTxScript, got.Inputs[i].PreviousTxScript)
		}

		require.Equal(t, tx.ExtendedBytes(), got.ExtendedBytes())
	}

	t.Run("unbatched", func(t *testing.T) {
		data, err := store.getUnbatched(ctx, hash, []fields.FieldName{fields.Tx})
		require.NoError(t, err)
		check(t, data.Tx)
	})

	t.Run("batched", func(t *testing.T) {
		items := []*utxo.UnresolvedMetaData{{Hash: *hash, Idx: 0}}
		require.NoError(t, store.BatchDecorate(ctx, items, fields.Tx))
		require.NoError(t, items[0].Err)
		check(t, items[0].Data.Tx)
	})

	t.Run("outpoint scope", func(t *testing.T) {
		want, err := subtree.NewTxInpointsFromInputs(tx.Inputs)
		require.NoError(t, err)

		data, err := store.getUnbatched(ctx, hash, []fields.FieldName{fields.TxInpoints})
		require.NoError(t, err)
		require.Equal(t, want.GetTxInpoints(), data.TxInpoints.GetTxInpoints())

		items := []*utxo.UnresolvedMetaData{{Hash: *hash, Idx: 0}}
		require.NoError(t, store.BatchDecorate(ctx, items, fields.TxInpoints))
		require.NoError(t, items[0].Err)
		require.Equal(t, want.GetTxInpoints(), items[0].Data.TxInpoints.GetTxInpoints())
	})
}
