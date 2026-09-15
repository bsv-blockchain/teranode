package sql

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/stretchr/testify/require"
)

// TestOutpointScopeNeverAttachesAHollowTx pins what Data.Tx looks like when the
// inputs read ran in the outpoint scope, on both read paths.
//
// The outpoint scope scans only previous_transaction_hash and previous_tx_idx,
// so the inputs it builds have no unlocking script, sequence number or previous
// output. Attaching those inputs to Data.Tx produced a transaction that
// meta.Data.TxIsSerializable accepts (it has inputs, and any outputs present are
// non-nil) but whose bytes are not the transaction's. The batch path did that
// for fields.TxInpoints alone, so the answer also depended on
// utxostore_getBatcherSize: nil Tx at the default of 1, hollow Tx above it.
//
// The end state asserted is the value a caller receives from each path, for
// each field set, and that the two paths agree.
func TestOutpointScopeNeverAttachesAHollowTx(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	// The byte comparisons in the fields.Tx cases only prove the extended inputs
	// came back if the fixture carries them.
	require.True(t, tx.IsExtended())

	_, err := store.Create(ctx, tx, 12345)
	require.NoError(t, err)

	hash := tx.TxIDChainHash()

	parentHashes := func(t *testing.T, data *meta.Data) {
		t.Helper()

		require.Len(t, data.TxInpoints.ParentTxHashes, 1)
		require.Equal(t, *tx.Inputs[0].PreviousTxIDChainHash(), data.TxInpoints.ParentTxHashes[0])
	}

	tests := []struct {
		name  string
		bins  []fields.FieldName
		check func(t *testing.T, data *meta.Data)
	}{
		{
			name: "tx inpoints only",
			bins: []fields.FieldName{fields.TxInpoints},
			check: func(t *testing.T, data *meta.Data) {
				parentHashes(t, data)
				require.Nil(t, data.Tx, "fields.TxInpoints must not attach a transaction")
				require.False(t, data.TxIsSerializable())
			},
		},
		{
			name: "MetaFields",
			bins: utxo.MetaFields,
			check: func(t *testing.T, data *meta.Data) {
				parentHashes(t, data)
				require.Nil(t, data.Tx, "utxo.MetaFields must not attach a transaction")
				require.False(t, data.TxIsSerializable())
			},
		},
		{
			name: "tx inpoints with outputs",
			bins: []fields.FieldName{fields.TxInpoints, fields.Outputs},
			check: func(t *testing.T, data *meta.Data) {
				parentHashes(t, data)
				require.NotNil(t, data.Tx, "fields.Outputs attaches the outputs")
				require.Len(t, data.Tx.Outputs, len(tx.Outputs))
				require.Empty(t, data.Tx.Inputs, "outpoint-only inputs must not be attached to Data.Tx")
				require.False(t, data.TxIsSerializable(),
					"a transaction without its real inputs must not report itself serializable")
			},
		},
		{
			name: "tx",
			bins: []fields.FieldName{fields.Tx},
			check: func(t *testing.T, data *meta.Data) {
				require.True(t, data.TxIsSerializable())
				require.Equal(t, tx.ExtendedBytes(), data.Tx.ExtendedBytes(),
					"fields.Tx must rebuild the whole transaction byte for byte")
			},
		},
		{
			name: "tx with tx inpoints",
			bins: []fields.FieldName{fields.Tx, fields.TxInpoints},
			check: func(t *testing.T, data *meta.Data) {
				parentHashes(t, data)
				require.True(t, data.TxIsSerializable())
				require.Equal(t, tx.ExtendedBytes(), data.Tx.ExtendedBytes())
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name+"/unbatched", func(t *testing.T) {
			data, err := store.getUnbatched(ctx, hash, tc.bins)
			require.NoError(t, err)
			require.NotNil(t, data)
			tc.check(t, data)
		})

		t.Run(tc.name+"/batch", func(t *testing.T) {
			items := []*utxo.UnresolvedMetaData{{Hash: *hash, Idx: 0}}

			require.NoError(t, store.BatchDecorate(ctx, items, tc.bins...))
			require.NoError(t, items[0].Err)
			require.NotNil(t, items[0].Data)
			tc.check(t, items[0].Data)
		})
	}
}
