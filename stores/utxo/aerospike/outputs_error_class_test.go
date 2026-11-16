package aerospike_test

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// Malformed local bins are storage faults, not evidence that a transaction or
// its serving peer is invalid. One bad record must also leave its batch peers
// readable.
func TestGetOutputsProjection_StorageFaults(t *testing.T) {
	settings := test.CreateBaseTestSettings(t)
	client, store, ctx, cleanup := initAerospike(t, settings, ulogger.NewErrorTestLogger(t))
	defer cleanup()
	cleanDB(t, client)
	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)
	healthy := createTransactionWithOutputs(2)
	_, err = store.Create(ctx, healthy, 0)
	require.NoError(t, err)
	hash := tx.TxIDChainHash()
	key, aErr := aerospike.NewKey(store.GetNamespace(), store.GetName(), hash[:])
	require.Nil(t, aErr)
	for _, tc := range []struct {
		name  string
		value interface{}
	}{
		{"truncated_output", []byte{0xff}},
		{"wrong_output_type", "invalid output"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bin := aerospike.NewListValue([]interface{}{tc.value})
			require.Nil(t, client.PutBins(nil, key, aerospike.NewBin(fields.Outputs.String(), bin)))
			_, err := store.Get(ctx, hash, fields.Outputs)
			require.Error(t, err)
			require.True(t, errors.Is(err, errors.ErrStorageError), "got %v", err)
			require.False(t, errors.Is(err, errors.ErrTxInvalid), "got %v", err)
			items := []*utxo.UnresolvedMetaData{{Hash: *hash}, {Hash: *healthy.TxIDChainHash()}}
			require.NoError(t, store.BatchDecorate(ctx, items, fields.Outputs))
			require.True(t, errors.Is(items[0].Err, errors.ErrStorageError), "got %v", items[0].Err)
			require.False(t, errors.Is(items[0].Err, errors.ErrTxInvalid))
			require.NoError(t, items[1].Err)
			require.NotNil(t, items[1].Data.Tx)
			require.Len(t, items[1].Data.Tx.Outputs, len(healthy.Outputs))
			for i, want := range healthy.Outputs {
				require.Equal(t, want.Satoshis, items[1].Data.Tx.Outputs[i].Satoshis)
				require.Equal(t, *want.LockingScript, *items[1].Data.Tx.Outputs[i].LockingScript)
			}
		})
	}
}
