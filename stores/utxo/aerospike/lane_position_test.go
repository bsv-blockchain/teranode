package aerospike_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranodeaerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/stretchr/testify/require"
)

// TestLanePositionContract runs the shared lane position contract on a real
// Aerospike.
func TestLanePositionContract(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	_, store, _, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	tests.LanePositionContract(t, store)
}

// TestLanePositionBin checks what the contract cannot see: the raw bin, on the
// master record only, for the paths that store a transaction outside the
// record.
func TestLanePositionBin(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, _, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	rawBin := func(t *testing.T, tx *bt.Tx, recordIdx uint32) (interface{}, bool) {
		t.Helper()

		keySource := uaerospike.CalculateKeySourceInternal(tx.TxIDChainHash(), recordIdx)
		key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource)
		require.NoError(t, err)

		rec, err := client.Get(util.GetAerospikeReadPolicy(tSettings), key)
		require.NoError(t, err)
		require.NotNil(t, rec)

		v, ok := rec.Bins[fields.LanePosition.String()]

		return v, ok
	}

	// A lane 200 position is negative as an int64, so this also pins the
	// bit-for-bit encoding in the stored bin.
	position := lane.Pack(200, 12345)

	t.Run("a transaction over the in-record size limit goes external and keeps the bin", func(t *testing.T) {
		cleanDB(t, client)

		// The test batch size is small, so reach the size limit with a few large
		// scripts rather than many outputs.
		tx := createTransactionWithOutputs(1)
		for range 3 {
			tx.Outputs = append(tx.Outputs, &bt.Output{
				Satoshis:      0,
				LockingScript: bscript.NewFromBytes(append([]byte{0x00, 0x6a, 0x4d, 0x00, 0x30}, make([]byte, 0x3000)...)),
			})
		}

		require.Greater(t, len(tx.ExtendedBytes()), teranodeaerospike.MaxTxSizeInStoreInBytes, "pre-condition: the tx must be too big to store in the record")
		require.Less(t, len(tx.Outputs), tSettings.UtxoStore.UtxoBatchSize, "pre-condition: the tx must fit one record's outputs")

		md, err := store.Create(ctx, tx, 100, utxo.WithLanePosition(position))
		require.NoError(t, err)
		require.Equal(t, position, md.LanePosition)

		v, ok := rawBin(t, tx, 0)
		require.True(t, ok, "the master record must carry the lane position bin")
		require.Equal(t, int(lane.ToSigned(position)), v)

		got, err := store.Get(ctx, tx.TxIDChainHash(), fields.LanePosition)
		require.NoError(t, err)
		require.Equal(t, position, got.LanePosition)
	})

	t.Run("a transaction split across records writes the bin on the master only", func(t *testing.T) {
		cleanDB(t, client)

		tx := createTransactionWithOutputs(tSettings.UtxoStore.UtxoBatchSize + 1)

		_, err := store.Create(ctx, tx, 100, utxo.WithLanePosition(position))
		require.NoError(t, err)

		v, ok := rawBin(t, tx, 0)
		require.True(t, ok, "the master record must carry the lane position bin")
		require.Equal(t, int(lane.ToSigned(position)), v)

		_, ok = rawBin(t, tx, 1)
		require.False(t, ok, "a pagination record must not carry the lane position bin")

		got, err := store.Get(ctx, tx.TxIDChainHash(), fields.LanePosition)
		require.NoError(t, err)
		require.Equal(t, position, got.LanePosition)
	})

	t.Run("a create without the option writes no bin", func(t *testing.T) {
		cleanDB(t, client)

		for name, tx := range map[string]*bt.Tx{
			"small":    createTransactionWithOutputs(3),
			"external": createTransactionWithOutputs(tSettings.UtxoStore.UtxoBatchSize + 1),
		} {
			_, err := store.Create(ctx, tx, 100)
			require.NoError(t, err, name)

			_, ok := rawBin(t, tx, 0)
			require.False(t, ok, "%s: no lane position must mean no bin at all", name)

			got, err := store.Get(ctx, tx.TxIDChainHash(), fields.LanePosition)
			require.NoError(t, err, name)
			require.Equal(t, lane.None, got.LanePosition, name)
		}
	})
}
