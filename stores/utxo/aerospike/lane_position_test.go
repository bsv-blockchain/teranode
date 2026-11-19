package aerospike_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
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
	t.Run("burns", func(t *testing.T) { tests.LanePositionBurns(t, store, "aerospike") })
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

		_, err := store.Create(ctx, tx, 100, utxo.WithLanePosition(position))
		require.NoError(t, err)

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

	// The size limits above are the store's own. This is the server's: a record under them that
	// the namespace still refuses is rebuilt as external by a separate call, which must pass the
	// lane position again. No configuration in the repo makes the server refuse one, so the batch
	// answers RECORD_TOO_BIG once and the rebuild then runs against the real server.
	t.Run("a record the server refuses as too big keeps the bin when it is rebuilt", func(t *testing.T) {
		cleanDB(t, client)

		tx := createTransactionWithOutputs(3)

		var refused atomic.Bool

		store.SetBatchOperateFn(func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
			if !refused.Swap(true) {
				for _, record := range records {
					record.BatchRec().Err = &aerospike.AerospikeError{ResultCode: types.RECORD_TOO_BIG}
				}

				return nil
			}

			return client.BatchOperate(policy, records)
		})
		t.Cleanup(func() { store.SetBatchOperateFn(nil) })

		_, err := store.Create(ctx, tx, 100, utxo.WithLanePosition(position))
		require.NoError(t, err)
		require.True(t, refused.Load(), "the create went through the refusal")

		v, ok := rawBin(t, tx, 0)
		require.True(t, ok, "the rebuilt master record must carry the lane position bin")
		require.Equal(t, int(lane.ToSigned(position)), v)

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

// TestLanePositionCreateConflictingWithNoPositionLeavesParentsAlone pins that Create takes its
// position before it writes a conflicting transaction's note onto the parents. A conflicting create
// that cannot get a position must fail with the parents untouched, or they would name a child that
// was never written. SpendAndCreate takes its position before it calls Create, so only a direct
// Create reaches this order.
func TestLanePositionCreateConflictingWithNoPositionLeavesParentsAlone(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, _, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	cleanDB(t, client)

	parent := createTransactionWithOutputs(1)
	_, err := store.Create(ctx, parent, 1)
	require.NoError(t, err)

	child := &bt.Tx{}
	require.NoError(t, child.From(parent.TxID(), 0, parent.Outputs[0].LockingScript.String(), parent.Outputs[0].Satoshis))
	require.NoError(t, child.AddP2PKHOutputFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))

	source := tests.NewScriptedLanePositions()
	store.SetLanePositionSource(source)
	t.Cleanup(func() { store.SetLanePositionSource(nil) })

	_, err = store.Create(ctx, child, 2, utxo.WithConflicting(true))
	require.ErrorIs(t, err, errors.ErrServiceUnavailable)
	require.Equal(t, 1, source.Calls(), "the create asked for a position")

	got, err := store.Get(ctx, parent.TxIDChainHash(), fields.ConflictingChildren)
	require.NoError(t, err)
	require.Empty(t, got.ConflictingChildren, "a create with no position wrote a conflict note onto its parent")
}
