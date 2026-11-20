package aerospike_test

import (
	"context"
	"encoding/binary"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranodeaerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/uaerospike"
	"github.com/prometheus/client_golang/prometheus"
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

	// A KEY_EXISTS can be the create's own retried write, so no refusal goes back; only a spend
	// refused before the create ran does.
	t.Run("returns", func(t *testing.T) { tests.LanePositionReturns(t, store, "aerospike", tests.LaneReturnRules{}, nil) })
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

// laneParent is a parent with n outputs, distinct by seed.
func laneParent(t *testing.T, seed uint32, n int) *bt.Tx {
	t.Helper()

	tx := bt.NewTx()
	require.NoError(t, tx.From("00000000000000000000000000000000000000000000000000000000000000aa", seed,
		"76a914000000000000000000000000000000000000000088ac", 1_000_000))

	for range n {
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1_000))
	}

	return tx
}

// laneChild spends the named outputs of parent.
func laneChild(t *testing.T, parent *bt.Tx, vouts ...uint32) *bt.Tx {
	t.Helper()

	child := bt.NewTx()

	for _, vout := range vouts {
		require.NoError(t, child.FromUTXOs(&bt.UTXO{
			TxIDHash:      parent.TxIDChainHash(),
			Vout:          vout,
			LockingScript: parent.Outputs[vout].LockingScript,
			Satoshis:      parent.Outputs[vout].Satoshis,
		}))
	}

	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))

	return child
}

// recordBin reads one bin of the record recordIdx of tx; ok is false when the bin is absent.
func recordBin(t *testing.T, client *uaerospike.Client, store *teranodeaerospike.Store, tx *bt.Tx, recordIdx uint32,
	bin fields.FieldName) (interface{}, bool) {
	t.Helper()

	keySource := uaerospike.CalculateKeySourceInternal(tx.TxIDChainHash(), recordIdx)
	key, err := aerospike.NewKey(store.GetNamespace(), store.GetName(), keySource)
	require.NoError(t, err)

	rec, err := client.Get(nil, key)
	require.NoError(t, err)

	v, ok := rec.Bins[bin.String()]

	return v, ok
}

// slotLanes decodes the slot bin's bytes into one position per output it covers.
func slotLanes(t *testing.T, v interface{}) []uint64 {
	t.Helper()

	b, ok := v.([]byte)
	require.True(t, ok, "the slot bin is bytes, got %T", v)
	require.Zero(t, len(b)%8)

	out := make([]uint64, len(b)/8)
	for i := range out {
		out[i] = binary.LittleEndian.Uint64(b[i*8:])
	}

	return out
}

// A spend that carries the spender's lane position records it in each parent output's slot, in the
// same write: teranode.lua on the stock server the tests run. A spend repeated by the same spender
// writes nothing, Unspend clears the position, and a spend without one leaves the record exactly as
// before, with no slot bin at all.
func TestSpendRecordsTheSpenderLanePositionInTheSlot(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)

	client, store, _, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	// Lane 200 sets the top bit, so the bytes must carry every bit.
	position := lane.Pack(200, 41_000)

	t.Run("fresh spends, a repeat and an unspend", func(t *testing.T) {
		parent := laneParent(t, 1, 3)
		_, err := store.Create(ctx, parent, 100)
		require.NoError(t, err)

		child := laneChild(t, parent, 0, 2)

		spends, err := store.SpendWithLanePosition(ctx, child, 101, position, utxo.IgnoreFlags{})
		require.NoError(t, err)
		require.Len(t, spends, 2)

		v, ok := recordBin(t, client, store, parent, 0, fields.SpendingLanePositions)
		require.True(t, ok, "the spend wrote the slot bin")
		require.Equal(t, []uint64{position, 0, position}, slotLanes(t, v), "grown to cover output 2, output 1 unspent")

		// The same spender again, with another position: an idempotent spend writes nothing.
		_, err = store.SpendWithLanePosition(ctx, child, 101, lane.Pack(3, 3), utxo.IgnoreFlags{})
		require.NoError(t, err)

		v, _ = recordBin(t, client, store, parent, 0, fields.SpendingLanePositions)
		require.Equal(t, []uint64{position, 0, position}, slotLanes(t, v), "a stored position never changes")

		require.NoError(t, store.Unspend(ctx, spends))

		v, _ = recordBin(t, client, store, parent, 0, fields.SpendingLanePositions)
		require.Equal(t, []uint64{0, 0, 0}, slotLanes(t, v), "Unspend clears the positions")
	})

	t.Run("a spend without a position writes no slot bin", func(t *testing.T) {
		parent := laneParent(t, 2, 2)
		_, err := store.Create(ctx, parent, 100)
		require.NoError(t, err)

		spends, err := store.Spend(ctx, laneChild(t, parent, 1), 101)
		require.NoError(t, err)

		_, ok := recordBin(t, client, store, parent, 0, fields.SpendingLanePositions)
		require.False(t, ok, "lane positions off: the record is what it always was")

		require.NoError(t, store.Unspend(ctx, spends))

		_, ok = recordBin(t, client, store, parent, 0, fields.SpendingLanePositions)
		require.False(t, ok)
	})

	t.Run("an output on a page record gets its slot on that record", func(t *testing.T) {
		batch := tSettings.UtxoStore.UtxoBatchSize
		parent := laneParent(t, 3, batch+2)
		_, err := store.Create(ctx, parent, 100)
		require.NoError(t, err)

		vout := uint32(batch + 1) //nolint:gosec // a small test batch size
		_, err = store.SpendWithLanePosition(ctx, laneChild(t, parent, vout), 101, position, utxo.IgnoreFlags{})
		require.NoError(t, err)

		_, ok := recordBin(t, client, store, parent, 0, fields.SpendingLanePositions)
		require.False(t, ok, "the master holds none of the spent outputs")

		v, ok := recordBin(t, client, store, parent, 1, fields.SpendingLanePositions)
		require.True(t, ok)
		require.Equal(t, []uint64{0, position}, slotLanes(t, v), "the page's second output")
	})
}

// A create names its parents' positions on the master record, and the validator's parent read
// gives back the parent's own position, at shadow only.
func TestParentLanePositionsOnAerospike(t *testing.T) {
	ctx := context.Background()
	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.LaneMigration = settings.LaneMigrationShadow

	client, store, _, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)

	require.True(t, store.LaneSlotPositionsRecorded(), "the server, running this build's Lua, passes the slot probe")

	parentPosition, childPosition := lane.Pack(7, 100), lane.Pack(7, 101)

	parent := laneParent(t, 4, 1)
	_, err := store.Create(ctx, parent, 100, utxo.WithLanePosition(parentPosition))
	require.NoError(t, err)

	answers, err := store.ParentOutputsForValidation(ctx, []utxo.Outpoint{{TxID: *parent.TxIDChainHash(), Vout: 0}})
	require.NoError(t, err)
	require.NoError(t, answers[0].Err)
	require.Equal(t, parentPosition, answers[0].LanePosition, "the parent read gives the parent's position")

	child := laneChild(t, parent, 0)
	md, err := store.Create(ctx, child, 101, utxo.WithLanePosition(childPosition), utxo.WithParentLanePositions([]uint64{parentPosition}))
	require.NoError(t, err)
	require.Equal(t, childPosition, md.LanePosition, "the create returns the position it wrote")

	v, ok := recordBin(t, client, store, child, 0, fields.ParentLanePositions)
	require.True(t, ok)
	require.Equal(t, []interface{}{int(lane.ToSigned(parentPosition))}, v)

	other := laneChild(t, laneParent(t, 5, 1), 0)
	_, err = store.Create(ctx, other, 101, utxo.WithLanePosition(lane.Pack(7, 102)), utxo.WithParentLanePositions([]uint64{lane.None}))
	require.NoError(t, err)

	_, ok = recordBin(t, client, store, other, 0, fields.ParentLanePositions)
	require.False(t, ok, "a parent without a position leaves the bin out")

	// A spend at shadow on a server that passed the probe counts as carrying its position.
	before := laneSpendsCarried(t)
	_, err = store.SpendWithLanePosition(ctx, laneChild(t, child, 0), 102, lane.Pack(7, 103), utxo.IgnoreFlags{})
	require.NoError(t, err)
	require.Equal(t, before+1, laneSpendsCarried(t))
}

// laneSpendsCarried reads teranode_lane_spend_positions_total{store="aerospike", carried="yes"}.
func laneSpendsCarried(t *testing.T) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() != "teranode_lane_spend_positions_total" {
			continue
		}

		for _, m := range family.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}

			if labels["store"] == "aerospike" && labels["carried"] == "yes" {
				return m.GetCounter().GetValue()
			}
		}
	}

	return 0
}
