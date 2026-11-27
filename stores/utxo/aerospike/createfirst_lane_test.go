package aerospike_test

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	teranodeaerospike "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// A create-first call takes one lane position and every exit either leaves it on a record, burns it,
// or hands it back to the source. These tests drive the whole call on a real Aerospike and pin each
// exit: a position goes back only when no record carries it, and one that a spend may have written
// into a parent's slot stays burned.

// createFirstLaneStore returns a create-first store with no position source. A test builds its
// fixtures first, since every create takes a position, and then calls give.
func createFirstLaneStore(t *testing.T) (*teranodeaerospike.Store, func(positions ...uint64) (*tests.ReturningLanePositions, func(*testing.T) tests.LaneReturns)) {
	t.Helper()

	logger := ulogger.NewErrorTestLogger(t)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UseCreateFirstOrder = true

	client, store, _, deferFn := initAerospike(t, tSettings, logger)
	t.Cleanup(deferFn)
	cleanDB(t, client)

	return store, func(positions ...uint64) (*tests.ReturningLanePositions, func(*testing.T) tests.LaneReturns) {
		source := tests.NewReturningLanePositions(positions...)
		store.SetLanePositionSource(source)
		t.Cleanup(func() { store.SetLanePositionSource(nil) })

		before := tests.ReadLaneReturns(t, "aerospike")

		return source, func(t *testing.T) tests.LaneReturns {
			return tests.ReadLaneReturns(t, "aerospike").Since(before)
		}
	}
}

func TestCreateFirstLane_SuccessLeavesThePositionOnTheRecord(t *testing.T) {
	ctx := context.Background()
	position := lane.Pack(210, 1)

	store, give := createFirstLaneStore(t)

	parent := laneParent(t, 31, 1)
	_, err := store.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := laneChild(t, parent, 0)

	source, returned := give(position)

	md, _, err := store.SpendAndCreate(ctx, child, 101)
	require.NoError(t, err)
	require.Equal(t, position, md.LanePosition, "the record carries the position the call took")

	require.Empty(t, source.Returned(), "a position on a record is not handed back")
	require.Empty(t, returned(t))
}

func TestCreateFirstLane_ARefusedSpendReturnsThePosition(t *testing.T) {
	ctx := context.Background()
	position := lane.Pack(211, 1)

	store, give := createFirstLaneStore(t)

	parent := laneParent(t, 32, 1)
	_, err := store.Create(ctx, parent, 100)
	require.NoError(t, err)

	_, err = store.Spend(ctx, laneChild(t, parent, 0), 100)
	require.NoError(t, err, "precondition: another transaction took the only input")

	rival := laneChild(t, parent, 0)
	require.NoError(t, rival.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 400), "a different transaction over the same input")

	source, returned := give(position, lane.Pack(211, 2))

	_, _, err = store.SpendAndCreate(ctx, rival, 101)
	require.ErrorIs(t, err, errors.ErrSpent)

	require.Equal(t, []uint64{position}, source.Returned(), "the refusal wrote nothing, so the position goes back")
	require.Equal(t, tests.LaneReturns{utxo.LaneReturnSpendRejected: 1}, returned(t))
}

func TestCreateFirstLane_ARefusalBesideASpentInputKeepsThePositionBurned(t *testing.T) {
	ctx := context.Background()
	position := lane.Pack(212, 1)

	store, give := createFirstLaneStore(t)

	free := laneParent(t, 33, 1)
	_, err := store.Create(ctx, free, 100)
	require.NoError(t, err)

	taken := laneParent(t, 34, 1)
	_, err = store.Create(ctx, taken, 100)
	require.NoError(t, err)

	_, err = store.Spend(ctx, laneChild(t, taken, 0), 100)
	require.NoError(t, err, "precondition: another transaction took the second input")

	child := bt.NewTx()

	for _, parent := range []*bt.Tx{free, taken} {
		require.NoError(t, child.FromUTXOs(&bt.UTXO{
			TxIDHash:      parent.TxIDChainHash(),
			Vout:          0,
			LockingScript: parent.Outputs[0].LockingScript,
			Satoshis:      parent.Outputs[0].Satoshis,
		}))
	}

	require.NoError(t, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))

	source, returned := give(position, lane.Pack(212, 2))

	_, _, err = store.SpendAndCreate(ctx, child, 101)
	require.ErrorIs(t, err, errors.ErrSpent)

	require.Empty(t, source.Returned(), "the free input may keep the position in its slot")
	require.Empty(t, returned(t))
}

// Aerospike never proves a refused create wrote nothing: a KEY_EXISTS can be the create's own retried
// write. So a duplicate burns its position and returns none, as the shared contract declares.
func TestCreateFirstLane_ADuplicateKeepsItsPositionBurned(t *testing.T) {
	ctx := context.Background()

	store, give := createFirstLaneStore(t)

	parent := laneParent(t, 35, 1)
	_, err := store.Create(ctx, parent, 100)
	require.NoError(t, err)

	child := laneChild(t, parent, 0)

	source, returned := give(lane.Pack(213, 1), lane.Pack(213, 2))

	_, _, err = store.SpendAndCreate(ctx, child, 101)
	require.NoError(t, err)

	_, _, err = store.SpendAndCreate(ctx, child, 101)
	require.ErrorIs(t, err, errors.ErrTxExists)

	require.Empty(t, source.Returned(), "the first position is on its record, the second is not proven unwritten")
	require.Empty(t, returned(t))
}

// A replay of a transaction the pruner removed is refused by the spend before it writes anything,
// so the position goes back.
func TestCreateFirstLane_ARefusedPrunedReplayReturnsThePosition(t *testing.T) {
	f := newReplayFixture(t, replayConfig{createFirst: true})

	position := lane.Pack(214, 1)
	source := tests.NewReturningLanePositions(position)
	f.store.SetLanePositionSource(source)
	t.Cleanup(func() { f.store.SetLanePositionSource(nil) })

	before := tests.ReadLaneReturns(t, "aerospike")

	f.prune(replayPruneBlock)

	_, _, err := f.store.SpendAndCreate(f.ctx, f.child, replayBlock)
	require.ErrorIs(t, err, errors.ErrUtxoSpendingTxPruned)
	require.False(t, f.childStored(), "the replay created nothing")

	require.Equal(t, []uint64{position}, source.Returned(), "the refusal wrote neither the spend nor the slot, so the position goes back")
	require.Equal(t, tests.LaneReturns{utxo.LaneReturnSpendRejected: 1}, tests.ReadLaneReturns(t, "aerospike").Since(before))

	_, ok := recordBin(t, f.client, f.store, f.child, 0, fields.LanePosition)
	require.False(t, ok, "no record carries it")
}
