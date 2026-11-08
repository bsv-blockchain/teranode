package tests

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

// These are the lane position's contract tests on a real store. Every create
// path writes the lane position it is given in the same write that creates the
// record, and Get returns it when the caller names fields.LanePosition. A
// record created without one reads back as lane.None.
//
// Each workload here uses its own seed (0xd0 to 0xdf) so the records do not
// collide with the other contract tests on a store shared between subtests.

// widest is lane 255 at the highest position: every bit set, so a store that
// keeps a signed 64-bit integer must carry it as a negative number and back.
var widest = lane.Pack(lane.MaxLane, lane.MaxPosition)

func requireLanePosition(t testing.TB, db utxostore.Store, tx *bt.Tx, want uint64, msgAndArgs ...interface{}) {
	t.Helper()

	md, err := db.Get(context.Background(), tx.TxIDChainHash(), fields.LanePosition)
	require.NoError(t, err, msgAndArgs...)
	require.Equal(t, want, md.LanePosition, msgAndArgs...)
}

// LanePositionCreateOnly: a create-only write, the validator's and the
// coinbase's shape, keeps the lane position, including lane 0 and lane 255,
// and a create without one reads back as lane.None.
func LanePositionCreateOnly(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	w := BuildMultiWorkload(t, 0xd0, 1, 3)

	positions := []uint64{lane.Pack(0, lane.FirstPosition), widest, lane.None}

	for i, root := range w.Roots {
		opts := []utxostore.CreateOption{utxostore.WithCreateOnly()}
		if positions[i] != lane.None {
			opts = append(opts, utxostore.WithLanePosition(positions[i]))
		}

		_, _, err := db.SpendAndCreate(ctx, root, 900, opts...)
		require.NoError(t, err, "root %d", i)
	}

	for i, root := range w.Roots {
		requireLanePosition(t, db, root, positions[i], "root %d", i)
	}
}

// LanePositionSpendAndCreate: the combined spend and create, the validator's
// main path, keeps the lane position on the created record.
func LanePositionSpendAndCreate(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	w := BuildMultiWorkload(t, 0xd1, 1, 2)
	w.StoreRoots(t, db, 900)

	want := []uint64{lane.Pack(7, 41_000), widest}

	for i, tx := range w.Txs {
		_, _, err := db.SpendAndCreate(ctx, tx, 901, utxostore.WithLanePosition(want[i]))
		require.NoError(t, err, "tx %d", i)
	}

	for i, tx := range w.Txs {
		requireLanePosition(t, db, tx, want[i], "tx %d", i)
	}
}

// LanePositionMinedCreate: a create that carries the block it is mined in,
// the coinbase's and block validation's shape, keeps the lane position.
func LanePositionMinedCreate(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	w := BuildMultiWorkload(t, 0xd2, 1, 2)
	w.StoreRoots(t, db, 900)

	want := []uint64{lane.Pack(3, 9), widest}

	for i, tx := range w.Txs {
		_, _, err := db.SpendAndCreate(ctx, tx, 901,
			utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 31, BlockHeight: 901, SubtreeIdx: 1}),
			utxostore.WithIgnoreLocked(true),
			utxostore.WithLanePosition(want[i]))
		require.NoError(t, err, "tx %d", i)
	}

	for i, tx := range w.Txs {
		requireLanePosition(t, db, tx, want[i], "tx %d", i)
	}
}

// LanePositionMulti: SpendAndCreateMulti gives each transaction of the list
// its own lane position, on the unmined path subtree validation uses, on the
// mined path block validation uses above the checkpoint, and on the
// below-checkpoint path. A list with the wrong number of lane positions is
// refused before any write.
func LanePositionMulti(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	cases := []struct {
		name string
		seed byte
		opts []utxostore.CreateOption
	}{
		{"unmined", 0xd3, []utxostore.CreateOption{utxostore.WithIgnoreLocked(true)}},
		{"mined above the checkpoint", 0xd4, []utxostore.CreateOption{
			utxostore.WithIgnoreLocked(true),
			utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 32, BlockHeight: 911, SubtreeIdx: 0}),
		}},
		{"below the checkpoint", 0xd5, []utxostore.CreateOption{
			utxostore.WithIgnoreLocked(true),
			utxostore.WithBelowCheckpoint(),
			utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 33, BlockHeight: 911, SubtreeIdx: 0}),
		}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := BuildMultiWorkload(t, c.seed, 3, 3)
			w.StoreRoots(t, db, 910)

			want := make([]uint64, len(w.Txs))
			for i := range want {
				want[i] = lane.Pack(uint8(250+i%6), 1_000_000+uint64(i)) //nolint:gosec // test data
			}

			want[len(want)-1] = widest

			results, err := db.SpendAndCreateMulti(ctx, w.Txs, 911, append(c.opts, utxostore.WithLanePositions(want))...)
			require.NoError(t, err)
			requireAllStatus(t, results, utxostore.MultiTxCreated)

			for i, tx := range w.Txs {
				requireLanePosition(t, db, tx, want[i], "tx %d", i)
			}
		})
	}

	t.Run("refused with the wrong count", func(t *testing.T) {
		w := BuildMultiWorkload(t, 0xd6, 1, 2)
		w.StoreRoots(t, db, 910)

		_, err := db.SpendAndCreateMulti(ctx, w.Txs, 911, utxostore.WithLanePositions([]uint64{lane.Pack(1, 1)}))
		require.True(t, utxostore.IsSpendAndCreateMultiRefused(err), "one lane position for two transactions: %v", err)

		requireNotStored(t, db, w.Txs...)
	})
}

// LanePositionAbsentWithoutOption: every create path left without a lane
// position, which is what every caller does with lane positions switched off,
// reads back as lane.None.
func LanePositionAbsentWithoutOption(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	w := BuildMultiWorkload(t, 0xd7, 2, 2)
	w.StoreRoots(t, db, 920)

	_, _, err := db.SpendAndCreate(ctx, w.Txs[0], 921)
	require.NoError(t, err)

	results, err := db.SpendAndCreateMulti(ctx, w.Txs[1:], 921, utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)
	requireAllStatus(t, results, utxostore.MultiTxCreated)

	for i, tx := range append(w.Roots, w.Txs...) {
		requireLanePosition(t, db, tx, lane.None, "record %d", i)
	}
}

// LanePositionContract runs every lane position contract test against db.
func LanePositionContract(t *testing.T, db utxostore.Store) {
	t.Run("create only", func(t *testing.T) { LanePositionCreateOnly(t, db) })
	t.Run("spend and create", func(t *testing.T) { LanePositionSpendAndCreate(t, db) })
	t.Run("mined create", func(t *testing.T) { LanePositionMinedCreate(t, db) })
	t.Run("multi", func(t *testing.T) { LanePositionMulti(t, db) })
	t.Run("absent without the option", func(t *testing.T) { LanePositionAbsentWithoutOption(t, db) })
}
