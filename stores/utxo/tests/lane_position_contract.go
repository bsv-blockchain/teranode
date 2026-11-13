package tests

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

// These are the lane position's contract tests on a real store. Every create
// path writes the lane position it is given in the same write that creates the
// record, and Get and BatchDecorate return it when the caller names
// fields.LanePosition. A record created without one reads back as lane.None.
//
// On utxoset the position lives on tx_ident, tx_mined and full preserved_parent
// rows only. Once a record's row is below the lookup floor or its tx_mined
// partition has dropped, a read answered from a live UTXO, an undo copy or a thin
// preserved row returns lane.None. These tests do not reach that state. A later
// phase that relies on the field must put it on those rows or read it elsewhere.
//
// The "below the checkpoint" case of LanePositionMulti takes the ordinary list
// route, because it does not set the options that reach utxoset's netted write.
// That write has its own test in stores/utxo/utxoset.
//
// Each workload here uses its own seed (0xd0 to 0xdf, 0xf0 to 0xf3) so the records do not
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

	items := make([]*utxostore.UnresolvedMetaData, len(w.Roots))
	for i, root := range w.Roots {
		items[i] = &utxostore.UnresolvedMetaData{Hash: *root.TxIDChainHash(), Idx: i}
	}

	require.NoError(t, db.BatchDecorate(ctx, items, fields.LanePosition))

	for i, item := range items {
		require.NoError(t, item.Err, "root %d", i)
		require.Equal(t, positions[i], item.Data.LanePosition, "BatchDecorate of root %d", i)
	}
}

// LanePositionWrittenOnce: a second create of the same transaction is refused as
// already existing, and the position the first one wrote survives. A store takes
// a new position on every call, so a retry offers a different one.
func LanePositionWrittenOnce(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	root := BuildMultiWorkload(t, 0xd8, 1, 1).Roots[0]
	first, second := lane.Pack(4, 100), lane.Pack(4, 200)

	_, _, err := db.SpendAndCreate(ctx, root, 900, utxostore.WithCreateOnly(), utxostore.WithLanePosition(first))
	require.NoError(t, err)

	_, _, err = db.SpendAndCreate(ctx, root, 900, utxostore.WithCreateOnly(), utxostore.WithLanePosition(second))
	require.True(t, errors.Is(err, errors.ErrTxExists), "the second create is refused: %v", err)

	requireLanePosition(t, db, root, first)
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

// ScriptedLanePositions is a LanePositionSource for tests: it hands out left in order, then reports
// that it has none. It is safe to call from a store's concurrent writers.
type ScriptedLanePositions struct {
	mu   sync.Mutex
	left []uint64
}

// NewScriptedLanePositions returns a source that hands out positions in order.
func NewScriptedLanePositions(positions ...uint64) *ScriptedLanePositions {
	return &ScriptedLanePositions{left: positions}
}

// Next implements utxostore.LanePositionSource.
func (s *ScriptedLanePositions) Next() (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.left) == 0 {
		return lane.None, false
	}

	position := s.left[0]
	s.left = s.left[1:]

	return position, true
}

// Prime implements utxostore.LanePositionSource. A scripted source has nothing to wait for.
func (s *ScriptedLanePositions) Prime() {}

// Remaining is how many positions have not been handed out.
func (s *ScriptedLanePositions) Remaining() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.left)
}

// useLanePositionSource gives db a source for the rest of the test. These tests run on a store shared
// between subtests, so the source comes off again when the test ends.
func useLanePositionSource(t *testing.T, db utxostore.Store, source utxostore.LanePositionSource) {
	t.Helper()

	taker, ok := utxostore.As[utxostore.LanePositionTaker](db)
	require.True(t, ok, "the store takes its lane positions from a source")

	taker.SetLanePositionSource(source)
	t.Cleanup(func() { taker.SetLanePositionSource(nil) })
}

// lanePositionsFrom makes count positions of one lane.
func lanePositionsFrom(laneNumber uint8, count int) []uint64 {
	positions := make([]uint64, count)
	for i := range positions {
		positions[i] = lane.Pack(laneNumber, uint64(i)+1) //nolint:gosec // i counts up from 0
	}

	return positions
}

// LanePositionFromSource: a store with a source gives each record it creates the next position, on
// the create-only path, the spend-and-create path and the mined path. A spend-only call creates no
// record, so it takes none. A position the caller names wins, and costs the source nothing. A source
// with nothing to give leaves the record with no position, and the create still succeeds.
func LanePositionFromSource(t *testing.T, db utxostore.Store) {
	ctx := context.Background()

	t.Run("every create takes the next position", func(t *testing.T) {
		w := BuildMultiWorkload(t, 0xf0, 1, 2)
		source := NewScriptedLanePositions(lanePositionsFrom(11, 4)...)
		useLanePositionSource(t, db, source)

		_, _, err := db.SpendAndCreate(ctx, w.Roots[0], 930, utxostore.WithCreateOnly())
		require.NoError(t, err)

		_, _, err = db.SpendAndCreate(ctx, w.Roots[1], 930, utxostore.WithCreateOnly())
		require.NoError(t, err)

		_, _, err = db.SpendAndCreate(ctx, w.Txs[0], 931)
		require.NoError(t, err)

		_, _, err = db.SpendAndCreate(ctx, w.Txs[1], 931, utxostore.WithIgnoreLocked(true),
			utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 34, BlockHeight: 931, SubtreeIdx: 0}))
		require.NoError(t, err)

		requireLanePosition(t, db, w.Roots[0], lane.Pack(11, 1), "first create")
		requireLanePosition(t, db, w.Roots[1], lane.Pack(11, 2), "second create")
		requireLanePosition(t, db, w.Txs[0], lane.Pack(11, 3), "spend and create")
		requireLanePosition(t, db, w.Txs[1], lane.Pack(11, 4), "mined create")
		require.Zero(t, source.Remaining())
	})

	t.Run("a spend-only call takes none", func(t *testing.T) {
		w := BuildMultiWorkload(t, 0xf1, 1, 1)
		w.StoreRoots(t, db, 930)

		source := NewScriptedLanePositions(lanePositionsFrom(12, 2)...)
		useLanePositionSource(t, db, source)

		_, _, err := db.SpendAndCreate(ctx, w.Txs[0], 931, utxostore.WithSpendOnly())
		require.NoError(t, err)
		require.Equal(t, 2, source.Remaining(), "a call that creates no record takes no position")
	})

	t.Run("a named position wins", func(t *testing.T) {
		w := BuildMultiWorkload(t, 0xf2, 1, 1)
		source := NewScriptedLanePositions(lanePositionsFrom(13, 1)...)
		useLanePositionSource(t, db, source)

		_, _, err := db.SpendAndCreate(ctx, w.Roots[0], 930, utxostore.WithCreateOnly(), utxostore.WithLanePosition(widest))
		require.NoError(t, err)

		requireLanePosition(t, db, w.Roots[0], widest)
		require.Equal(t, 1, source.Remaining(), "the source is not asked")
	})

	t.Run("a source with nothing to give leaves none", func(t *testing.T) {
		w := BuildMultiWorkload(t, 0xf3, 1, 1)
		useLanePositionSource(t, db, NewScriptedLanePositions())

		_, _, err := db.SpendAndCreate(ctx, w.Roots[0], 930, utxostore.WithCreateOnly())
		require.NoError(t, err, "a create never fails for want of a position")

		requireLanePosition(t, db, w.Roots[0], lane.None)
	})
}

// LanePositionMulti: SpendAndCreateMulti gives each transaction of the list its own position from the
// source, on the unmined path subtree validation uses, on the mined path block validation uses above
// the checkpoint, and on the below-checkpoint path. A store is free to write a list in any order, so
// the test holds the set of positions, not which record has which. A list naming one position for
// the whole list is refused before any write.
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

			// More than the list needs, because a store that retries a write can take again.
			source := NewScriptedLanePositions(append(lanePositionsFrom(250, 4*len(w.Txs)), widest)...)
			useLanePositionSource(t, db, source)

			results, err := db.SpendAndCreateMulti(ctx, w.Txs, 911, c.opts...)
			require.NoError(t, err)
			requireAllStatus(t, results, utxostore.MultiTxCreated)

			seen := make(map[uint64]bool, len(w.Txs))

			for i, tx := range w.Txs {
				md, err := db.Get(ctx, tx.TxIDChainHash(), fields.LanePosition)
				require.NoError(t, err, "tx %d", i)
				require.True(t, lane.Valid(md.LanePosition), "tx %d has a position", i)
				require.False(t, seen[md.LanePosition], "tx %d has its own position", i)

				seen[md.LanePosition] = true
			}
		})
	}

	t.Run("refused when it names one position", func(t *testing.T) {
		w := BuildMultiWorkload(t, 0xd6, 1, 2)
		w.StoreRoots(t, db, 910)

		_, err := db.SpendAndCreateMulti(ctx, w.Txs, 911, utxostore.WithLanePosition(lane.Pack(1, 1)))
		require.True(t, utxostore.IsSpendAndCreateMultiRefused(err), "one position for two transactions: %v", err)

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
	t.Run("written once", func(t *testing.T) { LanePositionWrittenOnce(t, db) })
	t.Run("spend and create", func(t *testing.T) { LanePositionSpendAndCreate(t, db) })
	t.Run("mined create", func(t *testing.T) { LanePositionMinedCreate(t, db) })
	t.Run("from a source", func(t *testing.T) { LanePositionFromSource(t, db) })
	t.Run("multi", func(t *testing.T) { LanePositionMulti(t, db) })
	t.Run("absent without the option", func(t *testing.T) { LanePositionAbsentWithoutOption(t, db) })
}
