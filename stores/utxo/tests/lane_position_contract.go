package tests

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/errors"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/prometheus/client_golang/prometheus"
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

// ScriptedLanePositions is a LanePositionSource for tests: it hands out left in order, then fails
// with service unavailable, as a source with no lease does. It is safe to call from a store's
// concurrent writers.
type ScriptedLanePositions struct {
	mu    sync.Mutex
	left  []uint64
	calls int

	// OnNext, if set, runs at the start of every Next, outside the lock.
	OnNext func()
}

// NewScriptedLanePositions returns a source that hands out positions in order.
func NewScriptedLanePositions(positions ...uint64) *ScriptedLanePositions {
	return &ScriptedLanePositions{left: positions}
}

// Next implements utxostore.LanePositionSource.
func (s *ScriptedLanePositions) Next(context.Context) (uint64, error) {
	if s.OnNext != nil {
		s.OnNext()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls++

	if len(s.left) == 0 {
		return lane.None, errors.NewServiceUnavailableError("scripted lane positions: none left")
	}

	position := s.left[0]
	s.left = s.left[1:]

	return position, nil
}

// Calls is how many times Next has been called.
func (s *ScriptedLanePositions) Calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.calls
}

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
// with nothing to give fails the create with service unavailable, and nothing is written or spent.
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

	t.Run("a create with no position fails and writes nothing", func(t *testing.T) {
		w := BuildMultiWorkload(t, 0xf3, 1, 1)
		useLanePositionSource(t, db, NewScriptedLanePositions())

		_, _, err := db.SpendAndCreate(ctx, w.Roots[0], 930, utxostore.WithCreateOnly())
		require.ErrorIs(t, err, errors.ErrServiceUnavailable, "no create goes out without a position")

		requireNotStored(t, db, w.Roots[0])
	})

	t.Run("invalid named position fails without allocating or writing", func(t *testing.T) {
		w := BuildMultiWorkload(t, 0xf6, 1, 1)
		source := NewScriptedLanePositions(1)
		useLanePositionSource(t, db, source)
		_, _, err := db.SpendAndCreate(ctx, w.Roots[0], 930,
			utxostore.WithCreateOnly(), utxostore.WithLanePosition(1<<56))
		require.Error(t, err)
		require.Zero(t, source.Calls(), "invalid explicit metadata must not be replaced")
		requireNotStored(t, db, w.Roots[0])
	})

	t.Run("invalid source position fails without writing", func(t *testing.T) {
		w := BuildMultiWorkload(t, 0xf7, 1, 1)
		source := NewScriptedLanePositions(0)
		useLanePositionSource(t, db, source)
		_, _, err := db.SpendAndCreate(ctx, w.Roots[0], 930, utxostore.WithCreateOnly())
		require.ErrorIs(t, err, errors.ErrProcessing)
		require.Equal(t, 1, source.Calls(), "invalid source output must not be retried")
		requireNotStored(t, db, w.Roots[0])
	})

	t.Run("a spend-and-create with no position spends nothing", func(t *testing.T) {
		w := BuildMultiWorkload(t, 0xf5, 1, 1)
		w.StoreRoots(t, db, 930)

		tx := w.Txs[0]
		inputs, err := utxostore.GetSpends(tx)
		require.NoError(t, err)

		requireUnspent := func(when string) {
			for i, in := range inputs {
				resp, err := db.GetSpend(context.Background(), in)
				require.NoError(t, err, "%s: input %d", when, i)
				require.Equal(t, int(utxostore.Status_OK), resp.Status, "%s: input %d is spent", when, i)
			}
		}

		// The position is taken before the first spend, so when the source is asked nothing is
		// spent yet. Taken after, the inputs would be spent here and only rolled back later.
		source := NewScriptedLanePositions()
		source.OnNext = func() { requireUnspent("when the position was taken") }
		useLanePositionSource(t, db, source)

		_, spends, err := db.SpendAndCreate(ctx, tx, 931)
		require.ErrorIs(t, err, errors.ErrServiceUnavailable)
		require.Empty(t, spends)
		require.Equal(t, 1, source.Calls())

		requireUnspent("after the call")
		requireNotStored(t, db, tx)
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

// LaneBurns is the burned lane position counter of one store at a moment, keyed "reason/caller".
type LaneBurns map[string]float64

// ReadLaneBurns reads teranode_lane_positions_burned_total for one store from the default registry.
func ReadLaneBurns(t testing.TB, store string) LaneBurns {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	burns := LaneBurns{}

	for _, family := range families {
		if family.GetName() != "teranode_lane_positions_burned_total" {
			continue
		}

		for _, m := range family.GetMetric() {
			labels := make(map[string]string, 3)
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}

			if labels["store"] == store {
				burns[labels["reason"]+"/"+labels["caller"]] = m.GetCounter().GetValue()
			}
		}
	}

	return burns
}

// Since returns what the counter gained after before, leaving out every label set that did not move.
func (b LaneBurns) Since(before LaneBurns) LaneBurns {
	diff := LaneBurns{}

	for k, v := range b {
		if d := v - before[k]; d != 0 {
			diff[k] = d
		}
	}

	return diff
}

// requireBurns runs f and requires the store's burned lane position counter to gain exactly want.
func requireBurns(t *testing.T, store string, want LaneBurns, f func()) {
	t.Helper()

	before := ReadLaneBurns(t, store)

	f()

	require.Equal(t, want, ReadLaneBurns(t, store).Since(before), "burned lane positions")
}

// directCreator is a store's own Create, which SpendAndCreate wraps on the Aerospike and SQL stores.
type directCreator interface {
	Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxostore.CreateOption) (*meta.Data, error)
}

// LanePositionBurns: a position a store takes from its source and writes no record with is counted
// once, labelled with the store, why it burned and which service asked for the create. A create
// that writes its record counts nothing, and neither does a position the caller named, which the
// caller took and is the caller's to count. store is the label the store counts under.
func LanePositionBurns(t *testing.T, db utxostore.Store, store string) {
	ctx := context.Background()

	t.Run("a written record burns nothing", func(t *testing.T) {
		w := BuildMultiWorkload(t, 0xc0, 2, 2)
		source := NewScriptedLanePositions(lanePositionsFrom(21, 8)...)
		useLanePositionSource(t, db, source)

		requireBurns(t, store, LaneBurns{}, func() {
			for _, root := range w.Roots {
				_, _, err := db.SpendAndCreate(ctx, root, 940, utxostore.WithCreateOnly(),
					utxostore.WithCaller(utxostore.CallerBlockAssembly))
				require.NoError(t, err)
			}

			_, _, err := db.SpendAndCreate(ctx, w.Txs[0], 941, utxostore.WithCaller(utxostore.CallerValidator))
			require.NoError(t, err)

			results, err := db.SpendAndCreateMulti(ctx, w.Txs[1:], 941, utxostore.WithIgnoreLocked(true),
				utxostore.WithCaller(utxostore.CallerSubtreeValidation))
			require.NoError(t, err)
			requireAllStatus(t, results, utxostore.MultiTxCreated)
		})

		// Every position taken is on a record: nothing was taken and lost.
		for i, tx := range append(w.Roots, w.Txs...) {
			md, err := db.Get(ctx, tx.TxIDChainHash(), fields.LanePosition)
			require.NoError(t, err, "record %d", i)
			require.True(t, lane.Valid(md.LanePosition), "record %d has a position", i)
		}

		require.Equal(t, len(w.Roots)+len(w.Txs), source.Calls(), "one position per record")
	})

	t.Run("a create of a stored transaction burns exists", func(t *testing.T) {
		root := BuildMultiWorkload(t, 0xc1, 1, 1).Roots[0]
		useLanePositionSource(t, db, NewScriptedLanePositions(lanePositionsFrom(22, 2)...))

		_, _, err := db.SpendAndCreate(ctx, root, 940, utxostore.WithCreateOnly())
		require.NoError(t, err)

		requireBurns(t, store, LaneBurns{utxostore.LaneBurnExists + "/validator": 1}, func() {
			_, _, err = db.SpendAndCreate(ctx, root, 940, utxostore.WithCreateOnly(),
				utxostore.WithCaller(utxostore.CallerValidator))
			require.ErrorIs(t, err, errors.ErrTxExists)
		})

		requireLanePosition(t, db, root, lane.Pack(22, 1), "the first position stays")
	})

	t.Run("a named position is the caller's to count", func(t *testing.T) {
		root := BuildMultiWorkload(t, 0xc2, 1, 1).Roots[0]
		source := NewScriptedLanePositions(lanePositionsFrom(23, 1)...)
		useLanePositionSource(t, db, source)

		_, _, err := db.SpendAndCreate(ctx, root, 940, utxostore.WithCreateOnly())
		require.NoError(t, err)

		requireBurns(t, store, LaneBurns{}, func() {
			_, _, err = db.SpendAndCreate(ctx, root, 940, utxostore.WithCreateOnly(),
				utxostore.WithLanePosition(lane.Pack(23, 9)))
			require.ErrorIs(t, err, errors.ErrTxExists)
		})
	})

	t.Run("a refused spend burns spend_rejected", func(t *testing.T) {
		w := BuildMultiWorkload(t, 0xc3, 1, 1)
		w.StoreRoots(t, db, 940)

		useLanePositionSource(t, db, NewScriptedLanePositions(lanePositionsFrom(24, 2)...))

		_, _, err := db.SpendAndCreate(ctx, w.Txs[0], 941)
		require.NoError(t, err)

		// Spends root output 0, which w.Txs[0] has spent.
		double := spendOneOutput(t, w.Roots[0], 0)

		requireBurns(t, store, LaneBurns{utxostore.LaneBurnSpendRejected + "/block_assembly": 1}, func() {
			_, _, err = db.SpendAndCreate(ctx, double, 941, utxostore.WithCaller(utxostore.CallerBlockAssembly))
			require.Error(t, err)
		})

		requireNotStored(t, db, double)
	})

	t.Run("a list with a stored transaction burns exists for it", func(t *testing.T) {
		w := BuildMultiWorkload(t, 0xc4, 1, 3)
		w.StoreRoots(t, db, 940)

		useLanePositionSource(t, db, NewScriptedLanePositions(lanePositionsFrom(25, 8)...))

		_, _, err := db.SpendAndCreate(ctx, w.Txs[1], 941)
		require.NoError(t, err)

		requireBurns(t, store, LaneBurns{utxostore.LaneBurnExists + "/subtree_validation": 1}, func() {
			results, err := db.SpendAndCreateMulti(ctx, w.Txs, 941, utxostore.WithIgnoreLocked(true),
				utxostore.WithCaller(utxostore.CallerSubtreeValidation))
			require.NoError(t, err)
			require.Equal(t, utxostore.MultiTxCreated, results[0].Status)
			require.Equal(t, utxostore.MultiTxExisted, results[1].Status)
			require.Equal(t, utxostore.MultiTxCreated, results[2].Status)
		})
	})

	t.Run("a direct Create counts its own position, and only its own", func(t *testing.T) {
		creator, ok := utxostore.As[directCreator](db)
		require.True(t, ok, "every store with lane positions has its own Create")

		w := BuildMultiWorkload(t, 0xc6, 1, 2)
		source := NewScriptedLanePositions(lanePositionsFrom(27, 3)...)
		useLanePositionSource(t, db, source)

		requireBurns(t, store, LaneBurns{}, func() {
			_, err := creator.Create(ctx, w.Roots[0], 940, utxostore.WithCaller(utxostore.CallerBlockAssembly))
			require.NoError(t, err)
		})

		requireLanePosition(t, db, w.Roots[0], lane.Pack(27, 1))

		requireBurns(t, store, LaneBurns{utxostore.LaneBurnExists + "/block_assembly": 1}, func() {
			_, err := creator.Create(ctx, w.Roots[0], 940, utxostore.WithCaller(utxostore.CallerBlockAssembly))
			require.ErrorIs(t, err, errors.ErrTxExists)
		})

		requireBurns(t, store, LaneBurns{}, func() {
			_, err := creator.Create(ctx, w.Roots[0], 940, utxostore.WithLanePosition(lane.Pack(27, 9)))
			require.ErrorIs(t, err, errors.ErrTxExists, "a named position is the caller's to count")
		})

		require.Equal(t, 2, source.Calls(), "the named create took nothing from the source")
	})

	t.Run("a call whose caller had already given up burns error", func(t *testing.T) {
		w := BuildMultiWorkload(t, 0xc7, 1, 1)
		w.StoreRoots(t, db, 940)

		useLanePositionSource(t, db, NewScriptedLanePositions(lanePositionsFrom(28, 1)...))

		cancelled, cancel := context.WithCancel(ctx)
		cancel()

		// The position is taken, and nothing is written with it: no store may report that as an
		// outcome it does not know.
		requireBurns(t, store, LaneBurns{utxostore.LaneBurnError + "/validator": 1}, func() {
			_, _, err := db.SpendAndCreate(cancelled, w.Txs[0], 941, utxostore.WithCaller(utxostore.CallerValidator))
			require.Error(t, err)
		})

		requireNotStored(t, db, w.Txs[0])
	})

	t.Run("a create with no caller counts under unknown", func(t *testing.T) {
		root := BuildMultiWorkload(t, 0xc5, 1, 1).Roots[0]
		useLanePositionSource(t, db, NewScriptedLanePositions(lanePositionsFrom(26, 2)...))

		_, _, err := db.SpendAndCreate(ctx, root, 940, utxostore.WithCreateOnly())
		require.NoError(t, err)

		requireBurns(t, store, LaneBurns{utxostore.LaneBurnExists + "/unknown": 1}, func() {
			_, _, err = db.SpendAndCreate(ctx, root, 940, utxostore.WithCreateOnly())
			require.ErrorIs(t, err, errors.ErrTxExists)
		})
	})
}
