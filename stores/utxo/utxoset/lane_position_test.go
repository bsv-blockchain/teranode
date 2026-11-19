package utxoset

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// newLaneStore and its siblings are the test stores with lane positions on, which is what gives the
// database the column.
func newLaneStore(t *testing.T) (*Store, context.Context) {
	t.Helper()

	return newTestStoreWith(t, func(ts *settings.Settings) { ts.UtxoStore.LaneMigration = settings.LaneMigrationShadow })
}

func newUncheckpointedLaneStore(t *testing.T) (*Store, context.Context) {
	t.Helper()

	return newTestStoreWith(t, func(ts *settings.Settings) {
		withCheckpoints(ts, nil)
		ts.UtxoStore.LaneMigration = settings.LaneMigrationShadow
	})
}

func newCheckpointLaneStore(t *testing.T) (*Store, context.Context) {
	t.Helper()

	return newTestStoreWith(t, func(ts *settings.Settings) {
		params := *ts.ChainCfgParams
		params.Checkpoints = []chaincfg.Checkpoint{{Height: checkpointFloor}}
		ts.ChainCfgParams = &params
		ts.UtxoStore.LaneMigration = settings.LaneMigrationShadow
	})
}

func newBodySkipLaneStore(t *testing.T) *Store {
	t.Helper()

	s, _ := newTestStoreWith(t, func(ts *settings.Settings) {
		ts.UtxoStore.SkipTxBodyBelowCheckpoint = true
		ts.UtxoStore.LaneMigration = settings.LaneMigrationShadow
	})

	return s
}

// reopenWithFlag opens the same database again, as a restart with the flag set to on would.
func reopenWithFlag(t *testing.T, s *Store, on bool) *Store {
	t.Helper()

	ts := *s.settings
	ts.UtxoStore.LaneMigration = settings.LaneMigrationOff

	if on {
		ts.UtxoStore.LaneMigration = settings.LaneMigrationShadow
	}

	u, err := url.Parse(testDSN(t))
	require.NoError(t, err)

	reopened, err := New(t.Context(), ulogger.TestLogger{}, &ts, u)
	require.NoError(t, err)

	t.Cleanup(func() { _ = reopened.Close(context.Background()) })

	return reopened
}

// With the flag off a database never gets the column, so its rows stay what main writes. That is the
// point of opening it only with the flag on: a NULL in a row with no other NULL costs 8 bytes.
func TestLaneColumnExistsOnlyWithTheFlagOn(t *testing.T) {
	s, ctx := newTestStore(t)

	for _, table := range laneColumnTables {
		require.False(t, hasLaneColumn(t, s, ctx, table), "flag off: %s has no lane_position", table)
	}

	// A read naming the field finds no column to read, and a position has nowhere to go. Neither fails.
	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 100, utxo.WithLanePosition(lane.Pack(2, 1)))
	require.NoError(t, err)
	requireGetLane(t, s, ctx, tx, lane.None, "flag off")

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err, "the copy into tx_mined runs its plain form")
}

// A step this build refuses must not add the column, even for a tool that opens the store without
// the daemon.
func TestOpeningAtARefusedLaneStepAddsNoColumn(t *testing.T) {
	s, ctx := newTestStore(t)

	for _, step := range []settings.LaneMigration{settings.LaneMigrationRows, settings.LaneMigrationLanes} {
		ts := *s.settings
		ts.UtxoStore.LaneMigration = step

		u, err := url.Parse(testDSN(t))
		require.NoError(t, err)

		refused, err := New(ctx, ulogger.TestLogger{}, &ts, u)
		require.Error(t, err, "%s", step)
		require.Nil(t, refused)

		for _, table := range laneColumnTables {
			require.False(t, hasLaneColumn(t, s, ctx, table), "%s: %s has no lane_position", step, table)
		}
	}
}

// Switching the flag on adds the column to a database in use. Records already there read as having no
// position, new ones get one, and it survives the flag going off again, because the copies follow the
// column and not the flag.
func TestLaneColumnArrivesWhenTheFlagGoesOnAndOutlivesIt(t *testing.T) {
	s, ctx := newTestStore(t)

	before := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, before, 100)
	require.NoError(t, err)

	on := reopenWithFlag(t, s, true)

	for _, table := range laneColumnTables {
		require.True(t, hasLaneColumn(t, on, ctx, table), "flag on: %s gains lane_position", table)
	}

	requireGetLane(t, on, ctx, before, lane.None, "a record from before the flag")

	want := lane.Pack(200, 7)
	during := mkTx(t, 1, 6_000)
	_, err = on.Create(ctx, during, 101, utxo.WithLanePosition(want))
	require.NoError(t, err)
	requireGetLane(t, on, ctx, during, want, "a record created with the flag on")

	off := reopenWithFlag(t, s, false)
	require.True(t, off.laneColumn, "the column outlives the flag")

	_, err = off.SetMinedMulti(ctx, hashes(during), utxo.MinedBlockInfo{BlockID: 8, BlockHeight: 101, OnLongestChain: true})
	require.NoError(t, err)

	v, null, ok := storedLane(t, off, ctx, "tx_mined", during)
	require.True(t, ok)
	require.False(t, null, "the copy carries the position with the flag off")
	require.Equal(t, lane.ToSigned(want), v)
	requireGetLane(t, off, ctx, during, want, "read with the flag off")
}

// TestLanePositionContractUtxoset runs the shared lane position contract. The multi contract's
// "below the checkpoint" case and the mined create need a store whose checkpoint is below the test
// heights to take the block-path route, and its "mined above the checkpoint" case needs one with
// none, so the contract runs on both kinds of store.
func TestLanePositionContractUtxoset(t *testing.T) {
	t.Run("no checkpoints", func(t *testing.T) {
		s, _ := newUncheckpointedLaneStore(t)
		tests.LanePositionContract(t, s)
		t.Run("burns", func(t *testing.T) { tests.LanePositionBurns(t, s, "utxoset") })
	})

	t.Run("no batchers", func(t *testing.T) {
		s, _ := newUnbatchedLaneStore(t)
		tests.LanePositionBurns(t, s, "utxoset")
	})

	t.Run("checkpointed", func(t *testing.T) {
		s, _ := newLaneStore(t)
		tests.LanePositionMinedCreate(t, s)
		tests.LanePositionMulti(t, s)
		tests.LanePositionAbsentWithoutOption(t, s)
	})
}

// storedLane reads the raw lane_position column of the named table for one transaction. ok is
// false when the table holds no row for it, and null is true when the column is NULL.
func storedLane(t *testing.T, s *Store, ctx context.Context, table string, tx *bt.Tx) (value int64, null, ok bool) {
	t.Helper()

	var v *int64

	rows, err := s.pool.Query(ctx, `SELECT lane_position FROM `+table+` WHERE txid = $1`, hashBytes(tx))
	require.NoError(t, err)

	defer rows.Close()

	if !rows.Next() {
		require.NoError(t, rows.Err())

		return 0, false, false
	}

	require.NoError(t, rows.Scan(&v))

	if v == nil {
		return 0, true, true
	}

	return *v, false, true
}

func requireGetLane(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx, want uint64, msg string) {
	t.Helper()

	got, err := s.Get(ctx, tx.TxIDChainHash(), fields.LanePosition)
	require.NoError(t, err, msg)
	require.Equal(t, want, got.LanePosition, msg)
}

// TestLanePositionSurvivesEveryCopyOfTheRow follows one transaction's lane position down the whole
// life of its row: written to the identity row by the create, copied to its containment row when
// a block is recorded, still there after the stamp deletes the identity row, and copied again to
// preserved_parent, which is what answers once the containment partition is gone.
func TestLanePositionSurvivesEveryCopyOfTheRow(t *testing.T) {
	s, ctx := newLaneStore(t)

	// Lane 200 sets the top bit, so the column holds a negative number.
	want := lane.Pack(200, 4_242)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 100, utxo.WithLanePosition(want))
	require.NoError(t, err)

	v, null, ok := storedLane(t, s, ctx, "tx_ident", parent)
	require.True(t, ok, "the unmined create writes an identity row")
	require.False(t, null)
	require.Equal(t, lane.ToSigned(want), v)
	require.Less(t, v, int64(0), "lane 200 is stored as a negative BIGINT by design")

	_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	v, null, ok = storedLane(t, s, ctx, "tx_mined", parent)
	require.True(t, ok, "recording the block writes a containment row")
	require.False(t, null, "the containment row copies the identity row's lane position")
	require.Equal(t, lane.ToSigned(want), v)

	tip := stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})
	require.False(t, identExists(t, s, ctx, parent), "the stamp deletes the identity row")
	require.Less(t, tip, uint32(900))

	// The only output is spent after the stamp, so its undo copy carries the stamped pair, and at a
	// height past the stamp so that copy does not hold the partition drop back.
	spendOneOutputInBlock(t, s, ctx, parent, 0, 900, 9)
	requireGetLane(t, s, ctx, parent, want, "answered from tx_mined alone")

	require.NoError(t, s.PreserveTransactions(ctx, []chainhash.Hash{*parent.TxIDChainHash()}, 5_000))

	v, null, ok = storedLane(t, s, ctx, "preserved_parent", parent)
	require.True(t, ok)
	require.False(t, null, "the preserved copy takes the containment row's lane position")
	require.Equal(t, lane.ToSigned(want), v)

	require.Equal(t, 1, dropStamped(t, s, ctx, tip), "the partition has to be gone for this to mean anything")

	_, _, ok = storedLane(t, s, ctx, "tx_mined", parent)
	require.False(t, ok, "tx_mined no longer holds the transaction")

	// The spend's journal row answers a read before the preserved copy does, and carries only the
	// block facts, so it goes too.
	_, err = s.dropSpendJournalPartitionsBelow(ctx, 2_000)
	require.NoError(t, err)

	requireGetLane(t, s, ctx, parent, want, "answered from preserved_parent alone")
}

// storedLaneInBlock reads the raw lane_position of one transaction's containment row in one block.
// null is true when the column is NULL.
func storedLaneInBlock(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx, blockID int32) (value int64, null bool) {
	t.Helper()

	var v *int64

	require.NoError(t, s.pool.QueryRow(ctx, `SELECT lane_position FROM tx_mined WHERE txid = $1 AND block_id = $2`,
		hashBytes(tx), blockID).Scan(&v))

	if v == nil {
		return 0, true
	}

	return *v, false
}

// TestLanePositionIsCopiedFromAnExistingContainmentRow: once the stamp has deleted the identity row,
// a rival block's containment row takes its payload from the existing containment row.
// TestLanePositionSurvivesEveryCopyOfTheRow only copies from the identity row. The rival is above
// the stamp fence, or the write is fenced out and this checks nothing.
func TestLanePositionIsCopiedFromAnExistingContainmentRow(t *testing.T) {
	s, ctx := newLaneStore(t)

	want := lane.Pack(200, 4_243)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 100, utxo.WithLanePosition(want))
	require.NoError(t, err)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})
	require.False(t, identExists(t, s, ctx, tx), "the stamp deletes the identity row")

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 9, BlockHeight: 293})
	require.NoError(t, err)

	v, null := storedLaneInBlock(t, s, ctx, tx, 9)
	require.False(t, null, "the rival block's row copies the existing row's lane position")
	require.Equal(t, lane.ToSigned(want), v)
}

// blockPathCreateRoutes are the two create statements a block-carrying create can take.
var blockPathCreateRoutes = []struct {
	name   string
	height uint32
	store  func(t *testing.T) (*Store, context.Context)
}{
	{"identity plan", 901, newUncheckpointedLaneStore},
	{"mined plan", checkpointFloor - 1, func(t *testing.T) (*Store, context.Context) { return newCheckpointLaneStore(t) }},
}

// TestAnExistingIdentityRowDecidesTheLanePosition: a block-path create of a transaction already
// stored unmined must not give its containment row a position the identity row does not hold,
// because Get then answers one way while the identity row lives and another way after. The store
// takes a new position on every create, so this is every block-path create over an existing record.
// The identity row's value wins, NULL included, on both create statements.
func TestAnExistingIdentityRowDecidesTheLanePosition(t *testing.T) {
	offered := lane.Pack(4, 200)

	for _, route := range blockPathCreateRoutes {
		t.Run(route.name, func(t *testing.T) {
			s, ctx := route.store(t)

			for _, c := range []struct {
				name  string
				first uint64
				vout  int
			}{{"first create had a position", lane.Pack(4, 100), 1}, {"first create had none", lane.None, 2}} {
				tx := mkTx(t, c.vout, 5_000)

				var opts []utxo.CreateOption
				if c.first != lane.None {
					opts = append(opts, utxo.WithLanePosition(c.first))
				}

				_, err := s.Create(ctx, tx, route.height-1, opts...)
				require.NoError(t, err, c.name)

				_, _ = s.Create(ctx, tx, route.height, minedAt(route.height, 42), utxo.WithLanePosition(offered))

				v, null, ok := storedLane(t, s, ctx, "tx_mined", tx)
				require.True(t, ok, "%s: the block-path create writes the containment row", c.name)

				if c.first == lane.None {
					require.True(t, null, "%s: tx_mined must not hold a position tx_ident does not", c.name)
				} else {
					require.False(t, null, c.name)
					require.Equal(t, lane.ToSigned(c.first), v, c.name)
				}

				requireGetLane(t, s, ctx, tx, c.first, c.name)
			}
		})
	}
}

// TestABlockPathCreateOfAStoredRecordKeepsItsPosition: a block-path create of a record the store
// already holds takes a new position at the entry, which is burned. The containment row must copy
// the position the record has, not the one just taken, or Get would return a position no identity
// row ever carried once the stamp deleted the identity row.
func TestABlockPathCreateOfAStoredRecordKeepsItsPosition(t *testing.T) {
	want, burned := lane.Pack(4, 100), lane.Pack(4, 900)

	for _, route := range blockPathCreateRoutes {
		t.Run(route.name, func(t *testing.T) {
			s, ctx := route.store(t)
			s.SetLanePositionSource(tests.NewScriptedLanePositions(burned))

			tx := mkTx(t, 1, 5_000)

			_, err := s.Create(ctx, tx, route.height-1, utxo.WithLanePosition(want))
			require.NoError(t, err)

			_, _ = s.Create(ctx, tx, route.height, minedAt(route.height, 42))

			v, null, ok := storedLane(t, s, ctx, "tx_mined", tx)
			require.True(t, ok, "the block-path create writes the containment row")
			require.False(t, null, "the containment row copies the identity row's position")
			require.Equal(t, lane.ToSigned(want), v)
		})
	}

	t.Run("from an existing containment row", func(t *testing.T) {
		s, ctx := newLaneStore(t)
		s.SetLanePositionSource(tests.NewScriptedLanePositions(burned))

		tx := mkTx(t, 3, 6_000)

		_, err := s.Create(ctx, tx, 100, utxo.WithLanePosition(want))
		require.NoError(t, err)

		_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
		require.NoError(t, err)

		stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})

		_, err = s.Create(ctx, tx, 293, minedAt(293, 9))
		require.ErrorIs(t, err, errors.ErrTxExists)

		v, null := storedLaneInBlock(t, s, ctx, tx, 9)
		require.False(t, null, "the new block's row copies the first block's position")
		require.Equal(t, lane.ToSigned(want), v)
	})
}

// TestALaneReadInABatchWithAReadThatDidNotAsk: the batch reads the column if any item asked, so the
// lane read gets its position whatever it shares a batch with.
func TestALaneReadInABatchWithAReadThatDidNotAsk(t *testing.T) {
	s, ctx := newTestStoreWith(t, func(ts *settings.Settings) {
		// Two items fill a batch, and the window is long enough that neither flushes alone.
		ts.UtxoStore.GetBatcherSize = 2
		ts.UtxoStore.GetBatcherDurationMillis = 5_000
		ts.UtxoStore.LaneMigration = settings.LaneMigrationShadow
	})
	require.NotNil(t, s.getBatcher)

	want := lane.Pack(200, 77)

	feeOnly := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, feeOnly, 100)
	require.NoError(t, err)

	withLane := mkTx(t, 1, 6_000)
	_, err = s.Create(ctx, withLane, 100, utxo.WithLanePosition(want))
	require.NoError(t, err)

	feeRead := make(chan error, 1)

	go func() {
		_, readErr := s.Get(ctx, feeOnly.TxIDChainHash(), fields.Fee)
		feeRead <- readErr
	}()

	requireGetLane(t, s, ctx, withLane, want, "the lane read shares a batch with a fee read")
	require.NoError(t, <-feeRead)
}

// TestARefusedCreateCannotGiveARecordASecondLanePosition: a create over a record with no identity row
// is refused but still writes the new block's mined row, as a coinbase seen in a second block does. The
// row must carry the position the record has, not the one this refused create took.
func TestARefusedCreateCannotGiveARecordASecondLanePosition(t *testing.T) {
	first, second := lane.Pack(200, 4_244), lane.Pack(4, 200)

	requireOneLanePosition := func(t *testing.T, s *Store, ctx context.Context, tx *bt.Tx) {
		t.Helper()

		rows, err := s.pool.Query(ctx, `SELECT block_id, lane_position FROM tx_mined WHERE txid = $1 ORDER BY block_id`, tx.TxIDChainHash()[:])
		require.NoError(t, err)

		defer rows.Close()

		seen := 0

		for rows.Next() {
			var (
				blockID  int32
				position *int64
			)

			require.NoError(t, rows.Scan(&blockID, &position))
			require.NotNil(t, position, "block %d", blockID)
			require.Equal(t, lane.ToSigned(first), *position, "block %d", blockID)

			seen++
		}

		require.NoError(t, rows.Err())
		require.Equal(t, 2, seen, "each block has its row")

		requireGetLane(t, s, ctx, tx, first, "the record keeps its first position")
	}

	t.Run("below the checkpoint", func(t *testing.T) {
		s, ctx := newCheckpointLaneStore(t)

		tx := mkTx(t, 3, 5_000)

		_, err := s.Create(ctx, tx, checkpointFloor-2, utxo.WithLanePosition(first),
			utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 41, BlockHeight: checkpointFloor - 2, OnLongestChain: true}))
		require.NoError(t, err)

		_, err = s.Create(ctx, tx, checkpointFloor-1, utxo.WithLanePosition(second),
			utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 42, BlockHeight: checkpointFloor - 1, OnLongestChain: true}))
		require.ErrorIs(t, err, errors.ErrTxExists)

		requireOneLanePosition(t, s, ctx, tx)
	})

	t.Run("after the stamp deleted the identity row", func(t *testing.T) {
		s, ctx := newLaneStore(t)

		tx := mkTx(t, 3, 6_000)

		_, err := s.Create(ctx, tx, 100, utxo.WithLanePosition(first))
		require.NoError(t, err)

		_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
		require.NoError(t, err)

		stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})

		_, err = s.Create(ctx, tx, 293, utxo.WithLanePosition(second),
			utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 9, BlockHeight: 293, OnLongestChain: true}))
		require.ErrorIs(t, err, errors.ErrTxExists)

		requireOneLanePosition(t, s, ctx, tx)
	})
}

// takeOrderSource records, at every Next, the connections the pool had out, which on this otherwise
// idle store are an open transaction's. It hands out positions of lane 201.
type takeOrderSource struct {
	pool                          *pgxpool.Pool
	nexts, connsAtNext, positions atomic.Int64
}

func (p *takeOrderSource) Next(context.Context) (uint64, error) {
	p.nexts.Add(1)
	p.connsAtNext.Add(int64(p.pool.Stat().AcquiredConns()))

	return lane.Pack(201, uint64(p.positions.Add(1))), nil //nolint:gosec // a test count
}

// TestUtxosetTakesPositionsBeforeItOpensATransaction: taking a position can wait, and must not wait
// with a Postgres transaction and its locks held, so each entry point that creates takes its
// positions before it opens one. A spend-only call takes none.
//
// Create has two paths. The batched one plans with no connection held. The direct one, taken by a
// conflicting create or a store with no create batcher, plans inside its transaction (createIn),
// which is where a wait would hold locks, so it has its own subtest.
func TestUtxosetTakesPositionsBeforeItOpensATransaction(t *testing.T) {
	ctx := t.Context()

	entryOn := func(t *testing.T, s *Store, call func(s *Store, w *tests.MultiWorkload) error) *takeOrderSource {
		t.Helper()

		w := tests.BuildMultiWorkload(t, 0xf4, 1, 1)
		w.StoreRoots(t, s, 900)

		source := &takeOrderSource{pool: s.pool}
		s.SetLanePositionSource(source)
		require.NoError(t, call(s, w))

		for _, tx := range w.Txs {
			md, err := s.Get(ctx, tx.TxIDChainHash(), fields.LanePosition)
			require.NoError(t, err)
			require.Equal(t, uint8(201), lane.Lane(md.LanePosition), "the record carries the position taken at the entry")
		}

		return source
	}

	entry := func(t *testing.T, call func(s *Store, w *tests.MultiWorkload) error) *takeOrderSource {
		t.Helper()

		s, _ := newUncheckpointedLaneStore(t)

		return entryOn(t, s, call)
	}

	createTx := func(s *Store, w *tests.MultiWorkload) error {
		_, err := s.Create(ctx, w.Txs[0], 901)
		return err
	}

	requireTakenFirst := func(t *testing.T, source *takeOrderSource) {
		t.Helper()
		require.NotZero(t, source.nexts.Load())
		require.Zero(t, source.connsAtNext.Load(), "a position was taken with a connection held")
	}

	t.Run("Create", func(t *testing.T) {
		requireTakenFirst(t, entry(t, createTx))
	})

	t.Run("Create on the direct path", func(t *testing.T) {
		s, _ := newTestStoreWith(t, func(ts *settings.Settings) {
			withCheckpoints(ts, nil)
			ts.UtxoStore.LaneMigration = settings.LaneMigrationShadow
			ts.UtxoStore.StoreBatcherSize = 1
		})
		require.Nil(t, s.createBatcher, "this subtest needs Create's direct path")

		requireTakenFirst(t, entryOn(t, s, createTx))
	})

	t.Run("SpendAndCreate", func(t *testing.T) {
		requireTakenFirst(t, entry(t, func(s *Store, w *tests.MultiWorkload) error {
			_, _, err := s.SpendAndCreate(ctx, w.Txs[0], 901)
			return err
		}))
	})

	t.Run("SpendAndCreateMulti", func(t *testing.T) {
		requireTakenFirst(t, entry(t, func(s *Store, w *tests.MultiWorkload) error {
			_, err := s.SpendAndCreateMulti(ctx, w.Txs, 901, utxo.WithIgnoreLocked(true))
			return err
		}))
	})

	t.Run("a spend-only call takes none", func(t *testing.T) {
		s, _ := newUncheckpointedLaneStore(t)

		w := tests.BuildMultiWorkload(t, 0xf4, 1, 1)
		w.StoreRoots(t, s, 900)

		source := &takeOrderSource{pool: s.pool}
		s.SetLanePositionSource(source)

		_, _, err := s.SpendAndCreate(ctx, w.Txs[0], 901, utxo.WithSpendOnly())
		require.NoError(t, err)
		require.Zero(t, source.nexts.Load())
	})
}

// TestLanePositionOnTheNettedBelowCheckpointWrite is the route quick validation and legacy sync take
// below the checkpoint. It is its own statement, so the contract's below-the-checkpoint case, which
// does not set the options that reach it, proves nothing about it. Each record gets its own position,
// taken for the whole list at the entry.
func TestLanePositionOnTheNettedBelowCheckpointWrite(t *testing.T) {
	s := newBodySkipLaneStore(t)
	ctx := t.Context()

	const height = 831

	w := tests.BuildMultiWorkload(t, 0xd9, 1, 1)
	w.StoreRoots(t, s, height-1)

	list := outpointOnly(nettedFamily(t, w))
	require.Len(t, list, 4)

	given := []uint64{lane.Pack(200, 1), lane.Pack(200, 2), lane.Pack(200, 3), lane.MaxPosition}
	s.SetLanePositionSource(tests.NewScriptedLanePositions(given...))

	results, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
	require.NoError(t, err)

	for i, r := range results {
		require.Equal(t, utxo.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
	}

	require.Zero(t, readNetState(t, s, ctx, w.Roots, list, height).journal, "this went through the netted write")

	got := make([]uint64, 0, len(list))

	for _, tx := range list {
		md, err := s.Get(ctx, tx.TxIDChainHash(), fields.LanePosition)
		require.NoError(t, err)

		got = append(got, md.LanePosition)
	}

	require.ElementsMatch(t, given, got, "each record its own position")
}

// Ignoring either netted route's position error must not let a list spend its roots or create
// records without positions.
func TestNettedMultiWithoutLanePositionsWritesNothing(t *testing.T) {
	for _, belowCheckpoint := range []bool{false, true} {
		name := "families"
		if belowCheckpoint {
			name = "below checkpoint"
		}

		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			var s *Store
			if belowCheckpoint {
				s = newBodySkipLaneStore(t)
			} else {
				s, _ = newUncheckpointedLaneStore(t)
			}

			const height = 831
			w := tests.BuildMultiWorkload(t, 0xda, 1, 1)
			w.StoreRoots(t, s, height-1)
			list := nettedFamily(t, w)
			opts := []utxo.CreateOption{utxo.WithIgnoreLocked(true)}
			if belowCheckpoint {
				list = outpointOnly(list)
				opts = belowCheckpointOptions(height)
			}

			prepared, err := utxo.PrepareSpendAndCreateMulti(list, opts...)
			require.NoError(t, err)
			below, err := s.nettedBelowGate(prepared.Options)
			require.NoError(t, err)
			require.Equal(t, belowCheckpoint, below, "the list selects the intended route")
			require.True(t, s.canNet(prepared.Options))

			before := readNetState(t, s, ctx, w.Roots, list, height)
			require.Equal(t, []string{"", ""}, w.RootSpends(t, s), "the root starts unspent")
			s.SetLanePositionSource(tests.NewScriptedLanePositions())

			results, err := s.SpendAndCreateMulti(ctx, list, height, opts...)
			require.ErrorIs(t, err, errors.ErrServiceUnavailable)
			require.Empty(t, results)
			require.Equal(t, before, readNetState(t, s, ctx, w.Roots, list, height), "no coins, journal or mined records changed")
			require.Equal(t, []string{"", ""}, w.RootSpends(t, s), "the root stays unspent")
			for _, tx := range list {
				_, err := s.Get(ctx, tx.TxIDChainHash(), fields.LanePosition)
				require.ErrorIs(t, err, errors.ErrTxNotFound, "no transaction of the list was stored")
			}
		})
	}
}

// TestLanePositionInABatchMixingMinedAndUnmined: one flush can carry both create statements, which
// makes each half a projection of the plan. Each half must keep its own positions.
func TestLanePositionInABatchMixingMinedAndUnmined(t *testing.T) {
	s, ctx := newCheckpointLaneStore(t)

	mined := mkTx(t, 1, 5_000)
	unmined := mkTx(t, 1, 6_000)

	wantMined, wantUnmined := lane.Pack(3, 11), lane.Pack(4, 22)

	minedOne := minedItem(mined, checkpointFloor-1, 1)
	minedOne.options.LanePosition = wantMined

	createBatchDirect(t, s, ctx, []*createItem{
		minedOne,
		{tx: unmined, blockHeight: checkpointFloor - 1, options: &utxo.CreateOptions{LanePosition: wantUnmined}},
	})

	requireGetLane(t, s, ctx, mined, wantMined, "the mined half")
	requireGetLane(t, s, ctx, unmined, wantUnmined, "the unmined half")

	v, null, ok := storedLane(t, s, ctx, "tx_mined", mined)
	require.True(t, ok)
	require.False(t, null)
	require.Equal(t, lane.ToSigned(wantMined), v)
}

// TestLanePositionThroughTheSecondTier: a read answered by the second tier returns the position,
// from either of its two sources. The containment row is the deterministic case: a parent spent
// before the stamp reached it, whose partition is below the lookup floor. The identity row only
// answers there in the create race, so that half runs the tier alone on a fresh result, as
// TestTier2BuildsAnUnminedRecordWithItsMarkerAndBody does.
func TestLanePositionThroughTheSecondTier(t *testing.T) {
	t.Run("from the containment row", func(t *testing.T) {
		s, ctx := newLaneStore(t)

		want := lane.Pack(200, 9)

		parent := mkTx(t, 1, 5_000)
		_, err := s.Create(ctx, parent, 99, utxo.WithLanePosition(want))
		require.NoError(t, err)

		_, err = s.SetMinedMulti(ctx, hashes(parent), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
		require.NoError(t, err)

		spendOneOutput(t, s, ctx, parent, 0, 101)
		stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})
		require.NoError(t, s.SetBlockHeight(1727))
		require.Equal(t, int32(864), s.lookupFloor(), "the partition is out of the first tier")

		before := testutil.ToFloat64(lookupTier2Keys.WithLabelValues("undo_zero"))

		requireGetLane(t, s, ctx, parent, want, "answered by the second tier")
		require.Equal(t, before+1, testutil.ToFloat64(lookupTier2Keys.WithLabelValues("undo_zero")), "through the second tier")
	})

	t.Run("from the identity row", func(t *testing.T) {
		s, ctx := newLaneStore(t)

		want := lane.Pack(201, 10)

		parent := mkTx(t, 2, 5_000)
		_, err := s.Create(ctx, parent, 100, utxo.WithLanePosition(want))
		require.NoError(t, err)

		h := *parent.TxIDChainHash()
		res := newLookupResult(1)
		res.wantLane = true

		require.NoError(t, s.readTier2(ctx, []chainhash.Hash{h}, &res))
		res.applyLanes()

		got, ok := res.found[h]
		require.True(t, ok)
		require.Equal(t, want, got.LanePosition)
	})
}

// TestLanePositionAbsentIsNullInEveryTable pins that a create made without a lane position,
// which is every create with the feature off, writes NULL on the identity and containment rows
// and reads back as lane.None.
func TestLanePositionAbsentIsNullInEveryTable(t *testing.T) {
	s, ctx := newLaneStore(t)

	tx := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, tx, 100)
	require.NoError(t, err)

	_, null, ok := storedLane(t, s, ctx, "tx_ident", tx)
	require.True(t, ok)
	require.True(t, null)

	_, err = s.SetMinedMulti(ctx, hashes(tx), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	_, null, ok = storedLane(t, s, ctx, "tx_mined", tx)
	require.True(t, ok)
	require.True(t, null)

	requireGetLane(t, s, ctx, tx, lane.None, "no lane position was written")

	// Not named, not returned: a read that does not ask leaves the field zero even when the row
	// holds one.
	withLane := mkTx(t, 2, 6_000)
	_, err = s.Create(ctx, withLane, 100, utxo.WithLanePosition(lane.Pack(1, 5)))
	require.NoError(t, err)

	got, err := s.Get(ctx, withLane.TxIDChainHash(), fields.Fee)
	require.NoError(t, err)
	require.Equal(t, lane.None, got.LanePosition)

	// The third table. A full preserved copy is taken from a stamped partition.
	stampThrough(t, s, ctx, 0, map[uint32]uint32{100: 7})
	require.NoError(t, s.PreserveTransactions(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, 5_000))

	_, null, ok = storedLane(t, s, ctx, "preserved_parent", tx)
	require.True(t, ok, "the preserved copy exists")
	require.True(t, null, "the preserved copy takes the NULL")
}

// TestLanePositionColumnsAreAddedToAnExistingSchema is the migration: a database made before the
// column existed gains it on the tables and partitions that already exist, with no value, and a
// second pass changes nothing.
func TestLanePositionColumnsAreAddedToAnExistingSchema(t *testing.T) {
	s, ctx := newLaneStore(t)

	for _, table := range laneColumnTables {
		require.True(t, hasLaneColumn(t, s, ctx, table), "the flag gives a fresh store %s.lane_position from its CREATE TABLE", table)
	}

	// Put one containment partition in place first, so there is an attached tx_mined partition
	// for the migration to reach as well as the parents.
	old := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, old, 100)
	require.NoError(t, err)
	_, err = s.SetMinedMulti(ctx, hashes(old), utxo.MinedBlockInfo{BlockID: 7, BlockHeight: 100, OnLongestChain: true})
	require.NoError(t, err)

	// Make the schema the previous build made. Dropping the column on a partitioned parent drops
	// it on every partition.
	for _, table := range []string{"tx_ident", "tx_mined", "preserved_parent"} {
		_, err = s.pool.Exec(ctx, `ALTER TABLE `+table+` DROP COLUMN lane_position`)
		require.NoError(t, err)
	}

	for _, table := range []string{"tx_ident", "tx_ident_l0", "tx_mined", "preserved_parent"} {
		require.False(t, hasLaneColumn(t, s, ctx, table), "%s is the old shape before the migration", table)
	}

	require.NoError(t, addLanePositionColumns(ctx, s.pool, laneColumnMigrationLimit))

	var partitions []string

	rows, err := s.pool.Query(ctx, `
		SELECT c.relname FROM pg_class c
		  JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = current_schema() AND c.relispartition
		   AND c.relname ~ '^(tx_ident_l|tx_mined_w)[0-9]+$'`)
	require.NoError(t, err)

	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))

		partitions = append(partitions, name)
	}

	rows.Close()
	require.NoError(t, rows.Err())
	require.NotEmpty(t, partitions)

	for _, table := range append([]string{"tx_ident", "tx_mined", "preserved_parent"}, partitions...) {
		require.True(t, hasLaneColumn(t, s, ctx, table), "%s gains lane_position", table)
	}

	// No backfill: the transaction written before the column existed reads back with none, and a
	// create after the migration keeps its value.
	requireGetLane(t, s, ctx, old, lane.None, "a row from before the column")

	fresh := mkTx(t, 1, 7_000)
	want := lane.Pack(2, 77)
	_, err = s.Create(ctx, fresh, 101, utxo.WithLanePosition(want))
	require.NoError(t, err)
	requireGetLane(t, s, ctx, fresh, want, "a create after the migration")

	// Running the schema again is a no-op.
	require.NoError(t, addLanePositionColumns(ctx, s.pool, laneColumnMigrationLimit))
	requireGetLane(t, s, ctx, fresh, want, "after a second schema pass")
}

// TestLanePositionMigrationHoldsOneTableAtATime is why the migration is not in schemaSQL. With a
// transaction open on tx_mined, the migration waits for it, and while it waits tx_ident has been
// altered and committed and answers queries, so no table is held while waiting on another.
func TestLanePositionMigrationHoldsOneTableAtATime(t *testing.T) {
	s, ctx := newLaneStore(t)

	for _, table := range laneColumnTables {
		_, err := s.pool.Exec(ctx, `ALTER TABLE `+table+` DROP COLUMN lane_position`)
		require.NoError(t, err)
	}

	blocker, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = blocker.Rollback(ctx) }()

	var n int64
	require.NoError(t, blocker.QueryRow(ctx, `SELECT count(*) FROM tx_mined`).Scan(&n))

	migrated := make(chan error, 1)

	go func() { migrated <- addLanePositionColumns(ctx, s.pool, laneColumnMigrationLimit) }()

	altered := false

	for give := time.Now().Add(time.Minute); time.Now().Before(give); time.Sleep(10 * time.Millisecond) {
		if altered = hasLaneColumn(t, s, ctx, "tx_ident"); altered {
			break
		}
	}

	require.True(t, altered, "tx_ident is altered without waiting for tx_mined")

	// Give the migration time to queue for tx_mined. A query that arrives while the ALTER is queued
	// queues behind it, so without the lock_timeout this read waits for the open transaction, which
	// does not end until after it.
	time.Sleep(100 * time.Millisecond)

	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	require.NoError(t, s.pool.QueryRow(readCtx, `SELECT count(*) FROM tx_ident`).Scan(&n), "tx_ident is not held while the migration waits for tx_mined")
	require.NoError(t, s.pool.QueryRow(readCtx, `SELECT count(*) FROM tx_mined`).Scan(&n), "a query on the table the migration is waiting for gets through")
	require.False(t, hasLaneColumn(t, s, ctx, "tx_mined"), "the open transaction is still holding the migration off tx_mined")

	require.NoError(t, blocker.Commit(ctx))

	select {
	case err := <-migrated:
		require.NoError(t, err)
	case <-time.After(time.Minute):
		require.Fail(t, "the migration did not finish once the transaction ended")
	}

	for _, table := range laneColumnTables {
		require.True(t, hasLaneColumn(t, s, ctx, table), "%s gains lane_position", table)
	}
}

// If another process keeps a transaction open past the retry limit, the store refuses to open with
// the lock timeout error, and the next start tries again.
func TestLanePositionMigrationGivesUpAtItsLimit(t *testing.T) {
	s, ctx := newLaneStore(t)

	for _, table := range laneColumnTables {
		_, err := s.pool.Exec(ctx, `ALTER TABLE `+table+` DROP COLUMN lane_position`)
		require.NoError(t, err)
	}

	blocker, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = blocker.Rollback(ctx) }()

	var n int64
	require.NoError(t, blocker.QueryRow(ctx, `SELECT count(*) FROM tx_mined`).Scan(&n))

	err = addLanePositionColumns(ctx, s.pool, time.Millisecond)
	require.Error(t, err, "the transaction is still open at the limit")
	require.Contains(t, err.Error(), "lock timeout", "the error says why")
	require.Contains(t, err.Error(), "tx_mined", "the error names the table")

	require.NoError(t, blocker.Rollback(ctx))
	require.NoError(t, addLanePositionColumns(ctx, s.pool, time.Millisecond), "the next start tries again")

	for _, table := range laneColumnTables {
		require.True(t, hasLaneColumn(t, s, ctx, table), "%s gains lane_position", table)
	}
}

// Every process that opens the store runs the migration. On a schema that already has the columns
// it must look first and not ask for the lock, or every start would queue behind the longest open
// transaction on these tables.
func TestLanePositionMigrationOnAMigratedSchemaTakesNoLock(t *testing.T) {
	s, ctx := newLaneStore(t)

	blocker, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = blocker.Rollback(ctx) }()

	var n int64
	require.NoError(t, blocker.QueryRow(ctx, `SELECT count(*) FROM tx_mined`).Scan(&n))

	// Without the catalog check this retries until the context ends, because the transaction stays
	// open throughout.
	schemaCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	require.NoError(t, addLanePositionColumns(schemaCtx, s.pool, laneColumnMigrationLimit))
	require.NoError(t, blocker.QueryRow(ctx, `SELECT 1`).Scan(&n), "the transaction was open throughout")
}

// addLane keeps a plan's lanes nil until a row has a position, then fills zeros for the rows before
// it. Without those zeros the array is shorter than the plan, every position lands on the wrong
// row and the rows at the end get none. Each statement gets a plan of its own, so the position
// arrays line up with no help from a projection. The zeros are the plan's own: the rows with no
// position must store NULL, in every table the route writes.
func TestLanePositionsKeepTheirRowsWhenTheFirstRowsHaveNone(t *testing.T) {
	want := []uint64{lane.None, lane.None, lane.Pack(3, 11), lane.Pack(4, 22)}

	checkpointed := func(t *testing.T) (*Store, context.Context) { return newCheckpointLaneStore(t) }

	blockCarrying := func(tx *bt.Tx, position uint64) *createItem {
		it := minedItem(tx, checkpointFloor-1, 1)
		it.options.LanePosition = position

		return it
	}

	for _, route := range []struct {
		name   string
		store  func(t *testing.T) (*Store, context.Context)
		tables []string
		item   func(tx *bt.Tx, position uint64) *createItem
	}{
		{"createIdentPlan", checkpointed, []string{"tx_ident"}, func(tx *bt.Tx, position uint64) *createItem {
			return &createItem{tx: tx, blockHeight: checkpointFloor - 1, options: &utxo.CreateOptions{LanePosition: position}}
		}},
		{"createIdentPlan with a block", newUncheckpointedLaneStore, []string{"tx_ident", "tx_mined"}, blockCarrying},
		{"createMinedPlan", checkpointed, []string{"tx_mined"}, blockCarrying},
	} {
		t.Run(route.name, func(t *testing.T) {
			s, ctx := route.store(t)

			txs := make([]*bt.Tx, len(want))
			items := make([]*createItem, len(want))

			for i, position := range want {
				txs[i] = mkTx(t, 1, uint64(5_000+i)) //nolint:gosec // test data
				items[i] = route.item(txs[i], position)
			}

			createBatchDirect(t, s, ctx, items)

			for i, tx := range txs {
				requireGetLane(t, s, ctx, tx, want[i], route.name)

				if want[i] != lane.None {
					continue
				}

				for _, table := range route.tables {
					_, null, ok := storedLane(t, s, ctx, table, tx)
					require.True(t, ok, "%s holds a row", table)
					require.True(t, null, "%s stores NULL, not 0", table)
				}
			}
		})
	}
}

func hasLaneColumn(t *testing.T, s *Store, ctx context.Context, table string) bool {
	t.Helper()

	var found bool

	require.NoError(t, s.pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM information_schema.columns
		                WHERE table_schema = current_schema() AND table_name = $1
		                  AND column_name = 'lane_position' AND data_type = 'bigint'
		                  AND is_nullable = 'YES' AND column_default IS NULL)`, table).Scan(&found))

	return found
}

// TestUtxosetRolledBackRoundKeepsItsPositions: a batch whose first round is rolled back, here by a
// forged claim among honest siblings, plans its creates again in the next round. Each surviving
// record carries the position its caller took at the entry, and the round takes none of its own.
func TestUtxosetRolledBackRoundKeepsItsPositions(t *testing.T) {
	s, ctx := newLaneStore(t)

	ps := parents(t, s, ctx, 3, 100)

	source := tests.NewScriptedLanePositions()
	s.SetLanePositionSource(source)

	honestA := spendOutput(t, ps[0], 0, 1)
	forged := spendOutput(t, ps[1], 0, 1)
	forged.Inputs[0].PreviousTxSatoshis = 5_000_000_000
	honestB := spendOutput(t, ps[2], 0, 1)

	wantA, wantForged, wantB := lane.Pack(9, 1), lane.Pack(9, 2), lane.Pack(9, 3)

	res := flush(t, s,
		batchItem(honestA, 101, utxo.WithLanePosition(wantA)),
		batchItem(forged, 101, utxo.WithLanePosition(wantForged)),
		batchItem(honestB, 101, utxo.WithLanePosition(wantB)))

	require.NoError(t, res[0].err)
	require.Error(t, res[1].err)
	require.NoError(t, res[2].err)

	requireGetLane(t, s, ctx, honestA, wantA, "the first honest sibling")
	requireGetLane(t, s, ctx, honestB, wantB, "the second honest sibling")
	require.Zero(t, source.Calls(), "a round takes no positions of its own")
}

// TestUtxosetRerunChunkKeepsItsPositions: the netted write finds a transaction of the list already
// stored under its lock, rolls the chunk back and runs it again without that one. The rerun plans
// its creates again and must reuse the positions taken at the entry: one per transaction, never more.
func TestUtxosetRerunChunkKeepsItsPositions(t *testing.T) {
	s, ctx := newUncheckpointedLaneStore(t)

	w := tests.BuildMultiWorkload(t, 0xf6, 2, 2)
	w.StoreRoots(t, s, 900)

	stored := lane.Pack(8, 1)
	_, err := s.Create(ctx, w.Txs[0], 901, utxo.WithLanePosition(stored))
	require.NoError(t, err)

	given := lanePositionsOf(9, len(w.Txs))
	source := tests.NewScriptedLanePositions(given...)
	s.SetLanePositionSource(source)

	results, err := s.SpendAndCreateMulti(ctx, w.Txs, 901, utxo.WithIgnoreLocked(true))
	require.NoError(t, err)
	require.Equal(t, utxo.MultiTxExisted, results[0].Status, "%v", results[0].Err)

	for i := 1; i < len(results); i++ {
		require.Equal(t, utxo.MultiTxCreated, results[i].Status, "tx %d: %v", i, results[i].Err)
	}

	require.Equal(t, len(w.Txs), source.Calls(), "one position per transaction, taken once")

	requireGetLane(t, s, ctx, w.Txs[0], stored, "the stored record keeps its own")

	for i := 1; i < len(w.Txs); i++ {
		requireGetLane(t, s, ctx, w.Txs[i], given[i], fmt.Sprintf("tx %d", i))
	}
}

func lanePositionsOf(l uint8, n int) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		out[i] = lane.Pack(l, uint64(i)+1) //nolint:gosec // i counts up from 0
	}

	return out
}

// useBurnSource gives s a source of positions for the rest of the test.
func useBurnSource(t *testing.T, s *Store, positions ...uint64) *tests.ScriptedLanePositions {
	t.Helper()

	source := tests.NewScriptedLanePositions(positions...)
	s.SetLanePositionSource(source)
	t.Cleanup(func() { s.SetLanePositionSource(nil) })

	return source
}

// burnPositions makes n positions in lane 30.
func burnPositions(n int) []uint64 {
	positions := make([]uint64, n)
	for i := range positions {
		positions[i] = lane.Pack(30, uint64(i)+1) //nolint:gosec // i counts up from 0
	}

	return positions
}

// The burn paths only the netted list writes have, which the shared contract cannot reach: a list
// takes one position per transaction before it writes anything, so whatever stops a transaction
// being written burns its position, and a list that fails part way still wrote the chunks that
// committed.
func TestLanePositionBurnsOfANettedList(t *testing.T) {
	failure := errors.NewProcessingError("injected failure")
	sv := utxo.WithCaller(utxo.CallerSubtreeValidation)

	t.Run("a chunk that fails burns rolled_back, and a chunk behind it list_failed", func(t *testing.T) {
		s, ctx := newUncheckpointedLaneStore(t)
		forceChunkOfOne(t)

		root := storedParent(t, s, ctx)
		parent := spendOutput(t, root, 0, 2)
		child := spendOutput(t, parent, 0, 3)
		grandchild := spendOutput(t, child, 0, 1)

		// Chunk 0 holds parent and commits. Chunk 1 holds child and fails. Chunk 2 holds
		// grandchild, waits for chunk 1, and never runs.
		multiChunkFault = func(chunk int, stage string) error {
			if chunk == 1 && stage == "spent" {
				return failure
			}

			return nil
		}

		t.Cleanup(func() { multiChunkFault = nil })

		useBurnSource(t, s, burnPositions(3)...)
		before := tests.ReadLaneBurns(t, "utxoset")

		_, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{parent, child, grandchild}, 1_100, utxo.WithIgnoreLocked(true), sv)
		multiChunkFault = nil
		require.ErrorIs(t, err, failure)

		require.Equal(t, tests.LaneBurns{
			utxo.LaneBurnRolledBack + "/subtree_validation": 1,
			utxo.LaneBurnListFailed + "/subtree_validation": 1,
		}, tests.ReadLaneBurns(t, "utxoset").Since(before))

		require.Equal(t, 1, identityCount(t, s, ctx, parent), "the committed chunk's position was written")
	})

	t.Run("a refused spend burns spend_rejected, and its child parent_failed", func(t *testing.T) {
		s, ctx := newUncheckpointedLaneStore(t)

		root := storedParent(t, s, ctx)

		// Spends root output 0 first, so the list's own spend of it is refused.
		_, _, err := s.SpendAndCreate(ctx, spendOutput(t, root, 0, 4), 1_100)
		require.NoError(t, err)

		parent := spendOutput(t, root, 0, 2)
		child := spendOutput(t, parent, 0, 1)

		useBurnSource(t, s, burnPositions(2)...)
		before := tests.ReadLaneBurns(t, "utxoset")

		results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{parent, child}, 1_100, utxo.WithIgnoreLocked(true), sv)
		require.NoError(t, err)
		require.Equal(t, utxo.MultiTxFailed, results[0].Status)
		require.Equal(t, utxo.MultiTxParentFailed, results[1].Status)

		require.Equal(t, tests.LaneBurns{
			utxo.LaneBurnSpendRejected + "/subtree_validation": 1,
			utxo.LaneBurnParentFailed + "/subtree_validation":  1,
		}, tests.ReadLaneBurns(t, "utxoset").Since(before))
	})

	t.Run("a take that fails part way burns take_failed for the positions already taken", func(t *testing.T) {
		s, ctx := newUncheckpointedLaneStore(t)

		root := storedParent(t, s, ctx)
		a := spendOutput(t, root, 0, 1)
		b := spendOutput(t, root, 1, 1)

		source := useBurnSource(t, s, burnPositions(1)...)
		before := tests.ReadLaneBurns(t, "utxoset")

		_, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{a, b}, 1_100, utxo.WithIgnoreLocked(true), sv)
		require.ErrorIs(t, err, errors.ErrServiceUnavailable)
		require.Equal(t, 2, source.Calls())

		require.Equal(t, tests.LaneBurns{utxo.LaneBurnTakeFailed + "/subtree_validation": 1},
			tests.ReadLaneBurns(t, "utxoset").Since(before))
	})

	t.Run("a below-checkpoint chunk that fails burns rolled_back, and the chunk after it list_failed", func(t *testing.T) {
		const height = 900

		// One worker runs the chunks in list order, so the first commits before the second fails:
		// with chunks side by side, the failure could cancel the first one's commit, which counts
		// outcome_unknown.
		s, ctx := newTestStoreWith(t, func(ts *settings.Settings) {
			ts.UtxoStore.LaneMigration = settings.LaneMigrationShadow
			ts.UtxoStore.NettedBelowWorkers = 1
		})
		require.Equal(t, 1, s.nettedBelowWorkers())
		withNettedBelowChunkTxs(t, 1)

		w := tests.BuildMultiWorkload(t, 0xc8, 1, 3)
		w.StoreRoots(t, s, height-1)

		list := outpointOnly(w.Txs)

		// The second transaction's chunk fails; the first commits; the third never starts.
		nettedBelowFault = func(chunk []int) error {
			if len(chunk) == 1 && chunk[0] == 1 {
				return failure
			}

			return nil
		}

		t.Cleanup(func() { nettedBelowFault = nil })

		useBurnSource(t, s, burnPositions(3)...)
		before := tests.ReadLaneBurns(t, "utxoset")

		_, err := s.SpendAndCreateMulti(ctx, list, height,
			append(belowCheckpointOptions(height), utxo.WithCaller(utxo.CallerBlockValidation))...)
		nettedBelowFault = nil
		require.ErrorIs(t, err, failure)

		require.Equal(t, tests.LaneBurns{
			utxo.LaneBurnRolledBack + "/block_validation": 1,
			utxo.LaneBurnListFailed + "/block_validation": 1,
		}, tests.ReadLaneBurns(t, "utxoset").Since(before))

		_, err = s.Get(ctx, list[0].TxIDChainHash())
		require.NoError(t, err, "the first chunk committed its position")

		for _, tx := range list[1:] {
			_, err = s.Get(ctx, tx.TxIDChainHash())
			require.ErrorIs(t, err, errors.ErrTxNotFound, "the failed chunk rolled back, and the one after it never ran")
		}
	})
}

// A create whose batch rolls back burns its position as rolled_back, not as a plain error.
func TestLanePositionBurnOfARolledBackCreateBatch(t *testing.T) {
	s, ctx := newUncheckpointedLaneStore(t)
	require.NotNil(t, s.createBatcher, "the test needs the batched create")

	useBurnSource(t, s, burnPositions(1)...)
	before := tests.ReadLaneBurns(t, "utxoset")

	// The batch fails with its database transaction open, so it rolls back.
	createBatchBeforeCommit = func() error { return errors.NewStorageError("injected failure before commit") }
	t.Cleanup(func() { createBatchBeforeCommit = nil })

	tx := mkTx(t, 1, 5_000)

	_, err := s.Create(ctx, tx, 1_000, utxo.WithCaller(utxo.CallerBlockAssembly))
	require.Error(t, err)

	require.Equal(t, tests.LaneBurns{utxo.LaneBurnRolledBack + "/block_assembly": 1},
		tests.ReadLaneBurns(t, "utxoset").Since(before))

	// The batch really rolled back: nothing of the transaction is stored.
	require.Equal(t, 0, identityCount(t, s, ctx, tx))
	require.Equal(t, 0, utxoCount(t, s, ctx, tx))

	_, err = s.Get(ctx, tx.TxIDChainHash())
	require.ErrorIs(t, err, errors.ErrTxNotFound)
}

// A batch whose COMMIT landed but reported failure reruns its items alone with the positions they
// took. The rerun finds each item's own record and answers ErrTxExists, which is the item's own
// write, not a burn.
func TestLanePositionBurnOfABatchRerunAfterALandedCommit(t *testing.T) {
	s, ctx := newUncheckpointedLaneStore(t)
	require.NotNil(t, s.spendAndCreateBatcher, "the test needs the batched spend-and-create")

	root := tests.BuildMultiWorkload(t, 0xc9, 1, 1).Roots[0]
	position := burnPositions(1)[0]

	useBurnSource(t, s, position)

	failed := false
	spendAndCreateBatchAfterCommit = func() error {
		if failed {
			return nil
		}

		failed = true

		return errors.NewStorageError("injected lost commit acknowledgement")
	}

	t.Cleanup(func() { spendAndCreateBatchAfterCommit = nil })

	before := tests.ReadLaneBurns(t, "utxoset")

	_, _, err := s.SpendAndCreate(ctx, root, 1_000, utxo.WithCreateOnly(), utxo.WithCaller(utxo.CallerValidator))
	require.ErrorIs(t, err, errors.ErrTxExists, "the rerun finds the record the batch wrote")
	require.True(t, failed)

	require.Equal(t, tests.LaneBurns{}, tests.ReadLaneBurns(t, "utxoset").Since(before), "the record carries the position")

	md, err := s.Get(ctx, root.TxIDChainHash(), fields.LanePosition)
	require.NoError(t, err)
	require.Equal(t, position, md.LanePosition)
}

// After a rerun fails, the stored record decides: the same position burned nothing, whatever the
// rerun met; another position burned for the rerun's own reason; no record, or a record that
// cannot be read within the bound, is an unknown outcome. The caller's context ending does not cut
// the read short.
func TestLanePositionBurnUnlessStored(t *testing.T) {
	s, ctx := newUncheckpointedLaneStore(t)

	stored := tests.BuildMultiWorkload(t, 0xca, 1, 1).Roots[0]
	mine := lane.Pack(31, 1)

	_, _, err := s.SpendAndCreate(ctx, stored, 1_000, utxo.WithCreateOnly(), utxo.WithLanePosition(mine))
	require.NoError(t, err)

	options := &utxo.CreateOptions{Caller: utxo.CallerValidator}
	exists := errors.NewTxExistsError("exists")
	storage := errors.NewStorageError("begin failed")

	burns := func(f func()) tests.LaneBurns {
		before := tests.ReadLaneBurns(t, "utxoset")

		f()

		return tests.ReadLaneBurns(t, "utxoset").Since(before)
	}

	require.Equal(t, tests.LaneBurns{}, burns(func() { s.burnUnlessStored(ctx, stored, options, mine, exists, nil) }),
		"the record carries the position")

	require.Equal(t, tests.LaneBurns{}, burns(func() { s.burnUnlessStored(ctx, stored, options, mine, storage, nil) }),
		"the record carries the position, though the rerun failed for another reason")

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	require.Equal(t, tests.LaneBurns{}, burns(func() { s.burnUnlessStored(cancelled, stored, options, mine, storage, nil) }),
		"the caller's context ended, and the read still finds the position")

	require.Equal(t, tests.LaneBurns{utxo.LaneBurnExists + "/validator": 1},
		burns(func() { s.burnUnlessStored(ctx, stored, options, lane.Pack(31, 2), exists, nil) }),
		"the record carries another position")

	require.Equal(t, tests.LaneBurns{utxo.LaneBurnError + "/validator": 1},
		burns(func() { s.burnUnlessStored(ctx, stored, options, lane.Pack(31, 6), storage, nil) }),
		"another position, and the rerun failed for a storage error")

	refused := []*utxo.Spend{{Err: errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil)}}
	require.Equal(t, tests.LaneBurns{utxo.LaneBurnSpendRejected + "/validator": 1},
		burns(func() { s.burnUnlessStored(ctx, stored, options, lane.Pack(31, 7), storage, refused) }),
		"another position, and the rerun's spend was refused")

	missing := tests.BuildMultiWorkload(t, 0xcb, 1, 1).Roots[0]
	require.Equal(t, tests.LaneBurns{utxo.LaneBurnOutcomeUnknown + "/validator": 1},
		burns(func() { s.burnUnlessStored(ctx, missing, options, lane.Pack(31, 3), storage, nil) }),
		"no record proves nothing: the failed batch's commit may still be completing")

	// A read that cannot finish within the bound gives up, so it never holds the caller. The
	// production bound is 5 seconds.
	require.Equal(t, 5*time.Second, laneBurnProbeTimeout)

	old := laneBurnProbeTimeout
	laneBurnProbeTimeout = time.Nanosecond

	t.Cleanup(func() { laneBurnProbeTimeout = old })

	start := time.Now()
	require.Equal(t, tests.LaneBurns{utxo.LaneBurnOutcomeUnknown + "/validator": 1},
		burns(func() { s.burnUnlessStored(ctx, stored, options, lane.Pack(31, 4), exists, nil) }))
	require.Less(t, time.Since(start), time.Second)
}

// A batch whose COMMIT landed but reported failure reruns its item, and the rerun can fail too,
// here at its own commit. The record the batch wrote carries the position, so nothing burned.
func TestLanePositionBurnOfAFailedRerunAfterALandedCommit(t *testing.T) {
	s, ctx := newUncheckpointedLaneStore(t)
	require.NotNil(t, s.spendAndCreateBatcher)

	root := tests.BuildMultiWorkload(t, 0xcd, 1, 1).Roots[0]
	position := burnPositions(1)[0]

	useBurnSource(t, s, position)

	var batchFailed atomic.Bool

	spendAndCreateBatchAfterCommit = func() error {
		if batchFailed.CompareAndSwap(false, true) {
			return errors.NewStorageError("injected lost batch commit acknowledgement")
		}

		return nil
	}

	t.Cleanup(func() { spendAndCreateBatchAfterCommit = nil })

	_ = failNextCommit(t)

	before := tests.ReadLaneBurns(t, "utxoset")

	_, _, err := s.SpendAndCreate(ctx, root, 1_000, utxo.WithCreateOnly(), utxo.WithCaller(utxo.CallerValidator))
	require.Error(t, err)
	require.False(t, errors.Is(err, errors.ErrTxExists), "the rerun failed at its commit: %v", err)

	require.Equal(t, tests.LaneBurns{}, tests.ReadLaneBurns(t, "utxoset").Since(before), "the record carries the position")

	md, err := s.Get(ctx, root.TxIDChainHash(), fields.LanePosition)
	require.NoError(t, err)
	require.Equal(t, position, md.LanePosition)
}

// failNextCommit makes the next commit of a transaction that writes creates report failure after it
// landed, as a lost acknowledgement does.
func failNextCommit(t *testing.T) error {
	t.Helper()

	failure := errors.NewStorageError("injected lost commit acknowledgement")

	var once atomic.Bool

	createCommitFault = func() error {
		if once.CompareAndSwap(false, true) {
			return failure
		}

		return nil
	}

	t.Cleanup(func() { createCommitFault = nil })

	return failure
}

// newUnbatchedLaneStore is a lane store with no create or spend-and-create batcher, so every create
// takes the single-write path.
func newUnbatchedLaneStore(t *testing.T) (*Store, context.Context) {
	t.Helper()

	s, ctx := newTestStoreWith(t, func(ts *settings.Settings) {
		withCheckpoints(ts, nil)
		ts.UtxoStore.LaneMigration = settings.LaneMigrationShadow
		ts.UtxoStore.StoreBatcherSize = 1
	})
	require.Nil(t, s.createBatcher)
	require.Nil(t, s.spendAndCreateBatcher)

	return s, ctx
}

// A COMMIT that reports failure may have landed, so the position it carried counts as
// outcome_unknown, on every utxoset path that commits creates.
func TestLanePositionBurnOfACommitThatReportedFailure(t *testing.T) {
	unknown := func(caller string) tests.LaneBurns {
		return tests.LaneBurns{utxo.LaneBurnOutcomeUnknown + "/" + caller: 1}
	}

	t.Run("a single-write Create", func(t *testing.T) {
		s, ctx := newUnbatchedLaneStore(t)
		useBurnSource(t, s, burnPositions(1)...)
		_ = failNextCommit(t)

		before := tests.ReadLaneBurns(t, "utxoset")

		_, err := s.Create(ctx, mkTx(t, 1, 5_000), 1_000, utxo.WithCaller(utxo.CallerBlockAssembly))
		require.Error(t, err)

		require.Equal(t, unknown("block_assembly"), tests.ReadLaneBurns(t, "utxoset").Since(before))
	})

	t.Run("a single-write SpendAndCreate", func(t *testing.T) {
		s, ctx := newUnbatchedLaneStore(t)
		useBurnSource(t, s, burnPositions(1)...)
		_ = failNextCommit(t)

		before := tests.ReadLaneBurns(t, "utxoset")

		_, _, err := s.SpendAndCreate(ctx, mkTx(t, 1, 5_000), 1_000, utxo.WithCreateOnly(), utxo.WithCaller(utxo.CallerValidator))
		require.Error(t, err)

		require.Equal(t, unknown("validator"), tests.ReadLaneBurns(t, "utxoset").Since(before))
	})

	t.Run("a create batch", func(t *testing.T) {
		s, ctx := newUncheckpointedLaneStore(t)
		require.NotNil(t, s.createBatcher)
		useBurnSource(t, s, burnPositions(1)...)
		_ = failNextCommit(t)

		before := tests.ReadLaneBurns(t, "utxoset")

		_, err := s.Create(ctx, mkTx(t, 1, 5_000), 1_000, utxo.WithCaller(utxo.CallerBlockAssembly))
		require.Error(t, err)

		require.Equal(t, unknown("block_assembly"), tests.ReadLaneBurns(t, "utxoset").Since(before))
	})

	t.Run("a list chunk above the checkpoint", func(t *testing.T) {
		s, ctx := newUncheckpointedLaneStore(t)
		forceChunkOfOne(t)

		root := storedParent(t, s, ctx)
		a := spendOutput(t, root, 0, 1)

		useBurnSource(t, s, burnPositions(1)...)
		failure := failNextCommit(t)

		before := tests.ReadLaneBurns(t, "utxoset")

		_, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{a}, 1_100, utxo.WithIgnoreLocked(true),
			utxo.WithCaller(utxo.CallerSubtreeValidation))
		require.ErrorIs(t, err, failure)

		require.Equal(t, unknown("subtree_validation"), tests.ReadLaneBurns(t, "utxoset").Since(before))
		require.Equal(t, 1, identityCount(t, s, ctx, a), "the commit landed")
	})

	t.Run("a list chunk whose earlier round settled some transactions", func(t *testing.T) {
		s, ctx := newUncheckpointedLaneStore(t)

		root := storedParent(t, s, ctx)

		// Spends root output 0 first, so the list's own spend of it is refused.
		_, _, err := s.SpendAndCreate(ctx, spendOutput(t, root, 0, 4), 1_100)
		require.NoError(t, err)

		refused := spendOutput(t, root, 0, 2)
		child := spendOutput(t, refused, 0, 1)
		valid := spendOutput(t, root, 1, 3)

		useBurnSource(t, s, burnPositions(3)...)
		failure := failNextCommit(t)

		before := tests.ReadLaneBurns(t, "utxoset")

		// The first round finds the refused spend and rolls back; the second writes only valid,
		// and its commit reports failure.
		_, err = s.SpendAndCreateMulti(ctx, []*bt.Tx{refused, child, valid}, 1_100, utxo.WithIgnoreLocked(true),
			utxo.WithCaller(utxo.CallerSubtreeValidation))
		require.ErrorIs(t, err, failure)

		require.Equal(t, tests.LaneBurns{
			utxo.LaneBurnSpendRejected + "/subtree_validation":  1,
			utxo.LaneBurnParentFailed + "/subtree_validation":   1,
			utxo.LaneBurnOutcomeUnknown + "/subtree_validation": 1,
		}, tests.ReadLaneBurns(t, "utxoset").Since(before))
	})

	t.Run("a list chunk that found a stored transaction", func(t *testing.T) {
		s, ctx := newUncheckpointedLaneStore(t)

		root := storedParent(t, s, ctx)
		stored := spendOutput(t, root, 0, 2)
		fresh := spendOutput(t, root, 1, 3)

		_, _, err := s.SpendAndCreate(ctx, stored, 1_100)
		require.NoError(t, err)

		useBurnSource(t, s, burnPositions(2)...)
		failure := failNextCommit(t)

		before := tests.ReadLaneBurns(t, "utxoset")

		// The chunk finds stored already held and settles it as existing; its write of fresh
		// commits and reports failure.
		_, err = s.SpendAndCreateMulti(ctx, []*bt.Tx{stored, fresh}, 1_100, utxo.WithIgnoreLocked(true),
			utxo.WithCaller(utxo.CallerSubtreeValidation))
		require.ErrorIs(t, err, failure)

		require.Equal(t, tests.LaneBurns{
			utxo.LaneBurnExists + "/subtree_validation":         1,
			utxo.LaneBurnOutcomeUnknown + "/subtree_validation": 1,
		}, tests.ReadLaneBurns(t, "utxoset").Since(before))
	})

	t.Run("a list chunk below the checkpoint", func(t *testing.T) {
		const height = 900

		s, ctx := newLaneStore(t)
		withNettedBelowChunkTxs(t, 1)

		w := tests.BuildMultiWorkload(t, 0xcc, 1, 1)
		w.StoreRoots(t, s, height-1)

		useBurnSource(t, s, burnPositions(1)...)
		failure := failNextCommit(t)

		before := tests.ReadLaneBurns(t, "utxoset")

		_, err := s.SpendAndCreateMulti(ctx, outpointOnly(w.Txs), height,
			append(belowCheckpointOptions(height), utxo.WithCaller(utxo.CallerBlockValidation))...)
		require.ErrorIs(t, err, failure)

		require.Equal(t, unknown("block_validation"), tests.ReadLaneBurns(t, "utxoset").Since(before))
	})
}

// A caller that stops waiting for its queued create counts its position as outcome_unknown. The batch
// still writes the record with the position, and its answer counts nothing more.
func TestLanePositionBurnOfAWaiterThatGaveUp(t *testing.T) {
	s, ctx := newUncheckpointedLaneStore(t)
	require.NotNil(t, s.createBatcher)

	position := burnPositions(1)[0]
	useBurnSource(t, s, position)

	held := make(chan struct{})
	release := make(chan struct{})
	markHeld := sync.OnceFunc(func() { close(held) })
	releaseBatch := sync.OnceFunc(func() { close(release) })

	createBatchBeforeCommit = func() error {
		markHeld()
		<-release

		return nil
	}

	// Released before the store closes, even when an assertion below fails, so a failure cannot
	// leave the batch, and the store's Close, waiting forever.
	t.Cleanup(func() {
		releaseBatch()
		createBatchBeforeCommit = nil
	})

	tx := mkTx(t, 1, 5_000)
	callCtx, cancel := context.WithCancel(ctx)

	before := tests.ReadLaneBurns(t, "utxoset")
	done := make(chan error, 1)

	go func() {
		_, err := s.Create(callCtx, tx, 1_000, utxo.WithCaller(utxo.CallerBlockAssembly))
		done <- err
	}()

	<-held
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)

	require.Equal(t, tests.LaneBurns{utxo.LaneBurnOutcomeUnknown + "/block_assembly": 1},
		tests.ReadLaneBurns(t, "utxoset").Since(before))

	releaseBatch()

	require.Eventually(t, func() bool {
		md, err := s.Get(ctx, tx.TxIDChainHash(), fields.LanePosition)
		return err == nil && md.LanePosition == position
	}, 10*time.Second, 20*time.Millisecond, "the batch wrote the record with the position")

	require.Equal(t, tests.LaneBurns{utxo.LaneBurnOutcomeUnknown + "/block_assembly": 1},
		tests.ReadLaneBurns(t, "utxoset").Since(before), "the batch's answer counts nothing more")
}

// The read-back reads the database directly, so it keeps its deadline even when the store's close
// lock is unavailable: a closing store, waiting on a writer stuck behind a full queue, holds it.
// Reading through the get batcher would wait on that lock past any deadline.
func TestLanePositionBurnUnlessStoredDoesNotWaitOnTheCloseLock(t *testing.T) {
	s, ctx := newUncheckpointedLaneStore(t)
	require.NotNil(t, s.getBatcher, "the test needs the batched get it must avoid")

	missing := tests.BuildMultiWorkload(t, 0xce, 1, 1).Roots[0]
	options := &utxo.CreateOptions{Caller: utxo.CallerValidator}

	old := laneBurnProbeTimeout
	laneBurnProbeTimeout = 200 * time.Millisecond

	t.Cleanup(func() { laneBurnProbeTimeout = old })

	s.closeMu.Lock()

	unlock := sync.OnceFunc(s.closeMu.Unlock)
	t.Cleanup(unlock)

	done := make(chan struct{})

	go func() {
		s.burnUnlessStored(ctx, missing, options, lane.Pack(31, 5), errors.NewStorageError("rerun failed"), nil)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		unlock()
		<-done
		t.Fatal("the read-back waited on the store's close lock")
	}

	unlock()
}

// A chunk round that fails before it re-marks what an earlier round found stored still counts that
// transaction as exists: the transaction is live in this round's status, but known stored.
func TestLanePositionFailedChunkKeepsAnEarlierRoundsExists(t *testing.T) {
	stored, fresh := &multiTx{}, &multiTx{}

	run := &chunkRun{
		its:           []*multiTx{stored, fresh},
		st:            []chunkStatus{chunkLive, chunkLive},
		res:           make([]utxo.SpendAndCreateMultiResult, 2),
		forcedExisted: []bool{true, false},
	}

	run.recordFailedBurns()

	require.Equal(t, utxo.LaneBurnExists, stored.burnReason)
	require.Equal(t, utxo.LaneBurnRolledBack, fresh.burnReason)
}

// A direct Create whose context has already ended fails before it opens a database transaction, so
// its position counts as error, and nothing is written.
func TestLanePositionBurnOfADirectCreateWithAnEndedContext(t *testing.T) {
	s, ctx := newUnbatchedLaneStore(t)
	useBurnSource(t, s, burnPositions(1)...)

	tx := mkTx(t, 1, 5_000)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	before := tests.ReadLaneBurns(t, "utxoset")

	_, err := s.Create(cancelled, tx, 1_000, utxo.WithCaller(utxo.CallerBlockAssembly))
	require.Error(t, err)

	require.Equal(t, tests.LaneBurns{utxo.LaneBurnError + "/block_assembly": 1},
		tests.ReadLaneBurns(t, "utxoset").Since(before))
	require.Equal(t, 0, identityCount(t, s, ctx, tx))
	require.Equal(t, 0, utxoCount(t, s, ctx, tx))
}
