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
	"github.com/jackc/pgx/v5/pgconn"
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
		t.Run("returns", func(t *testing.T) { tests.LanePositionReturns(t, s, "utxoset", utxosetLaneRules, utxosetLaneRows(s)) })
	})

	t.Run("no batchers", func(t *testing.T) {
		s, _ := newUnbatchedReturnStore(t)
		tests.LanePositionReturns(t, s, "utxoset", utxosetLaneRules, utxosetLaneRows(s))
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

// laneSchemaColumnsWant is every lane column PrepareLaneSchema installs, written out here rather
// than read from laneSchemaColumns so a column dropped from that list fails these tests.
// Each entry is table, column and the Postgres data_type: a lane position is a signed 64-bit
// integer (lane 128 and up is negative) and a parent list is eight bytes per parent.
var laneSchemaColumnsWant = [][3]string{
	{"tx_ident", "lane_position", "bigint"},
	{"tx_mined", "lane_position", "bigint"},
	{"preserved_parent", "lane_position", "bigint"},
	{"tx_ident", "parent_lane_positions", "bytea"},
	{"tx_mined", "parent_lane_positions", "bytea"},
	{"preserved_parent", "parent_lane_positions", "bytea"},
	{"utxo", "lane_position", "bigint"},
	{"spend_journal", "lane_position", "bigint"},
}

// columnType is a column's Postgres data_type, or "" when it has none.
func columnType(t *testing.T, s *Store, ctx context.Context, table, column string) string {
	t.Helper()

	var typ string

	err := s.pool.QueryRow(ctx, `SELECT data_type FROM information_schema.columns
	                              WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2`, table, column).Scan(&typ)
	if err != nil {
		return ""
	}

	return typ
}

// Opening the store with lane positions on adds only the record's own lane_position, on the three
// identity tables; the other five lane columns, which nothing in this build writes, wait for
// PrepareLaneSchema, so rows do not carry a null bitmap for columns that stay empty.
func TestLanePositionsOnAddsOnlyTheOwnPositionColumns(t *testing.T) {
	s, ctx := newTestStore(t)
	on := reopenWithFlag(t, s, true)

	for _, table := range []string{"tx_ident", "tx_mined", "preserved_parent"} {
		require.True(t, hasColumn(t, on, ctx, table, "lane_position"), table)
	}

	for _, c := range laneSchemaColumnsWant[3:] {
		require.False(t, hasColumn(t, on, ctx, c[0], c[1]), "%s.%s arrives only with PrepareLaneSchema", c[0], c[1])
	}

	installed, err := on.LaneSchemaInstalled(ctx)
	require.NoError(t, err)
	require.False(t, installed)

	require.NoError(t, on.PrepareLaneSchema(ctx))

	for _, c := range laneSchemaColumnsWant {
		require.True(t, hasColumn(t, on, ctx, c[0], c[1]), "%s.%s", c[0], c[1])
		require.Equal(t, c[2], columnType(t, on, ctx, c[0], c[1]), "%s.%s type", c[0], c[1])
	}

	installed, err = on.LaneSchemaInstalled(ctx)
	require.NoError(t, err)
	require.True(t, installed)

	// Any one column missing, the last in the list included, is not installed.
	_, err = on.pool.Exec(ctx, `ALTER TABLE spend_journal DROP COLUMN lane_position`)
	require.NoError(t, err)

	installed, err = on.LaneSchemaInstalled(ctx)
	require.NoError(t, err)
	require.False(t, installed)

	// A catalog query that fails is reported, not read as "not installed".
	cancelled, cancel := context.WithCancel(ctx)
	cancel()

	_, err = on.LaneSchemaInstalled(cancelled)
	require.Error(t, err)
}

// PrepareLaneSchema keeps retrying a lock timeout for its limit: a transaction that holds the
// table for a second delays it and does not fail it.
func TestPrepareLaneSchemaWaitsOutAShortLock(t *testing.T) {
	s, ctx := newTestStore(t)

	blocker, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	var n int64
	require.NoError(t, blocker.QueryRow(ctx, `SELECT count(*) FROM spend_journal`).Scan(&n))

	// Only this goroutine touches the blocking transaction from here on, and the test waits for it.
	released := make(chan struct{})

	go func() {
		defer close(released)

		time.Sleep(time.Second)

		_ = blocker.Rollback(ctx)
	}()

	err = s.PrepareLaneSchema(ctx)
	<-released

	require.NoError(t, err)
	require.True(t, hasColumn(t, s, ctx, "spend_journal", "lane_position"))
}

// The limit is one deadline for the pass, not one per attempt: with the table held well past the
// limit, the add gives up soon after the limit instead of retrying until the holder lets go.
func TestLaneColumnAddGivesUpAtItsLimitAcrossRetries(t *testing.T) {
	s, ctx := newTestStore(t)

	blocker, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = blocker.Rollback(ctx) }()

	var n int64
	require.NoError(t, blocker.QueryRow(ctx, `SELECT count(*) FROM tx_ident`).Scan(&n))

	// The context ends long after the limit, so an add that kept renewing its deadline stops only
	// there, and too late.
	addCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()

	started := time.Now()
	err = addLanePositionColumns(addCtx, s.pool, 1500*time.Millisecond)
	require.Error(t, err, "the table is still held at the limit")
	require.Less(t, time.Since(started), 8*time.Second)
}

// The deadline is shared by every column of a pass. The first column's table is held for two of
// the three seconds and then released; the second's is held throughout. The pass gives up on the
// second column at the shared deadline, about 3.6 s in. A fresh deadline per column would keep
// retrying the second until about 7 s.
func TestLaneColumnPassSharesOneDeadline(t *testing.T) {
	s, ctx := newTestStore(t)

	holdMined, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	defer func() { _ = holdMined.Rollback(ctx) }()

	var n int64
	require.NoError(t, holdMined.QueryRow(ctx, `SELECT count(*) FROM tx_mined`).Scan(&n))

	holdIdent, err := s.pool.Begin(ctx)
	require.NoError(t, err)

	require.NoError(t, holdIdent.QueryRow(ctx, `SELECT count(*) FROM tx_ident`).Scan(&n))

	// One release, from whichever goroutine gets there first: the timer, or the test ending.
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { _ = holdIdent.Rollback(context.Background()) }) }

	timer := time.AfterFunc(2*time.Second, release)
	defer func() {
		timer.Stop()
		release()
	}()

	addCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	started := time.Now()
	err = addLanePositionColumns(addCtx, s.pool, 3*time.Second)
	elapsed := time.Since(started)

	require.ErrorContains(t, err, "tx_mined", "the second column is still held")
	require.True(t, hasLaneColumn(t, s, ctx, "tx_ident"), "the first column went in once its table was free")
	require.Less(t, elapsed, 5500*time.Millisecond, "one deadline for the pass, not one per column")
}

// hasColumn reports whether table has column, nullable with no default.
func hasColumn(t *testing.T, s *Store, ctx context.Context, table, column string) bool {
	t.Helper()

	var found bool

	require.NoError(t, s.pool.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM information_schema.columns
		                WHERE table_schema = current_schema() AND table_name = $1
		                  AND column_name = $2 AND is_nullable = 'YES' AND column_default IS NULL)`, table, column).Scan(&found))

	return found
}

// The lane schema is installed at off, on a database holding outputs, journal rows and mined
// records, before any historical numbering. It rewrites no row, every attached partition gets the
// columns, and every disabled writer keeps working against it: create, spend, mine, the journal
// copy-forward and drop, Unspend and lookup, in the process that installed it and after a restart.
func TestLaneSchemaInstalledAtOffKeepsDisabledWritersWorking(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 3, 5_000)
	_, err := s.Create(ctx, parent, 1_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1_000, OnLongestChain: true}))
	require.NoError(t, err)

	waiting := spendOneOutput(t, s, ctx, parent, 0, 1_010)
	spendOneOutputInBlock(t, s, ctx, parent, 1, 1_010, 2)

	installed, err := s.LaneSchemaInstalled(ctx)
	require.NoError(t, err)
	require.False(t, installed, "off adds nothing on open")

	var before string
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT string_agg(encode(ukey::text::bytea, 'hex') || satoshis::text, ',' ORDER BY ukey) FROM utxo`).Scan(&before))

	require.NoError(t, s.PrepareLaneSchema(ctx))
	require.NoError(t, s.PrepareLaneSchema(ctx), "a second run is a no-op")

	installed, err = s.LaneSchemaInstalled(ctx)
	require.NoError(t, err)
	require.True(t, installed)

	for _, c := range laneSchemaColumnsWant {
		require.True(t, hasColumn(t, s, ctx, c[0], c[1]), "%s.%s", c[0], c[1])
	}

	for _, leaf := range []string{"utxo_p0", "utxo_p7", "tx_ident_l0"} {
		require.True(t, hasColumn(t, s, ctx, leaf, "lane_position"), "partition %s", leaf)
	}

	var journalLeaves []string

	rows, err := s.pool.Query(ctx, `SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = 'spend_journal'::regclass`)
	require.NoError(t, err)

	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))

		journalLeaves = append(journalLeaves, name)
	}

	rows.Close()
	require.NotEmpty(t, journalLeaves)

	for _, leaf := range journalLeaves {
		require.True(t, hasColumn(t, s, ctx, leaf, "lane_position"), "journal partition %s", leaf)
	}

	// No row was rewritten or given a value.
	var after string
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT string_agg(encode(ukey::text::bytea, 'hex') || satoshis::text, ',' ORDER BY ukey) FROM utxo`).Scan(&after))
	require.Equal(t, before, after)

	for _, c := range laneSchemaColumnsWant {
		var n int
		require.NoError(t, s.pool.QueryRow(ctx, `SELECT count(*) FROM `+c[0]+` WHERE `+c[1]+` IS NOT NULL`).Scan(&n))
		require.Zero(t, n, "%s.%s holds no value", c[0], c[1])
	}

	// The process that installed the schema still runs main's statements, and a restart at off
	// sees the columns and carries on.
	for name, store := range map[string]*Store{"installing process": s, "restarted at off": reopenWithFlag(t, s, false)} {
		fresh := mkTx(t, 2, 6_000+uint64(len(name)))
		_, err = store.Create(ctx, fresh, 1_020)
		require.NoError(t, err, name)

		spender := spendOneOutput(t, store, ctx, fresh, 0, 1_021)

		_, err = store.SetMinedMulti(ctx, hashes(fresh), utxo.MinedBlockInfo{BlockID: 3, BlockHeight: 1_022, OnLongestChain: true})
		require.NoError(t, err, name)

		spends, err := utxo.GetSpends(spender)
		require.NoError(t, err)
		require.NoError(t, store.Unspend(ctx, spends), name)
		require.Equal(t, 2, utxoCount(t, store, ctx, fresh), "%s: Unspend restores from the journal", name)

		_, err = store.Get(ctx, fresh.TxIDChainHash())
		require.NoError(t, err, name)
	}

	// The journal partition holding the waiting spend is retired: its row is copied forward into
	// a live partition and survives with the new column present.
	require.NoError(t, s.SetBlockHeight(1_728))

	dropped, err := s.dropSpendJournalPartitionsBelow(ctx, 1_440)
	require.NoError(t, err)
	require.Positive(t, dropped)
	require.Equal(t, 1, journalRowsFor(t, s, ctx, parent, 0), "the waiting spend is carried forward")

	spends, err := utxo.GetSpends(waiting)
	require.NoError(t, err)
	require.NoError(t, s.Unspend(ctx, spends))
	require.Equal(t, 2, utxoCount(t, s, ctx, parent), "the carried row rebuilds the coin")
}

// utxosetLaneRules: a refused claim goes back only for a create with no block, since one with a
// block still writes a containment row; a list's netted write returns nothing.
var utxosetLaneRules = tests.LaneReturnRules{Exists: true}

// utxosetLaneRows counts the rows of every table with a lane_position column that carry any of
// positions.
func utxosetLaneRows(s *Store) tests.LanePositionCounter {
	return func(t testing.TB, positions []uint64) int {
		t.Helper()

		signed := make([]int64, len(positions))
		for i, p := range positions {
			signed[i] = lane.ToSigned(p)
		}

		total := 0

		for _, table := range laneColumnTables {
			var n int
			require.NoError(t, s.pool.QueryRow(context.Background(),
				`SELECT count(*) FROM `+table+` WHERE lane_position = ANY($1)`, signed).Scan(&n))

			total += n
		}

		return total
	}
}

// newUnbatchedReturnStore is a lane store with no create or spend-and-create batcher, so every
// create takes the single-write path.
func newUnbatchedReturnStore(t *testing.T) (*Store, context.Context) {
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

// useReturningSource gives s a source that records the positions it hands back.
func useReturningSource(t *testing.T, s *Store, positions ...uint64) *tests.ReturningLanePositions {
	t.Helper()

	source := tests.NewReturningLanePositions(positions...)
	s.SetLanePositionSource(source)
	t.Cleanup(func() { s.SetLanePositionSource(nil) })

	return source
}

// returnPositions makes n positions in lane 30.
func returnPositions(n int) []uint64 {
	positions := make([]uint64, n)
	for i := range positions {
		positions[i] = lane.Pack(30, uint64(i)+1) //nolint:gosec // i counts up from 0
	}

	return positions
}

// requireReturnedNowhere requires source to have taken back exactly want, and no row of any table
// with a lane_position column to carry what it took back.
func requireReturnedNowhere(t *testing.T, s *Store, source *tests.ReturningLanePositions, want ...uint64) {
	t.Helper()

	returned := source.Returned()

	if len(want) == 0 {
		require.Empty(t, returned, "nothing goes back")
		return
	}

	require.ElementsMatch(t, want, returned)
	require.Zero(t, utxosetLaneRows(s)(t, returned), "a row carries a returned position")
}

// The returns only utxoset makes or refuses, which the shared contract cannot reach.
func TestLanePositionReturnsOfAUtxosetStore(t *testing.T) {
	failure := errors.NewProcessingError("injected failure")

	// A batch that fails before its COMMIT is sent rolls back whole, so its positions go back.
	t.Run("a create batch that rolled back", func(t *testing.T) {
		s, ctx := newUncheckpointedLaneStore(t)
		require.NotNil(t, s.createBatcher)

		source := useReturningSource(t, s, returnPositions(1)...)

		createBatchBeforeCommit = func() error { return failure }
		t.Cleanup(func() { createBatchBeforeCommit = nil })

		_, err := s.Create(ctx, mkTx(t, 1, 5_000), 1_000)
		require.Error(t, err)

		requireReturnedNowhere(t, s, source, returnPositions(1)...)
	})

	// A list chunk that fails before its COMMIT is sent rolls back whole, so its positions go back.
	// The chunk before it committed, and the chunk after it never started: neither returns.
	t.Run("a list chunk that rolled back, and not the chunk that never ran", func(t *testing.T) {
		s, ctx := newUncheckpointedLaneStore(t)
		forceChunkOfOne(t)

		root := storedParent(t, s, ctx)
		parent := spendOutput(t, root, 0, 2)
		child := spendOutput(t, parent, 0, 3)
		grandchild := spendOutput(t, child, 0, 1)

		// Chunk 0 holds parent and commits, chunk 1 holds child and rolls back, chunk 2 holds
		// grandchild, waits for chunk 1, and never runs.
		multiChunkFault = func(chunk int, stage string) error {
			if chunk == 1 && stage == "spent" {
				return failure
			}

			return nil
		}

		t.Cleanup(func() { multiChunkFault = nil })

		source := useReturningSource(t, s, returnPositions(3)...)

		_, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{parent, child, grandchild}, 1_100, utxo.WithIgnoreLocked(true))
		multiChunkFault = nil
		require.ErrorIs(t, err, failure)

		requireReturnedNowhere(t, s, source, returnPositions(3)[1])
		requireGetLane(t, s, ctx, parent, returnPositions(3)[0], "the committed chunk keeps its position")
	})

	// The same below the checkpoint, through the netted below-checkpoint write. One worker runs the
	// chunks in list order, so the first commits before the second fails.
	t.Run("a below-checkpoint chunk that rolled back", func(t *testing.T) {
		const height = 900

		s, ctx := newTestStoreWith(t, func(ts *settings.Settings) {
			ts.UtxoStore.LaneMigration = settings.LaneMigrationShadow
			ts.UtxoStore.NettedBelowWorkers = 1
		})
		require.Equal(t, 1, s.nettedBelowWorkers())
		withNettedBelowChunkTxs(t, 1)

		w := tests.BuildMultiWorkload(t, 0xbe, 1, 3)
		w.StoreRoots(t, s, height-1)

		list := outpointOnly(w.Txs)

		nettedBelowFault = func(chunk []int) error {
			if len(chunk) == 1 && chunk[0] == 1 {
				return failure
			}

			return nil
		}

		t.Cleanup(func() { nettedBelowFault = nil })

		source := useReturningSource(t, s, returnPositions(3)...)

		_, err := s.SpendAndCreateMulti(ctx, list, height, belowCheckpointOptions(height)...)
		nettedBelowFault = nil
		require.ErrorIs(t, err, failure)

		requireReturnedNowhere(t, s, source, returnPositions(3)[1])
		requireGetLane(t, s, ctx, list[0], returnPositions(3)[0], "the committed chunk keeps its position")
	})

	// A chunk that deadlocks rolls back and runs again with the same positions. When the second
	// attempt commits, the transaction carries its position, though the first attempt failed before
	// its COMMIT: nothing goes back.
	t.Run("a list chunk that deadlocked and then committed", func(t *testing.T) {
		s, ctx := newUncheckpointedLaneStore(t)

		root := storedParent(t, s, ctx)
		parent := spendOutput(t, root, 0, 2)

		deadlocked := false
		multiChunkFault = func(_ int, stage string) error {
			if stage == "spent" && !deadlocked {
				deadlocked = true

				return &pgconn.PgError{Code: "40P01", Message: "deadlock detected"}
			}

			return nil
		}

		t.Cleanup(func() { multiChunkFault = nil })

		source := useReturningSource(t, s, returnPositions(1)...)

		results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{parent}, 1_100, utxo.WithIgnoreLocked(true))
		multiChunkFault = nil
		require.NoError(t, err)
		require.True(t, deadlocked)
		require.Equal(t, utxo.MultiTxCreated, results[0].Status)

		requireReturnedNowhere(t, s, source)
		requireGetLane(t, s, ctx, parent, returnPositions(1)[0], "the second attempt wrote the position")
	})

	// A list's refused spend and its parent-failed child were not written, but they share their
	// round's writes, which this does not prove unwritten: both stay burned.
	t.Run("a list's refused spend and its child stay burned", func(t *testing.T) {
		s, ctx := newUncheckpointedLaneStore(t)

		root := storedParent(t, s, ctx)

		_, _, err := s.SpendAndCreate(ctx, spendOutput(t, root, 0, 4), 1_100)
		require.NoError(t, err)

		parent := spendOutput(t, root, 0, 2)
		child := spendOutput(t, parent, 0, 1)

		source := useReturningSource(t, s, returnPositions(2)...)

		results, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{parent, child}, 1_100, utxo.WithIgnoreLocked(true))
		require.NoError(t, err)
		require.Equal(t, utxo.MultiTxFailed, results[0].Status)
		require.Equal(t, utxo.MultiTxParentFailed, results[1].Status)

		requireReturnedNowhere(t, s, source)
	})

	// A chunk whose COMMIT was sent may have landed, so a failure after it returns nothing.
	t.Run("a list chunk that failed after its COMMIT was sent", func(t *testing.T) {
		s, ctx := newUncheckpointedLaneStore(t)
		forceChunkOfOne(t)

		root := storedParent(t, s, ctx)
		parent := spendOutput(t, root, 0, 2)

		multiCreateAfterCommit = func() error { return failure }
		t.Cleanup(func() { multiCreateAfterCommit = nil })

		source := useReturningSource(t, s, returnPositions(1)...)

		_, err := s.SpendAndCreateMulti(ctx, []*bt.Tx{parent}, 1_100, utxo.WithIgnoreLocked(true))
		multiCreateAfterCommit = nil
		require.ErrorIs(t, err, failure)

		requireReturnedNowhere(t, s, source)
		requireGetLane(t, s, ctx, parent, returnPositions(1)[0], "the commit landed with the position")
	})

	// A create batch whose COMMIT reported failure may have landed, and here it did: the record carries
	// the position, so it must not go back.
	t.Run("a create batch whose commit reported failure", func(t *testing.T) {
		s, ctx := newUncheckpointedLaneStore(t)
		require.NotNil(t, s.createBatcher)

		source := useReturningSource(t, s, returnPositions(1)...)

		createBatchAfterCommit = func() error { return failure }
		t.Cleanup(func() { createBatchAfterCommit = nil })

		tx := mkTx(t, 1, 5_000)

		_, err := s.Create(ctx, tx, 1_000)
		createBatchAfterCommit = nil
		require.Error(t, err)

		requireReturnedNowhere(t, s, source)
		requireGetLane(t, s, ctx, tx, returnPositions(1)[0], "the commit landed with the position")
	})

	// The batch's COMMIT landed and reported failure, so the item reruns alone and meets its own
	// record. That ErrTxExists is the item's own write and must not send the position back.
	t.Run("a rerun that meets the record its batch wrote", func(t *testing.T) {
		s, ctx := newUncheckpointedLaneStore(t)
		require.NotNil(t, s.spendAndCreateBatcher)

		root := tests.BuildMultiWorkload(t, 0xbf, 1, 1).Roots[0]
		source := useReturningSource(t, s, returnPositions(1)...)

		failed := false
		spendAndCreateBatchAfterCommit = func() error {
			if failed {
				return nil
			}

			failed = true

			return failure
		}

		t.Cleanup(func() { spendAndCreateBatchAfterCommit = nil })

		_, _, err := s.SpendAndCreate(ctx, root, 1_000, utxo.WithCreateOnly())
		require.ErrorIs(t, err, errors.ErrTxExists)
		require.True(t, failed)

		requireReturnedNowhere(t, s, source)
		requireGetLane(t, s, ctx, root, returnPositions(1)[0], "the record carries the position")
	})
}

// laneLeases is a lane.LeaseSource with one lane and no other holder, enough for a real allocator.
type laneLeases struct {
	mu   sync.Mutex
	high uint64
}

func (l *laneLeases) grant() lane.Grant {
	g := lane.Grant{Lane: 5, First: l.high + 1, Last: l.high + 1_000}
	l.high = g.Last

	return g
}

func (l *laneLeases) OpenLaneLease(context.Context, string) (lane.LeaseID, lane.Grant, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return lane.LeaseID{1}, l.grant(), nil
}

func (l *laneLeases) RenewLaneLease(_ context.Context, r lane.Renewal) (lane.RenewResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !r.WantGrant {
		return lane.RenewResult{}, nil
	}

	return lane.RenewResult{Granted: true, Grant: l.grant()}, nil
}

func (l *laneLeases) ReleaseLaneLease(context.Context, lane.Release) (bool, error) {
	return false, nil
}

// With the real allocator, a validator's second create of a stored transaction sends its position
// back, and the next create writes it, so the lane keeps its run with no hole.
func TestARefusedCreatesPositionIsWrittenByTheNextCreate(t *testing.T) {
	s, ctx := newUncheckpointedLaneStore(t)

	allocator := lane.NewAllocator(&laneLeases{}, "reuse-test", lane.Timings{Renew: time.Hour, Stop: 2 * time.Hour, GrantDuration: time.Hour}, nil)
	t.Cleanup(func() { _ = allocator.Close() })

	s.SetLanePositionSource(allocator)
	t.Cleanup(func() { s.SetLanePositionSource(nil) })

	w := tests.BuildMultiWorkload(t, 0xb6, 1, 2)

	_, _, err := s.SpendAndCreate(ctx, w.Roots[0], 1_000, utxo.WithCreateOnly())
	require.NoError(t, err)

	_, _, err = s.SpendAndCreate(ctx, w.Roots[0], 1_000, utxo.WithCreateOnly())
	require.ErrorIs(t, err, errors.ErrTxExists)

	_, _, err = s.SpendAndCreate(ctx, w.Roots[1], 1_000, utxo.WithCreateOnly())
	require.NoError(t, err)

	requireGetLane(t, s, ctx, w.Roots[0], lane.Pack(5, 1), "the first create")
	requireGetLane(t, s, ctx, w.Roots[1], lane.Pack(5, 2), "the refused create's position, written by the next")
}
