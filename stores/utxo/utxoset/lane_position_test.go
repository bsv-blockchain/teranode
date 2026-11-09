package utxoset

import (
	"context"
	"net/url"
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

	return newTestStoreWith(t, func(ts *settings.Settings) { ts.UtxoStore.LanePositions = true })
}

func newUncheckpointedLaneStore(t *testing.T) (*Store, context.Context) {
	t.Helper()

	return newTestStoreWith(t, func(ts *settings.Settings) {
		withCheckpoints(ts, nil)
		ts.UtxoStore.LanePositions = true
	})
}

func newCheckpointLaneStore(t *testing.T) (*Store, context.Context) {
	t.Helper()

	return newTestStoreWith(t, func(ts *settings.Settings) {
		params := *ts.ChainCfgParams
		params.Checkpoints = []chaincfg.Checkpoint{{Height: checkpointFloor}}
		ts.ChainCfgParams = &params
		ts.UtxoStore.LanePositions = true
	})
}

func newBodySkipLaneStore(t *testing.T) *Store {
	t.Helper()

	s, _ := newTestStoreWith(t, func(ts *settings.Settings) {
		ts.UtxoStore.SkipTxBodyBelowCheckpoint = true
		ts.UtxoStore.LanePositions = true
	})

	return s
}

// reopenWithFlag opens the same database again, as a restart with the flag set to on would.
func reopenWithFlag(t *testing.T, s *Store, on bool) *Store {
	t.Helper()

	ts := *s.settings
	ts.UtxoStore.LanePositions = on

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

// TestABlockPathCreateWithNoRangeInHandKeepsTheRecordsPosition: a database with the column and a source
// with no range in hand takes no position for anything in the plan. The containment row must still
// copy the position the record has, or the stored value would depend on whether a neighbour in the
// batch got one, and Get would lose the position once the stamp deleted the identity row.
func TestABlockPathCreateWithNoRangeInHandKeepsTheRecordsPosition(t *testing.T) {
	want := lane.Pack(4, 100)

	for _, route := range blockPathCreateRoutes {
		t.Run(route.name, func(t *testing.T) {
			s, ctx := route.store(t)
			s.SetLanePositionSource(tests.NewScriptedLanePositions())

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
		s.SetLanePositionSource(tests.NewScriptedLanePositions())

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
		ts.UtxoStore.LanePositions = true
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

// primeOrderSource records what a transaction would have held when Prime ran: the connections the
// pool had out, which on this otherwise idle store is the open transaction's. It also fails a Next
// that comes before Prime, which is a position taken with the wait still to come.
type primeOrderSource struct {
	pool                                   *pgxpool.Pool
	primes, nextsBeforePrime, connsAtPrime atomic.Int64
}

func (p *primeOrderSource) Prime() {
	p.primes.Add(1)
	p.connsAtPrime.Add(int64(p.pool.Stat().AcquiredConns()))
}

func (p *primeOrderSource) Next() (uint64, bool) {
	if p.primes.Load() == 0 {
		p.nextsBeforePrime.Add(1)
	}

	return lane.None, false
}

// TestUtxosetPrimesBeforeAnyTransaction: the first-range wait must not run with a Postgres transaction
// and its locks held, so each entry point that creates primes first. A spend-only call does not,
// because it takes no position and must not start the allocator.
func TestUtxosetPrimesBeforeAnyTransaction(t *testing.T) {
	ctx := t.Context()

	entry := func(t *testing.T, call func(s *Store, w *tests.MultiWorkload) error) *primeOrderSource {
		t.Helper()

		s, _ := newUncheckpointedLaneStore(t)
		w := tests.BuildMultiWorkload(t, 0xf4, 1, 1)
		w.StoreRoots(t, s, 900)

		source := &primeOrderSource{pool: s.pool}
		s.SetLanePositionSource(source)
		require.NoError(t, call(s, w))

		return source
	}

	t.Run("Create", func(t *testing.T) {
		source := entry(t, func(s *Store, w *tests.MultiWorkload) error {
			_, err := s.Create(ctx, w.Txs[0], 901)
			return err
		})
		require.NotZero(t, source.primes.Load())
		require.Zero(t, source.nextsBeforePrime.Load())
		require.Zero(t, source.connsAtPrime.Load(), "the wait ran with a connection held")
	})

	t.Run("SpendAndCreate", func(t *testing.T) {
		source := entry(t, func(s *Store, w *tests.MultiWorkload) error {
			_, _, err := s.SpendAndCreate(ctx, w.Txs[0], 901)
			return err
		})
		require.NotZero(t, source.primes.Load())
		require.Zero(t, source.nextsBeforePrime.Load())
		require.Zero(t, source.connsAtPrime.Load(), "the wait ran with a connection held")
	})

	t.Run("SpendAndCreateMulti", func(t *testing.T) {
		source := entry(t, func(s *Store, w *tests.MultiWorkload) error {
			_, err := s.SpendAndCreateMulti(ctx, w.Txs, 901, utxo.WithIgnoreLocked(true))
			return err
		})
		require.NotZero(t, source.primes.Load())
		require.Zero(t, source.nextsBeforePrime.Load())
		require.Zero(t, source.connsAtPrime.Load(), "the wait ran with a connection held")
	})

	t.Run("a spend-only call does not prime", func(t *testing.T) {
		source := entry(t, func(s *Store, w *tests.MultiWorkload) error {
			_, _, err := s.SpendAndCreate(ctx, w.Txs[0], 901, utxo.WithSpendOnly())
			return err
		})
		require.Zero(t, source.primes.Load())
	})
}

// TestLanePositionOnTheNettedBelowCheckpointWrite is the route quick validation and legacy sync take
// below the checkpoint. It is its own statement, so the contract's below-the-checkpoint case, which
// does not set the options that reach it, proves nothing about it. The source runs out partway through
// the list, so one record goes without a position beside the others that have theirs.
func TestLanePositionOnTheNettedBelowCheckpointWrite(t *testing.T) {
	s := newBodySkipLaneStore(t)
	ctx := t.Context()

	const height = 831

	w := tests.BuildMultiWorkload(t, 0xd9, 1, 1)
	w.StoreRoots(t, s, height-1)

	list := outpointOnly(nettedFamily(t, w))
	require.Len(t, list, 4)

	given := []uint64{lane.Pack(200, 1), lane.Pack(200, 2), lane.MaxPosition}
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

		if md.LanePosition == lane.None {
			_, null, ok := storedLane(t, s, ctx, "tx_mined", tx)
			require.True(t, ok)
			require.True(t, null, "a record with no position stores NULL, not 0")
		}
	}

	require.ElementsMatch(t, append(given, lane.None), got, "each record its own position, one with none")
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
