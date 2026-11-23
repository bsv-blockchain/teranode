package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/test/utils/postgres"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// laneMetricValue sums the samples of a teranode lane metric: a counter's value, or a
// histogram's observation count.
func laneMetricValue(t *testing.T, name string) float64 {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	var sum float64

	for _, f := range families {
		if f.GetName() != name {
			continue
		}

		for _, m := range f.GetMetric() {
			switch {
			case m.GetCounter() != nil:
				sum += m.GetCounter().GetValue()
			case m.GetHistogram() != nil:
				sum += float64(m.GetHistogram().GetSampleCount())
			}
		}
	}

	return sum
}

// laneGateInsert is the insert stage's observation count and its summed seconds, over every block
// label.
func laneGateInsert(t *testing.T) (uint64, float64) {
	t.Helper()

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	var (
		count uint64
		sum   float64
	)

	for _, f := range families {
		if f.GetName() != "teranode_lane_gate_seconds" {
			continue
		}

		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "stage" && l.GetValue() == lane.GateStageInsert {
					count += m.GetHistogram().GetSampleCount()
					sum += m.GetHistogram().GetSampleSum()
				}
			}
		}
	}

	return count, sum
}

// blockLaneRows is a block's rows: lane 7 taken whole from 1 to 90,000 and lane 200 with holes.
func blockLaneRows(t *testing.T) []lane.MapRow {
	t.Helper()

	full := roaring64.New()
	full.AddRange(1, 90_001)

	lane7, err := lane.BuildMapRows(7, full)
	require.NoError(t, err)

	return append(lane7, holedRows(t, 200, 5_000, 5_001, 5_003, 7_000)...)
}

func laneMapRowCount(t *testing.T, s *SQL) int {
	t.Helper()

	var n int
	require.NoError(t, s.db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM lane_map`).Scan(&n))

	return n
}

func requireRowsOf(t *testing.T, s *SQL, blockID uint64, want []lane.MapRow) {
	t.Helper()

	got, err := s.LaneMapRows(context.Background(), uint32(blockID)) //nolint:gosec // a test block ID
	require.NoError(t, err)
	require.Len(t, got, len(want))

	for i := range want {
		same, err := lane.SameRow(want[i], got[i])
		require.NoError(t, err)
		require.True(t, same, "row %d", i)
	}
}

func TestStoreBlockCommitsLaneMapRowsWithTheBlock(t *testing.T) {
	runLaneHandoutTests(t, "extend and fork", func(t *testing.T, s *SQL) {
		ctx := t.Context()
		rows := blockLaneRows(t)
		inserts, seconds := laneGateInsert(t)

		// The common extend, which without rows is one auto-committed INSERT.
		id1, _, err := s.StoreBlock(ctx, block1, "", options.WithLaneMapRows(rows))
		require.NoError(t, err)
		requireRowsOf(t, s, id1, rows)

		afterInserts, afterSeconds := laneGateInsert(t)
		require.Equal(t, inserts+1, afterInserts, "the insert stage is timed once")
		require.Greater(t, afterSeconds, seconds, "with the time the check and the insert took")

		// A block stored without rows, as at utxostore_laneMigration off, writes none and is not
		// timed.
		id2, _, err := s.StoreBlock(ctx, block2, "")
		require.NoError(t, err)
		requireRowsOf(t, s, id2, nil)

		stillInserts, _ := laneGateInsert(t)
		require.Equal(t, afterInserts, stillInserts, "a block without rows is not timed")

		// A fork off block1 goes through the fork path's transaction.
		fork := createBlock3OnFork(block1)
		forkRows := holedRows(t, 9, 1, 3)

		idFork, _, err := s.StoreBlock(ctx, fork, "", options.WithLaneMapRows(forkRows))
		require.NoError(t, err)
		require.False(t, getOnMainChain(t, s, fork.Hash().CloneBytes()), "the fork block took the fork path")
		requireRowsOf(t, s, idFork, forkRows)

		require.Equal(t, len(rows)+len(forkRows), laneMapRowCount(t, s))
	})
}

// The common extend's rows go in the block's own transaction, so the block and its rows commit
// together. On Postgres the rows' insert first takes its block's lane write lock; held here from
// another transaction, it stops the store after the block's INSERT and before the commit. Another
// connection then cannot see the block, and sees it with its rows once the lock is released.
func TestStoreBlockCommitsTheBlockAndItsRowsTogether(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping postgres-backed test in -short mode")
	}

	s, cleanup := setupLanePostgresStore(t)
	defer cleanup()

	ctx := t.Context()
	rows := blockLaneRows(t)

	id, err := s.AssignBlockID(ctx, block1.Hash())
	require.NoError(t, err)

	holder, err := s.db.BeginTx(ctx, nil)
	require.NoError(t, err)

	defer func() { _ = holder.Rollback() }()

	_, err = holder.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1, $2)`, laneMapLockClass, int32(id)) //nolint:gosec // a test block ID
	require.NoError(t, err)

	done := make(chan error, 1)

	go func() {
		_, _, storeErr := s.StoreBlock(ctx, block1, "", options.WithID(id), options.WithLaneMapRows(rows))
		done <- storeErr
	}()

	require.Eventually(t, func() bool {
		var waiting int
		require.NoError(t, s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted`).Scan(&waiting))

		return waiting > 0
	}, 10*time.Second, 10*time.Millisecond, "the store waits on the block's lane write lock")

	blockRows := func() int {
		var n int
		require.NoError(t, s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM blocks WHERE hash = $1`, block1.Hash().CloneBytes()).Scan(&n))

		return n
	}

	require.Zero(t, blockRows(), "the block's INSERT is not committed before its rows are in")

	require.NoError(t, holder.Commit())
	require.NoError(t, <-done)

	require.Equal(t, 1, blockRows())
	requireRowsOf(t, s, id, rows)
}

func TestStoreBlockWritesLaneMapRowsOnlyWithItsBlock(t *testing.T) {
	runLaneHandoutTests(t, "only with the block", func(t *testing.T, s *SQL) {
		ctx := t.Context()
		rows := blockLaneRows(t)

		id1, _, err := s.StoreBlock(ctx, block1, "", options.WithLaneMapRows(rows))
		require.NoError(t, err)

		// A retry of the same block fails as it always did and writes nothing new, with the same
		// rows or with others.
		_, _, err = s.StoreBlock(ctx, block1, "", options.WithLaneMapRows(rows))
		require.Error(t, err)

		_, _, err = s.StoreBlock(ctx, block1, "", options.WithLaneMapRows(holedRows(t, 3, 1, 5)))
		require.Error(t, err)

		requireRowsOf(t, s, id1, rows)
		require.Equal(t, len(rows), laneMapRowCount(t, s))

		// A block that is not stored, its parent unknown, leaves no rows anywhere and is not timed.
		orphan := createBlock3OnFork(createBlock3OnFork(block3))
		inserts, _ := laneGateInsert(t)

		_, _, err = s.StoreBlock(ctx, orphan, "", options.WithLaneMapRows(holedRows(t, 4, 1, 5)))
		require.Error(t, err)
		require.Equal(t, len(rows), laneMapRowCount(t, s))

		// A block stored as invalid gets no rows and is not timed.
		idInvalid, _, err := s.StoreBlock(ctx, block2, "", options.WithInvalid(true), options.WithLaneMapRows(holedRows(t, 5, 1, 5)))
		require.NoError(t, err)
		requireRowsOf(t, s, idInvalid, nil)

		after, _ := laneGateInsert(t)
		require.Equal(t, inserts, after, "neither the failed store nor the invalid block is timed")
	})
}

func TestStoreBlockStoresTheBlockWhenItsLaneMapRowsFail(t *testing.T) {
	runLaneHandoutTests(t, "rows fail", func(t *testing.T, s *SQL) {
		ctx := t.Context()

		// Rows that fail their check: the block is stored without them, and that is counted.
		failures := laneMetricValue(t, "teranode_lane_row_insert_failures_total")

		bad := []lane.MapRow{{Lane: 1, First: 5, Last: 4, Tag: lane.RowTagRange}}

		id1, _, err := s.StoreBlock(ctx, block1, "", options.WithLaneMapRows(bad))
		require.NoError(t, err)
		requireRowsOf(t, s, id1, nil)
		require.Equal(t, failures+1, laneMetricValue(t, "teranode_lane_row_insert_failures_total"))

		// Rows whose insert fails inside the block's transaction, because other rows are already
		// stored for the block ID the block is given: the insert is rolled back to its savepoint,
		// the block commits, and on Postgres the transaction is not left aborted.
		reusedID, err := s.AssignBlockID(ctx, block2.Hash())
		require.NoError(t, err)

		earlier := holedRows(t, 8, 1, 4)
		require.NoError(t, s.InsertLaneMapRows(ctx, uint32(reusedID), earlier)) //nolint:gosec // a test block ID

		inserts, _ := laneGateInsert(t)

		id2, _, err := s.StoreBlock(ctx, block2, "", options.WithID(reusedID), options.WithLaneMapRows(blockLaneRows(t)))
		require.NoError(t, err)
		require.Equal(t, reusedID, id2)

		afterInserts, _ := laneGateInsert(t)
		require.Equal(t, inserts, afterInserts, "rows rolled back to their savepoint are not timed")
		requireRowsOf(t, s, id2, earlier)
		require.Equal(t, failures+2, laneMetricValue(t, "teranode_lane_row_insert_failures_total"))

		stored, err := s.GetBlockExists(ctx, block2.Hash())
		require.NoError(t, err)
		require.True(t, stored)

		// The same on the fork path.
		fork := createBlock3OnFork(block1)

		forkID, err := s.AssignBlockID(ctx, fork.Hash())
		require.NoError(t, err)
		require.NoError(t, s.InsertLaneMapRows(ctx, uint32(forkID), earlier)) //nolint:gosec // a test block ID

		_, _, err = s.StoreBlock(ctx, fork, "", options.WithID(forkID), options.WithLaneMapRows(blockLaneRows(t)))
		require.NoError(t, err)
		require.False(t, getOnMainChain(t, s, fork.Hash().CloneBytes()))
		requireRowsOf(t, s, forkID, earlier)
		require.Equal(t, failures+3, laneMetricValue(t, "teranode_lane_row_insert_failures_total"))

		// A row statement that itself fails, here because lane_map is gone: Postgres aborts the
		// transaction on it, and only the rollback to the savepoint lets the block commit.
		_, err = s.db.ExecContext(ctx, `ALTER TABLE lane_map RENAME TO lane_map_away`)
		require.NoError(t, err)

		next := createBlock3OnFork(block2)

		_, _, err = s.StoreBlock(ctx, next, "", options.WithLaneMapRows(blockLaneRows(t)))
		require.NoError(t, err, "the block stores although its rows' statement failed")

		_, err = s.db.ExecContext(ctx, `ALTER TABLE lane_map_away RENAME TO lane_map`)
		require.NoError(t, err)

		stored, err = s.GetBlockExists(ctx, next.Hash())
		require.NoError(t, err)
		require.True(t, stored)
		require.Equal(t, failures+4, laneMetricValue(t, "teranode_lane_row_insert_failures_total"))
	})
}

// BenchmarkStoreBlockLaneMapRows is the insert stage of the gate on a large block: StoreBlock of a
// block extending the tip, with the rows of mainnet's backlog shape at 17 lanes (about 3.5
// million members, 2.4 MB of hole data), against the same StoreBlock without rows. The build
// stage is BenchmarkLaneGateBuild in util/lane.
func BenchmarkStoreBlockLaneMapRows(b *testing.B) {
	rows := benchmarkGateRows(b)

	for _, engine := range []string{"sqlite", "postgres"} {
		b.Run(engine, func(b *testing.B) {
			for _, withRows := range []bool{false, true} {
				name := "without_rows"
				if withRows {
					name = "with_rows"
				}

				b.Run(name, func(b *testing.B) {
					s := benchmarkStore(b, engine)
					parent := block1

					_, _, err := s.StoreBlock(context.Background(), parent, "")
					require.NoError(b, err)

					for b.Loop() {
						b.StopTimer()
						next := createBlock3OnFork(parent)
						b.StartTimer()

						var opts []options.StoreBlockOption
						if withRows {
							opts = append(opts, options.WithLaneMapRows(rows))
						}

						_, _, err := s.StoreBlock(context.Background(), next, "", opts...)
						require.NoError(b, err)

						parent = next
					}
				})
			}
		})
	}
}

// benchmarkGateRows builds the rows of the backlog shape BenchmarkLaneGateBuild uses: per lane,
// 9,000-position stretches over 1.435 million positions, a fifth of them taken at a density of 0.7.
func benchmarkGateRows(b *testing.B) []lane.MapRow {
	b.Helper()

	var all []lane.MapRow

	seed := uint64(1)

	for l := uint8(0); l < 17; l++ {
		bm := roaring64.New()

		for start := uint64(1); start <= 1_435_000; start += 9_000 {
			seed = seed*6364136223846793005 + 1442695040888963407
			if seed>>32%5 != 0 {
				continue
			}

			for p := start; p < start+9_000; p++ {
				seed = seed*6364136223846793005 + 1442695040888963407
				if seed>>32%10 < 7 {
					bm.Add(p)
				}
			}
		}

		rows, err := lane.BuildMapRows(l, bm)
		require.NoError(b, err)

		all = append(all, rows...)
	}

	return all
}

func benchmarkStore(b *testing.B, engine string) *SQL {
	b.Helper()

	if engine == "postgres" {
		connStr, teardown, err := postgres.SetupTestPostgresContainer()
		if err != nil {
			b.Skipf("PostgreSQL container not available: %v", err)
		}

		b.Cleanup(func() { _ = teardown() })

		storeURL, err := url.Parse(connStr)
		require.NoError(b, err)

		s, err := New(ulogger.TestLogger{}, storeURL, test.CreateBaseTestSettings(b))
		require.NoError(b, err)

		return s
	}

	return newOnMainChainTestStore(b)
}

// A failure of the savepoint statement itself leaves the transaction unusable, so it is returned,
// which fails the block's store, and it is not counted as rows dropped.
func TestInsertLaneMapRowsWithBlockReturnsASavepointFailureUncounted(t *testing.T) {
	runLaneHandoutTests(t, "savepoint fails", func(t *testing.T, s *SQL) {
		ctx := t.Context()
		failures := laneMetricValue(t, "teranode_lane_row_insert_failures_total")

		tx, err := s.db.BeginTx(ctx, nil)
		require.NoError(t, err)
		require.NoError(t, tx.Rollback()) // every statement on tx now fails, SAVEPOINT first

		rows := &laneRowsToStore{rows: blockLaneRows(t)}

		err = s.insertLaneMapRowsWithBlock(ctx, tx, block1, 5, rows)
		require.Error(t, err)
		require.Equal(t, failures, laneMetricValue(t, "teranode_lane_row_insert_failures_total"))
		requireRowsOf(t, s, 5, nil)
	})
}

// A block ID wider than the map's 32 bits leaves the rows out, and the block's transaction goes on.
// The attempt records it, and it is counted only when StoreBlock reports the attempt that stored
// the block (observe), once however many attempts ran.
func TestInsertLaneMapRowsWithBlockCountsABlockIDTooWide(t *testing.T) {
	runLaneHandoutTests(t, "wide ID", func(t *testing.T, s *SQL) {
		ctx := t.Context()
		failures := laneMetricValue(t, "teranode_lane_row_insert_failures_total")
		inserts, _ := laneGateInsert(t)

		tx, err := s.db.BeginTx(ctx, nil)
		require.NoError(t, err)

		defer func() { _ = tx.Rollback() }()

		rows := &laneRowsToStore{rows: blockLaneRows(t)}

		require.NoError(t, s.insertLaneMapRowsWithBlock(ctx, tx, block1, 1<<32, rows))
		require.NoError(t, s.insertLaneMapRowsWithBlock(ctx, tx, block1, 1<<32, rows), "a retried attempt")
		require.True(t, rows.refused)
		require.False(t, rows.stored)
		require.Equal(t, failures, laneMetricValue(t, "teranode_lane_row_insert_failures_total"), "nothing is counted before the block is stored")

		rows.observe()
		require.Equal(t, failures+1, laneMetricValue(t, "teranode_lane_row_insert_failures_total"), "counted once")

		after, _ := laneGateInsert(t)
		require.Equal(t, inserts, after, "rows left out are not timed")
	})
}

// Each attempt records afresh what it did: an attempt that stores the rows after one that left
// them out reports them stored, timed by its own insert alone.
func TestInsertLaneMapRowsWithBlockRecordsEachAttemptAfresh(t *testing.T) {
	runLaneHandoutTests(t, "attempts", func(t *testing.T, s *SQL) {
		ctx := t.Context()

		tx, err := s.db.BeginTx(ctx, nil)
		require.NoError(t, err)

		defer func() { _ = tx.Rollback() }()

		rows := &laneRowsToStore{rows: blockLaneRows(t)}

		require.NoError(t, s.insertLaneMapRowsWithBlock(ctx, tx, block1, 1<<32, rows))
		require.True(t, rows.refused)

		rows.insert = time.Hour // what an earlier attempt took must not be added in

		require.NoError(t, s.insertLaneMapRowsWithBlock(ctx, tx, block1, 6, rows))
		require.True(t, rows.stored)
		require.False(t, rows.refused)
		require.Less(t, rows.insert, time.Minute, "the last attempt's insert alone")

		failures := laneMetricValue(t, "teranode_lane_row_insert_failures_total")
		inserts, _ := laneGateInsert(t)

		rows.observe()
		require.Equal(t, failures, laneMetricValue(t, "teranode_lane_row_insert_failures_total"))

		after, _ := laneGateInsert(t)
		require.Equal(t, inserts+1, after)
	})
}

// DeleteBlock, which the rewind tool uses, removes the block and leaves its rows: rows are kept for
// ever, and block IDs are never handed out again, so the rows name no block.
func TestDeleteBlockLeavesItsLaneMapRows(t *testing.T) {
	runLaneHandoutTests(t, "delete", func(t *testing.T, s *SQL) {
		ctx := t.Context()
		rows := blockLaneRows(t)

		id1, _, err := s.StoreBlock(ctx, block1, "", options.WithLaneMapRows(rows))
		require.NoError(t, err)

		require.NoError(t, s.DeleteBlock(ctx, block1.Hash()))

		stored, err := s.GetBlockExists(ctx, block1.Hash())
		require.NoError(t, err)
		require.False(t, stored)

		requireRowsOf(t, s, id1, rows)
	})
}
