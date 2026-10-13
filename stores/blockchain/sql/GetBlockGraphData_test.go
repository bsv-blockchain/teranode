package sql

import (
	"context"
	"net/url"
	"sort"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestSQLGetBlockGraphData(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)

	t.Run("get graph data with empty chain", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		data, err := s.GetBlockGraphData(context.Background(), uint64(time.Hour.Milliseconds())) // nolint:gosec
		require.NoError(t, err)
		assert.Empty(t, data.DataPoints)
	})

	t.Run("get graph data with blocks", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		// Store blocks 1, 2, and 3
		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block3, "")
		require.NoError(t, err)

		data, err := s.GetBlockGraphData(context.Background(), uint64(time.Hour.Milliseconds())) // nolint:gosec
		require.NoError(t, err)
		assert.NotEmpty(t, data.DataPoints)
		// The store now returns the aggregated series the asset handler used to
		// build itself; assert it matches that aggregation exactly.
		requireMatchesRawAggregate(t, s, 0, data)
	})

	t.Run("get graph data with periodMillis=0 returns all blocks", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block3, "")
		require.NoError(t, err)

		// periodMillis=0 means block_time >= 0, which matches every block.
		data, err := s.GetBlockGraphData(context.Background(), 0)
		require.NoError(t, err)
		requireMatchesRawAggregate(t, s, 0, data)
	})

	t.Run("get graph data via mainChainRebuilding CTE branch", func(t *testing.T) {
		storeURL, err := url.Parse("sqlitememory:///")
		require.NoError(t, err)

		s, err := New(ulogger.TestLogger{}, storeURL, tSettings)
		require.NoError(t, err)

		_, _, err = s.StoreBlock(context.Background(), block1, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block2, "")
		require.NoError(t, err)
		_, _, err = s.StoreBlock(context.Background(), block3, "")
		require.NoError(t, err)

		// Simulate an in-progress rebuild so GetBlockGraphData uses the CTE branch.
		s.mainChainRebuilding.Add(1)
		defer s.mainChainRebuilding.Add(-1)

		data, err := s.GetBlockGraphData(context.Background(), 0)
		require.NoError(t, err)
		assert.NotEmpty(t, data.DataPoints)
	})
}

// referenceAggregate mirrors the aggregation the asset handler performed before
// it moved into the store: bucket every raw point by the ladder derived from the
// full timestamp range and sum the transaction counts per bucket.
func referenceAggregate(t *testing.T, timestamps []uint32, txCounts []uint64) []*model.DataPoint {
	t.Helper()

	require.Len(t, txCounts, len(timestamps))

	if len(timestamps) == 0 {
		return nil
	}

	minTS, maxTS := timestamps[0], timestamps[0]
	for _, ts := range timestamps {
		if ts < minTS {
			minTS = ts
		}

		if ts > maxTS {
			maxTS = ts
		}
	}

	bs := graphBucketSeconds(int64(maxTS - minTS))
	if len(timestamps) <= 1 || bs == 0 {
		out := make([]*model.DataPoint, 0, len(timestamps))
		for i, ts := range timestamps {
			out = append(out, &model.DataPoint{Timestamp: ts, TxCount: txCounts[i]})
		}

		return out
	}

	sums := make(map[uint32]uint64)
	order := make([]uint32, 0)

	for i, ts := range timestamps {
		b := uint32((uint64(ts) / uint64(bs)) * uint64(bs)) // nolint:gosec
		if _, ok := sums[b]; !ok {
			order = append(order, b)
		}

		sums[b] += txCounts[i]
	}

	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })

	out := make([]*model.DataPoint, 0, len(order))
	for _, b := range order {
		out = append(out, &model.DataPoint{Timestamp: b, TxCount: sums[b]})
	}

	return out
}

// TestSQLGetBlockGraphDataAggregatesInStore asserts that a long chain is
// bucketed by the store and that the result is identical to the aggregation
// the asset handler used to perform over the full raw series.
func TestSQLGetBlockGraphDataAggregatesInStore(t *testing.T) {
	const numBlocks = 400

	store := setupTestStore(t)
	ctx := context.Background()

	genesis, err := store.GetBlockByHeight(ctx, 0)
	require.NoError(t, err)

	timestamps := []uint32{genesis.Header.Timestamp}
	txCounts := []uint64{uint64(genesis.TransactionCount)}

	prev := genesis.Hash()
	// One block per day so the series spans well over a year.
	start := uint32(time.Now().Add(-time.Duration(numBlocks+1) * 24 * time.Hour).Unix()) // nolint:gosec

	for i := 0; i < numBlocks; i++ {
		block := createTestBlock(t, uint32(i+1), prev)   // nolint:gosec
		block.Header.Timestamp = start + uint32(i)*86400 // nolint:gosec

		_, _, err = store.StoreBlock(ctx, block, "")
		require.NoError(t, err)

		prev = block.Hash()

		// The store's flat query drops the height-1 block (its parent_id is
		// genesis's id, 0), so mirror that in the reference series.
		if i == 0 {
			continue
		}

		timestamps = append(timestamps, block.Header.Timestamp)
		txCounts = append(txCounts, uint64(block.TransactionCount))
	}

	want := referenceAggregate(t, timestamps, txCounts)

	data, err := store.GetBlockGraphData(ctx, 0)
	require.NoError(t, err)

	require.Less(t, len(data.DataPoints), numBlocks/2,
		"the store must not return one data point per block for a long chain")
	require.Len(t, data.DataPoints, len(want))

	for i := range want {
		require.Equal(t, want[i].Timestamp, data.DataPoints[i].Timestamp, "bucket %d timestamp", i)
		require.Equal(t, want[i].TxCount, data.DataPoints[i].TxCount, "bucket %d tx count", i)
	}
}

// rawGraphRows reads the unaggregated main-chain series straight from the
// database, i.e. what GetBlockGraphData used to return.
func rawGraphRows(t *testing.T, s *SQL, periodSeconds uint64) ([]uint32, []uint64) {
	t.Helper()

	rows, err := s.db.QueryContext(context.Background(), `
		SELECT block_time, tx_count
		FROM blocks
		WHERE on_main_chain = true
		  AND (id = 0 OR parent_id != 0)
		  AND block_time >= $1
		ORDER BY block_time ASC`, periodSeconds)
	require.NoError(t, err)

	defer rows.Close()

	var (
		timestamps []uint32
		txCounts   []uint64
	)

	for rows.Next() {
		var (
			ts uint32
			tx uint64
		)

		require.NoError(t, rows.Scan(&ts, &tx))

		timestamps = append(timestamps, ts)
		txCounts = append(txCounts, tx)
	}

	require.NoError(t, rows.Err())

	return timestamps, txCounts
}

// requireMatchesRawAggregate asserts the store's output is byte-for-byte the
// series the asset handler produced when it aggregated the full raw set.
func requireMatchesRawAggregate(t *testing.T, s *SQL, periodSeconds uint64, got *model.BlockDataPoints) {
	t.Helper()

	timestamps, txCounts := rawGraphRows(t, s, periodSeconds)
	want := referenceAggregate(t, timestamps, txCounts)

	require.Len(t, got.DataPoints, len(want))

	for i := range want {
		require.Equal(t, want[i].Timestamp, got.DataPoints[i].Timestamp, "point %d timestamp", i)
		require.Equal(t, want[i].TxCount, got.DataPoints[i].TxCount, "point %d tx count", i)
	}
}

// TestSQLGetBlockGraphDataAggregatesInStore_Postgres runs the aggregating query
// against real PostgreSQL: the bucket expression and the SUM cast have to be
// portable, and sqlite alone does not prove that.
func TestSQLGetBlockGraphDataAggregatesInStore_Postgres(t *testing.T) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(ctx,
		"postgres:13",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Minute),
		),
	)
	test.SkipIfContainerUnavailable(t, err)

	defer func() {
		require.NoError(t, pgContainer.Terminate(ctx))
	}()

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	dbURL, err := url.Parse(connStr)
	require.NoError(t, err)

	store, err := New(ulogger.TestLogger{}, dbURL, test.CreateBaseTestSettings(t))
	require.NoError(t, err)

	defer store.Close(ctx)

	genesis, err := store.GetBlockByHeight(ctx, 0)
	require.NoError(t, err)

	prev := genesis.Hash()
	start := uint32(time.Now().Add(-200 * 24 * time.Hour).Unix()) // nolint:gosec

	for i := 0; i < 60; i++ {
		block := createTestBlock(t, uint32(i+1), prev)   // nolint:gosec
		block.Header.Timestamp = start + uint32(i)*86400 // nolint:gosec

		_, _, err = store.StoreBlock(ctx, block, "")
		require.NoError(t, err)

		prev = block.Hash()
	}

	data, err := store.GetBlockGraphData(ctx, 0)
	require.NoError(t, err)
	require.NotEmpty(t, data.DataPoints)
	requireMatchesRawAggregate(t, store, 0, data)
	require.Less(t, len(data.DataPoints), 60)
}
