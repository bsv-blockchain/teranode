package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// slowSpendTimer is the spend batcher timer the level-width test runs with. At
// 50ms a level of 300 transactions written one at a time has a floor of 15s,
// so the test can allow a slow runner several seconds and still tell the two
// apart.
const slowSpendTimer = 50 * time.Millisecond

// newSQLiteStoreProductionBatchers opens a sqlitememory store with the batchers
// as production runs them: drain mode off, so a spend waits for the batcher's
// timer or a full batch rather than firing at once. The other SQLite tests turn
// drain mode on, which hides how the store behaves between those two. spendTimer
// replaces the configured spend batcher timer when it is positive.
func newSQLiteStoreProductionBatchers(t *testing.T, spendTimer time.Duration) utxostore.Store {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BatcherDrainMode = false
	tSettings.UtxoStore.SpendBatcherDrainMode = false

	if spendTimer > 0 {
		tSettings.UtxoStore.SpendBatcherDurationMillis = int(spendTimer / time.Millisecond)
	}

	storeURL, err := url.Parse("sqlitememory:///" + t.Name())
	require.NoError(t, err)

	db, err := New(context.Background(), ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	return db
}

// The shared SpendAndCreateMulti suite holds on SQLite with production batchers
// and the full per-level concurrency: overlapping spend transactions that fail
// one another with a table-lock error are retried, and every outcome matches.
func TestSpendAndCreateMultiSQLiteProductionBatchers(t *testing.T) {
	spendAndCreateMultiSuite(t, func(t *testing.T) utxostore.Store { return newSQLiteStoreProductionBatchers(t, 0) })
}

// A level is written as wide on SQLite as on Postgres. Written one transaction
// at a time, each spend waited out the batcher timer alone: 300 independent
// transactions took 3.1s on the production 10ms timer against 56ms as
// concurrent SpendAndCreate calls, and on regtest every block goes down this
// path. Serial writing has a floor no machine can lower, one timer wait per
// transaction, so the test widens the timer to make that floor 15s and allows
// the concurrent write 5s, enough for a slow runner under the race detector.
func TestSpendAndCreateMultiSQLiteWritesALevelConcurrently(t *testing.T) {
	ctx := context.Background()
	db := newSQLiteStoreProductionBatchers(t, slowSpendTimer)

	const width = 300

	w := tests.BuildMultiWorkload(t, 0x61, 1, width)
	w.StoreRoots(t, db, 99)

	start := time.Now()

	results, err := db.SpendAndCreateMulti(ctx, w.Txs, 100, utxostore.WithIgnoreLocked(true))
	require.NoError(t, err)

	elapsed := time.Since(start)

	for i, r := range results {
		require.Equal(t, utxostore.MultiTxCreated, r.Status, "tx %d: %v", i, r.Err)
	}

	for i, r := range w.Records(t, db) {
		require.True(t, r.ExistsInStore, "tx %d", i)
	}

	t.Logf("a level of %d independent transactions took %v (serial floor %v)", width, elapsed, time.Duration(width)*slowSpendTimer)

	// Serially the level is width timer waits, 15s at the very least;
	// concurrently it is one or two batches: 0.1s, or about 1.2s under -race.
	require.Less(t, elapsed, 5*time.Second, "a level of %d independent transactions took %v: written serially (floor %v)", width, elapsed, time.Duration(width)*slowSpendTimer)
}
