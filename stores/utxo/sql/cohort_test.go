package sql

import (
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/cohort"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// newCohortTestStore builds a sqlite-in-memory store on its own database name so
// the cohort tests cannot collide with the shared "sqlitememory:///test" store
// the rest of this package uses.
func newCohortTestStore(t *testing.T, dbName string) (context.Context, *Store) {
	t.Helper()
	initPrometheusMetrics()

	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second
	tSettings.BatcherDrainMode = true

	storeURL, err := url.Parse("sqlitememory:///" + dbName)
	require.NoError(t, err)

	store, err := New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = store.Close(ctx)
	})

	return ctx, store
}

// TestCohortRoundTrip stores a transaction with a cohort stamp and reads it back
// through both read paths — the unbatched single get and the batch-decorate
// chunk — to prove the label survives the write and both SELECTs.
//
// Only the plain insert path is exercised here: the CTE insert and the batched
// create are both gated on engine == "postgres" (see createWithRetry and the
// createBatcher setup in New), so sqlite cannot reach them. TestCohortInCreateCTESQL
// below covers the shape of the CTE statement those two paths share.
func TestCohortRoundTrip(t *testing.T) {
	ctx, store := newCohortTestStore(t, "cohort_round_trip")

	tx := newExtendedTxWithOutputs(t, 2)
	stamp := cohort.ID(1_700_000_000)

	md, err := store.Create(ctx, tx, 100, utxo.WithCohort(stamp))
	require.NoError(t, err)
	require.Equal(t, uint32(stamp), md.Cohort, "Create should return the stamped cohort")

	t.Run("unbatched get", func(t *testing.T) {
		data, err := store.getUnbatched(ctx, tx.TxIDChainHash(), utxo.MetaFieldsWithTx)
		require.NoError(t, err)
		require.Equal(t, uint32(stamp), data.Cohort)
	})

	t.Run("batch decorate", func(t *testing.T) {
		items := []*utxo.UnresolvedMetaData{{Hash: *tx.TxIDChainHash(), Idx: 0}}

		require.NoError(t, store.BatchDecorate(ctx, items, fields.Tx))
		require.NoError(t, items[0].Err)
		require.NotNil(t, items[0].Data)
		require.Equal(t, uint32(stamp), items[0].Data.Cohort)
	})

	t.Run("Get", func(t *testing.T) {
		data, err := store.Get(ctx, tx.TxIDChainHash())
		require.NoError(t, err)
		require.Equal(t, uint32(stamp), data.Cohort)
	})
}

// TestCohortDefaultsToUnset proves that a Create without WithCohort leaves the
// value at zero (cohort.Unset) on the returned metadata and on both read paths.
// This is the behaviour every caller gets while the issue-556 feature flag is
// off: nothing is stamped at all.
func TestCohortDefaultsToUnset(t *testing.T) {
	ctx, store := newCohortTestStore(t, "cohort_default_unset")

	tx := newExtendedTxWithOutputs(t, 2)

	md, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)
	require.Equal(t, uint32(cohort.Unset), md.Cohort)

	data, err := store.getUnbatched(ctx, tx.TxIDChainHash(), utxo.MetaFieldsWithTx)
	require.NoError(t, err)
	require.Equal(t, uint32(cohort.Unset), data.Cohort)

	items := []*utxo.UnresolvedMetaData{{Hash: *tx.TxIDChainHash(), Idx: 0}}
	require.NoError(t, store.BatchDecorate(ctx, items, fields.Tx))
	require.NoError(t, items[0].Err)
	require.Equal(t, uint32(cohort.Unset), items[0].Data.Cohort)
}

// TestCohortSchemaMigrationIsIdempotent exercises both branches of the sqlite
// migration. Re-running the schema creation over an up-to-date database must be
// a no-op (the "column already exists" branch, which is what an upgraded node
// does on every restart), and running it over a database whose transactions
// table has no cohort column must add one and leave the store usable — that is
// the ALTER branch, which is the only thing a real upgrade actually runs.
func TestCohortSchemaMigrationIsIdempotent(t *testing.T) {
	t.Run("re-running over an up-to-date schema is a no-op", func(t *testing.T) {
		ctx, store := newCohortTestStore(t, "cohort_migration_noop")

		require.NoError(t, createSqliteSchema(store.db))

		tx := newExtendedTxWithOutputs(t, 1)
		stamp := cohort.ID(cohort.GenesisTime + 1)

		_, err := store.Create(ctx, tx, 100, utxo.WithCohort(stamp))
		require.NoError(t, err)

		data, err := store.getUnbatched(ctx, tx.TxIDChainHash(), utxo.MetaFieldsWithTx)
		require.NoError(t, err)
		require.Equal(t, uint32(stamp), data.Cohort)
	})

	t.Run("a database without the column gets it added", func(t *testing.T) {
		ctx, store := newCohortTestStore(t, "cohort_migration_alter")

		// Rewind to the pre-cohort schema.
		_, err := store.db.ExecContext(ctx, `ALTER TABLE transactions DROP COLUMN cohort`)
		require.NoError(t, err)
		require.Equal(t, 0, cohortColumnCount(ctx, t, store), "column should be gone")

		// The migration must put it back...
		require.NoError(t, createSqliteSchema(store.db))
		require.Equal(t, 1, cohortColumnCount(ctx, t, store), "migration should re-add the column")

		// ...and the store must work against the migrated table.
		tx := newExtendedTxWithOutputs(t, 1)
		stamp := cohort.ID(cohort.GenesisTime + 1)

		_, err = store.Create(ctx, tx, 100, utxo.WithCohort(stamp))
		require.NoError(t, err)

		data, err := store.getUnbatched(ctx, tx.TxIDChainHash(), utxo.MetaFieldsWithTx)
		require.NoError(t, err)
		require.Equal(t, uint32(stamp), data.Cohort)
	})
}

// cohortColumnCount reports how many columns named "cohort" the transactions
// table has — 0 or 1.
func cohortColumnCount(ctx context.Context, t *testing.T, store *Store) int {
	t.Helper()

	var count int

	require.NoError(t, store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pragma_table_info('transactions') WHERE name = 'cohort'`,
	).Scan(&count))

	return count
}

// TestCohortInCreateCTESQL checks the postgres-only CTE insert statement names
// the cohort column and that its column list and VALUES list still line up.
// Neither of the two paths that execute this statement (createCTE and
// sendCreateBatch) can run against sqlite, so this is the only coverage the
// statement's shape gets without a postgres container.
func TestCohortInCreateCTESQL(t *testing.T) {
	const marker = "INSERT INTO transactions ("

	start := strings.Index(createCTESQL, marker)
	require.GreaterOrEqual(t, start, 0, "createCTESQL should insert into transactions")

	stmt := createCTESQL[start:]
	stmt = stmt[:strings.Index(stmt, "ON CONFLICT")]

	cols := stmt[len(marker):strings.Index(stmt, ")")]
	require.Contains(t, strings.Split(cols, ","), "cohort")

	valuesStart := strings.Index(stmt, "VALUES (")
	require.GreaterOrEqual(t, valuesStart, 0)

	values := stmt[valuesStart+len("VALUES ("):]
	values = values[:strings.Index(values, ")")]

	require.Equal(t, len(strings.Split(cols, ",")), len(strings.Split(values, ",")),
		"createCTESQL column list and VALUES list must have the same length")
	require.Contains(t, strings.Split(values, ","), "$27")
}
