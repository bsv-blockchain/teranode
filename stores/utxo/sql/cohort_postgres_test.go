package sql

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/cohort"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/stretchr/testify/require"
)

// TestCohortRoundTrip_Postgres runs the shared cohort round trip on the two
// Postgres-only create paths, which bind the cohort as $27 and which the SQLite
// suite never reaches: the single-statement createCTE (no create batcher) and
// the batched createCTE sent through the create batcher.
func TestCohortRoundTrip_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	for _, tc := range []struct {
		name         string
		batcherSize  int
		wantBatching bool
	}{
		{name: "createCTE without batcher", batcherSize: 1, wantBatching: false},
		{name: "createCTE through the create batcher", batcherSize: 64, wantBatching: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dbURL := startPostgres(t)
			ctx := context.Background()

			tSettings := test.CreateBaseTestSettings(t)
			tSettings.UtxoStore.DBTimeout = 30 * time.Second
			tSettings.UtxoStore.BatchSQLOperations = true
			tSettings.UtxoStore.StoreBatcherSize = tc.batcherSize
			tSettings.UtxoStore.StoreBatcherDurationMillis = 10
			tSettings.BatcherDrainMode = true
			tSettings.UtxoStore.GetBatcherSize = 1

			store, err := New(ctx, ulogger.TestLogger{}, tSettings, dbURL)
			require.NoError(t, err)
			t.Cleanup(func() { _ = store.Close(context.Background()) })

			if tc.wantBatching {
				require.NotNil(t, store.createBatcher, "the create batcher must be on for this case")
			} else {
				require.Nil(t, store.createBatcher, "the create batcher must be off for this case")
			}

			tests.CohortRoundTrip(t, store)
		})
	}
}

// TestCohortColumnMigration_Postgres runs the ADD COLUMN cohort migration on a
// real Postgres database that predates it: a row is created, the column is
// dropped so the table looks like an old deployment's, and the schema DDL runs
// again. The column must come back, the old row must read as cohort.Unset (the
// column default), and a new create must round-trip its cohort.
func TestCohortColumnMigration_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	dbURL := startPostgres(t)
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second
	tSettings.UtxoStore.StoreBatcherSize = 1
	tSettings.UtxoStore.GetBatcherSize = 1

	seeder, err := New(ctx, ulogger.TestLogger{}, tSettings, dbURL)
	require.NoError(t, err)

	oldTx := tests.ParentTx
	_, err = seeder.Create(ctx, oldTx, 100)
	require.NoError(t, err)

	// Close the store before touching the schema. In production the DDL runs in
	// New before any background worker starts; a live store's workers hold locks
	// on transactions and make ALTER TABLE deadlock.
	require.NoError(t, seeder.Close(ctx))

	db, err := usql.Open("postgres", dbURL.String())
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `ALTER TABLE transactions DROP COLUMN cohort`)
	require.NoError(t, err)

	require.NoError(t, createPostgresSchemaImpl(db))

	var hasColumn bool
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'transactions'::regclass AND attname = 'cohort' AND NOT attisdropped)`,
	).Scan(&hasColumn))
	require.True(t, hasColumn, "the migration must add the cohort column back")
	require.NoError(t, db.Close())

	store, err := New(ctx, ulogger.TestLogger{}, tSettings, dbURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	old, err := store.Get(ctx, oldTx.TxIDChainHash(), fields.Cohort)
	require.NoError(t, err)
	require.Equal(t, uint32(cohort.Unset), old.Cohort, "a row from before the migration must read as Unset")

	newTx := tests.Tx
	stamp := cohort.ID(1_700_000_000)

	_, err = store.Create(ctx, newTx, 100, utxo.WithCohort(stamp))
	require.NoError(t, err)

	got, err := store.Get(ctx, newTx.TxIDChainHash(), fields.Cohort)
	require.NoError(t, err)
	require.Equal(t, uint32(stamp), got.Cohort, "a create after the migration must round-trip its cohort")
}
