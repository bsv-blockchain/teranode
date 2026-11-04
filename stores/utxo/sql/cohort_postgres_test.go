package sql

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
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
