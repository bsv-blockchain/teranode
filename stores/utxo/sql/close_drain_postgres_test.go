package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startPostgres spins up a throwaway Postgres testcontainer and returns its URL.
func startPostgres(t *testing.T) *url.URL {
	t.Helper()
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
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, pgContainer.Terminate(ctx)) })

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	dbURL, err := url.Parse(connStr)
	require.NoError(t, err)

	return dbURL
}

// TestClose_DrainsQueuedCreate_Postgres is the regression test for the shutdown
// data-loss bug. The SQL create path enqueues into the background create batcher
// and blocks the caller until the batch callback commits the write. We configure
// the batcher so it never fires on its own (drain mode off, batch far larger than
// one item, long flush window), launch a Create in a goroutine so it parks
// waiting on its result, then call Close. Only Close's shutdown drain can flush
// the batch — so a Create that returns success, plus a row that survives a
// reconnect, proves Close waited for the in-flight write to commit before
// tearing down the DB connection.
//
// Against go-batcher v2.0.3 (fire-and-forget Close) this would fail: Close
// returned before the batcher worker ran the commit, the store closed the DB
// out from under it, and the queued Create was lost (and its caller wedged).
// go-batcher v2.0.4 makes Close block until the drain completes.
func TestClose_DrainsQueuedCreate_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	dbURL := startPostgres(t)
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second
	// Force the create to stay queued until shutdown: no immediate drain, a batch
	// far larger than our single item, and a flush window long enough that only
	// Close's shutdown drain can dispatch it.
	tSettings.BatcherDrainMode = false
	tSettings.UtxoStore.StoreBatcherSize = 1024
	tSettings.UtxoStore.StoreBatcherDurationMillis = 60000 // 60s; Close fires well before this
	// Keep reads synchronous so the verify Get below doesn't wait on a get-batch
	// timeout (getBatcher is only created when GetBatcherSize > 1).
	tSettings.UtxoStore.GetBatcherSize = 1

	store, err := New(ctx, ulogger.TestLogger{}, tSettings, dbURL)
	require.NoError(t, err)
	require.NotNil(t, store.createBatcher, "createBatcher must be enabled for Postgres with StoreBatcherSize > 1")

	tx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)

	// Create blocks until the batch callback signals its result, so run it in a
	// goroutine. With the batcher config above it parks until Close drains it.
	type createResult struct {
		meta *meta.Data
		err  error
	}
	resultCh := make(chan createResult, 1)
	go func() {
		m, cErr := store.Create(context.Background(), tx, 0)
		resultCh <- createResult{meta: m, err: cErr}
	}()

	// Give the Create goroutine time to enqueue into the batcher before we close.
	time.Sleep(500 * time.Millisecond)

	// Close must drain the queued create (commit it) before returning.
	closeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	require.NoError(t, store.Close(closeCtx))

	// The drained commit must have unblocked Create with a success result.
	select {
	case res := <-resultCh:
		require.NoError(t, res.err, "Create should succeed once Close drained its batch")
		require.NotNil(t, res.meta)
		assert.Equal(t, uint64(259), res.meta.SizeInBytes)
	case <-time.After(5 * time.Second):
		t.Fatal("Create did not return after Close drained the batcher — the queued write was lost")
	}

	// Re-open the same database and confirm the transaction was persisted durably.
	verifyStore, err := New(ctx, ulogger.TestLogger{}, tSettings, dbURL)
	require.NoError(t, err)
	t.Cleanup(func() { _ = verifyStore.Close(ctx) })

	got, err := verifyStore.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err, "transaction must be durably persisted after Close drained the create batcher")
	require.NotNil(t, got)
	assert.Equal(t, uint64(259), got.SizeInBytes)
}
