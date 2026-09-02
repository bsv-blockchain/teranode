package sql

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupPostgresStore starts a Postgres testcontainer and returns a *Store with
// the unlock batcher enabled (LockedBatcherSize > 1).
func setupPostgresStore(t *testing.T) (*Store, context.Context) {
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
	test.SkipIfContainerUnavailable(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pgContainer.Terminate(ctx))
	})

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	dbURL, err := url.Parse(connStr)
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second
	tSettings.BatcherDrainMode = true // batcher fires immediately in tests
	tSettings.UtxoStore.LockedBatcherSize = 64
	tSettings.UtxoStore.LockedBatcherDurationMillis = 5
	// Pin pruning mode: TestMinedThenSpendAllPrunes asserts default-mode
	// deletion and must not flip when a developer's settings_local.conf sets
	// pruner_utxoDefensiveEnabled=true.
	tSettings.Pruner.UTXODefensiveEnabled = false

	store, err := New(ctx, ulogger.TestLogger{}, tSettings, dbURL)
	require.NoError(t, err)

	require.NotNil(t, store.unlockBatcher, "unlockBatcher should be initialised for Postgres with LockedBatcherSize > 1")

	return store, ctx
}

// TestConflictWAL_Postgres exercises the conflict-resolution write-ahead log
// against real Postgres — the ON CONFLICT DO NOTHING idempotency path and the
// BYTEA intent_id / tx_hashes columns that the SQLite path cannot cover.
func TestConflictWAL_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, _ := setupPostgresStore(t)

	tests.ConflictWAL(t, store)
}

// TestConflictWALCrashRecovery_Postgres runs the per-step-boundary crash-restart
// harness against real Postgres. It exercises the FULL ProcessConflicting /
// ReverseProcessConflicting execution path (Mark/Unspend/Spend/Unmark + WAL),
// which the sqlitememory store cannot host — SetConflicting holds a transaction
// while the background get-batcher needs a second connection, and sqlitememory's
// single-connection model deadlocks. Postgres has a real connection pool.
func TestConflictWALCrashRecovery_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, _ := setupPostgresStore(t)

	tests.ConflictWALCrashRecovery(t, store)
}

// TestCreateMinedUnspendableSetsDAH_Postgres is the Postgres counterpart of the
// SQLite TestCreateMinedUnspendableSetsDAH: it exercises the real Postgres create
// path (CTE / batcher) and asserts that a transaction created already-mined with
// no spendable outputs (an OP_RETURN-only data carrier) is assigned a
// delete_at_height so the pruner can expire it. Before the fix this record had no
// delete_at_height and was retained forever.
func TestCreateMinedUnspendableSetsDAH_Postgres(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)

	retention := store.settings.GetUtxoStoreBlockHeightRetention()
	require.Greater(t, retention, uint32(0), "test requires a non-zero block height retention")

	const minedHeight = uint32(1000)

	// Build an OP_RETURN-only (zero spendable outputs) transaction.
	tx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)

	tx.Outputs = tx.Outputs[:0]
	require.NoError(t, tx.AddOpReturnOutput([]byte("teranode op_return data carrier")))
	defer func() { _ = store.Delete(ctx, tx.TxIDChainHash()) }()

	_, err = store.Create(ctx, tx, minedHeight, utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
		BlockID: 1, BlockHeight: minedHeight, SubtreeIdx: 0, OnLongestChain: true,
	}))
	require.NoError(t, err)

	var dah *int64
	err = store.db.QueryRowContext(ctx, "SELECT delete_at_height FROM transactions WHERE hash = $1", tx.TxIDChainHash()[:]).Scan(&dah)
	require.NoError(t, err)
	require.NotNil(t, dah, "a mined unspendable tx must be assigned delete_at_height via the Postgres create path")
	assert.Equal(t, int64(minedHeight)+int64(retention), *dah)
}

// TestUnlockBatcher_Postgres_SingleHash verifies that a single-hash
// SetLocked(false) call goes through the unlock batcher on Postgres and
// correctly clears the locked flag.
func TestUnlockBatcher_Postgres_SingleHash(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)

	err := store.SetBlockHeight(1000)
	require.NoError(t, err)

	// Create parent tx (needed because tests.Tx references it as input).
	_, err = store.Create(ctx, tests.ParentTx, 999)
	require.NoError(t, err)
	defer func() { _ = store.Delete(ctx, tests.ParentTx.TxIDChainHash()) }()

	// Create the test transaction as locked.
	_, err = store.Create(ctx, tests.Tx, 1000, utxo.WithLocked(true))
	require.NoError(t, err)
	defer func() { _ = store.Delete(ctx, tests.Tx.TxIDChainHash()) }()

	// Verify it is locked.
	txHash := *tests.Tx.TxIDChainHash()
	m, err := store.Get(ctx, &txHash)
	require.NoError(t, err)
	require.True(t, m.Locked, "tx should be locked after Create with WithLocked")

	// Unlock via single-hash path (this exercises the batcher).
	err = store.SetLocked(ctx, []chainhash.Hash{txHash}, false)
	require.NoError(t, err)

	// Verify it is unlocked.
	m, err = store.Get(ctx, &txHash)
	require.NoError(t, err)
	require.False(t, m.Locked, "tx should be unlocked after SetLocked(false) through batcher")
}

// TestUnlockBatcher_Postgres_DAH verifies that the batched unlock path
// correctly recalculates delete_at_height (DAH) for a fully-spent, mined,
// on-longest-chain transaction.
func TestUnlockBatcher_Postgres_DAH(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)

	err := store.SetBlockHeight(1000)
	require.NoError(t, err)

	// Create parent tx for the spend chain.
	_, err = store.Create(ctx, tests.ParentTx, 999)
	require.NoError(t, err)
	defer func() { _ = store.Delete(ctx, tests.ParentTx.TxIDChainHash()) }()

	// Create the child tx as mined on the longest chain (block_ids exist,
	// unmined_since is NULL) so the DAH branch in setUnlockedBulk triggers.
	_, err = store.Create(ctx, tests.Tx, 1000,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
			BlockID:        100,
			BlockHeight:    1000,
			SubtreeIdx:     0,
			OnLongestChain: true,
		}),
	)
	require.NoError(t, err)
	defer func() { _ = store.Delete(ctx, tests.Tx.TxIDChainHash()) }()

	txHash := *tests.Tx.TxIDChainHash()

	// Verify delete_at_height is NULL before spending.
	var dahBefore *int64
	err = store.db.QueryRowContext(ctx,
		"SELECT delete_at_height FROM transactions WHERE hash = $1",
		txHash[:]).Scan(&dahBefore)
	require.NoError(t, err)
	require.Nil(t, dahBefore, "delete_at_height should be NULL before spending")

	// Spend all outputs to make the tx fully spent.
	for i, out := range tests.Tx.Outputs {
		spendTx := bt.NewTx()
		require.NoError(t, spendTx.From(
			txHash.String(), uint32(i),
			out.LockingScript.String(),
			out.Satoshis,
		))
		require.NoError(t, spendTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000))

		_, spendErr := store.Spend(ctx, spendTx, 1001)
		require.NoError(t, spendErr)
	}

	// Lock the now-spent tx, then unlock via the batcher path.
	err = store.SetLocked(ctx, []chainhash.Hash{txHash}, true)
	require.NoError(t, err)

	m, err := store.Get(ctx, &txHash)
	require.NoError(t, err)
	require.True(t, m.Locked, "tx should be locked before batcher unlock")

	// Unlock via single-hash batcher path — this exercises DAH recalculation.
	err = store.SetLocked(ctx, []chainhash.Hash{txHash}, false)
	require.NoError(t, err)

	// Verify unlock.
	m, err = store.Get(ctx, &txHash)
	require.NoError(t, err)
	require.False(t, m.Locked, "tx should be unlocked after batcher unlock")

	// Verify that delete_at_height was set by the DAH recalculation.
	// For a fully-spent, mined, on-longest-chain tx: DAH = blockHeight + 1 + retention.
	// blockHeight=1000, retention=GlobalBlockHeightRetention(10), so DAH = 1011.
	var dahAfter *int64
	err = store.db.QueryRowContext(ctx,
		"SELECT delete_at_height FROM transactions WHERE hash = $1",
		txHash[:]).Scan(&dahAfter)
	require.NoError(t, err)
	require.NotNil(t, dahAfter, "delete_at_height should be set after unlock of fully-spent mined tx")
	require.Equal(t, int64(1011), *dahAfter, "delete_at_height should be blockHeight+1+retention")
}

// TestUnlockBatcher_Postgres_MultipleConcurrent verifies that multiple
// concurrent single-hash unlock calls complete correctly with the unlock
// batcher-enabled path.
func TestUnlockBatcher_Postgres_MultipleConcurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Postgres integration test in short mode")
	}

	store, ctx := setupPostgresStore(t)

	err := store.SetBlockHeight(1000)
	require.NoError(t, err)

	// Create a base tx so we can derive children from it.
	_, err = store.Create(ctx, tests.ParentTx, 999)
	require.NoError(t, err)
	defer func() { _ = store.Delete(ctx, tests.ParentTx.TxIDChainHash()) }()

	// Create and lock N transactions.
	const n = 10
	txs := make([]*bt.Tx, n)
	for i := 0; i < n; i++ {
		tx := bt.NewTx()
		require.NoError(t, tx.FromUTXOs(&bt.UTXO{
			TxIDHash:      tests.Tx.TxIDChainHash(),
			Vout:          0,
			LockingScript: tests.Tx.Inputs[0].PreviousTxScript,
			Satoshis:      tests.Tx.Inputs[0].PreviousTxSatoshis,
		}))
		tx.Inputs[0].UnlockingScript = tests.Tx.Inputs[0].UnlockingScript
		require.NoError(t, tx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", uint64(1000+i)))
		txs[i] = tx

		_, createErr := store.Create(ctx, tx, 1000, utxo.WithLocked(true))
		require.NoError(t, createErr)
		defer func(tx *bt.Tx) { _ = store.Delete(ctx, tx.TxIDChainHash()) }(tx)
	}

	// Unlock all concurrently.
	errCh := make(chan error, n)
	for _, tx := range txs {
		go func(txHash chainhash.Hash) {
			errCh <- store.SetLocked(ctx, []chainhash.Hash{txHash}, false)
		}(*tx.TxIDChainHash())
	}

	for i := 0; i < n; i++ {
		require.NoError(t, <-errCh)
	}

	// Verify all are unlocked.
	for _, tx := range txs {
		txHash := tx.TxIDChainHash()
		m, getErr := store.Get(ctx, txHash)
		require.NoError(t, getErr)
		require.False(t, m.Locked, "tx %s should be unlocked", txHash)
	}
}
