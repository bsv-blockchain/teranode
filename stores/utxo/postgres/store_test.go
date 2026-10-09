package postgres

import (
	"context"
	"math/rand"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// testDSN is the DSN setupTestStore connects to. It defaults to a local postgres
// for fast developer iteration; TestMain (see main_test.go) overrides it with a
// throwaway testcontainer when no local postgres is reachable, so the suite RUNS
// in CI (where coverage is collected) rather than skipping.
var testDSN = "postgresql://teranode:teranode@localhost:5432/teranode_test"

func setupTestStore(t *testing.T) (*Store, context.Context) {
	t.Helper()

	ctx := context.Background()

	// Clean slate -- drop all tables (including old v3 + v4 tables if present)
	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil {
		t.Skipf("Skipping: cannot connect to postgres: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("Skipping: cannot connect to postgres: %v", err)
	}
	_, _ = pool.Exec(ctx, `
		DROP FUNCTION IF EXISTS process_batch(BIGINT) CASCADE;
		DROP FUNCTION IF EXISTS process_delete_at_height(BIGINT) CASCADE;
		DROP PROCEDURE IF EXISTS materialize_loop() CASCADE;
		DROP PROCEDURE IF EXISTS dah_sweep_batch(BIGINT, INT) CASCADE;
		DROP PROCEDURE IF EXISTS dah_sweep_batch(INT, BIGINT, INT) CASCADE;
		DROP TABLE IF EXISTS conflicting_children, block_ids, spends, outputs, inputs,
			tx_state, transactions, txs, txs_raw, dah_watermark, dah_part_watermark, dah_sweep_control, dah_reconcile_cursor,
			dah_dirty_parents,
			create_queue, input_queue, output_queue, spend_queue, mined_queue,
			batch_notifications CASCADE;
		DROP TABLE IF EXISTS pending_unmined CASCADE;
		DROP INDEX IF EXISTS px_pu_backfill_marker;
	`)
	pool.Close()

	storeURL, err := url.Parse(testDSN)
	require.NoError(t, err)
	storeURL.Scheme = "postgres"

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second

	logger := ulogger.TestLogger{}
	store, err := New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)

	t.Cleanup(func() {
		store.Stop()
	})

	return store, ctx
}

// testExtendedTx returns an extended-format transaction parsed from a known hex string.
// This is the same tx used in the v3 sql_test.go tests.
// It is NOT a coinbase. It has 1 input (prevSatoshis=5BTC) and 2 outputs (5.56M + 44.44M sat), fee=0.
func testExtendedTx(t *testing.T) *bt.Tx {
	t.Helper()
	tx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)
	return tx
}

func TestSchemaCreation(t *testing.T) {
	store, ctx := setupTestStore(t)

	// Verify all tables exist by querying them (outputs table removed in Phase B —
	// per-output data is now stored as parallel arrays on the txs row; raw_tx
	// lives on the txs row).
	tables := []string{"txs", "spends"}
	for _, table := range tables {
		var count int
		err := store.pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count)
		require.NoError(t, err, "table %s should exist", table)
	}

	// Verify partitions exist (numPartitions per table).
	for _, table := range tables {
		var partCount int
		err := store.pool.QueryRow(ctx, `
			SELECT COUNT(*)
			FROM pg_inherits
			JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
			WHERE parent.relname = $1
		`, table).Scan(&partCount)
		require.NoError(t, err, "should be able to count partitions for %s", table)
		require.Equal(t, numPartitions, partCount, "table %s should have %d partitions", table, numPartitions)
	}

	// Verify the txs unmined_since BRIN exists. The legacy px_delete_at_height BRIN is
	// no longer created: pending_deletes is the only pruner path, so it is always
	// backfilled into the side-table and dropped.
	var idxCount int
	err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM pg_indexes
		WHERE indexname = 'px_unmined_since'
	`).Scan(&idxCount)
	require.NoError(t, err)
	require.Equal(t, 1, idxCount, "txs should have the px_unmined_since BRIN")

	var hasDAHBrin bool
	err = store.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname='px_delete_at_height')`).Scan(&hasDAHBrin)
	require.NoError(t, err)
	require.False(t, hasDAHBrin, "px_delete_at_height BRIN must not exist (pending_deletes is the only path)")
}

func TestHealth(t *testing.T) {
	store, ctx := setupTestStore(t)

	code, name, err := store.Health(ctx, true)
	require.NoError(t, err)
	require.Equal(t, 200, code)
	require.Equal(t, "Postgres UTXO Store", name)
}

func TestBlockState(t *testing.T) {
	store, _ := setupTestStore(t)

	require.Equal(t, uint32(0), store.GetBlockHeight())
	require.Equal(t, uint32(0), store.GetMedianBlockTime())

	err := store.SetBlockHeight(12345)
	require.NoError(t, err)
	require.Equal(t, uint32(12345), store.GetBlockHeight())

	err = store.SetMedianBlockTime(1700000000)
	require.NoError(t, err)
	require.Equal(t, uint32(1700000000), store.GetMedianBlockTime())

	state := store.GetBlockState()
	require.Equal(t, uint32(12345), state.Height)
	require.Equal(t, uint32(1700000000), state.MedianTime)
}

// ---------------------------------------------------------------------------
// Task 3 + 4 tests
// ---------------------------------------------------------------------------

func TestCreateAndGet(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)
	blockHeight := uint32(100)

	// Create as unmined (no block info)
	md, err := store.Create(ctx, tx, blockHeight)
	require.NoError(t, err)
	require.NotNil(t, md)

	// Verify basic metadata
	require.False(t, md.IsCoinbase, "should not be coinbase")
	require.Equal(t, uint64(259), md.SizeInBytes)
	require.Equal(t, blockHeight, md.UnminedSince, "unmined tx should have unminedSince = blockHeight")
	require.False(t, md.Conflicting)
	require.False(t, md.Locked)
	require.False(t, md.Frozen)

	// Get with all fields
	txHash := tx.TxIDChainHash()
	got, err := store.Get(ctx, txHash)
	require.NoError(t, err)
	require.NotNil(t, got)

	// Verify transaction metadata
	require.Equal(t, md.SizeInBytes, got.SizeInBytes)
	require.Equal(t, md.Fee, got.Fee)
	require.False(t, got.IsCoinbase)
	require.Equal(t, blockHeight, got.UnminedSince)
	require.False(t, got.Conflicting)
	require.False(t, got.Locked)
	require.False(t, got.Frozen)

	// Verify Tx reconstruction
	require.NotNil(t, got.Tx, "Get with default fields should include Tx")
	require.Equal(t, tx.Version, got.Tx.Version)
	require.Equal(t, tx.LockTime, got.Tx.LockTime)
	require.Equal(t, len(tx.Inputs), len(got.Tx.Inputs), "input count mismatch")
	require.Equal(t, len(tx.Outputs), len(got.Tx.Outputs), "output count mismatch")

	// Verify input details
	for i, input := range tx.Inputs {
		gotInput := got.Tx.Inputs[i]
		require.Equal(t, input.PreviousTxOutIndex, gotInput.PreviousTxOutIndex)
		require.Equal(t, input.SequenceNumber, gotInput.SequenceNumber)
	}

	// Verify output details
	for i, output := range tx.Outputs {
		if output == nil {
			continue
		}
		gotOutput := got.Tx.Outputs[i]
		require.Equal(t, output.Satoshis, gotOutput.Satoshis)
	}

	// Get with only specific fields
	got2, err := store.Get(ctx, txHash, fields.Fee, fields.SizeInBytes)
	require.NoError(t, err)
	require.NotNil(t, got2)
	require.Equal(t, md.Fee, got2.Fee)
	require.Equal(t, md.SizeInBytes, got2.SizeInBytes)
	require.Nil(t, got2.Tx, "should not include Tx when not requested")
}

func TestCreateAndGetMined(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)
	blockHeight := uint32(100)

	// Create as mined
	blockInfo := utxo.MinedBlockInfo{
		BlockID:     42,
		BlockHeight: 100,
		SubtreeIdx:  7,
	}
	md, err := store.Create(ctx, tx, blockHeight, utxo.WithMinedBlockInfo(blockInfo))
	require.NoError(t, err)
	require.NotNil(t, md)
	require.Equal(t, uint32(0), md.UnminedSince, "mined tx should have zero unminedSince")

	// Get and verify block IDs
	txHash := tx.TxIDChainHash()
	got, err := store.Get(ctx, txHash, fields.BlockIDs, fields.Fee)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint32(0), got.UnminedSince, "mined tx should have zero unminedSince from DB")
	require.Len(t, got.BlockIDs, 1)
	require.Equal(t, uint32(42), got.BlockIDs[0])
	require.Len(t, got.BlockHeights, 1)
	require.Equal(t, uint32(100), got.BlockHeights[0])
	require.Len(t, got.SubtreeIdxs, 1)
	require.Equal(t, 7, got.SubtreeIdxs[0])
}

func TestGetNonExistent(t *testing.T) {
	store, ctx := setupTestStore(t)

	fakeHash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	got, err := store.Get(ctx, fakeHash)
	require.Error(t, err)
	require.Nil(t, got)
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "should return ErrTxNotFound, got: %v", err)
}

func TestCreateWithLocked(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)

	md, err := store.Create(ctx, tx, 100, utxo.WithLocked(true))
	require.NoError(t, err)
	require.NotNil(t, md)
	require.True(t, md.Locked)

	// Verify via Get
	txHash := tx.TxIDChainHash()
	got, err := store.Get(ctx, txHash, fields.Locked)
	require.NoError(t, err)
	require.True(t, got.Locked, "locked flag should be true in DB")
}

func TestCreateWithConflicting(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)

	md, err := store.Create(ctx, tx, 100, utxo.WithConflicting(true))
	require.NoError(t, err)
	require.NotNil(t, md)
	require.True(t, md.Conflicting)

	// Verify via Get
	txHash := tx.TxIDChainHash()
	got, err := store.Get(ctx, txHash, fields.Conflicting)
	require.NoError(t, err)
	require.True(t, got.Conflicting, "conflicting flag should be true in DB")
}

func TestCreateWithFrozen(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)

	md, err := store.Create(ctx, tx, 100, utxo.WithFrozen(true))
	require.NoError(t, err)
	require.NotNil(t, md)
	require.True(t, md.Frozen)

	// Verify via Get
	txHash := tx.TxIDChainHash()
	got, err := store.Get(ctx, txHash)
	require.NoError(t, err)
	require.True(t, got.Frozen, "frozen flag should be true in DB")
}

func TestCreateDuplicate(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)

	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	// Second create should fail with TxExists
	_, err = store.Create(ctx, tx, 100)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxExists), "should return ErrTxExists, got: %v", err)
}

func TestGetSpendNotFound(t *testing.T) {
	store, ctx := setupTestStore(t)

	fakeHash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	resp, err := store.GetSpend(ctx, &utxo.Spend{
		TxID:     fakeHash,
		Vout:     0,
		UTXOHash: fakeHash,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, int(utxo.Status_NOT_FOUND), resp.Status)
}

func TestGetSpendOK(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)

	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	// Compute UTXO hash for output 0
	txHash := tx.TxIDChainHash()
	utxoHashes, err := utxo.GetUtxoHashes(tx, txHash)
	require.NoError(t, err)
	require.True(t, len(utxoHashes) > 0)

	resp, err := store.GetSpend(ctx, &utxo.Spend{
		TxID:     txHash,
		Vout:     0,
		UTXOHash: utxoHashes[0],
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	// Non-coinbase: coinbaseSpendingHeight=0, so status should be OK (unspent).
	require.Equal(t, int(utxo.Status_OK), resp.Status, "non-coinbase unspent output should be OK")
}

func TestGetMeta(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)

	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()
	var data meta.Data
	err = store.GetMeta(ctx, txHash, &data)
	require.NoError(t, err)
	require.False(t, data.IsCoinbase)
	require.Equal(t, uint64(259), data.SizeInBytes)
}

func TestBatchDecorate(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)

	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()
	items := []*utxo.UnresolvedMetaData{
		{Hash: *txHash, Idx: 0},
	}

	err = store.BatchDecorate(ctx, items, fields.Fee, fields.SizeInBytes, fields.IsCoinbase)
	require.NoError(t, err)
	require.Nil(t, items[0].Err)
	require.NotNil(t, items[0].Data)
	require.False(t, items[0].Data.IsCoinbase)
	require.Equal(t, uint64(259), items[0].Data.SizeInBytes)
}

func TestBatchDecorateNotFound(t *testing.T) {
	store, ctx := setupTestStore(t)

	fakeHash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	items := []*utxo.UnresolvedMetaData{
		{Hash: *fakeHash, Idx: 0},
	}

	err = store.BatchDecorate(ctx, items, fields.Fee)
	require.NoError(t, err) // BatchDecorate itself does not error on not-found
	require.NotNil(t, items[0].Err, "item should have not-found error")
	require.True(t, errors.Is(items[0].Err, errors.ErrTxNotFound))
}

func TestGetWithTxInpoints(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)

	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()
	got, err := store.Get(ctx, txHash, fields.TxInpoints)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotNil(t, got.TxInpoints)
}

func TestGetWithUtxos(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)

	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()
	got, err := store.Get(ctx, txHash, fields.Utxos, fields.Tx)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotNil(t, got.SpendingDatas, "should have SpendingDatas slice")
	require.Equal(t, len(tx.Outputs), len(got.SpendingDatas), "should have one SpendingData per output")
	// All outputs should be unspent (nil SpendingData)
	for i, sd := range got.SpendingDatas {
		require.Nil(t, sd, "output %d should be unspent (nil spending data)", i)
	}
}

// ---------------------------------------------------------------------------
// Test helpers for Spend + Conflicting tests
// ---------------------------------------------------------------------------

// getSpendingTx creates a transaction that spends the given output indices from parentTx.
func getSpendingTx(t *testing.T, parentTx *bt.Tx, vOut ...uint32) *bt.Tx {
	t.Helper()
	newTx := bt.NewTx()

	for _, outIdx := range vOut {
		err := newTx.From(
			parentTx.TxIDChainHash().String(),
			outIdx,
			parentTx.Outputs[outIdx].LockingScript.String(),
			parentTx.Outputs[outIdx].Satoshis,
		)
		require.NoError(t, err)
	}

	//nolint:gosec
	_ = newTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", rand.Uint64()%1000+1)
	//nolint:gosec
	_ = newTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", rand.Uint64()%1000+1)
	_ = newTx.ChangeToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", &bt.FeeQuote{})

	// Ensure each input has a non-nil unlocking script for the NOT NULL constraint.
	for _, input := range newTx.Inputs {
		if input.UnlockingScript == nil || len(*input.UnlockingScript) == 0 {
			input.UnlockingScript = bscript.NewFromBytes([]byte{0x30, 0x44})
		}
	}

	return newTx
}

// ---------------------------------------------------------------------------
// Task 5 + 6 tests
// ---------------------------------------------------------------------------

func TestSpendOutput(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	// Create the parent transaction.
	_, err := store.Create(ctx, parentTx, blockHeight)
	require.NoError(t, err)

	err = store.SetBlockHeight(blockHeight)
	require.NoError(t, err)

	// Build a spending transaction that spends output 0 of the parent.
	spendTx := getSpendingTx(t, parentTx, 0)

	// Spend should succeed.
	spends, err := store.Spend(ctx, spendTx, blockHeight+1)
	require.NoError(t, err)
	require.Len(t, spends, 1, "spending tx has 1 input, so 1 spend")

	// Verify the output is now spent via Get with Utxos field.
	parentHash := parentTx.TxIDChainHash()
	got, err := store.Get(ctx, parentHash, fields.Utxos, fields.Tx)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotNil(t, got.SpendingDatas, "should have SpendingDatas slice")

	// Output 0 should be spent.
	require.NotNil(t, got.SpendingDatas[0], "output 0 should be spent")
	require.Equal(t, spendTx.TxIDChainHash().String(), got.SpendingDatas[0].TxID.String())
	require.Equal(t, 0, got.SpendingDatas[0].Vin)

	// Other outputs should still be unspent.
	for i := 1; i < len(got.SpendingDatas); i++ {
		require.Nil(t, got.SpendingDatas[i], "output %d should still be unspent", i)
	}
}

func TestDoubleSpend(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	// Create the parent transaction.
	_, err := store.Create(ctx, parentTx, blockHeight)
	require.NoError(t, err)

	err = store.SetBlockHeight(blockHeight)
	require.NoError(t, err)

	// Build two competing spending transactions, both spending output 0.
	spendTxA := getSpendingTx(t, parentTx, 0)
	spendTxB := getSpendingTx(t, parentTx, 0)

	// First spend should succeed.
	_, err = store.Spend(ctx, spendTxA, blockHeight+1)
	require.NoError(t, err)

	// Second spend should fail with ErrSpent.
	spends, err := store.Spend(ctx, spendTxB, blockHeight+1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrSpent), "should return ErrSpent, got: %v", err)
	require.NotEmpty(t, spends)
	require.NotNil(t, spends[0].ConflictingTxID, "should have ConflictingTxID set")
	require.Equal(t, spendTxA.TxIDChainHash().String(), spends[0].ConflictingTxID.String(),
		"ConflictingTxID should be the first spending tx")
}

func TestSpendIdempotent(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	_, err := store.Create(ctx, parentTx, blockHeight)
	require.NoError(t, err)

	err = store.SetBlockHeight(blockHeight)
	require.NoError(t, err)

	spendTx := getSpendingTx(t, parentTx, 0)

	// First spend.
	_, err = store.Spend(ctx, spendTx, blockHeight+1)
	require.NoError(t, err)

	// Same spend again should be idempotent (no error, same spending data).
	_, err = store.Spend(ctx, spendTx, blockHeight+1)
	require.NoError(t, err)
}

func TestSpendNotFound(t *testing.T) {
	store, ctx := setupTestStore(t)
	blockHeight := uint32(100)

	err := store.SetBlockHeight(blockHeight)
	require.NoError(t, err)

	// Build a spending tx that references a non-existent parent.
	fakeTx := bt.NewTx()
	fakeHash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000099")
	require.NoError(t, err)
	_ = fakeTx.From(fakeHash.String(), 0, "76a91488ac", 5000)
	_ = fakeTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000)

	_, err = store.Spend(ctx, fakeTx, blockHeight+1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "should return ErrTxNotFound, got: %v", err)
}

func TestSpendFrozenTx(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	_, err := store.Create(ctx, parentTx, blockHeight, utxo.WithFrozen(true))
	require.NoError(t, err)

	err = store.SetBlockHeight(blockHeight)
	require.NoError(t, err)

	spendTx := getSpendingTx(t, parentTx, 0)

	_, err = store.Spend(ctx, spendTx, blockHeight+1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrFrozen), "should return ErrFrozen, got: %v", err)
}

func TestSpendLockedTx(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	_, err := store.Create(ctx, parentTx, blockHeight, utxo.WithLocked(true))
	require.NoError(t, err)

	err = store.SetBlockHeight(blockHeight)
	require.NoError(t, err)

	spendTx := getSpendingTx(t, parentTx, 0)

	// Without ignore flag: should fail with ErrLocked.
	_, err = store.Spend(ctx, spendTx, blockHeight+1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxLocked), "should return ErrLocked, got: %v", err)

	// With IgnoreLocked: should succeed.
	_, err = store.Spend(ctx, spendTx, blockHeight+1, utxo.IgnoreFlags{IgnoreLocked: true})
	require.NoError(t, err)
}

func TestSpendConflictingTx(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	_, err := store.Create(ctx, parentTx, blockHeight, utxo.WithConflicting(true))
	require.NoError(t, err)

	err = store.SetBlockHeight(blockHeight)
	require.NoError(t, err)

	spendTx := getSpendingTx(t, parentTx, 0)

	// Without ignore flag: should fail with ErrTxConflicting.
	_, err = store.Spend(ctx, spendTx, blockHeight+1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxConflicting), "should return ErrTxConflicting, got: %v", err)

	// With IgnoreConflicting: should succeed.
	_, err = store.Spend(ctx, spendTx, blockHeight+1, utxo.IgnoreFlags{IgnoreConflicting: true})
	require.NoError(t, err)
}

func TestSetLocked(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	// Create with locked=true.
	_, err := store.Create(ctx, parentTx, blockHeight, utxo.WithLocked(true))
	require.NoError(t, err)

	txHash := parentTx.TxIDChainHash()

	// Verify locked=true.
	got, err := store.Get(ctx, txHash, fields.Locked)
	require.NoError(t, err)
	require.True(t, got.Locked, "should be locked")

	// SetLocked(false).
	err = store.SetLocked(ctx, []chainhash.Hash{*txHash}, false)
	require.NoError(t, err)

	// Verify locked=false.
	got, err = store.Get(ctx, txHash, fields.Locked)
	require.NoError(t, err)
	require.False(t, got.Locked, "should be unlocked after SetLocked(false)")

	// SetLocked(true) again.
	err = store.SetLocked(ctx, []chainhash.Hash{*txHash}, true)
	require.NoError(t, err)

	got, err = store.Get(ctx, txHash, fields.Locked)
	require.NoError(t, err)
	require.True(t, got.Locked, "should be locked again")
}

func TestSetConflicting(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	_, err := store.Create(ctx, parentTx, blockHeight)
	require.NoError(t, err)

	txHash := parentTx.TxIDChainHash()

	// Initially not conflicting.
	got, err := store.Get(ctx, txHash, fields.Conflicting)
	require.NoError(t, err)
	require.False(t, got.Conflicting, "should not be conflicting initially")

	// Set conflicting=true.
	affectedSpends, childTxHashes, err := store.SetConflicting(ctx, []chainhash.Hash{*txHash}, true)
	require.NoError(t, err)
	// Parent spends = inputs of the conflicting tx (1 input).
	require.Len(t, affectedSpends, 1, "should have 1 affected parent spend")
	// No child spends since outputs are unspent.
	require.Empty(t, childTxHashes, "no spending children expected")

	// Verify conflicting=true.
	got, err = store.Get(ctx, txHash, fields.Conflicting)
	require.NoError(t, err)
	require.True(t, got.Conflicting, "should be conflicting after SetConflicting(true)")

	// Set conflicting=false.
	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*txHash}, false)
	require.NoError(t, err)

	got, err = store.Get(ctx, txHash, fields.Conflicting)
	require.NoError(t, err)
	require.False(t, got.Conflicting, "should not be conflicting after SetConflicting(false)")
}

// TestSetConflictingDoesNotStampPreservedRow verifies that marking a preserved tx conflicting does
// NOT stamp delete_at_height while it is inside its preservation window (mirrors the DAH sweep's
// preserve_until guard at dah_sweep.go:97 / dah_sweep_proc.go:162). The pruner deletes purely on the
// stamp and never re-checks preserve_until (pruner_provider deleteTombstoned DESIGN CONTRACT), so
// stamping a preserved row would tear it down before its preservation expires. The conflicting flag
// is still applied; the DAH is deferred to ProcessExpiredPreservations. The control half confirms an
// unpreserved conflicting tx is still stamped (the COALESCE/ELSE branch).
func TestSetConflictingDoesNotStampPreservedRow(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)
	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)
	hash := tx.TxIDChainHash()[:]

	readState := func() (conflicting bool, dah *int64) {
		t.Helper()
		require.NoError(t, store.pool.QueryRow(ctx,
			`SELECT conflicting, delete_at_height FROM txs WHERE hash = $1`, hash).Scan(&conflicting, &dah))
		return
	}

	// Preserved state: preserve_until set well into the future, delete_at_height cleared.
	_, err = store.pool.Exec(ctx,
		`UPDATE txs SET preserve_until = 1000000, delete_at_height = NULL WHERE hash = $1`, hash)
	require.NoError(t, err)

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, true)
	require.NoError(t, err)

	conflicting, dah := readState()
	require.True(t, conflicting, "conflicting flag must be set even while preserved")
	require.Nil(t, dah, "preserved row must NOT be stamped with a delete_at_height when marked conflicting")

	// Control: once the preservation is cleared, marking conflicting DOES stamp (ELSE/COALESCE branch).
	_, err = store.pool.Exec(ctx, `UPDATE txs SET preserve_until = NULL WHERE hash = $1`, hash)
	require.NoError(t, err)

	_, _, err = store.SetConflicting(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, true)
	require.NoError(t, err)

	_, dah = readState()
	require.NotNil(t, dah, "unpreserved conflicting tx must be stamped for deletion")
}

func TestMarkTransactionsOnLongestChain(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	// Create as unmined.
	_, err := store.Create(ctx, parentTx, blockHeight)
	require.NoError(t, err)

	txHash := parentTx.TxIDChainHash()

	// Verify unmined_since is set.
	got, err := store.Get(ctx, txHash)
	require.NoError(t, err)
	require.Equal(t, blockHeight, got.UnminedSince, "should be unmined")

	// Mark on longest chain (mined).
	err = store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*txHash}, true)
	require.NoError(t, err)

	got, err = store.Get(ctx, txHash)
	require.NoError(t, err)
	require.Equal(t, uint32(0), got.UnminedSince, "should be mined (unmined_since=0)")

	// Mark off longest chain (unmined again).
	err = store.SetBlockHeight(200)
	require.NoError(t, err)
	err = store.MarkTransactionsOnLongestChain(ctx, []chainhash.Hash{*txHash}, false)
	require.NoError(t, err)

	got, err = store.Get(ctx, txHash)
	require.NoError(t, err)
	require.Equal(t, uint32(200), got.UnminedSince, "should be unmined again at current block height")
}

func TestSetConflictingWithSpendingChildren(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	// Create parent tx.
	_, err := store.Create(ctx, parentTx, blockHeight)
	require.NoError(t, err)

	err = store.SetBlockHeight(blockHeight)
	require.NoError(t, err)

	// Create and execute a spending tx that spends output 0.
	spendTx := getSpendingTx(t, parentTx, 0)
	_, err = store.Create(ctx, spendTx, blockHeight)
	require.NoError(t, err)
	_, err = store.Spend(ctx, spendTx, blockHeight+1)
	require.NoError(t, err)

	txHash := parentTx.TxIDChainHash()

	// Set parent as conflicting -- should return the spending child.
	_, childTxHashes, err := store.SetConflicting(ctx, []chainhash.Hash{*txHash}, true)
	require.NoError(t, err)
	require.Len(t, childTxHashes, 1, "should have 1 spending child tx")
	require.Equal(t, spendTx.TxIDChainHash().String(), childTxHashes[0].String())
}

func TestUnspend(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	_, err := store.Create(ctx, parentTx, blockHeight)
	require.NoError(t, err)

	err = store.SetBlockHeight(blockHeight)
	require.NoError(t, err)

	spendTx := getSpendingTx(t, parentTx, 0)

	spends, err := store.Spend(ctx, spendTx, blockHeight+1)
	require.NoError(t, err)

	// Verify spent.
	parentHash := parentTx.TxIDChainHash()
	got, err := store.Get(ctx, parentHash, fields.Utxos)
	require.NoError(t, err)
	require.NotNil(t, got.SpendingDatas[0], "output 0 should be spent")

	// Unspend.
	err = store.Unspend(ctx, spends)
	require.NoError(t, err)

	// Verify unspent.
	got, err = store.Get(ctx, parentHash, fields.Utxos)
	require.NoError(t, err)
	require.Nil(t, got.SpendingDatas[0], "output 0 should be unspent after Unspend")
}

// ---------------------------------------------------------------------------
// Task 7 + 8 tests
// ---------------------------------------------------------------------------

func TestSetMinedMulti(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)
	blockHeight := uint32(100)

	// Create as unmined.
	_, err := store.Create(ctx, tx, blockHeight)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	// Verify unmined_since is set (not mined).
	got, err := store.Get(ctx, txHash)
	require.NoError(t, err)
	require.Equal(t, blockHeight, got.UnminedSince, "should be unmined initially")
	require.Empty(t, got.BlockIDs, "should have no block_ids initially")

	// SetMinedMulti: mine the tx into block 42 on longest chain.
	info := utxo.MinedBlockInfo{
		BlockID:        42,
		BlockHeight:    100,
		SubtreeIdx:     7,
		OnLongestChain: true,
	}
	result, err := store.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, info)
	require.NoError(t, err)
	require.Contains(t, result, *txHash)
	require.Equal(t, []uint32{42}, result[*txHash])

	// Verify locked=false and unmined_since=0 after mining.
	got, err = store.Get(ctx, txHash, fields.Locked, fields.BlockIDs)
	require.NoError(t, err)
	require.False(t, got.Locked, "should be unlocked after mining")
	require.Equal(t, uint32(0), got.UnminedSince, "should be mined (unmined_since=0)")
	require.Equal(t, []uint32{42}, got.BlockIDs)
}

func TestSetMinedMultiIdempotent(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)
	blockHeight := uint32(100)

	_, err := store.Create(ctx, tx, blockHeight)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	info := utxo.MinedBlockInfo{
		BlockID:        42,
		BlockHeight:    100,
		SubtreeIdx:     7,
		OnLongestChain: true,
	}

	// First call.
	result1, err := store.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, info)
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, result1[*txHash])

	// Second call (idempotent — array append adds duplicate).
	result2, err := store.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, info)
	require.NoError(t, err)
	// With array append, the same block_id appears twice. This is acceptable
	// because SetMinedMulti returns the raw block_ids array content.
	require.Contains(t, result2[*txHash], uint32(42))
}

func TestSetMinedMultiMultipleBlocks(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)
	blockHeight := uint32(100)

	_, err := store.Create(ctx, tx, blockHeight)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	// Mine into block 42.
	info1 := utxo.MinedBlockInfo{
		BlockID:        42,
		BlockHeight:    100,
		SubtreeIdx:     7,
		OnLongestChain: true,
	}
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, info1)
	require.NoError(t, err)

	// Mine into block 43 (competing block).
	info2 := utxo.MinedBlockInfo{
		BlockID:        43,
		BlockHeight:    100,
		SubtreeIdx:     3,
		OnLongestChain: false,
	}
	result, err := store.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, info2)
	require.NoError(t, err)
	require.Equal(t, []uint32{42, 43}, result[*txHash], "should have both block IDs")
}

func TestSetMinedMultiNotOnLongestChain(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)
	blockHeight := uint32(100)

	_, err := store.Create(ctx, tx, blockHeight)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	// Mine not on longest chain: should NOT clear unmined_since.
	info := utxo.MinedBlockInfo{
		BlockID:        42,
		BlockHeight:    100,
		SubtreeIdx:     7,
		OnLongestChain: false,
	}
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, info)
	require.NoError(t, err)

	got, err := store.Get(ctx, txHash)
	require.NoError(t, err)
	require.False(t, got.Locked, "should be unlocked")
	require.Equal(t, blockHeight, got.UnminedSince, "unmined_since should NOT be cleared when not on longest chain")
}

func TestUnsetMined(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)
	blockHeight := uint32(100)

	// Create and mine tx.
	_, err := store.Create(ctx, tx, blockHeight)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	info := utxo.MinedBlockInfo{
		BlockID:        42,
		BlockHeight:    100,
		SubtreeIdx:     7,
		OnLongestChain: true,
	}
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, info)
	require.NoError(t, err)

	// Verify mined.
	got, err := store.Get(ctx, txHash, fields.BlockIDs)
	require.NoError(t, err)
	require.Equal(t, []uint32{42}, got.BlockIDs)
	require.Equal(t, uint32(0), got.UnminedSince)

	// Unset mined (reorg).
	err = store.SetBlockHeight(150)
	require.NoError(t, err)

	unsetInfo := utxo.MinedBlockInfo{
		BlockID:    42,
		UnsetMined: true,
	}
	result, err := store.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, unsetInfo)
	require.NoError(t, err)
	require.Empty(t, result[*txHash], "should have no block_ids after unset")

	// Verify unmined_since is set after all block_ids removed. The gold-standard
	// value is blockHeight+1 (151) — matching the aerospike Lua and the sql store
	// (s.blockHeight.Load() + 1), not the bare tip.
	got, err = store.Get(ctx, txHash)
	require.NoError(t, err)
	require.Equal(t, uint32(151), got.UnminedSince, "should be unmined at current block height + 1")
}

func TestUnsetMinedPartial(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)
	blockHeight := uint32(100)

	_, err := store.Create(ctx, tx, blockHeight)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	// Mine into two blocks.
	info1 := utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 100, SubtreeIdx: 7, OnLongestChain: true}
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, info1)
	require.NoError(t, err)

	info2 := utxo.MinedBlockInfo{BlockID: 43, BlockHeight: 100, SubtreeIdx: 3, OnLongestChain: true}
	_, err = store.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, info2)
	require.NoError(t, err)

	// Unset only block 42 -- should still have block 43.
	unsetInfo := utxo.MinedBlockInfo{BlockID: 42, UnsetMined: true}
	result, err := store.SetMinedMulti(ctx, []*chainhash.Hash{txHash}, unsetInfo)
	require.NoError(t, err)
	require.Equal(t, []uint32{43}, result[*txHash], "should have only block 43")

	// unmined_since should NOT be set because block_ids remain.
	got, err := store.Get(ctx, txHash)
	require.NoError(t, err)
	require.Equal(t, uint32(0), got.UnminedSince, "should still be mined since block_ids remain")
}

func TestDelete(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)
	blockHeight := uint32(100)

	// Create tx.
	_, err := store.Create(ctx, tx, blockHeight)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	// Verify it exists.
	got, err := store.Get(ctx, txHash)
	require.NoError(t, err)
	require.NotNil(t, got)

	// Delete it.
	err = store.Delete(ctx, txHash)
	require.NoError(t, err)

	// Verify Get returns ErrTxNotFound.
	got, err = store.Get(ctx, txHash)
	require.Error(t, err)
	require.Nil(t, got)
	require.True(t, errors.Is(err, errors.ErrTxNotFound), "should return ErrTxNotFound, got: %v", err)
}

func TestDeleteWithBlockIDs(t *testing.T) {
	store, ctx := setupTestStore(t)
	tx := testExtendedTx(t)
	blockHeight := uint32(100)

	// Create as mined.
	blockInfo := utxo.MinedBlockInfo{BlockID: 42, BlockHeight: 100, SubtreeIdx: 7}
	_, err := store.Create(ctx, tx, blockHeight, utxo.WithMinedBlockInfo(blockInfo))
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	// Delete.
	err = store.Delete(ctx, txHash)
	require.NoError(t, err)

	// Verify deleted.
	_, err = store.Get(ctx, txHash)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound))

	// Verify txs row (including embedded block_ids) is gone.
	var count int
	err = store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM txs WHERE hash = $1`, txHash[:]).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "txs row should be deleted")
}

func TestDeleteWithSpends(t *testing.T) {
	store, ctx := setupTestStore(t)
	parentTx := testExtendedTx(t)
	blockHeight := uint32(100)

	// Create parent and spend output 0.
	_, err := store.Create(ctx, parentTx, blockHeight)
	require.NoError(t, err)

	err = store.SetBlockHeight(blockHeight)
	require.NoError(t, err)

	spendTx := getSpendingTx(t, parentTx, 0)
	_, err = store.Spend(ctx, spendTx, blockHeight+1)
	require.NoError(t, err)

	parentHash := parentTx.TxIDChainHash()

	// Delete the parent: should also remove its spends.
	err = store.Delete(ctx, parentHash)
	require.NoError(t, err)

	_, err = store.Get(ctx, parentHash)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxNotFound))

	// Verify spends are cleaned up.
	var count int
	err = store.pool.QueryRow(ctx, `SELECT COUNT(*) FROM spends WHERE prev_tx_hash = $1`, parentHash[:]).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "spends should be deleted")
}

func TestDeleteNonExistent(t *testing.T) {
	store, ctx := setupTestStore(t)

	fakeHash, err := chainhash.NewHashFromStr("0000000000000000000000000000000000000000000000000000000000000001")
	require.NoError(t, err)

	// Deleting a non-existent tx should not error (no rows affected).
	err = store.Delete(ctx, fakeHash)
	require.NoError(t, err)
}

func TestSetMinedMultiEmpty(t *testing.T) {
	store, _ := setupTestStore(t)

	// Empty hashes should return empty map.
	result, err := store.SetMinedMulti(context.Background(), nil, utxo.MinedBlockInfo{})
	require.NoError(t, err)
	require.Empty(t, result)
}

// ---------------------------------------------------------------------------
// Task 9 tests: Iterators
// ---------------------------------------------------------------------------

func TestGetUnminedTxIterator(t *testing.T) {
	store, ctx := setupTestStore(t)

	// Create an unmined transaction.
	tx := testExtendedTx(t)
	blockHeight := uint32(50)
	_, err := store.Create(ctx, tx, blockHeight)
	require.NoError(t, err)

	// GetUnminedTxIterator now reads from the pending_unmined side-table (not a
	// direct txs scan), populated by the write-behind projector. Flush it so the
	// just-created tx is projected before the reload read — mirroring production,
	// where the reload runs after the projector has drained / the backfill ran.
	require.NoError(t, store.flushPendingUnmined(ctx))

	// Get the iterator.
	iter, err := store.GetUnminedTxIterator()
	require.NoError(t, err)
	require.NotNil(t, iter)
	defer iter.Close()

	batch, err := iter.Next(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, batch, "should find at least one unmined tx")

	found := false
	txHash := tx.TxIDChainHash()
	for _, unmined := range batch {
		if unmined.Skip {
			continue
		}
		if unmined.Node != nil && unmined.Node.Hash == *txHash {
			found = true
			require.Equal(t, int(blockHeight), unmined.UnminedSince)
			require.False(t, unmined.Locked)
			require.NotNil(t, unmined.TxInpoints)
		}
	}
	require.True(t, found, "should find the created unmined tx in iterator")

	// Next batch should be empty.
	batch2, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Nil(t, batch2, "no more unmined txs expected")
}

func TestGetPrunableUnminedTxIterator(t *testing.T) {
	store, ctx := setupTestStore(t)

	// Create an unmined transaction at height 10.
	tx := testExtendedTx(t)
	_, err := store.Create(ctx, tx, 10)
	require.NoError(t, err)

	// GetPrunableUnminedTxIterator drives from pending_unmined (Task 3). The
	// write path (Task N) will populate it inline on Create; until then we seed
	// it manually here so the iterator can find the row.
	txHash := tx.TxIDChainHash()
	_, err = store.pool.Exec(ctx,
		`INSERT INTO pending_unmined (hash, unmined_since) VALUES ($1, 10)
		 ON CONFLICT (hash) DO NOTHING`,
		txHash[:])
	require.NoError(t, err)

	// Cutoff 5 -- too low, should not find the tx.
	iter, err := store.GetPrunableUnminedTxIterator(5)
	require.NoError(t, err)
	defer iter.Close()
	batch, err := iter.Next(ctx)
	require.NoError(t, err)
	require.Nil(t, batch, "should not find tx with cutoff < unmined_since")

	// Cutoff 10 -- should find the tx.
	iter2, err := store.GetPrunableUnminedTxIterator(10)
	require.NoError(t, err)
	defer iter2.Close()
	batch2, err := iter2.Next(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, batch2, "should find tx with cutoff >= unmined_since")
}

func TestQueryOldUnminedTransactions(t *testing.T) {
	store, ctx := setupTestStore(t)

	// Create an unmined transaction at height 20.
	tx := testExtendedTx(t)
	_, err := store.Create(ctx, tx, 20)
	require.NoError(t, err)

	// Query with cutoff 19 -- should not find.
	hashes, err := store.QueryOldUnminedTransactions(ctx, 19)
	require.NoError(t, err)
	require.Empty(t, hashes)

	// Query with cutoff 20 -- should find.
	hashes, err = store.QueryOldUnminedTransactions(ctx, 20)
	require.NoError(t, err)
	require.Len(t, hashes, 1)
	require.Equal(t, *tx.TxIDChainHash(), hashes[0])
}

// ---------------------------------------------------------------------------
// Task 10 tests: Alert System
// ---------------------------------------------------------------------------

func TestFreezeAndUnfreezeUTXOs(t *testing.T) {
	store, ctx := setupTestStore(t)

	// Create an unmined transaction.
	tx := testExtendedTx(t)
	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	// Build a spend for the first output.
	utxoHash, err := util.UTXOHashFromOutput(txHash, tx.Outputs[0], 0)
	require.NoError(t, err)

	spends := []*utxo.Spend{
		{TxID: txHash, Vout: 0, UTXOHash: utxoHash},
	}

	// Freeze.
	err = store.FreezeUTXOs(ctx, spends, nil)
	require.NoError(t, err)

	// Verify frozen via the txs.out_frozens packed bitmap (bit 0 = vout 0).
	var frozen bool
	err = store.pool.QueryRow(ctx,
		`SELECT out_frozens IS NOT NULL AND get_bit(out_frozens, 0) = 1 FROM txs WHERE hash = $1`, txHash[:]).Scan(&frozen)
	require.NoError(t, err)
	require.True(t, frozen)

	// Double-freeze should error.
	err = store.FreezeUTXOs(ctx, spends, nil)
	require.Error(t, err)

	// Unfreeze.
	err = store.UnFreezeUTXOs(ctx, spends, nil)
	require.NoError(t, err)

	// Verify unfrozen via the txs.out_frozens packed bitmap.
	err = store.pool.QueryRow(ctx,
		`SELECT out_frozens IS NOT NULL AND get_bit(out_frozens, 0) = 1 FROM txs WHERE hash = $1`, txHash[:]).Scan(&frozen)
	require.NoError(t, err)
	require.False(t, frozen)

	// Double-unfreeze should error.
	err = store.UnFreezeUTXOs(ctx, spends, nil)
	require.Error(t, err)
}

// TestUnfreezeDoesNotSetTxLevelFrozen guards the per-output / whole-tx freeze
// isolation. Freezing two outputs per-output must leave the tx-level `frozen`
// gate false; unfreezing ONE of them must NOT flip `frozen` true just because
// another output's bit is still set. The previous UnFreezeUTXOs recomputed
// `frozen = (any out_frozens bit set)`, which spuriously froze the whole tx
// (blocking every other, never-frozen output via the spend CTE's NOT tx_frozen).
func TestUnfreezeDoesNotSetTxLevelFrozen(t *testing.T) {
	store, ctx := setupTestStore(t)

	// testExtendedTx has two outputs (vout 0 and 1).
	tx := testExtendedTx(t)
	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()
	require.GreaterOrEqual(t, len(tx.Outputs), 2, "need a multi-output tx for this test")

	hash0, err := util.UTXOHashFromOutput(txHash, tx.Outputs[0], 0)
	require.NoError(t, err)
	hash1, err := util.UTXOHashFromOutput(txHash, tx.Outputs[1], 1)
	require.NoError(t, err)

	// Freeze both outputs per-output.
	require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{
		{TxID: txHash, Vout: 0, UTXOHash: hash0},
		{TxID: txHash, Vout: 1, UTXOHash: hash1},
	}, nil))

	// tx-level frozen must remain false — FreezeUTXOs never touches it.
	var txFrozen bool
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT frozen FROM txs WHERE hash = $1`, txHash[:]).Scan(&txFrozen))
	require.False(t, txFrozen, "FreezeUTXOs must not set the whole-tx frozen gate")

	// Unfreeze ONLY output 0; output 1's bit stays set.
	require.NoError(t, store.UnFreezeUTXOs(ctx, []*utxo.Spend{
		{TxID: txHash, Vout: 0, UTXOHash: hash0},
	}, nil))

	// The regression: tx-level frozen must STILL be false even though output 1's
	// per-output bit remains set.
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT frozen FROM txs WHERE hash = $1`, txHash[:]).Scan(&txFrozen))
	require.False(t, txFrozen, "unfreezing one output must not set the whole-tx frozen gate")

	// Output 0's bit cleared, output 1's bit still set (per-output state intact).
	var bit0, bit1 bool
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT get_bit(out_frozens, 0) = 1, get_bit(out_frozens, 1) = 1 FROM txs WHERE hash = $1`, txHash[:]).Scan(&bit0, &bit1))
	require.False(t, bit0, "output 0 should be unfrozen")
	require.True(t, bit1, "output 1 should remain frozen")
}

func TestReAssignUTXO(t *testing.T) {
	store, ctx := setupTestStore(t)

	// Create an unmined transaction.
	tx := testExtendedTx(t)
	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(200))

	txHash := tx.TxIDChainHash()

	utxoHash, err := util.UTXOHashFromOutput(txHash, tx.Outputs[0], 0)
	require.NoError(t, err)

	sourceSpend := &utxo.Spend{TxID: txHash, Vout: 0, UTXOHash: utxoHash}

	// Must freeze first.
	err = store.FreezeUTXOs(ctx, []*utxo.Spend{sourceSpend}, nil)
	require.NoError(t, err)

	// Reassign.
	newHash := chainhash.HashH([]byte("new-utxo-hash"))
	newSpend := &utxo.Spend{UTXOHash: &newHash}
	err = store.ReAssignUTXO(ctx, sourceSpend, newSpend, nil)
	require.NoError(t, err)

	// Verify: bit 0 of out_frozens should be clear, and the first 16-byte slot
	// of the flat utxo_hashes should hold the new hash's 128-bit prefix.
	var outputFrozen bool
	var storedUtxoHash []byte
	err = store.pool.QueryRow(ctx,
		`SELECT out_frozens IS NOT NULL AND get_bit(out_frozens, 0) = 1, substr(utxo_hashes, 1, 16) FROM txs WHERE hash = $1`,
		txHash[:]).Scan(&outputFrozen, &storedUtxoHash)
	require.NoError(t, err)
	require.False(t, outputFrozen, "should be unfrozen after reassign")
	require.Equal(t, newHash[:16], storedUtxoHash)
}

// TestReAssignUTXOSpendableInAtVoutGE1 pins the reassign maturity gate for outputs
// at vout >= 1. spendable_ins is NULL on create and written only by ReAssignUTXO;
// a bare `spendable_ins[vout+1] = v` on a NULL array yields a length-1 array with
// lower bound vout+1, so the reader guard `array_length(spendable_ins,1) >= vout+1`
// is false for vout >= 1 and the spendable-after height is silently dropped —
// making the reassigned UTXO immediately spendable instead of after the delay.
func TestReAssignUTXOSpendableInAtVoutGE1(t *testing.T) {
	store, ctx := setupTestStore(t)

	tx := testExtendedTx(t)
	require.GreaterOrEqual(t, len(tx.Outputs), 2, "need a vout>=1 output to exercise the bug")
	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)
	require.NoError(t, store.SetBlockHeight(200))

	txHash := tx.TxIDChainHash()
	const vout = uint32(1)
	utxoHash, err := util.UTXOHashFromOutput(txHash, tx.Outputs[vout], vout)
	require.NoError(t, err)
	src := &utxo.Spend{TxID: txHash, Vout: vout, UTXOHash: utxoHash}

	require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{src}, nil))

	newHash := chainhash.HashH([]byte("reassign-vout1"))
	require.NoError(t, store.ReAssignUTXO(ctx, src, &utxo.Spend{UTXOHash: &newHash}, nil))

	// Read spendable_in exactly as the spend/get path does (get.go:380).
	var spendableIn *int64
	require.NoError(t, store.pool.QueryRow(ctx,
		`SELECT CASE WHEN array_length(spendable_ins, 1) >= $2::int + 1 THEN spendable_ins[$2::int + 1] END
		   FROM txs WHERE hash = $1`, txHash[:], int32(vout)).Scan(&spendableIn))

	want := int64(200 + utxo.ReAssignedUtxoSpendableAfterBlocks)
	require.NotNil(t, spendableIn, "reassigned vout>=1 must record the spendable-after height, not NULL")
	require.Equal(t, want, *spendableIn, "spendable_in must be block_height + ReAssignedUtxoSpendableAfterBlocks")
}

// ---------------------------------------------------------------------------
// Task 11 tests: Preservation + Pruner
// ---------------------------------------------------------------------------

// TestPreserveTransactions verifies the prune-eligibility gate: PreserveTransactions
// only preserves txs that are actually at risk of pruning — those already carrying a
// delete_at_height stamp, or already preserved. A not-fully-spent tx (no DAH) is not at
// risk and is skipped, keeping it out of the preservation/expiry path.
func TestPreserveTransactions(t *testing.T) {
	t.Run("eligible_stamped_tx_is_preserved", func(t *testing.T) {
		store, ctx := setupTestStore(t)

		tx := testExtendedTx(t)
		_, err := store.Create(ctx, tx, 100)
		require.NoError(t, err)
		hash := tx.TxIDChainHash()

		// Simulate the sweep having stamped it (i.e. it is prune-eligible).
		_, err = store.pool.Exec(ctx, `UPDATE txs SET delete_at_height = 999 WHERE hash = $1`, hash[:])
		require.NoError(t, err)

		require.NoError(t, store.PreserveTransactions(ctx, []chainhash.Hash{*hash}, 500))

		var preserveUntil, dah *int64
		require.NoError(t, store.pool.QueryRow(ctx,
			`SELECT preserve_until, delete_at_height FROM txs WHERE hash = $1`, hash[:]).Scan(&preserveUntil, &dah))
		require.NotNil(t, preserveUntil)
		require.Equal(t, int64(500), *preserveUntil)
		require.Nil(t, dah, "delete_at_height should be cleared on preserve")
	})

	t.Run("ineligible_not_at_risk_tx_is_not_preserved", func(t *testing.T) {
		store, ctx := setupTestStore(t)

		tx := testExtendedTx(t)
		_, err := store.Create(ctx, tx, 100) // unmined + unspent: no DAH, not at risk
		require.NoError(t, err)
		hash := tx.TxIDChainHash()

		require.NoError(t, store.PreserveTransactions(ctx, []chainhash.Hash{*hash}, 500))

		var preserveUntil, dah *int64
		require.NoError(t, store.pool.QueryRow(ctx,
			`SELECT preserve_until, delete_at_height FROM txs WHERE hash = $1`, hash[:]).Scan(&preserveUntil, &dah))
		require.Nil(t, preserveUntil, "not-fully-spent tx (no DAH) must not be preserved")
		require.Nil(t, dah)
	})
}

// TestProcessExpiredPreservations verifies the invariant that delete_at_height is set
// only when a tx is genuinely safe to drop (conflicting, or mined + fully-spent + on the
// longest chain). On expiry preserve_until is always cleared; the DAH stamp is gated.
// preserve_until is injected directly so the expiry logic is exercised in isolation from
// PreserveTransactions' own gate.
func TestProcessExpiredPreservations(t *testing.T) {
	injectExpiredPreserve := func(ctx context.Context, t *testing.T, store *Store, hash []byte) {
		t.Helper()
		_, err := store.pool.Exec(ctx,
			`UPDATE txs SET preserve_until = 50, delete_at_height = NULL WHERE hash = $1`, hash)
		require.NoError(t, err)
	}
	read := func(ctx context.Context, t *testing.T, store *Store, hash []byte) (preserveUntil, dah *int64) {
		t.Helper()
		require.NoError(t, store.pool.QueryRow(ctx,
			`SELECT preserve_until, delete_at_height FROM txs WHERE hash = $1`, hash).Scan(&preserveUntil, &dah))
		return
	}

	t.Run("ineligible_unmined_unspent_tx_is_not_stamped", func(t *testing.T) {
		store, ctx := setupTestStore(t)
		tx := testExtendedTx(t)
		_, err := store.Create(ctx, tx, 100)
		require.NoError(t, err)
		hash := tx.TxIDChainHash()[:]
		injectExpiredPreserve(ctx, t, store, hash)

		require.NoError(t, store.ProcessExpiredPreservations(ctx, 100))

		preserveUntil, dah := read(ctx, t, store, hash)
		require.Nil(t, preserveUntil, "preserve_until must be cleared")
		require.Nil(t, dah, "unmined/unspent tx must NOT be stamped for deletion")
	})

	t.Run("eligible_mined_fully_spent_tx_is_stamped", func(t *testing.T) {
		// Setter-C reconcile (proc v15): eligibility is decided from the maintained
		// spent_bits bitmap, so the sweep must have folded the spends first
		// (bit_count(spent_bits) = spendable_count). Fold via a sweep, then expire
		// the preservation.
		store, ctx := setupTestStore(t)
		ret := int64(store.settings.GetUtxoStoreBlockHeightRetention())
		parent := newMinedSingleOutputTx(t, store, 100)
		spendAllOutputs(t, store, parent, 101)
		hash := parent.TxIDChainHash()[:]

		require.NoError(t, store.SetBlockHeight(105))
		_, err := procSweepUpTo(store, ctx, 105) // folds spent_bits to the full bitmap
		require.NoError(t, err)
		// The sweep already stamped it; clear that + inject an expired preservation so we
		// exercise the expiry path's own stamp decision in isolation.
		injectExpiredPreserve(ctx, t, store, hash)

		require.NoError(t, store.ProcessExpiredPreservations(ctx, 100))

		preserveUntil, dah := read(ctx, t, store, hash)
		require.Nil(t, preserveUntil, "preserve_until must be cleared")
		require.NotNil(t, dah, "eligible (mined + counter-complete) tx must be stamped for deletion")
		// completion = GREATEST(last_spend_height=101, mined_at_height=NULL->0)+1+retention.
		require.Equal(t, int64(101)+1+ret, *dah)
	})

	t.Run("lagging_bitmap_fully_spent_tx_is_NOT_stamped_early", func(t *testing.T) {
		// Setter-C reconcile (proc v15) lagging-bitmap rule: a tx is fully spent on
		// the chain but the background fold has NOT yet folded its spent_bits. The
		// expiry path must NOT stamp it (avoids stamping off a lagging/incomplete
		// bitmap); it just clears preserve_until and leaves DAH NULL. The sweep
		// stamps it later.
		store, ctx := setupTestStore(t)
		parent := newMinedSingleOutputTx(t, store, 100)
		spendAllOutputs(t, store, parent, 101) // fully spent, but NO sweep → bitmap still all-zero
		hash := parent.TxIDChainHash()[:]
		injectExpiredPreserve(ctx, t, store, hash)

		require.NoError(t, store.ProcessExpiredPreservations(ctx, 100))

		preserveUntil, dah := read(ctx, t, store, hash)
		require.Nil(t, preserveUntil, "preserve_until must be cleared even when not stamped")
		require.Nil(t, dah, "must NOT stamp early off a lagging bitmap; the sweep stamps it later")
	})

	t.Run("conflicting_tx_is_stamped_without_being_mined", func(t *testing.T) {
		store, ctx := setupTestStore(t)
		tx := testExtendedTx(t)
		_, err := store.Create(ctx, tx, 100) // unmined
		require.NoError(t, err)
		hash := tx.TxIDChainHash()[:]
		_, err = store.pool.Exec(ctx, `UPDATE txs SET conflicting = true WHERE hash = $1`, hash)
		require.NoError(t, err)
		injectExpiredPreserve(ctx, t, store, hash)

		require.NoError(t, store.ProcessExpiredPreservations(ctx, 100))

		preserveUntil, dah := read(ctx, t, store, hash)
		require.Nil(t, preserveUntil, "preserve_until must be cleared")
		require.NotNil(t, dah, "conflicting tx must be stamped even without being mined")
		require.Equal(t, int64(100)+int64(store.settings.GetUtxoStoreBlockHeightRetention()), *dah)
	})
}

func TestPrunerService(t *testing.T) {
	store, ctx := setupTestStore(t)

	// Create a tx and set delete_at_height. The pruner trusts the stamp (it does
	// no eligibility re-check — keeping the delete path lightweight is critical to
	// reclaim throughput), so a stamped tx is deleted at the prune height.
	tx := testExtendedTx(t)
	_, err := store.Create(ctx, tx, 100)
	require.NoError(t, err)

	txHash := tx.TxIDChainHash()

	// Stamp delete_at_height = 50 on both txs and the pending_deletes side-table.
	// The pruner enumerates doomed candidates from pending_deletes (the only path),
	// so a stamp that lives only on txs would never be seen — the real stamp paths
	// (SetMinedMulti S6, the DAH sweep proc, ProcessExpiredPreservations) always write
	// the side-table alongside the txs column, which this direct stamp mirrors.
	_, err = store.pool.Exec(ctx,
		`UPDATE txs SET delete_at_height = 50 WHERE hash = $1`,
		txHash[:])
	require.NoError(t, err)
	_, err = store.pool.Exec(ctx,
		`INSERT INTO pending_deletes (hash, delete_at_height) VALUES ($1, 50)
		 ON CONFLICT (hash) DO UPDATE SET delete_at_height = EXCLUDED.delete_at_height`,
		txHash[:])
	require.NoError(t, err)

	// Get pruner service.
	svc, err := store.GetPrunerService()
	require.NoError(t, err)
	require.NotNil(t, svc)

	svc.Start(ctx)

	// Prune at height 50 + margin should delete the tx. The delete-side
	// crash-replay margin (Task 7) only deletes rows whose delete_at_height is
	// at least `margin` blocks below the trigger, so the trigger height must
	// clear the stamp by that much.
	margin := uint32(store.settings.UtxoStore.PruneDeleteMarginBlocks) //nolint:gosec
	count, err := svc.Prune(ctx, 50+margin, "test-hash")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	// Verify the tx is gone.
	var txCount int
	err = store.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM txs WHERE hash = $1`, txHash[:]).Scan(&txCount)
	require.NoError(t, err)
	require.Equal(t, 0, txCount, "transaction should be deleted by pruner")
}

// ---------------------------------------------------------------------------
// TestArraySubscriptBoundary: verifies 0-based index → Postgres 1-based
// array subscript correctness after folding outputs into txs arrays.
// ---------------------------------------------------------------------------

// makeThreeOutputTx returns a transaction with exactly 3 outputs:
//
//	vout0 — P2PKH (spendable)
//	vout1 — OP_FALSE OP_RETURN data (unspendable, satoshis=0)
//	vout2 — P2PKH (spendable)
//
// The tx is built around the canonical testExtendedTx input so it passes
// validation (non-nil unlocking scripts).
func makeThreeOutputTx(t *testing.T) *bt.Tx {
	t.Helper()

	base := testExtendedTx(t) // 2 P2PKH outputs

	// Extract the two spendable outputs.
	out0 := base.Outputs[0] // vout0
	out1 := base.Outputs[1] // will become vout2

	// Build an OP_FALSE OP_RETURN data output for vout1.
	opReturnScript := bscript.NewFromBytes([]byte{0x00, 0x6a, 0x04, 0xba, 0xdc, 0x0f, 0xfe})

	// Reconstruct tx with 3 outputs in order: P2PKH, OP_RETURN, P2PKH.
	tx := bt.NewTx()
	tx.Version = base.Version
	tx.LockTime = base.LockTime
	tx.Inputs = base.Inputs
	tx.Outputs = []*bt.Output{
		out0,
		{Satoshis: 0, LockingScript: opReturnScript},
		out1,
	}
	return tx
}

func TestArraySubscriptBoundary(t *testing.T) {
	store, ctx := setupTestStore(t)
	blockHeight := uint32(100)
	require.NoError(t, store.SetBlockHeight(blockHeight))

	parentTx := makeThreeOutputTx(t)
	parentHash := parentTx.TxIDChainHash()

	_, err := store.Create(ctx, parentTx, blockHeight)
	require.NoError(t, err)

	// Verify the packed columns are populated correctly in txs.
	var utxoHashes []byte
	var spendableBits []byte
	var outCount, spendableCount int32
	err = store.pool.QueryRow(ctx,
		`SELECT utxo_hashes, out_spendables, out_count, spendable_count FROM txs WHERE hash = $1`, parentHash[:],
	).Scan(&utxoHashes, &spendableBits, &outCount, &spendableCount)
	require.NoError(t, err)
	require.Len(t, utxoHashes, 3*16, "3 outputs → 48-byte flat utxo_hashes (16-byte prefix stride)")
	require.Equal(t, int32(3), outCount, "3 outputs → out_count 3")
	require.Equal(t, int32(2), spendableCount, "OP_RETURN excluded → spendable_count 2")
	outSpendables := unpackBitmap(spendableBits, int(outCount))
	require.Len(t, outSpendables, 3)
	require.True(t, outSpendables[0], "vout0 (P2PKH) should be spendable")
	require.False(t, outSpendables[1], "vout1 (OP_RETURN zero-value) should NOT be spendable")
	require.True(t, outSpendables[2], "vout2 (P2PKH) should be spendable")

	// Spend vout0 — must succeed.
	spendTx0 := getSpendingTx(t, parentTx, 0)
	_, err = store.Create(ctx, spendTx0, blockHeight)
	require.NoError(t, err)
	spends0, err := store.Spend(ctx, spendTx0, blockHeight+1)
	require.NoError(t, err)
	require.Len(t, spends0, 1, "spending vout0 should produce 1 spend")

	// Spend vout2 — must succeed (catches off-by-one: existing tests only spend vout0).
	spendTx2 := getSpendingTx(t, parentTx, 2)
	_, err = store.Create(ctx, spendTx2, blockHeight)
	require.NoError(t, err)
	spends2, err := store.Spend(ctx, spendTx2, blockHeight+1)
	require.NoError(t, err)
	require.Len(t, spends2, 1, "spending vout2 should produce 1 spend (off-by-one check)")

	// Attempt to spend vout3 (OOB) — must return a TxNotFound-class error.
	utxoHashes3, err := utxo.GetUtxoHashes(parentTx, parentHash)
	require.NoError(t, err)
	// Use vout0's UTXO hash as a stand-in for the nonexistent vout3.
	oobSpend := &utxo.Spend{
		TxID:         parentHash,
		Vout:         3,
		UTXOHash:     utxoHashes3[0],
		SpendingData: spends0[0].SpendingData,
	}
	oobSpendTx := bt.NewTx()
	_ = oobSpendTx.From(parentHash.String(), 3, "76a91488ac", 1000)
	_ = oobSpendTx.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500)
	oobSpendTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x30, 0x44})

	_, err = store.Spend(ctx, oobSpendTx, blockHeight+1)
	require.Error(t, err, "spending OOB index vout3 must return an error")
	require.True(t, errors.Is(err, errors.ErrTxNotFound),
		"OOB spend should return ErrTxNotFound, got: %v", err)
	_ = oobSpend // suppress unused warning

	// Double-spend of vout0 with DIFFERENT spending data → ErrSpent.
	spendTxDup := getSpendingTx(t, parentTx, 0)
	_, err = store.Create(ctx, spendTxDup, blockHeight)
	require.NoError(t, err)
	_, err = store.Spend(ctx, spendTxDup, blockHeight+1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrSpent),
		"double-spend of vout0 with different spending data must return ErrSpent, got: %v", err)

	// Idempotent re-spend of vout0 with SAME tx → success.
	_, err = store.Spend(ctx, spendTx0, blockHeight+1)
	require.NoError(t, err, "idempotent re-spend of vout0 with same spending data must succeed")
}

// Ensure unused imports are used.
var _ = meta.Data{}
var _ = rand.Uint64

// ---------------------------------------------------------------------------
// Task 6: Clock-immunity GUCs on maintenance pool
// ---------------------------------------------------------------------------

// newTestStoreWithMaintPool creates a store with a maintenance pool enabled
// (PostgresMaintenancePoolConns = 2), used to test maintenance pool GUC pinning.
func newTestStoreWithMaintPool(t *testing.T) *Store {
	t.Helper()

	ctx := context.Background()

	// Clean slate -- drop all tables
	pool, err := pgxpool.New(ctx, testDSN)
	if err != nil {
		t.Skipf("Skipping: cannot connect to postgres: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("Skipping: cannot connect to postgres: %v", err)
	}
	_, _ = pool.Exec(ctx, `
		DROP FUNCTION IF EXISTS process_batch(BIGINT) CASCADE;
		DROP FUNCTION IF EXISTS process_delete_at_height(BIGINT) CASCADE;
		DROP PROCEDURE IF EXISTS materialize_loop() CASCADE;
		DROP PROCEDURE IF EXISTS dah_sweep_batch(BIGINT, INT) CASCADE;
		DROP PROCEDURE IF EXISTS dah_sweep_batch(INT, BIGINT, INT) CASCADE;
		DROP TABLE IF EXISTS conflicting_children, block_ids, spends, outputs, inputs,
			tx_state, transactions, txs, txs_raw, dah_watermark, dah_part_watermark, dah_sweep_control, dah_reconcile_cursor,
			dah_dirty_parents,
			create_queue, input_queue, output_queue, spend_queue, mined_queue,
			batch_notifications CASCADE;
		DROP TABLE IF EXISTS pending_unmined CASCADE;
		DROP INDEX IF EXISTS px_pu_backfill_marker;
	`)
	pool.Close()

	storeURL, err := url.Parse(testDSN)
	require.NoError(t, err)
	storeURL.Scheme = "postgres"

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.DBTimeout = 30 * time.Second
	tSettings.UtxoStore.PostgresMaintenancePoolConns = 2 // Enable maintenance pool for this test

	logger := ulogger.TestLogger{}
	store, err := New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)

	t.Cleanup(func() {
		store.Stop()
	})

	return store
}

// storeMainPoolRuntimeParam returns the value of a RuntimeParam on the main pool,
// or (_, false) if not set.
func storeMainPoolRuntimeParam(s *Store, key string) (string, bool) {
	if s.pool == nil || s.pool.Config() == nil || s.pool.Config().ConnConfig.RuntimeParams == nil {
		return "", false
	}
	val, ok := s.pool.Config().ConnConfig.RuntimeParams[key]
	return val, ok
}

func TestMaintPoolPinsClockImmunityGUCs(t *testing.T) {
	store := newTestStoreWithMaintPool(t)
	ctx := context.Background()

	// Verify maintenance pool has statement_timeout = 0
	var v string
	err := store.maint().QueryRow(ctx, `SHOW statement_timeout`).Scan(&v)
	require.NoError(t, err)
	require.Equal(t, "0", v, "maintenance pool should have statement_timeout = 0")

	// Verify maintenance pool has lock_timeout = 0
	err = store.maint().QueryRow(ctx, `SHOW lock_timeout`).Scan(&v)
	require.NoError(t, err)
	require.Equal(t, "0", v, "maintenance pool should have lock_timeout = 0")

	// Verify maintenance pool has idle_in_transaction_session_timeout = 0
	err = store.maint().QueryRow(ctx, `SHOW idle_in_transaction_session_timeout`).Scan(&v)
	require.NoError(t, err)
	require.Equal(t, "0", v, "maintenance pool should have idle_in_transaction_session_timeout = 0")

	// Verify that the RuntimeParams are actually set (present in the map) on the maintenance pool config.
	maintConfig := store.maintPool.Config()
	runtimeParams := maintConfig.ConnConfig.RuntimeParams
	require.NotNil(t, runtimeParams, "RuntimeParams should exist on maintenance pool")

	_, hasStatement := runtimeParams["statement_timeout"]
	require.True(t, hasStatement, "statement_timeout must be present in maintenance pool RuntimeParams")

	_, hasLock := runtimeParams["lock_timeout"]
	require.True(t, hasLock, "lock_timeout must be present in maintenance pool RuntimeParams")

	_, hasIdle := runtimeParams["idle_in_transaction_session_timeout"]
	require.True(t, hasIdle, "idle_in_transaction_session_timeout must be present in maintenance pool RuntimeParams")

	// Verify main pool does NOT have statement_timeout pinned (only synchronous_commit is set).
	_, mainHasIt := storeMainPoolRuntimeParam(store, "statement_timeout")
	require.False(t, mainHasIt, "main pool must NOT have statement_timeout pinned")
}
