// Package sql provides a SQL-based implementation of the UTXO store interface.
// It supports both PostgreSQL and SQLite backends with automatic schema creation
// and migration.
//
// # Features
//
//   - Full UTXO lifecycle management (create, spend, unspend)
//   - Transaction metadata storage
//   - Input/output tracking
//   - Block height and median time tracking
//   - Optional UTXO expiration with automatic cleanup
//   - Prometheus metrics integration
//   - Support for the alert system (freeze/unfreeze/reassign UTXOs)
//
// # Usage
//
//	store, err := sql.New(ctx, logger, settings, &url.URL{
//	    Scheme: "postgres",
//	    Host:   "localhost:5432",
//	    User:   "user",
//	    Path:   "dbname",
//	    RawQuery: "expiration=1h",
//	})
//
// # Database Schema
//
// The store uses the following tables:
//   - transactions: Stores base transaction data
//   - inputs: Stores transaction inputs with previous output references
//   - outputs: Stores transaction outputs and UTXO state
//   - block_ids: Stores which blocks a transaction appears in
//
// # Metrics
//
// The following Prometheus metrics are exposed:
//   - teranode_sql_utxo_get: Number of UTXO retrieval operations
//   - teranode_sql_utxo_spend: Number of UTXO spend operations
//   - teranode_sql_utxo_reset: Number of UTXO reset operations
//   - teranode_sql_utxo_delete: Number of UTXO delete operations
//   - teranode_sql_utxo_errors: Number of errors by function and type
package sql

import (
	"context"
	"database/sql"
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	spendpkg "github.com/bitcoin-sv/teranode/stores/utxo/spend"
	"github.com/bitcoin-sv/teranode/stores/utxo/tests"
	utxo2 "github.com/bitcoin-sv/teranode/test/longtest/stores/utxo"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setup(ctx context.Context, t *testing.T) (*Store, *bt.Tx) {
	initPrometheusMetrics()

	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings()
	tSettings.UtxoStore.DBTimeout = 30 * time.Second

	tx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)

	// storeUrl, err := url.Parse("postgres://teranode:teranode@localhost:5432/teranode")
	// storeUrl, err := url.Parse("sqlite:///test")
	utxoStoreURL, err := url.Parse("sqlitememory:///test")

	require.NoError(t, err)

	// Create the store
	utxoStore, err := New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	// Delete the tx so the tests can run cleanly...
	err = utxoStore.Delete(ctx, tx.TxIDChainHash())
	require.NoError(t, err)

	return utxoStore, tx
}

func TestCreate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, tx := setup(ctx, t)

	meta, err := utxoStore.Create(ctx, tx, 0)
	require.NoError(t, err)

	assert.Equal(t, uint64(259), meta.SizeInBytes)
}

func TestCreateDuplicate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, tx := setup(ctx, t)

	meta, err := utxoStore.Create(ctx, tx, 0)
	require.NoError(t, err)

	assert.Equal(t, uint64(259), meta.SizeInBytes)

	_, err = utxoStore.Create(ctx, tx, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxExists))
}

func TestGet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, tx := setup(ctx, t)

	blockHeight := uint32(12345)
	_, err := utxoStore.Create(ctx, tx, blockHeight)
	require.NoError(t, err)

	meta, err := utxoStore.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)

	assert.Equal(t, uint64(0), meta.Fee)
	assert.Equal(t, uint32(0), meta.LockTime)
	assert.False(t, meta.IsCoinbase)
	assert.Equal(t, uint64(259), meta.SizeInBytes)
	assert.Len(t, meta.TxInpoints.ParentTxHashes, 1)
	assert.Len(t, meta.Tx.Inputs, 1)
	assert.Len(t, meta.Tx.Outputs, 2)
	assert.Equal(t, uint64(50e8), meta.Tx.Inputs[0].PreviousTxSatoshis)
	assert.Len(t, meta.BlockIDs, 0)
	assert.Equal(t, "fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4", tx.TxIDChainHash().String())
	// Verify that UnminedSince is correctly retrieved for unmined transactions
	assert.Equal(t, blockHeight, meta.UnminedSince)
}

func TestGetMeta(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, tx := setup(ctx, t)

	blockHeight := uint32(54321)
	_, err := utxoStore.Create(ctx, tx, blockHeight)
	require.NoError(t, err)

	meta, err := utxoStore.GetMeta(ctx, tx.TxIDChainHash())
	require.NoError(t, err)

	assert.Nil(t, meta.Tx)
	// Verify that UnminedSince is correctly retrieved in GetMeta for unmined transactions
	assert.Equal(t, blockHeight, meta.UnminedSince)
}

func TestGetBlockIDs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, tx := setup(ctx, t)

	_, err := utxoStore.Create(ctx, tx, 0, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 123, SubtreeIdx: 1},
		utxo.MinedBlockInfo{BlockID: 2, BlockHeight: 124, SubtreeIdx: 2},
		utxo.MinedBlockInfo{BlockID: 3, BlockHeight: 125, SubtreeIdx: 3},
	))
	require.NoError(t, err)

	meta, err := utxoStore.GetMeta(ctx, tx.TxIDChainHash())
	require.NoError(t, err)

	assert.Len(t, meta.BlockIDs, 3)
}

func TestDelete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, tx := setup(ctx, t)

	_, err := utxoStore.Create(ctx, tx, 0)
	require.NoError(t, err)

	err = utxoStore.Delete(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
}

func TestSpend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, tx := setup(ctx, t)

	spendTx := utxo2.GetSpendingTx(tx, 0)

	spendTx2 := utxo2.GetSpendingTx(tx, 0)

	_, err := utxoStore.Create(ctx, tx, 0)
	require.NoError(t, err)

	_, err = utxoStore.Spend(ctx, spendTx)
	require.NoError(t, err)

	// Spend again with the same spendingTxID
	_, err = utxoStore.Spend(ctx, spendTx)
	require.NoError(t, err)

	_, err = utxoStore.Spend(ctx, spendTx2)
	require.Error(t, err)
}

func TestUnspend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, tx := setup(ctx, t)

	spendTx := utxo2.GetSpendingTx(tx, 0)

	_, err := utxoStore.Create(ctx, tx, 0)
	require.NoError(t, err)

	utxohash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	require.NoError(t, err)

	test1Hash := chainhash.HashH([]byte("test1"))
	spendingData1 := spendpkg.NewSpendingData(&test1Hash, 1)

	spend := &utxo.Spend{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxohash,
		SpendingData: spendingData1,
	}

	_, err = utxoStore.Spend(ctx, spendTx)
	require.NoError(t, err)

	// Unspend the utxo
	err = utxoStore.Unspend(ctx, []*utxo.Spend{spend})
	require.NoError(t, err)

	// Spend again with a different spendingTxID
	test2Hash := chainhash.HashH([]byte("test2"))
	spendingData2 := spendpkg.NewSpendingData(&test2Hash, 2)
	spend.SpendingData = spendingData2

	_, err = utxoStore.Spend(ctx, spendTx)
	require.NoError(t, err)
}

func TestGetSpend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, tx := setup(ctx, t)

	_, err := utxoStore.Create(ctx, tx, 0)
	require.NoError(t, err)

	utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	require.NoError(t, err)

	spend := &utxo.Spend{
		TxID:     tx.TxIDChainHash(),
		Vout:     0,
		UTXOHash: utxoHash,
	}

	res, err := utxoStore.GetSpend(ctx, spend)
	require.NoError(t, err)

	assert.Equal(t, int(utxo.Status_OK), res.Status)
}

func TestSetMinedMulti(t *testing.T) {
	t.Run("single block", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		utxoStore, tx := setup(ctx, t)

		_, err := utxoStore.Create(ctx, tx, 0)
		require.NoError(t, err)

		err = utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{
			BlockID:     1,
			BlockHeight: 1,
			SubtreeIdx:  0,
		})
		require.NoError(t, err)

		meta, err := utxoStore.GetMeta(ctx, tx.TxIDChainHash())
		require.NoError(t, err)

		assert.Len(t, meta.BlockIDs, 1)
		assert.Equal(t, uint32(1), meta.BlockIDs[0])
	})

	t.Run("single block - with tx set to unspendable", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		utxoStore, tx := setup(ctx, t)

		_, err := utxoStore.Create(ctx, tx, 0)
		require.NoError(t, err)

		err = utxoStore.SetUnspendable(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, true)
		require.NoError(t, err)

		meta, err := utxoStore.GetMeta(ctx, tx.TxIDChainHash())
		require.NoError(t, err)
		assert.True(t, meta.Unspendable)

		err = utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{
			BlockID:     1,
			BlockHeight: 1,
			SubtreeIdx:  0,
		})
		require.NoError(t, err)

		meta, err = utxoStore.GetMeta(ctx, tx.TxIDChainHash())
		require.NoError(t, err)

		assert.Len(t, meta.BlockIDs, 1)
		assert.Equal(t, uint32(1), meta.BlockIDs[0])
		assert.False(t, meta.Unspendable)
	})
}

func TestBatchDecorate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, tx := setup(ctx, t)

	_, err := utxoStore.Create(ctx, tx, 0)
	require.NoError(t, err)

	unresolved := utxo.UnresolvedMetaData{
		Hash: *tx.TxIDChainHash(),
		Idx:  0,
	}

	err = utxoStore.BatchDecorate(ctx, []*utxo.UnresolvedMetaData{&unresolved})
	require.NoError(t, err)

	assert.Equal(t, uint64(0), unresolved.Data.Fee)
	assert.Equal(t, uint32(0), unresolved.Data.LockTime)
	assert.False(t, unresolved.Data.IsCoinbase)
	assert.Equal(t, uint64(259), unresolved.Data.SizeInBytes)
	assert.Len(t, unresolved.Data.TxInpoints.ParentTxHashes, 1)
	assert.Len(t, unresolved.Data.Tx.Inputs, 1)
	assert.Len(t, unresolved.Data.Tx.Outputs, 2)
	assert.Equal(t, uint64(50e8), unresolved.Data.Tx.Inputs[0].PreviousTxSatoshis)
	assert.Len(t, unresolved.Data.BlockIDs, 0)
	assert.Equal(t, "fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4", unresolved.Data.Tx.TxIDChainHash().String())
}

func TestPreviousOutputsDecorate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, tx := setup(ctx, t)

	// The test transaction from setup() already has inputs that need decorating
	// Create a parent transaction that the test tx references
	parentTx, err := bt.NewTxFromString("010000000000000000ef012935b177236ec1cb75cd9fba86d84acac9d76ced9c1b22ba8de4cd2de85a8393000000004948304502200f653627aff050093a83dabc12a2a9b627041d424f2eb18849a2d587f1acd38f022100a23f94acd94a4d24049140d5fbe12448a880fd8f8c1c2b4141f83bef2be409be01ffffffff00f2052a01000000434104ed83808a903a7e25be91349815f5d545f0c9dbec60b8ea914a6d6cbe9f830628039641231e2dbc1c0ca809f13405eb01f3a06614717f7859b788bd1305d9a3f2ac0100f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac00000000")
	require.NoError(t, err)

	_, err = utxoStore.Create(ctx, parentTx, 0)
	require.NoError(t, err)

	err = utxoStore.PreviousOutputsDecorate(ctx, tx)
	require.NoError(t, err)

	assert.Equal(t, uint64(5_000_000_000), tx.Inputs[0].PreviousTxSatoshis)
	assert.Len(t, *tx.Inputs[0].PreviousTxScript, 25)
}

func TestCreateCoinbase(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, _ := setup(ctx, t)

	// Coinbase from block 500,000
	coinbaseTx, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff580320a107152f5669614254432f48656c6c6f20576f726c64212f2cfabe6d6dbcbb1b0222e1aeebaca2a9c905bb23a3ad0302898ec600a9033a87ec1645a446010000000000000010f829ba0b13a84def80c389cde9840000ffffffff0174fdaf4a000000001976a914f1c075a01882ae0972f95d3a4177c86c852b7d9188ac00000000")
	require.NoError(t, err)

	err = utxoStore.Delete(ctx, coinbaseTx.TxIDChainHash())
	require.NoError(t, err)

	meta, err := utxoStore.Create(ctx, coinbaseTx, 100)
	require.NoError(t, err)

	assert.Equal(t, uint64(1253047668), meta.Fee)
	assert.Equal(t, uint32(0), meta.LockTime)
	assert.True(t, meta.IsCoinbase)
	assert.Equal(t, uint64(173), meta.SizeInBytes)
	assert.Len(t, meta.TxInpoints.ParentTxHashes, 0)
	assert.Len(t, meta.Tx.Inputs, 1)
	assert.Len(t, meta.Tx.Outputs, 1)
	assert.Len(t, meta.BlockIDs, 0)
	assert.Equal(t, "5ebaa53d24c8246c439ccd9f142cbe93fc59582e7013733954120e9baab201df", coinbaseTx.TxIDChainHash().String())
}

func TestTombstoneAfterSpendAndUnspend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings()
	tSettings.UtxoStore.DBTimeout = 30 * time.Second
	tSettings.UtxoStore.BlockHeightRetention = 1 // Set low retention for this test

	tx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a18700000000" +
		"8c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5" +
		"ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c933158" +
		"6c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac02" +
		"00e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c" +
		"2f6b52de3d7c88ac00000000")
	require.NoError(t, err)

	utxoStoreURL, err := url.Parse("sqlitememory:///test_tombstone")
	require.NoError(t, err)

	utxoStore, err := New(ctx, logger, tSettings, utxoStoreURL)
	require.NoError(t, err)

	err = utxoStore.Delete(ctx, tx.TxIDChainHash())
	require.NoError(t, err)

	// Get the cleanup service (singleton)
	cleanupService, err := utxoStore.GetCleanupService()
	require.NoError(t, err)

	cleanupService.Start(ctx)

	// Part 1: Test tombstone after spend
	_, err = utxoStore.Create(ctx, tx, 0)
	require.NoError(t, err)

	// Create a spending transaction that spends outputs 0 and 1
	spendTx01 := utxo2.GetSpendingTx(tx, 0, 1)

	// Spend the transaction
	_, err = utxoStore.Spend(ctx, spendTx01)
	require.NoError(t, err)

	doneCh := make(chan string, 1)

	err = cleanupService.UpdateBlockHeight(1, doneCh)
	require.NoError(t, err)

	select {
	case <-doneCh:
		// Job completed successfully
	case <-time.After(5 * time.Second):
		require.Fail(t, "cleanup job did not complete within 5 seconds")
	}

	// Verify the transaction is now gone (tombstoned)
	_, err = utxoStore.Get(ctx, tx.TxIDChainHash())
	require.Error(t, err)
	assert.True(t, errors.Is(err, errors.ErrTxNotFound))

	// Part 2: Test tombstone after unspend
	err = utxoStore.SetBlockHeight(2)
	require.NoError(t, err)

	err = utxoStore.Delete(ctx, tx.TxIDChainHash())
	require.NoError(t, err)

	_, err = utxoStore.Create(ctx, tx, 0)
	require.NoError(t, err)

	// Calculate the UTXO hash for output 0
	utxohash0, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	require.NoError(t, err)

	spendingData := spendpkg.NewSpendingData(spendTx01.TxIDChainHash(), 1)

	// Create a spend record
	spend0 := &utxo.Spend{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxohash0,
		SpendingData: spendingData,
	}

	// Spend the transaction
	_, err = utxoStore.Spend(ctx, spendTx01)
	require.NoError(t, err)

	// Unspend output 0
	err = utxoStore.Unspend(ctx, []*utxo.Spend{spend0})
	require.NoError(t, err)

	// Run cleanup for block height 1
	doneCh = make(chan string, 1)

	err = cleanupService.UpdateBlockHeight(2, doneCh)
	require.NoError(t, err)

	select {
	case <-doneCh:
		// Job completed successfully
	case <-time.After(5 * time.Second):
		require.Fail(t, "cleanup job did not complete within 5 seconds")
	}

	// Verify the transaction is still there (not tombstoned)
	_, err = utxoStore.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)

}

func Test_SmokeTests(t *testing.T) {
	ctx := context.Background()

	t.Run("sql store", func(t *testing.T) {
		db, _ := setup(ctx, t)

		err := db.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.Store(t, db)
	})

	t.Run("sql spend", func(t *testing.T) {
		db, _ := setup(ctx, t)

		err := db.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.Spend(t, db)
	})

	t.Run("sql reset", func(t *testing.T) {
		db, _ := setup(ctx, t)

		err := db.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.Restore(t, db)
	})

	t.Run("sql freeze", func(t *testing.T) {
		db, _ := setup(ctx, t)

		err := db.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.Freeze(t, db)
	})

	t.Run("sql reassign", func(t *testing.T) {
		db, _ := setup(ctx, t)

		err := db.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.ReAssign(t, db)
	})

	t.Run("set mined", func(t *testing.T) {
		db, _ := setup(ctx, t)

		err := db.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.SetMined(t, db)
	})

	t.Run("sql conflicting tx", func(t *testing.T) {
		db, _ := setup(ctx, t)

		err := db.Delete(ctx, tests.TXHash)
		require.NoError(t, err)

		tests.Conflicting(t, db)
	})
}

func TestSetTTL(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	var (
		transactionID   int
		tombstoneMillis *int64
	)

	err = store.db.QueryRowContext(ctx, "SELECT id FROM transactions WHERE hash = $1", tx.TxIDChainHash()[:]).Scan(&transactionID)
	require.NoError(t, err)

	err = store.db.QueryRowContext(ctx, "SELECT delete_at_height FROM transactions WHERE hash = $1", tx.TxIDChainHash()[:]).Scan(&tombstoneMillis)
	require.NoError(t, err)

	assert.Nil(t, tombstoneMillis)

	txn, err := store.db.Begin()
	require.NoError(t, err)

	defer func() {
		_ = txn.Rollback()
	}()

	err = store.setDAH(ctx, txn, transactionID)
	require.NoError(t, err)

	err = txn.QueryRowContext(ctx, "SELECT delete_at_height FROM transactions WHERE hash = $1", tx.TxIDChainHash()[:]).Scan(&tombstoneMillis)
	require.NoError(t, err)

	assert.Nil(t, tombstoneMillis)

	// update all outputs to be spent
	_, err = txn.ExecContext(ctx, "UPDATE outputs SET spending_data = $1 WHERE transaction_id = $2", spendpkg.NewSpendingData(tx.TxIDChainHash(), 1).Bytes(), transactionID)
	require.NoError(t, err)

	err = store.setDAH(ctx, txn, transactionID)
	require.NoError(t, err)

	err = txn.QueryRowContext(ctx, "SELECT delete_at_height FROM transactions WHERE hash = $1", tx.TxIDChainHash()[:]).Scan(&tombstoneMillis)
	require.NoError(t, err)

	assert.NotNil(t, tombstoneMillis)

	// unset one of the outputs to be unspent
	_, err = txn.ExecContext(ctx, "UPDATE outputs SET spending_data = NULL WHERE transaction_id = $1 AND idx = 0", transactionID)
	require.NoError(t, err)

	err = store.setDAH(ctx, txn, transactionID)
	require.NoError(t, err)

	err = txn.QueryRowContext(ctx, "SELECT delete_at_height FROM transactions WHERE hash = $1", tx.TxIDChainHash()[:]).Scan(&tombstoneMillis)
	require.NoError(t, err)

	assert.Nil(t, tombstoneMillis)

	// mark the tx as conflicting, should set a tombstone
	_, err = txn.ExecContext(ctx, "UPDATE transactions SET conflicting = true WHERE id = $1", transactionID)
	require.NoError(t, err)

	err = store.setDAH(ctx, txn, transactionID)
	require.NoError(t, err)

	err = txn.QueryRowContext(ctx, "SELECT delete_at_height FROM transactions WHERE hash = $1", tx.TxIDChainHash()[:]).Scan(&tombstoneMillis)
	require.NoError(t, err)

	assert.NotNil(t, tombstoneMillis)
}

func TestUnmined(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	t.Run("check_empty_store", func(t *testing.T) {
		count := 0

		err := store.db.QueryRowContext(ctx, "SELECT COUNT(1) FROM transactions WHERE unmined_since IS NOT NULL").Scan(&count)
		require.NoError(t, err)

		assert.Equal(t, 0, count)
	})

	t.Run("check_not_mined_tx", func(t *testing.T) {
		_, err := store.Create(ctx, tx, 0)
		require.NoError(t, err)

		txMined := tx.Clone()
		txMined.Version++

		_, err = store.Create(ctx, txMined, 0, utxo.WithMinedBlockInfo(
			utxo.MinedBlockInfo{
				BlockID:     1,
				BlockHeight: 1,
				SubtreeIdx:  1,
			},
		))
		require.NoError(t, err)

		count := 0

		err = store.db.QueryRowContext(ctx, "SELECT COUNT(1) FROM transactions WHERE unmined_since IS NOT NULL").Scan(&count)
		require.NoError(t, err)

		assert.Equal(t, 1, count)
	})
}

func TestPreserveParentsOfOldUnminedTransactions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	// Test case 1: No parent preservation needed when blockHeight <= retention
	count, err := utxo.PreserveParentsOfOldUnminedTransactions(ctx, store, 5, store.settings, store.logger)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Test case 2: Create unmined transaction and verify unmined_since
	currentHeight := uint32(100)
	_, err = store.Create(ctx, tx, currentHeight)
	require.NoError(t, err)

	// Verify the transaction has unmined_since set
	var unminedSince sql.NullInt64
	err = store.db.QueryRowContext(ctx, "SELECT unmined_since FROM transactions WHERE hash = $1", tx.TxIDChainHash()[:]).Scan(&unminedSince)
	require.NoError(t, err)
	require.True(t, unminedSince.Valid)
	assert.Equal(t, int64(currentHeight), unminedSince.Int64)

	// Test case 3: Transaction should not have parents preserved if it's not old enough
	// Use the actual retention setting from the store
	retention := store.settings.UtxoStore.UnminedTxRetention
	cleanupHeight := currentHeight + retention - 1 // Just within retention period
	count, err = utxo.PreserveParentsOfOldUnminedTransactions(ctx, store, cleanupHeight, store.settings, store.logger)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Verify transaction is still there
	var txCount int
	err = store.db.QueryRowContext(ctx, "SELECT COUNT(1) FROM transactions WHERE hash = $1", tx.TxIDChainHash()[:]).Scan(&txCount)
	require.NoError(t, err)
	assert.Equal(t, 1, txCount)

	// Test case 4: Transaction should have its parents preserved when it's old enough
	// Set a preservation height that exceeds retention period
	cleanupHeight = currentHeight + retention + 1 // Beyond retention period
	count, err = utxo.PreserveParentsOfOldUnminedTransactions(ctx, store, cleanupHeight, store.settings, store.logger)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify transaction is still there (NOT deleted with the new behavior)
	err = store.db.QueryRowContext(ctx, "SELECT COUNT(1) FROM transactions WHERE hash = $1", tx.TxIDChainHash()[:]).Scan(&txCount)
	require.NoError(t, err)
	assert.Equal(t, 1, txCount) // Should still be 1, not deleted
}
