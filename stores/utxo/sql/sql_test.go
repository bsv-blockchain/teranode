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
	"net/url"
	"testing"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/stores/utxo/tests"
	utxo2 "github.com/bitcoin-sv/teranode/test/stores/utxo"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setup(ctx context.Context, t *testing.T) (*Store, *bt.Tx) {
	initPrometheusMetrics()

	logger := ulogger.TestLogger{}

	tSettings := test.CreateBaseTestSettings()
	tSettings.UtxoStore.DBTimeout = 30000 * time.Millisecond

	tx, err := bt.NewTxFromString("010000000000000000ef01032e38e9c0a84c6046d687d10556dcacc41d275ec55fc00779ac88fdf357a187000000008c493046022100c352d3dd993a981beba4a63ad15c209275ca9470abfcd57da93b58e4eb5dce82022100840792bc1f456062819f15d33ee7055cf7b5ee1af1ebcc6028d9cdb1c3af7748014104f46db5e9d61a9dc27b8d64ad23e7383a4e6ca164593c2527c038c0857eb67ee8e825dca65046b82c9331586c82e0fd1f633f25f87c161bc6f8a630121df2b3d3ffffffff00f2052a010000001976a91471d7dd96d9edda09180fe9d57a477b5acc9cad1188ac0200e32321000000001976a914c398efa9c392ba6013c5e04ee729755ef7f58b3288ac000fe208010000001976a914948c765a6914d43f2a7ac177da2c2f6b52de3d7c88ac00000000")
	require.NoError(t, err)

	// storeUrl, err := url.Parse("postgres://teranode:teranode@localhost:5432/teranode?expiration=1s")
	// storeUrl, err := url.Parse("sqlite:///test?expiration=1s")
	storeURL, err := url.Parse("sqlite:///test?expiration=1s")

	require.NoError(t, err)

	store, err := New(ctx, logger, tSettings, storeURL)
	require.NoError(t, err)

	// Delete the tx so the tests can run cleanly...
	err = store.Delete(ctx, tx.TxIDChainHash())
	require.NoError(t, err)

	return store, tx
}

func TestCreate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	meta, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	assert.Equal(t, uint64(259), meta.SizeInBytes)
}

func TestCreateDuplicate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	meta, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	assert.Equal(t, uint64(259), meta.SizeInBytes)

	_, err = store.Create(ctx, tx, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrTxExists))
}

func TestGet(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	meta, err := store.Get(ctx, tx.TxIDChainHash())
	require.NoError(t, err)

	assert.Equal(t, uint64(0), meta.Fee)
	assert.Equal(t, uint32(0), meta.LockTime)
	assert.False(t, meta.IsCoinbase)
	assert.Equal(t, uint64(259), meta.SizeInBytes)
	assert.Len(t, meta.ParentTxHashes, 1)
	assert.Len(t, meta.Tx.Inputs, 1)
	assert.Len(t, meta.Tx.Outputs, 2)
	assert.Equal(t, uint64(50e8), meta.Tx.Inputs[0].PreviousTxSatoshis)
	assert.Len(t, meta.BlockIDs, 0)
	assert.Equal(t, "fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4", tx.TxIDChainHash().String())
}

func TestGetMeta(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	meta, err := store.GetMeta(ctx, tx.TxIDChainHash())
	require.NoError(t, err)

	assert.Nil(t, meta.Tx)
}

func TestGetBlockIDs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	_, err := store.Create(ctx, tx, 0, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 123, SubtreeIdx: 1},
		utxo.MinedBlockInfo{BlockID: 2, BlockHeight: 124, SubtreeIdx: 2},
		utxo.MinedBlockInfo{BlockID: 3, BlockHeight: 125, SubtreeIdx: 3},
	))
	require.NoError(t, err)

	meta, err := store.GetMeta(ctx, tx.TxIDChainHash())
	require.NoError(t, err)

	assert.Len(t, meta.BlockIDs, 3)
}

func TestDelete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	err = store.Delete(ctx, tx.TxIDChainHash())
	require.NoError(t, err)
}

func TestSpend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	spendTx := utxo2.GetSpendingTx(tx, 0)

	spendTx2 := utxo2.GetSpendingTx(tx, 0)

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	_, err = store.Spend(ctx, spendTx)
	require.NoError(t, err)

	// Spend again with the same spendingTxID
	_, err = store.Spend(ctx, spendTx)
	require.NoError(t, err)

	_, err = store.Spend(ctx, spendTx2)
	require.Error(t, err)
}

func TestUnspend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	spendTx := utxo2.GetSpendingTx(tx, 0)

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	utxohash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	require.NoError(t, err)

	spendingTxID1 := chainhash.HashH([]byte("test1"))

	spend := &utxo.Spend{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxohash,
		SpendingTxID: &spendingTxID1,
	}

	_, err = store.Spend(ctx, spendTx)
	require.NoError(t, err)

	// Unspend the utxo
	err = store.Unspend(ctx, []*utxo.Spend{spend})
	require.NoError(t, err)

	// Spend again with a different spendingTxID
	spendingTxID2 := chainhash.HashH([]byte("test2"))
	spend.SpendingTxID = &spendingTxID2

	_, err = store.Spend(ctx, spendTx)
	require.NoError(t, err)
}

func TestGetSpend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	require.NoError(t, err)

	spend := &utxo.Spend{
		TxID:     tx.TxIDChainHash(),
		Vout:     0,
		UTXOHash: utxoHash,
	}

	res, err := store.GetSpend(ctx, spend)
	require.NoError(t, err)

	assert.Equal(t, int(utxo.Status_OK), res.Status)
}

func TestSetMinedMulti(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	err = store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{
		BlockID:     1,
		BlockHeight: 1,
		SubtreeIdx:  0,
	})
	require.NoError(t, err)

	meta, err := store.Get(ctx, tx.TxIDChainHash(), []string{"blockIDs", "blockHeights", "subtreeIdxs"})
	require.NoError(t, err)

	assert.Len(t, meta.BlockIDs, 1)
	assert.Equal(t, uint32(1), meta.BlockIDs[0])
	assert.Len(t, meta.BlockHeights, 1)
	assert.Equal(t, uint32(1), meta.BlockHeights[0])
	assert.Len(t, meta.SubtreeIdxs, 1)
	assert.Equal(t, 0, meta.SubtreeIdxs[0])
}

func TestBatchDecorate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	unresolved := utxo.UnresolvedMetaData{
		Hash: *tx.TxIDChainHash(),
		Idx:  0,
	}

	err = store.BatchDecorate(ctx, []*utxo.UnresolvedMetaData{&unresolved})
	require.NoError(t, err)

	assert.Equal(t, uint64(0), unresolved.Data.Fee)
	assert.Equal(t, uint32(0), unresolved.Data.LockTime)
	assert.False(t, unresolved.Data.IsCoinbase)
	assert.Equal(t, uint64(259), unresolved.Data.SizeInBytes)
	assert.Len(t, unresolved.Data.ParentTxHashes, 1)
	assert.Len(t, unresolved.Data.Tx.Inputs, 1)
	assert.Len(t, unresolved.Data.Tx.Outputs, 2)
	assert.Equal(t, uint64(50e8), unresolved.Data.Tx.Inputs[0].PreviousTxSatoshis)
	assert.Len(t, unresolved.Data.BlockIDs, 0)
	assert.Equal(t, "fff2525b8931402dd09222c50775608f75787bd2b87e56995a7bdd30f79702c4", unresolved.Data.Tx.TxIDChainHash().String())
}

func TestPreviousOutputsDecorate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	previousOutput := &meta.PreviousOutput{
		PreviousTxID: *tx.TxIDChainHash(),
		Vout:         0,
	}

	err = store.PreviousOutputsDecorate(ctx, []*meta.PreviousOutput{previousOutput})
	require.NoError(t, err)

	assert.Equal(t, uint64(556_000_000), previousOutput.Satoshis)
	assert.Len(t, previousOutput.LockingScript, 25)
}

func TestCreateCoinbase(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, _ := setup(ctx, t)

	// Coinbase from block 500,000
	coinbaseTx, err := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff580320a107152f5669614254432f48656c6c6f20576f726c64212f2cfabe6d6dbcbb1b0222e1aeebaca2a9c905bb23a3ad0302898ec600a9033a87ec1645a446010000000000000010f829ba0b13a84def80c389cde9840000ffffffff0174fdaf4a000000001976a914f1c075a01882ae0972f95d3a4177c86c852b7d9188ac00000000")
	require.NoError(t, err)

	err = store.Delete(ctx, coinbaseTx.TxIDChainHash())
	require.NoError(t, err)

	meta, err := store.Create(ctx, coinbaseTx, 100)
	require.NoError(t, err)

	assert.Equal(t, uint64(1253047668), meta.Fee)
	assert.Equal(t, uint32(0), meta.LockTime)
	assert.True(t, meta.IsCoinbase)
	assert.Equal(t, uint64(173), meta.SizeInBytes)
	assert.Len(t, meta.ParentTxHashes, 0)
	assert.Len(t, meta.Tx.Inputs, 1)
	assert.Len(t, meta.Tx.Outputs, 1)
	assert.Len(t, meta.BlockIDs, 0)
	assert.Equal(t, "5ebaa53d24c8246c439ccd9f142cbe93fc59582e7013733954120e9baab201df", coinbaseTx.TxIDChainHash().String())
}

func TestTombstoneAfterSpend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	spendTx01 := utxo2.GetSpendingTx(tx, 0, 1)

	_, err := store.Create(ctx, tx, 0)
	require.NoError(t, err)

	_, err = store.Spend(ctx, spendTx01)
	require.NoError(t, err)

	time.Sleep(1100 * time.Millisecond)

	err = deleteTombstoned(store.db)
	require.NoError(t, err)

	_, err = store.Get(ctx, tx.TxIDChainHash())
	require.Error(t, err)
	assert.True(t, errors.Is(err, errors.ErrTxNotFound))
}

func TestTombstoneAfterUnspend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, tx := setup(ctx, t)

	spendTx01 := utxo2.GetSpendingTx(tx, 0, 1)

	utxohash0, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), tx.Outputs[0], 0)
	require.NoError(t, err)

	spend0 := &utxo.Spend{
		TxID:         tx.TxIDChainHash(),
		Vout:         0,
		UTXOHash:     utxohash0,
		SpendingTxID: spendTx01.TxIDChainHash(),
	}

	_, err = store.Create(ctx, tx, 0)
	require.NoError(t, err)

	_, err = store.Spend(ctx, spendTx01)
	require.NoError(t, err)

	err = store.Unspend(ctx, []*utxo.Spend{spend0})
	require.NoError(t, err)

	time.Sleep(1100 * time.Millisecond)

	err = deleteTombstoned(store.db)
	require.NoError(t, err)

	_, err = store.Get(ctx, tx.TxIDChainHash())
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

		// make sure the parents of tx are inserted first, otherwise the conflicting check fails
		// because of the foreign key constraint
		q := `
		INSERT INTO transactions (
		  hash ,version ,lock_time ,fee ,size_in_bytes ,coinbase ,frozen ,conflicting
	  	) VALUES (
		  $1, $2, $3, $4, $5, $6, $7, $8
		)
		`

		for _, input := range tests.Tx.Inputs {
			_, _ = db.db.ExecContext(ctx, q, input.PreviousTxIDChainHash()[:], 1, 0, 0, 0, false, false, false)
		}

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

	err = store.db.QueryRowContext(ctx, "SELECT tombstone_millis FROM transactions WHERE hash = $1", tx.TxIDChainHash()[:]).Scan(&tombstoneMillis)
	require.NoError(t, err)

	assert.Nil(t, tombstoneMillis)

	txn, err := store.db.Begin()
	require.NoError(t, err)

	defer func() {
		_ = txn.Rollback()
	}()

	err = store.setTTL(ctx, txn, transactionID)
	require.NoError(t, err)

	err = txn.QueryRowContext(ctx, "SELECT tombstone_millis FROM transactions WHERE hash = $1", tx.TxIDChainHash()[:]).Scan(&tombstoneMillis)
	require.NoError(t, err)

	assert.Nil(t, tombstoneMillis)

	// update all outputs to be spent
	_, err = txn.ExecContext(ctx, "UPDATE outputs SET spending_transaction_id = 1 WHERE transaction_id = $1", transactionID)
	require.NoError(t, err)

	err = store.setTTL(ctx, txn, transactionID)
	require.NoError(t, err)

	err = txn.QueryRowContext(ctx, "SELECT tombstone_millis FROM transactions WHERE hash = $1", tx.TxIDChainHash()[:]).Scan(&tombstoneMillis)
	require.NoError(t, err)

	assert.NotNil(t, tombstoneMillis)

	// unset one of the outputs to be unspent
	_, err = txn.ExecContext(ctx, "UPDATE outputs SET spending_transaction_id = NULL WHERE transaction_id = $1 AND idx = 0", transactionID)
	require.NoError(t, err)

	err = store.setTTL(ctx, txn, transactionID)
	require.NoError(t, err)

	err = txn.QueryRowContext(ctx, "SELECT tombstone_millis FROM transactions WHERE hash = $1", tx.TxIDChainHash()[:]).Scan(&tombstoneMillis)
	require.NoError(t, err)

	assert.Nil(t, tombstoneMillis)

	// mark the tx as conflicting, should set a tombstone
	_, err = txn.ExecContext(ctx, "UPDATE transactions SET conflicting = true WHERE id = $1", transactionID)
	require.NoError(t, err)

	err = store.setTTL(ctx, txn, transactionID)
	require.NoError(t, err)

	err = txn.QueryRowContext(ctx, "SELECT tombstone_millis FROM transactions WHERE hash = $1", tx.TxIDChainHash()[:]).Scan(&tombstoneMillis)
	require.NoError(t, err)

	assert.NotNil(t, tombstoneMillis)
}
