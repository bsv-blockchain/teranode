package sql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_SetMinedMultiChunk_Success(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	hashes := CreateTestHashes(3)
	minedInfo := CreateTestMinedBlockInfo()

	SetupSetMinedMultiChunkMocks(mock, hashes, minedInfo)

	ctx := context.Background()
	result, err := store.setMinedMultiChunk(ctx, hashes, minedInfo)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 3, len(result))
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStore_SetMinedMultiChunk_ContextCancelled_BeforeStart(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, _ := CreateMockStore(logger)

	hashes := CreateTestHashes(2)
	minedInfo := CreateTestMinedBlockInfo()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// database/sql.BeginTx checks ctx.Err() before calling driver, so no
	// mock expectations are needed — the call returns context.Canceled immediately.
	result, err := store.setMinedMultiChunk(ctx, hashes, minedInfo)

	assert.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, result)
}

func TestStore_SetMinedMultiChunk_ContextCancelled_DuringExecution(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	hashes := CreateTestHashes(2)
	minedInfo := CreateTestMinedBlockInfo()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // clean up; cancellation effect is simulated by the mock error below

	// Allow BeginTx to succeed, then simulate cancellation during the first query
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT hash FROM transactions WHERE hash IN`).
		WillReturnError(context.Canceled)
	mock.ExpectRollback()

	result, err := store.setMinedMultiChunk(ctx, hashes, minedInfo)

	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestStore_SetMinedMultiChunk_BeginTransactionError(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	hashes := CreateTestHashes(2)
	minedInfo := CreateTestMinedBlockInfo()

	mock.ExpectBegin().WillReturnError(sql.ErrConnDone)

	ctx := context.Background()
	result, err := store.setMinedMultiChunk(ctx, hashes, minedInfo)

	assert.Error(t, err)
	assert.Equal(t, sql.ErrConnDone, err)
	assert.Nil(t, result)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStore_SetMinedMultiChunk_CheckExistsError(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	hashes := CreateTestHashes(2)
	minedInfo := CreateTestMinedBlockInfo()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT hash FROM transactions WHERE hash IN`).
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	ctx := context.Background()
	result, err := store.setMinedMultiChunk(ctx, hashes, minedInfo)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SQL error checking transaction existence")
	assert.Nil(t, result)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStore_SetMinedMultiChunk_InsertBlockIDsError(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	hashes := CreateTestHashes(2)
	minedInfo := CreateTestMinedBlockInfo()

	mock.ExpectBegin()
	existsRows := sqlmock.NewRows([]string{"hash"})
	for _, h := range hashes {
		existsRows.AddRow(h[:])
	}
	mock.ExpectQuery(`SELECT hash FROM transactions WHERE hash IN`).
		WillReturnRows(existsRows)
	mock.ExpectExec(`INSERT INTO block_ids`).
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	ctx := context.Background()
	result, err := store.setMinedMultiChunk(ctx, hashes, minedInfo)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SQL error inserting block_ids")
	assert.Nil(t, result)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStore_SetMinedMultiChunk_UpdateError(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	hashes := CreateTestHashes(2)
	minedInfo := CreateTestMinedBlockInfo()

	mock.ExpectBegin()
	existsRows := sqlmock.NewRows([]string{"hash"})
	for _, h := range hashes {
		existsRows.AddRow(h[:])
	}
	mock.ExpectQuery(`SELECT hash FROM transactions WHERE hash IN`).
		WillReturnRows(existsRows)
	mock.ExpectExec(`INSERT INTO block_ids`).
		WillReturnResult(sqlmock.NewResult(0, int64(len(hashes))))
	mock.ExpectExec(`UPDATE transactions`).
		WillReturnError(sql.ErrTxDone)
	mock.ExpectRollback()

	ctx := context.Background()
	result, err := store.setMinedMultiChunk(ctx, hashes, minedInfo)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SQL error updating transactions")
	assert.Nil(t, result)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStore_SetMinedMultiChunk_GetBlockIDsError(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	hashes := CreateTestHashes(2)
	minedInfo := CreateTestMinedBlockInfo()

	mock.ExpectBegin()
	existsRows := sqlmock.NewRows([]string{"hash"})
	for _, h := range hashes {
		existsRows.AddRow(h[:])
	}
	mock.ExpectQuery(`SELECT hash FROM transactions WHERE hash IN`).
		WillReturnRows(existsRows)
	mock.ExpectExec(`INSERT INTO block_ids`).
		WillReturnResult(sqlmock.NewResult(0, int64(len(hashes))))
	mock.ExpectExec(`UPDATE transactions`).
		WillReturnResult(sqlmock.NewResult(0, int64(len(hashes))))
	mock.ExpectQuery(`SELECT t\.hash, b\.block_id FROM transactions t`).
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	ctx := context.Background()
	result, err := store.setMinedMultiChunk(ctx, hashes, minedInfo)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SQL error fetching block IDs")
	assert.Nil(t, result)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStore_SetMinedMultiChunk_CommitError(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	hashes := CreateTestHashes(2)
	minedInfo := CreateTestMinedBlockInfo()

	mock.ExpectBegin()
	existsRows := sqlmock.NewRows([]string{"hash"})
	for _, h := range hashes {
		existsRows.AddRow(h[:])
	}
	mock.ExpectQuery(`SELECT hash FROM transactions WHERE hash IN`).
		WillReturnRows(existsRows)
	mock.ExpectExec(`INSERT INTO block_ids`).
		WillReturnResult(sqlmock.NewResult(0, int64(len(hashes))))
	mock.ExpectExec(`UPDATE transactions`).
		WillReturnResult(sqlmock.NewResult(0, int64(len(hashes))))

	blockIDRows := sqlmock.NewRows([]string{"hash", "block_id"})
	for _, h := range hashes {
		blockIDRows.AddRow(h[:], uint32(minedInfo.BlockID))
	}
	mock.ExpectQuery(`SELECT t\.hash, b\.block_id FROM transactions t`).
		WillReturnRows(blockIDRows)
	mock.ExpectCommit().WillReturnError(sql.ErrTxDone)

	ctx := context.Background()
	result, err := store.setMinedMultiChunk(ctx, hashes, minedInfo)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SQL error committing SetMinedMulti transaction")
	assert.Nil(t, result)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStore_SetMinedMultiChunk_RollbackError(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	hashes := CreateTestHashes(2)
	minedInfo := CreateTestMinedBlockInfo()

	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT hash FROM transactions WHERE hash IN`).
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback().WillReturnError(sql.ErrConnDone)

	ctx := context.Background()
	result, err := store.setMinedMultiChunk(ctx, hashes, minedInfo)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "SQL error checking transaction existence")
	assert.Nil(t, result)
}

func TestStore_SetMinedMulti_EmptyHashes(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	hashes := []*chainhash.Hash{}
	minedInfo := CreateTestMinedBlockInfo()

	ctx := context.Background()
	result, err := store.SetMinedMulti(ctx, hashes, minedInfo)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 0, len(result))
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStore_SetMinedMultiChunk_NoExistingTransactions(t *testing.T) {
	logger := ulogger.TestLogger{}
	store, mock := CreateMockStore(logger)
	defer func() { _ = mock.ExpectationsWereMet() }()

	hashes := CreateTestHashes(2)
	minedInfo := CreateTestMinedBlockInfo()

	mock.ExpectBegin()
	existsRows := sqlmock.NewRows([]string{"hash"})
	mock.ExpectQuery(`SELECT hash FROM transactions WHERE hash IN`).
		WillReturnRows(existsRows)
	mock.ExpectCommit()

	ctx := context.Background()
	result, err := store.setMinedMultiChunk(ctx, hashes, minedInfo)

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 0, len(result))
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestCreateTestHashes(t *testing.T) {
	hashes := CreateTestHashes(5)
	require.Equal(t, 5, len(hashes))

	hashSet := make(map[chainhash.Hash]bool)
	for _, hash := range hashes {
		require.NotNil(t, hash)
		require.False(t, hashSet[*hash], "Hash should be unique")
		hashSet[*hash] = true
	}
}

func TestCreateTestMinedBlockInfo(t *testing.T) {
	info := CreateTestMinedBlockInfo()
	require.Equal(t, uint32(1), info.BlockID)
	require.Equal(t, uint32(100), info.BlockHeight)
	require.Equal(t, 0, info.SubtreeIdx)
	require.Equal(t, false, info.UnsetMined)
}

// SetupSetMinedMultiChunkMocks configures mock expectations for a successful setMinedMultiChunk call.
func SetupSetMinedMultiChunkMocks(mock sqlmock.Sqlmock, hashes []*chainhash.Hash, minedInfo utxo.MinedBlockInfo) {
	mock.ExpectBegin()

	// Step 1: Check existence
	existsRows := sqlmock.NewRows([]string{"hash"})
	for _, h := range hashes {
		existsRows.AddRow(h[:])
	}
	mock.ExpectQuery(`SELECT hash FROM transactions WHERE hash IN`).
		WillReturnRows(existsRows)

	// Step 2: Insert block_ids
	mock.ExpectExec(`INSERT INTO block_ids`).
		WillReturnResult(sqlmock.NewResult(0, int64(len(hashes))))

	// Step 3: Update transactions
	mock.ExpectExec(`UPDATE transactions`).
		WillReturnResult(sqlmock.NewResult(0, int64(len(hashes))))

	// Step 4: Fetch block_ids (row per hash, no array_agg)
	blockIDRows := sqlmock.NewRows([]string{"hash", "block_id"})
	for _, h := range hashes {
		blockIDRows.AddRow(h[:], uint32(minedInfo.BlockID))
	}
	mock.ExpectQuery(`SELECT t\.hash, b\.block_id FROM transactions t`).
		WillReturnRows(blockIDRows)

	mock.ExpectCommit()
}
