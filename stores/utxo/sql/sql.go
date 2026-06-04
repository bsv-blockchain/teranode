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
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/batchermetrics"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/bsv-blockchain/teranode/util/usql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	pq "github.com/lib/pq"
	"golang.org/x/sync/errgroup"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// batchSpend represents a single UTXO spend request in a batch.
// Mirrors aerospike/spend.go batchSpend struct.
type batchSpend struct {
	spend             *utxo.Spend // UTXO to spend
	blockHeight       uint32      // Current block height
	errCh             chan error  // Channel for completion notification
	ignoreConflicting bool
	ignoreLocked      bool
}

// Store implements the UTXO store interface using a SQL database backend.
type Store struct {
	logger          ulogger.Logger
	settings        *settings.Settings
	db              *usql.DB
	storeURL        *url.URL
	engine          string
	blockHeight     atomic.Uint32
	medianBlockTime atomic.Uint32
	ctx             context.Context
	spendBatcher    *batcher.Batcher[batchSpend]
	getBatcher      *batcher.Batcher[batchGetItem]
	createBatcher   *batcher.Batcher[batchCreateItem]
	unlockBatcher   *batcher.Batcher[batchUnlockItem]
}

// batchUnlockItem represents a single SetLocked(false) request.
type batchUnlockItem struct {
	hash chainhash.Hash
	done chan error
}

// batchGetItemData holds the result of a batch get operation.
type batchGetItemData struct {
	Data *meta.Data
	Err  error
}

// batchGetItem represents a single item in a batch get operation.
type batchGetItem struct {
	hash   chainhash.Hash
	fields []fields.FieldName
	done   chan batchGetItemData
}

// batchCreateResult holds the result routed back to a Create() caller.
type batchCreateResult struct {
	Data *meta.Data
	Err  error
}

// batchCreateItem represents a single Create() request queued into the createBatcher.
type batchCreateItem struct {
	tx          *bt.Tx
	blockHeight uint32
	options     *utxo.CreateOptions
	done        chan batchCreateResult
}

// New creates a new SQL-based UTXO store.
// It supports both PostgreSQL and SQLite backends through the URL scheme.
//
// Supported URL schemes:
//   - postgres://: PostgreSQL database
//   - sqlite:///: SQLite file database
//   - sqlitememory:///: In-memory SQLite database
//
// URL parameters:
//   - expiration: Duration after which spent UTXOs are cleaned up
//   - logging: Enable SQL query logging
//
// Example URLs:
//
//	postgres://user:pass@localhost:5432/dbname?expiration=1h
//	sqlite:///path/to/db.sqlite?expiration=1h
//	sqlitememory:///test?expiration=1h
func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, storeURL *url.URL) (*Store, error) {
	initPrometheusMetrics()

	db, err := util.InitSQLDB(logger, storeURL, tSettings, tSettings.UtxoStore.PostgresPool)
	if err != nil {
		return nil, errors.NewStorageError("failed to init sql db", err)
	}

	switch storeURL.Scheme {
	case "postgres":
		if err = createPostgresSchema(db); err != nil {
			return nil, errors.NewStorageError("failed to create postgres schema", err)
		}

	case "sqlite", "sqlitememory":
		if err = createSqliteSchema(db); err != nil {
			return nil, errors.NewStorageError("failed to create sqlite schema", err)
		}

	default:
		return nil, errors.NewStorageError("unknown database engine: %s", storeURL.Scheme)
	}

	s := &Store{
		logger:          logger,
		settings:        tSettings,
		db:              db,
		storeURL:        storeURL,
		engine:          storeURL.Scheme,
		blockHeight:     atomic.Uint32{},
		medianBlockTime: atomic.Uint32{},
		ctx:             ctx,
	}

	otelTracer := tracing.Tracer("utxo").OTelTracer()
	batcherOpts := func(name string) []batcher.Option {
		return []batcher.Option{
			batcher.WithName(name),
			batcher.WithLogger(logger),
			batcher.WithMetrics(batchermetrics.Provider()),
			batcher.WithTracer(otelTracer),
		}
	}

	// Initialize spend batcher — mirrors aerospike/aerospike.go batcher setup.
	// Batches individual spend operations to control DB connection concurrency.
	// Always use background=false for SQL: batch callbacks must be serialized to
	// prevent PostgreSQL deadlocks from concurrent transactions locking overlapping
	// rows in different orders. Aerospike uses background=true because it has no
	// DB-level row locking.
	spendBatchSize := tSettings.UtxoStore.SpendBatcherSize
	spendBatchDuration := time.Duration(tSettings.UtxoStore.SpendBatcherDurationMillis) * time.Millisecond
	s.spendBatcher = batcher.NewWithPool(spendBatchSize, spendBatchDuration, s.sendSpendBatch, false, batcherOpts("sql_spend")...)
	if tSettings.BatcherDrainMode {
		s.spendBatcher.SetDrainMode(true)
	}
	// Tick interval applied after drain so go-batcher's drain-wins guard sees the
	// final state (no-op + warning under drain). Default 0 = disabled.
	if ms := tSettings.UtxoStore.SpendBatcherTickerIntervalMillis; ms > 0 {
		s.spendBatcher.SetTickInterval(time.Duration(ms) * time.Millisecond)
	}

	// Initialize get batcher — mirrors aerospike/get.go batcher setup.
	// Batches individual Get() calls into bulk SQL queries via BatchDecorate,
	// reducing connection pool pressure from N×4 queries to 4 queries per batch.
	getBatchSize := tSettings.UtxoStore.GetBatcherSize
	getBatchDuration := time.Duration(tSettings.UtxoStore.GetBatcherDurationMillis) * time.Millisecond
	if getBatchSize > 1 {
		s.getBatcher = batcher.NewWithPool(getBatchSize, getBatchDuration, s.sendGetBatch, true, batcherOpts("sql_get")...)
		if tSettings.BatcherDrainMode {
			s.getBatcher.SetDrainMode(true)
		}
		if ms := tSettings.UtxoStore.GetBatcherTickerIntervalMillis; ms > 0 {
			s.getBatcher.SetTickInterval(time.Duration(ms) * time.Millisecond)
		}
	}

	// Initialize create batcher — mirrors aerospike/aerospike.go storeBatcher setup.
	// Batches individual Create() calls into a single pgx.SendBatch with N CTEs,
	// reducing N network round-trips to 1. background=true because each CTE inserts
	// a unique transaction hash — no row overlap, no deadlock risk between batches.
	if storeURL.Scheme == "postgres" && tSettings.UtxoStore.StoreBatcherSize > 1 {
		storeBatchSize := tSettings.UtxoStore.StoreBatcherSize
		storeBatchDuration := time.Duration(tSettings.UtxoStore.StoreBatcherDurationMillis) * time.Millisecond
		s.createBatcher = batcher.NewWithPool(storeBatchSize, storeBatchDuration, s.sendCreateBatch, true, batcherOpts("sql_create")...)
		if tSettings.BatcherDrainMode {
			s.createBatcher.SetDrainMode(true)
		}
		if ms := tSettings.UtxoStore.StoreBatcherTickerIntervalMillis; ms > 0 {
			s.createBatcher.SetTickInterval(time.Duration(ms) * time.Millisecond)
		}
	}

	// Initialize unlock batcher for Postgres — batches single-hash SetLocked(false) calls.
	if storeURL.Scheme == "postgres" && tSettings.UtxoStore.LockedBatcherSize > 1 {
		unlockBatchSize := tSettings.UtxoStore.LockedBatcherSize
		unlockBatchDuration := time.Duration(tSettings.UtxoStore.LockedBatcherDurationMillis) * time.Millisecond
		s.unlockBatcher = batcher.NewWithPool(unlockBatchSize, unlockBatchDuration, s.sendUnlockBatch, true, batcherOpts("sql_unlock")...)
		if tSettings.BatcherDrainMode {
			s.unlockBatcher.SetDrainMode(true)
		}
		if ms := tSettings.UtxoStore.LockedBatcherTickerIntervalMillis; ms > 0 {
			s.unlockBatcher.SetTickInterval(time.Duration(ms) * time.Millisecond)
		}
	}

	return s, nil
}

func (s *Store) SetBlockHeight(blockHeight uint32) error {
	if blockHeight == 0 {
		return errors.NewInvalidArgumentError("block height cannot be zero")
	}

	s.logger.Debugf("setting block height to %d", blockHeight)
	s.blockHeight.Store(blockHeight)

	return nil
}

func (s *Store) GetBlockHeight() uint32 {
	return s.blockHeight.Load()
}

func (s *Store) SetMedianBlockTime(medianTime uint32) error {
	s.logger.Debugf("setting median block time to %d", medianTime)
	s.medianBlockTime.Store(medianTime)

	return nil
}

func (s *Store) GetMedianBlockTime() uint32 {
	return s.medianBlockTime.Load()
}

func (s *Store) GetBlockState() utxo.BlockState {
	return utxo.BlockState{
		Height:     s.blockHeight.Load(),
		MedianTime: s.medianBlockTime.Load(),
	}
}

// Health checks the database connection and returns status information.
func (s *Store) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	details := fmt.Sprintf("SQL Engine is %s", s.engine)

	var num int

	err := s.db.QueryRowContext(ctx, "SELECT 1").Scan(&num)
	if err != nil {
		return http.StatusServiceUnavailable, details, err
	}

	return http.StatusOK, details, nil
}

// Create stores a new transaction's outputs as UTXOs.
// For coinbase transactions, it sets the maturity period to 100 blocks.
func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	options := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "sql:Create")
	defer deferFn()

	// Postgres with createBatcher: enqueue and wait for batch callback
	if s.createBatcher != nil {
		return s.createBatched(ctx, tx, blockHeight, options)
	}

	// Try the operation with retry logic for lock errors
	var txMeta *meta.Data
	var err error

	for attempt := 0; attempt <= 3; attempt++ {
		txMeta, err = s.createWithRetry(ctx, tx, blockHeight, options)

		// If no error or not a lock error, return immediately
		if err == nil || !isLockError(err) {
			return txMeta, err
		}

		// For lock errors, retry with backoff
		if attempt < 3 {
			backoff := time.Duration(100<<attempt) * time.Millisecond // 100ms, 200ms, 400ms
			s.logger.Warnf("Database lock error during create (attempt %d): %v, retrying in %v", attempt+1, err, backoff)

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
				// Continue to next attempt
			}
		}
	}

	return txMeta, err
}

// createBatched enqueues a Create request into the createBatcher for bulk processing.
// Mirrors aerospike/create.go storeBatcher.Put pattern.
func (s *Store) createBatched(ctx context.Context, tx *bt.Tx, blockHeight uint32, options *utxo.CreateOptions) (*meta.Data, error) {
	done := make(chan batchCreateResult, 1)
	s.createBatcher.PutCtx(ctx, &batchCreateItem{
		tx:          tx,
		blockHeight: blockHeight,
		options:     options,
		done:        done,
	})

	select {
	case result := <-done:
		return result.Data, result.Err
	case <-ctx.Done():
		s.logger.Warnf("[createBatched] context cancelled while waiting for batcher result — tx may or may not be created")
		return nil, ctx.Err()
	}
}

func (s *Store) createWithRetry(ctx context.Context, tx *bt.Tx, blockHeight uint32, options *utxo.CreateOptions) (*meta.Data, error) {
	txMeta, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, errors.NewProcessingError("failed to get tx meta data", err)
	}

	if options.Conflicting {
		txMeta.Conflicting = true
	}

	if options.Locked {
		txMeta.Locked = true
	}

	var unminedSince interface{} = nil // Use nil for mined transactions
	if len(options.MinedBlockInfos) == 0 {
		unminedSince = blockHeight // Set UnminedSince only for unmined transactions
	}

	var txHash *chainhash.Hash
	if options.TxID != nil {
		txHash = options.TxID
	} else {
		txHash = tx.TxIDChainHash()
	}

	isCoinbase := tx.IsCoinbase()
	if options.IsCoinbase != nil {
		isCoinbase = *options.IsCoinbase
	}

	// Postgres with batching: single-CTE with UNNEST arrays — one round-trip, auto-atomic
	if s.settings.UtxoStore.BatchSQLOperations && s.engine == "postgres" {
		return s.createCTE(ctx, tx, blockHeight, options, txHash, txMeta, isCoinbase, unminedSince)
	}

	// Insert the transaction row...
	q := `
		INSERT INTO transactions (
		 hash
		,version
		,lock_time
		,fee
		,size_in_bytes
		,coinbase
		,frozen
		,conflicting
		,locked
		,unmined_since
	  ) VALUES (
		 $1
		,$2
		,$3
		,$4
		,$5
		,$6
		,$7
		,$8
		,$9
		,$10
		)
		RETURNING id
	`

	// Create a database transaction
	txn, err := s.db.Begin()
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = txn.Rollback()
	}()

	var transactionID int

	err = txn.QueryRowContext(
		ctx,
		q,
		txHash[:],
		tx.Version,
		tx.LockTime,
		txMeta.Fee,
		txMeta.SizeInBytes,
		isCoinbase,
		options.Frozen,
		options.Conflicting,
		options.Locked,
		unminedSince,
	).Scan(&transactionID)
	if err != nil {
		if pgErr := asPgUniqueViolation(err); pgErr != nil {
			return nil, errors.NewTxExistsError("Transaction already exists in postgres store (coinbase=%v):", tx.IsCoinbase(), err)
		} else if sqliteErr, ok := err.(*sqlite.Error); ok && sqliteErr.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE {
			return nil, errors.NewTxExistsError("Transaction already exists in sqlite store (coinbase=%v):", tx.IsCoinbase(), sqliteErr)
		}

		return nil, errors.NewStorageError("Failed to insert transaction", err)
	}

	// Insert inputs, outputs, and block_ids — use batched or per-row depending on setting
	if s.settings.UtxoStore.BatchSQLOperations {
		if err = s.createInputsBatched(ctx, txn, transactionID, tx); err != nil {
			return nil, err
		}
		if err = s.createOutputsBatched(ctx, txn, transactionID, txHash, tx, isCoinbase, blockHeight); err != nil {
			return nil, err
		}
		if len(options.MinedBlockInfos) > 0 {
			if err = s.createBlockIDsBatched(ctx, txn, transactionID, options.MinedBlockInfos); err != nil {
				return nil, err
			}
		}
	} else {
		if err = s.createInputsPerRow(ctx, txn, transactionID, tx); err != nil {
			return nil, err
		}
		if err = s.createOutputsPerRow(ctx, txn, transactionID, txHash, tx, isCoinbase, blockHeight); err != nil {
			return nil, err
		}
		if len(options.MinedBlockInfos) > 0 {
			if err = s.createBlockIDsPerRow(ctx, txn, transactionID, options.MinedBlockInfos); err != nil {
				return nil, err
			}
		}
	}

	if txMeta.Conflicting {
		if err = s.updateParentConflictingChildren(ctx, transactionID, tx, txn); err != nil {
			return nil, err
		}
	}

	if err = txn.Commit(); err != nil {
		return nil, err
	}

	return txMeta, nil
}

// createInputsBatched inserts all transaction inputs in chunked multi-value INSERTs.
func (s *Store) createInputsBatched(ctx context.Context, txn *sql.Tx, transactionID int, tx *bt.Tx) error {
	if len(tx.Inputs) == 0 {
		return nil
	}

	const colsPerRow = 8
	const maxRowsPerChunk = maxPostgresParams / colsPerRow
	baseSQL := `INSERT INTO inputs (transaction_id,idx,previous_transaction_hash,previous_tx_idx,previous_tx_satoshis,previous_tx_script,unlocking_script,sequence_number) VALUES `

	for chunkStart := 0; chunkStart < len(tx.Inputs); chunkStart += maxRowsPerChunk {
		chunkEnd := chunkStart + maxRowsPerChunk
		if chunkEnd > len(tx.Inputs) {
			chunkEnd = len(tx.Inputs)
		}
		chunkSize := chunkEnd - chunkStart

		q := buildMultiValueInsert(baseSQL, colsPerRow, chunkSize, 1)
		args := make([]interface{}, 0, chunkSize*colsPerRow)
		for i := chunkStart; i < chunkEnd; i++ {
			input := tx.Inputs[i]
			args = append(args,
				transactionID,
				i,
				input.PreviousTxIDChainHash()[:],
				input.PreviousTxOutIndex,
				input.PreviousTxSatoshis,
				input.PreviousTxScript,
				input.UnlockingScript,
				input.SequenceNumber,
			)
		}

		if _, err := txn.ExecContext(ctx, q, args...); err != nil {
			return classifyInsertError(err, tx.IsCoinbase(), "input")
		}
	}
	return nil
}

// createOutputsBatched inserts all transaction outputs in chunked multi-value INSERTs.
func (s *Store) createOutputsBatched(ctx context.Context, txn *sql.Tx, transactionID int, txHash *chainhash.Hash, tx *bt.Tx, isCoinbase bool, blockHeight uint32) error {
	// Collect non-nil outputs with their original indices
	type outputEntry struct {
		index  int
		output *bt.Output
	}
	outputs := make([]outputEntry, 0, len(tx.Outputs))
	for i, output := range tx.Outputs {
		if output != nil {
			outputs = append(outputs, outputEntry{index: i, output: output})
		}
	}
	if len(outputs) == 0 {
		return nil
	}

	var coinbaseSpendingHeight uint32
	if isCoinbase {
		coinbaseSpendingHeight = blockHeight + uint32(s.settings.ChainCfgParams.CoinbaseMaturity)
	}

	const colsPerRow = 7
	const maxRowsPerChunk = maxPostgresParams / colsPerRow
	baseSQL := `INSERT INTO outputs (transaction_id,idx,locking_script,satoshis,coinbase_spending_height,utxo_hash,spending_data) VALUES `

	for chunkStart := 0; chunkStart < len(outputs); chunkStart += maxRowsPerChunk {
		chunkEnd := chunkStart + maxRowsPerChunk
		if chunkEnd > len(outputs) {
			chunkEnd = len(outputs)
		}
		chunkSize := chunkEnd - chunkStart

		q := buildMultiValueInsert(baseSQL, colsPerRow, chunkSize, 1)
		args := make([]interface{}, 0, chunkSize*colsPerRow)
		for _, entry := range outputs[chunkStart:chunkEnd] {
			iUint32, err := safeconversion.IntToUint32(entry.index)
			if err != nil {
				return err
			}

			utxoHash, err := util.UTXOHashFromOutput(txHash, entry.output, iUint32)
			if err != nil {
				return err
			}

			args = append(args,
				transactionID,
				entry.index,
				entry.output.LockingScript,
				entry.output.Satoshis,
				coinbaseSpendingHeight,
				utxoHash[:],
				nil,
			)
		}

		if _, err := txn.ExecContext(ctx, q, args...); err != nil {
			return classifyInsertError(err, tx.IsCoinbase(), "output")
		}
	}
	return nil
}

// createBlockIDsBatched inserts all block_ids in chunked multi-value INSERTs.
func (s *Store) createBlockIDsBatched(ctx context.Context, txn *sql.Tx, transactionID int, blockInfos []utxo.MinedBlockInfo) error {
	const colsPerRow = 4
	const maxRowsPerChunk = maxPostgresParams / colsPerRow
	baseSQL := `INSERT INTO block_ids (transaction_id,block_id,block_height,subtree_idx) VALUES `

	for chunkStart := 0; chunkStart < len(blockInfos); chunkStart += maxRowsPerChunk {
		chunkEnd := chunkStart + maxRowsPerChunk
		if chunkEnd > len(blockInfos) {
			chunkEnd = len(blockInfos)
		}
		chunk := blockInfos[chunkStart:chunkEnd]

		q := buildMultiValueInsert(baseSQL, colsPerRow, len(chunk), 1)
		args := make([]interface{}, 0, len(chunk)*colsPerRow)
		for _, blockMeta := range chunk {
			args = append(args, transactionID, blockMeta.BlockID, blockMeta.BlockHeight, blockMeta.SubtreeIdx)
		}

		if _, err := txn.ExecContext(ctx, q, args...); err != nil {
			return classifyInsertError(err, false, "block_ids")
		}
	}
	return nil
}

// createCTESQL is the single CTE statement that inserts a transaction + all its inputs,
// outputs, and block_ids in one round-trip. UNNEST with array parameters means the SQL
// string is always identical regardless of transaction size — perfect for statement caching.
// Parameters: $1-$10 = transaction scalars, $11-$17 = input arrays, $18-$22 = output arrays, $23-$25 = block_id arrays.
const createCTESQL = `
WITH new_tx AS (
	INSERT INTO transactions (hash,version,lock_time,fee,size_in_bytes,coinbase,frozen,conflicting,locked,unmined_since)
	VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
	ON CONFLICT (hash) DO NOTHING
	RETURNING id
), ins_inputs AS (
	INSERT INTO inputs (transaction_id,idx,previous_transaction_hash,previous_tx_idx,previous_tx_satoshis,previous_tx_script,unlocking_script,sequence_number)
	SELECT new_tx.id, u.idx, u.prev_hash, u.prev_idx, u.prev_satoshis, u.prev_script, u.unlock_script, u.seq_num
	FROM new_tx, UNNEST($11::int[],$12::bytea[],$13::int[],$14::bigint[],$15::bytea[],$16::bytea[],$17::bigint[])
		AS u(idx, prev_hash, prev_idx, prev_satoshis, prev_script, unlock_script, seq_num)
), ins_outputs AS (
	INSERT INTO outputs (transaction_id,idx,locking_script,satoshis,coinbase_spending_height,utxo_hash,spending_data)
	SELECT new_tx.id, u.idx, u.locking_script, u.satoshis, u.csh, u.utxo_hash, NULL
	FROM new_tx, UNNEST($18::int[],$19::bytea[],$20::bigint[],$21::int[],$22::bytea[])
		AS u(idx, locking_script, satoshis, csh, utxo_hash)
), ins_block_ids AS (
	INSERT INTO block_ids (transaction_id,block_id,block_height,subtree_idx)
	SELECT new_tx.id, u.block_id, u.block_height, u.subtree_idx
	FROM new_tx, UNNEST($23::int[],$24::int[],$25::int[])
		AS u(block_id, block_height, subtree_idx)
)
SELECT id FROM new_tx
`

// createCTE executes a single CTE statement with UNNEST arrays to insert a transaction
// and all its inputs/outputs/block_ids in one network round-trip (postgres only).
// No explicit transaction needed — a single statement is auto-atomic.
func (s *Store) createCTE(ctx context.Context, btTx *bt.Tx, blockHeight uint32, options *utxo.CreateOptions, txHash *chainhash.Hash, txMeta *meta.Data, isCoinbase bool, unminedSince interface{}) (*meta.Data, error) {
	// Build array parameters for UNNEST
	inpArrs := buildInputArrays(btTx)
	outArrs, err := buildOutputArrays(s.settings, txHash, btTx, isCoinbase, blockHeight)
	if err != nil {
		return nil, err
	}
	blkArrs := buildBlockIDArrays(options.MinedBlockInfos)

	// Execute single CTE — one round-trip, auto-atomic
	sqlConn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer sqlConn.Close()

	var inserted bool
	err = sqlConn.Raw(func(driverConn interface{}) error {
		pgxConn := driverConn.(*stdlib.Conn).Conn()
		rows, execErr := pgxConn.Query(ctx, createCTESQL,
			// $1-$10: transaction scalars
			txHash[:], btTx.Version, btTx.LockTime, txMeta.Fee, txMeta.SizeInBytes,
			isCoinbase, options.Frozen, options.Conflicting, options.Locked, unminedSince,
			// $11-$17: input arrays
			inpArrs.idx, inpArrs.prevHash, inpArrs.prevIdx,
			inpArrs.prevSatoshis, inpArrs.prevScript,
			inpArrs.unlockScript, inpArrs.seqNum,
			// $18-$22: output arrays
			outArrs.idx, outArrs.lockingScript, outArrs.satoshis,
			outArrs.coinbaseSpendingHeight, outArrs.utxoHash,
			// $23-$25: block_id arrays
			blkArrs.blockID, blkArrs.blockHeight, blkArrs.subtreeIdx,
		)
		if execErr != nil {
			return execErr
		}
		defer rows.Close()
		inserted = rows.Next() // true if new_tx returned a row (insert succeeded)
		return rows.Err()
	})
	if err != nil {
		return nil, errors.NewStorageError("Failed to create UTXO", err)
	}
	if !inserted {
		return nil, errors.NewTxExistsError("Transaction already exists in postgres store (coinbase=%v):", isCoinbase)
	}

	// Handle conflicting children (rare path — separate round-trip only when needed)
	if txMeta.Conflicting {
		if err = s.insertConflictingChildrenPgx(ctx, btTx, txHash); err != nil {
			return nil, err
		}
	}

	return txMeta, nil
}

// inputArrayParams holds parallel arrays for UNNEST input insertion.
type inputArrayParams struct {
	idx          []int32
	prevHash     [][]byte
	prevIdx      []int32
	prevSatoshis []int64
	prevScript   [][]byte
	unlockScript [][]byte
	seqNum       []int64 // uint32 can exceed int32 max (0xFFFFFFFF)
}

// buildInputArrays packs transaction inputs into parallel arrays for UNNEST.
func buildInputArrays(btTx *bt.Tx) inputArrayParams {
	n := len(btTx.Inputs)
	if n == 0 {
		return inputArrayParams{}
	}
	p := inputArrayParams{
		idx:          make([]int32, n),
		prevHash:     make([][]byte, n),
		prevIdx:      make([]int32, n),
		prevSatoshis: make([]int64, n),
		prevScript:   make([][]byte, n),
		unlockScript: make([][]byte, n),
		seqNum:       make([]int64, n),
	}
	for i, input := range btTx.Inputs {
		p.idx[i] = int32(i)
		p.prevHash[i] = input.PreviousTxIDChainHash()[:]
		p.prevIdx[i] = int32(input.PreviousTxOutIndex)
		p.prevSatoshis[i] = int64(input.PreviousTxSatoshis)
		if input.PreviousTxScript != nil {
			p.prevScript[i] = []byte(*input.PreviousTxScript)
		}
		if input.UnlockingScript != nil {
			p.unlockScript[i] = []byte(*input.UnlockingScript)
		}
		p.seqNum[i] = int64(input.SequenceNumber)
	}
	return p
}

// outputArrayParams holds parallel arrays for UNNEST output insertion.
type outputArrayParams struct {
	idx                    []int32
	lockingScript          [][]byte
	satoshis               []int64
	coinbaseSpendingHeight []int32
	utxoHash               [][]byte
}

// buildOutputArrays packs transaction outputs into parallel arrays for UNNEST.
func buildOutputArrays(s *settings.Settings, txHash *chainhash.Hash, btTx *bt.Tx, isCoinbase bool, blockHeight uint32) (outputArrayParams, error) {
	// Count non-nil outputs
	count := 0
	for _, output := range btTx.Outputs {
		if output != nil {
			count++
		}
	}
	if count == 0 {
		return outputArrayParams{}, nil
	}

	var coinbaseSpendingHeight uint32
	if isCoinbase {
		coinbaseSpendingHeight = blockHeight + uint32(s.ChainCfgParams.CoinbaseMaturity)
	}

	p := outputArrayParams{
		idx:                    make([]int32, 0, count),
		lockingScript:          make([][]byte, 0, count),
		satoshis:               make([]int64, 0, count),
		coinbaseSpendingHeight: make([]int32, 0, count),
		utxoHash:               make([][]byte, 0, count),
	}
	for i, output := range btTx.Outputs {
		if output == nil {
			continue
		}
		iUint32, err := safeconversion.IntToUint32(i)
		if err != nil {
			return outputArrayParams{}, err
		}
		utxoHash, err := util.UTXOHashFromOutput(txHash, output, iUint32)
		if err != nil {
			return outputArrayParams{}, err
		}
		p.idx = append(p.idx, int32(i))
		if output.LockingScript != nil {
			p.lockingScript = append(p.lockingScript, []byte(*output.LockingScript))
		} else {
			p.lockingScript = append(p.lockingScript, nil)
		}
		p.satoshis = append(p.satoshis, int64(output.Satoshis))
		p.coinbaseSpendingHeight = append(p.coinbaseSpendingHeight, int32(coinbaseSpendingHeight))
		p.utxoHash = append(p.utxoHash, utxoHash[:])
	}
	return p, nil
}

// blockIDArrayParams holds parallel arrays for UNNEST block_id insertion.
type blockIDArrayParams struct {
	blockID     []int32
	blockHeight []int32
	subtreeIdx  []int32
}

// buildBlockIDArrays packs block info into parallel arrays for UNNEST.
func buildBlockIDArrays(blockInfos []utxo.MinedBlockInfo) blockIDArrayParams {
	n := len(blockInfos)
	if n == 0 {
		return blockIDArrayParams{}
	}
	p := blockIDArrayParams{
		blockID:     make([]int32, n),
		blockHeight: make([]int32, n),
		subtreeIdx:  make([]int32, n),
	}
	for i, info := range blockInfos {
		p.blockID[i] = int32(info.BlockID)
		p.blockHeight[i] = int32(info.BlockHeight)
		p.subtreeIdx[i] = int32(info.SubtreeIdx)
	}
	return p
}

// insertConflictingChildrenPgx inserts conflicting_children entries using pgx SendBatch.
// Only called for conflicting transactions (rare path).
func (s *Store) insertConflictingChildrenPgx(ctx context.Context, btTx *bt.Tx, txHash *chainhash.Hash) error {
	sqlConn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer sqlConn.Close()

	return sqlConn.Raw(func(driverConn interface{}) error {
		pgxConn := driverConn.(*stdlib.Conn).Conn()

		pgxTx, txErr := pgxConn.Begin(ctx)
		if txErr != nil {
			return txErr
		}
		defer pgxTx.Rollback(ctx) //nolint:errcheck

		var childTxID int
		if err := pgxTx.QueryRow(ctx, `SELECT id FROM transactions WHERE hash = $1`, txHash[:]).Scan(&childTxID); err != nil {
			return errors.NewStorageError("Failed to find transaction for conflicting children", err)
		}

		batch := &pgx.Batch{}
		q := `INSERT INTO conflicting_children (transaction_id, child_transaction_id) VALUES ((SELECT id FROM transactions WHERE hash = $1), $2) ON CONFLICT DO NOTHING`
		for _, input := range btTx.Inputs {
			batch.Queue(q, input.PreviousTxIDChainHash()[:], childTxID)
		}
		br := pgxTx.SendBatch(ctx, batch)
		if batchErr := br.Close(); batchErr != nil {
			return errors.NewStorageError("Failed to insert conflicting_children", batchErr)
		}
		return pgxTx.Commit(ctx)
	})
}

// preparedCreate holds pre-computed data for one item in a create batch.
type preparedCreate struct {
	txHash       *chainhash.Hash
	txMeta       *meta.Data
	isCoinbase   bool
	unminedSince interface{}
	inpArrs      inputArrayParams
	outArrs      outputArrayParams
	blkArrs      blockIDArrayParams
}

// sendCreateBatch is the batcher callback that processes a batch of Create operations
// in a single pgx.SendBatch — N CTEs in one network flush.
// Mirrors aerospike/create.go sendStoreBatch.
func (s *Store) sendCreateBatch(batch []*batchCreateItem) {
	// Phase 1: Pre-compute all array parameters (CPU only, no DB)
	prepared := make([]preparedCreate, len(batch))
	for i, item := range batch {
		txMeta, err := util.TxMetaDataFromTx(item.tx)
		if err != nil {
			item.done <- batchCreateResult{Err: errors.NewProcessingError("failed to get tx meta data", err)}
			continue
		}

		if item.options.Conflicting {
			txMeta.Conflicting = true
		}
		if item.options.Locked {
			txMeta.Locked = true
		}

		var unminedSince interface{}
		if len(item.options.MinedBlockInfos) == 0 {
			unminedSince = item.blockHeight
		}

		var txHash *chainhash.Hash
		if item.options.TxID != nil {
			txHash = item.options.TxID
		} else {
			txHash = item.tx.TxIDChainHash()
		}

		isCoinbase := item.tx.IsCoinbase()
		if item.options.IsCoinbase != nil {
			isCoinbase = *item.options.IsCoinbase
		}

		inpArrs := buildInputArrays(item.tx)
		outArrs, err := buildOutputArrays(s.settings, txHash, item.tx, isCoinbase, item.blockHeight)
		if err != nil {
			item.done <- batchCreateResult{Err: err}
			continue
		}
		blkArrs := buildBlockIDArrays(item.options.MinedBlockInfos)

		prepared[i] = preparedCreate{
			txHash:       txHash,
			txMeta:       txMeta,
			isCoinbase:   isCoinbase,
			unminedSince: unminedSince,
			inpArrs:      inpArrs,
			outArrs:      outArrs,
			blkArrs:      blkArrs,
		}
	}

	// Count valid items (those without prep errors — they already got their done channel written)
	validIndices := make([]int, 0, len(batch))
	for i := range batch {
		if prepared[i].txHash != nil {
			validIndices = append(validIndices, i)
		}
	}
	if len(validIndices) == 0 {
		return
	}

	// Phase 2: Get one pgx connection, queue all valid CTEs into SendBatch
	sqlConn, err := s.db.Conn(s.ctx)
	if err != nil {
		for _, idx := range validIndices {
			batch[idx].done <- batchCreateResult{Err: errors.NewStorageError("failed to get db connection", err)}
		}
		return
	}
	defer sqlConn.Close()

	connErr := sqlConn.Raw(func(driverConn interface{}) error {
		pgxConn := driverConn.(*stdlib.Conn).Conn()
		pgxBatch := &pgx.Batch{}

		for _, idx := range validIndices {
			p := &prepared[idx]
			item := batch[idx]
			pgxBatch.Queue(createCTESQL,
				// $1-$10: transaction scalars
				p.txHash[:], item.tx.Version, item.tx.LockTime,
				p.txMeta.Fee, p.txMeta.SizeInBytes,
				p.isCoinbase, item.options.Frozen, item.options.Conflicting,
				item.options.Locked, p.unminedSince,
				// $11-$17: input arrays
				p.inpArrs.idx, p.inpArrs.prevHash, p.inpArrs.prevIdx,
				p.inpArrs.prevSatoshis, p.inpArrs.prevScript,
				p.inpArrs.unlockScript, p.inpArrs.seqNum,
				// $18-$22: output arrays
				p.outArrs.idx, p.outArrs.lockingScript,
				p.outArrs.satoshis, p.outArrs.coinbaseSpendingHeight,
				p.outArrs.utxoHash,
				// $23-$25: block_id arrays
				p.blkArrs.blockID, p.blkArrs.blockHeight,
				p.blkArrs.subtreeIdx,
			)
		}

		br := pgxConn.SendBatch(s.ctx, pgxBatch)

		// Read results — collect but don't send to callers yet.
		// We must call br.Close() before signalling callers, because
		// pipelined auto-committed statements may not be fully visible
		// to other connections until the batch reader is closed.
		type batchResult struct {
			idx      int
			result   batchCreateResult
			logError bool
		}
		results := make([]batchResult, 0, len(validIndices))

		for _, idx := range validIndices {
			p := &prepared[idx]
			rows, queryErr := br.Query()
			var inserted bool
			if queryErr == nil {
				inserted = rows.Next() // true if new_tx returned a row (insert succeeded)
				if err := rows.Err(); err != nil {
					queryErr = err
				}
				rows.Close()
			}
			if queryErr != nil {
				results = append(results, batchResult{idx: idx, result: batchCreateResult{
					Err: errors.NewStorageError("Failed to create UTXO", queryErr),
				}, logError: true})
			} else if !inserted {
				// ON CONFLICT (hash) DO NOTHING — new_tx returned 0 rows, tx already exists
				results = append(results, batchResult{idx: idx, result: batchCreateResult{
					Err: errors.NewTxExistsError("Transaction already exists in postgres store (coinbase=%v)", p.isCoinbase),
				}})
			} else {
				results = append(results, batchResult{idx: idx, result: batchCreateResult{Data: p.txMeta}})
			}
		}

		// Close the batch reader — ensures all pipelined commits are finalized
		// and visible to other connections before callers proceed.
		closeErr := br.Close()
		if closeErr != nil {
			s.logger.Errorf("[sendCreateBatch] error closing batch results: %v", closeErr)
		}

		// Now signal callers — if Close() failed, override successes with errors
		// since the connection may be in an error state and data visibility is uncertain.
		for _, r := range results {
			if closeErr != nil && r.result.Err == nil {
				batch[r.idx].done <- batchCreateResult{
					Err: errors.NewStorageError("batch close failed, results may not be visible", closeErr),
				}
				continue
			}
			if r.logError {
				s.logger.Errorf("[sendCreateBatch] CTE failed for tx %x: %+v", prepared[r.idx].txHash[:], r.result.Err)
			}
			batch[r.idx].done <- r.result
		}

		// Return driver.ErrBadConn so database/sql discards the connection
		// from the pool rather than reusing it, and Phase 3 (conflicting
		// children) does not run on an uncertain state. The actual closeErr
		// is already logged above.
		if closeErr != nil {
			return driver.ErrBadConn
		}
		return nil
	})
	if connErr != nil {
		// Raw callback error — send to any items that haven't received a result yet
		for _, idx := range validIndices {
			select {
			case batch[idx].done <- batchCreateResult{Err: errors.NewStorageError("batch connection error", connErr)}:
			default:
				// already sent a result
			}
		}
		return
	}

	// Phase 3: Handle conflicting children (rare path — separate round-trips only when needed)
	for _, idx := range validIndices {
		p := &prepared[idx]
		if p.txMeta != nil && p.txMeta.Conflicting {
			if conflictErr := s.insertConflictingChildrenPgx(s.ctx, batch[idx].tx, p.txHash); conflictErr != nil {
				s.logger.Warnf("[sendCreateBatch] failed to insert conflicting children for %x: %v", p.txHash[:], conflictErr)
			}
		}
	}
}

// createInputsPerRow inserts transaction inputs one row at a time (original behavior).
func (s *Store) createInputsPerRow(ctx context.Context, txn *sql.Tx, transactionID int, tx *bt.Tx) error {
	q := `
		INSERT INTO inputs (
		 transaction_id
		,idx
		,previous_transaction_hash
		,previous_tx_idx
		,previous_tx_satoshis
		,previous_tx_script
		,unlocking_script
		,sequence_number
		) VALUES (
     $1
		,$2
		,$3
		,$4
		,$5
		,$6
		,$7
		,$8
		)
	`
	for i, input := range tx.Inputs {
		_, err := txn.ExecContext(
			ctx, q,
			transactionID, i,
			input.PreviousTxIDChainHash()[:],
			input.PreviousTxOutIndex,
			input.PreviousTxSatoshis,
			input.PreviousTxScript,
			input.UnlockingScript,
			input.SequenceNumber,
		)
		if err != nil {
			return classifyInsertError(err, tx.IsCoinbase(), "input")
		}
	}
	return nil
}

// createOutputsPerRow inserts transaction outputs one row at a time (original behavior).
func (s *Store) createOutputsPerRow(ctx context.Context, txn *sql.Tx, transactionID int, txHash *chainhash.Hash, tx *bt.Tx, isCoinbase bool, blockHeight uint32) error {
	q := `
		INSERT INTO outputs (
		 transaction_id
		,idx
		,locking_script
		,satoshis
		,coinbase_spending_height
		,utxo_hash
		,spending_data
		) VALUES (
		 $1
		,$2
		,$3
		,$4
		,$5
		,$6
		,$7
		)
	`

	var coinbaseSpendingHeight uint32
	if isCoinbase {
		coinbaseSpendingHeight = blockHeight + uint32(s.settings.ChainCfgParams.CoinbaseMaturity)
	}

	for i, output := range tx.Outputs {
		if output == nil {
			continue
		}

		iUint32, err := safeconversion.IntToUint32(i)
		if err != nil {
			return err
		}

		utxoHash, err := util.UTXOHashFromOutput(txHash, output, iUint32)
		if err != nil {
			return err
		}

		_, err = txn.ExecContext(
			ctx, q,
			transactionID, i,
			output.LockingScript,
			output.Satoshis,
			coinbaseSpendingHeight,
			utxoHash[:],
			nil,
		)
		if err != nil {
			return classifyInsertError(err, tx.IsCoinbase(), "output")
		}
	}
	return nil
}

// createBlockIDsPerRow inserts block_ids one row at a time (original behavior).
func (s *Store) createBlockIDsPerRow(ctx context.Context, txn *sql.Tx, transactionID int, blockInfos []utxo.MinedBlockInfo) error {
	q := `
		INSERT INTO block_ids (
		 transaction_id
		,block_id
		,block_height
		,subtree_idx
		) VALUES (
		 $1
		,$2
		,$3
		,$4
		)
	`
	for _, blockMeta := range blockInfos {
		_, err := txn.ExecContext(ctx, q, transactionID, blockMeta.BlockID, blockMeta.BlockHeight, blockMeta.SubtreeIdx)
		if err != nil {
			return classifyInsertError(err, false, "block_ids")
		}
	}
	return nil
}

// classifyInsertError converts constraint violation errors into appropriate typed errors.
func classifyInsertError(err error, isCoinbase bool, entity string) error {
	if pgErr := asPgUniqueViolation(err); pgErr != nil {
		return errors.NewTxExistsError("Transaction already exists in postgres store (coinbase=%v): %v", isCoinbase, err)
	}
	if sqliteErr, ok := err.(*sqlite.Error); ok && sqliteErr.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE {
		return errors.NewTxExistsError("Transaction already exists in sqlite store (coinbase=%v): %v", isCoinbase, sqliteErr)
	}
	return errors.NewStorageError("Failed to insert %s", entity, err)
}

// asPgUniqueViolation checks if err is a PostgreSQL unique constraint violation (23505)
// from either pgx (pgconn.PgError) or lib/pq (pq.Error).
func asPgUniqueViolation(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == usql.PgErrUniqueViolation {
		return pgErr
	}
	if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == usql.PgErrUniqueViolation {
		return pqErr
	}
	return nil
}

func (s *Store) updateParentConflictingChildren(ctx context.Context, transactionID int, tx *bt.Tx, txn *sql.Tx) error {
	// update all the parents to have this transaction as a conflicting child
	// do not fail if already exists
	q := `
			INSERT INTO conflicting_children (
			 transaction_id, child_transaction_id
			) VALUES (
			 (SELECT id FROM transactions WHERE hash = $1),
			 $2
			)
			ON CONFLICT DO NOTHING
		`

	for _, input := range tx.Inputs {
		if _, err := txn.ExecContext(ctx, q, input.PreviousTxIDChainHash()[:], transactionID); err != nil {
			return errors.NewStorageError("Failed to insert conflicting_children", err)
		}
	}

	return nil
}

func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	// Always use unbatched path for GetMeta — it's called infrequently
	// and batchDecorateChunk has a known issue where TxInpoints in MetaFields
	// causes data.Tx to be set when it shouldn't be.
	result, err := s.getUnbatched(ctx, hash, utxo.MetaFields)
	if err != nil {
		return err
	}

	if result != nil {
		*data = *result
	}

	return nil
}

// Get retrieves transaction metadata and optionally the full transaction data.
// The fields parameter controls which data is returned:
//   - tx: Full transaction data
//   - inputs: Transaction inputs
//   - outputs: Transaction outputs
//   - blockIDs: Block references
//   - parentTxHashes: Previous transaction hashes
func (s *Store) Get(ctx context.Context, hash *chainhash.Hash, fields ...fields.FieldName) (*meta.Data, error) {
	bins := utxo.MetaFieldsWithTx
	if len(fields) > 0 {
		bins = fields
	}

	return s.get(ctx, hash, bins)
}

func (s *Store) get(ctx context.Context, hash *chainhash.Hash, bins []fields.FieldName) (*meta.Data, error) {
	prometheusUtxoGet.Inc()

	// Use batcher for the common validator path (BlockIDs, BlockHeights, Tx, Inputs, Outputs).
	// Fall back to unbatched for fields that BatchDecorate doesn't support
	// (ConflictingChildren, Utxos) to avoid missing data.
	if s.getBatcher != nil && !contains(bins, fields.ConflictingChildren) && !contains(bins, fields.Utxos) {
		return s.getBatched(ctx, hash, bins)
	}

	return s.getUnbatched(ctx, hash, bins)
}

// getBatched queues a Get request into the batcher for bulk processing via BatchDecorate.
func (s *Store) getBatched(ctx context.Context, hash *chainhash.Hash, bins []fields.FieldName) (*meta.Data, error) {
	done := make(chan batchGetItemData, 1)
	item := &batchGetItem{hash: *hash, fields: bins, done: done}

	s.getBatcher.PutCtx(ctx, item)

	select {
	case data := <-done:
		return data.Data, data.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// sendGetBatch is the batcher callback that processes a batch of get operations
// in bulk SQL queries via BatchDecorate.
func (s *Store) sendGetBatch(batch []*batchGetItem) {
	items := make([]*utxo.UnresolvedMetaData, 0, len(batch))

	// Collect union of all requested fields across the batch
	fieldSet := make(map[fields.FieldName]struct{})
	for idx, item := range batch {
		items = append(items, &utxo.UnresolvedMetaData{
			Hash:   item.hash,
			Idx:    idx,
			Fields: item.fields,
		})
		for _, f := range item.fields {
			fieldSet[f] = struct{}{}
		}
	}

	allFields := make([]fields.FieldName, 0, len(fieldSet))
	for f := range fieldSet {
		allFields = append(allFields, f)
	}

	if err := s.BatchDecorate(s.ctx, items, allFields...); err != nil {
		for _, bItem := range batch {
			bItem.done <- batchGetItemData{Err: err}
		}
		return
	}

	for _, item := range items {
		batch[item.Idx].done <- batchGetItemData{
			Data: item.Data,
			Err:  item.Err,
		}
	}
}

func (s *Store) getUnbatched(ctx context.Context, hash *chainhash.Hash, bins []fields.FieldName) (*meta.Data, error) {

	// Always get the transaction row

	q := `
	  SELECT
		 id
		,version
		,lock_time
		,fee
		,size_in_bytes
		,coinbase
		,frozen
		,conflicting
		,locked
		,unmined_since
		,inserted_at
		FROM transactions
		WHERE hash = $1
	`

	data := &meta.Data{}

	var (
		id                int
		version           uint32
		lockTime          uint32
		spendingDataBytes []byte
		unminedSince      sql.NullInt64
		// inserted_at is TIMESTAMPTZ on postgres but TEXT on sqlite (see schema
		// in transactions table init). Postgres driver returns time.Time;
		// sqlite returns string. Scan into interface{} and branch on type.
		insertedAt any
	)

	err := s.db.QueryRowContext(ctx, q, hash[:]).Scan(&id, &version, &lockTime, &data.Fee, &data.SizeInBytes, &data.IsCoinbase, &data.Frozen, &data.Conflicting, &data.Locked, &unminedSince, &insertedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewTxNotFoundError("transaction %s not found", hash, err)
		}

		return nil, err
	}

	// Set UnminedSince from nullable field
	if unminedSince.Valid {
		const maxUint32 = 0xFFFFFFFF
		if unminedSince.Int64 >= 0 && unminedSince.Int64 <= maxUint32 {
			data.UnminedSince = uint32(unminedSince.Int64)
		}
	}

	// CreatedAt mirrors the aerospike bin: Unix milliseconds, set once at insert.
	// Always populate so callers like selectCountersForDemotedTx have it without
	// having to add fields.CreatedAt to every Get callsite that already passes
	// other field selectors.
	data.CreatedAt = parseInsertedAtMillis(insertedAt)

	tx := bt.Tx{
		Version:  version,
		LockTime: lockTime,
	}

	if contains(bins, fields.Tx) || contains(bins, fields.Inputs) || contains(bins, fields.TxInpoints) || contains(bins, fields.Utxos) {
		q := `
			SELECT
			 previous_transaction_hash
			,previous_tx_idx
			,previous_tx_satoshis
			,previous_tx_script
			,unlocking_script
			,sequence_number
			FROM inputs
			WHERE transaction_id = $1
			ORDER BY idx
		`

		rows, err := s.db.QueryContext(ctx, q, id)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			input := &bt.Input{}

			var previousTxHashBytes []byte
			var previousTxIdx int64

			if err := rows.Scan(&previousTxHashBytes, &previousTxIdx, &input.PreviousTxSatoshis, &input.PreviousTxScript, &input.UnlockingScript, &input.SequenceNumber); err != nil {
				return nil, err
			}
			input.PreviousTxOutIndex = uint32(previousTxIdx)

			previousTxHash, err := chainhash.NewHash(previousTxHashBytes)
			if err != nil {
				return nil, err
			}

			if err := input.PreviousTxIDAdd(previousTxHash); err != nil {
				return nil, err
			}

			tx.Inputs = append(tx.Inputs, input)
		}
	}

	if contains(bins, fields.Tx) || contains(bins, fields.Outputs) || contains(bins, fields.Utxos) {
		q := `SELECT locking_script, satoshis FROM outputs WHERE transaction_id = $1 ORDER BY idx`

		rows, err := s.db.QueryContext(ctx, q, id)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			output := &bt.Output{}

			if err := rows.Scan(&output.LockingScript, &output.Satoshis); err != nil {
				return nil, err
			}

			tx.Outputs = append(tx.Outputs, output)
		}
	}

	if contains(bins, fields.BlockIDs) {
		q := `
			SELECT
			    block_id,
				block_height,
				subtree_idx
			FROM block_ids
			WHERE transaction_id = $1
			ORDER BY block_id
		`

		rows, err := s.db.QueryContext(ctx, q, id)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var (
				blockID     uint32
				blockHeight uint32
				subtreeIdx  int
			)

			if err := rows.Scan(&blockID, &blockHeight, &subtreeIdx); err != nil {
				return nil, err
			}

			data.BlockIDs = append(data.BlockIDs, blockID)
			data.BlockHeights = append(data.BlockHeights, blockHeight)
			data.SubtreeIdxs = append(data.SubtreeIdxs, subtreeIdx)
		}
	}

	if contains(bins, fields.ConflictingChildren) {
		q := `
			SELECT conflicting_t.hash
			FROM conflicting_children c
			INNER JOIN transactions t ON c.transaction_id = t.id
			INNER JOIN transactions conflicting_t ON c.child_transaction_id = conflicting_t.id
			WHERE t.hash = $1
		`

		txHashStr := hex.EncodeToString(hash[:])
		s.logger.Infof("Getting conflicting children for tx %s", txHashStr)

		rows, err := s.db.QueryContext(ctx, q, hash[:])
		if err != nil {
			return nil, err
		}

		defer rows.Close()

		data.ConflictingChildren = make([]chainhash.Hash, 0, 16)

		for rows.Next() {
			if err = rows.Scan(&spendingDataBytes); err != nil {
				return nil, err
			}

			data.ConflictingChildren = append(data.ConflictingChildren, chainhash.Hash(spendingDataBytes))
		}
	}

	if contains(bins, fields.Utxos) {
		var (
			idx    int
			frozen bool
		)

		// get all the spending tx ids for this tx
		q := `
			SELECT o.idx, o.spending_data, o.frozen
			FROM transactions as t, outputs as o
			WHERE t.hash = $1
			  AND t.id = o.transaction_id
			ORDER BY o.idx
		`

		rows, err := s.db.QueryContext(ctx, q, hash[:])
		if err != nil {
			return nil, err
		}

		defer rows.Close()

		data.SpendingDatas = make([]*spendpkg.SpendingData, len(tx.Outputs)) // needs to be nullable

		for rows.Next() {
			if err = rows.Scan(&idx, &spendingDataBytes, &frozen); err != nil {
				return nil, err
			}

			if data.Frozen || frozen {
				data.SpendingDatas[idx] = spendpkg.NewSpendingData(&subtree.FrozenBytesTxHash, idx)
			} else if spendingDataBytes != nil {
				data.SpendingDatas[idx], err = spendpkg.NewSpendingDataFromBytes(spendingDataBytes)
				if err != nil {
					return nil, errors.NewProcessingError("failed to create hash from bytes", err)
				}
			} else {
				data.SpendingDatas[idx] = nil
			}
		}
	}

	if contains(bins, fields.Tx) {
		data.Tx = &tx
	}

	if contains(bins, fields.TxInpoints) {
		data.TxInpoints, err = subtree.NewTxInpointsFromInputs(tx.Inputs)
		if err != nil {
			return nil, errors.NewProcessingError("failed to create tx inpoints from inputs", err)
		}
	}

	return data, nil
}

func contains(slice []fields.FieldName, item fields.FieldName) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}

	return false
}

// parseInsertedAtMillis converts the inserted_at column value into Unix
// milliseconds. Postgres' driver returns time.Time; the sqlite driver
// returns a string (the layout depends on whether DEFAULT CURRENT_TIMESTAMP
// produced a "YYYY-MM-DD HH:MM:SS[.fff]" form). Returns 0 when the value is
// nil, an unrecognised type, or unparseable — callers treat 0 as "no
// CreatedAt hint available" (see isOlderCounter's missing-timestamp branch).
func parseInsertedAtMillis(v any) int64 {
	switch t := v.(type) {
	case nil:
		return 0
	case time.Time:
		return t.UnixMilli()
	case []byte:
		return parseInsertedAtMillis(string(t))
	case string:
		// Sqlite CURRENT_TIMESTAMP emits SQL-style "YYYY-MM-DD HH:MM:SS" by
		// default; tolerate fractional seconds and trailing 'Z' for safety.
		layouts := []string{
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05",
			time.RFC3339Nano,
			time.RFC3339,
		}

		for _, l := range layouts {
			if ts, err := time.Parse(l, t); err == nil {
				return ts.UnixMilli()
			}
		}

		return 0
	default:
		return 0
	}
}

// handleSpendPanic processes a recovered value from Spend's deferred recover
// and propagates it as an error. Without this, a panic during Spend would be
// logged but the caller would observe (nil, nil) — a silent failure that can
// mask UTXO state corruption.
//
// Uses ERR_UNKNOWN rather than ERR_PROCESSING so the block-validation retry
// classifier (services/blockvalidation/BlockValidation.go) does not treat a
// recovered panic as a transient infrastructure error and retry indefinitely
// against a broken path.
func handleSpendPanic(recovered any, err *error, logger ulogger.Logger) {
	if recovered == nil {
		return
	}

	prometheusUtxoErrors.WithLabelValues("Spend", "Failed Spend Cleaning").Inc()
	logger.Errorf("ERROR panic in sql Spend: %v\n%s", recovered, debug.Stack())

	if *err == nil {
		*err = errors.NewUnknownError("panic in Spend: %v", recovered)
	}
}

// Spend marks UTXOs as spent by updating their spending transaction ID.
// It performs several validations:
//   - Checks if the UTXO exists
//   - Verifies the UTXO is not frozen
//   - Confirms the UTXO matches the expected hash
//   - Validates coinbase maturity
//   - Ensures the UTXO is not already spent
//   - Optionally marks the transaction for cleanup if all outputs are spent
//
// The blockHeight parameter is used for coinbase maturity checking.
// If blockHeight is 0, the current block height is used.
func (s *Store) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) (spends []*utxo.Spend, err error) {
	defer func() {
		handleSpendPanic(recover(), &err, s.logger)
	}()

	if blockHeight == 0 {
		return nil, errors.NewProcessingError("blockHeight must be greater than zero")
	}

	useIgnoreConflicting := len(ignoreFlags) > 0 && ignoreFlags[0].IgnoreConflicting
	useIgnoreLocked := len(ignoreFlags) > 0 && ignoreFlags[0].IgnoreLocked

	spends, err = utxo.GetSpends(tx)
	if err != nil {
		return nil, err
	}

	if len(spends) == 0 {
		return nil, errors.NewProcessingError("No spends provided", nil)
	}

	// Mirrors aerospike spend.go:287-420 — enqueue each spend into the batcher,
	// wait for batch callback to signal completion via errCh.
	var (
		mu              sync.Mutex
		txAlreadyExists bool
		spentSpends     = make([]*utxo.Spend, 0, len(spends))
		g               errgroup.Group
	)

	// Cap per-tx concurrency into the spend batcher. Without this, a single tx
	// with tens of thousands of inputs (e.g. a consolidation) can flood the
	// batcher queue faster than its serialised callback can drain it, producing
	// a single bulk SELECT whose parameter count exceeds PostgreSQL's 65535
	// extended-protocol limit. Using SpendBatcherSize*SpendBatcherConcurrency
	// matches the pattern used in services/legacy/netsync/handle_block.go.
	if limit := s.settings.UtxoStore.SpendBatcherSize * s.settings.UtxoStore.SpendBatcherConcurrency; limit > 0 {
		g.SetLimit(limit)
	}

	for idx, spend := range spends {
		if spend == nil {
			return nil, errors.NewProcessingError("spend should not be nil")
		}

		idx := idx
		spend := spend

		g.Go(func() error {
			// Per-worker panic recovery. The parent's defer only catches panics in the
			// parent goroutine — errgroup propagates errors but does not recover panics
			// inside g.Go bodies, so without this a worker panic would crash the process.
			defer func() {
				handleSpendPanic(recover(), &spends[idx].Err, s.logger)
			}()

			errCh := make(chan error, 1)
			s.spendBatcher.PutCtx(ctx, &batchSpend{
				spend:             spend,
				blockHeight:       blockHeight,
				errCh:             errCh,
				ignoreConflicting: useIgnoreConflicting,
				ignoreLocked:      useIgnoreLocked,
			})

			// Wait for batch response with timeout to prevent indefinite blocking
			spendTimeout := s.settings.UtxoStore.SpendWaitTimeout
			if spendTimeout <= 0 {
				spendTimeout = 30 * time.Second
			}

			timer := time.NewTimer(spendTimeout)
			defer timer.Stop()

			var batchErr error
			select {
			case batchErr = <-errCh:
				// Batch completed successfully or with error
			case <-ctx.Done():
				spends[idx].Err = errors.NewContextCanceledError("[SPEND][%s:%d] context canceled while waiting for batch response", spend.TxID.String(), spend.Vout)
				return nil
			case <-timer.C:
				prometheusUtxoErrors.WithLabelValues("Spend", "BatchTimeout").Inc()
				spends[idx].Err = errors.NewServiceUnavailableError("[SPEND][%s:%d] batch operation timed out after %s", spend.TxID.String(), spend.Vout, spendTimeout)
				return nil
			}

			// Handle "already blessed" — parent tx not found but spending tx exists.
			// Mirrors aerospike spend.go:343-361.
			if batchErr != nil && errors.Is(batchErr, errors.ErrTxNotFound) {
				mu.Lock()
				exists := txAlreadyExists
				mu.Unlock()

				if exists {
					batchErr = nil
				} else {
					var spendingTxExists bool
					if scanErr := s.db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM transactions WHERE hash = $1)", tx.TxIDChainHash()[:]).Scan(&spendingTxExists); scanErr != nil {
						s.logger.Errorf("[Spend][%s] failed to check if spending tx exists: %v", tx.TxID(), scanErr)
					}
					if spendingTxExists {
						s.logger.Warnf("[Spend][%s] parent tx not found, but tx already exists in store, assuming already blessed", tx.TxID())
						batchErr = nil

						mu.Lock()
						txAlreadyExists = true
						mu.Unlock()
					}
				}
			}

			if batchErr != nil {
				spends[idx].Err = batchErr

				s.logger.Debugf("[SPEND][%s:%d] error in sql spend: %+v", spend.TxID.String(), spend.Vout, batchErr)

				var errSpent *errors.UtxoSpentErrData
				if errors.AsData(batchErr, &errSpent) {
					spends[idx].ConflictingTxID = errSpent.SpendingData.TxID
				}

				return nil
			}

			mu.Lock()
			spentSpends = append(spentSpends, spend)
			mu.Unlock()

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return nil, errors.NewError("error in sql spend (batched mode)", err)
	}

	if len(spends) != len(spentSpends) {
		// Rollback successful spends when the transaction has genuine validation failures
		// (double-spend, frozen, conflicting, hash mismatch). For transient errors, skip
		// rollback — the optimistic locking makes spends idempotent for the same spender.
		if needsSpendRollback(spends) {
			if unspendErr := s.Unspend(context.Background(), spentSpends); unspendErr != nil {
				s.logger.Errorf("error in sql unspend (batched mode): %v", unspendErr)
			}
		}

		var spendErrors error
		for _, spend := range spends {
			if spend.Err != nil {
				if spendErrors != nil {
					spendErrors = errors.Join(spendErrors, spend.Err)
				} else {
					spendErrors = spend.Err
				}
			}
		}

		return spends, errors.NewUtxoError("error in sql spend (batched mode) - errors", spendErrors)
	}

	prometheusUtxoSpend.Add(float64(len(spends)))

	return spends, nil
}

// needsSpendRollback returns true if any spend failed due to a validation error
// that indicates the transaction is genuinely invalid. Mirrors aerospike/spend.go.
func needsSpendRollback(spends []*utxo.Spend) bool {
	for _, spend := range spends {
		if spend.Err == nil {
			continue
		}
		if errors.Is(spend.Err, errors.ErrSpent) ||
			errors.Is(spend.Err, errors.ErrTxConflicting) ||
			errors.Is(spend.Err, errors.ErrFrozen) ||
			errors.Is(spend.Err, errors.ErrUtxoHashMismatch) {
			return true
		}
	}
	return false
}

// isDeadlock checks if a database error is a PostgreSQL deadlock (SQLSTATE 40P01)
// or a SQLite BUSY error that should be retried.
func isDeadlock(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == usql.PgErrDeadlockDetected {
		return true
	}
	return strings.Contains(err.Error(), "database is locked")
}

// sendSpendBatch is the batcher callback that processes a batch of spend operations
// in a single DB transaction. Called by the go-batcher when the batch is full or
// the timeout fires. Same SQL for both PostgreSQL and SQLite.
// Mirrors aerospike/spend.go sendSpendBatchLua.
//
// Single-phase design: SELECT + UPDATE outputs (validates and marks each output as spent).
// DAH updates are handled separately by SetMinedMulti and setDAH, not during Spend.
//
// The batcher is configured with background=false so batch callbacks are serialized.
// This prevents PostgreSQL deadlocks that occur when concurrent batches lock
// overlapping output rows (same transaction_id, different idx) in different orders.
// Aerospike doesn't need this because it uses optimistic single-key operations
// without DB-level row locking.
func (s *Store) sendSpendBatch(batch []*batchSpend) {
	batch = utxo.FilterConflictingDuplicateSpendClaims(batch,
		func(item *batchSpend) *utxo.Spend {
			if item == nil {
				return nil
			}
			return item.spend
		},
		func(item *batchSpend, err error) {
			item.errCh <- err
		},
	)
	if len(batch) == 0 {
		return
	}

	const maxRetries = 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		retryable := s.trySendSpendBatch(batch)
		if !retryable {
			return
		}
		s.logger.Warnf("[Spend] deadlock detected (attempt %d/%d), retrying batch of %d items", attempt+1, maxRetries, len(batch))
		time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
	}
	// Exhausted retries — send error to all items
	for _, item := range batch {
		item.errCh <- errors.NewStorageError("[Spend] deadlock persisted after %d retries", maxRetries)
	}
}

// trySendSpendBatch attempts to process a spend batch in a single DB transaction.
// Returns true if the error was a retryable deadlock and the batch should be retried.
// Returns false if the batch completed (success or non-retryable error) and results
// have been sent to all item errCh channels.
func (s *Store) trySendSpendBatch(batch []*batchSpend) (retryable bool) {
	if s.settings.UtxoStore.BatchSQLOperations && s.engine == "postgres" {
		return s.trySendSpendBatchBulk(batch)
	}
	return s.trySendSpendBatchPerRow(batch)
}

// spendSelectResult holds the result of a bulk SELECT for a single spend item.
type spendSelectResult struct {
	batchIdx               int
	transactionID          int
	coinbaseSpendingHeight uint32
	utxoHash               []byte
	spendingDataBytes      []byte
	frozen                 bool
	conflicting            bool
	locked                 bool
	spendableIn            *uint32
}

// trySendSpendBatchBulk uses bulk SELECT + bulk UPDATE for PostgreSQL.
func (s *Store) trySendSpendBatchBulk(batch []*batchSpend) (retryable bool) {
	txn, err := s.db.BeginTx(s.ctx, nil)
	if err != nil {
		for _, item := range batch {
			item.errCh <- errors.NewStorageError("[Spend] failed to begin transaction", err)
		}
		return false
	}
	defer func() {
		_ = txn.Rollback()
	}()

	// Phase 1: Bulk SELECT — fetch all output states in one query
	// Build VALUES list: (hash, idx, batch_idx)
	var sb strings.Builder
	sb.WriteString(`
		SELECT v.batch_idx,
		       o.transaction_id, o.coinbase_spending_height, o.utxo_hash,
		       o.spending_data, o.frozen OR t.frozen AS frozen, t.conflicting, t.locked, o.spendableIn
		FROM (VALUES `)
	args := make([]interface{}, 0, len(batch)*3)
	paramIdx := 1
	for i, item := range batch {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(fmt.Sprintf("($%d::bytea,$%d::int,$%d::int)", paramIdx, paramIdx+1, paramIdx+2))
		args = append(args, item.spend.TxID[:], item.spend.Vout, i)
		paramIdx += 3
	}
	sb.WriteString(`) AS v(hash,idx,batch_idx)
		JOIN transactions t ON t.hash = v.hash
		JOIN outputs o ON o.transaction_id = t.id AND o.idx = v.idx`)

	rows, err := txn.QueryContext(s.ctx, sb.String(), args...)
	if err != nil {
		if isDeadlock(err) {
			return true
		}
		for _, item := range batch {
			item.errCh <- errors.NewStorageError("[Spend] failed: bulk SELECT outputs", err)
		}
		return false
	}

	// Parse results into a map by batch index
	resultMap := make(map[int]*spendSelectResult, len(batch))
	for rows.Next() {
		r := &spendSelectResult{}
		if err := rows.Scan(&r.batchIdx, &r.transactionID, &r.coinbaseSpendingHeight,
			&r.utxoHash, &r.spendingDataBytes, &r.frozen, &r.conflicting, &r.locked, &r.spendableIn); err != nil {
			rows.Close()
			if isDeadlock(err) {
				return true
			}
			for _, item := range batch {
				item.errCh <- errors.NewStorageError("[Spend] failed: scanning bulk SELECT results", err)
			}
			return false
		}
		resultMap[r.batchIdx] = r
	}
	if err := rows.Close(); err != nil {
		if isDeadlock(err) {
			return true
		}
		for _, item := range batch {
			item.errCh <- errors.NewStorageError("[Spend] failed: closing bulk SELECT results", err)
		}
		return false
	}

	// Phase 2: Validate each item and build the bulk UPDATE set
	validationErrors := make(map[int]error, len(batch))
	type updateItem struct {
		batchIdx      int
		transactionID int
		vout          uint32
		spendingData  []byte
	}
	var toUpdate []updateItem
	// Parent tx IDs from idempotent re-spends (output already carries matching
	// spending_data). The CTE-based DAH recompute below only covers parents
	// touched by the bulk UPDATE, so it would miss these. Recording them here
	// lets us run a post-UPDATE DAH recompute so parents whose DAH was left
	// NULL by a pre-fix spend still get healed — matching the per-row path.
	idempotentParentIDs := make(map[int]struct{})
	// Note: we intentionally do NOT track conflicting parents as a separate
	// set. The post-UPDATE setDAH loop below already runs for every parent
	// that successfully had an output spent (via dedupedUpdate+updatedSet) or
	// idempotent-matched (via idempotentParentIDs), and setDAH internally
	// handles conflicting-vs-non-conflicting DAH semantics. Because we added
	// "AND NOT t.conflicting" to the CTE's dah_upd clause, conflicting parents
	// are exclusively routed through setDAH — no duplication, no risk of
	// calling setDAH for a parent whose items all ended up in validationErrors.

	for i, item := range batch {
		spend := item.spend
		r, found := resultMap[i]
		if !found {
			validationErrors[i] = errors.NewTxNotFoundError("output %s:%d not found", spend.TxID, spend.Vout)
			continue
		}

		if r.frozen {
			validationErrors[i] = errors.NewUtxoFrozenError("[Spend] utxo is frozen for %s:%d", spend.TxID, spend.Vout)
			continue
		}
		if r.conflicting && !item.ignoreConflicting {
			validationErrors[i] = errors.NewTxConflictingError("[Spend] tx is conflicting for %s:%d", spend.TxID, spend.Vout)
			continue
		}
		if r.locked && !item.ignoreLocked {
			validationErrors[i] = errors.NewTxLockedError("[Spend] utxo is not spendable for %s:%d", spend.TxID, spend.Vout)
			continue
		}
		if r.spendableIn != nil && *r.spendableIn > 0 && item.blockHeight < *r.spendableIn {
			validationErrors[i] = errors.NewTxLockedError("[Spend] utxo %s:%d is not spendable until %d", spend.TxID, spend.Vout, *r.spendableIn)
			continue
		}

		// Check if already spent by a different transaction
		if len(r.spendingDataBytes) > 0 {
			if spend.SpendingData != nil && !bytes.Equal(r.spendingDataBytes, spend.SpendingData.Bytes()) {
				existingSpendData, parseErr := spendpkg.NewSpendingDataFromBytes(r.spendingDataBytes)
				if parseErr != nil {
					validationErrors[i] = errors.NewProcessingError("failed to create spending data from bytes", parseErr)
					continue
				}
				validationErrors[i] = errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, existingSpendData)
				continue
			}
			// Idempotent re-spend: same spending data — treat as success without UPDATE.
			// Record the parent so DAH can still be (re)evaluated and heal NULL DAHs
			// left by spends that happened before the DAH-on-spend fix landed.
			idempotentParentIDs[r.transactionID] = struct{}{}
			continue
		}

		if !bytes.Equal(r.utxoHash, spend.UTXOHash[:]) {
			validationErrors[i] = errors.NewUtxoHashMismatchError("[Spend] utxo hash mismatch for %s:%d", spend.TxID, spend.Vout)
			continue
		}
		if r.coinbaseSpendingHeight > 0 && r.coinbaseSpendingHeight > item.blockHeight {
			validationErrors[i] = errors.NewTxCoinbaseImmatureError("[Spend] coinbase utxo not ready to spend for %s:%d, requires height %d, current %d", spend.TxID, spend.Vout, r.coinbaseSpendingHeight, item.blockHeight)
			continue
		}

		toUpdate = append(toUpdate, updateItem{
			batchIdx:      i,
			transactionID: r.transactionID,
			vout:          spend.Vout,
			spendingData:  spend.SpendingData.Bytes(),
		})
	}

	// Phase 3: Deduplicate toUpdate entries targeting the same (transactionID, vout).
	// Within a single UPDATE statement, PostgreSQL can only affect each row once.
	// Duplicate entries fall into two categories:
	//   1. Same spending data (same tx, idempotent re-spend): drop duplicate, mark successful after UPDATE.
	//   2. Different spending data (two competing txs, double-spend): the first arrival wins; record
	//      the second as UTXO_SPENT immediately — do NOT let it silently succeed.
	type utxoKey struct {
		transactionID int
		vout          uint32
	}
	type seenEntry struct {
		batchIdx     int
		spendingData []byte
	}
	seenKeys := make(map[utxoKey]seenEntry, len(toUpdate)) // key -> first entry
	var dedupedUpdate []updateItem
	for _, u := range toUpdate {
		key := utxoKey{u.transactionID, u.vout}
		if entry, seen := seenKeys[key]; seen {
			if !bytes.Equal(entry.spendingData, u.spendingData) {
				// Two different transactions competing for the same UTXO in this batch.
				// First arrival wins; record the second as a double-spend now.
				spend := batch[u.batchIdx].spend
				existingSpendData, parseErr := spendpkg.NewSpendingDataFromBytes(entry.spendingData)
				if parseErr != nil {
					validationErrors[u.batchIdx] = errors.NewProcessingError("failed to create spending data from bytes", parseErr)
				} else {
					validationErrors[u.batchIdx] = errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, existingSpendData)
				}
			}
			// else: same spending data — idempotent re-spend, will be marked successful after UPDATE
		} else {
			seenKeys[key] = seenEntry{batchIdx: u.batchIdx, spendingData: u.spendingData}
			dedupedUpdate = append(dedupedUpdate, u)
		}
	}

	// Bulk UPDATE with optimistic locking.
	// When retention > 0, the UPDATE is wrapped in a CTE that also runs a DAH
	// recompute on any parent tx whose last unspent output was just drained.
	// Mirrors aerospike's inline setDeleteAtHeight Lua call; without this, mined
	// txs spent gradually over time never get a DAH and disk usage grows unbounded.
	updatedSet := make(map[int]bool) // batchIdx -> updated
	if len(dedupedUpdate) > 0 {
		retention := s.settings.GetUtxoStoreBlockHeightRetention()
		wrapDAH := retention > 0

		var ub strings.Builder
		if wrapDAH {
			ub.WriteString(`WITH v(transaction_id,idx,spending_data,batch_idx) AS (VALUES `)
		} else {
			ub.WriteString(`
			UPDATE outputs o
			SET spending_data = v.spending_data
			FROM (VALUES `)
		}
		updateArgs := make([]interface{}, 0, len(dedupedUpdate)*4+1)
		pidx := 1
		for j, u := range dedupedUpdate {
			if j > 0 {
				ub.WriteByte(',')
			}
			ub.WriteString(fmt.Sprintf("($%d::int,$%d::int,$%d::bytea,$%d::int)", pidx, pidx+1, pidx+2, pidx+3))
			updateArgs = append(updateArgs, u.transactionID, u.vout, u.spendingData, u.batchIdx)
			pidx += 4
		}
		if wrapDAH {
			newDAH := int64(s.blockHeight.Load() + 1 + retention)
			dahIdx := pidx
			updateArgs = append(updateArgs, newDAH)
			// Postgres CTEs share a snapshot, so the count(*) over outputs inside
			// dah_upd sees the PRE-update state. "unspent-before == spent_in_batch"
			// ⇔ this batch just drained the tx's last unspent output.
			//
			// We split the UPDATE into two sibling CTEs:
			//   * upd_spent — only rows that were genuinely NULL before this UPDATE,
			//     i.e. truly spent by this batch. These feed the drain check.
			//   * upd_idem  — rows that were already spent with matching spending_data
			//     at statement-snapshot time (idempotent re-spend). Reported as
			//     "updated" so the caller sees success, but NOT counted in
			//     spent_in_batch — otherwise the count would exceed the real
			//     pre-state unspent count and the drain check would falsely miss.
			//
			// Edge case: if a concurrent transaction commits matching
			// spending_data AFTER our statement snapshot is taken, neither CTE
			// catches it (upd_spent's EPQ recheck sees non-NULL, upd_idem's
			// snapshot predicate sees NULL). That race is caught by a second
			// fresh-snapshot re-check after the statement — see the
			// concurrent-idempotent re-check below.
			ub.WriteString(fmt.Sprintf(`),
			upd_spent AS (
				UPDATE outputs o SET spending_data = v.spending_data FROM v
				WHERE o.transaction_id = v.transaction_id AND o.idx = v.idx
				  AND o.spending_data IS NULL
				RETURNING v.batch_idx, o.transaction_id
			),
			upd_idem AS (
				SELECT v.batch_idx
				FROM v
				JOIN outputs o
				  ON o.transaction_id = v.transaction_id AND o.idx = v.idx
				WHERE o.spending_data = v.spending_data
			),
			parents AS (
				SELECT transaction_id, count(*) AS spent_in_batch FROM upd_spent GROUP BY transaction_id
			),
			-- Aggregate the pre-update unspent count for every touched parent in
			-- one scan over outputs (driven by the parents set). Avoids the
			-- correlated subquery-per-parent pattern the drain check used to
			-- issue, which scaled with #parents × index lookup rather than
			-- #touched-outputs.
			unspent_before AS (
				SELECT o.transaction_id, count(*) AS n_unspent
				FROM outputs o
				JOIN parents p ON p.transaction_id = o.transaction_id
				WHERE o.spending_data IS NULL
				GROUP BY o.transaction_id
			),
			dah_upd AS (
				UPDATE transactions t SET delete_at_height = $%d
				FROM parents p
				LEFT JOIN unspent_before u ON u.transaction_id = p.transaction_id
				WHERE t.id = p.transaction_id
				  AND t.preserve_until IS NULL
				  AND t.unmined_since IS NULL
				  AND NOT t.conflicting
				  AND EXISTS (SELECT 1 FROM block_ids bi WHERE bi.transaction_id = t.id)
				  AND COALESCE(u.n_unspent, 0) = p.spent_in_batch
				  AND (t.delete_at_height IS NULL OR t.delete_at_height < $%d)
				RETURNING 1
			)
			SELECT batch_idx FROM upd_spent
			UNION ALL
			SELECT batch_idx FROM upd_idem`, dahIdx, dahIdx))
		} else {
			ub.WriteString(`) AS v(transaction_id,idx,spending_data,batch_idx)
			WHERE o.transaction_id = v.transaction_id AND o.idx = v.idx
			AND (o.spending_data IS NULL OR o.spending_data = v.spending_data)
			RETURNING v.batch_idx`)
		}

		uRows, err := txn.QueryContext(s.ctx, ub.String(), updateArgs...)
		if err != nil {
			if isDeadlock(err) {
				return true
			}
			for i, item := range batch {
				if valErr, ok := validationErrors[i]; ok {
					item.errCh <- valErr
				} else {
					item.errCh <- errors.NewStorageError("[Spend] failed: bulk UPDATE outputs", err)
				}
			}
			return false
		}

		for uRows.Next() {
			var bIdx int
			if err := uRows.Scan(&bIdx); err != nil {
				uRows.Close()
				if isDeadlock(err) {
					return true
				}
				for _, item := range batch {
					item.errCh <- errors.NewStorageError("[Spend] failed: scanning bulk UPDATE results", err)
				}
				return false
			}
			updatedSet[bIdx] = true
		}
		if err := uRows.Close(); err != nil {
			if isDeadlock(err) {
				return true
			}
			for _, item := range batch {
				item.errCh <- errors.NewStorageError("[Spend] failed: closing bulk UPDATE results", err)
			}
			return false
		}

		// Check for items that were not updated (concurrent spend between SELECT and UPDATE)
		// First pass: items absent from upd_spent AND upd_idem are candidates
		// for UtxoSpentError. Gather them for a concurrent-idempotent re-check
		// against a fresh snapshot before we finalise the error.
		var missedIdxs []int
		for _, u := range dedupedUpdate {
			if !updatedSet[u.batchIdx] {
				missedIdxs = append(missedIdxs, u.batchIdx)
			}
		}

		// The bulk UPDATE and the sibling upd_idem CTE share one statement
		// snapshot. If a concurrent transaction commits the SAME spending_data
		// on one of our target rows after our snapshot is taken, upd_spent's
		// EPQ recheck will reject the row (spending_data no longer NULL) and
		// upd_idem's snapshot-level predicate won't match it (NULL at snapshot,
		// filter is `= v.spending_data`). The row ends up in neither RETURNING
		// set and would be wrongly marked as UtxoSpentError.
		//
		// Run a second, fresh-snapshot SELECT over the missed rows to catch
		// those concurrent idempotent commits. Anything whose current
		// spending_data matches ours is a successful idempotent spend.
		if len(missedIdxs) > 0 {
			var sb strings.Builder
			sb.WriteString(`SELECT v.batch_idx FROM (VALUES `)
			args := make([]interface{}, 0, len(missedIdxs)*4)
			pidx := 1
			for i, bIdx := range missedIdxs {
				if i > 0 {
					sb.WriteByte(',')
				}
				sb.WriteString(fmt.Sprintf("($%d::int,$%d::int,$%d::bytea,$%d::int)", pidx, pidx+1, pidx+2, pidx+3))
				spend := batch[bIdx].spend
				r := resultMap[bIdx]
				args = append(args, r.transactionID, spend.Vout, spend.SpendingData.Bytes(), bIdx)
				pidx += 4
			}
			sb.WriteString(`) AS v(transaction_id,idx,spending_data,batch_idx)
			JOIN outputs o ON o.transaction_id = v.transaction_id AND o.idx = v.idx
			WHERE o.spending_data = v.spending_data`)

			iRows, err := txn.QueryContext(s.ctx, sb.String(), args...)
			if err != nil {
				if isDeadlock(err) {
					return true
				}
				for _, item := range batch {
					item.errCh <- errors.NewStorageError("[Spend] failed: concurrent-idempotent re-check", err)
				}
				return false
			}
			for iRows.Next() {
				var bIdx int
				if err := iRows.Scan(&bIdx); err != nil {
					iRows.Close()
					if isDeadlock(err) {
						return true
					}
					for _, item := range batch {
						item.errCh <- errors.NewStorageError("[Spend] failed: scanning concurrent-idempotent re-check", err)
					}
					return false
				}
				updatedSet[bIdx] = true
				// Parent has just had its output confirmed-spent by someone
				// else with our exact spending_data. Treat it as an idempotent
				// match for DAH-healing purposes, same as the in-statement path.
				idempotentParentIDs[resultMap[bIdx].transactionID] = struct{}{}
			}
			if err := iRows.Close(); err != nil {
				if isDeadlock(err) {
					return true
				}
				for _, item := range batch {
					item.errCh <- errors.NewStorageError("[Spend] failed: closing concurrent-idempotent re-check", err)
				}
				return false
			}
		}

		// Anything still not in updatedSet after the re-check is a genuine
		// UtxoSpentError (row was concurrently spent by a DIFFERENT spender).
		for _, u := range dedupedUpdate {
			if !updatedSet[u.batchIdx] {
				spend := batch[u.batchIdx].spend
				validationErrors[u.batchIdx] = errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, spend.SpendingData)
			}
		}
		// Mark idempotent duplicate batch entries as successful (same UTXO, same spending data).
		// Double-spend duplicates (different spending data) are already in validationErrors — skip them.
		for _, u := range toUpdate {
			key := utxoKey{u.transactionID, u.vout}
			if entry, ok := seenKeys[key]; ok && entry.batchIdx != u.batchIdx {
				if bytes.Equal(entry.spendingData, u.spendingData) && updatedSet[entry.batchIdx] {
					updatedSet[u.batchIdx] = true
				}
			}
		}
	}

	// Post-UPDATE DAH recompute via setDAH for every parent touched by this
	// bulk batch. The CTE's dah_upd clause is an in-statement fast path that
	// succeeds in the single-writer case, but it can miss DAH under concurrent
	// spends of different outputs of the same parent: each statement only sees
	// its own snapshot, so both observe unspent_before > spent_in_batch, skip
	// dah_upd, and leave DAH NULL even though the parent is fully drained
	// after both commits. Calling setDAH afterwards runs a fresh statement
	// under READ COMMITTED that sees (a) the latest committed writes of any
	// concurrent spend plus (b) this transaction's own pending writes — so
	// whoever commits last correctly deterministically sets DAH.
	//
	// setDAH is idempotent w.r.t. the CTE: if the CTE already set DAH to
	// newDAH, setDAH sees conditions met + existingDAH == newDAH and writes
	// the same value (no-op). Extra round-trip cost is O(distinct parents),
	// bounded by batch size.
	//
	// The set also includes idempotent parents (Phase 1 SELECT already showed
	// the output spent with matching spending_data — the CTE skips them).
	// Conflicting parents are NOT tracked separately: they arrive via the
	// same dedupedUpdate / idempotent paths whenever a spend actually targets
	// them, and setDAH internally picks the conflicting-DAH semantics.
	touchedParents := make(map[int]struct{}, len(dedupedUpdate)+len(idempotentParentIDs))
	for _, u := range dedupedUpdate {
		// Only count parents whose UPDATE actually landed — avoids issuing
		// setDAH for rows lost to a concurrent spender (UtxoSpentError path).
		if updatedSet[u.batchIdx] {
			touchedParents[u.transactionID] = struct{}{}
		}
	}
	for p := range idempotentParentIDs {
		touchedParents[p] = struct{}{}
	}
	if s.settings.GetUtxoStoreBlockHeightRetention() > 0 && len(touchedParents) > 0 {
		// Sort parent IDs before calling setDAH so concurrent bulk batches
		// acquire row locks on `transactions` in a deterministic order. Map
		// iteration is randomised per-run, which under contention would
		// otherwise inflate deadlock rates.
		sortedParents := make([]int, 0, len(touchedParents))
		for parentID := range touchedParents {
			sortedParents = append(sortedParents, parentID)
		}
		sort.Ints(sortedParents)
		for _, parentID := range sortedParents {
			if err := s.setDAH(s.ctx, txn, parentID); err != nil {
				if isDeadlock(err) {
					return true
				}
				for i, item := range batch {
					if valErr, ok := validationErrors[i]; ok {
						item.errCh <- valErr
					} else {
						item.errCh <- errors.NewStorageError("[Spend] failed to recompute DAH for bulk spend fallback", err)
					}
				}
				return false
			}
		}
	}

	// Commit
	if err := txn.Commit(); err != nil {
		if isDeadlock(err) {
			return true
		}
		for i, item := range batch {
			if valErr, ok := validationErrors[i]; ok {
				item.errCh <- valErr
			} else {
				item.errCh <- errors.NewStorageError("[Spend] failed to commit transaction", err)
			}
		}
		return false
	}

	// Signal results
	for i, item := range batch {
		if valErr, ok := validationErrors[i]; ok {
			item.errCh <- valErr
		} else {
			item.errCh <- nil // success
		}
	}
	return false
}

// trySendSpendBatchPerRow processes a spend batch with per-row SELECT+UPDATE (original behavior).
// Used for SQLite or when BatchSQLOperations is disabled.
func (s *Store) trySendSpendBatchPerRow(batch []*batchSpend) (retryable bool) {
	txn, err := s.db.BeginTx(s.ctx, nil)
	if err != nil {
		for _, item := range batch {
			item.errCh <- errors.NewStorageError("[Spend] failed to begin transaction", err)
		}
		return false
	}
	defer func() {
		_ = txn.Rollback()
	}()

	q1 := `
		SELECT
		 o.transaction_id
		,o.coinbase_spending_height
		,o.utxo_hash
		,o.spending_data
		,o.frozen OR t.frozen AS frozen
		,t.conflicting
		,t.locked
		,o.spendableIn
		FROM outputs o
		JOIN transactions t ON o.transaction_id = t.id
		WHERE t.hash = $1
		AND o.idx = $2
	`

	// Optimistic locking: spending_data IS NULL guard prevents concurrent double-spend
	q2 := `
		UPDATE outputs
		SET spending_data = $1
		WHERE transaction_id = $2
		AND idx = $3
		AND spending_data IS NULL
	`

	successItems := make([]*batchSpend, 0, len(batch))
	spentParentIDs := make(map[int]struct{}, len(batch)) // distinct parent tx ids whose outputs were just spent
	validationErrors := make(map[int]error)              // index -> validation error (non-retryable)
	aborted := false

	// Phase 1: SELECT + validate + UPDATE outputs
	for i, item := range batch {
		if aborted {
			item.errCh <- errors.NewStorageError("[Spend] batch aborted due to previous DB error")
			continue
		}

		spend := item.spend

		var (
			transactionID          int
			coinbaseSpendingHeight uint32
			utxoHash               []byte
			spendingDataBytes      []byte
			frozen                 bool
			conflicting            bool
			locked                 bool
			spendableIn            *uint32
		)

		err = txn.QueryRowContext(s.ctx, q1, spend.TxID[:], spend.Vout).Scan(
			&transactionID, &coinbaseSpendingHeight, &utxoHash,
			&spendingDataBytes, &frozen, &conflicting, &locked, &spendableIn,
		)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				validationErrors[i] = errors.NewTxNotFoundError("output %s:%d not found", spend.TxID, spend.Vout)
				continue
			}
			if isDeadlock(err) {
				return true // retryable
			}
			item.errCh <- errors.NewStorageError("[Spend] failed: SELECT output %s:%d - %v", spend.TxID, spend.Vout, err)
			aborted = true
			continue
		}

		// Validate the UTXO state
		if frozen {
			validationErrors[i] = errors.NewUtxoFrozenError("[Spend] utxo is frozen for %s:%d", spend.TxID, spend.Vout)
			continue
		}

		if conflicting && !item.ignoreConflicting {
			validationErrors[i] = errors.NewTxConflictingError("[Spend] tx is conflicting for %s:%d", spend.TxID, spend.Vout)
			continue
		}

		if locked && !item.ignoreLocked {
			validationErrors[i] = errors.NewTxLockedError("[Spend] utxo is not spendable for %s:%d", spend.TxID, spend.Vout)
			continue
		}

		if spendableIn != nil && *spendableIn > 0 && item.blockHeight < *spendableIn {
			validationErrors[i] = errors.NewTxLockedError("[Spend] utxo %s:%d is not spendable until %d", spend.TxID, spend.Vout, *spendableIn)
			continue
		}

		// Check if already spent by a different transaction
		if len(spendingDataBytes) > 0 {
			if spend.SpendingData != nil && !bytes.Equal(spendingDataBytes, spend.SpendingData.Bytes()) {
				existingSpendData, parseErr := spendpkg.NewSpendingDataFromBytes(spendingDataBytes)
				if parseErr != nil {
					validationErrors[i] = errors.NewProcessingError("failed to create spending data from bytes", parseErr)
					continue
				}
				validationErrors[i] = errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, existingSpendData)
				continue
			}
		}

		// Check UTXO hash matches
		if !bytes.Equal(utxoHash, spend.UTXOHash[:]) {
			validationErrors[i] = errors.NewUtxoHashMismatchError("[Spend] utxo hash mismatch for %s:%d", spend.TxID, spend.Vout)
			continue
		}

		// Check coinbase maturity
		if coinbaseSpendingHeight > 0 && coinbaseSpendingHeight > item.blockHeight {
			validationErrors[i] = errors.NewTxCoinbaseImmatureError("[Spend] coinbase utxo not ready to spend for %s:%d, requires height %d, current %d", spend.TxID, spend.Vout, coinbaseSpendingHeight, item.blockHeight)
			continue
		}

		// UPDATE outputs with optimistic locking
		result, err := txn.ExecContext(s.ctx, q2, spend.SpendingData.Bytes(), transactionID, spend.Vout)
		if err != nil {
			if isDeadlock(err) {
				return true // retryable
			}
			item.errCh <- errors.NewStorageError("[Spend] failed: UPDATE outputs for %s:%d", spend.TxID, spend.Vout, err)
			aborted = true
			continue
		}

		affected, err := result.RowsAffected()
		if err != nil {
			item.errCh <- errors.NewStorageError("[Spend] failed getting affected rows for %s:%d", spend.TxID, spend.Vout, err)
			aborted = true
			continue
		}

		if affected == 0 {
			// Idempotent re-spend: same tx spending the same output again.
			// Still record the parent so DAH can be (re)evaluated — this heals DAHs
			// that an earlier spend (before the DAH-on-spend fix) failed to set.
			if len(spendingDataBytes) > 0 && spend.SpendingData != nil && bytes.Equal(spendingDataBytes, spend.SpendingData.Bytes()) {
				successItems = append(successItems, item)
				spentParentIDs[transactionID] = struct{}{}
				continue
			}
			// Concurrently spent by a different tx between SELECT and UPDATE.
			// spendingDataBytes was NULL from SELECT (WHERE spending_data IS NULL),
			// so we don't have the actual conflicting spender — use current spend data.
			validationErrors[i] = errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, spend.SpendingData)
			continue
		}

		successItems = append(successItems, item)
		spentParentIDs[transactionID] = struct{}{}
	}

	// If aborted, roll back — send errors for items not yet notified
	if aborted {
		for i, item := range batch {
			if valErr, ok := validationErrors[i]; ok {
				item.errCh <- valErr
			}
			// Items that already received errors via errCh (DB errors) or
			// successItems are handled: successItems get rollback error below
		}
		for _, item := range successItems {
			item.errCh <- errors.NewStorageError("[Spend] batch rolled back due to DB error")
		}
		return false
	}

	// Recompute DAH for every parent tx whose outputs were just spent. Mirrors
	// aerospike's inline setDeleteAtHeight Lua call — if this batch drained the
	// last unspent output of a mined, on-longest-chain tx, setDAH sets DAH so
	// the pruner can later reclaim it. Without this, mined txs spent gradually
	// over time never become prunable and disk usage grows unbounded.
	//
	// Sort parent IDs so concurrent per-row batches acquire row locks in a
	// deterministic order and don't deadlock on cross-batch lock orderings.
	sortedSpentParents := make([]int, 0, len(spentParentIDs))
	for parentID := range spentParentIDs {
		sortedSpentParents = append(sortedSpentParents, parentID)
	}
	sort.Ints(sortedSpentParents)
	for _, parentID := range sortedSpentParents {
		if err := s.setDAH(s.ctx, txn, parentID); err != nil {
			if isDeadlock(err) {
				return true
			}
			for i, item := range batch {
				if valErr, ok := validationErrors[i]; ok {
					item.errCh <- valErr
				}
			}
			for _, item := range successItems {
				item.errCh <- errors.NewStorageError("[Spend] failed to recompute DAH", err)
			}
			return false
		}
	}

	// Commit
	if err := txn.Commit(); err != nil {
		if isDeadlock(err) {
			return true // retryable
		}
		for i, item := range batch {
			if valErr, ok := validationErrors[i]; ok {
				item.errCh <- valErr
			}
		}
		for _, item := range successItems {
			item.errCh <- errors.NewStorageError("[Spend] failed to commit transaction", err)
		}
		return false
	}

	// Signal results: validation errors and successes
	for i, item := range batch {
		if valErr, ok := validationErrors[i]; ok {
			item.errCh <- valErr
		}
	}
	for _, item := range successItems {
		item.errCh <- nil
	}
	return false
}

func (s *Store) setDAH(ctx context.Context, txn *sql.Tx, transactionID int) error {
	if s.settings.GetUtxoStoreBlockHeightRetention() == 0 {
		return nil
	}

	// Check transaction state: unspent outputs, conflicting, mined status, longest chain.
	// Mirrors aerospike setDeleteAtHeight which requires:
	//   allSpent AND hasBlockIDs AND isOnLongestChain (for non-conflicting txs)
	qUnspent := `
		SELECT count(o.idx), t.conflicting, t.preserve_until IS NOT NULL,
		       EXISTS(SELECT 1 FROM block_ids WHERE transaction_id = t.id) AS has_blocks,
		       t.unmined_since IS NULL AS is_on_longest_chain,
		       t.delete_at_height
		FROM transactions t
		LEFT JOIN outputs o ON t.id = o.transaction_id
		   AND o.spending_data IS NULL
		WHERE t.id = $1
		GROUP BY t.id
	`

	var (
		unspent              int
		conflicting          bool
		hasPreserveUntil     bool
		hasBlocks            bool
		isOnLongestChain     bool
		existingDAH          sql.NullInt64
		deleteAtHeightOrNull sql.NullInt64
	)

	if err := txn.QueryRowContext(ctx, qUnspent, transactionID).Scan(&unspent, &conflicting, &hasPreserveUntil, &hasBlocks, &isOnLongestChain, &existingDAH); err != nil {
		return errors.NewStorageError("[setDAH] error checking for unspent outputs for %d", transactionID, err)
	}

	// If preserve_until is set, don't touch DAH (mirrors aerospike)
	if hasPreserveUntil {
		return nil
	}

	retention := s.settings.GetUtxoStoreBlockHeightRetention()
	// +1 because blockHeight is updated asynchronously via blockchain notifications
	// and lags behind during block processing (mirrors aerospike set_mined.go:162)
	newDAH := int64(s.blockHeight.Load() + 1 + retention)

	if conflicting {
		// Conflicting: set DAH only if not already set (mirrors aerospike line 944-951)
		if !existingDAH.Valid {
			_ = deleteAtHeightOrNull.Scan(newDAH)
		} else {
			// Keep existing DAH
			deleteAtHeightOrNull = existingDAH
		}
	} else if unspent == 0 && hasBlocks && isOnLongestChain {
		// All outputs spent AND mined AND on longest chain: set/bump DAH
		// Mirrors aerospike: allSpent AND hasBlockIDs AND isOnLongestChain
		if !existingDAH.Valid || existingDAH.Int64 < newDAH {
			_ = deleteAtHeightOrNull.Scan(newDAH)
		} else {
			// Keep existing higher DAH
			deleteAtHeightOrNull = existingDAH
		}
	} else if existingDAH.Valid {
		// Conditions no longer met (e.g., unspend, no longer on longest chain):
		// Clear DAH (mirrors aerospike clearing DAH when conditions aren't met)
		// deleteAtHeightOrNull stays NULL
	}
	// else: conditions not met and no existing DAH, leave as NULL

	// Short-circuit: if the computed DAH equals what's already stored (including
	// NULL→NULL), skip the UPDATE entirely. Avoids taking a row lock, writing a
	// new row version and generating WAL for a no-op — matters on the spend hot
	// path where setDAH is called for every touched parent per batch.
	if dahUnchanged(existingDAH, deleteAtHeightOrNull) {
		return nil
	}

	// Update delete_at_height
	qUpdate := `
		UPDATE transactions
		SET delete_at_height = $2
		WHERE id = $1
	`

	if _, err := txn.ExecContext(ctx, qUpdate, transactionID, deleteAtHeightOrNull); err != nil {
		return errors.NewStorageError("[setDAH] error setting DAH for %d", transactionID, err)
	}

	return nil
}

// dahUnchanged reports whether two sql.NullInt64 DAH values are equivalent.
// NULL==NULL is true; NULL vs valid and valid vs valid with different values
// are false; valid vs valid with same value is true.
func dahUnchanged(a, b sql.NullInt64) bool {
	if a.Valid != b.Valid {
		return false
	}
	if !a.Valid {
		return true
	}
	return a.Int64 == b.Int64
}

// Unspend reverses a previous spend operation, marking UTXOs as unspent.
// This removes the spending transaction ID and any expiration timestamp.
// Commonly used during blockchain reorganizations.
func (s *Store) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {

	txn, err := s.db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		_ = txn.Rollback()
	}()

	// Match-on-spending_data is the safety guarantee: we only clear spending_data
	// when the caller's expected value matches what's stored. This prevents a stale
	// or wrong Spend record from silently wiping the legitimate spend.
	// SpendingData is mandatory: every production caller derives spends from
	// Spend()/SetConflicting()/GetSpends(), all of which populate it.
	q1 := `
		UPDATE outputs
		SET spending_data = NULL
		WHERE transaction_id IN (
			SELECT id FROM transactions WHERE hash = $1
		)
		AND idx = $2
		AND spending_data = $3
		RETURNING transaction_id
	`

	// qFindTxID runs after q1 returns 0 rows. Unspend is idempotent: if the output
	// row exists but our caller doesn't own the spend (stored data is NULL or
	// belongs to someone else), we no-op on spending_data but still need the
	// transaction_id to apply the locked/DAH housekeeping below — callers like
	// ProcessConflicting rely on the parent transaction's locked state being
	// updated regardless of whether the spend was the loser's. Only a missing
	// row is a real error (NotFound). Mirrors stores/utxo/aerospike/teranode.lua
	// where the unspend UDF returns STATUS_OK with no bin mutation for the same
	// non-owning cases.
	qFindTxID := `
		SELECT transaction_id FROM outputs
		WHERE transaction_id IN (
			SELECT id FROM transactions WHERE hash = $1
		)
		AND idx = $2
	`

	locked := false
	if len(flagAsLocked) > 0 {
		locked = flagAsLocked[0]
	}

	q2 := `
		UPDATE transactions
		SET
			locked = $2
		WHERE id = $1
	`

	for _, spend := range spends {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			if spend == nil {
				continue
			}

			if spend.SpendingData == nil {
				return errors.NewProcessingError("[Unspend] SpendingData is required for %s:%d", spend.TxID, spend.Vout)
			}

			var transactionID int

			err = txn.QueryRowContext(ctx, q1, spend.TxID[:], spend.Vout, spend.SpendingData.Bytes()).Scan(&transactionID)
			if err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					return err
				}

				// q1 declined to clear — caller isn't the spend's current owner.
				// Look up the transaction_id directly so the housekeeping below
				// still runs; the only real error here is a missing row.
				if err = txn.QueryRowContext(ctx, qFindTxID, spend.TxID[:], spend.Vout).Scan(&transactionID); err != nil {
					if errors.Is(err, sql.ErrNoRows) {
						return errors.NewNotFoundError("output %s:%d not found", spend.TxID, spend.Vout)
					}

					return err
				}
			}

			if _, err = txn.ExecContext(ctx, q2, transactionID, locked); err != nil {
				return errors.NewStorageError("[Unspend] error removing tombstone for %s:%d", spend.TxID, spend.Vout, err)
			}

			if err = s.setDAH(ctx, txn, transactionID); err != nil {
				return err
			}

			prometheusUtxoReset.Inc()
		}
	}

	if err = txn.Commit(); err != nil {
		return err
	}

	return nil
}

// Delete removes a transaction and all its associated data.
// This includes inputs, outputs, and block references.
func (s *Store) Delete(ctx context.Context, hash *chainhash.Hash) error {

	// Start a database transaction
	txn, err := s.db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		_ = txn.Rollback()
	}()

	// Delete the block_ids
	q := `
		DELETE FROM block_ids
		WHERE transaction_id IN (
			SELECT id FROM transactions WHERE hash = $1
		)
	`

	_, err = txn.ExecContext(ctx, q, hash[:])
	if err != nil {
		return err
	}

	// Delete the outputs
	q = `
		DELETE FROM outputs
		WHERE transaction_id IN (
			SELECT id FROM transactions WHERE hash = $1
		)
	`

	_, err = txn.ExecContext(ctx, q, hash[:])
	if err != nil {
		return err
	}

	// Delete the inputs
	q = `
		DELETE FROM inputs
		WHERE transaction_id IN (
			SELECT id FROM transactions WHERE hash = $1
		)
	`

	_, err = txn.ExecContext(ctx, q, hash[:])
	if err != nil {
		return err
	}

	// Delete the transaction
	q = `
		DELETE FROM transactions
		WHERE hash = $1
	`

	_, err = txn.ExecContext(ctx, q, hash[:])
	if err != nil {
		return err
	}

	// Commit the transaction
	if err := txn.Commit(); err != nil {
		return err
	}

	prometheusUtxoDelete.Inc()

	return nil
}

func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	if len(hashes) == 0 {
		return make(map[chainhash.Hash][]uint32), nil
	}

	resultMap := make(map[chainhash.Hash][]uint32)

	// Process hashes in chunks to stay within SQLite's parameter limit (999)
	for i := 0; i < len(hashes); i += maxINClauseSize {
		end := i + maxINClauseSize
		if end > len(hashes) {
			end = len(hashes)
		}

		// Check context before processing each chunk
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		chunkResult, err := s.setMinedMultiChunk(ctx, hashes[i:end], minedBlockInfo)

		if err != nil {
			return nil, errors.NewStorageError("SQL error in SetMinedMulti (chunk %d-%d): %v", i, end-1, err)
		}

		for hash, blockIDs := range chunkResult {
			resultMap[hash] = blockIDs
		}
	}

	return resultMap, nil
}

// setMinedMultiChunk processes a single chunk of hashes (up to maxINClauseSize).
// Uses portable IN ($1,$2,...,$N) clauses that work on both PostgreSQL and SQLite.
func (s *Store) setMinedMultiChunk(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	// Convert hashes to byte arrays
	hashBytes := make([][]byte, len(hashes))
	for i, hash := range hashes {
		hashBytes[i] = hash[:]
	}

	// Start a database transaction (use BeginTx so context cancellation is respected)
	txn, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	committed := false
	defer func() {
		if !committed {
			if rollbackErr := txn.Rollback(); rollbackErr != nil {
				s.logger.Warnf("Failed to rollback SetMinedMulti transaction: %v", rollbackErr)
			}
		}
	}()

	// Step 1: Check which transactions exist using IN clause
	inClause, inArgs := buildINClause(hashBytes, 1)
	qCheckExists := fmt.Sprintf(`SELECT hash FROM transactions WHERE hash IN %s`, inClause)

	rows, err := txn.QueryContext(ctx, qCheckExists, inArgs...)
	if err != nil {
		return nil, errors.NewStorageError("SQL error checking transaction existence: %v", err)
	}

	existingHashes := make(map[chainhash.Hash]bool)
	for rows.Next() {
		var hb []byte
		if err := rows.Scan(&hb); err != nil {
			rows.Close()
			return nil, errors.NewStorageError("SQL error scanning existing hash: %v", err)
		}
		var h chainhash.Hash
		copy(h[:], hb)
		existingHashes[h] = true
	}
	rows.Close()

	// Postcondition (see stores/utxo/Interface.go SetMinedMulti docstring): when
	// !UnsetMined every submitted hash MUST exist in the transactions table.
	// A missing hash means the tx was never persisted (or was pruned) — mirrors
	// the Aerospike KEY_NOT_FOUND handling so all backends fail closed identically.
	// UnsetMined tolerates missing rows (the tx is already gone).
	if !minedBlockInfo.UnsetMined && len(existingHashes) != len(hashes) {
		for _, h := range hashes {
			if !existingHashes[*h] {
				return nil, errors.NewTxNotFoundError("transaction not found: %s", h.String())
			}
		}
	}

	if len(existingHashes) == 0 {
		if err = txn.Commit(); err != nil {
			return nil, errors.NewStorageError("SQL error committing empty transaction: %v", err)
		}
		committed = true
		return make(map[chainhash.Hash][]uint32), nil
	}

	// Build IN clause for existing hashes only
	existingHashBytes := make([][]byte, 0, len(existingHashes))
	for h := range existingHashes {
		hCopy := h
		existingHashBytes = append(existingHashBytes, hCopy[:])
	}

	// Step 2: Insert or delete block_ids
	if minedBlockInfo.UnsetMined {
		// DELETE block_ids: block_id is $1, hashes start at $2
		inClause2, inArgs2 := buildINClause(existingHashBytes, 2)
		qRemove := fmt.Sprintf(`
			DELETE FROM block_ids
			WHERE transaction_id IN (
				SELECT id FROM transactions WHERE hash IN %s
			)
			AND block_id = $1
		`, inClause2)
		args := append([]interface{}{minedBlockInfo.BlockID}, inArgs2...)
		if _, err = txn.ExecContext(ctx, qRemove, args...); err != nil {
			return nil, errors.NewStorageError("SQL error removing block_ids: %v", err)
		}
	} else {
		// INSERT block_ids: block_id=$1, block_height=$2, subtree_idx=$3, hashes start at $4
		inClause2, inArgs2 := buildINClause(existingHashBytes, 4)
		qInsert := fmt.Sprintf(`
			INSERT INTO block_ids (transaction_id, block_id, block_height, subtree_idx)
			SELECT t.id, $1, $2, $3
			FROM transactions t
			WHERE t.hash IN %s
			ON CONFLICT DO NOTHING
		`, inClause2)
		args := append([]interface{}{minedBlockInfo.BlockID, minedBlockInfo.BlockHeight, minedBlockInfo.SubtreeIdx}, inArgs2...)
		if _, err = txn.ExecContext(ctx, qInsert, args...); err != nil {
			return nil, errors.NewStorageError("SQL error inserting block_ids: %v", err)
		}
	}

	// Step 3: Update transaction fields (locked, unmined_since, delete_at_height)
	// Mirrors aerospike's setMined->setDeleteAtHeight behavior
	var retention uint32
	if s.settings != nil {
		retention = s.settings.GetUtxoStoreBlockHeightRetention()
	}
	// +1 because blockHeight lags during block processing (mirrors aerospike set_mined.go:162)
	newDAH := int64(s.blockHeight.Load() + 1 + retention)

	if minedBlockInfo.OnLongestChain {
		if retention > 0 {
			// DAH is $1, hashes start at $2
			// Mirrors aerospike Lua setDeleteAtHeight logic:
			// 1. If preserve_until is set -> don't touch DAH
			// 2. If DAH already exists and is less than new value -> bump it forward
			// 3. If DAH is NULL and all UTXOs are spent -> set DAH for the first time
			// 4. Otherwise -> leave DAH unchanged
			inClause3, inArgs3 := buildINClause(existingHashBytes, 2)
			qUpdate := fmt.Sprintf(`
				UPDATE transactions
				SET locked = false
				   ,unmined_since = NULL
				   ,delete_at_height = CASE
				        WHEN preserve_until IS NOT NULL THEN delete_at_height
				        WHEN delete_at_height IS NOT NULL AND delete_at_height < $1 THEN $1
				        WHEN delete_at_height IS NULL
				             AND NOT EXISTS (
				                 SELECT 1 FROM outputs o
				                 WHERE o.transaction_id = transactions.id AND o.spending_data IS NULL
				             )
				             THEN $1
				        ELSE delete_at_height
				    END
				WHERE hash IN %s
			`, inClause3)
			args := append([]interface{}{newDAH}, inArgs3...)
			if _, err = txn.ExecContext(ctx, qUpdate, args...); err != nil {
				return nil, errors.NewStorageError("SQL error updating transactions: %v", err)
			}
		} else {
			inClause3, inArgs3 := buildINClause(existingHashBytes, 1)
			qUpdate := fmt.Sprintf(`
				UPDATE transactions
				SET locked = false, unmined_since = NULL
				WHERE hash IN %s
			`, inClause3)
			if _, err = txn.ExecContext(ctx, qUpdate, inArgs3...); err != nil {
				return nil, errors.NewStorageError("SQL error updating transactions: %v", err)
			}
		}
	} else {
		// Not on longest chain: clear delete_at_height, set locked = false
		if minedBlockInfo.UnsetMined {
			// UnsetMined path (block invalidation): unlock and clear delete_at_height
			// for all affected transactions, then set unmined_since only for those
			// with zero remaining block_ids.
			// This mirrors the aerospike Lua which sets unmined_since = currentBlockHeight
			// when #blocks == 0 after removing the block_id.
			// Use the store's current block height (not the invalidated block's height)
			// to match Aerospike's setMined UDF behavior.
			currentBlockHeight := s.blockHeight.Load() + 1

			// Step 1: Unlock and clear delete_at_height for all affected transactions
			inClause3, inArgs3 := buildINClause(existingHashBytes, 1)
			qUpdate := fmt.Sprintf(`
				UPDATE transactions
				SET locked = false
				   ,delete_at_height = NULL
				WHERE hash IN %s
			`, inClause3)
			if _, err = txn.ExecContext(ctx, qUpdate, inArgs3...); err != nil {
				return nil, errors.NewStorageError("SQL error updating transactions: %v", err)
			}

			// Step 2: Set unmined_since only for transactions with no remaining block_ids
			inClause3b, inArgs3b := buildINClause(existingHashBytes, 2)
			qUpdateUnmined := fmt.Sprintf(`
				WITH txs_without_blocks AS (
					SELECT t.id
					FROM transactions t
					LEFT JOIN block_ids bi ON bi.transaction_id = t.id
					WHERE t.hash IN %s
					GROUP BY t.id
					HAVING COUNT(bi.transaction_id) = 0
				)
				UPDATE transactions
				SET unmined_since = $1
				WHERE id IN (SELECT id FROM txs_without_blocks)
			`, inClause3b)
			args := append([]interface{}{currentBlockHeight}, inArgs3b...)
			if _, err = txn.ExecContext(ctx, qUpdateUnmined, args...); err != nil {
				return nil, errors.NewStorageError("SQL error updating unmined_since: %v", err)
			}
		} else {
			// Normal not-on-longest-chain path: MarkTransactionsOnLongestChain handles unmined_since
			inClause3, inArgs3 := buildINClause(existingHashBytes, 1)
			qUpdate := fmt.Sprintf(`
				UPDATE transactions
				SET locked = false, delete_at_height = NULL
				WHERE hash IN %s
			`, inClause3)
			if _, err = txn.ExecContext(ctx, qUpdate, inArgs3...); err != nil {
				return nil, errors.NewStorageError("SQL error updating transactions: %v", err)
			}
		}
	}

	// Step 4: Fetch block_ids for all existing transactions (aggregate in Go, no array_agg)
	inClause4, inArgs4 := buildINClause(existingHashBytes, 1)
	qGetBlockIDs := fmt.Sprintf(`
		SELECT t.hash, b.block_id
		FROM transactions t
		LEFT JOIN block_ids b ON t.id = b.transaction_id
		WHERE t.hash IN %s
		ORDER BY t.hash, b.block_id
	`, inClause4)

	rows, err = txn.QueryContext(ctx, qGetBlockIDs, inArgs4...)
	if err != nil {
		return nil, errors.NewStorageError("SQL error fetching block IDs: %v", err)
	}

	blockIDsMap := make(map[chainhash.Hash][]uint32)
	for rows.Next() {
		var hb []byte
		var blockID *uint32 // nullable from LEFT JOIN
		if err := rows.Scan(&hb, &blockID); err != nil {
			rows.Close()
			return nil, errors.NewStorageError("SQL error scanning block IDs: %v", err)
		}
		var h chainhash.Hash
		copy(h[:], hb)
		if blockID != nil {
			blockIDsMap[h] = append(blockIDsMap[h], *blockID)
		} else if _, exists := blockIDsMap[h]; !exists {
			// Ensure the hash is in the map even with no block_ids
			blockIDsMap[h] = nil
		}
	}
	rows.Close()

	if err = txn.Commit(); err != nil {
		return nil, errors.NewStorageError("SQL error committing SetMinedMulti transaction: %v", err)
	}
	committed = true

	return blockIDsMap, nil
}

func (s *Store) GetSpend(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {

	q := `
		SELECT
		 o.utxo_hash
		,o.coinbase_spending_height
		,o.spending_data
		,o.frozen OR t.frozen AS frozen
		,o.spendableIn
		,t.conflicting
		,t.locked
		FROM outputs o
		JOIN transactions t ON o.transaction_id = t.id
		WHERE t.hash = $1
		AND o.idx = $2
	`

	var (
		utxoHash               []byte
		coinbaseSpendingHeight uint32
		spendingDataBytes      []byte
		frozen                 bool
		spendableIn            *uint32
		conflicting            bool
		locked                 bool
	)

	err := s.db.QueryRowContext(ctx, q, spend.TxID[:], spend.Vout).Scan(&utxoHash, &coinbaseSpendingHeight, &spendingDataBytes, &frozen, &spendableIn, &conflicting, &locked)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Match aerospike behavior: return NOT_FOUND status instead of error
			return &utxo.SpendResponse{
				Status: int(utxo.Status_NOT_FOUND),
			}, nil
		}

		return nil, err
	}

	// Verify the caller-supplied hash matches the stored one. When the caller
	// passes nil (e.g. the bulk /api/v1/utxos endpoint, which intentionally
	// avoids fetching the full transaction to recompute it) we trust the
	// stored hash — the row was located by primary key (txid, vout) and the
	// stored hash is canonical.
	if spend.UTXOHash != nil && !bytes.Equal(utxoHash, spend.UTXOHash[:]) {
		return nil, errors.NewUtxoHashMismatchError("utxo hash mismatch for %s:%d", spend.TxID, spend.Vout)
	}

	var spendingData *spendpkg.SpendingData

	if len(spendingDataBytes) > 0 {
		spendingData, err = spendpkg.NewSpendingDataFromBytes(spendingDataBytes)
		if err != nil {
			return nil, err
		}
	}

	utxoStatus := utxo.CalculateUtxoStatus(spendingData, coinbaseSpendingHeight, s.blockHeight.Load())

	if frozen {
		utxoStatus = utxo.Status_FROZEN
		// this is needed in for instance conflict resolution where we check the spending data
		spendingData = spendpkg.NewSpendingData(&subtree.FrozenBytesTxHash, int(spend.Vout))
	}

	if conflicting {
		utxoStatus = utxo.Status_CONFLICTING
	}

	if locked {
		utxoStatus = utxo.Status_LOCKED
	}

	// spendableIn is set by the alert system to indicate when the UTXO can be spent
	if spendableIn != nil && s.GetBlockHeight() < *spendableIn {
		utxoStatus = utxo.Status_IMMATURE
	}

	return &utxo.SpendResponse{
		Status:       int(utxoStatus),
		SpendingData: spendingData,
		LockTime:     coinbaseSpendingHeight,
	}, nil
}

// BatchDecorate efficiently fetches metadata for multiple transactions using
// bulk IN-clause queries instead of individual per-transaction queries.
// For a batch of N transactions needing inputs and block_ids, this executes
// ~3 queries per chunk (transactions + inputs + block_ids) instead of ~3*N.
func (s *Store) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*utxo.UnresolvedMetaData, requestedFields ...fields.FieldName) error {
	bins := utxo.MetaFieldsWithTx
	if len(requestedFields) > 0 {
		bins = requestedFields
	}

	// Filter out nil entries and collect non-nil ones
	items := make([]*utxo.UnresolvedMetaData, 0, len(unresolvedMetaDataSlice))
	for _, item := range unresolvedMetaDataSlice {
		if item != nil {
			items = append(items, item)
		}
	}

	if len(items) == 0 {
		return nil
	}

	// Process in chunks of maxINClauseSize
	for i := 0; i < len(items); i += maxINClauseSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		end := i + maxINClauseSize
		if end > len(items) {
			end = len(items)
		}

		if err := s.batchDecorateChunk(ctx, items[i:end], bins); err != nil {
			return err
		}
	}

	return nil
}

// batchDecorateTxRow holds intermediate results for a single transaction during bulk fetch.
type batchDecorateTxRow struct {
	id       int
	data     *meta.Data
	version  uint32
	lockTime uint32
	hash     chainhash.Hash
}

// batchDecorateChunk fetches metadata for a chunk of transactions using bulk queries.
// It runs one query per table (transactions, inputs, block_ids, outputs) rather than
// one query per transaction per table.
func (s *Store) batchDecorateChunk(ctx context.Context, items []*utxo.UnresolvedMetaData, bins []fields.FieldName) error {

	// Build hash list and hash→item index for result mapping
	hashes := make([][]byte, len(items))
	hashToItems := make(map[chainhash.Hash][]*utxo.UnresolvedMetaData, len(items))
	for i, item := range items {
		hashes[i] = item.Hash[:]
		hashToItems[item.Hash] = append(hashToItems[item.Hash], item)
	}

	// Query 1: Bulk fetch from transactions table
	inClause, inArgs := buildINClause(hashes, 1)

	q := `SELECT hash, id, version, lock_time, fee, size_in_bytes, coinbase, frozen, conflicting, locked, unmined_since, inserted_at FROM transactions WHERE hash IN ` + inClause

	rows, err := s.db.QueryContext(ctx, q, inArgs...)
	if err != nil {
		return err
	}

	// Map internal DB id → txRow for subsequent queries
	idToTx := make(map[int]*batchDecorateTxRow, len(items))
	// Map hash → txRow for result assembly
	hashToTx := make(map[chainhash.Hash]*batchDecorateTxRow, len(items))

	for rows.Next() {
		var (
			hashBytes    []byte
			unminedSince sql.NullInt64
			insertedAt   any
		)

		row := &batchDecorateTxRow{data: &meta.Data{}}
		if err := rows.Scan(&hashBytes, &row.id, &row.version, &row.lockTime, &row.data.Fee, &row.data.SizeInBytes, &row.data.IsCoinbase, &row.data.Frozen, &row.data.Conflicting, &row.data.Locked, &unminedSince, &insertedAt); err != nil {
			rows.Close()
			return err
		}

		copy(row.hash[:], hashBytes)

		if unminedSince.Valid {
			const maxUint32 = 0xFFFFFFFF
			if unminedSince.Int64 >= 0 && unminedSince.Int64 <= maxUint32 {
				row.data.UnminedSince = uint32(unminedSince.Int64)
			}
		}

		row.data.CreatedAt = parseInsertedAtMillis(insertedAt)

		idToTx[row.id] = row
		hashToTx[row.hash] = row
	}
	rows.Close()

	// Mark not-found transactions
	for _, item := range items {
		if _, found := hashToTx[item.Hash]; !found {
			item.Err = errors.NewTxNotFoundError("transaction %s not found", &item.Hash)
		}
	}

	if len(idToTx) == 0 {
		return nil
	}

	// Build ID list for subsequent queries
	ids := make([]int, 0, len(idToTx))
	for id := range idToTx {
		ids = append(ids, id)
	}

	needInputs := contains(bins, fields.Tx) || contains(bins, fields.Inputs) || contains(bins, fields.TxInpoints) || contains(bins, fields.Utxos)
	needOutputs := contains(bins, fields.Tx) || contains(bins, fields.Outputs) || contains(bins, fields.Utxos)
	needBlockIDs := contains(bins, fields.BlockIDs)

	// Query 2: Bulk fetch inputs
	if needInputs {
		if err := s.batchDecorateInputs(ctx, ids, idToTx); err != nil {
			return err
		}
	}

	// Query 3: Bulk fetch outputs
	if needOutputs {
		if err := s.batchDecorateOutputs(ctx, ids, idToTx); err != nil {
			return err
		}
	}

	// Query 4: Bulk fetch block_ids
	if needBlockIDs {
		if err := s.batchDecorateBlockIDs(ctx, ids, idToTx); err != nil {
			return err
		}
	}

	// Assemble results into UnresolvedMetaData items
	for hash, matchedItems := range hashToItems {
		row, found := hashToTx[hash]
		if !found {
			continue // already marked as error above
		}

		// Build tx if needed for Tx or TxInpoints fields
		var tx *bt.Tx
		if contains(bins, fields.Tx) || contains(bins, fields.TxInpoints) {
			tx = &bt.Tx{
				Version:  row.version,
				LockTime: row.lockTime,
			}
			if needInputs && row.data.Tx != nil {
				tx.Inputs = row.data.Tx.Inputs
			}
			if needOutputs && row.data.Tx != nil {
				tx.Outputs = row.data.Tx.Outputs
			}
		}

		if contains(bins, fields.TxInpoints) && row.data.Tx != nil && len(row.data.Tx.Inputs) > 0 {
			row.data.TxInpoints, _ = subtree.NewTxInpointsFromInputs(row.data.Tx.Inputs)
		}

		if contains(bins, fields.Tx) || needInputs || needOutputs {
			row.data.Tx = tx
		} else {
			row.data.Tx = nil
		}

		for _, item := range matchedItems {
			item.Data = row.data
		}
	}

	return nil
}

// batchDecorateInputs bulk-fetches inputs for multiple transactions.
func (s *Store) batchDecorateInputs(ctx context.Context, ids []int, idToTx map[int]*batchDecorateTxRow) error {
	idPlaceholders := make([]string, len(ids))
	idArgs := make([]interface{}, len(ids))
	for i, id := range ids {
		idPlaceholders[i] = fmt.Sprintf("$%d", i+1)
		idArgs[i] = id
	}
	inClause := "(" + strings.Join(idPlaceholders, ",") + ")"

	q := `SELECT transaction_id, previous_transaction_hash, previous_tx_idx, previous_tx_satoshis, previous_tx_script, unlocking_script, sequence_number FROM inputs WHERE transaction_id IN ` + inClause + ` ORDER BY transaction_id, idx`

	rows, err := s.db.QueryContext(ctx, q, idArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			txID            int
			prevTxHashBytes []byte
		)
		input := &bt.Input{}
		var previousTxIdx int64
		if err := rows.Scan(&txID, &prevTxHashBytes, &previousTxIdx, &input.PreviousTxSatoshis, &input.PreviousTxScript, &input.UnlockingScript, &input.SequenceNumber); err != nil {
			return err
		}
		input.PreviousTxOutIndex = uint32(previousTxIdx)

		row := idToTx[txID]
		if row == nil {
			continue
		}

		previousTxHash, err := chainhash.NewHash(prevTxHashBytes)
		if err != nil {
			return err
		}
		if err := input.PreviousTxIDAdd(previousTxHash); err != nil {
			return err
		}

		// Store inputs in data.Tx temporarily
		if row.data.Tx == nil {
			row.data.Tx = &bt.Tx{Version: row.version, LockTime: row.lockTime}
		}
		row.data.Tx.Inputs = append(row.data.Tx.Inputs, input)
	}

	return nil
}

// batchDecorateOutputs bulk-fetches outputs for multiple transactions.
func (s *Store) batchDecorateOutputs(ctx context.Context, ids []int, idToTx map[int]*batchDecorateTxRow) error {
	idPlaceholders := make([]string, len(ids))
	idArgs := make([]interface{}, len(ids))
	for i, id := range ids {
		idPlaceholders[i] = fmt.Sprintf("$%d", i+1)
		idArgs[i] = id
	}
	inClause := "(" + strings.Join(idPlaceholders, ",") + ")"

	q := `SELECT transaction_id, locking_script, satoshis FROM outputs WHERE transaction_id IN ` + inClause + ` ORDER BY transaction_id, idx`

	rows, err := s.db.QueryContext(ctx, q, idArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var txID int
		output := &bt.Output{}
		if err := rows.Scan(&txID, &output.LockingScript, &output.Satoshis); err != nil {
			return err
		}

		row := idToTx[txID]
		if row == nil {
			continue
		}

		// Store outputs in data.Tx temporarily
		if row.data.Tx == nil {
			row.data.Tx = &bt.Tx{Version: row.version, LockTime: row.lockTime}
		}
		row.data.Tx.Outputs = append(row.data.Tx.Outputs, output)
	}

	return nil
}

// batchDecorateBlockIDs bulk-fetches block_ids for multiple transactions.
func (s *Store) batchDecorateBlockIDs(ctx context.Context, ids []int, idToTx map[int]*batchDecorateTxRow) error {
	idPlaceholders := make([]string, len(ids))
	idArgs := make([]interface{}, len(ids))
	for i, id := range ids {
		idPlaceholders[i] = fmt.Sprintf("$%d", i+1)
		idArgs[i] = id
	}
	inClause := "(" + strings.Join(idPlaceholders, ",") + ")"

	q := `SELECT transaction_id, block_id, block_height, subtree_idx FROM block_ids WHERE transaction_id IN ` + inClause + ` ORDER BY transaction_id, block_id`

	rows, err := s.db.QueryContext(ctx, q, idArgs...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			txID        int
			blockID     uint32
			blockHeight uint32
			subtreeIdx  int
		)
		if err := rows.Scan(&txID, &blockID, &blockHeight, &subtreeIdx); err != nil {
			return err
		}

		row := idToTx[txID]
		if row == nil {
			continue
		}

		row.data.BlockIDs = append(row.data.BlockIDs, blockID)
		row.data.BlockHeights = append(row.data.BlockHeights, blockHeight)
		row.data.SubtreeIdxs = append(row.data.SubtreeIdxs, subtreeIdx)
	}

	return nil
}

// PreviousOutputsDecorate fetches output information for transaction inputs.
// Uses bulk IN query instead of per-input sequential queries.
// Uses 60s timeout instead of DBTimeout (5s) because during legacy sync the DB is
// under heavy contention from concurrent creates/spends and queries can be slow.
func (s *Store) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {

	// Collect inputs that need decoration, grouped by parent tx hash
	type inputRef struct {
		inputIdx int
		outIdx   uint32
	}
	needsByParent := make(map[chainhash.Hash][]inputRef)

	for i, input := range tx.Inputs {
		if input == nil || input.PreviousTxScript != nil {
			continue
		}
		parentHash := *input.PreviousTxIDChainHash()
		needsByParent[parentHash] = append(needsByParent[parentHash], inputRef{
			inputIdx: i,
			outIdx:   input.PreviousTxOutIndex,
		})
	}

	if len(needsByParent) == 0 {
		return nil
	}

	// Collect unique composite (parent_hash, idx) pairs needed for decoration.
	// A valid tx won't reference the same outpoint twice, but dedup defensively
	// so we don't waste bind params / chunks on malformed input; also matches
	// BatchPreviousOutputsDecorate's shape.
	//
	// Preallocate to an upper bound on pairs (sum of all refs across parents).
	// Post-dedup the slice may be smaller, but this keeps append() from growing
	// the backing array for whole-block-sized input sets.
	pairCap := 0
	for _, refs := range needsByParent {
		pairCap += len(refs)
	}
	pairs := make([]outpointPair, 0, pairCap)
	for parentHash, refs := range needsByParent {
		// Hoist the hash copy outside the inner loop so each parent's array
		// escapes once, not once per unique referenced output.
		hCopy := parentHash
		hashSlice := hCopy[:]
		seenIdx := make(map[uint32]struct{}, len(refs))
		for _, ref := range refs {
			if _, seen := seenIdx[ref.outIdx]; seen {
				continue
			}
			seenIdx[ref.outIdx] = struct{}{}
			pairs = append(pairs, outpointPair{hash: hashSlice, idx: ref.outIdx})
		}
	}

	type outputKey struct {
		hash chainhash.Hash
		idx  uint32
	}
	type outputInfo struct {
		lockingScript []byte
		satoshis      uint64
	}
	results := make(map[outputKey]*outputInfo, len(pairs))

	// Chunk in maxINClauseSize-sized batches; 400 pairs = 800 params, safely
	// under SQLite's default 999 variable limit (and well under Postgres' 65535).
	for chunkStart := 0; chunkStart < len(pairs); chunkStart += maxINClauseSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		chunkEnd := chunkStart + maxINClauseSize
		if chunkEnd > len(pairs) {
			chunkEnd = len(pairs)
		}
		chunk := pairs[chunkStart:chunkEnd]

		valuesClause, args := buildCompositeValuesPairs(chunk, 1, s.engine)
		// CTE form is identical on Postgres (12+ inlines non-recursive
		// single-use CTEs) and the only form SQLite accepts as a typed join
		// source with column aliases — `FROM (VALUES ...) AS v(h, i)` is a
		// Postgres-only extension.
		q := `WITH v(h, i) AS (` + valuesClause + `)
			SELECT t.hash, o.idx, o.locking_script, o.satoshis
			FROM v
			JOIN transactions t ON t.hash = v.h
			JOIN outputs o ON o.transaction_id = t.id AND o.idx = v.i`

		rows, err := s.db.QueryContext(ctx, q, args...)
		if err != nil {
			return err
		}

		for rows.Next() {
			var hashBytes []byte
			var idx uint32
			var lockingScript []byte
			var satoshis uint64
			if err := rows.Scan(&hashBytes, &idx, &lockingScript, &satoshis); err != nil {
				rows.Close()
				return err
			}
			var h chainhash.Hash
			copy(h[:], hashBytes)
			results[outputKey{hash: h, idx: idx}] = &outputInfo{
				lockingScript: lockingScript,
				satoshis:      satoshis,
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
	}

	// Map results back to inputs and track missing
	var missingInputs []int
	for parentHash, refs := range needsByParent {
		for _, ref := range refs {
			key := outputKey{hash: parentHash, idx: ref.outIdx}
			if info, ok := results[key]; ok {
				tx.Inputs[ref.inputIdx].PreviousTxScript = bscript.NewFromBytes(info.lockingScript)
				tx.Inputs[ref.inputIdx].PreviousTxSatoshis = info.satoshis
			} else {
				missingInputs = append(missingInputs, ref.inputIdx)
			}
		}
	}

	if len(missingInputs) > 0 {
		// Diagnostic: log each missing parent and check if the tx row exists
		for _, idx := range missingInputs {
			input := tx.Inputs[idx]
			parentHash := input.PreviousTxIDChainHash()
			var txExists bool
			_ = s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM transactions WHERE hash = $1)`, parentHash[:]).Scan(&txExists)
			s.logger.Warnf("[PreviousOutputsDecorate] missing parent output: child=%x input[%d] parent=%x vout=%d parentTxExists=%v",
				tx.TxIDChainHash()[:], idx, parentHash[:], input.PreviousTxOutIndex, txExists)
		}
		return errors.NewProcessingError("failed to decorate previous outputs for tx %s", tx.TxIDChainHash())
	}

	return nil
}

// BatchPreviousOutputsDecorate fetches previous output information for inputs across
// multiple transactions in bulk. This is more efficient than calling PreviousOutputsDecorate
// per-transaction because it uses a single IN-clause query per chunk instead of
// individual lookups per input.
func (s *Store) BatchPreviousOutputsDecorate(ctx context.Context, txs []*bt.Tx) error {
	if len(txs) == 0 {
		return nil
	}

	// Collect all (parentTxHash, outputIdx) pairs that need decoration
	type inputRef struct {
		txIdx    int
		inputIdx int
		outIdx   uint32
	}
	// Map from parent tx hash -> list of input references that need that parent's outputs
	needsByParent := make(map[chainhash.Hash][]inputRef)

	for txIdx, tx := range txs {
		if tx == nil {
			continue
		}
		for inputIdx, input := range tx.Inputs {
			if input == nil || input.PreviousTxScript != nil {
				continue // already decorated or nil
			}
			parentHash := *input.PreviousTxIDChainHash()
			needsByParent[parentHash] = append(needsByParent[parentHash], inputRef{
				txIdx:    txIdx,
				inputIdx: inputIdx,
				outIdx:   input.PreviousTxOutIndex,
			})
		}
	}

	if len(needsByParent) == 0 {
		return nil
	}

	// Build the exact list of (parent-hash, output-index) pairs we need to fetch
	// and an index mapping each pair to the input slots it must populate. Using a
	// composite (t.hash, o.idx) IN predicate — instead of the older parent-hash IN
	// predicate — avoids scanning every output of every referenced parent, which
	// matters on data-carrier-heavy blocks where parents may have many MB of
	// script bytes in unreferenced outputs.
	//
	// Preallocate to an upper bound on pairs (sum of all refs across parents).
	// Post-dedup the slice may be smaller, but this keeps append() from growing
	// the backing array for whole-block calls where refs can be in the tens of thousands.
	estPairs := 0
	for _, refs := range needsByParent {
		estPairs += len(refs)
	}
	pairs := make([]outpointPair, 0, estPairs)
	// pairToRefs[i] holds every input slot that needs to be populated from
	// pairs[i]. One pair can satisfy multiple inputs (same outpoint referenced
	// by more than one tx in the block), so the slice-of-slices is required.
	// Keeping this per-pair means each chunk worker writes only into the input
	// slots its own pairs cover, so parallel workers never touch the same slot
	// and no synchronisation is needed between them.
	pairToRefs := make([][]inputRef, 0, estPairs)
	for h, refs := range needsByParent {
		hCopy := h
		hashSlice := hCopy[:]
		// Group refs by output index so we can attach each pair to every ref
		// that needs it (a parent output referenced by N inputs fetches once
		// and dispatches to all N slots).
		byIdx := make(map[uint32][]inputRef, len(refs))
		for _, ref := range refs {
			byIdx[ref.outIdx] = append(byIdx[ref.outIdx], ref)
		}
		for idx, idxRefs := range byIdx {
			pairs = append(pairs, outpointPair{hash: hashSlice, idx: idx})
			pairToRefs = append(pairToRefs, idxRefs)
		}
	}

	// Chunk the pair list, picking a size that fits comfortably under the
	// dialect's bind-parameter limit:
	//   - SQLite caps parameters at 999, and each pair uses 2 placeholders, so
	//     400 pairs = 800 params keeps headroom.
	//   - Postgres caps at 65535; fewer, larger queries win because the planner
	//     fuses the IN list and we save round-trips. 4000 pairs = 8000 params.
	chunkSize := maxINClauseSize
	if s.engine == string(util.Postgres) {
		chunkSize = postgresBatchDecorateChunkSize
	}
	if batchDecorateChunkSizeOverride > 0 {
		chunkSize = batchDecorateChunkSizeOverride
	}

	// Parallelise chunk queries when configured: each chunk fetches a disjoint
	// set of pairs, and the per-pair input slots in `pairToRefs` are disjoint
	// across chunks by construction, so workers write directly into
	// tx.Inputs[] without any shared state. `missingInputs` is the only
	// cross-worker counter and uses atomic.Int64.
	concurrency := s.settings.UtxoStore.BatchPreviousOutputsDecorateConcurrency
	if concurrency < 1 {
		concurrency = 1
	}

	var missingInputs atomic.Int64
	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, concurrency)

	for chunkStart := 0; chunkStart < len(pairs); chunkStart += chunkSize {
		chunkEnd := chunkStart + chunkSize
		if chunkEnd > len(pairs) {
			chunkEnd = len(pairs)
		}
		chunkPairs := pairs[chunkStart:chunkEnd]
		chunkRefs := pairToRefs[chunkStart:chunkEnd]

		g.Go(func() error {
			select {
			case <-gCtx.Done():
				return gCtx.Err()
			default:
			}

			valuesClause, args := buildCompositeValuesPairs(chunkPairs, 1, s.engine)
			// CTE form — same plan as a lateral-VALUES join on Postgres but
			// also works on SQLite, whose parser rejects `FROM (VALUES ...)
			// AS v(h, i)` with column aliases.
			q := `WITH v(h, i) AS (` + valuesClause + `)
				SELECT t.hash, o.idx, o.locking_script, o.satoshis
				FROM v
				JOIN transactions t ON t.hash = v.h
				JOIN outputs o ON o.transaction_id = t.id AND o.idx = v.i`

			rows, err := s.db.QueryContext(gCtx, q, args...)
			if err != nil {
				return err
			}
			defer rows.Close()

			// Build a lookup from (hash, idx) to the index within this chunk so
			// we can map each returned row back to its pairToRefs entry.
			type chunkKey struct {
				hash chainhash.Hash
				idx  uint32
			}
			chunkIndex := make(map[chunkKey]int, len(chunkPairs))
			for i, p := range chunkPairs {
				var h chainhash.Hash
				copy(h[:], p.hash)
				chunkIndex[chunkKey{hash: h, idx: p.idx}] = i
			}

			found := make([]bool, len(chunkPairs))
			for rows.Next() {
				var hashBytes []byte
				var idx uint32
				var lockingScript []byte
				var satoshis uint64
				if err := rows.Scan(&hashBytes, &idx, &lockingScript, &satoshis); err != nil {
					return err
				}
				var h chainhash.Hash
				copy(h[:], hashBytes)
				i, ok := chunkIndex[chunkKey{hash: h, idx: idx}]
				if !ok {
					// Postgres can return rows we didn't ask for only if the
					// query is wrong; defend anyway.
					continue
				}
				found[i] = true
				for _, ref := range chunkRefs[i] {
					txs[ref.txIdx].Inputs[ref.inputIdx].PreviousTxScript = bscript.NewFromBytes(lockingScript)
					txs[ref.txIdx].Inputs[ref.inputIdx].PreviousTxSatoshis = satoshis
				}
			}
			if err := rows.Err(); err != nil {
				return err
			}

			// Count any pairs the DB didn't return a row for; each maps to one
			// or more input slots still missing after this chunk.
			var localMissing int64
			for i, ok := range found {
				if !ok {
					localMissing += int64(len(chunkRefs[i]))
				}
			}
			if localMissing > 0 {
				missingInputs.Add(localMissing)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	if m := missingInputs.Load(); m > 0 {
		return errors.NewProcessingError("failed to decorate previous outputs: %d inputs could not be resolved", m)
	}

	return nil
}

func (s *Store) GetCounterConflicting(ctx context.Context, hash chainhash.Hash) ([]chainhash.Hash, error) {
	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "GetCounterConflicting",
		tracing.WithHistogram(prometheusSQLUtxoGetCounterConflicting),
	)

	defer deferFn()

	return utxo.GetCounterConflictingTxHashes(ctx, s, hash)
}

// GetConflictingChildren returns a list of conflicting transactions for a given transaction hash.
func (s *Store) GetConflictingChildren(ctx context.Context, hash chainhash.Hash) ([]chainhash.Hash, error) {
	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "GetConflicting",
		tracing.WithHistogram(prometheusSQLUtxoGetConflicting),
	)

	defer deferFn()

	return utxo.GetConflictingChildren(ctx, s, hash)
}

// SetConflicting marks a list of transactions as conflicting.
// It returns a list of spends that are affected by the conflicting status.
func (s *Store) SetConflicting(ctx context.Context, txHashes []chainhash.Hash, setValue bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	var deleteAtHeight sql.NullInt64

	if s.settings.GetUtxoStoreBlockHeightRetention() > 0 && setValue {
		// +1 because blockHeight lags during block processing (mirrors aerospike set_mined.go:162)
		if err := deleteAtHeight.Scan(int64(s.blockHeight.Load() + 1 + s.settings.GetUtxoStoreBlockHeightRetention())); err != nil {
			return nil, nil, err
		}
	}

	// When setting conflicting=true: set DAH only if not already set (mirrors aerospike line 944-951).
	// When clearing conflicting: clear DAH (conditions for deletion no longer met).
	var qUpdate string
	if setValue {
		qUpdate = `
			UPDATE transactions SET
			 conflicting = $2
			,delete_at_height = COALESCE(delete_at_height, $3)
			WHERE hash = $1
			RETURNING id
		`
	} else {
		qUpdate = `
			UPDATE transactions SET
			 conflicting = $2
			,delete_at_height = $3
			WHERE hash = $1
			RETURNING id
		`
	}

	affectedParentSpends := make([]*utxo.Spend, 0, len(txHashes))
	spendingTxHashes := make([]chainhash.Hash, 0, len(txHashes))

	var (
		transactionID int
		utxoHash      *chainhash.Hash
	)

	// Create a database transaction
	txn, err := s.db.Begin()
	if err != nil {
		return nil, nil, err
	}

	defer func() {
		_ = txn.Rollback()
	}()

	for _, conflictingTxHash := range txHashes {
		// get the extended tx
		txMeta, err := s.Get(ctx, &conflictingTxHash)
		if err != nil {
			return nil, nil, err
		}

		if err = txn.QueryRowContext(ctx, qUpdate, conflictingTxHash[:], setValue, deleteAtHeight).Scan(&transactionID); err != nil {
			return nil, nil, errors.NewStorageError("failed to set conflicting flag for %s", conflictingTxHash, err)
		}

		if err = s.updateParentConflictingChildren(ctx, transactionID, txMeta.Tx, txn); err != nil {
			return nil, nil, err
		}

		for i, input := range txMeta.Tx.Inputs {
			utxoHash, err = util.UTXOHashFromInput(input)
			if err != nil {
				return nil, nil, err
			}

			spend := &utxo.Spend{
				TxID:         input.PreviousTxIDChainHash(),
				Vout:         input.PreviousTxOutIndex,
				UTXOHash:     utxoHash,
				SpendingData: spendpkg.NewSpendingData(&conflictingTxHash, i),
			}

			affectedParentSpends = append(affectedParentSpends, spend)
		}

		for vOut, output := range txMeta.Tx.Outputs {
			vOutUint32, err := safeconversion.IntToUint32(vOut)
			if err != nil {
				return nil, nil, err
			}

			utxoHash, err = util.UTXOHashFromOutput(&conflictingTxHash, output, vOutUint32)
			if err != nil {
				return nil, nil, err
			}

			spend := &utxo.Spend{
				TxID:     &conflictingTxHash,
				Vout:     vOutUint32,
				UTXOHash: utxoHash,
			}

			// optimize to get all in 1 query
			spendResponse, err := s.GetSpend(ctx, spend)
			if err != nil {
				return nil, nil, err
			}

			if spendResponse.Status == int(utxo.Status_SPENT) && spendResponse.SpendingData != nil && spendResponse.SpendingData.TxID != nil {
				spendingTxHashes = append(spendingTxHashes, *spendResponse.SpendingData.TxID)
			}
		}
	}

	if err = txn.Commit(); err != nil {
		return nil, nil, errors.NewStorageError("failed to commit conflicting transaction", err)
	}

	return affectedParentSpends, spendingTxHashes, nil
}

// setLockedPipelined pipelines all lock UPDATE statements in a single network flush (postgres only).
func (s *Store) setLockedPipelined(ctx context.Context, txHashes []chainhash.Hash) error {
	sqlConn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer sqlConn.Close()

	return sqlConn.Raw(func(driverConn interface{}) error {
		pgxConn := driverConn.(*stdlib.Conn).Conn()

		batch := &pgx.Batch{}
		q := `UPDATE transactions SET locked = true, delete_at_height = NULL WHERE hash = $1`
		for i := range txHashes {
			batch.Queue(q, txHashes[i][:])
		}

		br := pgxConn.SendBatch(ctx, batch)
		if batchErr := br.Close(); batchErr != nil {
			return errors.NewStorageError("failed to pipeline set locked", batchErr)
		}
		return nil
	})
}

func (s *Store) SetLocked(ctx context.Context, txHashes []chainhash.Hash, setValue bool) error {
	// When locking (setValue=true), clear delete_at_height.
	// Mirrors aerospike setLocked: locked tx should not be pruned.
	// When unlocking (setValue=false), recalculate DAH based on current state.
	// Mirrors aerospike setLocked which restores DAH when conditions are met.

	if setValue {
		// Postgres: pipeline all lock UPDATEs in a single network flush
		if s.engine == "postgres" {
			return s.setLockedPipelined(ctx, txHashes)
		}
		// SQLite: sequential updates
		q := `
			UPDATE transactions
			SET locked = true, delete_at_height = NULL
			WHERE hash = $1
		`
		for _, txHash := range txHashes {
			if _, err := s.db.ExecContext(ctx, q, txHash[:]); err != nil {
				return errors.NewStorageError("failed to set locked flag for %s", txHash, err)
			}
		}
		return nil
	}

	// Postgres: single-hash unlock → use batcher.
	if s.unlockBatcher != nil && len(txHashes) == 1 {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		done := make(chan error, 1)
		s.unlockBatcher.PutCtx(ctx, &batchUnlockItem{hash: txHashes[0], done: done})
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			s.logger.Warnf("[setLockedBatched] context cancelled while waiting for batcher result — unlock may or may not be applied")
			return ctx.Err()
		}
	}

	// Postgres: bulk unlock + DAH in chunked UPDATE statements
	if s.engine == "postgres" {
		return s.setUnlockedBulk(ctx, txHashes)
	}

	// SQLite: sequential unlock + DAH
	txn, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		_ = txn.Rollback()
	}()

	q := `
		UPDATE transactions
		SET locked = false
		WHERE hash = $1
		RETURNING id
	`

	for _, txHash := range txHashes {
		var transactionID int
		err := txn.QueryRowContext(ctx, q, txHash[:]).Scan(&transactionID)
		if err != nil {
			return errors.NewStorageError("failed to clear locked flag for %s", txHash, err)
		}

		// Recalculate DAH now that the tx is unlocked
		if err = s.setDAH(ctx, txn, transactionID); err != nil {
			return err
		}
	}

	if err = txn.Commit(); err != nil {
		return errors.NewStorageError("failed to commit unlock transaction", err)
	}

	return nil
}

// setUnlockedBulk performs a bulk unlock + DAH recalculation for Postgres.
// Replaces 3N sequential queries (unlock + setDAH SELECT + setDAH UPDATE per tx)
// with 1 UPDATE per chunk of maxINClauseSize hashes.
func (s *Store) setUnlockedBulk(ctx context.Context, txHashes []chainhash.Hash) error {
	retention := s.settings.GetUtxoStoreBlockHeightRetention()

	for i := 0; i < len(txHashes); i += maxINClauseSize {
		end := i + maxINClauseSize
		if end > len(txHashes) {
			end = len(txHashes)
		}
		chunk := txHashes[i:end]

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		hashBytes := make([][]byte, len(chunk))
		for j := range chunk {
			hashBytes[j] = chunk[j][:]
		}

		if retention == 0 {
			// No DAH needed — simple bulk unlock
			inClause, args := buildINClause(hashBytes, 1)
			q := fmt.Sprintf(`UPDATE transactions SET locked = false WHERE hash IN %s`, inClause)
			if _, err := s.db.ExecContext(ctx, q, args...); err != nil {
				return errors.NewStorageError("failed to bulk unlock transactions", err)
			}
		} else {
			// Bulk unlock + DAH recalculation in a single UPDATE.
			// The CTE computes per-row state (unspent count, conflicting, etc.)
			// and the UPDATE applies the DAH logic from setDAH in one pass.
			newDAH := int64(s.blockHeight.Load() + 1 + retention)

			// $1 = newDAH, hashes start at $2
			inClause, hashArgs := buildINClause(hashBytes, 2)
			args := make([]interface{}, 0, 1+len(hashArgs))
			args = append(args, newDAH)
			args = append(args, hashArgs...)

			q := fmt.Sprintf(`
				WITH tx_state AS (
					SELECT t.id,
					       t.conflicting,
					       t.preserve_until IS NOT NULL AS has_preserve,
					       t.unmined_since IS NULL AS on_longest_chain,
					       t.delete_at_height,
					       (SELECT count(*) FROM outputs o
					        WHERE o.transaction_id = t.id AND o.spending_data IS NULL) AS unspent,
					       EXISTS(SELECT 1 FROM block_ids bi
					        WHERE bi.transaction_id = t.id) AS has_blocks
					FROM transactions t
					WHERE t.hash IN %s
				)
				UPDATE transactions t
				SET locked = false,
				    delete_at_height = CASE
				        WHEN s.has_preserve THEN t.delete_at_height
				        WHEN s.conflicting AND t.delete_at_height IS NULL THEN $1
				        WHEN s.conflicting THEN t.delete_at_height
				        WHEN s.unspent = 0 AND s.has_blocks AND s.on_longest_chain
				             AND (t.delete_at_height IS NULL OR t.delete_at_height < $1) THEN $1
				        WHEN s.unspent = 0 AND s.has_blocks AND s.on_longest_chain THEN t.delete_at_height
				        WHEN t.delete_at_height IS NOT NULL THEN NULL
				        ELSE t.delete_at_height
				    END
				FROM tx_state s
				WHERE t.id = s.id
			`, inClause)

			if _, err := s.db.ExecContext(ctx, q, args...); err != nil {
				return errors.NewStorageError("failed to bulk unlock+DAH transactions", err)
			}
		}
	}

	return nil
}

// MarkTransactionsOnLongestChain updates unmined_since for transactions based on their chain status.
//
// This function is critical for maintaining data integrity during blockchain reorganizations.
// It ensures transactions have the correct unmined_since value based on whether they are on
// the longest (main) chain or on a side chain.
//
// Behavior:
//   - onLongestChain=true: Clears unmined_since (transaction is mined on main chain)
//   - onLongestChain=false: Sets unmined_since to current height (transaction is unmined)
//
// CRITICAL - Resilient Error Handling (Must Not Fail Fast):
// This function attempts to update ALL transactions even if some fail. This is essential during
// large reorgs where millions of transactions need updating.
//
// Error handling strategy:
//   - Processes ALL transactions (does not stop on first error)
//   - Collects up to 10 errors for logging/debugging (prevents log spam)
//   - Logs summary: attempted, succeeded, failed counts
//   - Returns aggregated errors after attempting all transactions
//   - Missing transactions trigger FATAL error (data corruption)
//
// Why resilient processing is critical:
//   - Large reorgs can affect millions of transactions
//   - Transient network errors shouldn't prevent updating other transactions
//   - Maximizes data integrity by updating as many as possible
//   - Missing transaction = unrecoverable (FATAL to prevent corrupt state)
//
// Timing guarantee:
// This function is called synchronously from reset/reorg operations. By the time cleanup
// operations run (via setBestBlockHeader), all MarkTransactionsOnLongestChain calls have
// completed, ensuring cleanup only sees consistent transaction state.
//
// Called from:
//   - BlockAssembler.reset() - marks moveBack transactions during large reorgs
//   - SubtreeProcessor.reorgBlocks() - marks transactions during small/medium reorgs
//   - loadUnminedTransactions() - fixes data inconsistencies
//
// Parameters:
//   - ctx: Context for cancellation
//   - txHashes: Transactions to update (can be millions during large reorgs)
//   - onLongestChain: true = clear unmined_since (mined), false = set unmined_since (unmined)
//
// Returns:
//   - error: Aggregated errors (up to 10) if any failures occurred
//   - Note: Function calls logger.Fatalf for missing transactions before returning
func (s *Store) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	if len(txHashes) == 0 {
		return nil
	}

	attempted := len(txHashes)
	totalUpdated := 0
	allErrors := make([]error, 0, 10)
	errorCount := 0

	// Convert all hashes to byte arrays up front
	allHashBytes := make([][]byte, len(txHashes))
	for i := range txHashes {
		allHashBytes[i] = txHashes[i][:]
	}

	currentBlockHeight := s.GetBlockHeight()

	// Process in chunks to stay within SQLite's parameter limit
	for i := 0; i < len(allHashBytes); i += maxINClauseSize {
		end := i + maxINClauseSize
		if end > len(allHashBytes) {
			end = len(allHashBytes)
		}
		chunk := allHashBytes[i:end]

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var q string
		var args []interface{}

		if onLongestChain {
			// hashes start at $1
			inClause, inArgs := buildINClause(chunk, 1)
			q = fmt.Sprintf(`UPDATE transactions SET unmined_since = NULL WHERE hash IN %s`, inClause)
			args = inArgs
		} else {
			// currentBlockHeight is $1, hashes start at $2
			inClause, inArgs := buildINClause(chunk, 2)
			q = fmt.Sprintf(`UPDATE transactions SET unmined_since = $1 WHERE hash IN %s`, inClause)
			args = append([]interface{}{currentBlockHeight}, inArgs...)
		}

		result, err := s.db.ExecContext(ctx, q, args...)
		if err != nil {
			errorCount += len(chunk)
			if len(allErrors) < 10 {
				s.logger.Errorf("[MarkTransactionsOnLongestChain] chunk %d-%d error: %v", i, end-1, err)
				allErrors = append(allErrors, errors.NewStorageError("failed to mark chunk %d-%d: %v", i, end-1, err))
			}
			continue
		}

		rowsAffected, _ := result.RowsAffected()
		totalUpdated += int(rowsAffected)

		// If fewer rows updated than expected, find which ones are missing
		if int(rowsAffected) < len(chunk) {
			missingCount := len(chunk) - int(rowsAffected)
			errorCount += missingCount

			// Query to find which hashes actually exist to identify the missing ones
			qCheck := fmt.Sprintf(`SELECT hash FROM transactions WHERE hash IN %s`, func() string {
				cl, _ := buildINClause(chunk, 1)
				return cl
			}())
			_, checkArgs := buildINClause(chunk, 1)
			rows, qErr := s.db.QueryContext(ctx, qCheck, checkArgs...)
			if qErr != nil {
				s.logger.Errorf("[MarkTransactionsOnLongestChain] could not identify missing txs in chunk %d-%d: %v", i, end-1, qErr)
			} else {
				foundHashes := make(map[string]bool)
				for rows.Next() {
					var hb []byte
					if scanErr := rows.Scan(&hb); scanErr == nil {
						foundHashes[string(hb)] = true
					}
				}
				rows.Close()

				missingLogged := 0
				for _, hb := range chunk {
					if !foundHashes[string(hb)] {
						var h chainhash.Hash
						copy(h[:], hb)
						s.logger.Fatalf("CRITICAL: MISSING transaction %s during MarkTransactionsOnLongestChain - data integrity compromised", h)
						missingLogged++
						if missingLogged >= 10 {
							break
						}
					}
				}
			}
		}
	}

	// Log summary
	s.logger.Infof("[MarkTransactionsOnLongestChain] completed: attempted=%d, updated=%d, failed=%d, onLongestChain=%t",
		attempted, totalUpdated, errorCount, onLongestChain)

	if len(allErrors) > 0 {
		if errorCount > 10 {
			s.logger.Errorf("[MarkTransactionsOnLongestChain] only returned first 10 of %d errors", errorCount)
		}
		return errors.Join(allErrors...)
	}

	return nil
}

// DBExecutor interface for database operations needed by schema creation
type DBExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Close() error
}

// Advisory lock ID for UTXO schema creation serialization across pods.
const utxoSchemaLockID int64 = 7_265_726_117 // "tera" + "ux" in ASCII-ish

func createPostgresSchema(db *usql.DB) error {
	return usql.WithAdvisoryLock(context.Background(), db, utxoSchemaLockID, func() error {
		return createPostgresSchemaImpl(db)
	})
}

// createPostgresSchemaImpl contains the actual implementation, now testable
func createPostgresSchemaImpl(db DBExecutor) error {
	if _, err := db.Exec(`
      CREATE TABLE IF NOT EXISTS transactions (
         id               BIGSERIAL PRIMARY KEY
        ,hash             BYTEA NOT NULL
        ,version          BIGINT NOT NULL
        ,lock_time        BIGINT NOT NULL
        ,fee              BIGINT NOT NULL
		,size_in_bytes    BIGINT NOT NULL
		,coinbase         BOOLEAN DEFAULT FALSE NOT NULL
		,frozen           BOOLEAN DEFAULT FALSE NOT NULL
        ,conflicting      BOOLEAN DEFAULT FALSE NOT NULL
        ,locked           BOOLEAN DEFAULT FALSE NOT NULL
        ,delete_at_height BIGINT
        ,unmined_since    BIGINT
        ,preserve_until   BIGINT
        ,inserted_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	  );
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create transactions table - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_transactions_hash ON transactions (hash);`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create ux_transactions_hash index - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS px_unmined_since_transactions ON transactions (unmined_since) WHERE unmined_since IS NOT NULL;`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create px_unmined_since_transactions index - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS ux_transactions_delete_at_height ON transactions (delete_at_height) WHERE delete_at_height IS NOT NULL;`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create ux_transactions_delete_at_height index - [%+v]", err)
	}

	// The previous transaction hash may exist in this table
	if _, err := db.Exec(`
      CREATE TABLE IF NOT EXISTS inputs (
          transaction_id            BIGINT NOT NULL REFERENCES transactions(id) ON DELETE CASCADE
         ,idx                       BIGINT NOT NULL
         ,previous_transaction_hash BYTEA NOT NULL
         ,previous_tx_idx           BIGINT NOT NULL
         ,previous_tx_satoshis      BIGINT NOT NULL
         ,previous_tx_script        BYTEA
         ,unlocking_script          BYTEA NOT NULL
         ,sequence_number           BIGINT NOT NULL
      ,PRIMARY KEY (transaction_id, idx)
	  );
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create inputs table - [%+v]", err)
	}

	// Ensure inputs FK has ON DELETE CASCADE — only drop+recreate if it exists without CASCADE
	if _, err := db.Exec(`
		DO $$
		BEGIN
			IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'inputs_transaction_id_fkey' AND contype = 'f' AND confdeltype != 'c') THEN
				ALTER TABLE inputs DROP CONSTRAINT inputs_transaction_id_fkey;
			END IF;
			IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'inputs_transaction_id_fkey' AND contype = 'f') THEN
				ALTER TABLE inputs ADD CONSTRAINT inputs_transaction_id_fkey FOREIGN KEY (transaction_id) REFERENCES transactions(id) ON DELETE CASCADE;
			END IF;
		END $$;
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not ensure CASCADE foreign key on inputs table - [%+v]", err)
	}

	// All fields are NOT NULL except for the spending_data which is NULL for unspent outputs.
	// The utxo_hash is a hash of the transaction_id, idx, locking_script and satoshis and is used as a checksum of a utxo.
	// The spending_data is the transaction_id of the transaction that spends this utxo
	if _, err := db.Exec(`
      CREATE TABLE IF NOT EXISTS outputs (
         transaction_id           BIGINT NOT NULL REFERENCES transactions(id) ON DELETE CASCADE
        ,idx                      BIGINT NOT NULL
        ,locking_script           BYTEA NOT NULL
        ,satoshis                 BIGINT NOT NULL
        ,coinbase_spending_height BIGINT NOT NULL
        ,utxo_hash 			      BYTEA NOT NULL
        ,spending_data            BYTEA
        ,frozen                   BOOLEAN DEFAULT FALSE
        ,spendableIn              INT
        ,PRIMARY KEY (transaction_id, idx)
	  );
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create outputs table - [%+v]", err)
	}

	// Ensure outputs FK has ON DELETE CASCADE — only drop+recreate if it exists without CASCADE
	if _, err := db.Exec(`
		DO $$
		BEGIN
			IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'outputs_transaction_id_fkey' AND contype = 'f' AND confdeltype != 'c') THEN
				ALTER TABLE outputs DROP CONSTRAINT outputs_transaction_id_fkey;
			END IF;
			IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'outputs_transaction_id_fkey' AND contype = 'f') THEN
				ALTER TABLE outputs ADD CONSTRAINT outputs_transaction_id_fkey FOREIGN KEY (transaction_id) REFERENCES transactions(id) ON DELETE CASCADE;
			END IF;
		END $$;
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not ensure CASCADE foreign key on outputs table - [%+v]", err)
	}

	if _, err := db.Exec(`
      CREATE TABLE IF NOT EXISTS block_ids (
          transaction_id BIGINT NOT NULL REFERENCES transactions(id) ON DELETE CASCADE
         ,block_id       BIGINT NOT NULL
         ,block_height   BIGINT NOT NULL
         ,subtree_idx  BIGINT NOT NULL
         ,PRIMARY KEY (transaction_id, block_id)
	  );
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create block_ids table - [%+v]", err)
	}

	// Add new columns to block_ids table if they don't exist
	if _, err := db.Exec(`
		DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'block_ids'::regclass AND attname = 'block_height' AND NOT attisdropped) THEN
				ALTER TABLE block_ids ADD COLUMN block_height BIGINT NOT NULL;
			END IF;

			IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'block_ids'::regclass AND attname = 'subtree_idx' AND NOT attisdropped) THEN
				ALTER TABLE block_ids ADD COLUMN subtree_idx BIGINT NOT NULL;
			END IF;
		END $$;
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not add new columns to block_ids table - [%+v]", err)
	}

	// Add unmined_since column to transactions table if it doesn't exist
	if _, err := db.Exec(`
		DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'transactions'::regclass AND attname = 'unmined_since' AND NOT attisdropped) THEN
				ALTER TABLE transactions ADD COLUMN unmined_since BIGINT;
			END IF;
		END $$;
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not add unmined_since column to transactions table - [%+v]", err)
	}

	// Add preserve_until column to transactions table if it doesn't exist
	if _, err := db.Exec(`
		DO $$
		BEGIN
			IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = 'transactions'::regclass AND attname = 'preserve_until' AND NOT attisdropped) THEN
				ALTER TABLE transactions ADD COLUMN preserve_until BIGINT;
			END IF;
		END $$;
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not add preserve_until column to transactions table - [%+v]", err)
	}

	// Ensure block_ids FK has ON DELETE CASCADE — only drop+recreate if it exists without CASCADE
	if _, err := db.Exec(`
		DO $$
		BEGIN
			IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'block_ids_transaction_id_fkey' AND contype = 'f' AND confdeltype != 'c') THEN
				ALTER TABLE block_ids DROP CONSTRAINT block_ids_transaction_id_fkey;
			END IF;
			IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'block_ids_transaction_id_fkey' AND contype = 'f') THEN
				ALTER TABLE block_ids ADD CONSTRAINT block_ids_transaction_id_fkey FOREIGN KEY (transaction_id) REFERENCES transactions(id) ON DELETE CASCADE;
			END IF;
		END $$;
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not ensure CASCADE foreign key on block_ids table - [%+v]", err)
	}

	if _, err := db.Exec(`
      CREATE TABLE IF NOT EXISTS conflicting_children (
         transaction_id        BIGINT NOT NULL REFERENCES transactions(id) ON DELETE CASCADE
        ,child_transaction_id  BIGINT NOT NULL
        ,PRIMARY KEY (transaction_id, child_transaction_id)
	  );
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create conflicting_children table - [%+v]", err)
	}

	// Partial index over unspent outputs, keyed by parent transaction_id.
	// Used by the spend-path bulk UPDATE's unspent_before CTE stage, which
	// counts how many outputs of each touched parent are still unspent to
	// decide whether this batch has drained the parent's last unspent output
	// (so DAH can be set). Without this index Postgres falls back to the
	// composite PK's leading column only — scanning every output of every
	// touched parent and filtering spending_data IS NULL after the read.
	// For data-carrier / large-fan-out parents that's thousands of rows per
	// parent per batch and was surfacing as multi-second IO:DataFileRead
	// waits inside sendSpendBatch on mainnet. With this partial index the
	// count becomes an index-only scan over just the unspent rows, which on
	// a healthy chain is a small minority of the outputs table.
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS px_outputs_unspent_by_parent ON outputs (transaction_id) WHERE spending_data IS NULL;`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create px_outputs_unspent_by_parent index - [%+v]", err)
	}

	return nil
}

func createSqliteSchema(db *usql.DB) error {
	if _, err := db.Exec(`
      CREATE TABLE IF NOT EXISTS transactions (
         id               INTEGER PRIMARY KEY AUTOINCREMENT
        ,hash             BLOB NOT NULL
        ,version          BIGINT NOT NULL
        ,lock_time        BIGINT NOT NULL
        ,fee              BIGINT NOT NULL
        ,size_in_bytes    BIGINT NOT NULL
		,coinbase         BOOLEAN DEFAULT FALSE NOT NULL
		,frozen           BOOLEAN DEFAULT FALSE NOT NULL
        ,conflicting      BOOLEAN DEFAULT FALSE NOT NULL
        ,locked           BOOLEAN DEFAULT FALSE NOT NULL
        ,delete_at_height BIGINT
        ,unmined_since    BIGINT
        ,preserve_until   BIGINT
        ,inserted_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	  );
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create transactions table - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_transactions_hash ON transactions (hash);`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create ux_transactions_hash idx - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS px_unmined_since_transactions ON transactions (unmined_since) WHERE unmined_since IS NOT NULL;`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create px_unmined_since_transactions idx - [%+v]", err)
	}

	// The previous transaction hash may exist in this table
	if _, err := db.Exec(`
      CREATE TABLE IF NOT EXISTS inputs (
         transaction_id            INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE
        ,idx                       BIGINT NOT NULL
        ,previous_transaction_hash BLOB NOT NULL
        ,previous_tx_idx           BIGINT NOT NULL
        ,previous_tx_satoshis      BIGINT NOT NULL
        ,previous_tx_script        BLOB
        ,unlocking_script          BLOB NOT NULL
        ,sequence_number           BIGINT NOT NULL
      ,PRIMARY KEY (transaction_id, idx)
	  );
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create inputs table - [%+v]", err)
	}

	// All fields are NOT NULL except for the spending_data which is NULL for unspent outputs.
	// The utxo_hash is a hash of the transaction_id, idx, locking_script and satoshis and is used as a checksum of a utxo.
	// The spending_data is the transaction_id of the transaction and vin that spends this utxo
	if _, err := db.Exec(`
      CREATE TABLE IF NOT EXISTS outputs (
         transaction_id           INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE
        ,idx                      BIGINT NOT NULL
        ,locking_script           BLOB NOT NULL
        ,satoshis                 BIGINT NOT NULL
        ,coinbase_spending_height BIGINT NOT NULL
        ,utxo_hash                BLOB NOT NULL
        ,spending_data            BLOB
        ,frozen                   BOOLEAN DEFAULT FALSE
        ,spendableIn              INT
        ,PRIMARY KEY (transaction_id, idx)
	  );
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create outputs table - [%+v]", err)
	}

	// Partial index over unspent outputs — see Postgres schema for rationale.
	// SQLite supports partial indexes via the WHERE clause.
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS px_outputs_unspent_by_parent ON outputs (transaction_id) WHERE spending_data IS NULL;`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create px_outputs_unspent_by_parent index - [%+v]", err)
	}

	if _, err := db.Exec(`
      CREATE TABLE IF NOT EXISTS block_ids (
         transaction_id INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE
        ,block_id 			 BIGINT NOT NULL
        ,block_height        BIGINT NOT NULL
        ,subtree_idx       BIGINT NOT NULL
        ,PRIMARY KEY (transaction_id, block_id)
	  );
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create block_ids table - [%+v]", err)
	}

	if _, err := db.Exec(`
      CREATE TABLE IF NOT EXISTS conflicting_children (
         transaction_id        INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE
        ,child_transaction_id  BIGINT NOT NULL
        ,PRIMARY KEY (transaction_id, child_transaction_id)
	  );
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create conflicting_children table - [%+v]", err)
	}

	// Check if we need to migrate the block_ids table in SQLite
	rows, err := db.Query(`
		SELECT COUNT(*)
		FROM pragma_table_info('block_ids')
		WHERE name IN ('block_height', 'subtree_idx')
	`)
	if err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not check block_ids columns - [%+v]", err)
	}

	var columnCount int

	if rows.Next() {
		if err := rows.Scan(&columnCount); err != nil {
			_ = db.Close()
			return errors.NewStorageError("could not scan column count - [%+v]", err)
		}
	}

	rows.Close()

	// Only perform migration if the new columns don't exist
	if columnCount < 2 {
		// For SQLite, we just recreate the tables with the correct constraints
		// SQLite doesn't support dropping foreign key constraints directly
		if _, err := db.Exec(`
			CREATE TABLE IF NOT EXISTS block_ids_new (
				transaction_id INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE
				,block_id     INTEGER NOT NULL
				,block_height INTEGER NOT NULL
				,subtree_idx  INTEGER NOT NULL
				,PRIMARY KEY (transaction_id, block_id)
			);

			INSERT OR IGNORE INTO block_ids_new
			SELECT transaction_id, block_id, 0, 0
			FROM block_ids;

			DROP TABLE block_ids;

			ALTER TABLE block_ids_new RENAME TO block_ids;
		`); err != nil {
			_ = db.Close()
			return errors.NewStorageError("could not migrate block_ids table - [%+v]", err)
		}
	}

	// Check if we need to migrate the inputs table (check if ON DELETE CASCADE is missing)
	rows, err = db.Query(`
		SELECT sql FROM sqlite_master
		WHERE type='table' AND name='inputs'
		AND sql NOT LIKE '%ON DELETE CASCADE%'
	`)
	if err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not check inputs table - [%+v]", err)
	}

	needsInputsMigration := rows.Next()

	rows.Close()

	if needsInputsMigration {
		if _, err := db.Exec(`
			CREATE TABLE IF NOT EXISTS inputs_new (
				transaction_id               INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE
				,idx                        INTEGER NOT NULL
				,previous_transaction_hash  BLOB NOT NULL
				,previous_tx_idx           INTEGER NOT NULL
				,previous_tx_satoshis      INTEGER NOT NULL
				,previous_tx_script        BLOB
				,unlocking_script          BLOB NOT NULL
				,sequence_number           INTEGER NOT NULL
				,PRIMARY KEY (transaction_id, idx)
			);

			INSERT OR IGNORE INTO inputs_new
			SELECT * FROM inputs;

			DROP TABLE inputs;

			ALTER TABLE inputs_new RENAME TO inputs;
		`); err != nil {
			_ = db.Close()
			return errors.NewStorageError("could not migrate inputs table - [%+v]", err)
		}
	}

	// Check if we need to migrate the outputs table (check if ON DELETE CASCADE is missing)
	rows, err = db.Query(`
		SELECT sql FROM sqlite_master
		WHERE type='table' AND name='outputs'
		AND sql NOT LIKE '%ON DELETE CASCADE%'
	`)
	if err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not check outputs table - [%+v]", err)
	}

	needsOutputsMigration := rows.Next()

	rows.Close()

	if needsOutputsMigration {
		if _, err := db.Exec(`
			CREATE TABLE IF NOT EXISTS outputs_new (
				transaction_id    INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE
				,idx             INTEGER NOT NULL
				,satoshis        INTEGER NOT NULL
				,locking_script  BLOB NOT NULL
				,utxo_hash       BLOB NOT NULL
				,PRIMARY KEY (transaction_id, idx)
			);

			INSERT OR IGNORE INTO outputs_new
			SELECT * FROM outputs;

			DROP TABLE outputs;

			ALTER TABLE outputs_new RENAME TO outputs;
		`); err != nil {
			_ = db.Close()
			return errors.NewStorageError("could not migrate outputs table - [%+v]", err)
		}
	}

	// Check if we need to add the unmined_since column to transactions table
	rows, err = db.Query(`
		SELECT COUNT(*)
		FROM pragma_table_info('transactions')
		WHERE name = 'unmined_since'
	`)
	if err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not check transactions table for unmined_since column - [%+v]", err)
	}

	var unminedSinceColumnCount int

	if rows.Next() {
		if err := rows.Scan(&unminedSinceColumnCount); err != nil {
			_ = db.Close()
			return errors.NewStorageError("could not scan unmined_since column count - [%+v]", err)
		}
	}

	rows.Close()

	// Add unmined_since column if it doesn't exist
	if unminedSinceColumnCount == 0 {
		if _, err := db.Exec(`
			ALTER TABLE transactions ADD COLUMN unmined_since BIGINT;
		`); err != nil {
			_ = db.Close()
			return errors.NewStorageError("could not add unmined_since column to transactions table - [%+v]", err)
		}
	}

	// Check if we need to add the preserve_until column to transactions table
	rows, err = db.Query(`
		SELECT COUNT(*)
		FROM pragma_table_info('transactions')
		WHERE name = 'preserve_until'
	`)
	if err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not check transactions table for preserve_until column - [%+v]", err)
	}

	var preserveUntilColumnCount int

	if rows.Next() {
		if err := rows.Scan(&preserveUntilColumnCount); err != nil {
			_ = db.Close()
			return errors.NewStorageError("could not scan preserve_until column count - [%+v]", err)
		}
	}

	rows.Close()

	// Add preserve_until column if it doesn't exist
	if preserveUntilColumnCount == 0 {
		if _, err := db.Exec(`
			ALTER TABLE transactions ADD COLUMN preserve_until BIGINT;
		`); err != nil {
			_ = db.Close()
			return errors.NewStorageError("could not add preserve_until column to transactions table - [%+v]", err)
		}
	}

	return nil
}

// QueryOldUnminedTransactions returns transaction hashes for unmined transactions older than the cutoff height.
// This method is used by the store-agnostic cleanup implementation.
func (s *Store) QueryOldUnminedTransactions(ctx context.Context, cutoffBlockHeight uint32) ([]chainhash.Hash, error) {

	// Query to find old unmined transactions (extracted from PreserveParentsOfOldUnminedTransactions)
	q := `
		SELECT hash
		FROM transactions
		WHERE unmined_since IS NOT NULL
		  AND unmined_since <= $1
		ORDER BY unmined_since
		LIMIT 1000
	`

	rows, err := s.db.QueryContext(ctx, q, cutoffBlockHeight)
	if err != nil {
		return nil, errors.NewStorageError("failed to query old unmined transactions", err)
	}
	defer rows.Close()

	var txHashes []chainhash.Hash

	for rows.Next() {
		var hashBytes []byte

		if err := rows.Scan(&hashBytes); err != nil {
			s.logger.Errorf("[QueryOldUnminedTransactions] Error scanning transaction row: %v", err)
			continue
		}

		txHash := chainhash.Hash{}
		copy(txHash[:], hashBytes)
		txHashes = append(txHashes, txHash)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.NewStorageError("error iterating unmined transactions", err)
	}

	return txHashes, nil
}

// PreserveTransactions marks transactions to be preserved from deletion until a specific block height.
// This clears any existing DeleteAtHeight and sets PreserveUntil to the specified height.
// Used to protect parent transactions when cleaning up unmined transactions.
//
// IDEMPOTENCY: This operation is safely re-runnable:
// - SQL UPDATE returns 0 rows affected (not an error) if records already deleted
// - Multiple preservation attempts with same preserveUntil are idempotent
// - Missing transactions are logged but don't cause failures
func (s *Store) PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	if len(txIDs) == 0 {
		return nil
	}

	totalAffected := int64(0)

	// Process in chunks to stay within SQLite's parameter limit
	for i := 0; i < len(txIDs); i += maxINClauseSize {
		end := i + maxINClauseSize
		if end > len(txIDs) {
			end = len(txIDs)
		}

		chunk := make([][]byte, end-i)
		for j, txID := range txIDs[i:end] {
			chunk[j] = txID[:]
		}

		// preserveUntilHeight is $1, hashes start at $2
		inClause, inArgs := buildINClause(chunk, 2)
		query := fmt.Sprintf(`
			UPDATE transactions
			SET preserve_until = $1, delete_at_height = NULL
			WHERE hash IN %s
		`, inClause)
		args := append([]interface{}{preserveUntilHeight}, inArgs...)

		result, err := s.db.ExecContext(ctx, query, args...)
		if err != nil {
			return errors.NewStorageError("failed to preserve transactions", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			s.logger.Warnf("[PreserveTransactions] Could not get rows affected: %v", err)
		} else {
			totalAffected += rowsAffected
		}
	}

	s.logger.Debugf("[PreserveTransactions] Successfully preserved %d out of %d transactions", totalAffected, len(txIDs))

	return nil
}

// ProcessExpiredPreservations handles transactions whose preservation period has expired.
// For each transaction with PreserveUntil <= currentHeight, it sets an appropriate DeleteAtHeight
// and clears the PreserveUntil field.
func (s *Store) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error {
	deleteAtHeight := currentHeight + s.settings.GetUtxoStoreBlockHeightRetention()

	query := `
		UPDATE transactions
		SET delete_at_height = $1, preserve_until = NULL
		WHERE preserve_until IS NOT NULL AND preserve_until <= $2
	`

	result, err := s.db.ExecContext(ctx, query, deleteAtHeight, currentHeight)
	if err != nil {
		return errors.NewStorageError("failed to process expired preservations", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		s.logger.Warnf("[ProcessExpiredPreservations] Could not get rows affected: %v", err)
	} else {
		s.logger.Infof("[ProcessExpiredPreservations] Processed %d expired preservations at height %d", rowsAffected, currentHeight)
	}

	return nil
}

// RawDB returns the underlying *usql.DB connection. For test/debug use only.
func (s *Store) RawDB() *usql.DB {
	return s.db
}

// maxINClauseSize is the maximum number of hash placeholders in a single IN clause.
// SQLite's SQLITE_MAX_VARIABLE_NUMBER default is 999. We use 400 to leave
// headroom for additional parameters in the same query (e.g. block_id, height).
// maxPostgresParams is the safe upper bound for SQL bind parameters per statement.
// PostgreSQL supports 65535, but SQLite defaults to 999. Use the lower limit to cover both.
const maxPostgresParams = 999

const maxINClauseSize = 400

// postgresBatchDecorateChunkSize is used by BatchPreviousOutputsDecorate when
// the store is backed by Postgres. Postgres allows up to 65535 bind parameters
// per statement; each (hash, idx) pair uses 2 params, so 4000 pairs → 8000
// params, well under the limit. Larger chunks cut the number of round-trips by
// ~10× on a typical 1800-tx block, which is the dominant factor in per-block
// wall time during legacy sync. SQLite keeps maxINClauseSize (400) to stay
// under its 999-param default.
const postgresBatchDecorateChunkSize = 4000

// batchDecorateChunkSizeOverride is set by tests to force a smaller chunk size
// so the multi-chunk path is exercised without building 400+ fixtures. Zero
// means "use the engine-appropriate default". Never set in production.
var batchDecorateChunkSizeOverride int

// buildINClause generates a SQL IN clause placeholder string and corresponding args.
// startIdx is the 1-based parameter index for the first placeholder ($startIdx, $startIdx+1, ...).
// Returns the clause string like "($3,$4,$5)" and the args slice.
func buildINClause(hashes [][]byte, startIdx int) (string, []interface{}) {
	placeholders := make([]string, len(hashes))
	args := make([]interface{}, len(hashes))
	for i, h := range hashes {
		placeholders[i] = fmt.Sprintf("$%d", startIdx+i)
		args[i] = h
	}
	return "(" + strings.Join(placeholders, ",") + ")", args
}

// outpointPair represents a (transaction hash, output index) pair for a composite IN filter.
type outpointPair struct {
	hash []byte
	idx  uint32
}

// buildCompositeValuesPairs generates a VALUES list for (hash, idx) pairs
// intended to be used as a join source, e.g.
//
//	FROM (VALUES (...)) AS v(h, i) JOIN transactions t ON t.hash = v.h ...
//
// The Postgres variant annotates the FIRST row with `::bytea` / `::bigint`
// type casts so the planner can resolve VALUES column types early — without
// those, postgres defaults placeholder types to `text`, which forces runtime
// coercion at every join and blocks index use on transactions(hash). SQLite
// infers VALUES column types dynamically from the bound values and doesn't
// need (or support) the `::` cast syntax.
//
// startIdx is the 1-based parameter index for the first placeholder. Returns
// the `VALUES ...` clause (no surrounding parentheses — caller wraps it) and
// the args slice. For empty input, returns ("", nil).
//
// Why not `WHERE (t.hash, o.idx) IN ((h1,i1),...)`? Postgres plans that as
// `transaction_id = t.id`-only index scan on outputs_pkey with the idx
// predicate as a post-read filter, which reads every output of every matched
// parent. The VALUES-join form forces a per-pair composite-PK lookup
// (`transaction_id = t.id AND idx = v.i`), an ~90× cost reduction on 3-pair
// cases and a much larger reduction when parent txs have many outputs.
func buildCompositeValuesPairs(pairs []outpointPair, startIdx int, engine string) (string, []interface{}) {
	if len(pairs) == 0 {
		return "", nil
	}
	groups := make([]string, len(pairs))
	args := make([]interface{}, 0, len(pairs)*2)
	postgres := engine == string(util.Postgres)
	for i, p := range pairs {
		a := startIdx + (2 * i)
		b := a + 1
		if i == 0 && postgres {
			// Annotate types on the first row so postgres infers v.h as bytea
			// and v.i as bigint instead of text/unknown.
			groups[i] = fmt.Sprintf("($%d::bytea,$%d::bigint)", a, b)
		} else {
			groups[i] = fmt.Sprintf("($%d,$%d)", a, b)
		}
		args = append(args, p.hash, p.idx)
	}
	return "VALUES " + strings.Join(groups, ","), args
}

// buildMultiValueInsert generates a multi-row VALUES clause with positional parameters.
// baseSQL is the INSERT ... VALUES prefix (without the actual values).
// colsPerRow is the number of columns per row.
// numRows is the number of rows to insert.
// Returns the full SQL string and the starting parameter index for args population.
// Example: buildMultiValueInsert("INSERT INTO t (a,b) VALUES ", 2, 3, 1)
// -> "INSERT INTO t (a,b) VALUES ($1,$2),($3,$4),($5,$6)"
func buildMultiValueInsert(baseSQL string, colsPerRow, numRows, startIdx int) string {
	var sb strings.Builder
	sb.WriteString(baseSQL)
	paramIdx := startIdx
	for row := 0; row < numRows; row++ {
		if row > 0 {
			sb.WriteByte(',')
		}
		sb.WriteByte('(')
		for col := 0; col < colsPerRow; col++ {
			if col > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(fmt.Sprintf("$%d", paramIdx))
			paramIdx++
		}
		sb.WriteByte(')')
	}
	return sb.String()
}

// isLockError checks if the error is a database lock/deadlock error
func isLockError(err error) bool {
	if err == nil {
		return false
	}

	// PostgreSQL deadlock/lock errors (pgx driver)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == usql.PgErrSerializationFail || pgErr.Code == usql.PgErrDeadlockDetected || pgErr.Code == usql.PgErrLockNotAvailable
	}

	// PostgreSQL deadlock/lock errors (lib/pq fallback)
	if pqErr, ok := err.(*pq.Error); ok {
		return pqErr.Code == usql.PgErrSerializationFail || pqErr.Code == usql.PgErrDeadlockDetected || pqErr.Code == usql.PgErrLockNotAvailable
	}

	// SQLite busy/locked errors
	if sqliteErr, ok := err.(*sqlite.Error); ok {
		return sqliteErr.Code() == sqlite3.SQLITE_BUSY || sqliteErr.Code() == sqlite3.SQLITE_LOCKED
	}

	// Check error message for common lock patterns
	errStr := err.Error()
	return strings.Contains(errStr, "database is locked") ||
		strings.Contains(errStr, "deadlock") ||
		strings.Contains(errStr, "lock timeout")
}

// sendUnlockBatch processes a batch of SetLocked(false) calls via setUnlockedBulk,
// which chunks large batches into multiple UPDATE statements (maxINClauseSize=400).
func (s *Store) sendUnlockBatch(batch []*batchUnlockItem) {
	ctx := s.ctx

	if len(batch) == 1 {
		hashes := []chainhash.Hash{batch[0].hash}
		batch[0].done <- s.setUnlockedBulk(ctx, hashes)
		return
	}

	hashes := make([]chainhash.Hash, len(batch))
	for i, item := range batch {
		hashes[i] = item.hash
	}

	err := s.setUnlockedBulk(ctx, hashes)
	for _, item := range batch {
		item.done <- err
	}
}
