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
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
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
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/bsv-blockchain/teranode/util/usql"
	pq "github.com/lib/pq"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// Store implements the UTXO store interface using a SQL database backend.
type Store struct {
	logger          ulogger.Logger
	settings        *settings.Settings
	db              *usql.DB
	storeURL        *url.URL
	engine          string
	blockHeight     atomic.Uint32
	medianBlockTime atomic.Uint32
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
	}

	return s, nil
}

func (s *Store) SetBlockHeight(blockHeight uint32) error {
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

	// ctx, cancelTimeout := context.WithTimeout(ctx, s.settings.UtxoStore.DBTimeout)
	// defer cancelTimeout()

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
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
			return nil, errors.NewTxExistsError("Transaction already exists in postgres store (coinbase=%v):", tx.IsCoinbase(), err)
		} else if sqliteErr, ok := err.(*sqlite.Error); ok && sqliteErr.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE {
			return nil, errors.NewTxExistsError("Transaction already exists in sqlite store (coinbase=%v):", tx.IsCoinbase(), sqliteErr)
		}

		return nil, errors.NewStorageError("Failed to insert transaction", err)
	}

	// Insert the inputs...
	q = `
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
		_, err = txn.ExecContext(
			ctx,
			q,
			transactionID,
			i,
			input.PreviousTxIDChainHash()[:],
			input.PreviousTxOutIndex,
			input.PreviousTxSatoshis,
			input.PreviousTxScript,
			input.UnlockingScript,
			input.SequenceNumber,
		)
		if err != nil {
			if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
				return nil, errors.NewTxExistsError("Transaction already exists in postgres store (coinbase=%v): %v", tx.IsCoinbase(), err)
			} else if sqliteErr, ok := err.(*sqlite.Error); ok && sqliteErr.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE {
				return nil, errors.NewTxExistsError("Transaction already exists in sqlite store (coinbase=%v): %v", tx.IsCoinbase(), sqliteErr)
			}

			return nil, errors.NewStorageError("Failed to insert input", err)
		}
	}

	// Insert the outputs...
	q = `
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
		if output != nil {
			iUint32, err := safeconversion.IntToUint32(i)
			if err != nil {
				return nil, err
			}

			utxoHash, err := util.UTXOHashFromOutput(txHash, output, iUint32)
			if err != nil {
				return nil, err
			}

			_, err = txn.ExecContext(
				ctx,
				q,
				transactionID,
				i,
				output.LockingScript,
				output.Satoshis,
				coinbaseSpendingHeight,
				utxoHash[:],
				nil,
			)
			if err != nil {
				if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
					return nil, errors.NewTxExistsError("Transaction already exists in postgres store (coinbase=%v): %v", tx.IsCoinbase(), err)
				} else if sqliteErr, ok := err.(*sqlite.Error); ok && sqliteErr.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE {
					return nil, errors.NewTxExistsError("Transaction already exists in sqlite store (coinbase=%v): %v", tx.IsCoinbase(), sqliteErr)
				}

				return nil, errors.NewStorageError("Failed to insert output", err)
			}
		}
	}

	if len(options.MinedBlockInfos) > 0 {
		// Insert the block_ids...
		q = `
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

		for _, blockMeta := range options.MinedBlockInfos {
			_, err = txn.ExecContext(ctx, q, transactionID, blockMeta.BlockID, blockMeta.BlockHeight, blockMeta.SubtreeIdx)
			if err != nil {
				if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
					return nil, errors.NewTxExistsError("Transaction already exists in postgres store (coinbase=%v): %v", tx.IsCoinbase(), err)
				} else if sqliteErr, ok := err.(*sqlite.Error); ok && sqliteErr.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE {
					return nil, errors.NewTxExistsError("Transaction already exists in sqlite store (coinbase=%v): %v", tx.IsCoinbase(), sqliteErr)
				}

				return nil, errors.NewStorageError("Failed to insert block_ids", err)
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
	result, err := s.get(ctx, hash, utxo.MetaFields)
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

	ctx, cancelTimeout := context.WithTimeout(ctx, s.settings.UtxoStore.DBTimeout)
	defer cancelTimeout()

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
	)

	err := s.db.QueryRowContext(ctx, q, hash[:]).Scan(&id, &version, &lockTime, &data.Fee, &data.SizeInBytes, &data.IsCoinbase, &data.Frozen, &data.Conflicting, &data.Locked, &unminedSince)
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

			if err := rows.Scan(&previousTxHashBytes, &input.PreviousTxOutIndex, &input.PreviousTxSatoshis, &input.PreviousTxScript, &input.UnlockingScript, &input.SequenceNumber); err != nil {
				return nil, err
			}

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
func (s *Store) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	if blockHeight == 0 {
		return nil, errors.NewProcessingError("blockHeight must be greater than zero")
	}

	ctx, cancelTimeout := context.WithTimeout(ctx, s.settings.UtxoStore.DBTimeout)
	defer cancelTimeout()

	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			prometheusUtxoErrors.WithLabelValues("Spend", "Failed Spend Cleaning").Inc()
			s.logger.Errorf("ERROR panic in sql Spend: %v", recoverErr)
		}
	}()

	// try the operation with retry logic for lock errors
	var spends []*utxo.Spend
	var err error

	for attempt := 0; attempt <= 3; attempt++ {
		spends, err = s.spendWithRetry(ctx, tx, blockHeight, ignoreFlags...)

		// if no error or not a lock error, return immediately
		if err == nil || !isLockError(err) {
			return spends, err
		}

		// for lock errors, retry with backoff
		if attempt < 3 {
			backoff := time.Duration(100<<attempt) * time.Millisecond // 100ms, 200ms, 400ms
			s.logger.Warnf("Database lock error during spend (attempt %d): %v, retrying in %v", attempt+1, err, backoff)

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
				// continue to next attempt
			}
		}
	}

	return spends, err
}

func (s *Store) spendWithRetry(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	spends, err := utxo.GetSpends(tx)
	if err != nil {
		return nil, err
	}

	if len(spends) == 0 {
		return nil, errors.NewProcessingError("No spends provided", nil)
	}

	txn, err := s.db.Begin()
	if err != nil {
		return nil, err
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

	if s.engine == "postgres" {
		// use FOR UPDATE without SKIP LOCKED to ensure we wait for locked rows
		// rather than skipping them, which can cause false "not found" errors
		q1 += ` FOR UPDATE`
	}

	q2 := `
		UPDATE outputs
		SET spending_data = $1
		WHERE transaction_id = $2
		AND idx = $3
	`

	var errorFound bool

	useIgnoreConflicting := len(ignoreFlags) > 0 && ignoreFlags[0].IgnoreConflicting
	useIgnoreLocked := len(ignoreFlags) > 0 && ignoreFlags[0].IgnoreLocked

	for _, spend := range spends {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		default:
			if spend == nil {
				continue
			}

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

			err = txn.QueryRowContext(ctx, q1, spend.TxID[:], spend.Vout).Scan(&transactionID, &coinbaseSpendingHeight, &utxoHash, &spendingDataBytes, &frozen, &conflicting, &locked, &spendableIn)
			if err != nil {
				errorFound = true

				if errors.Is(err, sql.ErrNoRows) {
					// with SKIP LOCKED, this could mean the row is locked by another transaction or it genuinely doesn't exist
					if s.engine == "postgres" {
						spend.Err = errors.NewStorageError("output %s:%d not found or locked by another transaction", spend.TxID, spend.Vout)
					} else {
						spend.Err = errors.NewNotFoundError("output %s:%d not found", spend.TxID, spend.Vout)
					}
				} else {
					spend.Err = errors.NewStorageError("[Spend] failed: SELECT output FOR UPDATE %s:%d - %v", spend.TxID, spend.Vout, err)
				}

				continue
			}

			// If the utxo is frozen, it cannot be spent
			if frozen {
				errorFound = true
				spend.Err = errors.NewUtxoFrozenError("[Spend] utxo is frozen for %s:%d", spend.TxID, spend.Vout)

				continue
			}

			// If the tx is marked as conflicting, it cannot be spent
			if conflicting && !useIgnoreConflicting {
				errorFound = true
				spend.Err = errors.NewTxConflictingError("[Spend] tx is conflicting for %s:%d", spend.TxID, spend.Vout)

				continue
			}

			if locked && !useIgnoreLocked {
				errorFound = true
				spend.Err = errors.NewTxLockedError("[Spend] utxo is not spendable for %s:%d", spend.TxID, spend.Vout)

				continue
			}

			if spendableIn != nil {
				if *spendableIn > 0 && blockHeight < *spendableIn {
					errorFound = true
					spend.Err = errors.NewTxLockedError("[Spend] utxo %s:%d is not spendable until %d", spend.TxID, spend.Vout, *spendableIn)

					continue
				}
			}

			// Check if the utxo is already spent
			if len(spendingDataBytes) > 0 {
				if spend.SpendingData != nil && !bytes.Equal(spendingDataBytes, spend.SpendingData.Bytes()) {
					spendingData, err := spendpkg.NewSpendingDataFromBytes(spendingDataBytes)
					if err != nil {
						errorFound = true
						spend.Err = errors.NewProcessingError("failed to create spending data from bytes", err)

						continue
					}

					errorFound = true
					spend.Err = errors.NewUtxoSpentError(*spend.TxID, spend.Vout, *spend.UTXOHash, spend.SpendingData)
					spend.ConflictingTxID = spendingData.TxID

					continue
				}
			}

			// Check the utxo hash is correct
			if !bytes.Equal(utxoHash, spend.UTXOHash[:]) {
				errorFound = true
				spend.Err = errors.NewStorageError("[Spend] utxo hash mismatch for %s:%d", spend.TxID, spend.Vout)

				continue
			}

			// If this utxo has a coinbase spending height, check it is time to spend it
			if coinbaseSpendingHeight > 0 && coinbaseSpendingHeight > blockHeight {
				errorFound = true
				spend.Err = errors.NewStorageError("[Spend] coinbase utxo not ready to spend for %s:%d", spend.TxID, spend.Vout)

				continue
			}

			result, err := txn.ExecContext(ctx, q2, spend.SpendingData.Bytes(), transactionID, spend.Vout)
			if err != nil {
				errorFound = true
				spend.Err = errors.NewStorageError("[Spend] failed: UPDATE outputs: error spending utxo for %s:%d", spend.TxID, spend.Vout, err)

				continue
			}

			affected, err := result.RowsAffected()
			if err != nil {
				errorFound = true
				spend.Err = errors.NewStorageError("[Spend] failed getting affected rows: utxo not spent for %s:%d", spend.TxID, spend.Vout, err)

				continue
			}

			if affected == 0 {
				errorFound = true
				spend.Err = errors.NewStorageError("[Spend] utxo not spent for %s:%d", spend.TxID, spend.Vout)

				continue
			}

			if err = s.setDAH(ctx, txn, transactionID); err != nil {
				errorFound = true
				spend.Err = err
			}

			prometheusUtxoSpend.Inc()
		}
	}

	if errorFound {
		return spends, errors.NewUtxoError("One or more UTXOs could not be spent")
	} else {
		if err = txn.Commit(); err != nil {
			return nil, errors.NewStorageError("[Spend] failed to commit transaction", err)
		}
	}

	return spends, nil
}

func (s *Store) setDAH(ctx context.Context, txn *sql.Tx, transactionID int) error {
	if s.settings.GetUtxoStoreBlockHeightRetention() == 0 {
		return nil
	}

	// check whether the transaction has any unspent outputs
	qUnspent := `
		SELECT count(o.idx), t.conflicting
		FROM transactions t
		LEFT JOIN outputs o ON t.id = o.transaction_id
		   AND o.spending_data IS NULL
		WHERE t.id = $1
		GROUP BY t.id
	`

	var (
		unspent              int
		conflicting          bool
		deleteAtHeightOrNull sql.NullInt64
	)

	if err := txn.QueryRowContext(ctx, qUnspent, transactionID).Scan(&unspent, &conflicting); err != nil {
		return errors.NewStorageError("[setDAH] error checking for unspent outputs for %d", transactionID, err)
	}

	if unspent == 0 || conflicting {
		// Transaction is fully spent or conflicting
		// Set DAH at normal retention - cleanup service will verify child stability (288 blocks)
		// before actually deleting, providing the real safety guarantee
		conservativeRetention := s.settings.GetUtxoStoreBlockHeightRetention()
		_ = deleteAtHeightOrNull.Scan(int64(s.blockHeight.Load() + conservativeRetention))

		// Note: We do NOT track spending children separately
		// They are derived from outputs.spending_data when needed by cleanup
		// This ensures we verify ALL children (not just one) before parent deletion
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

// Unspend reverses a previous spend operation, marking UTXOs as unspent.
// This removes the spending transaction ID and any expiration timestamp.
// Commonly used during blockchain reorganizations.
func (s *Store) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	ctx, cancelTimeout := context.WithTimeout(ctx, s.settings.UtxoStore.DBTimeout)
	defer cancelTimeout()

	txn, err := s.db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		_ = txn.Rollback()
	}()

	q1 := `
		UPDATE outputs
		SET spending_data = NULL
		WHERE transaction_id IN (
			SELECT id FROM transactions WHERE hash = $1
		)
		AND idx = $2
		RETURNING transaction_id
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

			var transactionID int

			err = txn.QueryRowContext(ctx, q1, spend.TxID[:], spend.Vout).Scan(&transactionID)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return errors.NewNotFoundError("output %s:%d not found", spend.TxID, spend.Vout)
				}

				return err
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
	ctx, cancelTimeout := context.WithTimeout(ctx, s.settings.UtxoStore.DBTimeout)
	defer cancelTimeout()

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
	// Check if we're using PostgreSQL or SQLite
	isPostgres := s.storeURL.Scheme == "postgres"

	// For SQLite or small batches, fall back to the original implementation
	// SQLite doesn't support array operations, and small batches don't benefit from bulk operations
	if !isPostgres || len(hashes) < 10 {
		// Add timeout context for the original implementation
		ctxWithTimeout, cancelTimeout := context.WithTimeout(ctx, s.settings.UtxoStore.DBTimeout)
		defer cancelTimeout()
		return s.setMinedMultiOriginal(ctxWithTimeout, hashes, minedBlockInfo)
	}

	// For very large batches, split into smaller chunks to avoid long-running transactions
	const maxBatchSize = 500
	if len(hashes) > maxBatchSize {
		return s.setMinedMultiBatched(ctx, hashes, minedBlockInfo, maxBatchSize)
	}

	// Add timeout context and call the bulk implementation
	ctxWithTimeout, cancelTimeout := context.WithTimeout(ctx, s.settings.UtxoStore.DBTimeout)
	defer cancelTimeout()
	return s.setMinedMultiBulk(ctxWithTimeout, hashes, minedBlockInfo)
}

// setMinedMultiBatched handles very large batches by splitting them into smaller chunks
// This avoids long-running transactions that can cause timeouts and deadlocks
func (s *Store) setMinedMultiBatched(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo, batchSize int) (map[chainhash.Hash][]uint32, error) {
	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	resultMap := make(map[chainhash.Hash][]uint32)

	// Process hashes in batches
	for i := 0; i < len(hashes); i += batchSize {
		end := i + batchSize
		if end > len(hashes) {
			end = len(hashes)
		}

		batch := hashes[i:end]

		// Check context before processing each batch
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Process this batch with a timeout
		ctxWithTimeout, cancelTimeout := context.WithTimeout(ctx, s.settings.UtxoStore.DBTimeout)
		batchResult, err := s.setMinedMultiBulk(ctxWithTimeout, batch, minedBlockInfo)
		cancelTimeout()

		if err != nil {
			return nil, errors.NewStorageError("SQL error in batched operation (batch %d-%d): %v", i, end-1, err)
		}

		// Merge results
		for hash, blockIDs := range batchResult {
			resultMap[hash] = blockIDs
		}
	}

	return resultMap, nil
}

// setMinedMultiBulk is the core bulk implementation for medium-sized batches
func (s *Store) setMinedMultiBulk(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	// Check if context is already cancelled before starting
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Start a database transaction
	txn, err := s.db.Begin()
	if err != nil {
		return nil, err
	}

	// Use a flag to track if we should rollback
	committed := false
	defer func() {
		if !committed {
			if rollbackErr := txn.Rollback(); rollbackErr != nil {
				// Log rollback error but don't override original error
				s.logger.Warnf("Failed to rollback bulk transaction: %v", rollbackErr)
			}
		}
	}()

	// Convert hashes to byte arrays for PostgreSQL
	hashBytes := make([][]byte, len(hashes))
	for i, hash := range hashes {
		hashBytes[i] = hash[:]
	}

	// Step 1: Bulk check which transactions exist
	// Using ANY array operator for bulk comparison
	qCheckExists := `
		SELECT hash
		FROM transactions
		WHERE hash = ANY($1::bytea[])
	`

	// Check context before first database operation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	rows, err := txn.QueryContext(ctx, qCheckExists, pq.Array(hashBytes))
	if err != nil {
		return nil, errors.NewStorageError("SQL error checking transaction existence: %v", err)
	}

	existingHashes := make(map[chainhash.Hash]bool)
	for rows.Next() {
		var hashBytes []byte
		if err := rows.Scan(&hashBytes); err != nil {
			rows.Close()
			return nil, errors.NewStorageError("SQL error scanning existing hash: %v", err)
		}
		var hash chainhash.Hash
		copy(hash[:], hashBytes)
		existingHashes[hash] = true
	}
	rows.Close()

	// If no transactions exist, return early
	if len(existingHashes) == 0 {
		// Commit empty transaction
		if err = txn.Commit(); err != nil {
			return nil, errors.NewStorageError("SQL error committing empty transaction: %v", err)
		}
		committed = true
		return make(map[chainhash.Hash][]uint32), nil
	}

	// Prepare array of existing hashes only
	existingHashBytes := make([][]byte, 0, len(existingHashes))
	for hash := range existingHashes {
		h := hash // Create a copy to avoid pointer issues
		existingHashBytes = append(existingHashBytes, h[:])
	}

	// Check context before block_ids operations
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if minedBlockInfo.UnsetMined {
		// Step 2a: Bulk delete block_ids for unsetting mined status
		qBulkRemove := `
			DELETE FROM block_ids
			WHERE transaction_id IN (
				SELECT id FROM transactions WHERE hash = ANY($1::bytea[])
			)
			AND block_id = $2
		`
		if _, err = txn.ExecContext(ctx, qBulkRemove, pq.Array(existingHashBytes), minedBlockInfo.BlockID); err != nil {
			return nil, errors.NewStorageError("SQL error bulk removing block_ids: %v", err)
		}
	} else {
		// Step 2b: Bulk insert block_ids for setting mined status
		qBulkInsert := `
			INSERT INTO block_ids (transaction_id, block_id, block_height, subtree_idx)
			SELECT t.id, $2, $3, $4
			FROM transactions t
			WHERE t.hash = ANY($1::bytea[])
			ON CONFLICT DO NOTHING
		`
		if _, err = txn.ExecContext(ctx, qBulkInsert, pq.Array(existingHashBytes), minedBlockInfo.BlockID, minedBlockInfo.BlockHeight, minedBlockInfo.SubtreeIdx); err != nil {
			return nil, errors.NewStorageError("SQL error bulk inserting block_ids: %v", err)
		}
	}

	// Check context before transaction updates
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Step 3: Bulk update transactions to mark as mined
	qBulkUpdateLongestChain := `
		UPDATE transactions
		SET locked = false, unmined_since = NULL
		WHERE hash = ANY($1::bytea[])
	`

	qBulkUpdateNotLongestChain := `
		UPDATE transactions
		SET locked = false
		WHERE hash = ANY($1::bytea[])
	`

	if minedBlockInfo.OnLongestChain {
		if _, err = txn.ExecContext(ctx, qBulkUpdateLongestChain, pq.Array(existingHashBytes)); err != nil {
			return nil, errors.NewStorageError("SQL error bulk updating transactions: %v", err)
		}
	} else {
		if _, err = txn.ExecContext(ctx, qBulkUpdateNotLongestChain, pq.Array(existingHashBytes)); err != nil {
			return nil, errors.NewStorageError("SQL error bulk updating transactions: %v", err)
		}
	}

	// Check context before final fetch operation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Step 4: Bulk fetch all block IDs for the transactions
	qBulkGetBlockIDs := `
		SELECT t.hash, array_agg(b.block_id ORDER BY b.block_id)
		FROM transactions t
		LEFT JOIN block_ids b ON t.id = b.transaction_id
		WHERE t.hash = ANY($1::bytea[])
		GROUP BY t.hash
	`

	rows, err = txn.QueryContext(ctx, qBulkGetBlockIDs, pq.Array(existingHashBytes))
	if err != nil {
		return nil, errors.NewStorageError("SQL error bulk fetching block IDs: %v", err)
	}

	blockIDsMap := make(map[chainhash.Hash][]uint32)
	for rows.Next() {
		var hashBytes []byte
		var blockIDs pq.Int32Array
		if err := rows.Scan(&hashBytes, &blockIDs); err != nil {
			rows.Close()
			return nil, errors.NewStorageError("SQL error scanning block IDs: %v", err)
		}
		var hash chainhash.Hash
		copy(hash[:], hashBytes)

		// Convert pq.Int32Array to []uint32, filtering out NULLs (converted to 0)
		var uint32BlockIDs []uint32
		for _, id := range blockIDs {
			if id > 0 { // Skip NULL values which become 0
				uint32BlockIDs = append(uint32BlockIDs, uint32(id))
			}
		}
		blockIDsMap[hash] = uint32BlockIDs
	}
	rows.Close()

	// Commit the transaction
	if err = txn.Commit(); err != nil {
		return nil, errors.NewStorageError("SQL error committing transaction: %v", err)
	}
	committed = true

	return blockIDsMap, nil
}

// setMinedMultiOriginal is the original implementation that works with both SQLite and PostgreSQL
// but uses individual queries for each transaction (slower for large batches)
func (s *Store) setMinedMultiOriginal(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	// Start a database transaction
	txn, err := s.db.Begin()
	if err != nil {
		return nil, err
	}

	// Use a flag to track if we should rollback
	committed := false
	defer func() {
		if !committed {
			if rollbackErr := txn.Rollback(); rollbackErr != nil {
				// Log rollback error but don't override original error
				s.logger.Warnf("Failed to rollback original transaction: %v", rollbackErr)
			}
		}
	}()

	// Update the block_ids
	q := `
		INSERT INTO block_ids (
			transaction_id,
			block_id,
			block_height,
			subtree_idx
		) VALUES (
			(SELECT id FROM transactions WHERE hash = $1),
			$2,
			$3,
			$4
		)
		ON CONFLICT DO NOTHING;
    `

	// remove if minedBlockInfo.SetMined is false
	qRemove := `
		DELETE FROM block_ids
		WHERE transaction_id = (SELECT id FROM transactions WHERE hash = $1)
		AND block_id = $2;
	`

	qLongestChain := `
		UPDATE transactions SET
		 locked = false
		,unmined_since = NULL
		WHERE hash = $1;
	`

	qNotOnLongestChain := `
		UPDATE transactions SET
		 locked = false
		WHERE hash = $1;
	`

	qGet := `
		SELECT block_id FROM block_ids
		WHERE transaction_id = (SELECT id FROM transactions WHERE hash = $1)
	`

	blockIDsMap := make(map[chainhash.Hash][]uint32)

	for _, hash := range hashes {
		// First check if the transaction exists
		var txExists bool
		err := txn.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM transactions WHERE hash = $1)", hash[:]).Scan(&txExists)
		if err != nil {
			return nil, errors.NewStorageError("SQL error checking transaction existence for %s: %v", hash.String(), err)
		}

		// Skip if transaction doesn't exist yet (it might be processed later in the same block)
		if !txExists {
			continue
		}

		if minedBlockInfo.UnsetMined {
			// remove the block ID from the transaction
			if _, err = txn.ExecContext(ctx, qRemove, hash[:], minedBlockInfo.BlockID); err != nil {
				return nil, errors.NewStorageError("SQL error calling SetMinedMulti on tx %s:%v", hash.String(), err)
			}
		} else {
			if _, err = txn.ExecContext(ctx, q, hash[:], minedBlockInfo.BlockID, minedBlockInfo.BlockHeight, minedBlockInfo.SubtreeIdx); err != nil {
				return nil, errors.NewStorageError("SQL error calling SetMinedMulti on tx %s:%v", hash.String(), err)
			}
		}

		// get the current block IDs for the transaction
		rows, err := txn.QueryContext(ctx, qGet, hash[:])
		if err != nil {
			return nil, errors.NewStorageError("SQL error calling get block IDs on tx %s:%v", hash.String(), err)
		}

		var blockIDs []uint32
		for rows.Next() {
			var blockID uint32
			if err := rows.Scan(&blockID); err != nil {
				rows.Close()

				return nil, errors.NewStorageError("SQL error scanning block ID on tx %s:%v", hash.String(), err)
			}
			blockIDs = append(blockIDs, blockID)
		}
		rows.Close()

		blockIDsMap[*hash] = blockIDs
	}

	for _, hash := range hashes {
		// Only update transactions that exist in blockIDsMap (which means they exist in the database)
		if _, exists := blockIDsMap[*hash]; exists {
			if minedBlockInfo.OnLongestChain {
				if _, err = txn.ExecContext(ctx, qLongestChain, hash[:]); err != nil {
					return nil, errors.NewStorageError("SQL error calling update longest chain on tx %s:%v", hash.String(), err)
				}
			} else {
				if _, err = txn.ExecContext(ctx, qNotOnLongestChain, hash[:]); err != nil {
					return nil, errors.NewStorageError("SQL error calling update locked on tx %s:%v", hash.String(), err)
				}
			}
		}
	}

	// Commit the transaction
	if err = txn.Commit(); err != nil {
		return nil, errors.NewStorageError("SQL error committing original transaction: %v", err)
	}
	committed = true

	return blockIDsMap, nil
}

func (s *Store) GetSpend(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	ctx, cancelTimeout := context.WithTimeout(ctx, s.settings.UtxoStore.DBTimeout)
	defer cancelTimeout()

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
			return nil, errors.NewNotFoundError("utxo not found for %s:%d", spend.TxID, spend.Vout)
		}

		return nil, err
	}

	// check utxoHash is the same as expected
	if !bytes.Equal(utxoHash, spend.UTXOHash[:]) {
		return nil, errors.NewStorageError("utxo hash mismatch for %s:%d", spend.TxID, spend.Vout)
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

// BatchDecorate efficiently fetches metadata for multiple transactions.
// This is used to optimize bulk operations on transactions.
func (s *Store) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*utxo.UnresolvedMetaData, fields ...fields.FieldName) error {
	ctx, cancelTimeout := context.WithTimeout(ctx, s.settings.UtxoStore.DBTimeout)
	defer cancelTimeout()

	for _, unresolvedMetaData := range unresolvedMetaDataSlice {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			if unresolvedMetaData == nil {
				continue
			}

			data, err := s.Get(ctx, &unresolvedMetaData.Hash, fields...)
			if err != nil {
				unresolvedMetaData.Err = err
			} else {
				unresolvedMetaData.Data = data
			}
		}
	}

	return nil
}

// PreviousOutputsDecorate fetches output information for transaction inputs.
func (s *Store) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {
	ctx, cancelTimeout := context.WithTimeout(ctx, s.settings.UtxoStore.DBTimeout)
	defer cancelTimeout()

	q := `
		SELECT
		 o.locking_script
		,o.satoshis
		FROM outputs o
		JOIN transactions t ON o.transaction_id = t.id
		WHERE t.hash = $1
		AND o.idx = $2
	`

	var missingInputs []int
	for i, input := range tx.Inputs {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			if input == nil {
				continue
			}

			// Skip if already decorated
			if input.PreviousTxScript != nil {
				continue
			}

			err := s.db.QueryRowContext(ctx, q, input.PreviousTxIDChainHash()[:], input.PreviousTxOutIndex).Scan(&input.PreviousTxScript, &input.PreviousTxSatoshis)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					// Track missing inputs - they might be in the same block being processed
					missingInputs = append(missingInputs, i)
					continue
				}
				return err
			}
		}
	}

	// If we have missing inputs, return an error indicating they couldn't be found
	// The caller (ExtendTransaction) will handle this by trying alternative methods
	if len(missingInputs) > 0 {
		return errors.NewProcessingError("failed to decorate previous outputs for tx %s", tx.TxIDChainHash())
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
		if err := deleteAtHeight.Scan(int64(s.blockHeight.Load() + s.settings.GetUtxoStoreBlockHeightRetention())); err != nil {
			return nil, nil, err
		}
	}

	qUpdate := `
			UPDATE transactions SET
			 conflicting = $2
			,delete_at_height = $3
			WHERE hash = $1
			RETURNING id
		`

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

func (s *Store) SetLocked(ctx context.Context, txHashes []chainhash.Hash, setValue bool) error {
	q := `
			UPDATE transactions
			SET locked = $2
			WHERE hash = $1
		`

	for _, conflictingTxHash := range txHashes {
		_, err := s.db.ExecContext(ctx, q, conflictingTxHash[:], setValue)
		if err != nil {
			return errors.NewStorageError("failed to set locked flag for %s", conflictingTxHash, err)
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
	var q string
	allErrors := make([]error, 0, 10) // Pre-allocate capacity for up to 10 errors
	missingTxErrors := make([]error, 0, 10)
	attempted := len(txHashes)
	errorCount := 0

	if onLongestChain {
		// Transaction is on longest chain - unset unminedSince field (set to NULL)
		q = `
			UPDATE transactions
			SET unmined_since = NULL
			WHERE hash = $1
		`

		for _, txHash := range txHashes {
			result, err := s.db.ExecContext(ctx, q, txHash[:])
			if err != nil {
				errorCount++
				// Only log and collect first 10 errors to avoid spam (could be millions of transactions)
				if len(allErrors) < 10 {
					s.logger.Errorf("[MarkTransactionsOnLongestChain] error %d: transaction %s: %v", errorCount, txHash, err)
					allErrors = append(allErrors, errors.NewStorageError("failed to mark transaction %s as on longest chain", txHash, err))
				}
				// Continue processing remaining transactions
			} else {
				// Check if transaction was actually found and updated
				rowsAffected, _ := result.RowsAffected()
				if rowsAffected == 0 {
					errorCount++
					// Transaction not found - this is FATAL (data corruption)
					if len(missingTxErrors) < 10 {
						missingTxErrors = append(missingTxErrors, errors.NewStorageError("MISSING transaction %s", txHash))
					}
				}
			}
		}
	} else {
		// Transaction is not on longest chain - set unminedSince to current block height
		currentBlockHeight := s.GetBlockHeight()

		q = `
			UPDATE transactions
			SET unmined_since = $2
			WHERE hash = $1
		`

		for _, txHash := range txHashes {
			result, err := s.db.ExecContext(ctx, q, txHash[:], currentBlockHeight)
			if err != nil {
				errorCount++
				// Only log and collect first 10 errors to avoid spam (could be millions of transactions)
				if len(allErrors) < 10 {
					s.logger.Errorf("[MarkTransactionsOnLongestChain] error %d: transaction %s: %v", errorCount, txHash, err)
					allErrors = append(allErrors, errors.NewStorageError("failed to mark transaction %s as not on longest chain", txHash, err))
				}
				// Continue processing remaining transactions
			} else {
				// Check if transaction was actually found and updated
				rowsAffected, _ := result.RowsAffected()
				if rowsAffected == 0 {
					errorCount++
					// Transaction not found - this is FATAL (data corruption)
					if len(missingTxErrors) < 10 {
						missingTxErrors = append(missingTxErrors, errors.NewStorageError("MISSING transaction %s", txHash))
					}
				}
			}
		}
	}

	// Log summary
	succeeded := attempted - errorCount
	s.logger.Infof("[MarkTransactionsOnLongestChain] completed: attempted=%d, succeeded=%d, failed=%d, onLongestChain=%t",
		attempted, succeeded, errorCount, onLongestChain)

	// FATAL if we have missing transactions - this indicates data corruption
	if len(missingTxErrors) > 0 {
		s.logger.Fatalf("CRITICAL: %d missing transactions during MarkTransactionsOnLongestChain - data integrity compromised. First errors: %v",
			len(missingTxErrors), errors.Join(missingTxErrors...))
	}

	// Return aggregated errors (up to 10) for other error types
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

func createPostgresSchema(db *usql.DB) error {
	return createPostgresSchemaImpl(db)
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

	// Drop and recreate the foreign key constraint for inputs table if it exists
	if _, err := db.Exec(`
		DO $$
		BEGIN
			IF EXISTS (
				SELECT 1 FROM information_schema.table_constraints
				WHERE table_name = 'inputs'
				AND constraint_type = 'FOREIGN KEY'
				AND constraint_name = 'inputs_transaction_id_fkey'
			) THEN
				ALTER TABLE inputs DROP CONSTRAINT inputs_transaction_id_fkey;
			END IF;
		END $$;
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not drop existing foreign key constraint on inputs table - [%+v]", err)
	}

	// Add the new foreign key constraint with ON DELETE CASCADE for inputs
	if _, err := db.Exec(`
		DO $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM information_schema.table_constraints
				WHERE table_name = 'inputs'
				AND constraint_type = 'FOREIGN KEY'
				AND constraint_name = 'inputs_transaction_id_fkey'
			) THEN
				ALTER TABLE inputs
				ADD CONSTRAINT inputs_transaction_id_fkey
				FOREIGN KEY (transaction_id)
				REFERENCES transactions(id) ON DELETE CASCADE;
			END IF;
		END $$;
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not add new foreign key constraint with CASCADE on inputs table - [%+v]", err)
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

	// Drop and recreate the foreign key constraint for outputs table if it exists
	if _, err := db.Exec(`
		DO $$
		BEGIN
			IF EXISTS (
				SELECT 1 FROM information_schema.table_constraints
				WHERE table_name = 'outputs'
				AND constraint_type = 'FOREIGN KEY'
				AND constraint_name = 'outputs_transaction_id_fkey'
			) THEN
				ALTER TABLE outputs DROP CONSTRAINT outputs_transaction_id_fkey;
			END IF;
		END $$;
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not drop existing foreign key constraint on outputs table - [%+v]", err)
	}

	// Add the new foreign key constraint with ON DELETE CASCADE for outputs
	if _, err := db.Exec(`
		DO $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM information_schema.table_constraints
				WHERE table_name = 'outputs'
				AND constraint_type = 'FOREIGN KEY'
				AND constraint_name = 'outputs_transaction_id_fkey'
			) THEN
				ALTER TABLE outputs
				ADD CONSTRAINT outputs_transaction_id_fkey
				FOREIGN KEY (transaction_id)
				REFERENCES transactions(id) ON DELETE CASCADE;
			END IF;
		END $$;
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not add new foreign key constraint with CASCADE on outputs table - [%+v]", err)
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
			IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'block_ids' AND column_name = 'block_height') THEN
				ALTER TABLE block_ids ADD COLUMN block_height BIGINT NOT NULL;
			END IF;

			IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'block_ids' AND column_name = 'subtree_idx') THEN
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
			IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'transactions' AND column_name = 'unmined_since') THEN
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
			IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'transactions' AND column_name = 'preserve_until') THEN
				ALTER TABLE transactions ADD COLUMN preserve_until BIGINT;
			END IF;
		END $$;
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not add preserve_until column to transactions table - [%+v]", err)
	}

	// Drop the existing foreign key constraint if it exists
	if _, err := db.Exec(`
		DO $$
		BEGIN
			IF EXISTS (
				SELECT 1 FROM information_schema.table_constraints
				WHERE table_name = 'block_ids'
				AND constraint_type = 'FOREIGN KEY'
			) THEN
				ALTER TABLE block_ids DROP CONSTRAINT block_ids_transaction_id_fkey;
			END IF;
		END $$;
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not drop existing foreign key constraint - [%+v]", err)
	}

	// Add the new foreign key constraint with ON DELETE CASCADE
	if _, err := db.Exec(`
		DO $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM information_schema.table_constraints
				WHERE table_name = 'block_ids'
				AND constraint_type = 'FOREIGN KEY'
				AND constraint_name = 'block_ids_transaction_id_fkey'
			) THEN
				ALTER TABLE block_ids
				ADD CONSTRAINT block_ids_transaction_id_fkey
				FOREIGN KEY (transaction_id)
				REFERENCES transactions(id) ON DELETE CASCADE;
			END IF;
		END $$;
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not add new foreign key constraint with CASCADE - [%+v]", err)
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
	ctx, cancelTimeout := context.WithTimeout(ctx, s.settings.UtxoStore.DBTimeout)
	defer cancelTimeout()

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
func (s *Store) PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	if len(txIDs) == 0 {
		return nil
	}

	// Build placeholders for IN clause
	placeholders := make([]string, 0, len(txIDs))
	args := make([]interface{}, 0, len(txIDs))

	for _, txID := range txIDs {
		placeholders = append(placeholders, "?")
		args = append(args, txID[:])
	}
	args = append([]interface{}{preserveUntilHeight}, args...)

	query := fmt.Sprintf(`
    UPDATE transactions
    SET preserve_until = ?, delete_at_height = NULL
    WHERE hash IN (%s)
	`, strings.Join(placeholders, ","))

	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return errors.NewStorageError("failed to preserve transactions", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		s.logger.Warnf("[PreserveTransactions] Could not get rows affected: %v", err)
	} else {
		s.logger.Debugf("[PreserveTransactions] Successfully preserved %d out of %d transactions", rowsAffected, len(txIDs))
	}

	return nil
}

// ProcessExpiredPreservations handles transactions whose preservation period has expired.
// For each transaction with PreserveUntil <= currentHeight, it sets an appropriate DeleteAtHeight
// and clears the PreserveUntil field.
func (s *Store) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error {
	deleteAtHeight := currentHeight + s.settings.GetUtxoStoreBlockHeightRetention()

	query := `
		UPDATE transactions
		SET delete_at_height = ?, preserve_until = NULL
		WHERE preserve_until IS NOT NULL AND preserve_until <= ?
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

// isLockError checks if the error is a database lock/deadlock error
func isLockError(err error) bool {
	if err == nil {
		return false
	}

	// PostgreSQL deadlock/lock errors
	if pqErr, ok := err.(*pq.Error); ok {
		// 40001: serialization_failure
		// 40P01: deadlock_detected
		// 55P03: lock_not_available
		return pqErr.Code == "40001" || pqErr.Code == "40P01" || pqErr.Code == "55P03"
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
