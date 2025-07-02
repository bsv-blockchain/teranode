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

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/pkg/go-subtree"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	spendpkg "github.com/bitcoin-sv/teranode/stores/utxo/spend"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/bitcoin-sv/teranode/util/usql"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
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

	db, err := util.InitSQLDB(logger, storeURL, tSettings)
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

	txMeta, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, errors.NewProcessingError("failed to get tx meta data", err)
	}

	if options.Conflicting {
		txMeta.Conflicting = true
	}

	if options.Unspendable {
		txMeta.Unspendable = true
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
		,unspendable
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
		options.Unspendable,
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

func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	return s.get(ctx, hash, utxo.MetaFields)
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
		,unspendable
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

	err := s.db.QueryRowContext(ctx, q, hash[:]).Scan(&id, &version, &lockTime, &data.Fee, &data.SizeInBytes, &data.IsCoinbase, &data.Frozen, &data.Conflicting, &data.Unspendable, &unminedSince)
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
func (s *Store) Spend(ctx context.Context, tx *bt.Tx, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	ctx, cancelTimeout := context.WithTimeout(ctx, s.settings.UtxoStore.DBTimeout)
	defer cancelTimeout()

	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			prometheusUtxoErrors.WithLabelValues("Spend", "Failed Spend Cleaning").Inc()
			s.logger.Errorf("ERROR panic in sql Spend: %v", recoverErr)
		}
	}()

	blockHeight := s.GetBlockHeight()

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
		,t.unspendable
		,o.spendableIn
		FROM outputs o
		JOIN transactions t ON o.transaction_id = t.id
		WHERE t.hash = $1
		AND o.idx = $2
	`

	if s.engine == "postgres" {
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
	useIgnoreUnspendable := len(ignoreFlags) > 0 && ignoreFlags[0].IgnoreUnspendable

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
				unspendable            bool
				spendableIn            *uint32
			)

			err = txn.QueryRowContext(ctx, q1, spend.TxID[:], spend.Vout).Scan(&transactionID, &coinbaseSpendingHeight, &utxoHash, &spendingDataBytes, &frozen, &conflicting, &unspendable, &spendableIn)
			if err != nil {
				errorFound = true

				if errors.Is(err, sql.ErrNoRows) {
					spend.Err = errors.NewNotFoundError("output %s:%d not found", spend.TxID, spend.Vout)
				}

				spend.Err = errors.NewStorageError("[Spend] failed: SELECT output FOR UPDATE NOWAIT %s:%d", spend.TxID, spend.Vout, err)

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

			if unspendable && !useIgnoreUnspendable {
				errorFound = true
				spend.Err = errors.NewTxUnspendableError("[Spend] utxo is not spendable for %s:%d", spend.TxID, spend.Vout)

				continue
			}

			if spendableIn != nil {
				if *spendableIn > 0 && blockHeight < *spendableIn {
					errorFound = true
					spend.Err = errors.NewTxUnspendableError("[Spend] utxo %s:%d is not spendable until %d", spend.TxID, spend.Vout, *spendableIn)

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
			if coinbaseSpendingHeight > 0 && blockHeight < coinbaseSpendingHeight {
				errorFound = true
				spend.Err = errors.NewStorageError("[Spend]coinbase utxo not ready to spend for %s:%d", spend.TxID, spend.Vout)

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
		return spends, errors.NewTxInvalidError("One or more UTXOs could not be spent")
	} else {
		if err = txn.Commit(); err != nil {
			return nil, errors.NewStorageError("[Spend] failed to commit transaction", err)
		}
	}

	return spends, nil
}

func (s *Store) setDAH(ctx context.Context, txn *sql.Tx, transactionID int) error {
	// doing 2 updates is the only thing that works in both postgres and sqlite
	qSetDAH := `
		UPDATE transactions
		SET delete_at_height = $2
		WHERE id = $1
	`

	if s.settings.UtxoStore.BlockHeightRetention > 0 {
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
			// Now mark the transaction as tombstoned if there are no more unspent outputs
			_ = deleteAtHeightOrNull.Scan(int64(s.blockHeight.Load() + s.settings.UtxoStore.BlockHeightRetention))
		}

		if _, err := txn.ExecContext(ctx, qSetDAH, transactionID, deleteAtHeightOrNull); err != nil {
			return errors.NewStorageError("[setDAH] error setting DAH for %d", transactionID, err)
		}
	}

	return nil
}

// Unspend reverses a previous spend operation, marking UTXOs as unspent.
// This removes the spending transaction ID and any expiration timestamp.
// Commonly used during blockchain reorganizations.
func (s *Store) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsUnspendable ...bool) error {
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

	unspendable := false
	if len(flagAsUnspendable) > 0 {
		unspendable = flagAsUnspendable[0]
	}

	q2 := `
		UPDATE transactions
		SET
			unspendable = $2
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

			if _, err = txn.ExecContext(ctx, q2, transactionID, unspendable); err != nil {
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

func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) error {
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

	q2 := `
		UPDATE transactions SET
		 unspendable = false
		,unmined_since = NULL
		WHERE hash = $1;
	`

	for _, hash := range hashes {
		// TODO set all the values from minedBlockInfo
		if _, err = txn.ExecContext(ctx, q, hash[:], minedBlockInfo.BlockID, minedBlockInfo.BlockHeight, minedBlockInfo.SubtreeIdx); err != nil {
			return errors.NewStorageError("SQL error calling SetMinedMulti on tx %s:%v", hash.String(), err)
		}
	}

	for _, hash := range hashes {
		if _, err = txn.ExecContext(ctx, q2, hash[:]); err != nil {
			return errors.NewStorageError("SQL error calling update unspendable on tx %s:%v", hash.String(), err)
		}
	}

	// Commit the transaction
	if err = txn.Commit(); err != nil {
		return err
	}

	return nil
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
		,t.unspendable
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
		unspendable            bool
	)

	err := s.db.QueryRowContext(ctx, q, spend.TxID[:], spend.Vout).Scan(&utxoHash, &coinbaseSpendingHeight, &spendingDataBytes, &frozen, &spendableIn, &conflicting, &unspendable)
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

	if unspendable || (spendableIn != nil && s.GetBlockHeight() < *spendableIn) {
		utxoStatus = utxo.Status_UNSPENDABLE
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

	for _, input := range tx.Inputs {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			if input == nil {
				continue
			}

			err := s.db.QueryRowContext(ctx, q, input.PreviousTxIDChainHash()[:], input.PreviousTxOutIndex).Scan(&input.PreviousTxScript, &input.PreviousTxSatoshis)
			if err != nil {
				return err
			}
		}
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

	if s.settings.UtxoStore.BlockHeightRetention > 0 && setValue {
		if err := deleteAtHeight.Scan(int64(s.blockHeight.Load() + s.settings.UtxoStore.BlockHeightRetention)); err != nil {
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

func (s *Store) SetUnspendable(ctx context.Context, txHashes []chainhash.Hash, setValue bool) error {
	q := `
			UPDATE transactions
			SET unspendable = $2
			WHERE hash = $1
		`

	for _, conflictingTxHash := range txHashes {
		_, err := s.db.ExecContext(ctx, q, conflictingTxHash[:], setValue)
		if err != nil {
			return errors.NewStorageError("failed to set unspendable flag for %s", conflictingTxHash, err)
		}
	}

	return nil
}

func createPostgresSchema(db *usql.DB) error {
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
        ,unspendable      BOOLEAN DEFAULT FALSE NOT NULL
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
		ALTER TABLE inputs
		ADD CONSTRAINT inputs_transaction_id_fkey
		FOREIGN KEY (transaction_id)
		REFERENCES transactions(id) ON DELETE CASCADE;
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
		ALTER TABLE outputs
		ADD CONSTRAINT outputs_transaction_id_fkey
		FOREIGN KEY (transaction_id)
		REFERENCES transactions(id) ON DELETE CASCADE;
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
		ALTER TABLE block_ids
		ADD CONSTRAINT block_ids_transaction_id_fkey
		FOREIGN KEY (transaction_id)
		REFERENCES transactions(id) ON DELETE CASCADE;
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
        ,unspendable      BOOLEAN DEFAULT FALSE NOT NULL
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
	placeholders := make([]string, len(txIDs))
	args := make([]interface{}, len(txIDs)+1)

	for i, txID := range txIDs {
		placeholders[i] = "?"
		args[i] = txID[:]
	}

	args[len(txIDs)] = preserveUntilHeight

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
	deleteAtHeight := currentHeight + s.settings.UtxoStore.BlockHeightRetention

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
