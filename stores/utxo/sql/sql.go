package sql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/usql"
	pq "github.com/lib/pq"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

var (
	prometheusUtxoGet    prometheus.Counter
	prometheusUtxoSpend  prometheus.Counter
	prometheusUtxoReset  prometheus.Counter
	prometheusUtxoDelete prometheus.Counter
	prometheusUtxoErrors *prometheus.CounterVec
)

func init() {
	prometheusUtxoGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "sql_utxo_get",
			Help: "Number of utxo get calls done to sql",
		},
	)
	prometheusUtxoSpend = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "sql_utxo_spend",
			Help: "Number of utxo spend calls done to sql",
		},
	)
	prometheusUtxoReset = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "sql_utxo_reset",
			Help: "Number of utxo reset calls done to sql",
		},
	)
	prometheusUtxoDelete = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "sql_utxo_delete",
			Help: "Number of utxo delete calls done to sql",
		},
	)
	prometheusUtxoErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sql_utxo_errors",
			Help: "Number of utxo errors",
		},
		[]string{
			"function", //function raising the error
			"error",    // error returned
		},
	)
}

type Store struct {
	logger           ulogger.Logger
	db               *usql.DB
	engine           string
	blockHeight      atomic.Uint32
	dbTimeout        time.Duration
	expirationMillis uint64
}

func New(logger ulogger.Logger, storeUrl *url.URL) (*Store, error) {
	db, err := util.InitSQLDB(logger, storeUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to init sql db: %+v", err)
	}

	switch storeUrl.Scheme {
	case "postgres":
		if err = createPostgresSchema(db); err != nil {
			return nil, fmt.Errorf("failed to create postgres schema: %+v", err)
		}

	case "sqlite", "sqlitememory":
		if err = createSqliteSchema(db); err != nil {
			return nil, fmt.Errorf("failed to create sqlite schema: %+v", err)
		}

	default:
		return nil, fmt.Errorf("unknown database engine: %s", storeUrl.Scheme)
	}

	dbTimeoutMillis, _ := gocore.Config().GetInt("utxostore_dbTimeoutMillis", 5000)

	// Create a goroutine to remove transactions that are marked with a tombstone time
	go func() {
		for {
			time.Sleep(1 * time.Minute)

			if err := deleteTombstoned(db); err != nil {
				logger.Errorf("failed to delete tombstoned transactions: %v", err)
			}
		}
	}()

	s := &Store{
		logger:      logger,
		db:          db,
		engine:      storeUrl.Scheme,
		blockHeight: atomic.Uint32{},
		dbTimeout:   time.Duration(dbTimeoutMillis) * time.Millisecond,
	}

	expirationValue := storeUrl.Query().Get("expiration") // This is specified in seconds
	if expirationValue != "" {
		expiration64, err := strconv.ParseUint(expirationValue, 10, 64)
		if err != nil {
			logger.Fatalf("could not parse expiration %s: %v", expirationValue, err)
		}
		s.expirationMillis = expiration64 * 1000
	}

	return s, nil
}

func (s *Store) SetBlockHeight(blockHeight uint32) error {
	s.logger.Debugf("setting block height to %d", blockHeight)
	s.blockHeight.Store(blockHeight)
	return nil
}

func (s *Store) GetBlockHeight() (uint32, error) {
	return s.blockHeight.Load(), nil
}

func (s *Store) Health(ctx context.Context) (int, string, error) {
	details := fmt.Sprintf("SQL Engine is %s", s.engine)

	var num int
	err := s.db.QueryRowContext(ctx, "SELECT 1").Scan(&num)
	if err != nil {
		return -1, details, err
	}
	return 0, details, nil
}

func (s *Store) Create(ctx context.Context, tx *bt.Tx, blockIDs ...uint32) (*meta.Data, error) {
	start, stat, _ := util.StartStatFromContext(ctx, "Create")

	defer func() {
		stat.AddTime(start)
	}()

	ctx, cancelTimeout := context.WithTimeout(ctx, s.dbTimeout)
	defer cancelTimeout()

	txMeta, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "failed to get tx meta data", err)
	}

	// Insert the transaction row...
	q := `
		INSERT INTO transactions (
		 hash
		,version
		,lock_time
		,fee
		,size_in_bytes
	  ) VALUES (
		 $1
		,$2
		,$3
		,$4
		,$5
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

	var transactionId int

	err = txn.QueryRowContext(ctx, q, tx.TxIDChainHash()[:], tx.Version, tx.LockTime, txMeta.Fee, txMeta.SizeInBytes).Scan(&transactionId)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
			return nil, errors.New(errors.ERR_TX_ALREADY_EXISTS, "Transaction already exists in postgres store: %v", err)
		} else if sqliteErr, ok := err.(*sqlite.Error); ok && sqliteErr.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE {
			return nil, errors.New(errors.ERR_TX_ALREADY_EXISTS, "Transaction already exists in sqlite store: %v", sqliteErr)
		}
		return nil, fmt.Errorf("Failed to insert transaction: %v", err)
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
		_, err = txn.ExecContext(ctx, q, transactionId, i, input.PreviousTxIDChainHash()[:], input.PreviousTxOutIndex, input.PreviousTxSatoshis, input.PreviousTxScript, input.UnlockingScript, input.SequenceNumber)
		if err != nil {
			if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
				return nil, errors.New(errors.ERR_TX_ALREADY_EXISTS, "Transaction already exists in postgres store: %v", err)
			} else if sqliteErr, ok := err.(*sqlite.Error); ok && sqliteErr.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE {
				return nil, errors.New(errors.ERR_TX_ALREADY_EXISTS, "Transaction already exists in sqlite store: %v", sqliteErr)
			}
			return nil, fmt.Errorf("Failed to insert input: %v", err)
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
		,spending_transaction_id
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

	if tx.IsCoinbase() {
		currentHeight, err := s.GetBlockHeight()
		if err != nil {
			return nil, err
		}

		coinbaseSpendingHeight = currentHeight + 100
	}

	for i, output := range tx.Outputs {
		utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), output, uint32(i))
		if err != nil {
			return nil, err
		}

		_, err = txn.ExecContext(ctx, q, transactionId, i, output.LockingScript, output.Satoshis, coinbaseSpendingHeight, utxoHash[:], nil)
		if err != nil {
			if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
				return nil, errors.New(errors.ERR_TX_ALREADY_EXISTS, "Transaction already exists in postgres store: %v", err)
			} else if sqliteErr, ok := err.(*sqlite.Error); ok && sqliteErr.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE {
				return nil, errors.New(errors.ERR_TX_ALREADY_EXISTS, "Transaction already exists in sqlite store: %v", sqliteErr)
			}
			return nil, fmt.Errorf("Failed to insert output: %v", err)
		}
	}

	if len(blockIDs) > 0 {
		// Insert the block_ids...
		q = `
			INSERT INTO block_ids (
		 	 transaction_id
			,block_id
			) VALUES (
			 $1
			,$2
			)
		`

		for _, blockID := range blockIDs {
			_, err = txn.ExecContext(ctx, q, transactionId, blockID)
			if err != nil {
				if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
					return nil, errors.New(errors.ERR_TX_ALREADY_EXISTS, "Transaction already exists in postgres store: %v", err)
				} else if sqliteErr, ok := err.(*sqlite.Error); ok && sqliteErr.Code() == sqlite3.SQLITE_CONSTRAINT_UNIQUE {
					return nil, errors.New(errors.ERR_TX_ALREADY_EXISTS, "Transaction already exists in sqlite store: %v", sqliteErr)
				}
				return nil, fmt.Errorf("Failed to insert block_ids: %v", err)
			}
		}
	}

	if err := txn.Commit(); err != nil {
		return nil, err
	}

	return txMeta, nil
}

func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	return s.get(ctx, hash, utxo.MetaFields)
}

func (s *Store) Get(ctx context.Context, hash *chainhash.Hash, fields ...[]string) (*meta.Data, error) {
	bins := utxo.MetaFieldsWithTx
	if len(fields) > 0 {
		bins = fields[0]
	}
	return s.get(ctx, hash, bins)
}

func (s *Store) get(ctx context.Context, hash *chainhash.Hash, bins []string) (*meta.Data, error) {

	prometheusUtxoGet.Inc()

	ctx, cancelTimeout := context.WithTimeout(ctx, s.dbTimeout)
	defer cancelTimeout()

	// Always get the transaction row

	q := `
	  SELECT
		 id
		,version
		,lock_time
		,fee
		,size_in_bytes
		FROM transactions
		WHERE hash = $1
	`

	data := &meta.Data{}

	var id int
	var version uint32
	var lockTime uint32

	err := s.db.QueryRowContext(ctx, q, hash[:]).Scan(&id, &version, &lockTime, &data.Fee, &data.SizeInBytes)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.New(errors.ERR_TX_NOT_FOUND, "transaction %s not found: %v", hash, err)
		}

		return nil, err
	}

	tx := bt.Tx{
		Version:  version,
		LockTime: lockTime,
	}

	if contains(bins, "tx") || contains(bins, "inputs") || contains(bins, "parentTxHashes") {
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

	if contains(bins, "tx") || contains(bins, "outputs") {
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

	if contains(bins, "blockIDs") {
		q := `SELECT block_id FROM block_ids WHERE transaction_id = $1 ORDER BY block_id`

		rows, err := s.db.QueryContext(ctx, q, id)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		for rows.Next() {
			var blockID uint32

			if err := rows.Scan(&blockID); err != nil {
				return nil, err
			}

			data.BlockIDs = append(data.BlockIDs, blockID)
		}
	}

	if contains(bins, "tx") {
		data.Tx = &tx
	}

	if contains(bins, "parentTxHashes") {
		for _, input := range tx.Inputs {
			data.ParentTxHashes = append(data.ParentTxHashes, *input.PreviousTxIDChainHash())
		}
	}

	return data, nil
}

func contains(slice []string, item string) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

func (s *Store) Spend(ctx context.Context, spends []*utxo.Spend) (err error) {
	ctx, cancelTimeout := context.WithTimeout(ctx, s.dbTimeout)
	defer cancelTimeout()

	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			prometheusUtxoErrors.WithLabelValues("Spend", "Failed Spend Cleaning").Inc()
			fmt.Printf("ERROR panic in sql Spend: %v\n", recoverErr)
		}
	}()

	txn, err := s.db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		_ = txn.Rollback()
	}()

	for _, spend := range spends {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			if spend == nil {
				continue
			}

			q := `
				SELECT
				 o.transaction_id
				,o.coinbase_spending_height
				,o.utxo_hash
				,o.spending_transaction_id
				FROM outputs o
				JOIN transactions t ON o.transaction_id = t.id
				WHERE t.hash = $1
				AND o.idx = $2
			`

			// Lock the row for update
			switch s.engine {
			case "sqlite", "sqlitememory":
				// Try to lock the row without waiting
				// Using busy timeout as 0 to simulate NO WAIT behavior
				_, err = txn.Exec("PRAGMA busy_timeout = 0;")
				if err != nil {
					return err
				}

			case "postgres":
				q += `
				FOR UPDATE NOWAIT
			`
			}

			var transactionId int
			var coinbaseSpendingHeight uint32
			var utxoHash []byte
			var spendingTransactionID []byte

			err := txn.QueryRowContext(ctx, q, spend.TxID[:], spend.Vout).Scan(&transactionId, &coinbaseSpendingHeight, &utxoHash, &spendingTransactionID)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return errors.New(errors.ERR_NOT_FOUND, "output %s:%d not found", spend.TxID[:], spend.Vout)
				}
				return err
			}

			// Check if the utxo is already spent
			if len(spendingTransactionID) > 0 && !bytes.Equal(spendingTransactionID, spend.SpendingTxID[:]) {
				return fmt.Errorf("[Spend] utxo already spent for %s:%d", spend.TxID, spend.Vout)
			}

			// Check the utxo hash is correct
			if !bytes.Equal(utxoHash, spend.Hash[:]) {
				return fmt.Errorf("[Spend] utxo hash mismatch for %s:%d", spend.TxID, spend.Vout)
			}

			// If this utxo has a coinbase spending height, check it is time to spend it
			if coinbaseSpendingHeight > 0 && s.blockHeight.Load() < coinbaseSpendingHeight {
				return fmt.Errorf("[Spend] coinbase utxo not ready to spend for %s:%d", spend.TxID, spend.Vout)
			}

			q = `
				UPDATE outputs
				SET spending_transaction_id = $1
				WHERE transaction_id = $2
				AND idx = $3
			`

			result, err := txn.ExecContext(ctx, q, spend.SpendingTxID[:], transactionId, spend.Vout)
			if err != nil {
				return fmt.Errorf("[Spend] error spending utxo for %s:%d: %v", spend.TxID, spend.Vout, err)
			}

			affected, err := result.RowsAffected()
			if err != nil {
				return err
			}

			if affected == 0 {
				return fmt.Errorf("[Spend] utxo not spent for %s:%d", spend.TxID, spend.Vout)
			}

			if s.expirationMillis > 0 {
				// Now mark the transaction as tombstoned if there are no more unspent outputs
				q = `
					UPDATE transactions
					SET tombstone_millis = $2
					WHERE id = $1
					AND NOT EXISTS (
						SELECT 1 FROM outputs WHERE transaction_id = $1 AND spending_transaction_id IS NULL
					)
				`

				tombstoneTime := time.Now().Add(time.Duration(s.expirationMillis)*time.Millisecond).UnixNano() / 1e6

				if _, err := txn.ExecContext(ctx, q, transactionId, tombstoneTime); err != nil {
					return err
				}
			}

			prometheusUtxoSpend.Inc()
		}
	}

	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}

func (s *Store) UnSpend(ctx context.Context, spends []*utxostore.Spend) error {
	ctx, cancelTimeout := context.WithTimeout(ctx, s.dbTimeout)
	defer cancelTimeout()

	txn, err := s.db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		_ = txn.Rollback()
	}()

	for _, spend := range spends {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			q := `
				UPDATE outputs
				SET spending_transaction_id = NULL
				WHERE transaction_id IN (
					SELECT id FROM transactions WHERE hash = $1
				)
				AND idx = $2
			`
			if _, err := s.db.ExecContext(ctx, q, spend.TxID[:], spend.Vout); err != nil {
				return err
			}

			prometheusUtxoReset.Inc()
		}
	}

	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}

func (s *Store) Delete(ctx context.Context, hash *chainhash.Hash) error {
	ctx, cancelTimeout := context.WithTimeout(ctx, s.dbTimeout)
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

func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error {
	ctx, cancelTimeout := context.WithTimeout(ctx, s.dbTimeout)
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
		 transaction_id	
		,block_id
		) VALUES (
		 (SELECT id FROM transactions WHERE hash = $1)
		,$2
		)
		ON CONFLICT DO NOTHING
	`

	for _, hash := range hashes {
		_, err = txn.ExecContext(ctx, q, hash[:], blockID)
		if err != nil {
			return err
		}
	}

	// Commit the transaction
	if err := txn.Commit(); err != nil {
		return err
	}

	return nil
}

func (s *Store) GetSpend(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	ctx, cancelTimeout := context.WithTimeout(ctx, s.dbTimeout)
	defer cancelTimeout()

	q := `
		SELECT
		 o.coinbase_spending_height
		,o.spending_transaction_id
		FROM outputs o
		JOIN transactions t ON o.transaction_id = t.id
		WHERE t.hash = $1
		AND o.idx = $2
	`

	var coinbaseSpendingHeight uint32
	var spendingTransactionID []byte

	err := s.db.QueryRowContext(ctx, q, spend.TxID[:], spend.Vout).Scan(&coinbaseSpendingHeight, &spendingTransactionID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.New(errors.ERR_NOT_FOUND, "utxo not found for %s:%d", spend.TxID, spend.Vout)
		}

		return nil, err
	}

	var spendingTxId *chainhash.Hash

	if len(spendingTransactionID) > 0 {
		spendingTxId, err = chainhash.NewHash(spendingTransactionID)
		if err != nil {
			return nil, err
		}
	}

	return &utxo.SpendResponse{
		Status:       int(utxostore.CalculateUtxoStatus(spendingTxId, coinbaseSpendingHeight, s.blockHeight.Load())),
		SpendingTxID: spendingTxId,
		LockTime:     coinbaseSpendingHeight,
	}, nil
}

func (s *Store) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*utxo.UnresolvedMetaData, fields ...string) error {
	ctx, cancelTimeout := context.WithTimeout(ctx, s.dbTimeout)
	defer cancelTimeout()

	for _, unresolvedMetaData := range unresolvedMetaDataSlice {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			if unresolvedMetaData == nil {
				continue
			}

			data, err := s.Get(ctx, &unresolvedMetaData.Hash)
			if err != nil {
				unresolvedMetaData.Err = err
			} else {
				unresolvedMetaData.Data = data
			}
		}
	}

	return nil
}

func (s *Store) PreviousOutputsDecorate(ctx context.Context, outpoints []*meta.PreviousOutput) error {
	ctx, cancelTimeout := context.WithTimeout(ctx, s.dbTimeout)
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

	for _, outpoint := range outpoints {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
			if outpoint == nil {
				continue
			}

			err := s.db.QueryRowContext(ctx, q, outpoint.PreviousTxID[:], outpoint.Vout).Scan(&outpoint.LockingScript, &outpoint.Satoshis)
			if err != nil {
				return err
			}
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
      ,fee				      BIGINT NOT NULL
			,size_in_bytes    BIGINT NOT NULL
			,tombstone_millis BIGINT
      ,inserted_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	  );
	`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create transactions table - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_transactions_hash ON transactions (hash);`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create ux_transactions_hash index - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS ux_transactions_tombstone_millis ON transactions (tombstone_millis) WHERE tombstone_millis IS NOT NULL;`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create ux_transactions_hash index - [%+v]", err)
	}

	// The previous transaction hash may exist in this table
	if _, err := db.Exec(`
      CREATE TABLE IF NOT EXISTS inputs (
	     transaction_id            BIGINT NOT NULL REFERENCES transactions(id)
			,idx 				               BIGINT NOT NULL
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
		return fmt.Errorf("could not create inputs table - [%+v]", err)
	}

	// All fields are NOT NULL except for the spending_transaction_id which is NULL for unspent outputs.
	// The utxo_hash is a hash of the transaction_id, idx, locking_script and satoshis and is used as a checksum of a utxo.
	// The spending_transaction_id is the transaction_id of the transaction that spends this utxo but we do not use referential integrity here as
	// the spending transaction may not have been removed from the database.
	if _, err := db.Exec(`
      CREATE TABLE IF NOT EXISTS outputs (
	      transaction_id           BIGINT NOT NULL REFERENCES transactions(id)
			 ,idx 				             BIGINT NOT NULL
			 ,locking_script           BYTEA NOT NULL
			 ,satoshis                 BIGINT NOT NULL
			 ,coinbase_spending_height BIGINT NOT NULL
			 ,utxo_hash 			         BYTEA NOT NULL
			 ,spending_transaction_id  BYTEA
			 ,PRIMARY KEY (transaction_id, idx)
	  );
	`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create outputs table - [%+v]", err)
	}

	if _, err := db.Exec(`
      CREATE TABLE IF NOT EXISTS block_ids (
	      transaction_id BIGINT NOT NULL REFERENCES transactions(id)
			 ,block_id 			 BIGINT NOT NULL
			 ,PRIMARY KEY (transaction_id, block_id)
	  );
	`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create block_ids table - [%+v]", err)
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
			,fee				      BIGINT NOT NULL
			,size_in_bytes    BIGINT NOT NULL
			,tombstone_millis BIGINT
      ,inserted_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	  );
	`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create transactions table - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_transactions_hash ON transactions (hash);`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create ux_transactions_hash idx - [%+v]", err)
	}

	// The previous transaction hash may exist in this table
	if _, err := db.Exec(`
      CREATE TABLE IF NOT EXISTS inputs (
	     transaction_id            INTEGER NOT NULL REFERENCES transactions(id)
			,idx 				               BIGINT NOT NULL
			,previous_transaction_hash BLOB NOT NULL
			,previous_tx_idx           BIGINT NOT NULL
	    ,previous_tx_satoshis      BIGINT NOT NULL
			,previous_tx_script        BLOB
			,unlocking_script          BYTEA NOT NULL
			,sequence_number           BIGINT NOT NULL
      ,PRIMARY KEY (transaction_id, idx)
	  );
	`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create inputs table - [%+v]", err)
	}

	// All fields are NOT NULL except for the spending_transaction_id which is NULL for unspent outputs.
	// The utxo_hash is a hash of the transaction_id, idx, locking_script and satoshis and is used as a checksum of a utxo.
	// The spending_transaction_id is the transaction_id of the transaction that spends this utxo but we do not use referential integrity here as
	// the spending transaction may not have been removed from the database.
	if _, err := db.Exec(`
      CREATE TABLE IF NOT EXISTS outputs (
	      transaction_id           INTEGER NOT NULL REFERENCES transactions(id)
			 ,idx 				             BIGINT NOT NULL
			 ,locking_script           BLOB NOT NULL
			 ,satoshis                 BIGINT NOT NULL
			 ,coinbase_spending_height BIGINT NOT NULL
			 ,utxo_hash 			         BLOB NOT NULL
			 ,spending_transaction_id  BLOB
			 ,PRIMARY KEY (transaction_id, idx)
	  );
	`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create outputs table - [%+v]", err)
	}

	if _, err := db.Exec(`
      CREATE TABLE IF NOT EXISTS block_ids (
	      transaction_id INTEGER NOT NULL REFERENCES transactions(id)
			 ,block_id 			 BIGINT NOT NULL
			 ,PRIMARY KEY (transaction_id, block_id)
	  );
	`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create block_ids table - [%+v]", err)
	}

	return nil
}

func deleteTombstoned(db *usql.DB) error {
	q := `SELECT id FROM transactions WHERE tombstone_millis < $1;`

	rows, err := db.Query(q, time.Now().UnixNano()/1e6)
	if err != nil {
		return fmt.Errorf("failed to get transactions with tombstone: %v", err)
	}

	defer rows.Close()

	for rows.Next() {
		var id int

		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("failed to scan transaction id: %v", err)
		}

		if _, err := db.Exec("DELETE FROM block_ids WHERE transaction_id = $1", id); err != nil {
			return fmt.Errorf("failed to delete block_ids: %v", err)
		}

		if _, err := db.Exec("DELETE FROM outputs WHERE transaction_id = $1", id); err != nil {
			return fmt.Errorf("failed to delete outputs: %v", err)
		}

		if _, err := db.Exec("DELETE FROM inputs WHERE transaction_id = $1", id); err != nil {
			return fmt.Errorf("failed to delete inputs: %v", err)
		}
		if _, err := db.Exec("DELETE FROM transactions WHERE id = $1", id); err != nil {
			return fmt.Errorf("failed to delete transaction: %v", err)
		}
	}

	return nil
}
