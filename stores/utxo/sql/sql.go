package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/usql"
	pq "github.com/lib/pq"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	_ "modernc.org/sqlite"
)

var (
	prometheusUtxoGet   prometheus.Counter
	prometheusUtxoStore prometheus.Counter
	// prometheusUtxoReStore    prometheus.Counter
	// prometheusUtxoStoreSpent prometheus.Counter
	prometheusUtxoSpend prometheus.Counter
	// prometheusUtxoReSpend    prometheus.Counter
	// prometheusUtxoSpendSpent prometheus.Counter
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
	prometheusUtxoStore = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "sql_utxo_store",
			Help: "Number of utxo store calls done to sql",
		},
	)
	// prometheusUtxoStoreSpent = promauto.NewCounter(
	// 	prometheus.CounterOpts{
	// 		Name: "sql_utxo_store_spent",
	// 		Help: "Number of utxo store calls that were already spent to sql",
	// 	},
	// )
	// prometheusUtxoReStore = promauto.NewCounter(
	// 	prometheus.CounterOpts{
	// 		Name: "sql_utxo_restore",
	// 		Help: "Number of utxo restore calls done to sql",
	// 	},
	// )
	prometheusUtxoSpend = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "sql_utxo_spend",
			Help: "Number of utxo spend calls done to sql",
		},
	)
	// prometheusUtxoReSpend = promauto.NewCounter(
	// 	prometheus.CounterOpts{
	// 		Name: "sql_utxo_respend",
	// 		Help: "Number of utxo respend calls done to sql",
	// 	},
	// )
	// prometheusUtxoSpendSpent = promauto.NewCounter(
	// 	prometheus.CounterOpts{
	// 		Name: "sql_utxo_spend_spent",
	// 		Help: "Number of utxo spend calls that were already spent done to sql",
	// 	},
	// )
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
	logger      ulogger.Logger
	db          *usql.DB
	engine      string
	blockHeight uint32
	dbTimeout   time.Duration
}

func New(logger ulogger.Logger, storeUrl *url.URL) (*Store, error) {
	logger = logger.New("uxsql")

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

	s := &Store{
		logger:    logger,
		db:        db,
		engine:    storeUrl.Scheme,
		dbTimeout: time.Duration(dbTimeoutMillis) * time.Millisecond,
	}

	return s, nil
}

func (s *Store) SetBlockHeight(blockHeight uint32) error {
	s.blockHeight = blockHeight
	return nil
}

func (s *Store) GetBlockHeight() (uint32, error) {
	return s.blockHeight, nil
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

func (s *Store) Get(cntxt context.Context, spend *utxostore.Spend) (*utxostore.Response, error) {
	prometheusUtxoGet.Inc()

	ctx, cancelTimeout := context.WithTimeout(cntxt, s.dbTimeout)
	defer cancelTimeout()

	var lockTime uint32
	var txIdBytes []byte
	err := s.db.QueryRowContext(ctx, "SELECT lock_time, tx_id FROM utxos WHERE hash = $1", spend.Hash[:]).
		Scan(&lockTime, &txIdBytes)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, utxostore.ErrNotFound
		}
		return nil, err
	}

	var txHash *chainhash.Hash
	if txIdBytes != nil {
		txHash, err = chainhash.NewHash(txIdBytes)
		if err != nil {
			return nil, err
		}
	}

	return &utxostore.Response{
		Status:       int(utxostore.CalculateUtxoStatus(txHash, lockTime, s.blockHeight)),
		LockTime:     lockTime,
		SpendingTxID: txHash,
	}, nil
}

// Store stores the utxos of the tx in aerospike
// the lockTime optional argument is needed for coinbase transactions that do not contain the lock time
func (s *Store) Store(cntxt context.Context, tx *bt.Tx, lockTime ...uint32) error {
	ctx, cancelTimeout := context.WithTimeout(cntxt, s.dbTimeout)
	defer cancelTimeout()

	utxoHashes, err := utxostore.GetUtxoHashes(tx)
	if err != nil {
		return err
	}

	storeLockTime := tx.LockTime
	if len(lockTime) > 0 {
		storeLockTime = lockTime[0]
	}

	switch s.engine {
	case "postgres":

		// Prepare the copy operation
		txn, err := s.db.Begin()
		if err != nil {
			return err
		}

		defer func() {
			_ = txn.Rollback()
		}()

		stmt, err := txn.Prepare(pq.CopyIn("utxos", "lock_time", "hash"))
		if err != nil {
			return err
		}

		for _, hash := range utxoHashes {
			if _, err := stmt.ExecContext(ctx, storeLockTime, hash[:]); err != nil {
				s.logger.Errorf("error storing utxo: %s", err.Error())
				return err
			}
		}

		// Execute the batch transaction
		_, err = stmt.ExecContext(ctx)
		if err != nil {
			return err
		}

		if err := stmt.Close(); err != nil {
			return err
		}
		if err := txn.Commit(); err != nil {
			return err
		}

	case "sqlite", "sqlitememory":
		// Prepare the copy operation
		const batchSize = 500

		qBase := `
				INSERT INTO utxos
						(lock_time, hash)
				VALUES
		`
		qRow := fmt.Sprintf("(%d, ?)", storeLockTime)

		for i := 0; i < len(utxoHashes); i += batchSize {
			var valuesStrings []string
			var valuesArgs []interface{}

			end := i + batchSize
			if end > len(utxoHashes) {
				end = len(utxoHashes)
			}

			for j := i; j < end; j++ {
				valuesStrings = append(valuesStrings, qRow)
				valuesArgs = append(valuesArgs, utxoHashes[j][:])
			}

			q := qBase + strings.Join(valuesStrings, ",")

			select {
			case <-ctx.Done():
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					return fmt.Errorf("[sql.go.Store] context timeout, managed to get through %d of %d", i, len(utxoHashes))
				}
				return fmt.Errorf("[sql.go.Store] context cancelled, managed to get through %d of %d", i, len(utxoHashes))
			default:
				_, err = s.db.ExecContext(ctx, q, valuesArgs...)
				if err != nil {
					return err
				}
			}
		}
	default:
		return fmt.Errorf("unknown database engine: %s", s.engine)
	}

	prometheusUtxoStore.Add(float64(len(utxoHashes)))

	return nil
}

// StoreFromHashes stores the utxos of the tx in aerospike
// TODO not tested for SQL
func (s *Store) StoreFromHashes(cntxt context.Context, _ chainhash.Hash, hashes []chainhash.Hash, lockTime uint32) error {
	ctx, cancelTimeout := context.WithTimeout(cntxt, s.dbTimeout)
	defer cancelTimeout()

	switch s.engine {
	case "postgres":

		// Prepare the copy operation
		txn, err := s.db.Begin()
		if err != nil {
			return err
		}

		defer func() {
			_ = txn.Rollback()
		}()

		stmt, err := txn.Prepare(pq.CopyIn("utxos", "lock_time", "hash"))
		if err != nil {
			return err
		}

		for _, hash := range hashes {
			if _, err := stmt.ExecContext(ctx, lockTime, hash[:]); err != nil {
				s.logger.Errorf("error storing utxo: %s", err.Error())
				return err
			}
		}

		// Execute the batch transaction
		_, err = stmt.ExecContext(ctx)
		if err != nil {
			return err
		}

		if err := stmt.Close(); err != nil {
			return err
		}
		if err := txn.Commit(); err != nil {
			return err
		}

	case "sqlite", "sqlitememory":
		// Prepare the copy operation
		const batchSize = 500

		qBase := `
			INSERT INTO utxos
				(lock_time, hash)
			VALUES
		`
		qRow := fmt.Sprintf("(%d, ?)", lockTime)

		for i := 0; i < len(hashes); i += batchSize {
			var valuesStrings []string
			var valuesArgs []interface{}

			end := i + batchSize
			if end > len(hashes) {
				end = len(hashes)
			}

			for j := i; j < end; j++ {
				valuesStrings = append(valuesStrings, qRow)
				valuesArgs = append(valuesArgs, hashes[j][:])
			}

			q := qBase + strings.Join(valuesStrings, ",")

			select {
			case <-ctx.Done():
				if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					return fmt.Errorf("[sql.go.StoreFromHashes] context timeout, managed to get through %d of %d", i, len(hashes))
				}
				return fmt.Errorf("[sql.go.StoreFromHashes] context cancelled, managed to get through %d of %d", i, len(hashes))
			default:
				_, err := s.db.ExecContext(ctx, q, valuesArgs...)
				if err != nil {
					return err
				}
			}
		}
	default:
		return fmt.Errorf("unknown database engine: %s", s.engine)
	}

	prometheusUtxoStore.Add(float64(len(hashes)))
	return nil
}

func (s *Store) Spend(cntxt context.Context, spends []*utxostore.Spend) (err error) {
	ctx, cancelTimeout := context.WithTimeout(cntxt, s.dbTimeout)
	defer cancelTimeout()

	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			prometheusUtxoErrors.WithLabelValues("Spend", "Failed Spend Cleaning").Inc()
			fmt.Printf("ERROR panic in sql Spend: %v\n", recoverErr)
		}
	}()

	var result sql.Result
	for _, spend := range spends {
		q := `
			UPDATE utxos
			SET tx_id = $1
			WHERE hash = $2
			  AND (lock_time <= $3 OR (lock_time >= 500000000 AND lock_time <= $4))
			  AND tx_id IS NULL
		`
		result, err = s.db.ExecContext(ctx, q, spend.SpendingTxID[:], spend.Hash[:], s.blockHeight, time.Now().Unix())
		if err != nil {
			return fmt.Errorf("[Spend][%s] error spending utxo: %s", spend.Hash.String(), err.Error())
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}

		var utxo *utxostore.Response
		if affected == 0 {
			utxo, err = s.Get(ctx, spend)
			if err != nil {
				return err
			}
			if utxo.SpendingTxID != nil {
				if utxo.SpendingTxID.IsEqual(spend.SpendingTxID) {
					return nil
				} else {
					return utxostore.NewErrSpent(utxo.SpendingTxID)
				}
			} else if !util.ValidLockTime(utxo.LockTime, s.blockHeight) {
				return utxostore.NewErrLockTime(utxo.LockTime, s.blockHeight)
			}
		}

		prometheusUtxoSpend.Inc()
	}

	return nil
}

func (s *Store) UnSpend(cntxt context.Context, spends []*utxostore.Spend) error {
	ctx, cancelTimeout := context.WithTimeout(cntxt, s.dbTimeout)
	defer cancelTimeout()

	for _, spend := range spends {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("[sql.go.UnSpend] context cancelled")
			}
			return fmt.Errorf("[sql.go.UnSpend] context cancelled")
		default:
			if err := s.unSpend(ctx, spend); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Store) unSpend(ctx context.Context, spend *utxostore.Spend) error {
	q := `
		UPDATE utxos
		SET tx_id = NULL
		WHERE hash = $1
	`
	if _, err := s.db.ExecContext(ctx, q, spend.Hash[:]); err != nil {
		return err
	}

	prometheusUtxoReset.Inc()

	return nil
}

func (s *Store) Delete(cntxt context.Context, tx *bt.Tx) error {
	ctx, cancelTimeout := context.WithTimeout(cntxt, s.dbTimeout)
	defer cancelTimeout()
	for vOut, output := range tx.Outputs {
		utxoHash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), output, uint32(vOut))
		if err != nil {
			return err
		}

		if err = s.delete(ctx, utxoHash); err != nil {
			return err
		}
	}

	prometheusUtxoDelete.Inc()

	return nil
}

func (s *Store) DeleteSpends(_ bool) {
	// noop
}

func (s *Store) delete(ctx context.Context, hash *chainhash.Hash) error {
	q := `
		DELETE FROM utxos
		WHERE hash = $1
	`
	if _, err := s.db.ExecContext(ctx, q, hash[:]); err != nil {
		return err
	}

	return nil
}

func createPostgresSchema(db *usql.DB) error {
	if _, err := db.Exec(`
      CREATE TABLE IF NOT EXISTS utxos (
	    id            BIGSERIAL PRIMARY KEY
	    ,hash         BYTEA NOT NULL
	    ,lock_time    BIGINT NOT NULL
	    ,tx_id        BYTEA NULL
        ,inserted_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	  );
	`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create utxos table - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_utxos_hash ON utxos (hash);`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create ux_utxos_hash index - [%+v]", err)
	}

	return nil
}

func createSqliteSchema(db *usql.DB) error {
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS utxos (
		 id           INTEGER PRIMARY KEY AUTOINCREMENT
	    ,hash           BLOB NOT NULL
        ,lock_time		BIGINT NOT NULL
	    ,tx_id          BLOB NULL
        ,inserted_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	  );
	`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create utxos table - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_utxos_hash ON utxos (hash);`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create ux_utxos_hash index - [%+v]", err)
	}

	return nil
}
