package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"time"

	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	_ "github.com/lib/pq"
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
	db          *sql.DB
	engine      string
	blockHeight uint32
}

func New(storeUrl *url.URL) (*Store, error) {
	logger := gocore.Log("uxsql")

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

	s := &Store{
		db:     db,
		engine: storeUrl.Scheme,
	}

	return s, nil
}

func (s *Store) SetBlockHeight(blockHeight uint32) error {
	s.blockHeight = blockHeight
	return nil
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

func (s *Store) Get(ctx context.Context, spend *utxostore.Spend) (*utxostore.Response, error) {
	prometheusUtxoGet.Inc()

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
func (s *Store) Store(ctx context.Context, tx *bt.Tx, lockTime ...uint32) error {
	_, utxoHashes, err := utxostore.GetFeesAndUtxoHashes(tx)
	if err != nil {
		return err
	}

	storeLockTime := tx.LockTime
	if len(lockTime) > 0 {
		storeLockTime = lockTime[0]
	}

	q := `
		INSERT INTO utxos
		    (lock_time, hash)
		VALUES
	`

	variables := make([]interface{}, 0, len(utxoHashes))
	for _, hash := range utxoHashes {
		variables = append(variables, hash[:])
		q += fmt.Sprintf("(%d, $%d),", storeLockTime, len(variables))
	}

	// remove last comma from query
	q = q[:len(q)-1]

	if _, err = s.db.ExecContext(ctx, q, variables...); err != nil {
		return err
	}

	prometheusUtxoStore.Add(float64(len(utxoHashes)))

	return nil
}

func (s *Store) Spend(ctx context.Context, spends []*utxostore.Spend) (err error) {
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
			return err
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
					return utxostore.ErrSpent
				}
			} else if !util.ValidLockTime(utxo.LockTime, s.blockHeight) {
				return utxostore.ErrLockTime
			}
		}

		prometheusUtxoSpend.Inc()
	}

	return nil
}

func (s *Store) Reset(ctx context.Context, spend *utxostore.Spend) error {
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

func (s *Store) Delete(ctx context.Context, spend *utxostore.Spend) error {
	q := `
		DELETE FROM utxos
		WHERE hash = $1
	`
	if _, err := s.db.ExecContext(ctx, q, spend.Hash[:]); err != nil {
		return err
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

func createPostgresSchema(db *sql.DB) error {
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

func createSqliteSchema(db *sql.DB) error {
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
