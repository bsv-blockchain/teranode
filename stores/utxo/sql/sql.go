package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	_ "github.com/lib/pq"
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

func (s *Store) Get(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	prometheusUtxoGet.Inc()

	var lockTime uint32
	var txIdBytes []byte
	err := s.db.QueryRowContext(ctx, "SELECT lock_time, tx_id FROM utxos WHERE hash = $1", hash[:]).
		Scan(&lockTime, &txIdBytes)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &utxostore.UTXOResponse{
				Status: int(utxostore_api.Status_NOT_FOUND),
			}, nil
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

	return &utxostore.UTXOResponse{
		Status:       int(utxostore.CalculateUtxoStatus(txHash, lockTime, s.blockHeight)),
		LockTime:     lockTime,
		SpendingTxID: txHash,
	}, nil
}

func (s *Store) Store(ctx context.Context, hash *chainhash.Hash, nLockTime uint32) (*utxostore.UTXOResponse, error) {
	q := `
		INSERT INTO utxos
		    (hash, lock_time)
		VALUES
		    ($1, $2)
	`
	if _, err := s.db.Exec(q, hash[:], nLockTime); err != nil {
		// check whether we already set this utxo with the same tx_id
		var txIdBytes []byte
		err = s.db.QueryRowContext(ctx, "SELECT tx_id FROM utxos WHERE hash = $1", hash[:]).Scan(&txIdBytes)
		if err != nil {
			return nil, err
		}

		// if we have the same tx_id, we are good
		if txIdBytes == nil {
			return &utxostore.UTXOResponse{
				Status: int(utxostore_api.Status_OK),
			}, nil
		} else if [32]byte(txIdBytes) == *hash {
			return &utxostore.UTXOResponse{
				Status:       int(utxostore_api.Status_SPENT),
				SpendingTxID: hash,
			}, nil
		}

		return nil, err
	}

	prometheusUtxoStore.Inc()

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK), // should be created, we need this for the block assembly
	}, nil
}

func (s *Store) BatchStore(ctx context.Context, hashes []*chainhash.Hash, nLockTime uint32) (*utxostore.BatchResponse, error) {
	var h *chainhash.Hash
	for _, h = range hashes {
		_, err := s.Store(ctx, h, nLockTime)
		if err != nil {
			return nil, err
		}
	}

	return &utxostore.BatchResponse{
		Status: 0,
	}, nil
}

func (s *Store) Spend(ctx context.Context, hash *chainhash.Hash, txID *chainhash.Hash) (utxoResponse *utxostore.UTXOResponse, err error) {
	defer func() {
		if recoverErr := recover(); recoverErr != nil {
			prometheusUtxoErrors.WithLabelValues("Spend", "Failed Spend Cleaning").Inc()
			fmt.Printf("ERROR panic in sql Spend: %v\n", recoverErr)
		}
	}()

	q := `
		UPDATE utxos
		SET tx_id = $1
		WHERE hash = $2
		  AND (lock_time <= $3 OR (lock_time >= 500000000 AND lock_time <= $4))
		  AND tx_id IS NULL
	`
	result, err := s.db.ExecContext(ctx, q, txID[:], hash[:], s.blockHeight, time.Now().Unix())
	if err != nil {
		return nil, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}

	var utxo *utxostore.UTXOResponse
	if affected == 0 {
		utxo, err = s.Get(ctx, hash)
		if err != nil {
			return nil, err
		}
		if utxo.SpendingTxID != nil {
			if utxo.SpendingTxID.IsEqual(txID) {
				return &utxostore.UTXOResponse{
					Status:       int(utxostore_api.Status_OK),
					LockTime:     utxo.LockTime,
					SpendingTxID: utxo.SpendingTxID,
				}, nil
			} else {
				return &utxostore.UTXOResponse{
					Status:       int(utxostore_api.Status_SPENT),
					LockTime:     utxo.LockTime,
					SpendingTxID: utxo.SpendingTxID,
				}, nil
			}
		} else if (utxo.LockTime > s.blockHeight && utxo.LockTime < 500000000) || utxo.LockTime > uint32(time.Now().Unix()) {
			return &utxostore.UTXOResponse{
				Status: int(utxostore_api.Status_LOCKED),
			}, nil
		}
	}

	prometheusUtxoSpend.Inc()

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (s *Store) Reset(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	q := `
		UPDATE utxos
		SET tx_id = NULL
		WHERE hash = $1
	`
	if _, err := s.db.ExecContext(ctx, q, hash[:]); err != nil {
		return nil, err
	}

	prometheusUtxoReset.Inc()

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (s *Store) Delete(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	q := `
		DELETE FROM utxos
		WHERE hash = $1
	`
	if _, err := s.db.ExecContext(ctx, q, hash[:]); err != nil {
		return nil, err
	}

	prometheusUtxoDelete.Inc()

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
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
