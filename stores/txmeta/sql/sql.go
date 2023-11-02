package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
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
	prometheusTxMetaGet      prometheus.Counter
	prometheusTxMetaSet      prometheus.Counter
	prometheusTxMetaSetMined prometheus.Counter
	prometheusTxMetaDelete   prometheus.Counter
)

func init() {
	prometheusTxMetaGet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "sql_txmeta_get",
			Help: "Number of txmeta get calls done to sql db",
		},
	)
	prometheusTxMetaSet = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "sql_txmeta_set",
			Help: "Number of txmeta set calls done to sql db",
		},
	)
	prometheusTxMetaSetMined = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "sql_txmeta_set_mined",
			Help: "Number of txmeta set_mined calls done to sql db",
		},
	)
	prometheusTxMetaDelete = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "sql_txmeta_delete",
			Help: "Number of txmeta delete calls done to sql db",
		},
	)
}

type Store struct {
	db     *sql.DB
	engine string
}

func New(storeUrl *url.URL) (*Store, error) {
	logger := gocore.Log("tmsql")

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

func (s *Store) Get(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	q := `
	SELECT
		txBytes,
	    fee,
		size_in_bytes,
	    parents,
	    blocks,
	    lock_time
	FROM txmeta
	WHERE hash = $1`

	var txBytes []byte
	var fee uint64
	var sizeInBytes uint64
	var parents []byte
	var blocks []byte
	var lockTime uint32

	err := s.db.QueryRowContext(ctx, q, hash[:]).Scan(&txBytes, &fee, &sizeInBytes, &parents, &blocks, &lockTime)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, txmeta.ErrNotFound
		}
		return nil, fmt.Errorf("failed to get txmeta: %+v", err)
	}

	tx, err := bt.NewTxFromBytes(txBytes)
	if err != nil {
		return nil, err
	}

	var h *chainhash.Hash
	parentTxHashes := make([]*chainhash.Hash, 0, len(parents)/chainhash.HashSize)
	for i := 0; i < len(parents); i += chainhash.HashSize {
		h, err = chainhash.NewHash(parents[i : i+chainhash.HashSize])
		if err != nil {
			return nil, fmt.Errorf("failed to parse parent hash: %+v", err)
		}
		parentTxHashes = append(parentTxHashes, h)
	}

	blockHashes := make([]*chainhash.Hash, 0, len(blocks)/chainhash.HashSize)
	for i := 0; i < len(blocks); i += chainhash.HashSize {
		h, err = chainhash.NewHash(blocks[i : i+chainhash.HashSize])
		if err != nil {
			return nil, fmt.Errorf("failed to parse block hash: %+v", err)
		}
		blockHashes = append(blockHashes, h)
	}

	prometheusTxMetaGet.Inc()

	return &txmeta.Data{
		Tx:             tx,
		Fee:            fee,
		SizeInBytes:    sizeInBytes,
		ParentTxHashes: parentTxHashes,
		BlockHashes:    blockHashes,
	}, nil
}

func (s *Store) Create(ctx context.Context, tx *bt.Tx) (*txmeta.Data, error) {
	q := `
		INSERT INTO txmeta
		    (txBytes, hash, fee, size_in_bytes, parents, lock_time)
		VALUES ($1, $2, $3, $4, $5, $6)`

	txBytes := tx.ExtendedBytes()
	hash := tx.TxIDChainHash()
	data, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, err
	}

	parents := make([]byte, 0, len(data.ParentTxHashes)*chainhash.HashSize)
	for _, parent := range data.ParentTxHashes {
		parents = append(parents, parent[:]...)
	}

	_, err = s.db.ExecContext(ctx, q, txBytes, hash[:], data.Fee, data.SizeInBytes, parents, tx.LockTime)
	if err != nil {
		postgresErr := "duplicate key value violates unique constraint"
		sqLiteErr := "UNIQUE constraint failed"
		if strings.Contains(err.Error(), postgresErr) || strings.Contains(err.Error(), sqLiteErr) {
			return data, errors.Join(fmt.Errorf("failed to insert txmeta: %+v", txmeta.ErrAlreadyExists))
		}
		return data, errors.Join(fmt.Errorf("failed to insert txmeta: %+v", err))
	}

	prometheusTxMetaSet.Inc()

	return data, nil
}

func (s *Store) SetMined(ctx context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) error {
	// add block hash to blocks array
	var q string
	if s.engine == "postgres" {
		q = `
		UPDATE txmeta
		SET blocks = COALESCE(blocks, '') || $2
		WHERE hash = $1`
	} else {
		q = `
		UPDATE txmeta
		SET blocks = ifnull(blocks, "") || $2
		WHERE hash = $1`
	}
	_, err := s.db.ExecContext(ctx, q, hash[:], blockHash[:])
	if err != nil {
		return fmt.Errorf("failed to update txmeta: %+v", err)
	}

	prometheusTxMetaSetMined.Inc()

	return nil
}

func (s *Store) Delete(ctx context.Context, hash *chainhash.Hash) error {
	q := `
		DELETE FROM txmeta
		WHERE hash = $1`
	_, err := s.db.ExecContext(ctx, q, hash[:])
	if err != nil {
		return fmt.Errorf("failed to delete txmeta: %+v", err)
	}

	prometheusTxMetaDelete.Inc()

	return nil
}

func createPostgresSchema(db *sql.DB) error {
	if _, err := db.Exec(`
    CREATE TABLE IF NOT EXISTS txmeta (
	   id            BIGSERIAL PRIMARY KEY
		,txBytes       BYTEA NULL
	  ,hash          BYTEA NOT NULL
	  ,fee           BIGINT NOT NULL
		,size_in_bytes BIGINT NOT NULL
	  ,parents       BYTEA NULL
	  ,blocks        BYTEA NULL
	  ,lock_time     BIGINT NOT NULL
    ,inserted_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	  );
	`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create tx meta table - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_txmeta_hash ON txmeta (hash);`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create ux_txmeta_hash index - [%+v]", err)
	}

	return nil
}

func createSqliteSchema(db *sql.DB) error {
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS txmeta (
		 id            INTEGER PRIMARY KEY AUTOINCREMENT
		,txBytes       BLOB NULL
	  ,hash          BLOB NOT NULL
    ,fee    		   BIGINT NOT NULL
		,size_in_bytes BIGINT NOT NULL
	  ,parents       BLOB NULL
	  ,blocks        BLOB NULL
    ,lock_time	   BIGINT NOT NULL
    ,inserted_at   TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	  );
	`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create tx meta table - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_txmeta_hash ON txmeta (hash);`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create ux_txmeta_hash index - [%+v]", err)
	}

	return nil
}
