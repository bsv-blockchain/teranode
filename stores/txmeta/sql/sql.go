package sql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/util"
	_ "github.com/lib/pq"
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
	    status,
	    fee,
	    parents,
	    utxos,
	    blocks,
	    lock_time
	FROM txmeta
	WHERE hash = $1`

	var status int
	var fee uint64
	var parents []byte
	var utxos []byte
	var blocks []byte
	var lockTime uint32

	err := s.db.QueryRowContext(ctx, q, hash[:]).Scan(&status, &fee, &parents, &utxos, &blocks, &lockTime)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, txmeta.ErrNotFound
		}
		return nil, fmt.Errorf("failed to get txmeta: %+v", err)
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

	utxoHashes := make([]*chainhash.Hash, 0, len(utxos)/chainhash.HashSize)
	for i := 0; i < len(utxos); i += chainhash.HashSize {
		h, err = chainhash.NewHash(utxos[i : i+chainhash.HashSize])
		if err != nil {
			return nil, fmt.Errorf("failed to parse utxo hash: %+v", err)
		}
		utxoHashes = append(utxoHashes, h)
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
		Status:         txmeta.TxStatus(status),
		Fee:            fee,
		UtxoHashes:     utxoHashes,
		ParentTxHashes: parentTxHashes,
		BlockHashes:    blockHashes,
		LockTime:       lockTime,
	}, nil
}

func (s *Store) Create(ctx context.Context, hash *chainhash.Hash, fee uint64, parentTxHashes []*chainhash.Hash, utxoHashes []*chainhash.Hash, nLockTime uint32) error {
	q := `
		INSERT INTO txmeta
		    (hash, status, fee, parents, utxos, lock_time)
		VALUES ($1, $2, $3, $4, $5, $6)`

	parents := make([]byte, 0, len(parentTxHashes)*chainhash.HashSize)
	for _, parent := range parentTxHashes {
		parents = append(parents, parent[:]...)
	}

	utxos := make([]byte, 0, len(utxoHashes)*chainhash.HashSize)
	for _, utxo := range utxoHashes {
		utxos = append(utxos, utxo[:]...)
	}

	_, err := s.db.ExecContext(ctx, q, hash[:], txmeta.Validated, fee, parents, utxos, nLockTime)
	if err != nil {
		return fmt.Errorf("failed to insert txmeta: %+v", err)
	}

	prometheusTxMetaSet.Inc()

	return nil
}

func (s *Store) SetMined(ctx context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) error {
	// add block hash to blocks array
	var q string
	if s.engine == "postgres" {
		q = `
		UPDATE txmeta
		SET blocks = COALESCE(blocks, '') || $2, status = $3
		WHERE hash = $1`
	} else {
		q = `
		UPDATE txmeta
		SET blocks = ifnull(blocks, "") || $2, status = $3
		WHERE hash = $1`
	}
	_, err := s.db.ExecContext(ctx, q, hash[:], blockHash[:], int(txmeta.Confirmed))
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
	    ,hash         BYTEA NOT NULL
		,status       INT NOT NULL
	    ,fee          BIGINT NOT NULL
	    ,parents      BYTEA NULL
	    ,utxos        BYTEA NULL
	    ,blocks       BYTEA NULL
	    ,lock_time    BIGINT NOT NULL
        ,inserted_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	  );
	`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create utxos table - [%+v]", err)
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
		 id           INTEGER PRIMARY KEY AUTOINCREMENT
	    ,hash           BLOB NOT NULL
		,status         INT NOT NULL
        ,fee    		BIGINT NOT NULL
	    ,parents        BLOB NULL
	    ,utxos          BLOB NULL
	    ,blocks         BLOB NULL
        ,lock_time		BIGINT NOT NULL
        ,inserted_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	  );
	`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create utxos table - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_txmeta_hash ON txmeta (hash);`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create ux_txmeta_hash index - [%+v]", err)
	}

	return nil
}
