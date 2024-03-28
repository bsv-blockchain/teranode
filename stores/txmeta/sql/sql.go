package sql

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/usql"
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
	logger    ulogger.Logger
	db        *usql.DB
	engine    string
	dbTimeout time.Duration
}

func New(logger ulogger.Logger, storeUrl *url.URL) (*Store, error) {
	logger = logger.New("tmsql")

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

	dbTimeout, _ := gocore.Config().GetInt("txmeta_store_dbTimeoutMillis", 5000)

	s := &Store{
		logger:    logger,
		db:        db,
		engine:    storeUrl.Scheme,
		dbTimeout: time.Duration(dbTimeout) * time.Millisecond,
	}

	return s, nil
}

func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	return s.Get(ctx, hash)
}

func (s *Store) Get(cntxt context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	ctx, cancelTimeout := context.WithTimeout(cntxt, s.dbTimeout)
	defer cancelTimeout()

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
			return nil, txmeta.NewErrTxmetaNotFound(hash)
		}
		return nil, fmt.Errorf("failed to get txmeta: %+v", err)
	}

	tx, err := bt.NewTxFromBytes(txBytes)
	if err != nil {
		return nil, err
	}

	parentTxHashes := make([]chainhash.Hash, 0, len(parents)/chainhash.HashSize)
	for i := 0; i < len(parents); i += chainhash.HashSize {
		parentTxHashes = append(parentTxHashes, chainhash.Hash(parents[i:i+chainhash.HashSize]))
	}

	var blockIDs []uint32
	for i := 0; i < len(blocks); i += 4 {
		blockID := binary.LittleEndian.Uint32(blocks[i : i+4])
		blockIDs = append(blockIDs, blockID)
	}

	prometheusTxMetaGet.Inc()

	return &txmeta.Data{
		Tx:             tx,
		Fee:            fee,
		SizeInBytes:    sizeInBytes,
		ParentTxHashes: parentTxHashes,
		BlockIDs:       blockIDs,
	}, nil
}

func (s *Store) MetaBatchDecorate(ctx context.Context, items []*txmeta.MissingTxHash, fields ...string) error {
	// TODO make this into a batch call
	for _, item := range items {
		data, err := s.Get(ctx, &item.Hash)
		if err != nil {
			if uerr, ok := err.(*ubsverrors.Error); ok {
				if uerr.Code == ubsverrors.ERR_NOT_FOUND {
					continue
				}
			}
			return err
		}
		item.Data = data
	}

	return nil
}

func (s *Store) Create(cntxt context.Context, tx *bt.Tx) (*txmeta.Data, error) {
	ctx, cancelTimeout := context.WithTimeout(cntxt, s.dbTimeout)
	defer cancelTimeout()

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
			return data, fmt.Errorf("failed to insert tx meta: %w", txmeta.NewErrTxmetaAlreadyExists(hash))
		}
		return data, fmt.Errorf("failed to insert tx meta: %w", err)
	}

	prometheusTxMetaSet.Inc()

	return data, nil
}

func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) (err error) {
	for _, hash := range hashes {
		if err = s.SetMined(ctx, hash, blockID); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) SetMined(cntxt context.Context, hash *chainhash.Hash, blockID uint32) error {
	ctx, cancelTimeout := context.WithTimeout(cntxt, s.dbTimeout)
	defer cancelTimeout()

	blockIDBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(blockIDBytes, blockID)

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
	_, err := s.db.ExecContext(ctx, q, hash[:], blockIDBytes)
	if err != nil {
		return fmt.Errorf("failed to update txmeta: %+v", err)
	}

	prometheusTxMetaSetMined.Inc()

	return nil
}

func (s *Store) Delete(cntxt context.Context, hash *chainhash.Hash) error {
	ctx, cancelTimeout := context.WithTimeout(cntxt, s.dbTimeout)
	defer cancelTimeout()

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

func createPostgresSchema(db *usql.DB) error {
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

func createSqliteSchema(db *usql.DB) error {
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS txmeta (
		id             INTEGER PRIMARY KEY AUTOINCREMENT
		,txBytes       BLOB NULL
		,hash          BLOB NOT NULL
		,fee    	   BIGINT NOT NULL
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
