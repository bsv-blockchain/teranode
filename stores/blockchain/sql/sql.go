package sql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"

	"github.com/TAAL-GmbH/ubsv/model"
	"github.com/labstack/gommon/random"
	_ "github.com/lib/pq"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	_ "modernc.org/sqlite"
)

type SQL struct {
	db     *sql.DB
	engine string
}

func init() {
	gocore.NewStat("blockchain")
}

func New(storeUrl *url.URL) (*SQL, error) {
	var db *sql.DB
	var err error

	var memory bool

	logLevel, _ := gocore.Config().Get("logLevel")
	logger := gocore.Log("bcsql", gocore.NewLogLevelFromString(logLevel))

	switch storeUrl.Scheme {
	case "postgres":
		dbHost := storeUrl.Hostname()
		port := storeUrl.Port()
		dbPort, _ := strconv.Atoi(port)
		dbName := storeUrl.Path[1:]
		dbUser := ""
		dbPassword := ""
		if storeUrl.User != nil {
			dbUser = storeUrl.User.Username()
			dbPassword, _ = storeUrl.User.Password()
		}

		dbInfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable host=%s port=%d", dbUser, dbPassword, dbName, dbHost, dbPort)

		db, err = sql.Open(storeUrl.Scheme, dbInfo)
		if err != nil {
			return nil, fmt.Errorf("failed to open postgres DB: %+v", err)
		}

		idleConns, _ := gocore.Config().GetInt("blockchain_postgresMaxIdleConns", 10)
		db.SetMaxIdleConns(idleConns)
		maxOpenConns, _ := gocore.Config().GetInt("blockchain_postgresMaxOpenConns", 80)
		db.SetMaxOpenConns(maxOpenConns)

		if err = createPostgresSchema(db); err != nil {
			return nil, fmt.Errorf("failed to create postgres schema: %+v", err)
		}

	case "sqlitememory":
		memory = true
		fallthrough
	case "sqlite":
		var filename string
		if memory {
			filename = fmt.Sprintf("file:%s?mode=memory&cache=shared", random.String(16))
		} else {
			folder, _ := gocore.Config().Get("dataFolder", "data")
			if err = os.MkdirAll(folder, 0755); err != nil {
				return nil, fmt.Errorf("failed to create data folder %s: %+v", folder, err)
			}

			filename, err = filepath.Abs(path.Join(folder, "blockchain.db"))
			if err != nil {
				return nil, fmt.Errorf("failed to get absolute path for sqlite DB: %+v", err)
			}

			// filename = fmt.Sprintf("file:%s?cache=shared&mode=rwc", filename)
			filename = fmt.Sprintf("%s?cache=shared&_pragma=busy_timeout=10000&_pragma=journal_mode=WAL", filename)
		}

		logger.Infof("Using sqlite DB: %s", filename)

		db, err = sql.Open("sqlite", filename)
		if err != nil {
			return nil, fmt.Errorf("failed to open sqlite DB: %+v", err)
		}

		if _, err = db.Exec(`PRAGMA foreign_keys = ON;`); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("could not enable foreign keys support: %+v", err)
		}

		if _, err = db.Exec(`PRAGMA locking_mode = SHARED;`); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("could not enable shared locking mode: %+v", err)
		}

		if err = createSqliteSchema(db); err != nil {
			return nil, fmt.Errorf("failed to create sqlite schema: %+v", err)
		}

	default:
		return nil, fmt.Errorf("unknown database engine: %s", storeUrl.Scheme)
	}

	s := &SQL{
		db:     db,
		engine: storeUrl.Scheme,
	}

	err = s.insertGenesisTransaction(logger)
	if err != nil {
		return nil, fmt.Errorf("failed to insert genesis transaction: %+v", err)
	}

	return s, nil
}

func (s *SQL) Close() error {
	return s.db.Close()
}

func createPostgresSchema(db *sql.DB) error {
	if _, err := db.Exec(`
      CREATE TABLE IF NOT EXISTS blocks (
	    id              BIGSERIAL PRIMARY KEY
		,parent_id	    BIGSERIAL REFERENCES blocks(id)
        ,version        INTEGER NOT NULL
	    ,hash           BYTEA NOT NULL
	    ,previous_hash  BYTEA NOT NULL
	    ,merkle_root    BYTEA NOT NULL
        ,block_time     BIGINT NOT NULL
        ,n_bits         BYTEA NOT NULL
        ,nonce          BIGINT NOT NULL
	    ,height         BIGINT NOT NULL
        ,chain_work     BYTEA NOT NULL
		,tx_count       BIGINT NOT NULL
		,subtree_count  BIGINT NOT NULL
        ,subtrees       BYTEA NOT NULL
        ,coinbase_tx    BYTEA NOT NULL
	    ,orphaned       BOOLEAN NOT NULL DEFAULT FALSE
        ,inserted_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	  );
	`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create blocks table - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_blocks_hash ON blocks (hash);`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create ux_blocks_hash index - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS pux_blocks_height ON blocks(height) WHERE orphaned = FALSE;`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create pux_blocks_height index - [%+v]", err)
	}

	if _, err := db.Exec(`
		CREATE OR REPLACE FUNCTION reverse_bytes_iter(bytes bytea, length int, midpoint int, index int)
		RETURNS bytea AS
		$$
		  SELECT CASE WHEN index >= midpoint THEN bytes ELSE
			reverse_bytes_iter(
			  set_byte(
				set_byte(bytes, index, get_byte(bytes, length-index)),
				length-index, get_byte(bytes, index)
			  ),
			  length, midpoint, index + 1
			)
		  END;
		$$ LANGUAGE SQL IMMUTABLE;
   `); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create block_transactions_map table - [%+v]", err)
	}

	if _, err := db.Exec(`
		CREATE OR REPLACE FUNCTION reverse_bytes(bytes bytea) RETURNS bytea AS
		'SELECT reverse_bytes_iter(bytes, octet_length(bytes)-1, octet_length(bytes)/2, 0)'
		LANGUAGE SQL IMMUTABLE;
	`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create block_transactions_map table - [%+v]", err)
	}

	// TODO check for the genesis block and add it if missing

	return nil
}

func createSqliteSchema(db *sql.DB) error {
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS blocks (
		 id           INTEGER PRIMARY KEY AUTOINCREMENT
		,parentId	  INTEGER REFERENCES blocks(id)
        ,version        INTEGER NOT NULL
	    ,hash           BLOB NOT NULL
	    ,previous_hash  BLOB NOT NULL
	    ,merkle_root    BLOB NOT NULL
        ,block_time		BIGINT NOT NULL
        ,n_bits         BLOB NOT NULL
        ,nonce          BIGINT NOT NULL
	    ,height         BIGINT NOT NULL
        ,chain_work     BLOB NOT NULL
		,tx_count       BIGINT NOT NULL
		,subtree_count  BIGINT NOT NULL
		,subtrees       BLOB NOT NULL
        ,coinbase_tx    BLOB NOT NULL
	    ,orphaned       BOOLEAN NOT NULL DEFAULT FALSE
        ,inserted_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	  );
	`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create blocks table - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_blocks_hash ON blocks (hash);`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create ux_blocks_hash index - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS pux_blocks_height ON blocks(height) WHERE orphaned = FALSE;`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create pux_blocks_height index - [%+v]", err)
	}

	// TODO check for the genesis block and add it if missing

	return nil
}

func (s *SQL) insertGenesisTransaction(logger utils.Logger) error {
	q := `
		SELECT
	     count(*)
		FROM blocks b
	`

	var err error
	var blockCount uint64
	if err = s.db.QueryRow(q).Scan(
		&blockCount,
	); err != nil {
		return err
	}

	if blockCount == 0 {
		// insert genesis block
		previousBlock := &chainhash.Hash{}
		merkleRoot, _ := chainhash.NewHashFromStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b")
		bits, _ := hex.DecodeString("1d00ffff")
		subtree, _ := chainhash.NewHashFromStr("4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b")
		coinbaseTx, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73ffffffff0100f2052a01000000434104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac00000000")

		txID := coinbaseTx.TxID()
		_ = txID

		genesisBlock := &model.Block{
			Header: &model.BlockHeader{
				Version:        1,
				Timestamp:      1231006505,
				Nonce:          2083236893,
				HashPrevBlock:  previousBlock,
				HashMerkleRoot: merkleRoot,
				Bits:           bits,
			},
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
			Subtrees:         []*chainhash.Hash{subtree},
		}

		// turn off foreign key checks when inserting the genesis block
		if s.engine == "sqlite" || s.engine == "sqlitememory" {
			_, _ = s.db.Exec("PRAGMA foreign_keys = OFF")
		} else if s.engine == "postgres" {
			_, _ = s.db.Exec("SET session_replication_role = 'replica'")
		}

		err = s.StoreBlock(context.Background(), genesisBlock)
		logger.Infof("genesis block inserted")

		// turn foreign key checks back on
		if s.engine == "sqlite" || s.engine == "sqlitememory" {
			_, _ = s.db.Exec("PRAGMA foreign_keys = ON")
		} else if s.engine == "postgres" {
			_, _ = s.db.Exec("SET session_replication_role = 'origin'")
		}
	}

	return err
}
