package sql

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"

	"github.com/TAAL-GmbH/ubsv/stores/blockchain"
	"github.com/labstack/gommon/random"
	_ "github.com/lib/pq"
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

func New(storeUrl *url.URL) (blockchain.Store, error) {
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

	case "sqlite_memory":
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

	return &SQL{
		db:     db,
		engine: storeUrl.Scheme,
	}, nil
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
	    ,height         BIGINT NOT NULL
	    ,processed_at   TIMESTAMPTZ
		,tx_count       BIGINT NOT NULL
		,subtrees       BYTEA NOT NULL
        ,bits           BIGINT NOT NULL
        ,chain_work     BYTEA NOT NULL
	    ,forked         BOOLEAN NOT NULL DEFAULT FALSE
        ,inserted_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	  );
	`); err != nil {
		db.Close()
		return fmt.Errorf("could not create blocks table - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_blocks_hash ON blocks (hash);`); err != nil {
		db.Close()
		return fmt.Errorf("could not create ux_blocks_hash index - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS pux_blocks_height ON blocks(height) WHERE orphanedyn = FALSE;`); err != nil {
		db.Close()
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
		db.Close()
		return fmt.Errorf("could not create block_transactions_map table - [%+v]", err)
	}

	if _, err := db.Exec(`
		CREATE OR REPLACE FUNCTION reverse_bytes(bytes bytea) RETURNS bytea AS
		'SELECT reverse_bytes_iter(bytes, octet_length(bytes)-1, octet_length(bytes)/2, 0)'
		LANGUAGE SQL IMMUTABLE;
	`); err != nil {
		db.Close()
		return fmt.Errorf("could not create block_transactions_map table - [%+v]", err)
	}

	return nil
}

func createSqliteSchema(db *sql.DB) error {
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS blocks (
		 id           INTEGER PRIMARY KEY AUTOINCREMENT
		,parentId	  INTEGER REFERENCES blocks(id)
		,hash         BLOB NOT NULL
	    ,prevhash     BLOB NOT NULL
		,merkleroot   BLOB NOT NULL
	    ,height       BIGINT NOT NULL
	    ,processed_at TEXT
		,size		  BIGINT
		,tx_count	  BIGINT
		,subtrees     BLOB NOT NULL
		,orphanedyn   BOOLEAN NOT NULL DEFAULT FALSE
		,inserted_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	  );
	`); err != nil {
		db.Close()
		return fmt.Errorf("could not create blocks table - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_blocks_hash ON blocks (hash);`); err != nil {
		db.Close()
		return fmt.Errorf("could not create ux_blocks_hash index - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS pux_blocks_height ON blocks(height) WHERE orphanedyn = FALSE;`); err != nil {
		db.Close()
		return fmt.Errorf("could not create pux_blocks_height index - [%+v]", err)
	}

	return nil
}
