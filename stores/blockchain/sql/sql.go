// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines including PostgreSQL
// and SQLite.
//
// The implementation includes:
// - Efficient block and transaction storage and retrieval
// - Block header caching for performance optimization
// - Support for chain reorganization
// - Block validation status tracking
// - Chain state management
// - Database schema creation and migration
// - Performance optimizations for bulk imports
//
// The SQL store can be configured with different caching strategies and
// performance settings based on the deployment requirements.
package sql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/usql"
	_ "github.com/lib/pq"
	_ "modernc.org/sqlite"
)

// SQL implements the blockchain.Store interface using SQL database backends.
// It provides a complete implementation of blockchain data storage and retrieval
// operations with support for different SQL engines, caching mechanisms, and
// performance optimizations.
type SQL struct {
	// db is the underlying SQL database connection pool
	db *usql.DB
	// engine identifies which SQL engine is being used (PostgreSQL, SQLite, etc.)
	engine util.SQLEngine
	// logger provides structured logging capabilities
	logger ulogger.Logger
	// responseCache provides a time-based cache for frequently accessed query results
	// with automatic generation tracking to prevent stale results from being cached
	responseCache *GenerationalCache
	// cacheTTL defines the time-to-live duration for cached items
	cacheTTL time.Duration
	// chainParams contains the blockchain network parameters (mainnet, testnet, etc.)
	chainParams *chaincfg.Params
}

// New creates and initializes a new SQL blockchain store instance.
//
// This constructor function establishes a database connection based on the provided URL,
// initializes the appropriate schema for the selected SQL engine, and configures caching
// and performance settings. For PostgreSQL, it applies optimizations based on whether
// the store is being used for bulk imports (seeder mode).
//
// Parameters:
//   - logger: Logger instance for recording operational events and errors
//   - storeURL: URL containing connection parameters and engine selection
//   - tSettings: Application settings containing cache configuration and other parameters
//
// Returns:
//   - *SQL: Initialized SQL store instance ready for blockchain operations
//   - error: Any error encountered during initialization, wrapped as StorageError
func New(logger ulogger.Logger, storeURL *url.URL, tSettings *settings.Settings) (*SQL, error) {
	logger = logger.New("bcsql")

	db, err := util.InitSQLDB(logger, storeURL, tSettings, tSettings.BlockChain.PostgresPool)
	if err != nil {
		return nil, errors.NewStorageError("failed to init sql db", err)
	}

	switch util.SQLEngine(storeURL.Scheme) {
	case util.Postgres:
		const trueStr = "true"

		// offOrOn := "on"
		trueOrFalse := trueStr

		// The 'seeder' query parameter is used to optimize bulk imports by bypassing index creation.
		// Creating indexes during data insertion can significantly slow down the process, so we skip
		// index creation when 'seeder=true' is specified in the query parameters.
		if err = createPostgresSchema(db, storeURL.Query().Get("seeder") != trueStr); err != nil {
			return nil, errors.NewStorageError("failed to create postgres schema", err)
		}

		if storeURL.Query().Get("seeder") == trueStr {
			// offOrOn = "off"
			trueOrFalse = "false"

			logger.Infof("Aggressively optimizing Postgres for bulk import")
		}

		// _, err = db.Exec(fmt.Sprintf(`ALTER SYSTEM SET synchronous_commit = '%s'`, offOrOn))
		// if err != nil {
		// 	return nil, errors.NewStorageError("failed to set synchronous_commit "+offOrOn, err)
		// }

		// _, err = db.Exec(fmt.Sprintf(`ALTER SYSTEM SET fsync = '%s'`, offOrOn))
		// if err != nil {
		// 	return nil, errors.NewStorageError("failed to set fsync "+offOrOn, err)
		// }

		// _, err = db.Exec(fmt.Sprintf(`ALTER SYSTEM SET full_page_writes = '%s'`, offOrOn))
		// if err != nil {
		// 	return nil, errors.NewStorageError("failed to set full_page_writes "+offOrOn, err)
		// }

		// _, err = db.Exec(`SELECT pg_reload_conf()`)
		// if err != nil {
		// 	return nil, errors.NewStorageError("failed to reload postgres config", err)
		// }

		_, err = db.Exec(fmt.Sprintf(`ALTER TABLE blocks SET (autovacuum_enabled = '%s')`, trueOrFalse))
		if err != nil {
			return nil, errors.NewStorageError("failed to set autovacuum_enabled "+trueOrFalse, err)
		}

	case util.Sqlite, util.SqliteMemory:
		if err = createSqliteSchema(db); err != nil {
			return nil, errors.NewStorageError("failed to create sqlite schema", err)
		}

	default:
		return nil, errors.NewStorageError("unknown database engine: %s", storeURL.Scheme)
	}

	s := &SQL{
		db:            db,
		engine:        util.SQLEngine(storeURL.Scheme),
		logger:        logger,
		responseCache: NewGenerationalCache(),
		cacheTTL:      2 * time.Minute,
		chainParams:   tSettings.ChainCfgParams,
	}

	err = s.insertGenesisTransaction(logger)
	if err != nil {
		return nil, errors.NewStorageError("failed to insert genesis transaction", err)
	}

	return s, nil
}

func (s *SQL) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	// Check if the database connection is alive
	err := s.db.PingContext(ctx)
	if err != nil {
		return http.StatusFailedDependency, "Database connection error", err
	}

	return http.StatusOK, "OK", nil
}

func (s *SQL) GetDB() *usql.DB {
	return s.db
}

func (s *SQL) GetDBEngine() util.SQLEngine {
	return s.engine
}

func (s *SQL) Close() error {
	return s.db.Close()
}

func createPostgresSchema(db *usql.DB, withIndexes bool) error {
	if _, err := db.Exec(`
      CREATE TABLE IF NOT EXISTS state (
	    key            VARCHAR(32) PRIMARY KEY
	    ,data          BYTEA NOT NULL
        ,inserted_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        ,updated_at    TIMESTAMPTZ NULL
	  );
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create state table", err)
	}

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
		,size_in_bytes  BIGINT NOT NULL
		,subtree_count  BIGINT NOT NULL
        ,subtrees       BYTEA NOT NULL
        ,coinbase_tx    BYTEA NOT NULL
		,invalid	    BOOLEAN NOT NULL DEFAULT FALSE
        ,mined_set 	    BOOLEAN NOT NULL DEFAULT FALSE
        ,subtrees_set   BOOLEAN NOT NULL DEFAULT FALSE
    	,peer_id	    TEXT NOT NULL
    	,inserted_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
		,processed_at   TIMESTAMPTZ NULL
		,persisted_at   TIMESTAMPTZ NULL
	  );
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create blocks table", err)
	}

	// change the blocks table peer_id column to TEXT, if it is not already
	_, _ = db.Exec(`ALTER TABLE blocks ALTER COLUMN peer_id TYPE TEXT;`)

	// add the processed_at column to the blocks table if it does not exist
	err := db.QueryRow("SELECT column_name FROM information_schema.columns WHERE table_name='blocks' AND column_name='processed_at'").Scan(new(string))
	if err != nil {
		if err == sql.ErrNoRows {
			_, err := db.Exec(`ALTER TABLE blocks ADD COLUMN processed_at TIMESTAMPTZ NULL;`)
			if err != nil {
				_ = db.Close()
				return errors.NewStorageError("could not add processed_at column to blocks table", err)
			}
		} else {
			return errors.NewStorageError("could not check for processed_at column in blocks table", err)
		}
	}

	// add the persisted_at column to the blocks table if it does not exist
	err = db.QueryRow("SELECT column_name FROM information_schema.columns WHERE table_name='blocks' AND column_name='persisted_at'").Scan(new(string))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			_, err := db.Exec(`ALTER TABLE blocks ADD COLUMN persisted_at TIMESTAMPTZ NULL;`)
			if err != nil {
				_ = db.Close()
				return errors.NewStorageError("could not add persisted_at column to blocks table", err)
			}
		} else {
			return errors.NewStorageError("could not check for persisted_at column in blocks table", err)
		}
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_blocks_hash ON blocks (hash);`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create ux_blocks_hash index", err)
	}

	if withIndexes {
		if _, err := db.Exec(`DROP INDEX IF EXISTS pux_blocks_height;`); err != nil {
			_ = db.Close()
			return errors.NewStorageError("could not drop pux_blocks_height index", err)
		}

		if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_chain_work_id ON blocks (chain_work DESC, id ASC);`); err != nil {
			_ = db.Close()
			return errors.NewStorageError("could not create idx_chain_work_id index", err)
		}

		if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_chain_work_peer_id ON blocks (chain_work DESC, peer_id ASC, id ASC);`); err != nil {
			_ = db.Close()
			return errors.NewStorageError("could not create idx_chain_work_peer_id index", err)
		}

		if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_mined_set ON blocks (mined_set) WHERE mined_set = false;`); err != nil {
			_ = db.Close()
			return errors.NewStorageError("could not create idx_mined_set index", err)
		}

		if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_subtrees_set ON blocks (subtrees_set) WHERE subtrees_set = false;`); err != nil {
			_ = db.Close()
			return errors.NewStorageError("could not create idx_subtrees_set index", err)
		}

		if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_height ON blocks (height);`); err != nil {
			_ = db.Close()
			return errors.NewStorageError("could not create idx_height index", err)
		}

		if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_parent_id ON blocks (parent_id);`); err != nil {
			_ = db.Close()
			return errors.NewStorageError("could not create idx_parent_id index", err)
		}

		if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_persisted_at ON blocks (persisted_at) WHERE persisted_at IS NULL;`); err != nil {
			_ = db.Close()
			return errors.NewStorageError("could not create idx_persisted_at index", err)
		}
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
		return errors.NewStorageError("could not create block_transactions_map table", err)
	}

	if _, err := db.Exec(`
		CREATE OR REPLACE FUNCTION reverse_bytes(bytes bytea) RETURNS bytea AS
		'SELECT reverse_bytes_iter(bytes, octet_length(bytes)-1, octet_length(bytes)/2, 0)'
		LANGUAGE SQL IMMUTABLE;
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create block_transactions_map table", err)
	}

	return nil
}

func createSqliteSchema(db *usql.DB) error {
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS state (
		 key            VARCHAR(32) PRIMARY KEY
	    ,data           BLOB NOT NULL
        ,inserted_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        ,updated_at     TEXT NULL
	  );
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create blocks table", err)
	}

	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS blocks (
		 id           INTEGER PRIMARY KEY AUTOINCREMENT
		,parent_id	  INTEGER REFERENCES blocks(id)
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
		,size_in_bytes  BIGINT NOT NULL
		,subtree_count  BIGINT NOT NULL
		,subtrees       BLOB NOT NULL
        ,coinbase_tx    BLOB NOT NULL
		,invalid	    BOOLEAN NOT NULL DEFAULT FALSE
	    ,mined_set 	    BOOLEAN NOT NULL DEFAULT FALSE
        ,subtrees_set   BOOLEAN NOT NULL DEFAULT FALSE
     	,peer_id	    TEXT NOT NULL
        ,inserted_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		,processed_at   TEXT NULL
		,persisted_at   TEXT NULL
	  );
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create blocks table", err)
	}

	// add the processed_at column to the blocks table if it does not exist
	err := db.QueryRow("SELECT name FROM pragma_table_info('blocks') WHERE name='processed_at'").Scan(new(string))
	if err != nil {
		if err == sql.ErrNoRows {
			_, err := db.Exec(`ALTER TABLE blocks ADD COLUMN processed_at TEXT NULL;`)
			if err != nil {
				_ = db.Close()
				return errors.NewStorageError("could not add processed_at column to blocks table", err)
			}
		} else {
			return errors.NewStorageError("could not check for processed_at column in blocks table", err)
		}
	}

	// add the persisted_at column to the blocks table if it does not exist
	err = db.QueryRow("SELECT name FROM pragma_table_info('blocks') WHERE name='persisted_at'").Scan(new(string))
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			_, err := db.Exec(`ALTER TABLE blocks ADD COLUMN persisted_at TEXT NULL;`)
			if err != nil {
				_ = db.Close()
				return errors.NewStorageError("could not add persisted_at column to blocks table", err)
			}
		} else {
			return errors.NewStorageError("could not check for persisted_at column in blocks table", err)
		}
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_blocks_hash ON blocks (hash);`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create ux_blocks_hash index", err)
	}

	if _, err := db.Exec(`DROP INDEX IF EXISTS pux_blocks_height;`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not drop pux_blocks_height index", err)
	}

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_chain_work_id ON blocks (chain_work DESC, id ASC);`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create idx_chain_work_id index", err)
	}

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_chain_work_peer_id ON blocks (chain_work DESC, peer_id ASC, id ASC);`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create idx_chain_work_peer_id index", err)
	}

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_mined_set ON blocks (mined_set) WHERE mined_set = false;`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create idx_mined_set index", err)
	}

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_subtrees_set ON blocks (subtrees_set) WHERE subtrees_set = false;`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create idx_subtrees_set index", err)
	}

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_persisted_at ON blocks (persisted_at) WHERE persisted_at IS NULL;`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create idx_persisted_at index", err)
	}

	return nil
}

func (s *SQL) insertGenesisTransaction(logger ulogger.Logger) error {
	q := `
		SELECT
	     hash
		FROM blocks b
		WHERE b.height = 0
	`

	var (
		err  error
		hash []byte
	)

	if err = s.db.QueryRow(q).Scan(
		&hash,
	); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}

	if len(hash) == 0 {
		wireGenesisBlock := s.chainParams.GenesisBlock

		genesisBlock, err := model.NewBlockFromMsgBlock(wireGenesisBlock, nil)
		if err != nil {
			return err
		}

		// turn off foreign key checks when inserting the genesis block
		if s.engine == util.Sqlite || s.engine == util.SqliteMemory {
			_, _ = s.db.Exec("PRAGMA foreign_keys = OFF")
		} else if s.engine == util.Postgres {
			_, _ = s.db.Exec("SET session_replication_role = 'replica'")
		}

		_, _, err = s.StoreBlock(context.Background(), genesisBlock, "", options.WithID(0), options.WithMinedSet(true), options.WithSubtreesSet(true))
		if err != nil {
			return errors.NewStorageError("failed to insert genesis block", err)
		}

		logger.Infof("genesis block inserted")

		// turn foreign key checks back on
		if s.engine == util.Sqlite || s.engine == util.SqliteMemory {
			_, _ = s.db.Exec("PRAGMA foreign_keys = ON")
		} else if s.engine == util.Postgres {
			_, _ = s.db.Exec("SET session_replication_role = 'origin'")
		}
	} else if !bytes.Equal(hash, s.chainParams.GenesisHash[:]) {
		// Check the chainParams genesis block hash is the same as the one in the database
		return errors.NewConfigurationError("genesis block hash mismatch: bytes is %x, expected %x", hash, s.chainParams.GenesisHash[:])
	}

	return nil
}

// ResetResponseCache clears all entries from the response cache.
//
// This method is called when the blockchain state changes significantly, such as during
// chain reorganizations, block invalidations, or new block additions. Clearing the response
// cache ensures that subsequent queries will retrieve fresh data from the database rather
// than potentially stale cached data.
//
// In Teranode's high-throughput architecture, maintaining cache consistency is critical
// for ensuring accurate blockchain state across all components. This method provides a
// simple but effective mechanism for cache invalidation when the underlying data changes.
//
// The implementation uses the GenerationalCache's DeleteAll method to efficiently remove all
// cached entries in a single operation and increment the generation counter atomically.
//
// The generation counter increment prevents stale query results from being cached after
// invalidation. When a goroutine starts a query and captures the generation via GetWithToken,
// if the cache is reset during the query, the generation will have changed and Set() will
// skip caching the now-stale result. This solves race conditions where concurrent queries
// could overwrite fresh cache entries with stale data.
func (s *SQL) ResetResponseCache() {
	s.responseCache.DeleteAll()
}

// ExportBlockchainCSV exports the blockchain data to a CSV file for analysis or backup purposes.
// This method extracts key blockchain metadata including block hashes, heights, timestamps,
// and chain work values, writing them to the specified file in CSV format.
//
// The export includes all blocks in the blockchain and is useful for:
// - Data analysis and visualization of blockchain metrics
// - Creating backups of critical blockchain metadata
// - Migrating data to external systems or databases
// - Debugging and auditing blockchain state
//
// The operation is potentially resource-intensive for large blockchains and may take
// significant time to complete depending on the blockchain size.
//
// Parameters:
//   - ctx: Context for the operation, allowing for cancellation and timeouts
//   - filePath: The path where the CSV file should be created
//
// Returns:
//   - error: Any error encountered during the export process, including file creation
//     or database query errors
func (s *SQL) ExportBlockchainCSV(ctx context.Context, filePath string) error {
	f, err := os.Create(filePath)
	if err != nil {
		return errors.NewStorageError("could not create export file", err)
	}

	defer f.Close()
	w := csv.NewWriter(f)

	defer w.Flush()
	// header
	header := []string{"version", "hash", "previous_hash", "merkle_root", "block_time", "n_bits", "nonce", "height", "chain_work", "tx_count", "size_in_bytes", "subtree_count", "subtrees", "coinbase_tx", "invalid", "mined_set", "subtrees_set", "peer_id"}
	if err := w.Write(header); err != nil {
		return errors.NewStorageError("could not write CSV header", err)
	}

	rows, err := s.db.QueryContext(ctx, `SELECT version, hash, previous_hash, merkle_root, block_time, n_bits, nonce, height, chain_work, tx_count, size_in_bytes, subtree_count, subtrees, coinbase_tx, invalid, mined_set, subtrees_set, peer_id FROM blocks ORDER BY height ASC`)
	if err != nil {
		return errors.NewStorageError("could not query blocks", err)
	}

	defer rows.Close()

	for rows.Next() {
		var ver int

		var hash, prev, merkle, nBits, cw, subs, cb []byte

		var bt, nonce, height, txc, size, scnt int64

		var invalid, mined, sset bool

		var peer string

		if err := rows.Scan(&ver, &hash, &prev, &merkle, &bt, &nBits, &nonce, &height, &cw, &txc, &size, &scnt, &subs, &cb, &invalid, &mined, &sset, &peer); err != nil {
			return errors.NewStorageError("could not scan row", err)
		}

		rec := []string{
			strconv.Itoa(ver),
			hex.EncodeToString(hash),
			hex.EncodeToString(prev),
			hex.EncodeToString(merkle),
			strconv.FormatInt(bt, 10),
			hex.EncodeToString(nBits),
			strconv.FormatInt(nonce, 10),
			strconv.FormatInt(height, 10),
			hex.EncodeToString(cw),
			strconv.FormatInt(txc, 10),
			strconv.FormatInt(size, 10),
			strconv.FormatInt(scnt, 10),
			hex.EncodeToString(subs),
			hex.EncodeToString(cb),
			strconv.FormatBool(invalid),
			strconv.FormatBool(mined),
			strconv.FormatBool(sset),
			peer,
		}

		if err := w.Write(rec); err != nil {
			return errors.NewStorageError("could not write record", err)
		}
	}

	return rows.Err()
}

func (s *SQL) ImportBlockchainCSV(ctx context.Context, filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return errors.NewStorageError("could not open import file", err)
	}

	defer f.Close()
	r := csv.NewReader(f)

	// read and validate CSV
	if _, err := r.Read(); err != nil {
		return errors.NewStorageError("could not read CSV header", err)
	}

	records, err := r.ReadAll()
	if err != nil {
		return errors.NewStorageError("could not read CSV records", err)
	}
	// verify genesis block hash matches settings
	expected := hex.EncodeToString(s.chainParams.GenesisHash.CloneBytes())
	if records[0][1] != expected {
		return errors.NewProcessingError("import aborted: genesis block hash mismatch; got %s, want %s", records[0][1], expected)
	}
	// ensure there are blocks beyond genesis
	if len(records) <= 1 {
		return errors.NewProcessingError("import aborted: CSV contains only genesis block")
	}
	// iterate records for import
	for _, rec := range records {
		ver, _ := strconv.Atoi(rec[0])
		hash, _ := hex.DecodeString(rec[1])
		prev, _ := hex.DecodeString(rec[2])
		merkle, _ := hex.DecodeString(rec[3])
		bt, _ := strconv.ParseInt(rec[4], 10, 64)
		nBits, _ := hex.DecodeString(rec[5])
		nonce, _ := strconv.ParseInt(rec[6], 10, 64)

		height, _ := strconv.ParseInt(rec[7], 10, 64)

		cw, _ := hex.DecodeString(rec[8])
		txc, _ := strconv.ParseInt(rec[9], 10, 64)
		size, _ := strconv.ParseInt(rec[10], 10, 64)
		scnt, _ := strconv.ParseInt(rec[11], 10, 64)
		subs, _ := hex.DecodeString(rec[12])
		cb, _ := hex.DecodeString(rec[13])
		invalid, _ := strconv.ParseBool(rec[14])
		mined, _ := strconv.ParseBool(rec[15])
		sset, _ := strconv.ParseBool(rec[16])
		peer := rec[17]

		var pid sql.NullInt64
		if height != 0 {
			err = s.db.QueryRowContext(ctx, `SELECT id FROM blocks WHERE hash=$1`, prev).Scan(&pid)
			if err != nil {
				return errors.NewStorageError("could not lookup parent", err)
			}
		}

		// handle genesis record: insert only if missing
		if height == 0 {
			var exists bool
			if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM blocks WHERE height=0)`).Scan(&exists); err != nil {
				return errors.NewStorageError("could not check genesis existence", err)
			}
			if !exists {
				_, err = s.db.ExecContext(ctx, `INSERT INTO blocks(parent_id,version,hash,previous_hash,merkle_root,block_time,n_bits,nonce,height,chain_work,tx_count,size_in_bytes,subtree_count,subtrees,coinbase_tx,invalid,mined_set,subtrees_set,peer_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)`, pid, ver, hash, prev, merkle, bt, nBits, nonce, height, cw, txc, size, scnt, subs, cb, invalid, mined, sset, peer)
				if err != nil {
					return errors.NewStorageError("could not insert genesis block", err)
				}
			}
			continue
		}

		_, err = s.db.ExecContext(ctx, `INSERT INTO blocks(parent_id,version,hash,previous_hash,merkle_root,block_time,n_bits,nonce,height,chain_work,tx_count,size_in_bytes,subtree_count,subtrees,coinbase_tx,invalid,mined_set,subtrees_set,peer_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)`, pid, ver, hash, prev, merkle, bt, nBits, nonce, height, cw, txc, size, scnt, subs, cb, invalid, mined, sset, peer)
		if err != nil {
			return errors.NewStorageError("could not insert block", err)
		}
	}

	return nil
}
