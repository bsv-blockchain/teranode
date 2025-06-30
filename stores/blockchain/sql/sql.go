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
	"sync"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/pkg/go-chaincfg"
	"github.com/bitcoin-sv/teranode/pkg/go-safe-conversion"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/usql"
	"github.com/jellydator/ttlcache/v3"
	_ "github.com/lib/pq"
	"github.com/libsv/go-bt/v2/chainhash"
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
	responseCache *ttlcache.Cache[chainhash.Hash, any]
	// cacheTTL defines the time-to-live duration for cached items
	cacheTTL time.Duration
	// blocksCache provides specialized caching for block headers and metadata
	blocksCache blockchainCache
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

	db, err := util.InitSQLDB(logger, storeURL, tSettings)
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
		responseCache: ttlcache.New[chainhash.Hash, any](ttlcache.WithTTL[chainhash.Hash, any](2 * time.Minute)),
		cacheTTL:      2 * time.Minute,
		blocksCache:   *NewBlockchainCache(tSettings),
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

		_, _, err = s.StoreBlock(context.Background(), genesisBlock, "")
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

// blockchainCache provides an in-memory caching mechanism for frequently accessed blockchain data.
// It maintains maps of block headers, metadata, and existence flags to reduce database queries.
// The cache is thread-safe with a read-write mutex protecting all operations.
//
// This cache significantly improves performance for header-related queries which are common
// in blockchain operations, especially during chain synchronization and block validation.
type blockchainCache struct {
	// enabled determines whether the cache is active
	enabled bool
	// headers maps block hashes to their corresponding headers
	headers map[chainhash.Hash]*model.BlockHeader
	// metas maps block hashes to their metadata (height, status, etc.)
	metas map[chainhash.Hash]*model.BlockHeaderMeta
	// existsCache maps block hashes to boolean existence flags
	existsCache map[chainhash.Hash]bool
	// chain maintains an ordered list of block hashes representing the current best chain
	chain []chainhash.Hash
	// mutex provides thread-safe access to all cache data
	mutex sync.RWMutex
	// cacheSize defines the maximum number of entries to keep in the cache
	cacheSize int
}

// NewBlockchainCache creates and initializes a new blockchainCache instance.
//
// This function configures the cache based on application settings, determining whether
// the cache should be enabled and what its size should be. When enabled, the cache
// significantly improves performance for frequently accessed blockchain data by reducing
// database queries.
//
// Parameters:
//   - tSettings: Application settings containing cache configuration parameters
//
// Returns:
//   - *blockchainCache: A new, initialized blockchain cache instance
func NewBlockchainCache(tSettings *settings.Settings) *blockchainCache {
	cacheSize := tSettings.Block.StoreCacheSize

	return &blockchainCache{
		enabled:     cacheSize > 0,
		headers:     make(map[chainhash.Hash]*model.BlockHeader, cacheSize),
		metas:       make(map[chainhash.Hash]*model.BlockHeaderMeta, cacheSize),
		existsCache: make(map[chainhash.Hash]bool, cacheSize),
		chain:       make([]chainhash.Hash, 0, cacheSize),
		cacheSize:   cacheSize,
		mutex:       sync.RWMutex{},
	}
}

// AddBlockHeader adds a new block header to the cache and updates the chain state.
// This method serves as a public wrapper around the internal addBlockHeader method,
// providing thread-safety through a write lock.
//
// The method ensures that the blockchain cache remains consistent by adding both
// the header and its metadata, and potentially updating the best chain representation.
//
// Parameters:
//   - blockHeader: The block header to add to the cache
//   - blockHeaderMeta: Metadata associated with the block header, including height and chain work
//
// Returns:
//   - bool: True if the operation was successful, false otherwise (e.g., if the cache is disabled)
func (c *blockchainCache) AddBlockHeader(blockHeader *model.BlockHeader, blockHeaderMeta *model.BlockHeaderMeta) bool {
	if !c.enabled {
		return true
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.addBlockHeader(blockHeader, blockHeaderMeta)
}

func (c *blockchainCache) addBlockHeader(blockHeader *model.BlockHeader, blockHeaderMeta *model.BlockHeaderMeta) bool {
	const (
		added    = true
		notAdded = false
	)

	c.headers[*blockHeader.Hash()] = blockHeader
	c.metas[*blockHeader.Hash()] = blockHeaderMeta
	c.existsCache[*blockHeader.Hash()] = true

	if len(c.chain) == 0 {
		c.chain = append(c.chain, *blockHeader.Hash())
		return added
	}

	bestBlockHash := c.chain[len(c.chain)-1]
	if *blockHeader.HashPrevBlock == bestBlockHash {
		c.chain = append(c.chain, *blockHeader.Hash())

		// only keep last 200 blocks in cache
		if len(c.chain) >= c.cacheSize {
			oldestHash := c.chain[0]
			delete(c.headers, oldestHash)
			delete(c.metas, oldestHash)
			c.chain = c.chain[1:]
		}

		return added
	}

	return notAdded
}

// RebuildBlockchain reconstructs the entire blockchain cache from the provided headers and metadata.
// This method is typically called after a database reload or when the cache needs to be
// completely refreshed to ensure consistency with the underlying storage.
//
// The method performs a complete reset of the cache and then rebuilds it by:
// - Clearing all existing cached data (headers, metadata, existence flags, chain)
// - Adding each header and its metadata to the cache in sequence
// - Reconstructing the best chain representation
//
// This operation is protected by a write lock to ensure thread safety during the rebuild process.
//
// Parameters:
//   - blockHeaders: Slice of block headers to add to the cache
//   - blockHeaderMetas: Slice of metadata corresponding to the block headers
//
// Note: The slices must be of equal length and the elements at the same index must correspond
// to the same block. The method silently returns if the cache is disabled.
func (c *blockchainCache) RebuildBlockchain(blockHeaders []*model.BlockHeader, blockHeaderMetas []*model.BlockHeaderMeta) {
	if !c.enabled {
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.headers = make(map[chainhash.Hash]*model.BlockHeader, c.cacheSize)
	c.metas = make(map[chainhash.Hash]*model.BlockHeaderMeta, c.cacheSize)
	c.existsCache = make(map[chainhash.Hash]bool, c.cacheSize)
	c.chain = c.chain[:0]

	if blockHeaders == nil {
		return
	}

	// Iterate backwards assuming headers are sorted newest first (descending height)
	for i := len(blockHeaders) - 1; i >= 0; i-- {
		c.addBlockHeader(blockHeaders[i], blockHeaderMetas[i])
	}
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
// The implementation uses the ttlcache's DeleteAll method to efficiently remove all
// cached entries in a single operation. This is more efficient than selectively
// invalidating entries, especially during major state changes where most cached data
// would need to be refreshed anyway.
func (s *SQL) ResetResponseCache() {
	s.responseCache.DeleteAll()
}

// ResetBlocksCache refreshes the blocks cache with the most recent blockchain data.
//
// This method performs a complete reset and rebuild of the blocks cache, ensuring that
// it contains the most up-to-date blockchain state. It is a critical operation in
// maintaining blockchain consensus integrity, particularly after events that alter
// the blockchain's structure or state.
//
// In Bitcoin's design, the blockchain can undergo reorganizations when a competing chain
// with greater cumulative proof-of-work is discovered. During these events, the node must
// quickly adapt to the new canonical chain state. This method ensures that the in-memory
// representation of the blockchain (the cache) accurately reflects the current state of
// the database after such events.
//
// The implementation follows a carefully designed process to maintain consistency:
//
// 1. Clear the existing cache by calling RebuildBlockchain with nil parameters
//   - This immediately invalidates all cached data, preventing stale data access
//   - The operation is atomic, ensuring no partial cache state exists during transition
//
// 2. Query the database for the current best block header to determine the chain tip
//   - Uses the GetBestBlockHeader database method which identifies the tip by highest chainwork
//   - This ensures the cache rebuilds from the current consensus-valid chain tip
//
// 3. Retrieve the most recent block headers up to the cache size limit
//   - Fetches headers in reverse order from the tip to populate the most relevant portion
//     of the blockchain (the most recent blocks)
//   - Optimizes for the common access pattern where recent blocks are queried most frequently
//   - Limits the query to the configured cache size to prevent memory exhaustion
//
// 4. Rebuild the cache with these fresh headers
//   - Repopulates all cache maps (headers, metadata, existence flags)
//   - Reconstructs the in-memory chain representation for efficient traversal
//
// This cache reset operation is invoked in several critical scenarios:
//
// - After blockchain reorganizations (via InvalidateBlock)
// - After new block storage (via StoreBlock)
// - When inconsistencies are detected between cache and database state
// - During node startup to initialize the cache with current blockchain state
//
// In Teranode's high-throughput architecture for BSV, maintaining an accurate and efficient
// blocks cache is essential for performance at scale. The cache significantly reduces database
// load during intensive operations like block validation, transaction verification, and
// peer synchronization, which are particularly demanding in BSV's unlimited scaling model.
//
// Parameters:
//   - ctx: Context for the database operations, allowing for cancellation and timeouts
//
// Returns:
//   - error: Any error encountered during the cache reset process, specifically:
//   - StorageError when database operations fail during best block header retrieval
//   - ProcessingError when header reconstruction or cache population fails
func (s *SQL) ResetBlocksCache(ctx context.Context) error {
	s.logger.Warnf("Reset")
	defer s.logger.Warnf("Reset completed")

	// empty the cache
	s.blocksCache.RebuildBlockchain(nil, nil)

	bestBlockHeader, _, err := s.GetBestBlockHeader(ctx)
	if err != nil {
		return err
	}

	var (
		blockHeaders     []*model.BlockHeader
		blockHeaderMetas []*model.BlockHeaderMeta
	)

	blockHeaders, blockHeaderMetas, err = s.GetBlockHeaders(ctx, bestBlockHeader.Hash(), uint64(s.blocksCache.cacheSize))
	if err != nil {
		return err
	}

	// re-fill the cache with data from the db
	// these headers are in descending order of height (newest first)
	s.blocksCache.RebuildBlockchain(blockHeaders, blockHeaderMetas)

	return nil
}

// GetBestBlockHeader retrieves the header of the block at the tip of the best chain.
// The best chain is determined by the highest cumulative work, which typically corresponds
// to the longest valid chain in the blockchain.
//
// This method is critical for blockchain consensus as it identifies the current chain tip
// that new blocks should build upon. It's used for:
// - Determining the next block's parent hash
// - Calculating the current blockchain height
// - Validating incoming blocks against the current best chain
//
// The method uses a read lock to ensure thread safety while accessing the cache data.
// If the cache is disabled or empty, it returns nil values, indicating that the caller
// should fall back to database queries.
//
// Returns:
//   - *model.BlockHeader: The header of the best block in the chain, or nil if unavailable
//   - *model.BlockHeaderMeta: Metadata for the best block including height and work, or nil if unavailable
func (c *blockchainCache) GetBestBlockHeader() (*model.BlockHeader, *model.BlockHeaderMeta) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if len(c.chain) == 0 {
		return nil, nil
	}

	hash := c.chain[len(c.chain)-1]

	return c.headers[hash], c.metas[hash]
}

// GetBlockHeader retrieves a block header and its metadata from the cache by its hash.
// This method provides fast access to block headers without requiring database queries,
// significantly improving performance for frequently accessed headers.
//
// The method is commonly used for:
// - Validating block references (previous block hash)
// - Checking block existence before processing
// - Retrieving block metadata like height and chain work
// - Building block chains and verifying continuity
//
// The method uses a read lock to ensure thread safety while accessing the cache data.
// If the requested header is not found in the cache or if the cache is disabled,
// it returns nil values, indicating that the caller should fall back to database queries.
//
// Parameters:
//   - hash: The unique hash identifier of the block header to retrieve
//
// Returns:
//   - *model.BlockHeader: The requested block header, or nil if not found in cache
//   - *model.BlockHeaderMeta: Metadata for the block including height and work, or nil if not found
func (c *blockchainCache) GetBlockHeader(hash chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	blockHeader, ok := c.headers[hash]
	if !ok {
		return nil, nil
	}

	return blockHeader, c.metas[hash]
}

func (c *blockchainCache) GetBlockHeadersFromHeight(height uint32, limit int) ([]*model.BlockHeader, []*model.BlockHeaderMeta) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if len(c.chain) == 0 {
		return nil, nil
	}

	for i, hash := range c.chain {
		meta := c.metas[hash]
		if meta.Height == height {
			if i+limit > len(c.chain) {
				// can't get all the headers requested, so return nothing
				return nil, nil
			}

			headers := make([]*model.BlockHeader, 0, limit)
			metas := make([]*model.BlockHeaderMeta, 0, limit)

			for j := i; j < i+limit; j++ {
				headers = append(headers, c.headers[c.chain[j]])
				metas = append(metas, c.metas[c.chain[j]])
			}

			return headers, metas
		}
	}

	return nil, nil
}

func (c *blockchainCache) GetBlockHeadersByHeight(startHeight, endHeight uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta) {
	// Not implemented
	return nil, nil
}

func (c *blockchainCache) GetBlockHeaders(blockHashFrom *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if len(c.chain) == 0 {
		return nil, nil
	}

	limit, err := safe.Uint64ToInt(numberOfHeaders)
	if err != nil {
		return nil, nil
	}

	for i, hash := range c.chain {
		if hash == *blockHashFrom {
			if i < limit {
				// can't get all the headers requested, so return nothing
				return nil, nil
			}

			headers := make([]*model.BlockHeader, 0, limit)
			metas := make([]*model.BlockHeaderMeta, 0, limit)

			for j := i; j > i-limit; j-- {
				headers = append(headers, c.headers[c.chain[j]])
				metas = append(metas, c.metas[c.chain[j]])
			}

			return headers, metas
		}
	}

	return nil, nil
}

// GetExists checks if a block with the given hash exists in the blockchain.
// This method provides a lightweight way to verify block existence using a dedicated
// existence cache, optimized for frequent existence checks.
//
// The existence cache is separate from the header cache and is specifically designed
// to handle high-volume existence queries efficiently. This approach reduces memory
// overhead and improves performance for common blockchain operations that only need
// to know if a block exists without requiring its full header data.
//
// Parameters:
//   - blockHash: The unique hash identifier of the block to check
//
// Returns:
//   - bool: True if the block exists in the blockchain, false if it doesn't
//   - bool: True if the existence information was found in the cache (cache hit),
//     false if the information wasn't cached (cache miss)
func (c *blockchainCache) GetExists(blockHash chainhash.Hash) (bool, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	exists, ok := c.existsCache[blockHash]

	return exists, ok
}

// SetExists updates the existence cache with information about whether a block exists.
// This method is called after database operations that determine block existence,
// allowing subsequent existence checks to be served from the cache without database queries.
//
// In blockchain systems, block existence checks are extremely frequent operations that occur
// during various critical processes:
//
// - Block validation: Verifying that referenced previous blocks exist
// - Transaction validation: Checking if blocks containing transactions exist
// - Chain synchronization: Determining which blocks to request from peers
// - Reorganization handling: Identifying which blocks remain valid after reorgs
// - Double-spend prevention: Verifying block inclusion for transactions
//
// The implementation uses a dedicated existence cache separate from the header cache for
// several blockchain-specific optimizations:
//
// 1. Memory Efficiency: Stores only boolean existence flags rather than full headers
//   - Critical for BSV's unlimited scaling approach where the blockchain can grow very large
//   - Allows caching existence information for many more blocks than would be possible
//     with full header caching
//
// 2. Performance Optimization: Provides O(1) lookup time for existence checks
//   - Eliminates database queries for frequently checked blocks
//   - Particularly valuable during block propagation and validation where many
//     existence checks occur in rapid succession
//
// 3. Consensus Protection: Reduces database load during intensive operations
//   - Helps maintain node responsiveness during reorganizations or attacks
//   - Prevents database contention when multiple components need existence information
//
// The method is thread-safe, using the cache's read-write mutex to protect concurrent
// access to the existence map. It silently returns if the cache is disabled, ensuring
// graceful degradation when caching is turned off.
//
// Parameters:
//   - blockHash: The unique hash identifier of the block to update in the cache
//   - exists: Boolean flag indicating whether the block exists in the blockchain
func (c *blockchainCache) SetExists(blockHash chainhash.Hash, exists bool) {
	if !c.enabled {
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.existsCache[blockHash] = exists
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
