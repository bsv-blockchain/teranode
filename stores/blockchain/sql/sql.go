package sql

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/usql"
	"github.com/jellydator/ttlcache/v3"
	_ "github.com/lib/pq"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	_ "modernc.org/sqlite"
)

type SQL struct {
	db            *usql.DB
	engine        util.SQLEngine
	logger        ulogger.Logger
	responseCache *ttlcache.Cache[chainhash.Hash, any]
	cacheTTL      time.Duration
	blocksCache   blockchainCache
	chainParams   *chaincfg.Params
}

func New(logger ulogger.Logger, storeURL *url.URL) (*SQL, error) {
	logger = logger.New("bcsql")

	network, _ := gocore.Config().Get("network", "mainnet")

	params, err := chaincfg.GetChainParams(network)
	if err != nil {
		logger.Fatalf("Unknown network: %s", network)
	}

	db, err := util.InitSQLDB(logger, storeURL)
	if err != nil {
		return nil, errors.NewStorageError("failed to init sql db", err)
	}

	fmt.Println("Store URL: ", storeURL.Scheme)
	switch util.SQLEngine(storeURL.Scheme) {
	case util.Postgres:
		if err = createPostgresSchema(db); err != nil {
			return nil, errors.NewStorageError("failed to create postgres schema", err)
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
		cacheTTL:      2 * time.Minute,
		responseCache: ttlcache.New[chainhash.Hash, any](ttlcache.WithTTL[chainhash.Hash, any](2 * time.Minute)),
		blocksCache:   *NewBlockchainCache(),
		chainParams:   params,
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

func createPostgresSchema(db *usql.DB) error {
	if _, err := db.Exec(`
      CREATE TABLE IF NOT EXISTS state (
	    key            VARCHAR(32) PRIMARY KEY
	    ,data          BYTEA NOT NULL
        ,fsm_state	 VARCHAR(32) NULL
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
        ,peer_id	    VARCHAR(64) NOT NULL
    	,inserted_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	  );
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create blocks table", err)
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

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_height ON blocks (height);`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create idx_height index", err)
	}

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_parent_id ON blocks (parent_id);`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create idx_parent_id index", err)
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
	    ,fsm_state      VARCHAR(32) NULL
        ,inserted_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        ,updated_at     TEXT NULL
	  );
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create blocks table", err)
	}

	// Check if 'fsm_state' column exists, and add it if it doesn't
	if _, err := db.Exec(`
        ALTER TABLE state ADD COLUMN fsm_state VARCHAR(32) NULL;
    `); err != nil {
		// Ignore "duplicate column name" error (SQLite code 1)
		if !strings.Contains(err.Error(), "duplicate column name: fsm_state") {
			_ = db.Close()
			return errors.NewStorageError("could not alter state table to add fsm_state column", err)
		}
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
     	,peer_id	    VARCHAR(64) NOT NULL
        ,inserted_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	  );
	`); err != nil {
		_ = db.Close()
		return errors.NewStorageError("could not create blocks table", err)
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
	     count(*)
		FROM blocks b
	`

	var (
		err        error
		blockCount uint64
	)

	if err = s.db.QueryRow(q).Scan(
		&blockCount,
	); err != nil {
		return err
	}

	if blockCount == 0 {
		wireGenesisBlock := s.chainParams.GenesisBlock
		// wireGenesisBlock := chaincfg.MainNetParams.GenesisBlock

		genesisBlock, err := model.NewBlockFromMsgBlock(wireGenesisBlock)
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
	}

	return nil
}

type blockchainCache struct {
	enabled     bool
	headers     map[chainhash.Hash]*model.BlockHeader
	metas       map[chainhash.Hash]*model.BlockHeaderMeta
	existsCache map[chainhash.Hash]bool
	chain       []chainhash.Hash
	mutex       sync.RWMutex
	cacheSize   int
}

func NewBlockchainCache() *blockchainCache {
	cacheSize, _ := gocore.Config().GetInt("blockchain_store_cache_size", 200)
	return &blockchainCache{
		enabled:     gocore.Config().GetBool("blockchain_store_cache_enabled", true),
		headers:     make(map[chainhash.Hash]*model.BlockHeader, cacheSize),
		metas:       make(map[chainhash.Hash]*model.BlockHeaderMeta, cacheSize),
		existsCache: make(map[chainhash.Hash]bool, cacheSize),
		chain:       make([]chainhash.Hash, 0, cacheSize),
		cacheSize:   cacheSize,
		mutex:       sync.RWMutex{},
	}
}

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

	// height := block.Height
	// if len(c.chain) != 0 && height == 0 {
	// 	return false, errors.NewStorageError("block height is 0")
	// }

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

	for i, blockHeader := range blockHeaders {
		c.addBlockHeader(blockHeader, blockHeaderMetas[i])
	}
}
func (s *SQL) ResetResponseCache() {
	s.responseCache.DeleteAll()
}

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
	s.blocksCache.RebuildBlockchain(blockHeaders, blockHeaderMetas)

	return nil
}

func (c *blockchainCache) GetBestBlockHeader() (*model.BlockHeader, *model.BlockHeaderMeta) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if len(c.chain) == 0 {
		return nil, nil
	}

	hash := c.chain[len(c.chain)-1]

	return c.headers[hash], c.metas[hash]
}

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

	// nolint: gosec
	limit := int(numberOfHeaders)

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

func (c *blockchainCache) GetExists(blockHash chainhash.Hash) (bool, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	exists, ok := c.existsCache[blockHash]

	return exists, ok
}

func (c *blockchainCache) SetExists(blockHash chainhash.Hash, exists bool) {
	if !c.enabled {
		return
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.existsCache[blockHash] = exists
}
