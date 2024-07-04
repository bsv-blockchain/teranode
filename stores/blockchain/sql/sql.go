package sql

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/usql"
	"github.com/jellydator/ttlcache/v3"
	_ "github.com/lib/pq"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	_ "modernc.org/sqlite"
)

type SQL struct {
	db            *usql.DB
	engine        util.SQLEngine
	logger        ulogger.Logger
	responseCache *ttlcache.Cache[chainhash.Hash, any]
	cacheTTL      time.Duration
	blocksCache   blockchainCache
}

var ()

func New(logger ulogger.Logger, storeUrl *url.URL) (*SQL, error) {
	logger = logger.New("bcsql")

	db, err := util.InitSQLDB(logger, storeUrl)
	if err != nil {
		return nil, fmt.Errorf("failed to init sql db: %+v", err)
	}

	switch util.SQLEngine(storeUrl.Scheme) {
	case util.Postgres:
		if err = createPostgresSchema(db); err != nil {
			return nil, fmt.Errorf("failed to create postgres schema: %+v", err)
		}

	case util.Sqlite, util.SqliteMemory:
		if err = createSqliteSchema(db); err != nil {
			return nil, fmt.Errorf("failed to create sqlite schema: %+v", err)
		}

	default:
		return nil, fmt.Errorf("unknown database engine: %s", storeUrl.Scheme)
	}

	s := &SQL{
		db:            db,
		engine:        util.SQLEngine(storeUrl.Scheme),
		logger:        logger,
		cacheTTL:      2 * time.Minute,
		responseCache: ttlcache.New[chainhash.Hash, any](ttlcache.WithTTL[chainhash.Hash, any](2 * time.Minute)),
		blocksCache:   *NewBlockchainCache(),
	}

	err = s.insertGenesisTransaction(logger)
	if err != nil {
		return nil, fmt.Errorf("failed to insert genesis transaction: %+v", err)
	}

	return s, nil
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
        ,inserted_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        ,updated_at    TIMESTAMPTZ NULL
	  );
	`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create state table - [%+v]", err)
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
		return fmt.Errorf("could not create blocks table - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_blocks_hash ON blocks (hash);`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create ux_blocks_hash index - [%+v]", err)
	}

	if _, err := db.Exec(`DROP INDEX IF EXISTS pux_blocks_height;`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not drop pux_blocks_height index - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_chain_work_id ON blocks (chain_work DESC, id ASC);`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create idx_chain_work_id index - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_chain_work_peer_id ON blocks (chain_work DESC, peer_id ASC, id ASC);`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create idx_chain_work_peer_id index - [%+v]", err)
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
		return fmt.Errorf("could not create blocks table - [%+v]", err)
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
		return fmt.Errorf("could not create blocks table - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_blocks_hash ON blocks (hash);`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create ux_blocks_hash index - [%+v]", err)
	}

	if _, err := db.Exec(`DROP INDEX IF EXISTS pux_blocks_height;`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not drop pux_blocks_height index - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_chain_work_id ON blocks (chain_work DESC, id ASC);`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create idx_chain_work_id index - [%+v]", err)
	}

	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_chain_work_peer_id ON blocks (chain_work DESC, peer_id ASC, id ASC);`); err != nil {
		_ = db.Close()
		return fmt.Errorf("could not create idx_chain_work_peer_id index - [%+v]", err)
	}

	return nil
}

func (s *SQL) insertGenesisTransaction(logger ulogger.Logger) error {
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
		coinbaseTx, _ := bt.NewTxFromString("01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff4d04ffff001d0104455468652054696d65732030332f4a616e2f32303039204368616e63656c6c6f72206f6e206272696e6b206f66207365636f6e64206261696c6f757420666f722062616e6b73ffffffff0100f2052a01000000434104678afdb0fe5548271967f1a67130b7105cd6a828e03909a67962e0ea1f61deb649f6bc3f4cef38c4f35504e51ec112de5c384df7ba0b8d578a4c702b6bf11d5fac00000000")
		txID := coinbaseTx.TxID()
		_ = txID

		genesisBlock := &model.Block{
			Header:           model.GenesisBlockHeader,
			CoinbaseTx:       coinbaseTx,
			TransactionCount: 1,
			SizeInBytes:      285,
			Subtrees:         []*chainhash.Hash{},
		}

		// turn off foreign key checks when inserting the genesis block
		if s.engine == util.Sqlite || s.engine == util.SqliteMemory {
			_, _ = s.db.Exec("PRAGMA foreign_keys = OFF")
		} else if s.engine == util.Postgres {
			_, _ = s.db.Exec("SET session_replication_role = 'replica'")
		}

		_, err = s.StoreBlock(context.Background(), genesisBlock, "")
		if err != nil {
			return fmt.Errorf("failed to insert genesis block: %+v", err)
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
	headers     map[chainhash.Hash]*model.BlockHeader
	metas       map[chainhash.Hash]*model.BlockHeaderMeta
	existsCache map[chainhash.Hash]bool
	chain       []chainhash.Hash
	mutex       sync.RWMutex
}

func NewBlockchainCache() *blockchainCache {
	return &blockchainCache{
		headers:     make(map[chainhash.Hash]*model.BlockHeader, 100),
		metas:       make(map[chainhash.Hash]*model.BlockHeaderMeta, 100),
		existsCache: make(map[chainhash.Hash]bool, 100),
		chain:       make([]chainhash.Hash, 0, 100),
		mutex:       sync.RWMutex{},
	}
}

func (c *blockchainCache) AddBlockHeader(blockHeader *model.BlockHeader, blockHeaderMeta *model.BlockHeaderMeta) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	return c.addBlockHeader(blockHeader, blockHeaderMeta)
}

func (c *blockchainCache) addBlockHeader(blockHeader *model.BlockHeader, blockHeaderMeta *model.BlockHeaderMeta) bool {
	const added = true
	const notAdded = false

	// height := block.Height
	// if len(c.chain) != 0 && height == 0 {
	// 	return false, fmt.Errorf("block height is 0")
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
		if len(c.chain) >= 200 {
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
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.headers = make(map[chainhash.Hash]*model.BlockHeader, 100)
	c.metas = make(map[chainhash.Hash]*model.BlockHeaderMeta, 100)
	c.existsCache = make(map[chainhash.Hash]bool, 100)
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

	bestBlockHeader, _, err := s.GetBestBlockHeader(ctx)
	if err != nil {
		return err
	}

	var blockHeaders []*model.BlockHeader
	var blockHeaderMetas []*model.BlockHeaderMeta
	blockHeaders, blockHeaderMetas, err = s.GetBlockHeaders(ctx, bestBlockHeader.Hash(), 100)
	if err != nil {
		return err
	}

	s.blocksCache.RebuildBlockchain(blockHeaders, blockHeaderMetas)

	return nil

}

func (c *blockchainCache) GetBestBlockHeader() (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if len(c.chain) == 0 {
		return nil, nil, nil
	}

	hash := c.chain[len(c.chain)-1]

	return c.headers[hash], c.metas[hash], nil
}

func (c *blockchainCache) GetBlockHeader(hash chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	blockHeader, ok := c.headers[hash]
	if !ok {
		return nil, nil, nil
	}

	return blockHeader, c.metas[hash], nil
}

func (c *blockchainCache) GetBlockHeadersFromHeight(height uint32, limit int) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if len(c.chain) == 0 {
		return nil, nil, nil
	}

	for i, hash := range c.chain {
		meta := c.metas[hash]
		if meta.Height == height {
			if i+limit > len(c.chain) {
				// can't get all the headers requested, so return nothing
				return nil, nil, nil
			}

			headers := make([]*model.BlockHeader, 0, limit)
			metas := make([]*model.BlockHeaderMeta, 0, limit)
			for j := i; j < i+limit; j++ {
				headers = append(headers, c.headers[c.chain[j]])
				metas = append(metas, c.metas[c.chain[j]])
			}

			return headers, metas, nil
		}
	}

	return nil, nil, nil
}

func (c *blockchainCache) GetBlockHeaders(blockHashFrom *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if len(c.chain) == 0 {
		return nil, nil, nil
	}

	limit := int(numberOfHeaders)
	for i, hash := range c.chain {
		if hash == *blockHashFrom {
			if i < limit {
				// can't get all the headers requested, so return nothing
				return nil, nil, nil
			}

			headers := make([]*model.BlockHeader, 0, limit)
			metas := make([]*model.BlockHeaderMeta, 0, limit)
			for j := i; j > i-limit; j-- {
				headers = append(headers, c.headers[c.chain[j]])
				metas = append(metas, c.metas[c.chain[j]])
			}

			return headers, metas, nil
		}
	}

	return nil, nil, nil
}

func (c *blockchainCache) GetExists(blockHash chainhash.Hash) (bool, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	exists, ok := c.existsCache[blockHash]
	return exists, ok
}

func (c *blockchainCache) SetExists(blockHash chainhash.Hash, exists bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.existsCache[blockHash] = exists
}
