package coinbase

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blobserver"
	"github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type processBlockFound struct {
	hash    *chainhash.Hash
	baseURL string
}

type processBlockCatchup struct {
	block   *model.Block
	baseURL string
}

type Coinbase struct {
	db               *sql.DB
	engine           util.SQLEngine
	store            blockchain.Store
	blobServerClient *blobserver.Client
	running          bool
	blockFoundCh     chan processBlockFound
	catchupCh        chan processBlockCatchup
	logger           utils.Logger
	mu               sync.Mutex
}

// NewCoinbase builds on top of the blockchain store to provide a coinbase tracker
// Only SQL databases are supported
func NewCoinbase(logger utils.Logger, store blockchain.Store) (*Coinbase, error) {
	engine := store.GetDBEngine()
	if engine != util.Postgres && engine != util.Sqlite && engine != util.SqliteMemory {
		return nil, fmt.Errorf("unsupported database engine: %s", engine)
	}

	c := &Coinbase{
		store:        store,
		db:           store.GetDB(),
		engine:       engine,
		blockFoundCh: make(chan processBlockFound, 100),
		catchupCh:    make(chan processBlockCatchup),
		logger:       logger,
	}

	return c, nil
}

func (c *Coinbase) Init(ctx context.Context) (err error) {
	if err = c.createTables(ctx); err != nil {
		return fmt.Errorf("failed to create coinbase tables: %s", err)
	}

	blobServerAddr, ok := gocore.Config().Get("coinbase_blobserverGrpcAddress")
	if !ok {
		blobServerAddr, ok = gocore.Config().Get("blobserver_grpcAddress")
		if !ok {
			return errors.New("no blobserver_grpcAddress setting found")
		}
	}

	c.blobServerClient, err = blobserver.NewClient(ctx, c.logger, blobServerAddr)
	if err != nil {
		return fmt.Errorf("failed to create blobserver client: %s", err)
	}

	// process blocks found from channel
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case catchup := <-c.catchupCh:
				{
					if err = c.catchup(ctx, catchup.block, catchup.baseURL); err != nil {
						c.logger.Errorf("failed to catchup from [%s] [%v]", catchup.block.Hash().String(), err)
					}
				}
			case block := <-c.blockFoundCh:
				{
					if _, err = c.processBlock(ctx, block.hash, block.baseURL); err != nil {
						c.logger.Errorf("failed to process block [%s] [%v]", block.hash.String(), err)
					}
				}
			}
		}
	}()

	// start blob server listener
	go func() {
		<-ctx.Done()
		c.logger.Infof("[CoinbaseTracker] context done, closing client")
		c.running = false
	}()

	go func() {
		ch, err := c.blobServerClient.Subscribe(ctx, "")
		if err != nil {
			c.logger.Errorf("could not subscribe to blob server: %v", err)
			return
		}

		for {
			select {
			case <-ctx.Done():
				return
			case notification := <-ch:
				if notification.Type == model.NotificationType_Block {
					c.blockFoundCh <- processBlockFound{
						hash:    notification.Hash,
						baseURL: notification.BaseURL,
					}
				}
			}
		}
	}()

	// get the best block header on startup and process
	go func() {
		blockHeader, _, err := c.blobServerClient.GetBestBlockHeader(ctx)
		if err != nil {
			c.logger.Errorf("could not get best block header from blob server [%v]", err)
			return
		}
		c.blockFoundCh <- processBlockFound{
			hash:    blockHeader.Hash(),
			baseURL: "",
		}
	}()

	return nil
}

func (c *Coinbase) createTables(ctx context.Context) error {
	// Init coinbase tables in db
	if c.engine == util.Postgres {
		if _, err := c.db.ExecContext(ctx, `
			CREATE TABLE IF NOT EXISTS coinbase_utxos (
	    		id              BIGSERIAL PRIMARY KEY
			    ,block_id 	    BIGINT NOT NULL REFERENCES blocks (id)
				,block_hash     BYTEA NOT NULL
				,tx_id          BYTEA NOT NULL
				,vout           INTEGER NOT NULL
				,locking_script BYTEA NOT NULL
				,satoshis       BIGINT NOT NULL
				,address        TEXT
			    ,spendable      BOOLEAN NOT NULL DEFAULT FALSE
				,inserted_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
				,reserved_at    TIMESTAMPTZ NULL
				,spent_at       TIMESTAMPTZ NULL
				,spent_by_tx_id BYTEA NULL
			 )
		`); err != nil {
			return err
		}
	} else {
		if _, err := c.db.ExecContext(ctx, `
			CREATE TABLE IF NOT EXISTS coinbase_utxos (
				id              INTEGER PRIMARY KEY AUTOINCREMENT
			    ,block_id 	    BIGINT NOT NULL REFERENCES blocks (id)
				,block_hash     BLOB NOT NULL
				,tx_id          BLOB NOT NULL
				,vout           INTEGER NOT NULL
				,locking_script BLOB NOT NULL
				,satoshis       BIGINT NOT NULL
				,address        text
			    ,spendable      BOOLEAN NOT NULL DEFAULT FALSE
				,inserted_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
				,reserved_at    TEXT NULL
				,spent_at       TEXT NULL
				,spent_by_tx_id BLOB NULL
			 )
		`); err != nil {
			return err
		}
	}

	if _, err := c.db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_coinbase_utxos_tx_id_vout ON coinbase_utxos (tx_id, vout);`); err != nil {
		_ = c.db.Close()
		return fmt.Errorf("could not create ux_coinbase_utxos_tx_id_vout index - [%+v]", err)
	}

	if _, err := c.db.Exec(`CREATE INDEX IF NOT EXISTS ux_coinbase_utxos_block_spendable ON coinbase_utxos (block_id, spendable, address, reserved_at, id);`); err != nil {
		_ = c.db.Close()
		return fmt.Errorf("could not create ux_coinbase_utxos_tx_id_vout index - [%+v]", err)
	}

	return nil
}

func (c *Coinbase) catchup(ctx context.Context, fromBlock *model.Block, baseURL string) error {
	c.logger.Infof("catching up from %s on server %s", fromBlock.Hash().String(), baseURL)

	catchupBlockHeaders := []*model.BlockHeader{fromBlock.Header}
	var exists bool

	fromBlockHeaderHash := fromBlock.Header.HashPrevBlock

LOOP:
	for {
		c.logger.Debugf("getting block headers for catchup from [%s]", fromBlockHeaderHash.String())
		blockHeaders, _, err := c.blobServerClient.GetBlockHeaders(ctx, fromBlockHeaderHash, 1000)
		if err != nil {
			return err
		}

		if len(blockHeaders) == 0 {
			return fmt.Errorf("failed to get block headers from [%s]", fromBlockHeaderHash.String())
		}

		for _, blockHeader := range blockHeaders {
			exists, err = c.store.GetBlockExists(ctx, blockHeader.Hash())
			if err != nil {
				return fmt.Errorf("failed to check if block exists [%w]", err)
			}
			if exists {
				break LOOP
			}

			catchupBlockHeaders = append(catchupBlockHeaders, blockHeader)

			fromBlockHeaderHash = blockHeader.HashPrevBlock
			if fromBlockHeaderHash.IsEqual(&chainhash.Hash{}) {
				return fmt.Errorf("failed to find parent block header, last was: %s", blockHeader.String())
			}
		}
	}

	c.logger.Infof("catching up from [%s] to [%s]", catchupBlockHeaders[len(catchupBlockHeaders)-1].String(), catchupBlockHeaders[0].String())

	// process the catchup block headers in reverse order
	for i := len(catchupBlockHeaders) - 1; i >= 0; i-- {
		blockHeader := catchupBlockHeaders[i]

		block, err := c.blobServerClient.GetBlock(ctx, blockHeader.Hash())
		if err != nil {
			return errors.Join(fmt.Errorf("failed to get block [%s] [%v]", blockHeader.String(), err))
		}

		err = c.storeBlock(ctx, block)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Coinbase) processBlock(ctx context.Context, blockHash *chainhash.Hash, baseUrl string) (*model.Block, error) {
	c.logger.Debugf("processing block: %s", blockHash.String())

	// check whether we already have the parent block
	exists, err := c.store.GetBlockExists(ctx, blockHash)
	if err != nil {
		return nil, fmt.Errorf("could not check whether block exists %+v", err)
	}
	if exists {
		return nil, nil
	}

	block, err := c.blobServerClient.GetBlock(ctx, blockHash)
	if err != nil {
		return block, err
	}

	// check whether we already have the parent block
	exists, err = c.store.GetBlockExists(ctx, block.Header.HashPrevBlock)
	if err != nil {
		return nil, fmt.Errorf("could not check whether block exists %+v", err)
	}
	if !exists {
		go func() {
			c.catchupCh <- processBlockCatchup{
				block:   block,
				baseURL: baseUrl,
			}
		}()
		return nil, nil
	}

	err = c.storeBlock(ctx, block)
	if err != nil {
		return nil, err
	}

	return block, err
}

func (c *Coinbase) storeBlock(ctx context.Context, block *model.Block) error {
	blockId, err := c.store.StoreBlock(ctx, block)
	if err != nil {
		return fmt.Errorf("could not store block: %+v", err)
	}

	// process coinbase into utxos
	err = c.processCoinbase(ctx, blockId, block.Hash(), block.CoinbaseTx)
	if err != nil {
		return fmt.Errorf("could not process coinbase %+v", err)
	}

	return nil
}

func (c *Coinbase) processCoinbase(ctx context.Context, blockId uint64, blockHash *chainhash.Hash, coinbaseTx *bt.Tx) error {
	c.logger.Infof("processing coinbase: %s, for block: %s with %d utxos", coinbaseTx.TxID(), blockHash.String(), len(coinbaseTx.Outputs))

	for vout, output := range coinbaseTx.Outputs {
		if !output.LockingScript.IsP2PKH() {
			c.logger.Warnf("only p2pkh coinbase outputs are supported: %s:%d", coinbaseTx.TxID(), vout)
			continue
		}

		addresses, err := output.LockingScript.Addresses()
		if err != nil {
			return err
		}

		if _, err = c.db.ExecContext(ctx, `
			INSERT INTO coinbase_utxos (block_id, block_hash, tx_id, vout, locking_script, satoshis, address)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
		`, blockId, blockHash[:], coinbaseTx.TxIDChainHash()[:], vout, output.LockingScript, output.Satoshis, addresses[0]); err != nil {
			return fmt.Errorf("could not insert coinbase utxo: %+v", err)
		}
	}

	// update everything 100 blocks old on this chain to spendable
	q := `
		WITH LongestChainTip AS (
			SELECT id, height
			FROM blocks
			ORDER BY chain_work DESC, id ASC
			LIMIT 1
		)

		UPDATE coinbase_utxos
		SET spendable = true
		WHERE spendable = false AND block_id IN (
			WITH RECURSIVE ChainBlocks AS (
				SELECT id, parent_id, height
				FROM blocks
				WHERE id = (SELECT id FROM LongestChainTip)
				UNION ALL
				SELECT b.id, b.parent_id, b.height
				FROM blocks b
				JOIN ChainBlocks cb ON b.id = cb.parent_id
				WHERE b.id != cb.id
			)
			SELECT id FROM ChainBlocks
			WHERE height < (SELECT height - 100 FROM LongestChainTip)
		)
	`
	if _, err := c.db.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("could not update coinbase utxos to spendable: %+v", err)
	}

	return nil
}

func (c *Coinbase) ReserveUtxo(ctx context.Context, address string) (*bt.UTXO, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	utxo := &bt.UTXO{
		SequenceNumber: 0xffffffff,
	}

	var utxoBytes []byte

	var id uint64
	if err := c.db.QueryRowContext(ctx, `
		SELECT u.id, u.tx_id, u.vout, u.locking_script, u.satoshis
		FROM coinbase_utxos u, blocks b
		WHERE u.block_id = b.id
		  And u.spendable = true
		  AND u.address = $1
		  AND u.reserved_at IS NULL
		ORDER BY u.id ASC
		LIMIT 1
	`, address).Scan(
		&id,
		&utxoBytes,
		&utxo.Vout,
		&utxo.LockingScript,
		&utxo.Satoshis,
	); err != nil {
		return nil, err
	}

	var err error
	utxo.TxIDHash, err = chainhash.NewHash(utxoBytes)
	if err != nil {
		return nil, err
	}

	if _, err = c.db.ExecContext(ctx, `
		UPDATE coinbase_utxos
		SET reserved_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`, id); err != nil {
		return nil, err
	}

	return utxo, nil
}

func (c *Coinbase) MarkUtxoAsSpent(ctx context.Context, txId *chainhash.Hash, vout uint32, spentByTxID *chainhash.Hash) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, err := c.db.ExecContext(ctx, `
		UPDATE coinbase_utxos
		SET spent_at = CURRENT_TIMESTAMP
		  , spent_by_tx_id = $1
		WHERE tx_id = $2
		  AND vout = $3
	`, spentByTxID[:], txId[:], vout); err != nil {
		return err
	}

	return nil
}
