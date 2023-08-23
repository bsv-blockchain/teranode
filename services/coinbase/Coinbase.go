package coinbase

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blobserver/blobserver_api"
	"github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Coinbase struct {
	db          *sql.DB
	engine      util.SQLEngine
	store       blockchain.Store
	blockHeight uint32
	running     bool
	logger      utils.Logger
	mu          sync.Mutex
}

// NewCoinbase builds on top of the blockchain store to provide a coinbase tracker
// Only SQL databases are supported
func NewCoinbase(logger utils.Logger, store blockchain.Store) (*Coinbase, error) {
	engine := store.GetDBEngine()
	if engine != util.Postgres && engine != util.Sqlite && engine != util.SqliteMemory {
		return nil, fmt.Errorf("unsupported database engine: %s", engine)
	}

	c := &Coinbase{
		store:  store,
		db:     store.GetDB(),
		engine: engine,
		logger: logger,
	}

	return c, nil
}

func (c *Coinbase) Init(ctx context.Context) error {
	if err := c.createTables(ctx); err != nil {
		return fmt.Errorf("failed to create coinbase tables: %s", err)
	}

	blobServerAddr, ok := gocore.Config().Get("blobserver_grpcAddress")
	if !ok {
		return errors.New("no blobserver_grpcAddress setting found")
	}

	blobServerConn, err := util.GetGRPCClient(ctx, blobServerAddr, &util.ConnectionOptions{})
	if err != nil {
		return err
	}

	blobServerClient := blobserver_api.NewBlobServerAPIClient(blobServerConn)

	// start block height listener
	go func() {
		timer := time.NewTimer(10 * time.Second)
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				_, blockHeight, err := c.store.GetBestBlockHeader(ctx)
				if err != nil {
					c.logger.Errorf("error getting block height: %s", err)
					continue
				}
				c.blockHeight = blockHeight
			}
		}
	}()

	// start blob server listener
	go func() {
		<-ctx.Done()
		c.logger.Infof("[CoinbaseTracker] context done, closing client")
		c.running = false
		err = blobServerConn.Close()
		if err != nil {
			c.logger.Errorf("[CoinbaseTracker] failed to close connection", err)
		}
	}()

	go func() {
		c.running = true

		var stream blobserver_api.BlobServerAPI_SubscribeClient
		var resp *blobserver_api.Notification
		var blockHash *chainhash.Hash

		for c.running {
			c.logger.Infof("starting new subscription to blobserver: %v", blobServerAddr)
			stream, err = blobServerClient.Subscribe(ctx, &blobserver_api.SubscribeRequest{
				Source: "coinbase",
			})
			if err != nil {
				c.logger.Errorf("could not subscribe to blobserver: %v", err)
				time.Sleep(10 * time.Second)
				continue
			}

			for c.running {
				resp, err = stream.Recv()
				if err != nil {
					if !strings.Contains(err.Error(), context.Canceled.Error()) {
						c.logger.Errorf("could not receive from blobserver: %v", err)
					}
					_ = stream.CloseSend()
					time.Sleep(10 * time.Second)
					break
				}

				if resp.Type == blobserver_api.Type_Block {
					blockHash, err = chainhash.NewHash(resp.Hash)
					if err != nil {
						c.logger.Errorf("could not create hash from bytes", "err", err)
						continue
					}
					c.logger.Debugf("Received BLOCK notification: %s", blockHash.String())

					_, err = c.processBlock(ctx, blockHash, resp.BaseUrl)
					if err != nil {
						c.logger.Errorf("could not process block %+v", err)
						continue
					}
				}
			}
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
				,block_hash     BYTEA NOT NULL
				,tx_id          BYTEA NOT NULL
				,vout           INTEGER NOT NULL
				,locking_script BYTEA NOT NULL
				,satoshis       BIGINT NOT NULL
				,address        TEXT
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
				,block_hash     BLOB NOT NULL
				,tx_id          BLOB NOT NULL
				,vout           INTEGER NOT NULL
				,locking_script BLOB NOT NULL
				,satoshis       BIGINT NOT NULL
				,address        text
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

	return nil
}

func (c *Coinbase) processBlock(ctx context.Context, blockHash *chainhash.Hash, baseUrl string) (*model.Block, error) {
	c.logger.Debugf("processing block: %s", blockHash.String())

	// get the block
	b, err := util.DoHTTPRequest(ctx, fmt.Sprintf("%s/block/%s", baseUrl, blockHash.String()))
	if err != nil {
		return nil, fmt.Errorf("could not get block from blobserver %+v", err)
	}

	block, err := model.NewBlockFromBytes(b)
	if err != nil {
		return nil, fmt.Errorf("could not get block from network %+v", err)
	}

	// check whether we already have the parent block
	exists, err := c.store.GetBlockExists(ctx, block.Header.HashPrevBlock)
	if err != nil {
		return nil, fmt.Errorf("could not check whether block exists %+v", err)
	}

	if !exists {
		// get the parent block, before storing the new block
		c.logger.Debugf("getting parent block: %s", block.Header.HashPrevBlock.String())
		_, err = c.processBlock(ctx, block.Header.HashPrevBlock, baseUrl)
		if err != nil {
			return nil, fmt.Errorf("could not get parent block %s: %+v", block.Header.HashPrevBlock.String(), err)
		}
		// SQLite doesn't like concurrent writes, so we sleep for a bit
		time.Sleep(100 * time.Millisecond)
	}

	err = c.store.StoreBlock(ctx, block)
	if err != nil {
		return nil, fmt.Errorf("could not store block: %+v", err)
	}

	// process coinbase into utxos
	err = c.processCoinbase(ctx, blockHash, block.CoinbaseTx)
	if err != nil {
		return nil, fmt.Errorf("could not process coinbase %+v", err)
	}

	return block, err
}

func (c *Coinbase) processCoinbase(ctx context.Context, blockHash *chainhash.Hash, coinbaseTx *bt.Tx) error {
	c.logger.Debugf("processing coinbase: %s, for block: %s", coinbaseTx.TxID(), blockHash.String())

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
			INSERT INTO coinbase_utxos (block_hash, tx_id, vout, locking_script, satoshis, address)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, blockHash[:], coinbaseTx.TxIDChainHash()[:], vout, output.LockingScript, output.Satoshis, addresses[0]); err != nil {
			return err
		}
	}

	return nil
}

func (c *Coinbase) ReserveUtxo(ctx context.Context, address string) (*bt.UTXO, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	utxo := &bt.UTXO{
		SequenceNumber: 0xffffffff,
	}

	var id uint64
	if err := c.db.QueryRowContext(ctx, `
		SELECT u.id, u.tx_id, u.vout, u.locking_script, u.satoshis
		FROM coinbase_utxos u, blocks b
		WHERE u.block_hash = b.hash
		  AND b.height < (SELECT MAX(height) - 100 FROM blocks)
		  And b.orphaned = false
		  AND u.address = $1
		  AND u.reserved_at IS NULL
		ORDER BY u.id ASC
		LIMIT 1
	`, address).Scan(
		&id,
		&utxo.TxID,
		&utxo.Vout,
		&utxo.LockingScript,
		&utxo.Satoshis,
	); err != nil {
		return nil, err
	}

	if _, err := c.db.ExecContext(ctx, `
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
