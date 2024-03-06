package coinbase

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/asset"
	"github.com/bitcoin-sv/ubsv/services/coinbase/coinbase_api"
	"github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/distributor"
	"github.com/bitcoin-sv/ubsv/util/p2p"
	"github.com/bitcoin-sv/ubsv/util/usql"
	"github.com/lib/pq"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/gocore"
)

var coinbaseStat = gocore.NewStat("coinbase")

type processBlockFound struct {
	hash    *chainhash.Hash
	baseURL string
}

type processBlockCatchup struct {
	block   *model.Block
	baseURL string
}

type Coinbase struct {
	db           *usql.DB
	engine       util.SQLEngine
	store        blockchain.Store
	AssetClient  *asset.Client
	distributor  *distributor.Distributor
	privateKey   *bec.PrivateKey
	running      bool
	blockFoundCh chan processBlockFound
	catchupCh    chan processBlockCatchup
	logger       ulogger.Logger
	address      string
	dbTimeout    time.Duration
	peerSync     *p2p.PeerHeight
	waitForPeers bool
	g            *errgroup.Group
	gCtx         context.Context
}

// NewCoinbase builds on top of the blockchain store to provide a coinbase tracker
// Only SQL databases are supported
func NewCoinbase(logger ulogger.Logger, store blockchain.Store) (*Coinbase, error) {
	engine := store.GetDBEngine()
	if engine != util.Postgres && engine != util.Sqlite && engine != util.SqliteMemory {
		return nil, fmt.Errorf("unsupported database engine: %s", engine)
	}

	coinbasePrivKey, found := gocore.Config().Get("coinbase_wallet_private_key")
	if !found {
		return nil, errors.New("coinbase_wallet_private_key not found in config")
	}

	privateKey, err := wif.DecodeWIF(coinbasePrivKey)
	if err != nil {
		return nil, fmt.Errorf("can't decode coinbase priv key: ^%v", err)
	}

	coinbaseAddr, err := bscript.NewAddressFromPublicKey(privateKey.PrivKey.PubKey(), true)
	if err != nil {
		return nil, fmt.Errorf("can't create coinbase address: %v", err)
	}

	backoffDuration, err, _ := gocore.Config().GetDuration("distributor_backoff_duration", 1*time.Second)
	if err != nil {
		return nil, fmt.Errorf("could not parse distributor_backoff_duration: %v", err)
	}

	maxRetries, _ := gocore.Config().GetInt("distributor_max_retries", 3)

	failureTolerance, _ := gocore.Config().GetInt("distributor_failure_tolerance", 0)

	d, err := distributor.NewDistributor(logger, distributor.WithBackoffDuration(backoffDuration), distributor.WithRetryAttempts(int32(maxRetries)), distributor.WithFailureTolerance(failureTolerance))
	if err != nil {
		return nil, fmt.Errorf("could not create distributor: %v", err)
	}

	dbTimeoutMillis, _ := gocore.Config().GetInt("blockchain_store_dbTimeoutMillis", 5000)

	addresses, ok := gocore.Config().Get("propagation_grpcAddresses")
	if !ok {
		panic("[PeerStatus] propagation_grpcAddresses not found")
	}
	numberOfExpectedPeers := 1 + strings.Count(addresses, "|") // each | is a ip:port separator

	peerStatusTimeout, err, _ := gocore.Config().GetDuration("peerStatus_timeout", 30*time.Second)
	if err != nil {
		panic(fmt.Sprintf("[PeerStatus] failed to parse peerStatus_timeout: %s", err))
	}

	waitForPeers := gocore.Config().GetBool("coinbase_wait_for_peers", false)

	g, gCtx := errgroup.WithContext(context.Background())
	g.SetLimit(runtime.NumCPU())

	c := &Coinbase{
		store:        store,
		db:           store.GetDB(),
		engine:       engine,
		blockFoundCh: make(chan processBlockFound, 100),
		catchupCh:    make(chan processBlockCatchup),
		distributor:  d,
		logger:       logger,
		privateKey:   privateKey.PrivKey,
		address:      coinbaseAddr.AddressString,
		dbTimeout:    time.Duration(dbTimeoutMillis) * time.Millisecond,
		peerSync:     p2p.NewPeerHeight(logger, "coinbase", numberOfExpectedPeers, peerStatusTimeout),
		waitForPeers: waitForPeers,
		g:            g,
		gCtx:         gCtx,
	}

	threshold, found := gocore.Config().GetInt("coinbase_notification_threshold")
	if found {
		go c.monitorSpendableUTXOs(uint64(threshold))
	}

	return c, nil
}

func (c *Coinbase) Init(ctx context.Context) (err error) {
	if err = c.createTables(ctx); err != nil {
		return fmt.Errorf("failed to create coinbase tables: %s", err)
	}

	AssetAddr, ok := gocore.Config().Get("coinbase_assetGrpcAddress")
	if !ok {
		AssetAddr, ok = gocore.Config().Get("asset_grpcAddress")
		if !ok {
			return errors.New("no asset_grpcAddress setting found")
		}
	}

	c.AssetClient, err = asset.NewClient(ctx, c.logger, AssetAddr)
	if err != nil {
		return fmt.Errorf("failed to create Asset client: %s", err)
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
		ch, err := c.AssetClient.Subscribe(ctx, "")
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
		//Wait until min_height is reached
		coinbase_should_wait := gocore.Config().GetBool("coinbase_should_wait", false)
		coinbase_wait_until_block, _ := gocore.Config().GetInt("coinbase_wait_until_block", 0)
		if coinbase_should_wait {
			c.logger.Infof("Waiting until block %d to start coinbase", coinbase_wait_until_block)
			for {
				_, bestBlockHeader, err := c.AssetClient.GetBestBlockHeader(ctx)
				if err != nil {
					c.logger.Errorf("could not get best block header from blob server [%v]", err)
					return
				}
				if bestBlockHeader >= uint32(coinbase_wait_until_block) {
					break
				}
				c.logger.Infof("Waiting until block %d to start coinbase, currently at %d", coinbase_wait_until_block, bestBlockHeader)
				time.Sleep(1 * time.Second)
			}
		}
		blockHeader, _, err := c.AssetClient.GetBestBlockHeader(ctx)
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

	var idType string
	var bType string

	switch c.engine {
	case util.Postgres:
		idType = "BIGSERIAL"
		bType = "BYTEA"
	case util.Sqlite, util.SqliteMemory:
		idType = "INTEGER PRIMARY KEY AUTOINCREMENT"
		bType = "BLOB"
	default:
		return fmt.Errorf("unsupported database engine: %s", c.engine)
	}

	// Init coinbase tables in db
	if _, err := c.db.ExecContext(ctx, fmt.Sprintf(`
		  CREATE TABLE IF NOT EXISTS coinbase_utxos (
			 inserted_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	    ,block_id 	    BIGINT NOT NULL REFERENCES blocks (id)
			,txid           %s NOT NULL
			,vout           INTEGER NOT NULL
			,locking_script %s NOT NULL
			,satoshis       BIGINT NOT NULL
			,processed_at   TIMESTAMPTZ
		 )
		`, bType, bType)); err != nil {
		return err
	}

	if _, err := c.db.ExecContext(ctx, fmt.Sprintf(`
		  CREATE TABLE IF NOT EXISTS spendable_utxos (
			 id						  %s
			,inserted_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	    ,txid           %s NOT NULL
			,vout           INTEGER NOT NULL
			,locking_script %s NOT NULL
			,satoshis       BIGINT NOT NULL
		 )
		`, idType, bType, bType)); err != nil {
		return err
	}

	if _, err := c.db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS ux_coinbase_utxos_txid_vout ON coinbase_utxos (txid, vout);`); err != nil {
		_ = c.db.Close()
		return fmt.Errorf("could not create ux_coinbase_utxos_txid_vout index - [%+v]", err)
	}

	if _, err := c.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS ux_coinbase_utxos_processed_at ON coinbase_utxos (processed_at ASC);`); err != nil {
		_ = c.db.Close()
		return fmt.Errorf("could not create ux_coinbase_utxos_processed_at index - [%+v]", err)
	}

	if _, err := c.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS ux_spendable_utxos_inserted_at ON spendable_utxos (inserted_at ASC);`); err != nil {
		_ = c.db.Close()
		return fmt.Errorf("could not create ux_spendable_utxos_inserted_at index - [%+v]", err)
	}

	return nil
}

func (c *Coinbase) catchup(cntxt context.Context, fromBlock *model.Block, baseURL string) error {
	start, stat, ctx := util.NewStatFromContext(cntxt, "catchup", stats)
	defer func() {
		stat.AddTime(start)
	}()

	c.logger.Infof("catching up from %s on server %s", fromBlock.Hash().String(), baseURL)

	catchupBlockHeaders := []*model.BlockHeader{fromBlock.Header}
	var exists bool

	fromBlockHeaderHash := fromBlock.Header.HashPrevBlock

LOOP:
	for {
		c.logger.Debugf("getting block headers for catchup from [%s]", fromBlockHeaderHash.String())
		blockHeaders, _, err := c.AssetClient.GetBlockHeaders(ctx, fromBlockHeaderHash, 1000)
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

		block, err := c.AssetClient.GetBlock(ctx, blockHeader.Hash())
		if err != nil {
			return errors.Join(fmt.Errorf("failed to get block [%s]", blockHeader.String()), err)
		}

		err = c.storeBlock(ctx, block)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Coinbase) processBlock(cntxt context.Context, blockHash *chainhash.Hash, baseUrl string) (*model.Block, error) {
	start, stat, ctx := util.NewStatFromContext(cntxt, "processBlock", coinbaseStat)
	defer func() {
		stat.AddTime(start)
	}()

	c.logger.Debugf("processing block: %s", blockHash.String())

	// check whether we already have the block
	exists, err := c.store.GetBlockExists(ctx, blockHash)
	if err != nil {
		return nil, fmt.Errorf("could not check whether block exists %+v", err)
	}
	if exists {
		return nil, nil
	}

	block, err := c.AssetClient.GetBlock(ctx, blockHash)
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
	start, stat, ctx := util.StartStatFromContext(ctx, "storeBlock")
	defer func() {
		stat.AddTime(start)
	}()

	//ctxTimeout, cancelTimeout := context.WithTimeout(ctx, c.dbTimeout)
	//defer cancelTimeout()

	blockId, err := c.store.StoreBlock(ctx, block, "")
	if err != nil {
		return fmt.Errorf("could not store block: %+v", err)
	}

	// process coinbase into utxos
	// first check whether we are in sync with the blob server, otherwise we wait for the next block
	_, blobBestBlockHeight, _ := c.AssetClient.GetBestBlockHeader(ctx)
	_, coinbaseBestBlockMeta, _ := c.store.GetBestBlockHeader(ctx)

	if c.waitForPeers {
		/* Wait until all nodes are at least on same block height as this coinbase block */
		/* Do this before attempting to distribute the coinbase splitting transactions to all nodes */
		err = c.peerSync.WaitForAllPeers(ctx, blobBestBlockHeight, true)
		if err != nil {
			return fmt.Errorf("peers are not in sync: %s", err)
		}
	}

	if blobBestBlockHeight >= coinbaseBestBlockMeta.Height {
		err = c.processCoinbase(ctx, blockId, block.Hash(), block.CoinbaseTx)
		if err != nil {
			return fmt.Errorf("could not process coinbase %+v", err)
		}
	}

	return nil
}

func (c *Coinbase) processCoinbase(ctx context.Context, blockId uint64, blockHash *chainhash.Hash, coinbaseTx *bt.Tx) error {
	start, stat, ctx := util.StartStatFromContext(ctx, "processCoinbase")
	defer func() {
		stat.AddTime(start)
	}()

	//ctx, cancelTimeout := context.WithTimeout(ctx, c.dbTimeout)
	//defer cancelTimeout()

	c.logger.Infof("processing coinbase: %s, for block: %s with %d utxos", coinbaseTx.TxID(), blockHash.String(), len(coinbaseTx.Outputs))

	if err := c.insertCoinbaseUTXOs(ctx, blockId, coinbaseTx); err != nil {
		return fmt.Errorf("could not insert coinbase utxos: %+v", err)
	}

	// Create a timestamp variable to insert into the TIMESTAMPTZ field
	timestamp := time.Now().UTC()

	// update everything 100 blocks old on this chain to spendable
	q := `
		WITH LongestChainTip AS (
			SELECT id, height
			FROM blocks
			ORDER BY chain_work DESC, inserted_at ASC
			LIMIT 1
		)

		UPDATE coinbase_utxos
		SET
		 processed_at = $1
		WHERE processed_at IS NULL
		AND block_id IN (
			WITH RECURSIVE ChainBlocks AS (
				SELECT
				 id
				,parent_id
				,height
				FROM blocks
				WHERE id = (SELECT id FROM LongestChainTip)
				UNION ALL
				SELECT
				 b.id
				,b.parent_id
				,b.height
				FROM blocks b
				JOIN ChainBlocks cb ON b.id = cb.parent_id
				WHERE b.id != cb.id
			)
			SELECT id FROM ChainBlocks
			WHERE height < (SELECT height - 110 FROM LongestChainTip)
		)
	`
	if _, err := c.db.ExecContext(ctx, q, timestamp); err != nil {
		return fmt.Errorf("could not update coinbase_utxos to be processed: %+v", err)
	}

	_, _, ctx = util.NewStatFromContext(context.Background(), "go routine", stat, false)

	if err := c.createSpendingUtxos(ctx, timestamp); err != nil {
		return fmt.Errorf("could not create spending utxos: %w", err)
	}

	return nil
}

func (c *Coinbase) createSpendingUtxos(ctx context.Context, timestamp time.Time) error {
	start, stat, ctx := util.StartStatFromContext(ctx, "createSpendingUtxos")
	defer func() {
		stat.AddTime(start)
	}()

	//ctx, cancelTimeout := context.WithTimeout(ctx, c.dbTimeout)
	//defer cancelTimeout()

	q := `
	  SELECT
	   txid
	  ,vout
	  ,locking_script
  	,satoshis
	  FROM coinbase_utxos
	  WHERE processed_at = $1
	`

	rows, err := c.db.QueryContext(ctx, q, timestamp)
	if err != nil {
		return fmt.Errorf("could not get coinbase utxos: %w", err)
	}

	defer rows.Close()

	utxos := make([]*bt.UTXO, 0)
	for rows.Next() {
		var txid []byte
		var vout uint32
		var lockingScript bscript.Script
		var satoshis uint64

		if err := rows.Scan(&txid, &vout, &lockingScript, &satoshis); err != nil {
			return fmt.Errorf("could not scan coinbase utxo: %w", err)
		}

		hash, err := chainhash.NewHash(txid)
		if err != nil {
			return fmt.Errorf("could not create hash from txid: %w", err)
		}

		utxos = append(utxos, &bt.UTXO{
			TxIDHash:      hash,
			Vout:          vout,
			LockingScript: &lockingScript,
			Satoshis:      satoshis,
		})
	}

	for _, utxo := range utxos {
		utxo := utxo
		// create the utxos in the background
		// we don't have a method to revert anything that goes wrong anyway
		c.g.Go(func() error {
			c.logger.Infof("createSpendingUtxos coinbase: %s: utxo %d", utxo.TxIDHash, utxo.Vout)
			delay, err, _ := gocore.Config().GetDuration("coinbase_store_createSpendingUtxos_delay", 0*time.Second)
			if err != nil {
				panic(fmt.Sprintf("could not parse coinbase_store_createSpendingUtxos_delay: %v", err))
			}
			time.Sleep(delay)
			if err := c.splitUtxo(c.gCtx, utxo); err != nil {
				return fmt.Errorf("could not split utxo: %w", err)
			}
			return nil
		})
	}

	return nil
}

func (c *Coinbase) splitUtxo(cntxt context.Context, utxo *bt.UTXO) error {
	start, stat, ctx := util.StartStatFromContext(cntxt, "splitUtxo")
	defer func() {
		stat.AddTime(start)
	}()

	//ctx, cancelTimeout := context.WithTimeout(ctx, c.dbTimeout)
	//defer cancelTimeout()

	tx := bt.NewTx()

	if err := tx.FromUTXOs(utxo); err != nil {
		return fmt.Errorf("error creating initial transaction: %v", err)
	}

	var splitSatoshis = uint64(10_000_000)
	amountRemaining := utxo.Satoshis

	for amountRemaining > splitSatoshis {

		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout splitting the satoshis")
		default:
			tx.AddOutput(&bt.Output{
				LockingScript: utxo.LockingScript,
				Satoshis:      splitSatoshis,
			})

			amountRemaining -= splitSatoshis
		}
	}

	tx.AddOutput(&bt.Output{
		LockingScript: utxo.LockingScript,
		Satoshis:      amountRemaining,
	})

	unlockerGetter := unlocker.Getter{PrivateKey: c.privateKey}
	if err := tx.FillAllInputs(ctx, &unlockerGetter); err != nil {
		return fmt.Errorf("error filling splitting inputs: %v", err)
	}

	if _, err := c.distributor.SendTransaction(ctx, tx); err != nil {
		return fmt.Errorf("error sending splitting transaction %s: %v", tx.TxIDChainHash().String(), err)
	}

	// Insert the spendable utxos....
	return c.insertSpendableUTXOs(ctx, tx)
}

func (c *Coinbase) RequestFunds(ctx context.Context, address string, disableDistribute bool) (*bt.Tx, error) {
	//ctx, cancelTimeout := context.WithTimeout(ctx, c.dbTimeout)
	//defer cancelTimeout()

	start, stat, ctx := util.NewStatFromContext(ctx, "RequestFunds", coinbaseStat)
	defer func() {
		stat.AddTime(start)
	}()

	var utxo *bt.UTXO
	var err error

	switch c.engine {
	case util.Postgres:
		utxo, err = c.requestFundsPostgres(ctx, address)
	case util.Sqlite, util.SqliteMemory:
		utxo, err = c.requestFundsSqlite(ctx, address)
	default:
		return nil, fmt.Errorf("unsupported database engine: %s", c.engine)
	}

	if err != nil {
		return nil, fmt.Errorf("error requesting funds: %v", err)
	}

	tx := bt.NewTx()

	if err = tx.FromUTXOs(utxo); err != nil {
		return nil, fmt.Errorf("error creating initial transaction: %v", err)
	}

	splits := uint64(100)

	// Split the utxo into 100 outputs satoshis
	sats := utxo.Satoshis / splits
	remainder := utxo.Satoshis % splits

	for i := 0; i < int(splits); i++ {
		if i == 0 && remainder > 0 {
			if err = tx.PayToAddress(address, sats+remainder); err != nil {
				return nil, fmt.Errorf("error paying to address: %v", err)
			}
			continue
		}

		if err = tx.PayToAddress(address, sats); err != nil {
			return nil, fmt.Errorf("error paying to address: %v", err)
		}
	}

	unlockerGetter := unlocker.Getter{PrivateKey: c.privateKey}
	if err = tx.FillAllInputs(ctx, &unlockerGetter); err != nil {
		return nil, fmt.Errorf("error filling initial inputs: %v", err)
	}

	if !disableDistribute {
		if _, err = c.distributor.SendTransaction(ctx, tx); err != nil {
			return nil, fmt.Errorf("error sending initial transaction: %v", err)
		}

		c.logger.Debugf("Sent funding transaction %s", tx.TxIDChainHash().String())
	}

	return tx, nil
}

func (c *Coinbase) DistributeTransaction(ctx context.Context, tx *bt.Tx) ([]*distributor.ResponseWrapper, error) {
	return c.distributor.SendTransaction(ctx, tx)
}

func (c *Coinbase) requestFundsPostgres(ctx context.Context, address string) (*bt.UTXO, error) {
	// Get the oldest spendable utxo
	var txid []byte
	var vout uint32
	var lockingScript bscript.Script
	var satoshis uint64

	if err := c.db.QueryRowContext(ctx, `
	DELETE FROM spendable_utxos
	WHERE id = (
		SELECT id
		FROM spendable_utxos
		ORDER BY inserted_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	)
	RETURNING txid, vout, locking_script, satoshis;
`).Scan(&txid, &vout, &lockingScript, &satoshis); err != nil {
		return nil, err
	}

	hash, err := chainhash.NewHash(txid)
	if err != nil {
		return nil, err
	}

	utxo := &bt.UTXO{
		TxIDHash:      hash,
		Vout:          vout,
		LockingScript: &lockingScript,
		Satoshis:      satoshis,
	}

	return utxo, nil
}
func (c *Coinbase) requestFundsSqlite(ctx context.Context, address string) (*bt.UTXO, error) {
	//ctx, cancelTimeout := context.WithTimeout(cntxt, c.dbTimeout)
	//defer cancelTimeout()

	txn, err := c.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.IsolationLevel(sql.LevelWriteCommitted),
	})
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = txn.Rollback()
	}()

	// Get the oldest spendable utxo
	var txid []byte
	var vout uint32
	var lockingScript bscript.Script
	var satoshis uint64

	if err := txn.QueryRowContext(ctx, `
		SELECT txid, vout, locking_script, satoshis
		FROM spendable_utxos
		ORDER BY inserted_at ASC
		LIMIT 1
	`).Scan(&txid, &vout, &lockingScript, &satoshis); err != nil {
		return nil, err
	}

	if _, err := txn.ExecContext(ctx, `DELETE FROM spendable_utxos WHERE txid = $1 AND vout = $2`, txid, vout); err != nil {
		return nil, err
	}

	if err := txn.Commit(); err != nil {
		return nil, err
	}

	hash, err := chainhash.NewHash(txid)
	if err != nil {
		return nil, err
	}

	utxo := &bt.UTXO{
		TxIDHash:      hash,
		Vout:          vout,
		LockingScript: &lockingScript,
		Satoshis:      satoshis,
	}

	return utxo, nil
}

func (c *Coinbase) insertCoinbaseUTXOs(ctx context.Context, blockId uint64, tx *bt.Tx) error {
	//ctx, cancelTimeout := context.WithTimeout(cntxt, c.dbTimeout)
	//defer cancelTimeout()

	start, stat, ctx := util.StartStatFromContext(ctx, "insertCoinbaseUTXOs")
	defer func() {
		stat.AddTime(start)
	}()

	var txn *sql.Tx
	var stmt *sql.Stmt
	var err error

	hash := tx.TxIDChainHash()[:]

	switch c.engine {
	case util.Sqlite, util.SqliteMemory:
		stmt, err = c.db.PrepareContext(ctx, `INSERT INTO coinbase_utxos (
			block_id, txid, vout, locking_script, satoshis
			)	VALUES (
			$1, $2, $3, $4, $5
			)`)
		if err != nil {
			return err
		}

	case util.Postgres:
		// Prepare the copy operation
		txn, err = c.db.Begin()
		if err != nil {
			return err
		}

		defer func() {
			_ = txn.Rollback()
		}()

		stmt, err = txn.Prepare(pq.CopyIn("coinbase_utxos", "block_id", "txid", "vout", "locking_script", "satoshis"))
		if err != nil {
			return err
		}

	default:
		return fmt.Errorf("unsupported database engine: %s", c.engine)
	}

	for vout, output := range tx.Outputs {
		if !output.LockingScript.IsP2PKH() {
			c.logger.Warnf("only p2pkh coinbase outputs are supported: %s:%d", tx.TxIDChainHash().String(), vout)
			continue
		}

		addresses, err := output.LockingScript.Addresses()
		if err != nil {
			return err
		}

		if addresses[0] == c.address {
			if _, err = stmt.ExecContext(ctx, blockId, hash, vout, output.LockingScript, output.Satoshis); err != nil {
				return fmt.Errorf("could not insert coinbase utxo: %+v", err)
			}
		}
	}

	if c.engine == util.Postgres {
		// Execute the batch transaction
		_, err = stmt.ExecContext(ctx)
		if err != nil {
			return err
		}

		if err := stmt.Close(); err != nil {
			return err
		}
		if err := txn.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func (c *Coinbase) insertSpendableUTXOs(ctx context.Context, tx *bt.Tx) error {
	//ctx, cancelTimeout := context.WithTimeout(cntxt, c.dbTimeout)
	//defer cancelTimeout()

	start, stat, ctx := util.StartStatFromContext(ctx, "insertSpendableUTXOs")
	defer func() {
		stat.AddTime(start)
	}()

	var txn *sql.Tx
	var stmt *sql.Stmt
	var err error

	hash := tx.TxIDChainHash()[:]

	switch c.engine {
	case util.Sqlite, util.SqliteMemory:
		stmt, err = c.db.PrepareContext(ctx, `INSERT INTO spendable_utxos (
			txid, vout, locking_script, satoshis
			)	VALUES (
			$1, $2, $3, $4
			)`)
		if err != nil {
			return err
		}

	case util.Postgres:
		// Prepare the copy operation
		txn, err = c.db.Begin()
		if err != nil {
			return err
		}

		defer func() {
			// Silently ignore rollback errors
			_ = txn.Rollback()
		}()

		stmt, err = txn.Prepare(pq.CopyIn("spendable_utxos", "txid", "vout", "locking_script", "satoshis"))
		if err != nil {
			return err
		}

	default:
		return fmt.Errorf("unsupported database engine: %s", c.engine)
	}

	for vout, output := range tx.Outputs {
		if _, err := stmt.ExecContext(ctx, hash, vout, output.LockingScript, output.Satoshis); err != nil {
			return fmt.Errorf("could not insert spendable utxo %s: %+v", tx.TxIDChainHash().String(), err)
		}
	}

	switch c.engine {
	case util.Sqlite, util.SqliteMemory:
		if err := stmt.Close(); err != nil {
			return err
		}
	case util.Postgres:
		// Execute the batch transaction
		_, err = stmt.ExecContext(ctx)
		if err != nil {
			return err
		}

		if err := stmt.Close(); err != nil {
			return err
		}
		if err := txn.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func (c *Coinbase) getBalance(ctx context.Context) (*coinbase_api.GetBalanceResponse, error) {
	res := &coinbase_api.GetBalanceResponse{}

	if err := c.db.QueryRowContext(ctx, `
		SELECT
		 COUNT(*)
		,COALESCE(SUM(satoshis), 0)
		FROM spendable_utxos;
	`).Scan(&res.NumberOfUtxos, &res.TotalSatoshis); err != nil {
		return nil, err
	}

	return res, nil
}

func (c *Coinbase) monitorSpendableUTXOs(threshold uint64) {
	ticker := time.NewTicker(1 * time.Minute)
	alreadyNotified := false

	channel, _ := gocore.Config().Get("slack_channel")
	clientName, _ := gocore.Config().Get("asset_clientName")

	for range ticker.C {
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			res, err := c.getBalance(ctx)
			if err != nil {
				c.logger.Errorf("could not get balance: %v", err)
				return
			}

			availableUtxos := res.GetNumberOfUtxos()

			if availableUtxos < threshold && !alreadyNotified {
				c.logger.Warnf("*Spending Threshold Warning - %s*\nSpendable utxos (%s) has fallen below threshold of %s", clientName, comma(availableUtxos), comma(threshold))
				if channel != "" {
					if err := postMessageToSlack(channel, fmt.Sprintf("*Spending Threshold Warning - %s*\nSpendable utxos (%s) has fallen below threshold of %s", clientName, comma(availableUtxos), comma(threshold))); err != nil {
						c.logger.Warnf("could not post to slack: %v", err)
					}
				}
				alreadyNotified = true
			} else if availableUtxos >= threshold && alreadyNotified {
				alreadyNotified = false
			}
		}()
	}
}

func comma(value uint64) string {
	str := fmt.Sprintf("%d", value)
	n := len(str)
	if n <= 3 {
		return str
	}

	var b strings.Builder
	for i, c := range str {
		if i > 0 && (n-i)%3 == 0 {
			b.WriteRune(',')
		}
		b.WriteRune(c)
	}
	return b.String()
}
