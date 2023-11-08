package coinbase

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blobserver"
	"github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/distributor"
	"github.com/lib/pq"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bk/wif"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/libsv/go-bt/v2/unlocker"
	"github.com/ordishs/go-utils"
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
	db               *sql.DB
	engine           util.SQLEngine
	store            blockchain.Store
	blobServerClient *blobserver.Client
	distributor      *distributor.Distributor
	privateKey       *bec.PrivateKey
	running          bool
	blockFoundCh     chan processBlockFound
	catchupCh        chan processBlockCatchup
	logger           utils.Logger
	address          string
	dbTimeout        time.Duration
}

// NewCoinbase builds on top of the blockchain store to provide a coinbase tracker
// Only SQL databases are supported
func NewCoinbase(logger utils.Logger, store blockchain.Store) (*Coinbase, error) {
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

	d, err := distributor.NewDistributor(logger, distributor.WithBackoffDuration(1*time.Second), distributor.WithRetryAttempts(3), distributor.WithFailureTolerance(0))
	if err != nil {
		return nil, fmt.Errorf("could not create distributor: %v", err)
	}

	dbTimeoutMillis, _ := gocore.Config().GetInt("blockchain_store_dbTimeoutMillis", 5000)

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

	if _, err := c.db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS ux_coinbase_utxos_txid_vout ON coinbase_utxos (txid, vout);`); err != nil {
		_ = c.db.Close()
		return fmt.Errorf("could not create ux_coinbase_utxos_txid_vout index - [%+v]", err)
	}

	if _, err := c.db.Exec(`CREATE INDEX IF NOT EXISTS ux_coinbase_utxos_processed_at ON coinbase_utxos (processed_at ASC);`); err != nil {
		_ = c.db.Close()
		return fmt.Errorf("could not create ux_coinbase_utxos_processed_at index - [%+v]", err)
	}

	if _, err := c.db.Exec(`CREATE INDEX IF NOT EXISTS ux_spendable_utxos_inserted_at ON spendable_utxos (inserted_at ASC);`); err != nil {
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

func (c *Coinbase) storeBlock(cntxt context.Context, block *model.Block) error {
	start, stat, ctx := util.StartStatFromContext(cntxt, "storeBlock")
	defer func() {
		stat.AddTime(start)
	}()

	ctxTimeout, cancelTimeout := context.WithTimeout(ctx, c.dbTimeout)
	defer cancelTimeout()

	blockId, err := c.store.StoreBlock(ctxTimeout, block)
	if err != nil {
		return fmt.Errorf("could not store block: %+v", err)
	}

	// process coinbase into utxos
	err = c.processCoinbase(ctxTimeout, blockId, block.Hash(), block.CoinbaseTx)
	if err != nil {
		return fmt.Errorf("could not process coinbase %+v", err)
	}

	return nil
}

func (c *Coinbase) processCoinbase(cntxt context.Context, blockId uint64, blockHash *chainhash.Hash, coinbaseTx *bt.Tx) error {
	start, stat, ctx := util.StartStatFromContext(cntxt, "processCoinbase")
	defer func() {
		stat.AddTime(start)
	}()

	ctx, cancelTimeout := context.WithTimeout(ctx, c.dbTimeout)
	defer cancelTimeout()

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
	go c.createSpendingUtxos(ctx, timestamp)

	return nil
}

func (c *Coinbase) createSpendingUtxos(cntxt context.Context, timestamp time.Time) {
	start, stat, ctx := util.StartStatFromContext(cntxt, "createSpendingUtxos")
	defer func() {
		stat.AddTime(start)
	}()

	ctx, cancelTimeout := context.WithTimeout(ctx, c.dbTimeout)
	defer cancelTimeout()

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
		c.logger.Errorf("could not get coinbase utxos: %+v", err)
		return
	}

	defer rows.Close()

	for rows.Next() {
		var txid []byte
		var vout uint32
		var lockingScript bscript.Script
		var satoshis uint64

		if err := rows.Scan(&txid, &vout, &lockingScript, &satoshis); err != nil {
			c.logger.Errorf("could not scan coinbase utxo: %+v", err)
			return
		}

		hash, err := chainhash.NewHash(txid)
		if err != nil {
			c.logger.Errorf("could not create hash from txid: %+v", err)
			continue
		}

		utxo := &bt.UTXO{
			TxIDHash:      hash,
			Vout:          vout,
			LockingScript: &lockingScript,
			Satoshis:      satoshis,
		}

		c.logger.Infof("createSpendingUtxos coinbase: %s: utxo %d", hash, vout)

		if err := c.splitUtxo(ctx, utxo); err != nil {
			c.logger.Errorf("could not split utxo: %+v", err)
		}
	}
}

func (c *Coinbase) splitUtxo(cntxt context.Context, utxo *bt.UTXO) error {
	start, stat, ctx := util.StartStatFromContext(cntxt, "splitUtxo")
	defer func() {
		stat.AddTime(start)
	}()

	ctx, cancelTimeout := context.WithTimeout(ctx, c.dbTimeout)
	defer cancelTimeout()

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

	if err := c.distributor.SendTransaction(ctx, tx); err != nil {
		return fmt.Errorf("error sending splitting transaction: %v", err)
	}

	// Insert the spendable utxos....
	return c.insertSpendableUTXOs(ctx, tx)
}

func (c *Coinbase) RequestFunds(cntxt context.Context, address string) (*bt.Tx, error) {
	ctx, cancelTimeout := context.WithTimeout(cntxt, c.dbTimeout)
	defer cancelTimeout()

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

	// Split the utxo into 1,000 outputs satoshis
	sats := utxo.Satoshis / 1000
	remainder := utxo.Satoshis % 1000

	for i := 0; i < 1000; i++ {
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

	if err = c.distributor.SendTransaction(ctx, tx); err != nil {
		return nil, fmt.Errorf("error sending initial transaction: %v", err)
	}

	c.logger.Debugf("Sent funding transaction %s", tx.TxIDChainHash().String())

	return tx, nil
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
func (c *Coinbase) requestFundsSqlite(cntxt context.Context, address string) (*bt.UTXO, error) {
	ctx, cancelTimeout := context.WithTimeout(cntxt, c.dbTimeout)
	defer cancelTimeout()

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

func (c *Coinbase) insertCoinbaseUTXOs(cntxt context.Context, blockId uint64, tx *bt.Tx) error {
	ctx, cancelTimeout := context.WithTimeout(cntxt, c.dbTimeout)
	defer cancelTimeout()

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

func (c *Coinbase) insertSpendableUTXOs(cntxt context.Context, tx *bt.Tx) error {
	ctx, cancelTimeout := context.WithTimeout(cntxt, c.dbTimeout)
	defer cancelTimeout()

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
			return fmt.Errorf("could not insert spendable utxo: %+v", err)
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
