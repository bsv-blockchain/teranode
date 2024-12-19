package coinbase

import (
	"context"
	"database/sql"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	bc "github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/coinbase/coinbase_api"
	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/stores/blockchain"
	"github.com/bitcoin-sv/ubsv/tracing"
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
	"golang.org/x/sync/errgroup"
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
	db               *usql.DB
	settings         *settings.Settings
	engine           util.SQLEngine
	blockchainClient bc.ClientI
	store            blockchain.Store
	distributor      *distributor.Distributor
	privateKey       *bec.PrivateKey
	running          bool
	blockFoundCh     chan processBlockFound
	catchupCh        chan processBlockCatchup
	logger           ulogger.Logger
	address          string
	dbTimeout        time.Duration
	peerSync         *p2p.PeerHeight
	g                *errgroup.Group
	gCtx             context.Context
	stats            *gocore.Stat
	minConfirmations uint16
}

// NewCoinbase builds on top of the blockchain store to provide a coinbase tracker
// Only SQL databases are supported
func NewCoinbase(logger ulogger.Logger, tSettings *settings.Settings, blockchainClient bc.ClientI, store blockchain.Store) (*Coinbase, error) {
	tSettings.Policy.MinMiningTxFee = 0

	engine := store.GetDBEngine()
	if engine != util.Postgres && engine != util.Sqlite && engine != util.SqliteMemory {
		return nil, errors.NewStorageError("unsupported database engine: %s", engine)
	}

	coinbasePrivKey := tSettings.Coinbase.WalletPrivateKey
	if coinbasePrivKey == "" {
		return nil, errors.NewConfigurationError("coinbase_wallet_private_key not found in config")
	}

	privateKey, err := wif.DecodeWIF(coinbasePrivKey)
	if err != nil {
		return nil, errors.NewConfigurationError("can't decode coinbase priv key: ^%v", err)
	}

	coinbaseAddr, err := bscript.NewAddressFromPublicKey(privateKey.PrivKey.PubKey(), true)
	if err != nil {
		return nil, errors.NewConfigurationError("can't create coinbase address", err)
	}

	backoffDuration := tSettings.Coinbase.DistributorBackoffDuration

	maxRetries := tSettings.Coinbase.DistributorMaxRetries

	failureTolerance := tSettings.Coinbase.DistributorFailureTolerance

	d, err := distributor.NewDistributor(context.Background(), logger, tSettings, distributor.WithBackoffDuration(backoffDuration), distributor.WithRetryAttempts(int32(maxRetries)), distributor.WithFailureTolerance(failureTolerance)) //nolint:gosec
	if err != nil {
		return nil, errors.NewServiceError("could not create distributor", err)
	}

	dbTimeoutMillis := tSettings.BlockChain.StoreDBTimeoutMillis

	addresses := tSettings.Propagation.GRPCAddresses
	if len(addresses) == 0 {
		return nil, errors.NewConfigurationError("[PeerStatus] propagation_grpcAddresses not found")
	}

	numberOfExpectedPeers := len(addresses)

	peerStatusTimeout := tSettings.Coinbase.PeerStatusTimeout

	minConfirmations := tSettings.ChainCfgParams.CoinbaseMaturity

	g, gCtx := errgroup.WithContext(context.Background())
	g.SetLimit(runtime.NumCPU())

	peerSync, err := p2p.NewPeerHeight(logger, tSettings, "coinbase", numberOfExpectedPeers, peerStatusTimeout)
	if err != nil {
		return nil, errors.NewServiceError("could not create peer sync service", err)
	}

	c := &Coinbase{
		blockchainClient: blockchainClient,
		settings:         tSettings,
		store:            store,
		db:               store.GetDB(),
		engine:           engine,
		blockFoundCh:     make(chan processBlockFound, 100),
		catchupCh:        make(chan processBlockCatchup),
		distributor:      d,
		logger:           logger,
		privateKey:       privateKey.PrivKey,
		address:          coinbaseAddr.AddressString,
		dbTimeout:        time.Duration(dbTimeoutMillis) * time.Millisecond,
		peerSync:         peerSync,
		g:                g,
		gCtx:             gCtx,
		stats:            gocore.NewStat("coinbase"),
		minConfirmations: minConfirmations,
	}

	threshold := tSettings.Coinbase.NotificationThreshold
	if threshold > 0 {
		go c.monitorSpendableUTXOs(uint64(threshold))
	}

	return c, nil
}

func (c *Coinbase) Init(ctx context.Context) (err error) {
	if err = c.createTables(ctx); err != nil {
		return errors.NewProcessingError("failed to create coinbase tables", err)
	}

	notification, err := c.blockchainClient.Subscribe(ctx, "coinbase")
	if err != nil {
		return errors.NewServiceError("failed to subscribe to coinbase notifications", err)
	}

	// process notifications from blockchainClient subscription
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case n := <-notification:
				{
					// convert hash to chainhash
					hash, err := chainhash.NewHash(n.Hash)
					if err != nil {
						c.logger.Errorf("[Coinbase] failed to convert hash to chainhash: %s", err)
						continue
					}

					if n.Type == model.NotificationType_Block {
						c.blockFoundCh <- processBlockFound{
							hash:    hash,
							baseURL: n.Base_URL,
						}
					}
				}
			}
		}
	}()

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
		return errors.NewStorageError("unsupported database engine: %s", c.engine)
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
	//nolint:gocritic
	switch c.engine {
	case util.Postgres:
		if _, err := c.db.ExecContext(ctx, `
			CREATE TABLE IF NOT EXISTS spendable_utxos_log (
				id BIGSERIAL PRIMARY KEY,
				 change_type CHAR(1)  -- 'I' for insert, 'D' for delete, 'U' for update
				,utxo_count_change BIGINT DEFAULT 0
				,satoshis_change BIGINT DEFAULT 0
			);
			`); err != nil {
			return err
		}

		if _, err := c.db.ExecContext(ctx, `
			CREATE TABLE IF NOT EXISTS spendable_utxos_balance (
				utxo_count BIGINT DEFAULT 0
				,total_satoshis BIGINT DEFAULT 0
			);
			`); err != nil {
			return err
		}

		if _, err := c.db.ExecContext(ctx, `
			INSERT INTO spendable_utxos_balance (
				 utxo_count
				,total_satoshis
			) SELECT
					 COALESCE(COUNT(*), 0)
					,COALESCE(SUM(satoshis), 0)
				FROM
					spendable_utxos;
		`); err != nil {
			return err
		}

		if _, err := c.db.ExecContext(ctx, `
				CREATE OR REPLACE FUNCTION log_spendable_utxos_balance()
				RETURNS TRIGGER AS $$
				BEGIN
						IF OLD IS NULL THEN
								-- INSERT
								INSERT INTO spendable_utxos_log (change_type, utxo_count_change, satoshis_change)
								VALUES ('I', 1, NEW.satoshis);
						ELSIF NEW IS NULL THEN
								-- DELETE
								INSERT INTO spendable_utxos_log (change_type, utxo_count_change, satoshis_change)
								VALUES ('D', -1, -OLD.satoshis);
						ELSE
								-- UPDATE
								INSERT INTO spendable_utxos_log (change_type, utxo_count_change, satoshis_change)
								VALUES ('U', 0, NEW.satoshis - OLD.satoshis);
						END IF;

						RETURN NULL;
				END;
				$$ LANGUAGE plpgsql;
			`); err != nil {
			return err
		}

		if _, err := c.db.ExecContext(ctx, `
				CREATE OR REPLACE TRIGGER trigger_spendable_utxos_log
				AFTER INSERT OR UPDATE OR DELETE ON spendable_utxos
				FOR EACH ROW
				EXECUTE FUNCTION log_spendable_utxos_balance();
			`); err != nil {
			return err
		}

		// this need the cron extension to be install on postgres server
		// for the moment (until this solution is proven viable) we will use a go routine
		// to update the spendable balance (see monitorSpendableUTXOs)
		// if _, err := c.db.ExecContext(ctx, `
		// SELECT cron.schedule('*/1 * * * *', $$ -- Run every minute
		// DECLARE
		// 	total_changes RECORD;
		// BEGIN
		// 	SELECT
		// 			COALESCE(SUM(utxo_count_change), 0) AS total_utxo_count
		// 			,COALESCE(SUM(satoshis_change), 0) AS total_satoshis
		// 	INTO total_changes
		// 	FROM spendable_utxos_log;

		// 	UPDATE spendable_utxos_balance
		// 	SET
		// 		utxo_count = utxo_count + total_changes.total_utxo_count
		// 		,total_satoshis = total_satoshis + total_changes.total_satoshis;

		// 	DELETE FROM spendable_utxos_log;
		// END;
		// $$);
		// 	`); err != nil {
		// 	return err
		// }

		if _, err := c.db.ExecContext(ctx, `
				CREATE OR REPLACE FUNCTION get_spendable_utxos_balance()
				RETURNS TABLE (utxo_count BIGINT, total_satoshis BIGINT) AS $$
				BEGIN
					RETURN QUERY

					SELECT
						 sb.utxo_count + CAST(sl.utxo_count_change AS BIGINT) AS utxo_count
						,sb.total_satoshis + CAST(sl.satoshis_change AS BIGINT) AS total_satoshis
					FROM
						 spendable_utxos_balance sb
						,(
								SELECT
										 COALESCE(SUM(utxo_count_change), 0) AS utxo_count_change
										,COALESCE(SUM(satoshis_change), 0) AS satoshis_change
								FROM
										spendable_utxos_log
						) sl;
				END;
				$$ LANGUAGE plpgsql;
			`); err != nil {
			return err
		}
	} // end switch

	if _, err := c.db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS ux_coinbase_utxos_txid_vout ON coinbase_utxos (txid, vout);`); err != nil {
		_ = c.db.Close()
		return errors.NewStorageError("could not create ux_coinbase_utxos_txid_vout index - [%+v]", err)
	}

	if _, err := c.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS ux_coinbase_utxos_processed_at ON coinbase_utxos (processed_at ASC);`); err != nil {
		_ = c.db.Close()
		return errors.NewStorageError("could not create ux_coinbase_utxos_processed_at index - [%+v]", err)
	}

	if _, err := c.db.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS ux_spendable_utxos_inserted_at ON spendable_utxos (inserted_at ASC);`); err != nil {
		_ = c.db.Close()
		return errors.NewStorageError("could not create ux_spendable_utxos_inserted_at index - [%+v]", err)
	}

	return nil
}

func (c *Coinbase) catchup(cntxt context.Context, fromBlock *model.Block, baseURL string) error {
	start, stat, ctx := tracing.NewStatFromContext(cntxt, "catchup", c.stats)
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
		blockHeaders, _, err := c.blockchainClient.GetBlockHeaders(ctx, fromBlockHeaderHash, 1000)
		if err != nil {
			return errors.NewServiceError("failed to get block headers from [%s]", fromBlockHeaderHash.String(), err)
		}

		if len(blockHeaders) == 0 {
			return errors.NewServiceError("failed to get block headers from [%s]", fromBlockHeaderHash.String())
		}

		for _, blockHeader := range blockHeaders {
			exists, err = c.store.GetBlockExists(ctx, blockHeader.Hash())
			if err != nil {
				return errors.NewServiceError("failed to check if block exists", err)
			}
			if exists {
				break LOOP
			}

			catchupBlockHeaders = append(catchupBlockHeaders, blockHeader)

			fromBlockHeaderHash = blockHeader.HashPrevBlock
			if fromBlockHeaderHash.IsEqual(&chainhash.Hash{}) {
				return errors.NewProcessingError("failed to find parent block header, last was: %s", blockHeader.String())
			}
		}
	}

	c.logger.Infof("catching up from [%s] to [%s]", catchupBlockHeaders[len(catchupBlockHeaders)-1].String(), catchupBlockHeaders[0].String())

	// process the catchup block headers in reverse order
	for i := len(catchupBlockHeaders) - 1; i >= 0; i-- {
		blockHeader := catchupBlockHeaders[i]

		block, err := c.blockchainClient.GetBlock(ctx, blockHeader.Hash())
		if err != nil {
			return errors.NewServiceError("failed to get block [%s]", blockHeader.String(), err)
		}

		err = c.storeBlock(ctx, block)
		if err != nil {
			// storeBlock will have already wrapped the error properly
			return err
		}
	}

	return nil
}

func (c *Coinbase) processBlock(cntxt context.Context, blockHash *chainhash.Hash, baseURL string) (*model.Block, error) {
	start, stat, ctx := tracing.NewStatFromContext(cntxt, "processBlock", coinbaseStat)
	defer func() {
		stat.AddTime(start)
	}()

	c.logger.Debugf("processing block: %s", blockHash.String())

	exists, err := c.store.GetBlockExists(ctx, blockHash)
	if err != nil {
		return nil, errors.NewStorageError("could not check whether block exists", err)
	}

	if exists {
		c.logger.Debugf("skipping block that already exists: %s", blockHash.String())
		return nil, nil
	}

	block, err := c.blockchainClient.GetBlock(ctx, blockHash)
	if err != nil {
		return block, err
	}

	// check whether we already have the parent block
	exists, err = c.store.GetBlockExists(ctx, block.Header.HashPrevBlock)
	if err != nil {
		return nil, errors.NewStorageError("could not check whether block exists", err)
	}

	if !exists {
		go func() {
			c.catchupCh <- processBlockCatchup{
				block:   block,
				baseURL: baseURL,
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
	ctx, _, deferFn := tracing.StartTracing(ctx, "storeBlock")
	defer deferFn()

	// ctxTimeout, cancelTimeout := context.WithTimeout(ctx, c.dbTimeout)
	// defer cancelTimeout()

	blockId, height, err := c.store.StoreBlock(ctx, block, "") //nolint:stylecheck
	if err != nil {
		return errors.NewStorageError("could not store block", err)
	}

	if c.settings.Coinbase.WaitForPeers {
		/* Wait until all nodes are at least on same block height as this coinbase block */
		/* Do this before attempting to distribute the coinbase splitting transactions to all nodes */
		err = c.peerSync.WaitForAllPeers(ctx, height, true)
		if err != nil {
			return errors.NewError("peers are not in sync", err)
		}
	}

	err = c.processCoinbase(ctx, blockId, block.Hash(), block.CoinbaseTx)
	if err != nil {
		return errors.NewProcessingError("could not process coinbase", err)
	}

	return nil
}

func (c *Coinbase) processCoinbase(ctx context.Context, blockId uint64, blockHash *chainhash.Hash, coinbaseTx *bt.Tx) error { //nolint:stylecheck
	ctx, stat, deferFn := tracing.StartTracing(ctx, "processCoinbase")
	defer deferFn()

	// ctx, cancelTimeout := context.WithTimeout(ctx, c.dbTimeout)
	// defer cancelTimeout()

	c.logger.Infof("processing coinbase: %s, for block: %s with %d utxos", coinbaseTx.TxID(), blockHash.String(), len(coinbaseTx.Outputs))

	if err := c.insertCoinbaseUTXOs(ctx, blockId, coinbaseTx); err != nil {
		return errors.NewProcessingError("could not insert coinbase utxos", err)
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
			WHERE height <= (SELECT height - $2 FROM LongestChainTip)
		)
	`
	if _, err := c.db.ExecContext(ctx, q, timestamp, c.minConfirmations); err != nil {
		return errors.NewStorageError("could not update coinbase_utxos to be processed", err)
	}

	_, _, ctx = tracing.NewStatFromContext(context.Background(), "go routine", stat, false)

	if err := c.createSpendingUtxos(ctx, timestamp); err != nil {
		return errors.NewProcessingError("could not create spending utxos", err)
	}

	return nil
}

func (c *Coinbase) createSpendingUtxos(ctx context.Context, timestamp time.Time) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "createSpendingUtxos")
	defer deferFn()

	// ctx, cancelTimeout := context.WithTimeout(ctx, c.dbTimeout)
	// defer cancelTimeout()

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
		return errors.NewStorageError("could not get coinbase utxos", err)
	}

	defer rows.Close()

	utxos := make([]*bt.UTXO, 0)

	for rows.Next() {
		var txid []byte

		var vout uint32

		var lockingScript bscript.Script

		var satoshis uint64

		if err = rows.Scan(&txid, &vout, &lockingScript, &satoshis); err != nil {
			return errors.NewProcessingError("could not scan coinbase utxo", err)
		}

		hash, err := chainhash.NewHash(txid)
		if err != nil {
			return errors.NewProcessingError("could not create hash from txid", err)
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
		ch := make(chan struct{})
		c.g.Go(func() error {
			defer close(ch)

			if err := c.splitUtxo(c.gCtx, utxo); err != nil {
				return errors.NewProcessingError("could not split utxo", err)
			}
			// TODO remove this when we start using postgres cron job to do this periodically
			if err := c.aggregateBalance(ctx); err != nil {
				return errors.NewProcessingError("could not aggregate balance", err)
			}

			return nil
		})
		<-ch
	}

	return nil
}

func (c *Coinbase) splitUtxo(ctx context.Context, utxo *bt.UTXO) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "splitUtxo")
	defer deferFn()

	tx := bt.NewTx()

	if err := tx.FromUTXOs(utxo); err != nil {
		return errors.NewProcessingError("error creating initial transaction", err)
	}

	var splitSatoshis = uint64(10_000_000)

	amountRemaining := utxo.Satoshis

	for amountRemaining > splitSatoshis {
		select {
		case <-ctx.Done():
			return errors.NewContextCanceledError("timeout splitting the satoshis")
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
		return errors.NewProcessingError("error filling splitting inputs", err)
	}

	c.logger.Infof("[splitUtxo] sending splitting tx %s for coinbase tx %s vout %d", tx.TxIDChainHash().String(), utxo.TxIDHash, utxo.Vout)

	if !c.settings.Coinbase.TestMode {
		if _, err := c.distributor.SendTransaction(ctx, tx); err != nil {
			return errors.NewServiceError("error sending splitting tx %s for coinbase tx %s vout %d", tx.TxIDChainHash().String(), utxo.TxIDHash, utxo.Vout, err)
		}
	}

	// Insert the spendable utxos....
	return c.insertSpendableUTXOs(ctx, tx)
}

func (c *Coinbase) RequestFunds(ctx context.Context, address string, disableDistribute bool) (*bt.Tx, error) {
	var utxo *bt.UTXO

	var err error

	switch c.engine {
	case util.Postgres:
		utxo, err = c.requestFundsPostgres(ctx, address)
	case util.Sqlite, util.SqliteMemory:
		utxo, err = c.requestFundsSqlite(ctx, address)
	default:
		return nil, errors.NewStorageError("unsupported database engine: %s", c.engine)
	}

	if err != nil {
		return nil, errors.NewError("error requesting funds", err)
	}

	tx := bt.NewTx()

	if err = tx.FromUTXOs(utxo); err != nil {
		return nil, errors.NewProcessingError("error creating initial transaction", err)
	}

	splits := uint64(100)

	// Split the utxo into 100 outputs satoshis
	sats := utxo.Satoshis / splits
	remainder := utxo.Satoshis % splits

	//nolint:gosec
	for i := 0; i < int(splits); i++ {
		if i == 0 && remainder > 0 {
			if err = tx.PayToAddress(address, sats+remainder); err != nil {
				return nil, errors.NewProcessingError("error paying to address", err)
			}

			continue
		}

		if err = tx.PayToAddress(address, sats); err != nil {
			return nil, errors.NewProcessingError("error paying to address", err)
		}
	}

	unlockerGetter := unlocker.Getter{PrivateKey: c.privateKey}
	if err = tx.FillAllInputs(ctx, &unlockerGetter); err != nil {
		return nil, errors.NewProcessingError("error filling initial inputs", err)
	}

	if !disableDistribute {
		if _, err = c.distributor.SendTransaction(ctx, tx); err != nil {
			return nil, errors.NewServiceError("error sending initial transaction", err)
		}

		c.logger.Debugf("Sent funding transaction %s", tx.TxIDChainHash().String())
	}

	return tx, nil
}

func (c *Coinbase) DistributeTransaction(ctx context.Context, tx *bt.Tx) ([]*distributor.ResponseWrapper, error) {
	return c.distributor.SendTransaction(ctx, tx)
}

func (c *Coinbase) requestFundsPostgres(ctx context.Context, _ string) (*bt.UTXO, error) {
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
func (c *Coinbase) requestFundsSqlite(ctx context.Context, _ string) (*bt.UTXO, error) {
	// ctx, cancelTimeout := context.WithTimeout(cntxt, c.dbTimeout)
	// defer cancelTimeout()
	txn, err := c.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelWriteCommitted,
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

func (c *Coinbase) insertCoinbaseUTXOs(ctx context.Context, blockId uint64, tx *bt.Tx) error { //nolint:stylecheck
	ctx, _, deferFn := tracing.StartTracing(ctx, "insertCoinbaseUTXOs")
	defer deferFn()

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
		return errors.NewStorageError("unsupported database engine: %s", c.engine)
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
				return errors.NewStorageError("could not insert coinbase utxo", err)
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
	ctx, _, deferFn := tracing.StartTracing(ctx, "insertSpendableUTXOs")
	defer deferFn()

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
		return errors.NewStorageError("unsupported database engine: %s", c.engine)
	}

	for vout, output := range tx.Outputs {
		if _, err = stmt.ExecContext(ctx, hash, vout, output.LockingScript, output.Satoshis); err != nil {
			return errors.NewStorageError("could not insert spendable utxo %s", tx.TxIDChainHash().String(), err)
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

/*
*
Run this periodically to update the spendable balance
Failure to run this will result in a progressively slower getBalance().
Ideally this is a cron job running on the postgres server
but this means installing the cron extension and I don't want to hassle devops
until this is a proven solution.
*/
func (c *Coinbase) aggregateBalance(ctx context.Context) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "aggregateBalance")
	defer deferFn()
	//nolint:gocritic
	switch c.engine {
	case util.Postgres:
		if _, err := c.db.ExecContext(ctx, `
				DO $$
				DECLARE
					total_changes RECORD;
				BEGIN
					SELECT
						 COALESCE(SUM(utxo_count_change), 0) AS total_utxo_count
						,COALESCE(SUM(satoshis_change), 0) AS total_satoshis
					INTO total_changes
					FROM spendable_utxos_log;

					UPDATE spendable_utxos_balance
					SET
						 utxo_count = utxo_count + total_changes.total_utxo_count
						,total_satoshis = total_satoshis + total_changes.total_satoshis;

					DELETE FROM spendable_utxos_log;
				END;
				$$;
			`); err != nil {
			return err
		}
	} // switch

	return nil
}

func (c *Coinbase) getBalance(ctx context.Context) (*coinbase_api.GetBalanceResponse, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "getBalance")
	defer deferFn()

	res := &coinbase_api.GetBalanceResponse{}

	switch c.engine {
	case util.Postgres:
		if err := c.db.QueryRowContext(ctx, `
			SELECT * FROM get_spendable_utxos_balance()
		`).Scan(&res.NumberOfUtxos, &res.TotalSatoshis); err != nil {
			return nil, err
		}
	default:
		if err := c.db.QueryRowContext(ctx, `
			SELECT
		   COUNT(*)
			,COALESCE(SUM(satoshis), 0)
			FROM spendable_utxos;
		`).Scan(&res.NumberOfUtxos, &res.TotalSatoshis); err != nil {
			return nil, err
		}
	} // switch

	return res, nil
}

func (c *Coinbase) monitorSpendableUTXOs(threshold uint64) {
	ticker := time.NewTicker(1 * time.Minute)
	alreadyNotified := false

	channel := c.settings.Coinbase.SlackChannel
	clientName := c.settings.ClientName

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
					if err := postMessageToSlack(channel, fmt.Sprintf("*Spending Threshold Warning - %s*\nSpendable utxos (%s) has fallen below threshold of %s", clientName, comma(availableUtxos), comma(threshold)), c.settings.Coinbase.SlackToken); err != nil {
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
