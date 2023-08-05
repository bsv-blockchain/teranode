package coinbasetracker

import (
	"context"
	"strconv"
	"time"

	"github.com/TAAL-GmbH/ubsv/db"
	"github.com/TAAL-GmbH/ubsv/db/base"
	"github.com/TAAL-GmbH/ubsv/db/model"
	networkModel "github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type CoinbaseTracker struct {
	logger           utils.Logger
	blockchainClient blockchain.ClientI
	store            base.DbManager
	ch               chan bool
}

func NewCoinbaseTracker(logger utils.Logger, blockchainClient blockchain.ClientI) *CoinbaseTracker {

	store, ok := gocore.Config().Get("coinbasetracker_store")
	if !ok {
		logger.Warnf("coinbasetracker_store is not set. Using sqlite.")
		store = "sqlite"
	}

	store_config, ok := gocore.Config().Get("coinbasetracker_store_config")
	if !ok {
		logger.Warnf("coinbasetracker_store_config is not set. Using sqlite in-mem.")
		store_config = "file::memory:?cache=shared"
	}

	ct := &CoinbaseTracker{
		logger:           logger,
		blockchainClient: blockchainClient,
		store:            db.Create(store, store_config),
		ch:               make(chan bool),
	}

	go ct.synchronize()

	return ct
}

func (ct *CoinbaseTracker) synchronize() {
	timer := time.NewTimer(time.Millisecond * 100)
	for {
		select {
		case <-ct.ch:
			return
		case <-timer.C:
			ct.syncUp()
			timer = time.NewTimer(time.Millisecond * 100)
		}
	}
}

func (ct *CoinbaseTracker) syncUp() {
	block_network, err := ct.GetBestBlockFromNetwork(context.Background())
	if err != nil {
		ct.logger.Errorf("failed to get best block from network: %s", err.Error())
		return
	}
	block_db, err := ct.GetBestBlockFromDb(context.Background())
	if err != nil {
		ct.logger.Errorf("failed to get best block from database: %s", err.Error())
		return
	}
	if !block_db.Equal(block_network) {
		err = ct.AddBlock(context.Background(), block_network)
		if err != nil {
			ct.logger.Errorf("failed to add block to the store: %s", err.Error())
			return
		}
	}
	ct.checkStoreForBlockGaps()
}

func (ct *CoinbaseTracker) checkStoreForBlockGaps() {
	// How do we detect the latest block?
	// The latest block is not referenced by any other blocks
	// and has the current max height
	block_db, err := ct.GetBestBlockFromDb(context.Background())
	if err != nil {
		ct.logger.Errorf("failed to get best block from database: %s", err.Error())
		return
	}
	// At this point we should have the latest network block in the database
	if IsGenesisBlock(block_db) {
		// This is the genesis block - nothing else to do
		return
	}
	// loop through until genesis block is reached, requesting missing blocks
	// along the way
	block_hash := block_db.PrevBlockHash
	for {
		cond := []interface{}{"blockhash = ? ", block_hash}
		prev_block_i, err := ct.store.Read_Cond(&model.Block{}, cond)
		if err != nil {
			ct.logger.Errorf("failed to read block %s from database: %s", &block_hash, err.Error())
		}
		if prev_block_i == nil {
			// missing block condition
			block_network, err := ct.GetBlockFromNetwork(context.Background(), block_hash)
			if err != nil {
				ct.logger.Errorf("failed to get network block %s, %s", block_hash, err.Error())
				break
			}
			block_db, err := NetworkBlockToStoreBlock(block_network)
			if err != nil {
				ct.logger.Errorf("failed to translate network block %s to store block, %s", block_hash, err.Error())
				break
			}

			err = ct.AddBlock(context.Background(), block_db)
			if err != nil {
				ct.logger.Errorf("failed to add block %s to store, %s", block_hash, err.Error())
				panic(err.Error())
			}
			block_hash = block_db.PrevBlockHash
		} else {
			prev_block := prev_block_i.(*model.Block)
			// If the previous block is genesis block - all is good and nothing to do
			if IsGenesisBlock(prev_block) {
				return
			}
			block_hash = prev_block.PrevBlockHash
		}
	}
}

func (ct *CoinbaseTracker) Stop() error {
	ct.ch <- true
	close(ct.ch)
	return nil
}

func (ct *CoinbaseTracker) GetBestBlockFromDb(ctx context.Context) (*model.Block, error) {
	m := &model.Block{}
	err := ct.store.Read(m) // TODO: change logic to retrieve truly best block
	return m, err
}
func (u *CoinbaseTracker) GetBestBlockFromNetwork(ctx context.Context) (*model.Block, error) {
	return nil, nil
}
func (u *CoinbaseTracker) GetBlockFromNetwork(ctx context.Context, hash string) (*networkModel.Block, error) {
	// get block from network
	// unmarshal block
	// 	b, err := networkModel.NewBlockFromBytes()
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	coinbaseTx := b.CoinbaseTx
	// for _, utxo := range coinbaseTx.Outputs{
	// 	// store coinbase
	// }

	return nil, nil
}

// AddBlock will add a block to the database
func (ct *CoinbaseTracker) AddBlock(ctx context.Context, block *model.Block) error {
	return ct.store.Create(block)
}

// AddUtxo will add a utxo to the database
func (ct *CoinbaseTracker) AddUtxo(ctx context.Context, utxo *model.UTXO) error {
	return ct.store.Create(utxo)
}

func (ct *CoinbaseTracker) GetUtxos(ctx context.Context, address string, amount uint64) ([]*bt.UTXO, error) {
	cond := []interface{}{"address = ? AND amount = ?", address, strconv.FormatInt(int64(amount), 10)}
	utxos := []model.UTXO{}
	payload, err := ct.store.Read_All_Cond(&utxos, cond)
	if err != nil {
		return nil, err
	}
	res := []*bt.UTXO{}
	for _, i := range payload {
		v, ok := i.(*model.UTXO)
		if !ok {
			ct.logger.Errorf("received result is not a model.UTXO")
			// should we panic?
			continue
		}
		script := bscript.Script([]byte(v.Script))
		res = append(res, &bt.UTXO{
			TxID:          []byte(v.Txid),
			Vout:          v.Vout,
			LockingScript: &script,
			Satoshis:      v.Amount,
		})
	}
	return res, nil
}

func (ct *CoinbaseTracker) SubmitTransaction(ctx context.Context, transaction []byte) error {
	return nil
}
