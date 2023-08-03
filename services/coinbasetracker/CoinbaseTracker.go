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
	// do sync up between the db and the network
}

func (ct *CoinbaseTracker) Stop() error {
	ct.ch <- true
	close(ct.ch)
	return nil
}

func (ct *CoinbaseTracker) GetBestBlockFromDb(ctx context.Context) (*model.Block, error) {
	m := &model.Block{}
	err := ct.store.Read(m)
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
