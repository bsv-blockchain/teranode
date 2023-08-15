package coinbasetracker

import (
	"context"
	// "database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/TAAL-GmbH/ubsv/db"
	"github.com/TAAL-GmbH/ubsv/db/base"
	"github.com/TAAL-GmbH/ubsv/db/model"
	sqlerr "github.com/mattn/go-sqlite3"
	"gorm.io/gorm"

	// networkModel "github.com/TAAL-GmbH/ubsv/model"
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
	lock             sync.Mutex
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

	// synchronize_reserved, _ := gocore.Config().GetInt("coinbasetracker_timeout_reserved", 3600)

	ct := &CoinbaseTracker{
		logger:           logger,
		blockchainClient: blockchainClient,
		store:            db.Create(store, store_config),
		lock:             sync.Mutex{},
	}

	// go ct.synchronize(synchronize_reserved)

	return ct
}

// func (ct *CoinbaseTracker) synchronize(timeout int) {
// 	timer := time.NewTimer(time.Second * 10)
// 	for {
// 		select {
// 		case <-ct.ch:
// 			return
// 		case <-timer.C:
// 			ct.manageReserved(timeout)
// 			timer = time.NewTimer(time.Second * 10)
// 		}
// 	}
// }

// func (ct *CoinbaseTracker) manageReserved(timeout int) {
// 	// select all utxos where reserved is set and the time between
// 	// UpdatedAt and now exceeds the timeout value
// 	// If such utxos are found - unset reserved back to false
// 	err := ct.ResetUtxoReserved(context.Background(), timeout)
// 	if err != nil {
// 		ct.logger.Errorf("failed to reset reserved utxos: %s", err.Error())
// 	}
// }

func (ct *CoinbaseTracker) Stop() error {
	return nil
}

func (ct *CoinbaseTracker) GetUtxo(ctx context.Context, address string) (*bt.UTXO, error) {
	// start transaction
	ct.lock.Lock()
	defer ct.lock.Unlock()

	stmt := "SELECT ID, txid, vout, locking_script, satoshis FROM utxos WHERE address = ? AND status = '1' LIMIT 1"
	vals := []interface{}{address}
	payload, err := ct.store.TxSelectForUpdate(nil, stmt, vals)
	if err != nil {
		ct.logger.Errorf("Tx Select for update: %s", err.Error())
		return nil, err
	}
	if len(payload) != 1 {
		ct.logger.Errorf("[\U0001F6AB] no utxos found for address %s", address)
		return nil, errors.New("did not find any utxos")
	}
	// Check if we have utxo amounts that cover the amount
	utxo_candidates := []*model.UTXO{}
	u, _ := payload[0].(*model.UTXO)
	utxo_candidates = append(utxo_candidates, u)

	// return the first utxo that satisfies the given amount
	s, _ := hex.DecodeString(utxo_candidates[0].LockingScript)
	script := bscript.Script(s)
	txId, _ := hex.DecodeString(utxo_candidates[0].Txid)

	stmt = "txid = ? AND vout = ? AND satoshis = ?"
	i := ct.store.GetDB()
	tx, _ := i.(*gorm.DB)
	t1 := time.Now()
	utxoIds := []interface{}{utxo_candidates[0].Txid, utxo_candidates[0].Vout, utxo_candidates[0].Satoshis}

	tx = tx.Model(&model.UTXO{}).Where(stmt, utxoIds...).Updates(map[string]interface{}{"status": 2})
	if tx.Error == nil {
		ct.logger.Warnf("reserve utoxs took %d seconds", time.Since(t1).Seconds())
	}
	return &bt.UTXO{
		TxID:          txId,
		Vout:          utxo_candidates[0].Vout,
		LockingScript: &script,
		Satoshis:      utxo_candidates[0].Satoshis,
	}, err
}

func (ct *CoinbaseTracker) SetUtxoSpent(ctx context.Context, txids []interface{}) error {
	var stmt string
	if len(txids) > 1 {
		stmt = "ID IN ? and status = 2"
	} else {
		stmt = "ID = ? and status = 2"
	}
	return ct.store.UpdateBatch("utxos", stmt, txids, map[string]interface{}{"spent": true, "reserved": false})
}

func (ct *CoinbaseTracker) SetUtxoReserved(ctx context.Context, i any, utxoIds []interface{}) error {
	var stmt string
	if len(utxoIds) > 1 {
		stmt = "ID IN (?) AND status = 1"
	} else {
		stmt = "ID = ? AND status = 1"
	}
	tx, ok := i.(*gorm.DB)
	if !ok {
		panic("could not cast to gorm.DB")
	}
	var err error
	retries := 0
	savepoint := fmt.Sprintf("set-utxo-reserved-%s", time.Now().String())
	tx = tx.SavePoint(savepoint)
	t1 := time.Now()
	for {
		err = ct.store.TxUpdateBatch(tx, &model.UTXO{}, stmt, utxoIds, map[string]interface{}{"status": model.StatusReserved})
		if err != nil {
			e, ok := err.(sqlerr.Error)
			if ok {
				ct.logger.Errorf("%s : code %d", e.Error(), e.ExtendedCode)
			} else {
				if tx != nil {
					tx.RollbackTo(savepoint)
				}
			}
			time.Sleep(250 * time.Millisecond)
			retries++
			continue
		}
		if retries > 1 {
			ct.logger.Errorf("\U000026A0 reserving utxos succeeded after %d tries and took %f sec(s)", retries, time.Since(t1).Seconds())
		}
		break
	}
	return err
}

func (ct *CoinbaseTracker) UpdateUtxoReserved(ctx context.Context, tx any, dur time.Duration) error {
	vals := []interface{}{time.Now().Add(-dur), false, true}
	return ct.store.TxUpdateBatch(tx, "utxos", "updated_at < ? AND status = 2", vals, map[string]interface{}{"status": model.StatusSpendable})
}

func (ct *CoinbaseTracker) SubmitTransaction(ctx context.Context, utxo *model.UTXO) error {
	// send to node
	// set utxos as spent
	err := LockSpent(ct.store, ct.logger, utxo)
	if err != nil {
		ct.logger.Errorf("failed to lock spent: %s", err.Error())
	}
	return nil
}
