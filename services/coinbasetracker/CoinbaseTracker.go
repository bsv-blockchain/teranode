package coinbasetracker

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
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
	ch               chan bool
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
		ch:               make(chan bool),
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
	ct.ch <- true
	close(ct.ch)
	return nil
}

func (ct *CoinbaseTracker) GetUtxos(ctx context.Context, address string, amount uint64) ([]*bt.UTXO, error) {
	// DevNote: Get a combination of UTXOs that satisfy the amount from the lowest
	// number of inputs that are equal or greater than the desired amount
	// or a higher number of inputs that are equal or greater than the desired amount.
	// This is an itterative process where we continue looking for UTXOs that can satisfy the amount
	// the amount.

	// DevNote: This is our first stab at the problem: find a single UTXO that is equal or
	// larger than the desired amount.
	// Need to sort by the lowest to highest amount. The first lowest amount should be sufficient.
	// If none exist, then grab lower amounts and build the best amount combination
	// that satisfies the amount.
	res := []*bt.UTXO{}

	utxoIds := []interface{}{}

	// start transaction
	txopts := []*sql.TxOptions{{Isolation: sql.LevelSerializable, ReadOnly: false}}
	ct.lock.Lock()
	defer ct.lock.Unlock()
	dtx, err := ct.store.TxBegin(txopts...)
	if err != nil {
		ct.logger.Errorf("error starting transaction: %s", err.Error())
		return nil, err
	}

	defer ct.store.TxCommit(dtx)

	stmt := "SELECT ID, txid, vout, locking_script, satoshis FROM utxos WHERE address = ? AND satoshis >= ? AND status = 1"
	vals := []interface{}{address, strconv.FormatInt(int64(amount), 10)}

	payload, err := ct.store.TxSelectForUpdate(dtx, stmt, vals)
	if err != nil {
		ct.logger.Errorf("Tx Select for update: %s", err.Error())
		return nil, err
	}
	// Check if we have utxo amounts that cover the amount
	if len(payload) > 0 {
		utxo_candidates := []*model.UTXO{}
		for _, i := range payload {
			u, ok := i.(*model.UTXO)
			if !ok {
				err := errors.New("received result is not a model.UTXO")
				ct.logger.Errorf("Cannot process one of the UTXO results: %s", err.Error())
				panic(err.Error())
			}
			utxo_candidates = append(utxo_candidates, u)
		}

		sort.Slice(utxo_candidates, func(i, j int) bool {
			return utxo_candidates[i].Satoshis < utxo_candidates[j].Satoshis
		})

		// return the first utxo that satisfies the given amount
		s, _ := hex.DecodeString(utxo_candidates[0].LockingScript)
		script := bscript.Script(s)
		txId, _ := hex.DecodeString(utxo_candidates[0].Txid)
		res = append(res, &bt.UTXO{
			TxID:          txId,
			Vout:          utxo_candidates[0].Vout,
			LockingScript: &script,
			Satoshis:      utxo_candidates[0].Satoshis,
		})
		utxoIds = append(utxoIds, utxo_candidates[0].ID)

	} else {
		// we are at a point where we couldn't find a single transaction that
		// satisfies the given amount; Check if we can find an ideal combination
		// of utxo candidates that satisfy the given amount
		stmt = "SELECT ID, txid, vout, locking_script, satoshis FROM utxos WHERE address = ? AND satoshis > 0 AND satoshis < ? AND status = 1"
		vals = []interface{}{address, strconv.FormatInt(int64(amount), 10)}
		res = []*bt.UTXO{}

		payload, err = ct.store.TxSelectForUpdate(dtx, stmt, vals)

		if err != nil {
			return nil, err
		}
		if len(payload) > 0 {
			utxo_candidates := []*model.UTXO{}
			for _, i := range payload {
				u, ok := i.(*model.UTXO)
				if !ok {
					err := errors.New("received result is not a model.UTXO")
					ct.logger.Errorf("Cannot process one of the UTXO results: %s", err.Error())
					panic(err.Error())
				}
				utxo_candidates = append(utxo_candidates, u)
			}
			if len(payload) > 0 {
				utxo_candidates = []*model.UTXO{}
				for _, i := range payload {
					u, ok := i.(*model.UTXO)
					if !ok {
						err := errors.New("received result is not a model.UTXO")
						ct.logger.Errorf("Cannot process one of the UTXO results: %s", err.Error())
						panic(err.Error())
					}
					utxo_candidates = append(utxo_candidates, u)
				}
				// let's do a reverse sort
				sort.Slice(utxo_candidates, func(i, j int) bool {
					return utxo_candidates[i].Satoshis > utxo_candidates[j].Satoshis
				})

				var total uint64 = 0
				// traverse the utxo candidates from greatest amount down
				// and keep adding tx until the total counter becomes equal to or greater
				// than the desired amount.
				for _, u := range utxo_candidates {
					total += uint64(u.Satoshis)
					s, _ := hex.DecodeString(u.LockingScript)

					script := bscript.Script(s)
					utxoIds = append(utxoIds, u.ID)

					res = append(res, &bt.UTXO{
						TxID:          []byte(u.Txid),
						Vout:          u.Vout,
						LockingScript: &script,
						Satoshis:      u.Satoshis,
					})
					// sufficient funds have been accumulated - we want to return as few
					// transactions as possible to preserve better transactional anonymity
					if total >= amount {
						break
					}
				}
			}
		}
	}
	if len(utxoIds) > 0 {
		// after the best input candidates have been selected, mark them as reserved
		err = ct.SetUtxoReserved(context.Background(), dtx, utxoIds)
		if err != nil {
			ct.logger.Errorf("error marking utxo as reserved: %s", err.Error())
			err = ct.store.TxRollback(dtx)
			if err != nil {
				ct.logger.Errorf("error in rolling back marking utxo as reserved: %s", err.Error())
			}
		}
	} else {
		ct.logger.Errorf("[\U0001F6AB] no utxos found for address %s and satoshis %d", address, amount)
	}
	return res, err
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
		stmt = "ID IN ? AND status = 1"
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
	for retries < 10 {
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
		break
	}
	return err
}

func (ct *CoinbaseTracker) UpdateUtxoReserved(ctx context.Context, tx any, dur time.Duration) error {
	vals := []interface{}{time.Now().Add(-dur), false, true}
	return ct.store.TxUpdateBatch(tx, "utxos", "updated_at < ? AND status = 2", vals, map[string]interface{}{"status": model.StatusSpendable})
}

func (ct *CoinbaseTracker) SubmitTransaction(ctx context.Context, transaction []byte) error {
	// send to node
	// set utxos as spent

	return nil
}
