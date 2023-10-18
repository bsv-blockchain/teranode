package redis

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/redis/go-redis/v9"
)

type Redis struct {
	url                *url.URL
	rdb                *redis.Client
	heightMutex        sync.RWMutex
	currentBlockHeight uint32
}

// NewRedis returns a new Redis store.
// We will use the chainhash.Hash as the key and the value will be a comma separated string of the following:
// 1. The status of the UTXO
// 2. The locktime of the UTXO
// 3. The spending transaction ID 64-byte hex string if it is spent
// For example:
// 0,0 would be an unspent UTXO
// 0,80000000 would be an unspent UTXO with a locktime of 80000000

func NewRedis(u *url.URL) (utxostore.Interface, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     u.Host,
		Password: "TfocK5PCg7",
	})

	return &Redis{
		url: u,
		rdb: rdb,
	}, nil
}

func (rr *Redis) SetBlockHeight(height uint32) error {
	rr.heightMutex.Lock()
	defer rr.heightMutex.Unlock()

	rr.currentBlockHeight = height
	return nil
}

func (rr *Redis) getBlockHeight() uint32 {
	rr.heightMutex.RLock()
	defer rr.heightMutex.RUnlock()

	return rr.currentBlockHeight
}

func (rr *Redis) Health(ctx context.Context) (int, string, error) {
	return 0, "Redis Ring", nil
}

func (rr *Redis) Get(ctx context.Context, spend *utxostore.Spend) (*utxostore.Response, error) {
	res := rr.rdb.Get(ctx, spend.Hash.String())

	if res.Err() != nil {
		return nil, res.Err()
	}

	if res.Val() == string(redis.Nil) {
		return &utxostore.Response{
			Status: int(utxostore_api.Status_NOT_FOUND),
		}, nil
	}

	v := NewValueFromString(res.Val())

	status := utxostore_api.Status_OK
	if v.SpendingTxID != nil {
		status = utxostore_api.Status_SPENT
	} else if v.LockTime > 500000000 && int64(v.LockTime) > time.Now().UTC().Unix() {
		status = utxostore_api.Status_LOCKED
	} else if v.LockTime > 0 && v.LockTime < rr.getBlockHeight() {
		status = utxostore_api.Status_LOCKED
	}

	return &utxostore.Response{
		Status:       int(status),
		LockTime:     v.LockTime,
		SpendingTxID: v.SpendingTxID,
	}, nil
}

func (rr *Redis) Store(ctx context.Context, tx *bt.Tx) error {
	v := &Value{
		LockTime: tx.LockTime,
	}
	value := v.String()
	txIDHash := tx.TxIDChainHash()

	utxoHashes := make([]*chainhash.Hash, len(tx.Outputs))
	for i, output := range tx.Outputs {
		if output.Satoshis > 0 { // only do outputs with value
			hash, err := util.UTXOHashFromOutput(txIDHash, output, uint32(i))
			if err != nil {
				return err
			}

			utxoHashes[i] = hash
		}
	}

	for outputIdx, hash := range utxoHashes {
		err := rr.storeUtxo(ctx, hash, value)
		if err != nil {
			for i := 0; i < outputIdx; i++ {
				// revert the created utxos
				_ = rr.Delete(ctx, &utxostore.Spend{
					TxID: txIDHash,
					Vout: uint32(i),
					Hash: hash,
				})
			}
			return err
		}
	}

	return nil
}

func (rr *Redis) storeUtxo(ctx context.Context, hash *chainhash.Hash, value string) error {
	res := rr.rdb.SetNX(ctx, hash.String(), value, 0)
	if res.Err() != nil {
		return res.Err()
	}

	if !res.Val() {
		return utxostore.ErrAlreadyExists
	}

	return nil
}

func (rr *Redis) _Spend(ctx context.Context, spends []*utxostore.Spend) (err error) {
	for idx, spend := range spends {
		if err = rr.spend(ctx, spend); err != nil {
			for i := 0; i < idx; i++ {
				// revert the created utxos
				_ = rr.Reset(ctx, spends[i])
			}
			return err
		}
	}

	return nil
}

func (rr *Redis) Spend(ctx context.Context, hash *chainhash.Hash, spendingTxID *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	return spend(ctx, rr.rdb, hash, spendingTxID, rr.getBlockHeight())
}

func (rr *Redis) Reset(ctx context.Context, spend *utxostore.Spend) error {
	err := rr.rdb.Watch(ctx, func(tx *redis.Tx) error {
		res := tx.Get(ctx, spend.Hash.String())
		if res.Err() != nil {
			return res.Err()
		}

		v := NewValueFromString(res.Val())

		v.SpendingTxID = nil

		res2 := tx.Set(ctx, spend.Hash.String(), v.String(), 0)
		if res2.Err() != nil {
			return res2.Err()
		}

		return nil
	}, spend.Hash.String())

	if err != nil {
		return err
	}

	return nil
}

func (rr *Redis) Delete(ctx context.Context, spend *utxostore.Spend) error {
	res := rr.rdb.Del(ctx, spend.Hash.String())

	if res.Err() != nil {
		return res.Err()
	}

	return nil
}

func (rr *Redis) DeleteSpends(deleteSpends bool) {
}
