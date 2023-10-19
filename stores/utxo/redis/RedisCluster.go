package redis

import (
	"context"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/redis/go-redis/v9"
)

type RedisCluster struct {
	url                *url.URL
	rdb                *redis.ClusterClient
	heightMutex        sync.RWMutex
	currentBlockHeight uint32
}

// NewRedisCluster returns a new Redis store.
// We will use the chainhash.Hash as the key and the value will be a comma separated string of the following:
// 1. The status of the UTXO
// 2. The locktime of the UTXO
// 3. The spending transaction ID 64-byte hex string if it is spent
// For example:
// 0,0 would be an unspent UTXO
// 0,80000000 would be an unspent UTXO with a locktime of 80000000

func NewRedisCluster(u *url.URL) (*RedisCluster, error) {
	hosts := strings.Split(u.Host, ",")

	addrs := make([]string, 0)
	addrs = append(addrs, hosts...)

	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    addrs,
		Password: "TfocK5PCg7",
	})

	return &RedisCluster{
		url: u,
		rdb: rdb,
	}, nil
}

func (rr *RedisCluster) SetBlockHeight(height uint32) error {
	rr.heightMutex.Lock()
	defer rr.heightMutex.Unlock()

	rr.currentBlockHeight = height
	return nil
}

func (rr *RedisCluster) getBlockHeight() uint32 {
	rr.heightMutex.RLock()
	defer rr.heightMutex.RUnlock()

	return rr.currentBlockHeight
}

func (rr *RedisCluster) Health(ctx context.Context) (int, string, error) {
	return 0, "Redis Ring", nil
}

func (rr *RedisCluster) Get(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	res := rr.rdb.Get(ctx, hash.String())

	if res.Err() != nil {
		return nil, res.Err()
	}

	if res.Val() == string(redis.Nil) {
		return &utxostore.UTXOResponse{
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

	return &utxostore.UTXOResponse{
		Status:       int(status),
		LockTime:     v.LockTime,
		SpendingTxID: v.SpendingTxID,
	}, nil
}

func (rr *RedisCluster) Store(ctx context.Context, hash *chainhash.Hash, nLockTime uint32) (*utxostore.UTXOResponse, error) {
	v := &Value{
		LockTime: nLockTime,
	}

	res := rr.rdb.SetNX(ctx, hash.String(), v.String(), 0)
	if res.Err() != nil {
		return nil, res.Err()
	}

	if !res.Val() {
		// This means the key already existed
		return &utxostore.UTXOResponse{
			Status: int(utxostore_api.Status_ALREADY_EXISTS),
		}, nil
	}

	return &utxostore.UTXOResponse{
		Status:   int(utxostore_api.Status_OK),
		LockTime: nLockTime,
	}, nil
}

func (rr *RedisCluster) BatchStore(ctx context.Context, hashes []*chainhash.Hash, nLockTime uint32) (*utxostore.BatchResponse, error) {
	return &utxostore.BatchResponse{
		Status: 0,
	}, nil
}

func (rr *RedisCluster) Spend(ctx context.Context, hash *chainhash.Hash, spendingTxID *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	return spend(ctx, rr.rdb, hash, spendingTxID, rr.getBlockHeight())
}

func (rr *RedisCluster) Reset(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	err := rr.rdb.Watch(ctx, func(tx *redis.Tx) error {
		res := tx.Get(ctx, hash.String())
		if res.Err() != nil {
			return res.Err()
		}

		v := NewValueFromString(res.Val())

		v.SpendingTxID = nil

		res2 := tx.Set(ctx, hash.String(), v.String(), 0)
		if res2.Err() != nil {
			return res2.Err()
		}

		return nil
	}, hash.String())

	if err != nil {
		return nil, err
	}

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (rr *RedisCluster) Delete(ctx context.Context, hash *chainhash.Hash) (*utxostore.UTXOResponse, error) {
	res := rr.rdb.Del(ctx, hash.String())

	if res.Err() != nil {
		return nil, res.Err()
	}

	return &utxostore.UTXOResponse{
		Status: int(utxostore_api.Status_OK),
	}, nil
}

func (rr *RedisCluster) DeleteSpends(deleteSpends bool) {
}
