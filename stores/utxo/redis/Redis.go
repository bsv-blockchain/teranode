package redis

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"github.com/redis/go-redis/v9"
)

type Redis struct {
	url                *url.URL
	rdb                redis.Cmdable
	mode               string
	heightMutex        sync.RWMutex
	currentBlockHeight uint32
	spentUtxoTtl       time.Duration
}

func NewRedisClient(u *url.URL, password ...string) (*Redis, error) {
	o := &redis.Options{
		Addr: u.Host,
	}

	p, ok := u.User.Password()
	if ok && p != "" {
		o.Password = p
	}

	// If optional password is set, override...
	if len(password) > 0 && password[0] != "" {
		o.Password = password[0]
	}

	rdb := redis.NewClient(o)

	spentUtxoTtl, _ := gocore.Config().GetInt("spent_utxo_ttl", 60)

	return &Redis{
		url:          u,
		mode:         "client",
		rdb:          rdb,
		spentUtxoTtl: time.Duration(spentUtxoTtl) * time.Second,
	}, nil
}

func NewRedisCluster(u *url.URL, password ...string) (*Redis, error) {

	ro := &redis.Options{
		Addr: u.Host,
	}

	if u.Path != "" {
		db, err := strconv.Atoi(u.Path[1:])
		if err != nil {
			return nil, fmt.Errorf("path must be an integer: %v", err)
		}

		ro.DB = db
	}

	if u.User != nil && u.User.Username() != "" {
		ro.Username = u.User.Username()
	}

	p, ok := u.User.Password()
	if ok {
		ro.Password = p
	}

	// If optional password is set, override...
	if len(password) > 0 && password[0] != "" {
		ro.Password = password[0]
	}

	o := &redis.ClusterOptions{
		NewClient: func(opt *redis.Options) *redis.Client {
			return redis.NewClient(ro)
		},
	}

	rdb := redis.NewClusterClient(o)

	spentUtxoTtl, _ := gocore.Config().GetInt("spent_utxo_ttl", 60)

	return &Redis{
		url:          u,
		mode:         "cluster",
		rdb:          rdb,
		spentUtxoTtl: time.Duration(spentUtxoTtl) * time.Second,
	}, nil
}

func NewRedisRing(u *url.URL, password ...string) (*Redis, error) {
	hosts := strings.Split(u.Host, ",")

	addrs := make(map[string]string)
	for i, host := range hosts {
		addrs[fmt.Sprintf("shard%d", i)] = host
	}

	o := &redis.RingOptions{
		Addrs: addrs,
	}

	p, ok := u.User.Password()
	if ok && p != "" {
		o.Password = p
	}

	// If optional password is set, override...
	if len(password) > 0 && password[0] != "" {
		o.Password = password[0]
	}

	rdb := redis.NewRing(o)

	spentUtxoTtl, _ := gocore.Config().GetInt("spent_utxo_ttl", 60)

	return &Redis{
		url:          u,
		mode:         "ring",
		rdb:          rdb,
		spentUtxoTtl: time.Duration(spentUtxoTtl) * time.Second,
	}, nil
}

func (r *Redis) SetBlockHeight(height uint32) error {
	r.heightMutex.Lock()
	defer r.heightMutex.Unlock()

	r.currentBlockHeight = height
	return nil
}

func (r *Redis) getBlockHeight() uint32 {
	r.heightMutex.RLock()
	defer r.heightMutex.RUnlock()

	return r.currentBlockHeight
}

func (r *Redis) Health(ctx context.Context) (int, string, error) {
	return 0, fmt.Sprintf("Redis %s", r.mode), nil
}

func (r *Redis) Get(ctx context.Context, spend *utxostore.Spend) (*utxostore.Response, error) {
	res := r.rdb.Get(ctx, spend.Hash.String())
	if res.Err() != nil && res.Err() != redis.Nil {
		return nil, res.Err()
	}

	if res.Val() == string(redis.Nil) || res.Err() == redis.Nil {
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
	} else if v.LockTime > 0 && v.LockTime < r.getBlockHeight() {
		status = utxostore_api.Status_LOCKED
	}

	return &utxostore.Response{
		Status:       int(status),
		LockTime:     v.LockTime,
		SpendingTxID: v.SpendingTxID,
	}, nil
}

// Store stores the utxos of the tx in aerospike
// the lockTime optional argument is needed for coinbase transactions that do not contain the lock time
func (r *Redis) Store(ctx context.Context, tx *bt.Tx, lockTime ...uint32) error {
	storeLockTime := tx.LockTime
	if len(lockTime) > 0 {
		storeLockTime = lockTime[0]
	}
	v := &Value{
		LockTime: storeLockTime,
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

	for _, hash := range utxoHashes {
		err := r.storeUtxo(ctx, hash, value)
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *Redis) storeUtxo(ctx context.Context, hash *chainhash.Hash, value string) error {
	res := r.rdb.SetNX(ctx, hash.String(), value, 0)
	if res.Err() != nil {
		return res.Err()
	}

	if !res.Val() {
		return utxostore.ErrAlreadyExists
	}

	return nil
}

func (r *Redis) Spend(ctx context.Context, spends []*utxostore.Spend) (err error) {
	spentSpends := make([]*utxostore.Spend, 0, len(spends))

	for _, spend := range spends {
		if err = spendUtxo(ctx, r.rdb, spend, r.getBlockHeight()); err != nil {

			// revert the spent utxos
			_ = r.UnSpend(ctx, spentSpends)
			return err
		} else {
			spentSpends = append(spentSpends, spend)
		}
		r.rdb.Expire(ctx, spend.Hash.String(), r.spentUtxoTtl)
	}

	return nil
}

func (r *Redis) UnSpend(ctx context.Context, spends []*utxostore.Spend) (err error) {
	for _, spend := range spends {
		res := r.rdb.Get(ctx, spend.Hash.String())
		if res.Err() != nil {
			return res.Err()
		}

		v := NewValueFromString(res.Val())

		v.SpendingTxID = nil

		res2 := r.rdb.Set(ctx, spend.Hash.String(), v.String(), 0)
		if res2.Err() != nil {
			return res2.Err()
		}
	}

	return nil
}

func (r *Redis) Delete(ctx context.Context, tx *bt.Tx) error {
	if !tx.IsExtended() {
		return errors.New("tx must be in extended format")
	}

	for i, output := range tx.Outputs {
		if output.Satoshis > 0 { // only do outputs with value
			hash, err := util.UTXOHashFromOutput(tx.TxIDChainHash(), output, uint32(i))
			if err != nil {
				return err
			}

			res := r.rdb.Del(ctx, hash.String())

			if res.Err() != nil {
				return res.Err()
			}
		}
	}

	return nil
}

func (r *Redis) DeleteSpends(deleteSpends bool) {
}
