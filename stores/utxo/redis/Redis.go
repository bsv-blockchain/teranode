package redis

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

type Redis struct {
	url                *url.URL
	logger             ulogger.Logger
	rdb                redis.Cmdable
	mode               string
	heightMutex        sync.RWMutex
	currentBlockHeight uint32
	spentUtxoTtl       uint32
	timeout            time.Duration
}

func NewRedisClient(logger ulogger.Logger, u *url.URL, password ...string) (*Redis, error) {
	logger.Infof("RedisClient using host: %v", u.Host)
	o := &redis.Options{
		Addr:        u.Host,
		PoolTimeout: time.Duration(10) * time.Second,
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
	timeout, _ := gocore.Config().GetInt("utxostore_dbTimeoutMillis", 5000)

	return &Redis{
		url:          u,
		logger:       logger,
		mode:         "client",
		rdb:          rdb,
		spentUtxoTtl: uint32(spentUtxoTtl),
		timeout:      time.Duration(timeout) * time.Millisecond,
	}, nil
}

func NewRedisCluster(logger ulogger.Logger, u *url.URL, password ...string) (*Redis, error) {
	hosts := strings.Split(u.Host, ",")
	logger.Infof("RedisCluster using hosts: %v", hosts)

	addrs := make([]string, 0)
	addrs = append(addrs, hosts...)

	o := &redis.ClusterOptions{
		Addrs:       addrs,
		PoolTimeout: time.Duration(10) * time.Second,
	}

	p, ok := u.User.Password()
	if ok && p != "" {
		o.Password = p
	}

	// If optional password is set, override...
	if len(password) > 0 && password[0] != "" {
		o.Password = password[0]
	}

	rdb := redis.NewClusterClient(o)

	spentUtxoTtl, _ := gocore.Config().GetInt("spent_utxo_ttl", 60)
	timeout, _ := gocore.Config().GetInt("utxostore_dbTimeoutMillis", 5000)

	return &Redis{
		url:          u,
		logger:       logger,
		mode:         "cluster",
		rdb:          rdb,
		spentUtxoTtl: uint32(spentUtxoTtl),
		timeout:      time.Duration(timeout) * time.Millisecond,
	}, nil
}

func NewRedisRing(logger ulogger.Logger, u *url.URL, password ...string) (*Redis, error) {
	hosts := strings.Split(u.Host, ",")
	logger.Infof("RedisRing using hosts: %v", hosts)

	addrs := make(map[string]string)
	for i, host := range hosts {
		addrs[fmt.Sprintf("shard%d", i)] = host
	}

	o := &redis.RingOptions{
		Addrs:       addrs,
		PoolTimeout: time.Duration(10) * time.Second,
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
	timeout, _ := gocore.Config().GetInt("utxostore_dbTimeoutMillis", 5000)

	return &Redis{
		url:          u,
		logger:       logger,
		mode:         "ring",
		rdb:          rdb,
		spentUtxoTtl: uint32(spentUtxoTtl),
		timeout:      time.Duration(timeout) * time.Millisecond,
	}, nil
}

func (r *Redis) SetBlockHeight(height uint32) error {
	r.heightMutex.Lock()
	defer r.heightMutex.Unlock()

	r.currentBlockHeight = height
	return nil
}

func (r *Redis) GetBlockHeight() (uint32, error) {
	return r.currentBlockHeight, nil
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
			Status: int(utxostore.Status_NOT_FOUND),
		}, nil
	}

	v := NewValueFromString(res.Val())

	status := utxostore.Status_OK
	if v.SpendingTxID != nil {
		status = utxostore.Status_SPENT
	} else if v.LockTime > 500000000 && int64(v.LockTime) > time.Now().UTC().Unix() {
		status = utxostore.Status_LOCKED
	} else if v.LockTime > 0 && v.LockTime < r.getBlockHeight() {
		status = utxostore.Status_LOCKED
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
	// if r.timeout > 0 {
	// 	var cancel context.CancelFunc
	// 	ctx, cancel = context.WithTimeout(ctx, r.timeout)
	// 	defer cancel()
	// }

	storeLockTime := tx.LockTime
	if len(lockTime) > 0 {
		storeLockTime = lockTime[0]
	}
	v := &Value{
		LockTime: storeLockTime,
	}
	value := v.String()
	txIDHash := tx.TxIDChainHash()

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(16) // TODO what is a safe number here?

	var nrStored = atomic.Uint64{}
	for i, output := range tx.Outputs {
		if output.Satoshis > 0 { // only do outputs with value
			i := i
			output := output
			g.Go(func() error {
				hash, err := util.UTXOHashFromOutput(txIDHash, output, uint32(i))
				if err != nil {
					return err
				}

				if err = r.storeUtxo(gCtx, hash, value); err != nil {
					return err
				}

				nrStored.Add(1)

				return nil
			})
		}
	}

	if err := g.Wait(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("timeout storing %d of %d utxos", nrStored.Load(), len(tx.Outputs))
		}

		return err
	}

	return nil
}

// StoreFromHashes stores the utxos of the tx in aerospike
// TODO not tested for Redis
func (r *Redis) StoreFromHashes(ctx context.Context, _ chainhash.Hash, hashes []chainhash.Hash, lockTime uint32) error {
	v := &Value{
		LockTime: lockTime,
	}
	value := v.String()

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(16) // TODO what is a safe number here?

	var nrStored = atomic.Uint64{}
	for _, hash := range hashes {
		hash := hash
		g.Go(func() error {
			if err := r.storeUtxo(gCtx, &hash, value); err != nil {
				return err
			}

			nrStored.Add(1)

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("timeout storing %d of %d utxos", nrStored.Load(), len(hashes))
		}

		return err
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
	// if r.timeout > 0 {
	// 	var cancel context.CancelFunc
	// 	ctx, cancel = context.WithTimeout(ctx, r.timeout)
	// 	defer cancel()
	// }

	spentSpends := make([]*utxostore.Spend, 0, len(spends))

	for i, spend := range spends {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout spending %d of %d utxos", i, len(spends))
		default:
			if spend == nil {
				continue
			}

			if err = spendUtxo(ctx, r.rdb, spend, r.getBlockHeight(), r.spentUtxoTtl); err != nil {
				// revert the spent utxos
				_ = r.UnSpend(ctx, spentSpends)
				return err
			} else {
				spentSpends = append(spentSpends, spend)
			}
		}
	}

	return nil
}

func (r *Redis) UnSpend(ctx context.Context, spends []*utxostore.Spend) (err error) {
	if r.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}

	for i, spend := range spends {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout unspending %d of %d utxos", i, len(spends))
		default:
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
	}

	return nil
}

func (r *Redis) Delete(ctx context.Context, tx *bt.Tx) error {
	if r.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}

	for i, output := range tx.Outputs {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout deleting %d of %d utxos", i, len(tx.Outputs))
		default:
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
	}

	return nil
}

func (r *Redis) DeleteSpends(deleteSpends bool) {
}
