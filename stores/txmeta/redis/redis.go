package redis

import (
	"context"
	"net/url"
	"sync"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/redis/go-redis/v9"
)

type Redis struct {
	mu                 sync.Mutex
	url                *url.URL
	rdb                redis.Cmdable
	mode               string
	heightMutex        sync.RWMutex
	currentBlockHeight uint32
}

func New(u *url.URL, password ...string) *Redis {
	o := &redis.Options{
		Addr: u.Host,
	}

	if len(password) > 0 && password[0] != "" {
		o.Password = password[0]
	}

	rdb := redis.NewClient(o)

	return &Redis{
		url:  u,
		mode: "client",
		rdb:  rdb,
	}
}

func (r *Redis) Get(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	res := r.rdb.Get(ctx, hash.String())

	if res.Err() != nil {
		return nil, res.Err()
	}

	if res.Val() == string(redis.Nil) {
		return nil, txmeta.ErrNotFound
	}

	d, err := txmeta.NewDataFromBytes([]byte(res.Val()))
	if err != nil {
		return nil, err
	}

	return d, nil
}

func (r *Redis) Create(_ context.Context, tx *bt.Tx) (*txmeta.Data, error) {
	data, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, err
	}

	res := r.rdb.SetNX(context.Background(), tx.TxIDChainHash().String(), data.Bytes(), 0)
	if res.Val() == false {
		return nil, txmeta.ErrAlreadyExists
	}
	if res.Err() != nil {
		return nil, res.Err()
	}

	return data, nil
}

// SetMined TODO this should be improved to use a lua script or something more native in Redis
func (r *Redis) SetMined(_ context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	res := r.rdb.Get(context.Background(), hash.String())
	if res.Err() != nil {
		return res.Err()
	}

	if res.Val() == string(redis.Nil) {
		return txmeta.ErrNotFound
	}

	newBytes := append([]byte(res.Val()), blockHash.CloneBytes()...)

	resSet := r.rdb.Set(context.Background(), hash.String(), newBytes, 0)
	if resSet.Err() != nil {
		return resSet.Err()
	}

	return nil
}

func (r *Redis) Delete(_ context.Context, hash *chainhash.Hash) error {
	res := r.rdb.Del(context.Background(), hash.String())
	if res.Err() != nil {
		return res.Err()
	}

	return nil
}
