package redis

import (
	"context"
	_ "embed"
	"fmt"
	"net/url"
	"strconv"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/redis/go-redis/v9"
)

//go:embed update_blockhash.lua
var scriptString string
var luaScript = redis.NewScript(scriptString)

type Redis struct {
	url  *url.URL
	rdb  redis.Cmdable
	mode string
}

func New(u *url.URL, password ...string) (*Redis, error) {

	ro := &redis.Options{
		Addr: u.Host,
	}

	if u.Path != "" {
		db, err := strconv.Atoi(u.Path)
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

	return &Redis{
		url:  u,
		mode: "cluster",
		rdb:  rdb,
	}, nil
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
	if !res.Val() {
		return data, txmeta.ErrAlreadyExists
	}
	if res.Err() != nil {
		return nil, res.Err()
	}

	return data, nil
}

// SetMined uses a lua script to update the block hash of a transaction
func (r *Redis) SetMined(ctx context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) error {
	res, err := luaScript.Run(ctx, r.rdb, []string{hash.String()}, blockHash.CloneBytes()).Result()
	if err != nil {
		return err
	}
	s, ok := res.(string)
	if !ok {
		return fmt.Errorf("unknown response from spend: %v", res)
	}
	if s == "OK" {
		return nil
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
