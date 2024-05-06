package redis

import (
	"context"
	_ "embed"
	"encoding/binary"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"github.com/redis/go-redis/v9"
)

//go:embed update_blockhash.lua
var scriptString string
var luaScript = redis.NewScript(scriptString)

type Redis struct {
	url       *url.URL
	logger    ulogger.Logger
	rdb       redis.Cmdable
	txMetaTtl time.Duration
	mode      string
}

func NewRedisClient(logger ulogger.Logger, u *url.URL, password ...string) (*Redis, error) {
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

	txMetaTtl, _ := gocore.Config().GetInt("txmeta_store_ttl", 60*60*2) // in seconds

	return &Redis{
		url:       u,
		logger:    logger,
		mode:      "client",
		txMetaTtl: time.Duration(txMetaTtl) * time.Second,
		rdb:       rdb,
	}, nil
}

func NewRedisCluster(logger ulogger.Logger, u *url.URL, password ...string) (*Redis, error) {
	hosts := strings.Split(u.Host, ",")

	addrs := make([]string, 0)
	addrs = append(addrs, hosts...)

	o := &redis.ClusterOptions{
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

	rdb := redis.NewClusterClient(o)

	txMetaTtl, _ := gocore.Config().GetInt("txmeta_store_ttl", 60*60*2) // in seconds

	return &Redis{
		url:       u,
		logger:    logger,
		mode:      "cluster",
		txMetaTtl: time.Duration(txMetaTtl) * time.Second,
		rdb:       rdb,
	}, nil
}

func NewRedisRing(logger ulogger.Logger, u *url.URL, password ...string) (*Redis, error) {
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

	txMetaTtl, _ := gocore.Config().GetInt("txmeta_store_ttl", 60*60*2) // in seconds

	return &Redis{
		url:       u,
		logger:    logger,
		mode:      "ring",
		txMetaTtl: time.Duration(txMetaTtl) * time.Second,
		rdb:       rdb,
	}, nil
}

func (r *Redis) Get(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	res := r.rdb.Get(ctx, hash.String())

	if res.Err() != nil {
		if res.Err() == redis.Nil {
			return nil, txmeta.NewErrTxmetaNotFound(hash)
		}
		return nil, res.Err()
	}

	if res.Val() == string(redis.Nil) {
		return nil, txmeta.NewErrTxmetaNotFound(hash)
	}

	d, err := txmeta.NewDataFromBytes([]byte(res.Val()))
	if err != nil {
		return nil, err
	}

	return d, nil
}

func (r *Redis) GetMeta(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	res := r.rdb.Get(ctx, hash.String())

	if res.Err() != nil {
		if res.Err() == redis.Nil {
			return nil, txmeta.NewErrTxmetaNotFound(hash)
		}
		return nil, res.Err()
	}

	if res.Val() == string(redis.Nil) {
		return nil, txmeta.NewErrTxmetaNotFound(hash)
	}

	resBytes := []byte(res.Val())
	data := &txmeta.Data{}
	txmeta.NewMetaDataFromBytes(&resBytes, data)

	return data, nil
}

func (r *Redis) MetaBatchDecorate(ctx context.Context, items []*txmeta.MissingTxHash, fields ...string) error {
	// TODO make this into a batch call
	for _, item := range items {
		data, err := r.Get(ctx, &item.Hash)
		if err != nil {
			if uerr, ok := err.(*errors.Error); ok {
				if uerr.Code == errors.ERR_NOT_FOUND {
					continue
				}
			}
			return err
		}
		item.Data = data
	}

	return nil
}

func (r *Redis) Create(_ context.Context, tx *bt.Tx, lockTime ...uint32) (*txmeta.Data, error) {
	data, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, err
	}

	res := r.rdb.SetNX(context.Background(), tx.TxIDChainHash().String(), data.Bytes(), r.txMetaTtl)
	if res.Err() != nil {
		return nil, res.Err()
	}
	if !res.Val() {
		return data, txmeta.NewErrTxmetaAlreadyExists(tx.TxIDChainHash())
	}

	return data, nil
}

func (r *Redis) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) (err error) {
	for _, hash := range hashes {
		if err = r.SetMined(ctx, hash, blockID); err != nil {
			return err
		}
	}

	return nil
}

// SetMined uses a lua script to update the block id (bytes) of a transaction
func (r *Redis) SetMined(ctx context.Context, hash *chainhash.Hash, blockID uint32) error {
	var blockBytes [4]byte
	binary.LittleEndian.PutUint32(blockBytes[:], blockID)

	res, err := luaScript.Run(ctx, r.rdb, []string{hash.String()}, blockBytes[:]).Result()
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
