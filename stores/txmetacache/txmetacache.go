package txmetacache

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

type metrics struct {
	insertions atomic.Uint64
	hits       atomic.Uint64
	misses     atomic.Uint64
	evictions  atomic.Uint64
}

// CachedData struct for the cached transaction metadata
// do not change order, has been optimized for size: https://golangprojectstructure.com/how-to-make-go-structs-more-efficient/
type CachedData struct {
	ParentTxHashes []*chainhash.Hash `json:"parentTxHashes"`
	BlockHashes    []*chainhash.Hash `json:"blockHashes"` // TODO change this to use the db ids instead of the hashes
	Fee            uint64            `json:"fee"`
	SizeInBytes    uint64            `json:"sizeInBytes"`
}

type TxMetaCache struct {
	txMetaStore txmeta.Store
	cache       *ImprovedCache
	metrics     metrics
	logger      ulogger.Logger
}

func NewTxMetaCache(ctx context.Context, logger ulogger.Logger, txMetaStore txmeta.Store, options ...int) txmeta.Store {
	initPrometheusMetrics()

	maxMB, _ := gocore.Config().GetInt("txMetaCacheMaxMB", 128)
	if len(options) > 0 {
		maxMB = options[0]
	}

	m := &TxMetaCache{
		txMetaStore: txMetaStore,
		cache:       NewImprovedCache(maxMB * 1024 * 1024),
		metrics:     metrics{},
		logger:      logger,
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if prometheusBlockValidationTxMetaCacheSize != nil {
					prometheusBlockValidationTxMetaCacheSize.Set(float64(m.Length()))
					prometheusBlockValidationTxMetaCacheInsertions.Set(float64(m.metrics.insertions.Load()))
					prometheusBlockValidationTxMetaCacheHits.Set(float64(m.metrics.hits.Load()))
					prometheusBlockValidationTxMetaCacheMisses.Set(float64(m.metrics.misses.Load()))
					prometheusBlockValidationTxMetaCacheEvictions.Set(float64(m.metrics.evictions.Load()))
				}

				time.Sleep(5 * time.Second)
			}
		}
	}()

	return m
}

func (t *TxMetaCache) SetCache(hash *chainhash.Hash, txMeta *txmeta.Data) error {
	txMeta.Tx = nil
	t.cache.Set(hash[:], txMeta.MetaBytes())

	t.metrics.insertions.Add(1)

	return nil
}

func (t *TxMetaCache) SetCacheMulti(hashes map[chainhash.Hash]*txmeta.Data) error {
	for hash, txMeta := range hashes {
		txMeta.Tx = nil
		t.cache.Set(hash[:], txMeta.MetaBytes())
	}

	t.metrics.insertions.Add(uint64(len(hashes)))
	return nil
}

func (t *TxMetaCache) GetMeta(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	cachedBytes := make([]byte, 0)
	t.cache.Get(&cachedBytes, hash[:])

	if len(cachedBytes) > 0 {
		t.metrics.hits.Add(1)
		txmetaData := txmeta.Data{}
		txmeta.NewMetaDataFromBytes(&cachedBytes, &txmetaData)
		return &txmetaData, nil
	}
	t.metrics.misses.Add(1)

	t.logger.Warnf("txMetaCache miss for %s", hash.String())

	txMeta, err := t.txMetaStore.GetMeta(ctx, hash)
	if err != nil {
		return nil, err
	}

	// add to cache
	txMeta.Tx = nil
	_ = t.SetCache(hash, txMeta)

	return txMeta, nil
}

func (t *TxMetaCache) Get(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	cachedBytes := make([]byte, 0)
	t.cache.Get(&cachedBytes, hash[:])
	// if found in cache
	if len(cachedBytes) > 0 {
		t.metrics.hits.Add(1)

		txmetaData := txmeta.Data{}
		txmeta.NewMetaDataFromBytes(&cachedBytes, &txmetaData)
		return &txmetaData, nil
	}

	// if not found in the cache, add it to the cache, record cache miss
	t.metrics.misses.Add(1)
	txMeta, err := t.txMetaStore.Get(ctx, hash)
	if err != nil {
		return nil, err
	}

	// add to cache
	txMeta.Tx = nil
	_ = t.SetCache(hash, txMeta)

	return txMeta, nil
}

func (t *TxMetaCache) Create(ctx context.Context, tx *bt.Tx) (*txmeta.Data, error) {
	txMeta, err := t.txMetaStore.Create(ctx, tx)
	if err != nil {
		return txMeta, err
	}

	// add to cache
	txMeta.Tx = nil
	_ = t.SetCache(tx.TxIDChainHash(), txMeta)

	return txMeta, nil
}

func (t *TxMetaCache) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) (err error) {
	// do not update the aerospike tx meta store, it kills the aerospike server
	//err := t.txMetaStore.SetMinedMulti(ctx, hashes, blockID)
	//if err != nil {
	//	return err
	//}

	// remove const, parameterize

	// currently CPU is overwhelmed
	// workload
	// 1- cont. write and read a.t.m txMeta. Locks should be short. Buckets are smaller, less update per bucket -> less lock time per bucket
	// 2- blockIDs: readaing or writing a lot. Multi makes sense. Big batfcfh comes at once, longer lock is fine

	// G: why we can't call it with goroutines?
	for _, hash := range hashes {
		err = t.setMinedInCache(ctx, hash, blockID)
		if err != nil {
			return err
		}
	}

	// call improved cache setmulti

	return nil
}

// 1 move blockID slice out of metacache
// 2 implement improvedcache setmulti with locks

func (t *TxMetaCache) SetMined(ctx context.Context, hash *chainhash.Hash, blockID uint32) error {
	// G: why not comment out the following as well?
	// Does only SetMinedMulti called. Yes block validaiton only calls that
	err := t.txMetaStore.SetMined(ctx, hash, blockID)
	if err != nil {
		return err
	}

	err = t.setMinedInCache(ctx, hash, blockID)
	if err != nil {
		return err
	}

	return nil
}

func (t *TxMetaCache) setMinedInCache(ctx context.Context, hash *chainhash.Hash, blockID uint32) (err error) {
	var txMeta *txmeta.Data
	txMeta, err = t.Get(ctx, hash)
	if err != nil {
		txMeta, err = t.txMetaStore.Get(ctx, hash)
	}
	if err != nil {
		return err
	}

	if txMeta.BlockIDs == nil {
		txMeta.BlockIDs = []uint32{
			blockID,
		}
	} else {
		txMeta.BlockIDs = append(txMeta.BlockIDs, blockID)
	}

	txMeta.Tx = nil
	_ = t.SetCache(hash, txMeta)

	return nil
}

func (t *TxMetaCache) Delete(_ context.Context, hash *chainhash.Hash) error {
	t.cache.Del(hash[:])
	t.metrics.evictions.Add(1)
	return nil
}

func (t *TxMetaCache) Length() int {
	s := &Stats{}
	t.cache.UpdateStats(s)
	return int(s.EntriesCount)
}

// func (t *TxMetaCache) BytesSize() int {
// 	s := &Stats{}
// 	t.cache.UpdateStats(s)
// 	return int(s.BytesSize)
// }
