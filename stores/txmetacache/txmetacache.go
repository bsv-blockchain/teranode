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
	if _, ok := txMetaStore.(*TxMetaCache); ok {
		// txMetaStore is a TxMetaCache, this is not allowed
		panic("Cannot use TxMetaCache as the underlying store for TxMetaCache")
	}

	initPrometheusMetrics()

	maxMB, _ := gocore.Config().GetInt("txMetaCacheMaxMB", 32)
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

func (t *TxMetaCache) SetCacheMulti(keys [][]byte, values [][]byte) error {
	err := t.cache.SetMulti(keys, values)
	if err != nil {
		return err
	}
	t.metrics.insertions.Add(uint64(len(keys)))
	return nil
}

func (t *TxMetaCache) GetMetaCached(_ context.Context, hash *chainhash.Hash) *txmeta.Data {
	cachedBytes := make([]byte, 0)
	_ = t.cache.Get(&cachedBytes, hash[:])

	if len(cachedBytes) > 0 {
		t.metrics.hits.Add(1)
		txmetaData := txmeta.Data{}
		txmeta.NewMetaDataFromBytes(&cachedBytes, &txmetaData)

		return &txmetaData
	}
	t.metrics.misses.Add(1)

	return nil
}

func (t *TxMetaCache) GetMeta(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	cachedBytes := make([]byte, 0)
	_ = t.cache.Get(&cachedBytes, hash[:])

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

	prometheusBlockValidationTxMetaCacheGetOrigin.Add(1)

	// add to cache
	txMeta.Tx = nil
	_ = t.SetCache(hash, txMeta)

	return txMeta, nil
}

func (t *TxMetaCache) Get(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	cachedBytes := make([]byte, 0)
	_ = t.cache.Get(&cachedBytes, hash[:])
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

	prometheusBlockValidationTxMetaCacheGetOrigin.Add(1)

	// add to cache
	txMeta.Tx = nil
	_ = t.SetCache(hash, txMeta)

	return txMeta, nil
}

func (t *TxMetaCache) GetMulti(ctx context.Context, hashes []*chainhash.Hash, fields ...string) (map[chainhash.Hash]*txmeta.Data, error) {
	m, err := t.txMetaStore.GetMulti(ctx, hashes, fields...)
	if err != nil {
		return nil, err
	}

	for hash, txMeta := range m {
		txMeta.Tx = nil
		_ = t.SetCache(&hash, txMeta)
	}

	return m, nil
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

	for _, hash := range hashes {
		err = t.setMinedInCache(ctx, hash, blockID)
		if err != nil {
			return err
		}
	}

	// call improved cache setmulti

	return nil
}

func (t *TxMetaCache) SetMined(ctx context.Context, hash *chainhash.Hash, blockID uint32) error {
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
