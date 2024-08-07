package txmetacache

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"

	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util/types"
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
	utxoStore utxo.Store
	cache     *ImprovedCache
	metrics   metrics
	logger    ulogger.Logger
}

type CacheStats struct {
	EntriesCount       uint64
	TrimCount          uint64
	TotalMapSize       uint64
	TotalElementsAdded uint64
}

func NewTxMetaCache(ctx context.Context, logger ulogger.Logger, utxoStore utxo.Store, options ...int) utxo.Store {
	if _, ok := utxoStore.(*TxMetaCache); ok {
		// txMetaStore is a TxMetaCache, this is not allowed
		panic("Cannot use TxMetaCache as the underlying store for TxMetaCache")
	}

	initPrometheusMetrics()

	maxMB, _ := gocore.Config().GetInt("txMetaCacheMaxMB", 256)

	if len(options) > 0 && options[0] > 256 {
		maxMB = options[0]
	}

	m := &TxMetaCache{
		utxoStore: utxoStore,
		cache:     NewImprovedCache(maxMB*1024*1024, types.Trimmed),
		metrics:   metrics{},
		logger:    logger,
	}

	go func() {
		ticker := time.NewTicker(5 * time.Second)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cacheStats := m.GetCacheStats()
				if prometheusBlockValidationTxMetaCacheInsertions != nil {
					prometheusBlockValidationTxMetaCacheSize.Set(float64(cacheStats.EntriesCount))
					prometheusBlockValidationTxMetaCacheInsertions.Set(float64(m.metrics.insertions.Load()))
					prometheusBlockValidationTxMetaCacheHits.Set(float64(m.metrics.hits.Load()))
					prometheusBlockValidationTxMetaCacheMisses.Set(float64(m.metrics.misses.Load()))
					prometheusBlockValidationTxMetaCacheEvictions.Set(float64(m.metrics.evictions.Load()))
					prometheusBlockValidationTxMetaCacheTrims.Set(float64(cacheStats.TrimCount))
					prometheusBlockValidationTxMetaCacheMapSize.Set(float64(cacheStats.TotalMapSize))
					prometheusBlockValidationTxMetaCacheTotalElementsAdded.Set(float64(cacheStats.TotalElementsAdded))
				}
			}
		}
	}()

	return m
}

func (t *TxMetaCache) SetCache(hash *chainhash.Hash, txMeta *meta.Data) error {
	txMeta.Tx = nil
	err := t.cache.Set(hash[:], txMeta.MetaBytes())
	if err != nil {
		return err
	}

	t.metrics.insertions.Add(1)

	return nil
}

func (t *TxMetaCache) SetCacheFromBytes(key, txMetaBytes []byte) error {
	err := t.cache.Set(key, txMetaBytes)
	if err != nil {
		return err
	}

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

func (t *TxMetaCache) GetMetaCached(_ context.Context, hash *chainhash.Hash) *meta.Data {
	cachedBytes := make([]byte, 0)
	_ = t.cache.Get(&cachedBytes, hash[:])

	if len(cachedBytes) > 0 {
		t.metrics.hits.Add(1)
		txmetaData := meta.Data{}
		meta.NewMetaDataFromBytes(&cachedBytes, &txmetaData)

		return &txmetaData
	}
	t.metrics.misses.Add(1)

	return nil
}

func (t *TxMetaCache) GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	cachedBytes := make([]byte, 0)
	_ = t.cache.Get(&cachedBytes, hash[:])

	if len(cachedBytes) > 0 {
		t.metrics.hits.Add(1)
		txmetaData := meta.Data{}
		meta.NewMetaDataFromBytes(&cachedBytes, &txmetaData)
		txmetaData.BlockIDs = make([]uint32, 0) // this is expected behavior, needs to be non-nil
		return &txmetaData, nil
	}
	t.metrics.misses.Add(1)

	t.logger.Warnf("txMetaCache miss for %s", hash.String())

	txMeta, err := t.utxoStore.GetMeta(ctx, hash)
	if err != nil {
		return nil, err
	}

	prometheusBlockValidationTxMetaCacheGetOrigin.Add(1)

	// add to cache
	txMeta.Tx = nil
	_ = t.SetCache(hash, txMeta)

	return txMeta, nil
}

func (t *TxMetaCache) Get(ctx context.Context, hash *chainhash.Hash, _ ...[]string) (*meta.Data, error) {
	cachedBytes := make([]byte, 0)
	_ = t.cache.Get(&cachedBytes, hash[:])
	// if found in cache
	if len(cachedBytes) > 0 {
		t.metrics.hits.Add(1)

		txmetaData := meta.Data{}
		meta.NewMetaDataFromBytes(&cachedBytes, &txmetaData)
		return &txmetaData, nil
	}

	// if not found in the cache, add it to the cache, record cache miss
	t.metrics.misses.Add(1)
	txMeta, err := t.utxoStore.Get(ctx, hash)
	if err != nil {
		return nil, err
	}

	prometheusBlockValidationTxMetaCacheGetOrigin.Add(1)

	// add to cache
	txMeta.Tx = nil
	_ = t.SetCache(hash, txMeta)

	return txMeta, nil
}

func (t *TxMetaCache) BatchDecorate(ctx context.Context, hashes []*utxo.UnresolvedMetaData, fields ...string) error {
	if err := t.utxoStore.BatchDecorate(ctx, hashes, fields...); err != nil {
		return err
	}

	prometheusBlockValidationTxMetaCacheGetOrigin.Add(float64(len(hashes)))

	for _, data := range hashes {
		if data.Data != nil {
			data.Data.Tx = nil
			_ = t.SetCache(&data.Hash, data.Data)
		}
	}

	return nil
}

func (t *TxMetaCache) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, blockIDs ...uint32) (*meta.Data, error) {
	txMeta, err := t.utxoStore.Create(ctx, tx, blockHeight, blockIDs...)
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
	err := t.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{hash}, blockID)
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
	var txMeta *meta.Data
	txMeta, err = t.Get(ctx, hash)
	if err != nil {
		txMeta, err = t.utxoStore.Get(ctx, hash)
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

func (t *TxMetaCache) GetCacheStats() *CacheStats {
	s := &Stats{}
	t.cache.UpdateStats(s)

	return &CacheStats{
		EntriesCount:       s.EntriesCount,
		TrimCount:          s.TrimCount,
		TotalMapSize:       s.TotalMapSize,
		TotalElementsAdded: s.TotalElementsAdded,
	}
}

func (t *TxMetaCache) Health(ctx context.Context) (int, string, error) {
	return t.utxoStore.Health(ctx)
}

func (t *TxMetaCache) GetSpend(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	return t.utxoStore.GetSpend(ctx, spend)
}

func (t *TxMetaCache) Spend(ctx context.Context, spends []*utxo.Spend, blockHeight uint32) error {
	return t.utxoStore.Spend(ctx, spends, blockHeight)
}

func (t *TxMetaCache) UnSpend(ctx context.Context, spends []*utxo.Spend) error {
	return t.utxoStore.UnSpend(ctx, spends)
}

func (t *TxMetaCache) PreviousOutputsDecorate(ctx context.Context, outpoints []*meta.PreviousOutput) error {
	return t.utxoStore.PreviousOutputsDecorate(ctx, outpoints)
}

func (t *TxMetaCache) SetBlockHeight(height uint32) error {
	return t.utxoStore.SetBlockHeight(height)
}

func (t *TxMetaCache) GetBlockHeight() uint32 {
	return t.utxoStore.GetBlockHeight()
}

func (t *TxMetaCache) SetMedianBlockTime(height uint32) error {
	return t.utxoStore.SetMedianBlockTime(height)
}

func (t *TxMetaCache) GetMedianBlockTime() uint32 {
	return t.utxoStore.GetMedianBlockTime()
}
