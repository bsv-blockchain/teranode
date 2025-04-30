package txmetacache

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
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

type BucketType int

const (
	Unallocated BucketType = iota
	Preallocated
	Trimmed
)

func NewTxMetaCache(
	ctx context.Context,
	logger ulogger.Logger,
	utxoStore utxo.Store,
	bucketType BucketType,
	maxMBOverride ...int,
) (utxo.Store, error) {
	if _, ok := utxoStore.(*TxMetaCache); ok {
		// txMetaStore is a TxMetaCache, this is not allowed
		return nil, errors.NewServiceError("Cannot use TxMetaCache as the underlying store for TxMetaCache")
	}

	initPrometheusMetrics()

	// base size (MB) from config
	maxMB, _ := gocore.Config().GetInt("txMetaCacheMaxMB", 256)
	// override if caller passed one
	if len(maxMBOverride) > 0 && maxMBOverride[0] > 0 {
		maxMB = maxMBOverride[0]
	}

	cache, err := NewImprovedCache(maxMB*1024*1024, bucketType)
	if err != nil {
		return nil, errors.NewProcessingError("error creating cache", err)
	}

	m := &TxMetaCache{
		utxoStore: utxoStore,
		cache:     cache,
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

	return m, nil
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

	if err := t.cache.Get(&cachedBytes, hash[:]); err != nil {
		t.metrics.misses.Add(1)

		return nil
	}

	if len(cachedBytes) == 0 {
		t.metrics.misses.Add(1)
		t.logger.Warnf("txMetaCache empty for %s", hash.String())

		return nil
	}

	t.metrics.hits.Add(1)

	txmetaData := meta.Data{}
	meta.NewMetaDataFromBytes(&cachedBytes, &txmetaData)

	return &txmetaData
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

func (t *TxMetaCache) Get(ctx context.Context, hash *chainhash.Hash, fields ...fields.FieldName) (*meta.Data, error) {
	cachedBytes := make([]byte, 0)
	if err := t.cache.Get(&cachedBytes, hash[:]); err != nil {
		t.logger.Warnf("txMetaCache GET miss for %s", hash.String())
	}

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

func (t *TxMetaCache) BatchDecorate(ctx context.Context, hashes []*utxo.UnresolvedMetaData, fields ...fields.FieldName) error {
	if err := t.utxoStore.BatchDecorate(ctx, hashes, fields...); err != nil {
		return err
	}

	prometheusBlockValidationTxMetaCacheGetOrigin.Add(float64(len(hashes)))

	for _, data := range hashes {
		if data.Data != nil {
			data.Data.Tx = nil

			if len(data.Data.ParentTxHashes) > 48 {
				t.logger.Warnf("stored tx meta maybe too big for txmeta cache, size: %d, parent hash count: %d", data.Data.SizeInBytes, len(data.Data.ParentTxHashes))
			}

			if err := t.SetCache(&data.Hash, data.Data); err != nil {
				if errors.Is(err, errors.ErrProcessing) {
					t.logger.Debugf("error setting cache for txMeta [%s]: %v", data.Hash.String(), err)
				} else {
					t.logger.Errorf("error setting cache for txMeta [%s]: %v", data.Hash.String(), err)
				}
			}
		}
	}

	return nil
}

func (t *TxMetaCache) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...utxo.CreateOption) (*meta.Data, error) {
	txMeta, err := t.utxoStore.Create(ctx, tx, blockHeight, opts...)
	if err != nil {
		return txMeta, err
	}

	options := &utxo.CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

	var txHash *chainhash.Hash

	if options.TxID != nil {
		txHash = options.TxID
	} else {
		txHash = tx.TxIDChainHash()
	}

	// add to cache
	txMeta.Tx = nil
	_ = t.SetCache(txHash, txMeta)

	return txMeta, nil
}

func (t *TxMetaCache) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (err error) {
	// do not update the aerospike tx meta store, it kills the aerospike server
	// err := t.txMetaStore.SetMinedMulti(ctx, hashes, blockID)
	// if err != nil {
	//   return err
	// }
	for _, hash := range hashes {
		err = t.setMinedInCache(ctx, hash, minedBlockInfo)
		if err != nil {
			return err
		}
	}

	// call improved cache setmulti

	return nil
}

func (t *TxMetaCache) SetMinedMultiParallel(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) (err error) {
	err = t.setMinedInCacheParallel(ctx, hashes, blockID)
	if err != nil {
		return err
	}

	return nil
}

func (t *TxMetaCache) SetMined(ctx context.Context, hash *chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) error {
	err := t.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{hash}, minedBlockInfo)
	if err != nil {
		return err
	}

	err = t.setMinedInCache(ctx, hash, minedBlockInfo)
	if err != nil {
		return err
	}

	return nil
}

func (t *TxMetaCache) setMinedInCache(ctx context.Context, hash *chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (err error) {
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
			minedBlockInfo.BlockID,
		}
	} else {
		txMeta.BlockIDs = append(txMeta.BlockIDs, minedBlockInfo.BlockID)
	}

	txMeta.Tx = nil
	_ = t.SetCache(hash, txMeta)

	return nil
}

func (t *TxMetaCache) setMinedInCacheParallel(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) (err error) {
	var txMeta *meta.Data

	g := errgroup.Group{}
	g.SetLimit(100)

	for _, hash := range hashes {
		hash := hash

		g.Go(func() error {
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

			// t.SetCache(hash, txMeta) always returns nil
			return t.SetCache(hash, txMeta)
		})
	}

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

func (t *TxMetaCache) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return t.utxoStore.Health(ctx, checkLiveness)
}

func (t *TxMetaCache) GetSpend(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	return t.utxoStore.GetSpend(ctx, spend)
}

func (t *TxMetaCache) Spend(ctx context.Context, tx *bt.Tx, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	return t.utxoStore.Spend(ctx, tx)
}

func (t *TxMetaCache) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsUnspendable ...bool) error {
	return t.utxoStore.Unspend(ctx, spends, flagAsUnspendable...)
}

func (t *TxMetaCache) PreviousOutputsDecorate(ctx context.Context, outpoints []*meta.PreviousOutput) error {
	return t.utxoStore.PreviousOutputsDecorate(ctx, outpoints)
}

func (t *TxMetaCache) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	if err := t.utxoStore.FreezeUTXOs(ctx, spends, tSettings); err != nil {
		return err
	}

	for _, spend := range spends {
		_ = t.Delete(ctx, spend.TxID)
	}

	return nil
}

func (t *TxMetaCache) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	if err := t.utxoStore.UnFreezeUTXOs(ctx, spends, tSettings); err != nil {
		return err
	}

	for _, spend := range spends {
		_ = t.Delete(ctx, spend.TxID)
	}

	return nil
}

func (t *TxMetaCache) ReAssignUTXO(ctx context.Context, utxo *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	if err := t.utxoStore.ReAssignUTXO(ctx, utxo, newUtxo, tSettings); err != nil {
		return err
	}

	return t.Delete(ctx, utxo.TxID)
}

func (t *TxMetaCache) GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return t.utxoStore.GetCounterConflicting(ctx, txHash)
}

func (t *TxMetaCache) GetConflictingChildren(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return t.utxoStore.GetConflictingChildren(ctx, txHash)
}

func (t *TxMetaCache) SetConflicting(ctx context.Context, txHashes []chainhash.Hash, setValue bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	return t.utxoStore.SetConflicting(ctx, txHashes, setValue)
}

func (t *TxMetaCache) SetUnspendable(ctx context.Context, txHashes []chainhash.Hash, setValue bool) error {
	return nil
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
