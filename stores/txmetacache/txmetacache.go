package txmetacache

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
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
	txMetaStore   txmeta.Store
	cache         map[[1]byte]*util.SyncedSwissMap[chainhash.Hash, *txmeta.Data]
	cacheTTL      time.Duration
	cacheTTLQueue *LockFreeTTLQueue
	metrics       metrics
	logger        ulogger.Logger
}

func NewTxMetaCache(ctx context.Context, logger ulogger.Logger, txMetaStore txmeta.Store, options ...int) txmeta.Store {
	initPrometheusMetrics()

	cacheMaxSize, _ := gocore.Config().GetInt("txMetaCacheMaxSize", 1_000_000_000)
	ttl, _ := gocore.Config().GetInt("txMetaCacheTTL", 15)
	if ttl <= 0 {
		ttl = 5
	}
	cacheTtl := time.Duration(ttl) * time.Minute
	if len(options) > 0 {
		cacheMaxSize = options[0]
	}
	if len(options) > 1 {
		ttl = options[1]
		cacheTtl = time.Duration(ttl) * time.Second
	}

	m := &TxMetaCache{
		txMetaStore:   txMetaStore,
		cache:         make(map[[1]byte]*util.SyncedSwissMap[chainhash.Hash, *txmeta.Data]),
		cacheTTL:      cacheTtl,
		cacheTTLQueue: NewLockFreeTTLQueue(int64(cacheMaxSize)),
		metrics:       metrics{},
		logger:        logger,
	}

	for i := 0; i < 256; i++ {
		m.cache[[1]byte{byte(i)}] = util.NewSyncedSwissMap[chainhash.Hash, *txmeta.Data](uint32(1024 * 1024))
	}

	go func() {
		size := 1000
		batches := make(map[[1]byte][]chainhash.Hash)
		lengths := make(map[[1]byte]int)

		for i := 0; i < 256; i++ {
			batches[[1]byte{byte(i)}] = make([]chainhash.Hash, size)
		}

		for {
			select {
			case <-ctx.Done():
				return
			default:

				item := m.cacheTTLQueue.dequeue(time.Now().Add(-m.cacheTTL).UnixMilli())
				if item == nil || lengths[[1]byte{item.hash[0]}] == size {

					// process batch for each shard
					for shard, hashes := range batches {
						index := lengths[shard]
						if index > 0 {
							// Make a copy of hashes to avoid data race
							hashesCopy := make([]chainhash.Hash, index)
							copy(hashesCopy, hashes[:index])
							go func(sm *util.SyncedSwissMap[chainhash.Hash, *txmeta.Data]) {
								sm.DeleteBatch(hashesCopy)
							}(m.cache[shard])
							m.metrics.evictions.Add(uint64(index))

							// reset length
							lengths[shard] = 0
						}
					}

					if item == nil {
						time.Sleep(100 * time.Millisecond)
					}
				} else {
					shard := [1]byte{item.hash[0]}
					hashes := batches[shard]
					length := lengths[shard]
					hashes[length] = *item.hash
					lengths[shard]++
				}
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if prometheusBlockValidationTxMetaCacheSize != nil {
					prometheusBlockValidationTxMetaCacheSize.Set(float64(m.cacheTTLQueue.length()))
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
	t.cache[[1]byte{hash[0]}].Set(*hash, txMeta)
	t.cacheTTLQueue.enqueue(&ttlQueueItem{hash: hash})

	t.metrics.insertions.Add(1)

	return nil
}

func (t *TxMetaCache) GetCache(hash *chainhash.Hash) (*txmeta.Data, bool) {
	cached, ok := t.cache[[1]byte{hash[0]}].Get(*hash)
	if ok {
		t.metrics.hits.Add(1)
		return cached, ok
	}

	t.metrics.misses.Add(1)
	return nil, false
}

func (t *TxMetaCache) GetMeta(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	cached, ok := t.GetCache(hash)
	if ok {
		return cached, nil
	}

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
	cached, ok := t.GetCache(hash)
	if ok {
		return cached, nil
	}

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

func (t *TxMetaCache) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error {
	err := t.txMetaStore.SetMinedMulti(ctx, hashes, blockID)
	if err != nil {
		return err
	}

	for _, hash := range hashes {
		err = t.setMinedInCache(ctx, hash, blockID)
		if err != nil {
			return err
		}
	}

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
	cached, ok := t.cache[[1]byte{hash[0]}].Get(*hash)
	if ok {
		txMeta = cached
		if txMeta.BlockIDs == nil {
			txMeta.BlockIDs = []uint32{
				blockID,
			}
		} else {
			txMeta.BlockIDs = append(txMeta.BlockIDs, blockID)
		}
	} else {
		txMeta, err = t.txMetaStore.Get(ctx, hash)
		if err != nil {
			return err
		}
	}

	txMeta.Tx = nil
	_ = t.SetCache(hash, txMeta)

	return nil
}

func (t *TxMetaCache) Delete(ctx context.Context, hash *chainhash.Hash) error {
	err := t.txMetaStore.Delete(ctx, hash)
	if err != nil {
		return err
	}

	t.cache[[1]byte{hash[0]}].Delete(*hash)
	t.metrics.evictions.Add(1)

	return nil
}
