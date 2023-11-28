package blockvalidation

import (
	"context"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/jellydator/ttlcache/v3"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

type txMetaCache struct {
	txMetaStore txmeta.Store
	cache       *ttlcache.Cache[chainhash.Hash, *txmeta.Data]
	cacheTTL    time.Duration
}

func newTxMetaCache(txMetaStore txmeta.Store) txmeta.Store {
	m := &txMetaCache{
		txMetaStore: txMetaStore,
		cache:       ttlcache.New[chainhash.Hash, *txmeta.Data](),
		cacheTTL:    15 * time.Minute, // until block is mined
	}

	go m.cache.Start()

	go func() {
		for {
			metrics := m.cache.Metrics()
			prometheusBlockValidationTxMetaCacheSize.Set(float64(m.cache.Len()))
			prometheusBlockValidationTxMetaCacheInsertions.Set(float64(metrics.Insertions))
			prometheusBlockValidationTxMetaCacheHits.Set(float64(metrics.Hits))
			prometheusBlockValidationTxMetaCacheMisses.Set(float64(metrics.Misses))
			prometheusBlockValidationTxMetaCacheEvictions.Set(float64(metrics.Evictions))

			time.Sleep(5 * time.Second)
		}
	}()

	return m
}

func (t txMetaCache) SetCache(_ context.Context, hash *chainhash.Hash, txMeta *txmeta.Data) error {
	txMeta.Tx = nil
	_ = t.cache.Set(*hash, txMeta, t.cacheTTL)

	return nil
}

func (t txMetaCache) GetMeta(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	cached := t.cache.Get(*hash)
	if cached != nil && cached.Value() != nil {
		return cached.Value(), nil
	}

	txMeta, err := t.txMetaStore.GetMeta(ctx, hash)
	if err != nil {
		return nil, err
	}

	// add to cache
	txMeta.Tx = nil
	_ = t.cache.Set(*hash, txMeta, t.cacheTTL)

	return txMeta, nil
}

func (t txMetaCache) Get(ctx context.Context, hash *chainhash.Hash) (*txmeta.Data, error) {
	cached := t.cache.Get(*hash)
	if cached != nil && cached.Value() != nil {
		return cached.Value(), nil
	}

	txMeta, err := t.txMetaStore.Get(ctx, hash)
	if err != nil {
		return nil, err
	}

	// add to cache
	txMeta.Tx = nil
	_ = t.cache.Set(*hash, txMeta, t.cacheTTL)

	return txMeta, nil
}

func (t txMetaCache) Create(ctx context.Context, tx *bt.Tx) (*txmeta.Data, error) {
	txMeta, err := t.txMetaStore.Create(ctx, tx)
	if err != nil {
		return txMeta, err
	}

	// add to cache
	txMeta.Tx = nil
	_ = t.cache.Set(*tx.TxIDChainHash(), txMeta, t.cacheTTL)

	return txMeta, nil
}

func (t txMetaCache) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockHash *chainhash.Hash) error {
	err := t.txMetaStore.SetMinedMulti(ctx, hashes, blockHash)
	if err != nil {
		return err
	}

	for _, hash := range hashes {
		err = t.setMinedInCache(ctx, hash, blockHash)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t txMetaCache) SetMined(ctx context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) error {
	err := t.txMetaStore.SetMined(ctx, hash, blockHash)
	if err != nil {
		return err
	}

	err = t.setMinedInCache(ctx, hash, blockHash)
	if err != nil {
		return err
	}

	return nil
}

func (t txMetaCache) setMinedInCache(ctx context.Context, hash *chainhash.Hash, blockHash *chainhash.Hash) (err error) {
	var txMeta *txmeta.Data
	cached := t.cache.Get(*hash)
	if cached != nil && cached.Value() != nil {
		txMeta = cached.Value()
		if txMeta.BlockHashes == nil {
			txMeta.BlockHashes = []*chainhash.Hash{
				blockHash,
			}
		} else {
			txMeta.BlockHashes = append(txMeta.BlockHashes, blockHash)
		}
	} else {
		txMeta, err = t.txMetaStore.Get(ctx, hash)
		if err != nil {
			return err
		}
	}

	txMeta.Tx = nil
	_ = t.cache.Set(*hash, txMeta, t.cacheTTL)

	return nil
}

func (t txMetaCache) Delete(ctx context.Context, hash *chainhash.Hash) error {
	err := t.txMetaStore.Delete(ctx, hash)
	if err != nil {
		return err
	}

	t.cache.Delete(*hash)

	return nil
}
