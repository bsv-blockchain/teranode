// Package txmetacache provides a memory-efficient caching layer for transaction metadata
// in the Teranode blockchain system. It serves as a performance optimization layer
// that wraps around an underlying UTXO store to reduce database load and improve
// transaction processing throughput.
//
// Key features:
// - Implements a configurable memory caching mechanism for transaction metadata
// - Uses height-based expiration to maintain cache freshness
// - Integrates with Prometheus for operational metrics and monitoring
// - Provides both single and batch operations for transaction metadata retrieval and updates
// - Supports parallel processing for improved performance on multi-core systems
//
// The package works by intercepting calls to the underlying UTXO store, caching results
// in memory, and proxying other calls through to the underlying store. This architecture
// significantly reduces database load for frequently accessed transaction data during
// block validation and mempool management.
package txmetacache

import (
	"context"
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

// metrics holds atomic counters for collecting operational statistics about the cache.
// These metrics are exposed via Prometheus for monitoring cache effectiveness and performance.
// All counters use atomic operations to ensure thread-safety in concurrent environments.
//
// These metrics are critical for operational monitoring and can help identify:
// - Cache hit ratio (hits vs. misses) to evaluate cache effectiveness
// - Insertion and eviction rates to detect memory pressure
// - Age-related expiration patterns through hitOldTx tracking
type metrics struct {
	insertions atomic.Uint64 // Tracks number of items inserted into the cache; indicates write throughput
	hits       atomic.Uint64 // Tracks number of successful cache retrievals; indicates cache effectiveness
	misses     atomic.Uint64 // Tracks number of failed cache retrievals; helps identify sizing issues
	evictions  atomic.Uint64 // Tracks number of items evicted from the cache; indicates memory pressure
	hitOldTx   atomic.Uint64 // Tracks number of cache hits for outdated transactions; monitors expiration policy
}

// TxMetaCache wraps a utxo.Store implementation and adds caching capabilities for transaction metadata.
// This significantly improves performance for frequently accessed transaction information while providing the
// same interface but adding caching capabilities.
//
// The cache is designed for high-throughput blockchain environments where transaction metadata
// is frequently accessed during validation, mining, and mempool management operations.
// It implements an LRU-like eviction policy based on block height to ensure that older, less
// relevant transactions are removed first when memory pressure occurs.
//
// Thread-safety: All operations are thread-safe and can be called concurrently from multiple goroutines.
type TxMetaCache struct {
	utxoStore                     utxo.Store     // The underlying UTXO store that this cache wraps; provides persistence
	cache                         *ImprovedCache // The in-memory cache implementation; provides high-performance access
	metrics                       metrics        // Performance metrics for monitoring cache efficiency and throughput
	logger                        ulogger.Logger // Logger for operational logging and diagnostic information
	noOfBlocksToKeepInTxMetaCache uint32         // Configuration for cache expiration based on block height; controls data retention
}

// CacheStats provides statistical information about the current state of the cache.
// These statistics are useful for monitoring cache utilization and performance.
// The metrics exposed through this struct are critical for operational monitoring,
// capacity planning, and performance tuning of the transaction metadata cache.
type CacheStats struct {
	TrimCount          uint64 // Number of trim operations performed; indicates memory management activity
	TotalMapSize       uint64 // Total size of all map buckets in the cache; reflects memory consumption
	TotalElementsAdded uint64 // Cumulative count of all elements added to the cache; measures total throughput

	// EntriesCount is the current number of entries in the cache map.
	// This includes both valid and stale entries.
	// Use ValidEntriesCount for the actual number of readable entries.
	// EntriesCount uint64

	// ValidEntriesCount is the number of valid (readable) entries.
	// This equals CurrentGenEntries + PreviousGenEntries.
	ValidEntriesCount uint64

	// CurrentGenEntries is the count of entries written in the current generation.
	CurrentGenEntries uint64

	// PreviousGenEntries is the count of valid entries from the previous generation
	// that have not yet been overwritten.
	PreviousGenEntries uint64
}

// BucketType defines the allocation strategy for the cache's internal buckets.
// This affects memory usage patterns and performance characteristics of the cache.
// The choice of bucket type has significant implications for memory allocation,
// startup time, and runtime performance in high-throughput environments.
type BucketType int

const (
	// Unallocated indicates that memory for buckets will be allocated on demand.
	// This strategy minimizes initial memory usage and is suitable for environments
	// with constrained resources or where cache usage patterns are unpredictable.
	// However, it may lead to more frequent memory allocations during operation.
	Unallocated BucketType = iota

	// Preallocated indicates that memory for buckets will be allocated upfront.
	// This strategy improves runtime performance by avoiding allocations during
	// high-throughput operations, but increases initial memory consumption and
	// startup time. Recommended for production environments with predictable load.
	Preallocated

	// Trimmed indicates that the cache should maintain a trimmed state,
	// which can help control memory usage. This strategy periodically removes
	// less frequently used entries to maintain a smaller memory footprint.
	// Suitable for long-running services with memory constraints.
	Trimmed
)

// NewTxMetaCache creates a new transaction metadata cache that wraps an existing UTXO store.
// The cache intercepts and handles transaction metadata operations to improve performance
// while maintaining the same interface as the underlying store.
//
// Parameters:
// - ctx: Context for lifecycle management and cancellation
// - tSettings: Teranode settings containing cache configuration parameters
// - logger: Logger for operational logging and diagnostics
// - utxoStore: The underlying UTXO store that this cache will wrap
// - bucketType: Strategy for memory allocation in the cache (Unallocated, Preallocated, or Trimmed)
// - maxMBOverride: Optional override for the cache size in MB (defaults to config value if not provided)
//
// Returns:
// - A utxo.Store interface that can be used in place of the original store
// - Error if initialization fails
//
// The function starts a background goroutine that updates Prometheus metrics every 5 seconds
// to provide operational visibility into cache performance.
func NewTxMetaCache(
	ctx context.Context,
	tSettings *settings.Settings,
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

	cache, err := New(maxMB*1024*1024, bucketType)
	if err != nil {
		return nil, errors.NewProcessingError("error creating cache", err)
	}

	const percentageOfGlobalBlockHeightRetentionToKeep = 10

	var noOfBlocksToKeepInTxMetaCache uint32

	if tSettings.GlobalBlockHeightRetention < percentageOfGlobalBlockHeightRetentionToKeep {
		noOfBlocksToKeepInTxMetaCache = 1
	} else {
		noOfBlocksToKeepInTxMetaCache = tSettings.GlobalBlockHeightRetention / percentageOfGlobalBlockHeightRetentionToKeep
	}

	m := &TxMetaCache{
		utxoStore:                     utxoStore,
		cache:                         cache,
		metrics:                       metrics{},
		logger:                        logger,
		noOfBlocksToKeepInTxMetaCache: noOfBlocksToKeepInTxMetaCache,
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
					// prometheusBlockValidationTxMetaCacheSize.Set(float64(cacheStats.EntriesCount))
					prometheusBlockValidationTxMetaCacheInsertions.Set(float64(m.metrics.insertions.Load()))
					prometheusBlockValidationTxMetaCacheHits.Set(float64(m.metrics.hits.Load()))
					prometheusBlockValidationTxMetaCacheMisses.Set(float64(m.metrics.misses.Load()))
					prometheusBlockValidationTxMetaCacheEvictions.Set(float64(m.metrics.evictions.Load()))
					prometheusBlockValidationTxMetaCacheTrims.Set(float64(cacheStats.TrimCount))
					prometheusBlockValidationTxMetaCacheMapSize.Set(float64(cacheStats.TotalMapSize))
					prometheusBlockValidationTxMetaCacheTotalElementsAdded.Set(float64(cacheStats.TotalElementsAdded))
					prometheusBlockValidationTxMetaCacheValidEntriesCount.Set(float64(cacheStats.ValidEntriesCount))
					prometheusBlockValidationTxMetaCacheCurrentGenEntries.Set(float64(cacheStats.CurrentGenEntries))
					prometheusBlockValidationTxMetaCachePreviousGenEntries.Set(float64(cacheStats.PreviousGenEntries))
					prometheusBlockValidationTxMetaCacheHitOldTx.Set(float64(m.metrics.hitOldTx.Load()))
				}
			}
		}
	}()

	return m, nil
}

// SetCache adds or updates transaction metadata in the cache for a given transaction hash.
// Before storing, it removes the actual transaction from the metadata to save memory,
// and appends the current block height to support expiration policies.
//
// Parameters:
// - hash: Transaction hash to use as the cache key
// - txMeta: Transaction metadata to store in the cache
//
// Returns:
// - Error if the cache operation fails
func (t *TxMetaCache) SetCache(hash *chainhash.Hash, txMeta *meta.Data) error {
	txMetaBytes, err := txMeta.MetaBytes()
	if err != nil {
		return err
	}

	return t.SetCacheFromBytes(hash.CloneBytes(), txMetaBytes)
}

// SetCacheFromBytes adds or updates transaction metadata in the cache using raw byte slices.
// This is a lower-level alternative to SetCache that avoids unnecessary conversions
// when the cache key and metadata are already available as byte slices.
//
// Parameters:
// - key: Raw byte slice to use as the cache key (typically a transaction hash)
// - txMetaBytes: Serialized transaction metadata to store in the cache
//
// Returns:
// - Error if the cache operation fails
func (t *TxMetaCache) SetCacheFromBytes(key, txMetaBytes []byte) error {
	err := t.cache.Set(key, t.appendHeightToValue(txMetaBytes))
	if err != nil {
		return err
	}

	t.metrics.insertions.Add(1)

	return nil
}

// SetCacheMulti performs a batch operation to add or update multiple transaction metadata
// entries in the cache simultaneously. This is more efficient than calling SetCacheFromBytes
// multiple times when many transactions need to be cached at once.
//
// Parameters:
// - keys: Slice of raw byte slices to use as cache keys (typically transaction hashes)
// - values: Slice of serialized transaction metadata to store in the cache, corresponding to keys
//
// Returns:
// - Error if the batch cache operation fails
func (t *TxMetaCache) SetCacheMulti(keys [][]byte, values [][]byte) error {
	valuesWithHeight := make([][]byte, len(values))
	for i, value := range values {
		valuesWithHeight[i] = t.appendHeightToValue(value)
	}

	err := t.cache.SetMulti(keys, valuesWithHeight)
	if err != nil {
		return err
	}

	t.metrics.insertions.Add(uint64(len(keys)))

	return nil
}

func (t *TxMetaCache) SetCacheMultiValuesRaw(keys [][]byte, values [][]byte) error {

	err := t.cache.SetMulti(keys, values)
	if err != nil {
		return err
	}

	t.metrics.insertions.Add(uint64(len(keys)))

	return nil
}

// GetMetaCached retrieves transaction metadata from the cache without falling back to the
// underlying store. This provides a way to check if data is available in the cache only.
//
// Parameters:
// - ctx: Context for the operation (not used, but maintained for interface consistency)
// - hash: Transaction hash to use as the cache key
//
// Returns:
// - Pointer to the cached transaction metadata if found and not expired, nil otherwise
// - Error if the cache operation fails or if the data cannot be unmarshalled
//
// The function performs several checks:
// 1. Verifies the data exists in the cache
// 2. Validates that the data is not empty
// 3. Checks if the data has expired based on block height
// All these conditions have corresponding metrics incremented for monitoring.
func (t *TxMetaCache) GetMetaCached(_ context.Context, hash chainhash.Hash, txmetaData *meta.Data) (bool, error) {
	cachedBytes := make([]byte, 0, 64)

	if err := t.cache.Get(&cachedBytes, hash[:]); err != nil {
		t.metrics.misses.Add(1)

		return false, err
	}

	if len(cachedBytes) == 0 {
		t.metrics.misses.Add(1)
		t.logger.Warnf("txMetaCache empty for %s", hash.String())

		return false, nil
	}

	t.metrics.hits.Add(1)

	if err := meta.NewMetaDataFromBytes(cachedBytes, txmetaData); err != nil {
		return false, errors.NewProcessingError("Failed to unmarshal txmetaData", err)
	}

	return true, nil
}

// GetMeta retrieves transaction metadata for a given transaction hash, first checking the cache
// and falling back to the underlying store if needed.
//
// Parameters:
// - ctx: Context for the operation
// - hash: Transaction hash to retrieve metadata for
// - data: Pre-allocated meta.Data struct to populate with the retrieved metadata
//
// Returns:
// - Error if retrieval fails
//
// This is one of the primary interface methods that proxies calls to the underlying store
// with a caching layer in between for improved performance.
func (t *TxMetaCache) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	cachedBytes := make([]byte, 0)

	err := t.cache.Get(&cachedBytes, hash[:])
	if err != nil && !errors.Is(err, errors.ErrNotFound) {
		return err
	}

	if len(cachedBytes) > 0 {
		t.metrics.hits.Add(1)

		if err = meta.NewMetaDataFromBytes(cachedBytes, data); err != nil {
			return err
		}

		data.BlockIDs = make([]uint32, 0) // this is expected behavior, needs to be non-nil

		return nil
	}

	t.metrics.misses.Add(1)
	t.logger.Warnf("txMetaCache GetMeta miss for %s", hash.String())

	if err := t.utxoStore.GetMeta(ctx, hash, data); err != nil {
		return err
	}

	prometheusBlockValidationTxMetaCacheGetOrigin.Add(1)

	// add to cache, but only if the blockIDs have not been set
	if len(data.BlockIDs) == 0 {
		// don't return errors from SetCache, as it is not critical if the cache fails to set
		_ = t.SetCache(hash, data)
	}

	return nil
}

// Get retrieves transaction data from the underlying store.
// It never uses a caching mechanism, because the Get function by default
// returns fields that are never cached in the txmetacache
//
// Parameters:
// - ctx: Context for the operation
// - hash: Hash of the transaction to retrieve metadata for
// - fields: Optional list of specific metadata fields to retrieve
//
// Returns:
// - Transaction data if found
// - Error if retrieval fails
func (t *TxMetaCache) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	return t.utxoStore.Get(ctx, hash, f...)
}

// BatchDecorate retrieves metadata for multiple transactions in a single batch operation.
// This is more efficient than calling Get for each transaction individually.
//
// Parameters:
// - ctx: Context for the operation
// - hashes: List of unresolved transaction metadata objects to populate
// - fields: Optional list of specific metadata fields to retrieve
//
// Returns:
// - Error if the batch operation fails
//
// The method optimizes retrieval by first checking the cache for each transaction,
// then batching any cache misses into a single call to the underlying store.
func (t *TxMetaCache) BatchDecorate(ctx context.Context, hashes []*utxo.UnresolvedMetaData, fields ...fields.FieldName) error {
	if err := t.utxoStore.BatchDecorate(ctx, hashes, fields...); err != nil {
		return err
	}

	prometheusBlockValidationTxMetaCacheGetOrigin.Add(float64(len(hashes)))

	// Batch cache population: build keys/values once and call SetCacheMulti.
	// Note: values are serialized the same way SetCache() does (MetaBytes + height appended inside SetCacheMulti).
	keys := make([][]byte, 0, len(hashes))
	values := make([][]byte, 0, len(hashes))

	for _, data := range hashes {
		if data == nil || data.Data == nil {
			continue
		}

		if len(data.Data.TxInpoints.ParentTxHashes) > 48 {
			t.logger.Warnf("stored tx meta maybe too big for txmeta cache, size: %d, parent hash count: %d", data.Data.SizeInBytes, len(data.Data.TxInpoints.ParentTxHashes))
		}

		// get the metabytes directly here.
		txMetaBytes, err := data.Data.MetaBytes()
		if err != nil {
			if errors.Is(err, errors.ErrProcessing) {
				t.logger.Debugf("error serializing txMeta for [%s]: %v", data.Hash.String(), err)
			} else {
				t.logger.Errorf("error serializing txMeta for [%s]: %v", data.Hash.String(), err)
			}
			continue
		}

		keys = append(keys, data.Hash.CloneBytes())
		values = append(values, txMetaBytes)
	}

	if len(keys) > 0 {
		if err := t.SetCacheMultiValuesRaw(keys, values); err != nil {
			// Preserve prior behavior: cache population failures are logged but don't fail BatchDecorate.
			if errors.Is(err, errors.ErrProcessing) {
				t.logger.Debugf("error setting cache batch for %d txMeta entries: %v", len(keys), err)
			} else {
				t.logger.Errorf("error setting cache batch for %d txMeta entries: %v", len(keys), err)
			}
		}
	}

	return nil
}

// Create adds a new transaction to the system, updating both the underlying store and the cache.
// This is typically called when a new transaction is seen, either from the mempool or in a block.
//
// Parameters:
// - ctx: Context for the operation
// - tx: The transaction to create metadata for
// - blockHeight: The current blockchain height
// - opts: Optional creation options such as block information
//
// Returns:
// - The created transaction metadata
// - Error if creation fails
//
// This method delegates the creation to the underlying store and then adds the result to the cache.
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

	// add to cache, but only if the blockIDs have not been set
	if len(txMeta.BlockIDs) == 0 && !txMeta.Conflicting {
		// don't return errors from SetCache, as it is not critical if the cache fails to set
		_ = t.SetCache(txHash, txMeta)
	}

	return txMeta, nil
}

// setMinedInCache updates the cache with information about a transaction that has been mined in a block.
// This internal helper method is used by both SetMined and SetMinedMulti to maintain cache consistency.
//
// Parameters:
// - ctx: Context for the operation
// - hash: Hash of the transaction to mark as mined
// - minedBlockInfo: Information about the block where the transaction was mined
//
// Returns:
// - Error if updating the transaction metadata fails
func (t *TxMetaCache) setMinedInCache(ctx context.Context, hash *chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (blockIDs []uint32, err error) {
	var txMeta *meta.Data

	txMeta, err = t.Get(ctx, hash)
	if err != nil {
		txMeta, err = t.utxoStore.Get(ctx, hash)
	}

	if err != nil {
		return nil, err
	}

	if txMeta.BlockIDs == nil {
		txMeta.BlockIDs = []uint32{
			minedBlockInfo.BlockID,
		}
	} else {
		txMeta.BlockIDs = append(txMeta.BlockIDs, minedBlockInfo.BlockID)
	}

	// if the blockID is not set, then we need to set it
	if len(txMeta.BlockIDs) == 0 {
		// don't return errors from SetCache, as it is not critical if the cache fails to set
		_ = t.SetCache(hash, txMeta)
	}

	return txMeta.BlockIDs, nil
}

// SetMined marks a transaction as mined in a specific block, updating both the
// underlying store and the cache to maintain consistency.
//
// Parameters:
// - ctx: Context for the operation
// - hash: Hash of the transaction to mark as mined
// - minedBlockInfo: Information about the block where the transaction was mined
//
// Returns:
// - Error if updating either the underlying store or cache fails
func (t *TxMetaCache) SetMined(ctx context.Context, hash *chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) ([]uint32, error) {
	blockIDsMap, err := t.utxoStore.SetMinedMulti(ctx, []*chainhash.Hash{hash}, minedBlockInfo)
	if err != nil {
		return nil, err
	}

	_, err = t.setMinedInCache(ctx, hash, minedBlockInfo)
	if err != nil {
		return nil, err
	}

	return blockIDsMap[*hash], nil
}

// SetMinedMulti marks multiple transactions as mined in a specific block.
// This batch operation is more efficient than calling SetMined multiple times.
//
// Parameters:
// - ctx: Context for the operation
// - hashes: List of transaction hashes to mark as mined
// - minedBlockInfo: Information about the block where the transactions were mined
//
// Returns:
// - Error if updating the cache for any transaction fails
func (t *TxMetaCache) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	blockIDsMap := make(map[chainhash.Hash][]uint32, len(hashes))

	for _, hash := range hashes {
		blockIDs, err := t.setMinedInCache(ctx, hash, minedBlockInfo)
		if err != nil {
			return nil, err
		}

		blockIDsMap[*hash] = blockIDs
	}

	return blockIDsMap, nil
}

// SetMinedMultiParallel marks multiple transactions as mined in a specific block using parallel processing.
// This method provides better performance for large transaction batches by distributing the work
// across multiple goroutines.
//
// Parameters:
// - ctx: Context for the operation
// - hashes: List of transaction hashes to mark as mined
// - blockID: ID of the block where the transactions were mined
//
// Returns:
// - Error if updating the cache for any transaction fails
func (t *TxMetaCache) SetMinedMultiParallel(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) (err error) {
	err = t.setMinedInCacheParallel(ctx, hashes, blockID)
	if err != nil {
		return err
	}

	return nil
}

func (t *TxMetaCache) GetUnminedTxIterator(bool) (utxo.UnminedTxIterator, error) {
	return nil, errors.NewProcessingError("not implemented")
}

func (t *TxMetaCache) GetPrunableUnminedTxIterator(cutoffBlockHeight uint32) (utxo.UnminedTxIterator, error) {
	return t.utxoStore.GetPrunableUnminedTxIterator(cutoffBlockHeight)
}

// setMinedInCacheParallel is an internal helper method that updates the mined status
// of multiple transactions in parallel, using goroutines to improve performance.
//
// Parameters:
// - ctx: Context for the operation
// - hashes: List of transaction hashes to mark as mined
// - blockID: ID of the block where the transactions were mined
//
// Returns:
// - Error if updating the cache for any transaction fails
//
// The method uses an errgroup to manage concurrent updates while properly handling errors.
func (t *TxMetaCache) setMinedInCacheParallel(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) (err error) {
	var txMeta *meta.Data

	g := new(errgroup.Group)
	util.SafeSetLimit(g, 100)

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

			return t.SetCache(hash, txMeta)
		})
	}

	return nil
}

// Delete removes a transaction's metadata from the cache.
// This is typically used when a transaction becomes invalid or is no longer needed.
// Unlike other methods that delegate to the underlying store, this method only affects
// the in-memory cache and does not modify the persistent storage.
//
// Parameters:
// - ctx: Context for the operation (unused but required by interface)
// - hash: Hash of the transaction to delete
//
// Returns:
// - Error if deletion from the cache fails (currently always returns nil)
//
// This method is used internally by operations that modify transaction state, such as
// FreezeUTXOs and ReAssignUTXO, to ensure cache consistency with the underlying store.
// It also increments the evictions metric to track manual cache removals.
func (t *TxMetaCache) Delete(_ context.Context, hash *chainhash.Hash) error {
	t.cache.Del(hash[:])
	t.metrics.evictions.Add(1)

	return nil
}

// appendHeightToValue appends the current block height to the end of the txMetaBytes.
// This allows for height-based expiration of cache entries, enabling the cache to automatically
// discard entries that are no longer relevant as the blockchain grows.
//
// Parameters:
// - txMetaBytes: The serialized transaction metadata
//
// Returns:
// - A new byte slice containing the original metadata followed by the current block height
//
// The height is encoded as a little-endian uint32 in the last 4 bytes of the returned slice.
func (t *TxMetaCache) appendHeightToValue(txMetaBytes []byte) []byte {
	height := t.utxoStore.GetBlockHeight()
	valueWithHeight := make([]byte, len(txMetaBytes)+4)
	copy(valueWithHeight, txMetaBytes)
	binary.BigEndian.PutUint32(valueWithHeight[len(txMetaBytes):], height)

	return valueWithHeight
}

// GetCacheStats retrieves current operational statistics from the underlying cache.
// These statistics are useful for monitoring cache performance and behavior.
//
// Returns:
// - A CacheStats structure containing current statistical information about the cache
//
// This method is primarily used by the metrics reporting goroutine to collect and expose
// cache performance data via Prometheus, but can also be used for debugging or diagnostics.
func (t *TxMetaCache) GetCacheStats() *CacheStats {
	s := &Stats{}
	t.cache.UpdateStats(s)

	return &CacheStats{
		// EntriesCount:        s.EntriesCount,
		TrimCount:          s.TrimCount,
		TotalMapSize:       s.TotalMapSize,
		TotalElementsAdded: s.TotalElementsAdded,
		ValidEntriesCount:  s.ValidEntriesCount,
		CurrentGenEntries:  s.CurrentGenEntries,
		PreviousGenEntries: s.PreviousGenEntries,
	}
}

// Health performs a health check on both the cache and the underlying store.
// This is used for monitoring and operational status reporting.
//
// Parameters:
// - ctx: Context for the operation
// - checkLiveness: Whether to perform additional liveness checks
//
// Returns:
// - HTTP status code indicating health status
// - Descriptive message about the health state
// - Error if the health check encounters a problem
func (t *TxMetaCache) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return t.utxoStore.Health(ctx, checkLiveness)
}

// GetSpend retrieves information about a specific UTXO spend attempt.
// This method delegates directly to the underlying UTXO store without caching.
//
// Parameters:
// - ctx: Context for the operation
// - spend: The spend object containing information about the UTXO being spent
//
// Returns:
// - SpendResponse containing the result of the spend attempt
// - Error if the retrieval fails
func (t *TxMetaCache) GetSpend(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error) {
	return t.utxoStore.GetSpend(ctx, spend)
}

// Spend marks UTXOs as spent by a transaction.
// This method delegates directly to the underlying UTXO store without caching.
//
// Parameters:
// - ctx: Context for the operation
// - tx: The transaction that spends the UTXOs
// - ignoreFlags: Optional flags to modify spending behavior
//
// Returns:
// - Array of Spend objects representing the spent UTXOs
// - Error if the spend operation fails
func (t *TxMetaCache) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	return t.utxoStore.Spend(ctx, tx, blockHeight, ignoreFlags...)
}

// Unspend marks previously spent UTXOs as unspent.
// This method delegates directly to the underlying UTXO store without caching.
//
// Parameters:
// - ctx: Context for the operation
// - spends: Array of Spend objects to mark as unspent
// - flagAsLocked: Optional flag to mark the UTXOs as locked
//
// Returns:
// - Error if the unspend operation fails
func (t *TxMetaCache) Unspend(ctx context.Context, spends []*utxo.Spend, flagAsLocked ...bool) error {
	return t.utxoStore.Unspend(ctx, spends, flagAsLocked...)
}

// PreviousOutputsDecorate populates previous output information for a list of outpoints.
// This method delegates directly to the underlying UTXO store without caching.
//
// Parameters:
// - ctx: Context for the operation
// - outpoints: List of previous outputs to decorate with additional information
//
// Returns:
// - Error if the decoration operation fails
func (t *TxMetaCache) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {
	return t.utxoStore.PreviousOutputsDecorate(ctx, tx)
}

// FreezeUTXOs marks UTXOs as frozen and not spendable in the underlying store
// and removes any related entries from the cache to ensure consistency.
//
// Parameters:
// - ctx: Context for the operation
// - spends: Array of Spend objects representing UTXOs to freeze
// - tSettings: Transaction settings that control the freeze behavior
//
// Returns:
// - Error if the freeze operation fails
//
// This method ensures cache consistency by removing any cached entries for
// transactions affected by the freeze operation.
func (t *TxMetaCache) FreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	if err := t.utxoStore.FreezeUTXOs(ctx, spends, tSettings); err != nil {
		return err
	}

	for _, spend := range spends {
		_ = t.Delete(ctx, spend.TxID)
	}

	return nil
}

// UnFreezeUTXOs removes the frozen status from UTXOs in the underlying store.
// This method ensures that the cache remains consistent with the underlying store.
//
// Parameters:
// - ctx: Context for the operation
// - spends: Array of Spend objects representing UTXOs to unfreeze
// - tSettings: Transaction settings that control the unfreeze behavior
//
// Returns:
// - Error if the unfreeze operation fails
func (t *TxMetaCache) UnFreezeUTXOs(ctx context.Context, spends []*utxo.Spend, tSettings *settings.Settings) error {
	if err := t.utxoStore.UnFreezeUTXOs(ctx, spends, tSettings); err != nil {
		return err
	}

	for _, spend := range spends {
		_ = t.Delete(ctx, spend.TxID)
	}

	return nil
}

// ReAssignUTXO reassigns a UTXO from one transaction to another in the underlying store
// and ensures cache consistency by removing any related entries.
//
// Parameters:
// - ctx: Context for the operation
// - utxo: The original UTXO to reassign
// - newUtxo: The new UTXO to assign to
// - tSettings: Transaction settings that control the reassignment behavior
//
// Returns:
// - Error if the reassignment operation fails
//
// This method maintains cache consistency by removing both the original and new
// transaction entries from the cache after the reassignment is complete.
func (t *TxMetaCache) ReAssignUTXO(ctx context.Context, utxo *utxo.Spend, newUtxo *utxo.Spend, tSettings *settings.Settings) error {
	if err := t.utxoStore.ReAssignUTXO(ctx, utxo, newUtxo, tSettings); err != nil {
		return err
	}

	return t.Delete(ctx, utxo.TxID)
}

// GetCounterConflicting retrieves a list of transactions that are in conflict with the specified transaction.
// This method delegates directly to the underlying UTXO store without caching.
func (t *TxMetaCache) GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return t.utxoStore.GetCounterConflicting(ctx, txHash)
}

// GetConflictingChildren retrieves a list of child transactions that conflict with the specified transaction.
// This method delegates directly to the underlying UTXO store without caching.
//
// Parameters:
// - ctx: Context for the operation
// - txHash: Hash of the transaction to check for conflicting children
//
// Returns:
// - Array of transaction hashes that are conflicting children of the specified transaction
// - Error if the retrieval fails
func (t *TxMetaCache) GetConflictingChildren(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error) {
	return t.utxoStore.GetConflictingChildren(ctx, txHash)
}

// SetConflicting marks transactions as conflicting or non-conflicting in the underlying store.
// This method delegates directly to the underlying UTXO store without caching.
//
// Parameters:
// - ctx: Context for the operation
// - txHashes: Array of transaction hashes to mark as conflicting or non-conflicting
// - setValue: Whether to mark the transactions as conflicting (true) or non-conflicting (false)
//
// Returns:
// - Array of Spend objects representing the affected UTXOs
// - Array of transaction hashes that were successfully marked
// - Error if the operation fails
func (t *TxMetaCache) SetConflicting(ctx context.Context, txHashes []chainhash.Hash, setValue bool) ([]*utxo.Spend, []chainhash.Hash, error) {
	return t.utxoStore.SetConflicting(ctx, txHashes, setValue)
}

// SetLocked marks transactions as locked and not spendable.
//
// Parameters:
// - ctx: Context for the operation
// - txHashes: Array of transaction hashes to mark as locked or not locked
// - setValue: Whether to mark the transactions as locked (true) or not locked (false)
//
// Returns:
// - Error if the operation fails
func (t *TxMetaCache) SetLocked(ctx context.Context, txHashes []chainhash.Hash, setValue bool) error {
	return t.utxoStore.SetLocked(ctx, txHashes, setValue)
}

// MarkTransactionsOnLongestChain marks transactions as being on the longest chain or not.
//
// Parameters:
// - ctx: Context for the operation
// - txHashes: Array of transaction hashes to mark as on/not on longest chain
// - onLongestChain: Whether transactions are on the longest chain
//
// Returns:
// - Error if the operation fails
func (t *TxMetaCache) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	return t.utxoStore.MarkTransactionsOnLongestChain(ctx, txHashes, onLongestChain)
}

// SetBlockHeight updates the current block height in the underlying store.
// This is critical for cache expiration as it determines which cached entries are considered stale.
// The block height is a fundamental parameter for the cache's LRU-like eviction policy, which
// removes entries based on their age relative to the current blockchain tip.
//
// Parameters:
// - height: The new blockchain height to set
//
// Returns:
// - Error if updating the block height fails
//
// This method should be called whenever a new block is added to the blockchain to ensure
// that the cache properly manages its entries based on the latest blockchain state.
func (t *TxMetaCache) SetBlockHeight(height uint32) error {
	return t.utxoStore.SetBlockHeight(height)
}

// GetBlockHeight retrieves the current blockchain height from the underlying store.
// This height is used to determine which cache entries should be considered expired.
//
// Returns:
// - The current blockchain height
func (t *TxMetaCache) GetBlockHeight() uint32 {
	return t.utxoStore.GetBlockHeight()
}

// SetMedianBlockTime updates the median block time in the underlying store.
// This is used for transaction validation and other time-based operations.
// The median block time is a critical parameter for validating time-locked transactions
// and ensuring that transactions with time-based conditions are properly processed.
//
// Parameters:
// - height: Block height used to calculate the median time
//
// Returns:
// - Error if updating the median block time fails
//
// This method should be called whenever the blockchain tip changes to ensure
// that time-based transaction validations use the most current median time value.
func (t *TxMetaCache) SetMedianBlockTime(height uint32) error {
	return t.utxoStore.SetMedianBlockTime(height)
}

// GetMedianBlockTime retrieves the current median block time from the underlying store.
// This time value is used for validating time-based transaction conditions.
// The median time is calculated from a window of recent blocks and provides a more
// stable time reference than using the latest block's timestamp directly.
//
// Returns:
// - The current median block time as a Unix timestamp (seconds since epoch)
//
// This method is typically used during transaction validation to check time-locked
// transactions (using nLockTime or CHECKLOCKTIMEVERIFY) against the blockchain's
// consensus-determined time.
func (t *TxMetaCache) GetMedianBlockTime() uint32 {
	return t.utxoStore.GetMedianBlockTime()
}

func (t *TxMetaCache) GetBlockState() utxo.BlockState {
	return t.utxoStore.GetBlockState()
}

// QueryOldUnminedTransactions forwards the query request to the underlying UTXO store.
// The cache doesn't directly manage unmined transactions, so this is a pass-through operation.
func (t *TxMetaCache) QueryOldUnminedTransactions(ctx context.Context, cutoffBlockHeight uint32) ([]chainhash.Hash, error) {
	return t.utxoStore.QueryOldUnminedTransactions(ctx, cutoffBlockHeight)
}

// PreserveTransactions forwards the preservation request to the underlying UTXO store.
// The cache doesn't directly manage transaction preservation, so this is a pass-through operation.
func (t *TxMetaCache) PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error {
	return t.utxoStore.PreserveTransactions(ctx, txIDs, preserveUntilHeight)
}

// ProcessExpiredPreservations forwards the request to the underlying UTXO store.
// The cache doesn't directly manage preservation expiry, so this is a pass-through operation.
func (t *TxMetaCache) ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error {
	return t.utxoStore.ProcessExpiredPreservations(ctx, currentHeight)
}
