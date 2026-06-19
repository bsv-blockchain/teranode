// Package teraslab provides a TeraSlab-based implementation of the UTXO store interface.
// It uses the TeraSlab binary wire protocol for high-performance UTXO operations.
package teraslab

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	teraslab "github.com/icellan/teraslab/client/go"

	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// Ensure Store implements the utxo.Store interface.
var _ utxo.Store = (*Store)(nil)

type batcherIfc[T any] interface {
	Put(item *T, payloadSize ...int)
	Trigger()
	SetDrainMode(enabled bool)
	// Close drains all queued items through the flush fn and blocks until the
	// worker goroutine has fully unwound. Idempotent. Satisfied by
	// *batcher.Batcher[T] (go-batcher v2.0.4).
	Close()
}

// Store implements the UTXO store interface using TeraSlab.
// It is thread-safe for concurrent access.
type Store struct {
	client          *teraslab.Client
	blockHeight     atomic.Uint32
	medianBlockTime atomic.Uint32
	logger          ulogger.Logger
	settings        *settings.Settings
	utxoBatchSize   int

	storeBatcher  batcherIfc[batchStoreItem]
	getBatcher    batcherIfc[batchGetItem]
	spendBatcher  batcherIfc[batchSpendItem]
	lockedBatcher batcherIfc[batchLockedItem]
}

// New creates a new TeraSlab-based UTXO store.
// The URL format is: teraslab://host:port?pool_size=16&cluster=host2:port2,host3:port3
func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, teraslabURL *url.URL) (*Store, error) {
	addr := teraslabURL.Host

	cfg := teraslab.ClientConfig{
		Addr: addr,
	}

	// Parse pool_size from query parameters
	if poolSizeStr := teraslabURL.Query().Get("pool_size"); poolSizeStr != "" {
		poolSize, err := strconv.Atoi(poolSizeStr)
		if err != nil {
			return nil, fmt.Errorf("invalid pool_size: %w", err)
		}
		cfg.Pool.MaxConns = poolSize
	}

	// Parse cluster seeds from query parameters
	if clusterStr := teraslabURL.Query().Get("cluster"); clusterStr != "" {
		seeds := strings.Split(clusterStr, ",")
		// Include the primary address as a seed
		cfg.Seeds = append([]string{addr}, seeds...)
		cfg.Addr = "" // Clear addr when using cluster mode
	}

	client, err := teraslab.New(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("teraslab client init: %w", err)
	}

	utxoBatchSize := tSettings.UtxoStore.UtxoBatchSize
	if utxoBatchSize < 1 {
		utxoBatchSize = 20000
	}

	s := &Store{
		client:        client,
		logger:        logger,
		settings:      tSettings,
		utxoBatchSize: utxoBatchSize,
	}

	// Initialize batchers
	storeBatchSize := tSettings.UtxoStore.StoreBatcherSize
	storeBatchDuration := time.Duration(tSettings.UtxoStore.StoreBatcherDurationMillis) * time.Millisecond
	s.storeBatcher = batcher.New(storeBatchSize, storeBatchDuration, s.sendStoreBatch, !tSettings.BatcherDrainMode)

	getBatchSize := tSettings.UtxoStore.GetBatcherSize
	getBatchDuration := time.Duration(tSettings.UtxoStore.GetBatcherDurationMillis) * time.Millisecond
	s.getBatcher = batcher.New(getBatchSize, getBatchDuration, s.sendGetBatch, !tSettings.BatcherDrainMode)

	spendBatchSize := tSettings.UtxoStore.SpendBatcherSize
	spendBatchDuration := time.Duration(tSettings.UtxoStore.SpendBatcherDurationMillis) * time.Millisecond
	s.spendBatcher = batcher.New(spendBatchSize, spendBatchDuration, s.sendSpendBatch, !tSettings.BatcherDrainMode)

	lockedBatcherSize := tSettings.UtxoStore.LockedBatcherSize
	lockedBatchDuration := time.Duration(tSettings.UtxoStore.LockedBatcherDurationMillis) * time.Millisecond
	s.lockedBatcher = batcher.New(lockedBatcherSize, lockedBatchDuration, s.sendLockedBatch, !tSettings.BatcherDrainMode)

	if tSettings.BatcherDrainMode {
		s.getBatcher.SetDrainMode(true)
		s.spendBatcher.SetDrainMode(true)
		s.lockedBatcher.SetDrainMode(true)
		s.storeBatcher.SetDrainMode(true)
	}

	logger.Infof("[TeraSlab] store initialised with address: %s", addr)

	return s, nil
}

// Health checks the health status of the TeraSlab store.
func (s *Store) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	details := "TeraSlab store"

	if err := s.client.Health(ctx); err != nil {
		return http.StatusServiceUnavailable, details, err
	}

	return http.StatusOK, details, nil
}

// SetBlockHeight updates the current block height in the store.
func (s *Store) SetBlockHeight(blockHeight uint32) error {
	if blockHeight == 0 {
		return fmt.Errorf("block height cannot be zero")
	}
	s.blockHeight.Store(blockHeight)
	return nil
}

// GetBlockHeight returns the current block height from the store.
func (s *Store) GetBlockHeight() uint32 {
	return s.blockHeight.Load()
}

// SetMedianBlockTime updates the median block time in the store.
func (s *Store) SetMedianBlockTime(medianTime uint32) error {
	s.medianBlockTime.Store(medianTime)
	return nil
}

// GetMedianBlockTime returns the current median block time from the store.
func (s *Store) GetMedianBlockTime() uint32 {
	return s.medianBlockTime.Load()
}

// GetBlockState returns an atomic snapshot of both block height and median block time.
func (s *Store) GetBlockState() utxo.BlockState {
	return utxo.BlockState{
		Height:     s.blockHeight.Load(),
		MedianTime: s.medianBlockTime.Load(),
	}
}
