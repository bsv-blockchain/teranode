// Package teraslab provides a TeraSlab-based implementation of the UTXO store interface.
// It uses the TeraSlab binary wire protocol for high-performance UTXO operations.
package teraslab

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/conflictwal"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/usql"
	teraslab "github.com/icellan/teraslab/client/go"
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

	storeBatcher batcherIfc[batchStoreItem]
	getBatcher   batcherIfc[batchGetItem]
	spendBatcher batcherIfc[batchSpendItem]

	// walDB / walEngine back the conflict-resolution WAL (#861). The TeraSlab
	// server cannot hold arbitrary intent records, so the WAL reuses the shared
	// conflictwal package over a dedicated SQL connection (Postgres in prod,
	// SQLite for dev — see New).
	walDB     *usql.DB
	walEngine string
}

// New creates a new TeraSlab-based UTXO store.
// The URL format is: teraslab://host:port?pool_size=16&cluster=host2:port2,host3:port3&cluster_secret=...
func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, teraslabURL *url.URL) (*Store, error) {
	addr := teraslabURL.Host

	cfg := teraslab.ClientConfig{
		Addr: addr,
	}

	// Parse pool_size from query parameters
	if poolSizeStr := teraslabURL.Query().Get("pool_size"); poolSizeStr != "" {
		poolSize, err := strconv.Atoi(poolSizeStr)
		if err != nil {
			// Typed so callers can classify via errors.Is, matching the other backends.
			return nil, errors.NewInvalidArgumentError("invalid pool_size %q", poolSizeStr, err)
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

	// Parse the optional cluster_secret used to HMAC-sign inter-node opcodes
	// (OP_GET_PARTITION_MAP) when the cluster runs with a shared secret. The
	// secret travels in the connection URL's query, never in logs (only the
	// host is logged below).
	if secret := teraslabURL.Query().Get("cluster_secret"); secret != "" {
		cfg.ClusterSecret = []byte(secret)
	}

	client, err := teraslab.New(ctx, cfg)
	if err != nil {
		// Connecting to the external TeraSlab server failed — a storage-class
		// fault. Typed so callers can classify it and the cause is preserved.
		return nil, errors.NewStorageError("teraslab client init", err)
	}

	utxoBatchSize := tSettings.UtxoStore.UtxoBatchSize
	if utxoBatchSize < 1 {
		utxoBatchSize = 20000
	}

	// Open the conflict-resolution WAL backing store (#861). The TeraSlab server
	// cannot hold arbitrary intent records, so the WAL reuses the standard SQL
	// store mechanism: set utxostore_teraslab_conflictWalStore to a postgres://
	// URL in production (durable, shared, scalable); defaults to a local SQLite
	// file under DataFolder for development.
	walURL := tSettings.UtxoStore.TeraSlabConflictWALStore
	if walURL == nil || walURL.String() == "" {
		walURL, err = url.Parse("sqlite:///teraslab_conflict_wal")
		if err != nil {
			return nil, errors.NewStorageError("teraslab: parse default conflict WAL URL", err)
		}
	}

	walDB, err := util.InitSQLDB(logger, walURL, tSettings, tSettings.UtxoStore.PostgresPool)
	if err != nil {
		return nil, errors.NewStorageError("teraslab: open conflict WAL store", err)
	}

	if err := conflictwal.CreateTable(walDB, walURL.Scheme); err != nil {
		_ = walDB.Close()
		return nil, err
	}

	s := &Store{
		client:        client,
		logger:        logger,
		settings:      tSettings,
		utxoBatchSize: utxoBatchSize,
		walDB:         walDB,
		walEngine:     walURL.Scheme,
	}

	// Initialize batchers
	storeBatchSize := tSettings.UtxoStore.StoreBatcherSize
	storeBatchDuration := time.Duration(tSettings.UtxoStore.StoreBatcherDurationMillis) * time.Millisecond
	s.storeBatcher = batcher.New(storeBatchSize, storeBatchDuration, s.sendStoreBatch, !tSettings.BatcherDrainMode)

	getBatchSize := tSettings.UtxoStore.GetBatcherSize
	getBatchDuration := time.Duration(tSettings.UtxoStore.GetBatcherDurationMillis) * time.Millisecond
	s.getBatcher = batcher.New(getBatchSize, getBatchDuration, s.sendGetBatch, !tSettings.BatcherDrainMode)

	// Spend batcher: coalesces per-transaction spends into shared SpendBatch RPCs
	// (grouped by params in sendSpendBatch) so the server amortizes its per-RPC
	// redo fsync across many transactions — without this every validated tx
	// incurs its own fsync, which caps catchup throughput on fsync-bound storage.
	spendBatchSize := tSettings.UtxoStore.SpendBatcherSize
	spendBatchDuration := time.Duration(tSettings.UtxoStore.SpendBatcherDurationMillis) * time.Millisecond
	s.spendBatcher = batcher.New(spendBatchSize, spendBatchDuration, s.sendSpendBatch, !tSettings.BatcherDrainMode)

	if tSettings.BatcherDrainMode {
		s.getBatcher.SetDrainMode(true)
		s.storeBatcher.SetDrainMode(true)
		s.spendBatcher.SetDrainMode(true)
	}

	logger.Infof("[TeraSlab] store initialised with address: %s", addr)

	return s, nil
}

// Health checks the health status of the TeraSlab store.
//
// It always performs a shallow server health check (OpHealth). When
// checkLiveness is true it additionally issues a Ping (OpPing) round trip to
// confirm the connection is live end-to-end; any error from either check is
// returned with a StatusServiceUnavailable code.
func (s *Store) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	details := "TeraSlab store"

	if err := s.client.Health(ctx); err != nil {
		return http.StatusServiceUnavailable, details, err
	}

	if checkLiveness {
		if _, err := s.client.Ping(ctx); err != nil {
			return http.StatusServiceUnavailable, details, err
		}
	}

	return http.StatusOK, details, nil
}

// SetBlockHeight updates the current block height in the store.
func (s *Store) SetBlockHeight(blockHeight uint32) error {
	if blockHeight == 0 {
		// Use the typed invalid-argument error so callers can classify it via
		// errors.Is, matching the Aerospike/SQL backends.
		return errors.NewInvalidArgumentError("block height cannot be zero")
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
