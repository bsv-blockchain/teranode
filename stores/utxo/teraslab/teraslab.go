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

	storeBatcher     batcherIfc[batchStoreItem]
	getBatcher       batcherIfc[batchGetItem]
	spendBatcher     batcherIfc[batchSpendItem]
	setLockedBatcher batcherIfc[batchSetLockedCall]
	decorateBatcher  batcherIfc[batchDecorateCall]

	// setLockedFn and getRecordFn are seams over the two client RPCs that the
	// coalescing SetLocked / BatchDecorate batchers issue. They default to the
	// real client methods in New(); tests substitute fakes to count wire calls
	// without a live server. All other ops call s.client directly.
	setLockedFn func(ctx context.Context, value bool, txids []teraslab.TxID) (*teraslab.BatchResult, error)
	getRecordFn func(ctx context.Context, fieldMask uint32, txids []teraslab.TxID) ([]teraslab.TxRecord, error)

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

	// An in-memory backend makes the crash-safety WAL non-durable: intents for an
	// in-flight ProcessConflicting are lost on restart, so a torn conflict
	// operation cannot be replayed. Warn loudly rather than silently degrade.
	if walURL.Scheme == "sqlitememory" {
		logger.Warnf("[TeraSlab] conflict WAL backend is in-memory (%s) — intents will NOT survive a restart, defeating crash-safety replay; set utxostore_teraslab_conflictWalStore to a file-backed sqlite:// or a postgres:// URL for durability", walURL.Redacted())
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

	// Default the RPC seams to the real client. Tests override these.
	s.setLockedFn = client.SetLockedBatch
	s.getRecordFn = client.GetRecordBatch

	// Initialize batchers. background (async batch callbacks) and drain mode
	// (flush immediately vs accumulate to size/timeout) are orthogonal go-batcher
	// knobs: wire background from the dedicated BatcherBackground setting (matching
	// aerospike.go and block assembly), and apply drain mode separately below.
	storeBatchSize := tSettings.UtxoStore.StoreBatcherSize
	storeBatchDuration := time.Duration(tSettings.UtxoStore.StoreBatcherDurationMillis) * time.Millisecond
	s.storeBatcher = batcher.New(storeBatchSize, storeBatchDuration, s.sendStoreBatch, tSettings.BatcherBackground)

	getBatchSize := tSettings.UtxoStore.GetBatcherSize
	getBatchDuration := time.Duration(tSettings.UtxoStore.GetBatcherDurationMillis) * time.Millisecond
	s.getBatcher = batcher.New(getBatchSize, getBatchDuration, s.sendGetBatch, tSettings.BatcherBackground)

	// Spend batcher: coalesces per-transaction spends into shared SpendBatch RPCs
	// (grouped by params in sendSpendBatch) so the server amortizes its per-RPC
	// redo fsync across many transactions — without this every validated tx
	// incurs its own fsync, which caps catchup throughput on fsync-bound storage.
	spendBatchSize := tSettings.UtxoStore.SpendBatcherSize
	spendBatchDuration := time.Duration(tSettings.UtxoStore.SpendBatcherDurationMillis) * time.Millisecond
	s.spendBatcher = batcher.New(spendBatchSize, spendBatchDuration, s.sendSpendBatch, tSettings.BatcherBackground)

	// SetLocked batcher: coalesce concurrent SetLocked() calls sharing the same
	// `value` bool into one wire SetLockedBatch RPC (grouped by value inside
	// sendSetLockedBatch). Reuses the get batcher window since SetLocked, like
	// Get, fans out across many concurrent single-hash callers.
	s.setLockedBatcher = batcher.New(getBatchSize, getBatchDuration, s.sendSetLockedBatch, tSettings.BatcherBackground)

	// BatchDecorate batcher: merge concurrent BatchDecorate() calls into one
	// GetRecordBatch RPC over the union of their txids/field masks.
	s.decorateBatcher = batcher.New(getBatchSize, getBatchDuration, s.sendDecorateBatch, tSettings.BatcherBackground)

	if tSettings.BatcherDrainMode {
		s.getBatcher.SetDrainMode(true)
		s.storeBatcher.SetDrainMode(true)
		s.spendBatcher.SetDrainMode(true)
		s.setLockedBatcher.SetDrainMode(true)
		s.decorateBatcher.SetDrainMode(true)
	}

	logger.Infof("[TeraSlab] store initialised with address: %s", addr)

	return s, nil
}

// SupportsOutpointOnlySpend reports whether this store honours the below-checkpoint
// outpoint-only spend fast path (spending from an input's outpoint alone, without
// decorated parent data). TeraSlab derives the UTXO hash from the decorated input
// and hard-errors when PreviousTxScript is absent (see Spend / util.UTXOHashFromInput),
// so it does not support the fast path — returns false, matching the Aerospike backend.
func (s *Store) SupportsOutpointOnlySpend() bool { return false }

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
