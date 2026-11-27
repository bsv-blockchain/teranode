package packedsql

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var _ utxo.Store = (*Store)(nil)

type Store struct {
	logger        ulogger.Logger
	settings      *settings.Settings
	pool          *pgxpool.Pool
	pageSize      uint32
	blockState    atomic.Uint64
	createBatcher *batcher.Batcher[createItem]
	spendChans    []chan *spendWork
	spendWG       sync.WaitGroup
	closeOnce     sync.Once

	// bgCtx bounds all work the pipeline performs on behalf of callers whose own context
	// has already been satisfied (group-committed batches). It is cancelled once the
	// workers have drained, so a wedged connection cannot outlive the store.
	bgCtx    context.Context
	bgCancel context.CancelFunc

	// closeMu guards sends into the pipeline against Close closing the channels underneath
	// them. Senders hold it for read; Close takes it for write before closing anything.
	closeMu sync.RWMutex
	closed  bool
}

func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, storeURL *url.URL) (*Store, error) {
	if storeURL == nil {
		return nil, errors.NewInvalidArgumentError("packedsql: store URL is required")
	}

	pgURL := *storeURL
	pgURL.Scheme = "postgres"

	config, err := pgxpool.ParseConfig(pgURL.String())
	if err != nil {
		return nil, errors.NewConfigurationError("packedsql: invalid store URL", err)
	}

	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheDescribe

	poolSettings := tSettings.UtxoStore.PostgresPool
	if poolSettings == nil {
		poolSettings = &tSettings.Postgres
	}

	if poolSettings.MaxOpenConns > 0 {
		config.MaxConns = int32(poolSettings.MaxOpenConns) //nolint:gosec
	}

	if poolSettings.MaxIdleConns > 0 {
		config.MinConns = int32(poolSettings.MaxIdleConns) //nolint:gosec
	}

	if poolSettings.ConnMaxLifetime > 0 {
		config.MaxConnLifetime = poolSettings.ConnMaxLifetime
	}

	if poolSettings.ConnMaxIdleTime > 0 {
		config.MaxConnIdleTime = poolSettings.ConnMaxIdleTime
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, errors.NewStorageError("packedsql: failed to create connection pool", err)
	}

	if err = createSchema(ctx, pool, tSettings.UtxoStore.PackedSQLPartitions, tSettings.UtxoStore.PackedSQLPageSize); err != nil {
		pool.Close()
		return nil, err
	}

	bgCtx, bgCancel := context.WithCancel(context.WithoutCancel(ctx))

	s := &Store{
		logger:   logger,
		settings: tSettings,
		pool:     pool,
		pageSize: uint32(tSettings.UtxoStore.PackedSQLPageSize), //nolint:gosec
		bgCtx:    bgCtx,
		bgCancel: bgCancel,
	}

	s.startPipeline()

	return s, nil
}

func (s *Store) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	details := "packedsql store on PostgreSQL"

	var num int
	if err := s.pool.QueryRow(ctx, "SELECT 1").Scan(&num); err != nil {
		return http.StatusServiceUnavailable, details, err
	}

	return http.StatusOK, details, nil
}

func (s *Store) Close(ctx context.Context) error {
	done := make(chan struct{})

	go func() {
		defer close(done)

		s.closeOnce.Do(func() {
			// Block until every in-flight send has landed, then fence off new ones so
			// closing the channels below cannot race a send.
			s.closeMu.Lock()
			s.closed = true
			s.closeMu.Unlock()

			for _, ch := range s.spendChans {
				close(ch)
			}

			s.spendWG.Wait()

			if s.createBatcher != nil {
				s.createBatcher.Close()
			}

			s.bgCancel()
			s.pool.Close()
		})
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Store) SupportsOutpointOnlySpend() bool {
	return true
}

func packBlockState(height, medianTime uint32) uint64 {
	return uint64(height)<<32 | uint64(medianTime)
}

func (s *Store) SetBlockHeight(height uint32) error {
	if height == 0 {
		return errors.NewInvalidArgumentError("packedsql: block height must be non-zero")
	}

	for {
		old := s.blockState.Load()
		if s.blockState.CompareAndSwap(old, packBlockState(height, uint32(old))) { //nolint:gosec
			return nil
		}
	}
}

func (s *Store) GetBlockHeight() uint32 {
	return uint32(s.blockState.Load() >> 32) //nolint:gosec
}

func (s *Store) SetMedianBlockTime(medianTime uint32) error {
	for {
		old := s.blockState.Load()
		if s.blockState.CompareAndSwap(old, old&0xFFFFFFFF00000000|uint64(medianTime)) {
			return nil
		}
	}
}

func (s *Store) GetMedianBlockTime() uint32 {
	return uint32(s.blockState.Load()) //nolint:gosec
}

func (s *Store) SetBlockState(height, medianTime uint32) error {
	if height == 0 {
		return errors.NewInvalidArgumentError("packedsql: block height must be non-zero")
	}

	s.blockState.Store(packBlockState(height, medianTime))

	return nil
}

func (s *Store) GetBlockState() utxo.BlockState {
	v := s.blockState.Load()

	return utxo.BlockState{
		Height:     uint32(v >> 32), //nolint:gosec
		MedianTime: uint32(v),       //nolint:gosec
	}
}
