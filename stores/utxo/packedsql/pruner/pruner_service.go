package pruner

import (
	"context"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/pruner"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/jackc/pgx/v5/pgxpool"
)

const deleteChunkSize = 10_000

type Service struct {
	logger           ulogger.Logger
	pool             *pgxpool.Pool
	safetyWindow     uint32
	defensiveEnabled bool
	observers        []pruner.Observer
	observersMu      sync.Mutex
}

type Options struct {
	Logger ulogger.Logger
	Pool   *pgxpool.Pool
}

func NewService(tSettings *settings.Settings, opts Options) (*Service, error) {
	if opts.Logger == nil {
		return nil, errors.NewProcessingError("logger is required")
	}

	if tSettings == nil {
		return nil, errors.NewProcessingError("settings is required")
	}

	if opts.Pool == nil {
		return nil, errors.NewProcessingError("pool is required")
	}

	return &Service{
		logger:           opts.Logger,
		pool:             opts.Pool,
		safetyWindow:     tSettings.GlobalBlockHeightRetention,
		defensiveEnabled: tSettings.Pruner.UTXODefensiveEnabled,
	}, nil
}

func (s *Service) Start(ctx context.Context) {
}

func (s *Service) AddObserver(observer pruner.Observer) {
	s.observersMu.Lock()
	defer s.observersMu.Unlock()

	s.observers = append(s.observers, observer)
}

func (s *Service) Prune(ctx context.Context, height uint32, blockHashStr string) (int64, error) {
	startTime := time.Now()

	deleted, err := s.deleteTombstoned(ctx, height)
	if err != nil {
		s.logger.Errorf("[packedsql pruner][%s:%d] cleanup failed: %v", blockHashStr, height, err)
		return 0, err
	}

	if deleted > 0 {
		s.logger.Infof("[packedsql pruner][%s:%d] deleted %d transactions in %s", blockHashStr, height, deleted, time.Since(startTime))
	}

	s.observersMu.Lock()
	observers := make([]pruner.Observer, len(s.observers))
	copy(observers, s.observers)
	s.observersMu.Unlock()

	for _, o := range observers {
		o.OnPruneComplete(height, deleted)
	}

	return deleted, nil
}

const victimsSQL = `SELECT hash FROM packed_txs
WHERE delete_at_height IS NOT NULL AND delete_at_height <= $1
LIMIT $2`

const defensiveVictimsSQL = `SELECT t.hash FROM packed_txs t
WHERE t.delete_at_height IS NOT NULL AND t.delete_at_height <= $1
AND NOT EXISTS (
  SELECT 1
  FROM generate_series(0, t.page0_count - 1) AS g(slot)
  CROSS JOIN LATERAL (SELECT substring(t.spends FROM g.slot * 36 + 1 FOR 32) AS spender) sp
  WHERE octet_length(sp.spender) = 32 AND sp.spender <> '\x0000000000000000000000000000000000000000000000000000000000000000'::bytea
    AND NOT EXISTS (
      SELECT 1 FROM packed_txs child
      WHERE child.hash = sp.spender
        AND child.unmined_since IS NULL
        AND EXISTS (
          SELECT 1 FROM generate_series(0, octet_length(child.block_refs) / 12 - 1) AS b(i)
          WHERE (get_byte(child.block_refs, b.i * 12 + 4)
               + get_byte(child.block_refs, b.i * 12 + 5) * 256
               + get_byte(child.block_refs, b.i * 12 + 6) * 65536
               + get_byte(child.block_refs, b.i * 12 + 7) * 16777216) <= $3
        )
    )
)
LIMIT $2`

func (s *Service) deleteTombstoned(ctx context.Context, blockHeight uint32) (int64, error) {
	var total int64

	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}

		var (
			victims [][]byte
			err     error
		)

		if s.defensiveEnabled {
			stableHorizon := int64(blockHeight) - int64(s.safetyWindow)
			victims, err = s.selectVictims(ctx, defensiveVictimsSQL, int64(blockHeight), stableHorizon)
		} else {
			victims, err = s.selectVictims(ctx, victimsSQL, int64(blockHeight), 0)
		}

		if err != nil {
			return total, err
		}

		if len(victims) == 0 {
			return total, nil
		}

		deleted, err := s.deleteVictims(ctx, victims)
		if err != nil {
			return total, err
		}

		total += deleted

		if len(victims) < deleteChunkSize {
			return total, nil
		}
	}
}

func (s *Service) selectVictims(ctx context.Context, query string, height, extra int64) ([][]byte, error) {
	args := []any{height, deleteChunkSize}
	if query == defensiveVictimsSQL {
		args = append(args, extra)
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, errors.NewStorageError("packedsql pruner: victim query failed", err)
	}

	defer rows.Close()

	var victims [][]byte

	for rows.Next() {
		var h []byte
		if err = rows.Scan(&h); err != nil {
			return nil, errors.NewStorageError("packedsql pruner: victim scan failed", err)
		}

		victims = append(victims, h)
	}

	return victims, rows.Err()
}

func (s *Service) deleteVictims(ctx context.Context, victims [][]byte) (int64, error) {
	dbTx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, errors.NewStorageError("packedsql pruner: failed to begin delete transaction", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	for _, stmt := range []string{
		`DELETE FROM packed_tx_pages WHERE hash = ANY($1::bytea[])`,
		`DELETE FROM utxo_overrides WHERE hash = ANY($1::bytea[])`,
		`DELETE FROM conflicting_children WHERE hash = ANY($1::bytea[]) OR child_hash = ANY($1::bytea[])`,
	} {
		if _, err = dbTx.Exec(ctx, stmt, victims); err != nil {
			return 0, errors.NewStorageError("packedsql pruner: side-table delete failed", err)
		}
	}

	ct, err := dbTx.Exec(ctx, `DELETE FROM packed_txs WHERE hash = ANY($1::bytea[])`, victims)
	if err != nil {
		return 0, errors.NewStorageError("packedsql pruner: master delete failed", err)
	}

	if err = dbTx.Commit(ctx); err != nil {
		return 0, errors.NewStorageError("packedsql pruner: failed to commit delete transaction", err)
	}

	return ct.RowsAffected(), nil
}
