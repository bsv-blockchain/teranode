package sql

import (
	"context"
	"database/sql"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/jellydator/ttlcache/v3"
)

// AssignBlockID returns a stable block ID for the given block hash. Unlike
// GetNextBlockID (which always burns a fresh nextval), repeated calls for the
// same hash return the SAME id, and concurrent callers converge on one id. This
// is the single authority both ingestion paths use so a block's UTXO mined-info
// and its committed blocks row can never reference different ids.
//
// Resolution order:
//  1. If the block is already committed, return its authoritative id.
//  2. If an id is already reserved for this hash, return it.
//  3. Otherwise reserve a fresh nextval id and remember it until commit.
func (s *SQL) AssignBlockID(ctx context.Context, blockHash *chainhash.Hash) (uint64, error) {
	if id, ok, err := s.blockIDByHash(ctx, blockHash); err != nil {
		return 0, err
	} else if ok {
		return id, nil
	}

	// The mutex is deliberately held across the cache lookup, the committed
	// re-check below (a DB query) and the nextval, so that concurrent callers for
	// the same hash serialize and exactly one allocates. Re-ordering the DB call
	// out of the lock would reopen the race this method exists to close. The lock
	// is only contended when callers race on the same block — the intended case —
	// and each holder releases after one SELECT.
	s.blockIDReservationMu.Lock()
	defer s.blockIDReservationMu.Unlock()

	if item := s.blockIDReservations.Get(*blockHash); item != nil {
		return item.Value(), nil
	}

	// Re-check committed under the lock: a concurrent StoreBlock could have
	// committed this hash between the unlocked check above and acquiring the lock.
	if id, ok, err := s.blockIDByHash(ctx, blockHash); err != nil {
		return 0, err
	} else if ok {
		return id, nil
	}

	id, err := s.GetNextBlockID(ctx)
	if err != nil {
		return 0, err
	}

	// ttlcache.DefaultTTL applies the cache-wide default set in New (blockIDReservationTTL).
	s.blockIDReservations.Set(*blockHash, id, ttlcache.DefaultTTL)

	return id, nil
}

// blockIDByHash returns the committed id for a block hash, or ok=false if no
// such row exists yet.
func (s *SQL) blockIDByHash(ctx context.Context, blockHash *chainhash.Hash) (uint64, bool, error) {
	var id uint64
	err := s.db.QueryRowContext(ctx, `SELECT id FROM blocks WHERE hash = $1`, blockHash[:]).Scan(&id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, errors.NewStorageError("failed to look up block id by hash", err)
	}
	return id, true, nil
}
