package sql

import (
	"context"
	"database/sql"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// SetBlockMinedSet sets mined_set = true. With a non-nil expectedGeneration the write
// only lands while the block's mined_set_gen still equals it.
//
// A tagging run (BlockValidation.setTxMinedStatus) reads mined_set_gen once at the
// start, tags or untags the block's transactions, then calls this. Every write that
// clears mined_set (ClearBlockMinedSet, InvalidateBlock, RevalidateBlock) also bumps
// mined_set_gen. If one of them ran in between, an unconditional write here would undo
// the reset and the block would never be revisited. In that case the row is left
// untouched and an ErrStateError is returned, so the caller can re-queue the block.
// A counter, not the invalid flag, is compared because ClearBlockMinedSet does not
// touch invalid, and an invalidate then revalidate inside one run would put invalid
// back to its starting value.
//
// A nil expectedGeneration is the unconditional write, kept for callers that predate
// the generation (an older blockvalidation talking to this blockchain service).
func (s *SQL) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash, expectedGeneration *uint64) error {
	if expectedGeneration == nil {
		s.logger.Debugf("SetBlockMinedSet %s (unconditional)", blockHash.String())
	} else {
		s.logger.Debugf("SetBlockMinedSet %s (expectedGeneration=%d)", blockHash.String(), *expectedGeneration)
	}

	// Invalidate response cache to ensure cached blocks reflect updated mined_set field
	defer s.ResetResponseCache()

	var (
		res sql.Result
		err error
	)

	if expectedGeneration == nil {
		res, err = s.db.ExecContext(ctx, `
			UPDATE blocks
			SET mined_set = true
			WHERE hash = $1
		`, blockHash.CloneBytes())
	} else {
		res, err = s.db.ExecContext(ctx, `
			UPDATE blocks
			SET mined_set = true
			WHERE hash = $1
			AND mined_set_gen = $2
		`, blockHash.CloneBytes(), *expectedGeneration)
	}

	if err != nil {
		return errors.NewStorageError("error updating block mined_set", err)
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return errors.NewStorageError("error reading rows affected for block %s mined_set", blockHash.String(), err)
	}

	if rows > 0 {
		return nil
	}

	if expectedGeneration == nil {
		return errors.NewStorageError("block %s mined_set was not updated", blockHash.String())
	}

	// Nothing updated: either the block is missing or its generation moved.
	var generation uint64
	if err = s.db.QueryRowContext(ctx, `SELECT mined_set_gen FROM blocks WHERE hash = $1`, blockHash.CloneBytes()).Scan(&generation); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errors.NewStorageError("block %s mined_set was not updated", blockHash.String())
		}

		return errors.NewStorageError("error reading block %s mined_set_gen", blockHash.String(), err)
	}

	if generation == *expectedGeneration {
		// The generation did not move, so the guard was not what stopped the write. Report
		// a storage fault: the caller charges retries on it, while a state error would
		// re-queue for free and could loop forever.
		return errors.NewStorageError("block %s mined_set was not updated although mined_set_gen is unchanged at %d", blockHash.String(), generation)
	}

	return errors.NewStateError("block %s mined_set not updated: mined_set_gen is %d, tagging run started at %d", blockHash.String(), generation, *expectedGeneration)
}
