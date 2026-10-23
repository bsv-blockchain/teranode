package sql

import (
	"context"
	"database/sql"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// SetBlockMinedSet sets mined_set = true, but only while the block's invalid flag
// still equals expectInvalid, the value the caller's tagging run started from.
//
// A tagging run (BlockValidation.setTxMinedStatus) reads invalid once, tags or
// untags the block's transactions, then calls this. If InvalidateBlock or
// RevalidateBlock ran in between, they cleared mined_set so the block would be
// processed again; an unconditional write here would undo that and the block
// would never be revisited. In that case the row is left untouched and an
// ErrStateError is returned, so the caller can re-queue the block.
func (s *SQL) SetBlockMinedSet(ctx context.Context, blockHash *chainhash.Hash, expectInvalid bool) error {
	s.logger.Debugf("SetBlockMinedSet %s (expectInvalid=%t)", blockHash.String(), expectInvalid)

	// Invalidate response cache to ensure cached blocks reflect updated mined_set field
	defer s.ResetResponseCache()

	q := `
		UPDATE blocks
		SET mined_set = true
		WHERE hash = $1
		AND invalid = $2
	`

	res, err := s.db.ExecContext(ctx, q, blockHash.CloneBytes(), expectInvalid)
	if err != nil {
		return errors.NewStorageError("error updating block mined_set", err)
	}

	if rows, _ := res.RowsAffected(); rows > 0 {
		return nil
	}

	// Nothing updated: either the block is missing or its invalid flag moved.
	var invalid bool
	if err = s.db.QueryRowContext(ctx, `SELECT invalid FROM blocks WHERE hash = $1`, blockHash.CloneBytes()).Scan(&invalid); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return errors.NewStorageError("block %s mined_set was not updated", blockHash.String())
		}

		return errors.NewStorageError("error reading block %s invalid flag", blockHash.String(), err)
	}

	return errors.NewStateError("block %s mined_set not updated: invalid is %t, tagging run expected %t", blockHash.String(), invalid, expectInvalid)
}
