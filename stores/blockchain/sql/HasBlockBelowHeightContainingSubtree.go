package sql

import (
	"context"
	"database/sql"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// HasBlockBelowHeightContainingSubtree returns true if any block with
// height <= maxHeight has the given subtree hash embedded in its serialised
// subtrees blob. Unlike FindBlocksContainingSubtree, this helper does not
// filter by validity or main-chain membership: during a rewind we want to
// preserve a subtree blob as long as ANY surviving block references it, even
// if that block is on a losing fork or currently marked invalid.
func (s *SQL) HasBlockBelowHeightContainingSubtree(ctx context.Context, subtreeHash *chainhash.Hash, maxHeight uint32) (bool, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:HasBlockBelowHeightContainingSubtree")
	defer deferFn()

	if subtreeHash == nil {
		return false, errors.NewInvalidArgumentError("subtree hash cannot be nil")
	}

	subtreeSearchClause := "instr(subtrees, $1) > 0"
	if s.engine == util.Postgres {
		subtreeSearchClause = "position($1 in subtrees) > 0"
	}

	q := `
		SELECT 1
		FROM blocks
		WHERE height <= $2
		  AND ` + subtreeSearchClause + `
		LIMIT 1
	`

	var found int
	err := s.db.QueryRowContext(ctx, q, subtreeHash.CloneBytes(), maxHeight).Scan(&found)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, errors.NewStorageError("error searching blocks for subtree", err)
	}

	return true, nil
}
