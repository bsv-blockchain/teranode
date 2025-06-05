package sql

import (
	"context"
	"database/sql"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) GetHashOfAncestorBlock(ctx context.Context, hash *chainhash.Hash, depth int) (*chainhash.Hash, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetHashOfAncestorBlock")
	defer deferFn()

	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	var pastHash []byte

	q := `WITH RECURSIVE ChainBlocks AS (
		SELECT
			id,
			hash,
			parent_id,
			0 AS depth  -- Start at depth 0 for the input block
		FROM
			blocks
		WHERE
			hash = $1

		UNION ALL

		SELECT
			b.id,
			b.hash,
			b.parent_id,
			cb.depth + 1
		FROM
			blocks b
		INNER JOIN
			ChainBlocks cb ON b.id = cb.parent_id
		WHERE
			cb.depth < $2 AND cb.depth < (SELECT COUNT(*) FROM blocks)
	)
	SELECT
	hash
	FROM
		ChainBlocks
	WHERE
		depth = $2  -- This will now correctly get the block $2 blocks back
	ORDER BY
		depth DESC
	LIMIT 1`

	if err := s.db.QueryRowContext(ctx, q, hash[:], depth).Scan(
		&pastHash,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.NewStorageError("can't get hash %d before block %s", depth, hash.String(), errors.ErrNotFound)
		}

		return nil, errors.NewStorageError("can't get hash %d before block %s", depth, hash.String(), err)
	}

	ph, err := chainhash.NewHash(pastHash)
	if err != nil {
		return nil, errors.NewProcessingError("failed to convert pastHash", err)
	}

	return ph, nil
}
