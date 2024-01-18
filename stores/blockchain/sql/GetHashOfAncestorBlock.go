package sql

import (
	"context"
	"fmt"
	"time"

	"database/sql"
	"errors"

	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) GetHashOfAncestorBlock(ctx context.Context, hash *chainhash.Hash, depth int) (*chainhash.Hash, error) {
	start, stat, ctx := util.StartStatFromContext(ctx, "GetHashOfAncestorBlock")
	defer func() {
		stat.AddTime(start)
	}()

	ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	var pastHash []byte

	q := `WITH RECURSIVE ChainBlocks AS (
		SELECT 
			id, 
			hash, 
			parent_id, 
			1 AS depth
		FROM 
			blocks
		WHERE 
			hash = $1  -- $1 is the placeholder for the starting block hash (h1)
	
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
			cb.depth < $2 AND cb.depth < (SELECT COUNT(*) FROM blocks)  -- Ensure depth doesn't exceed the number of blocks
	)
	SELECT 
	hash
	FROM 
		ChainBlocks
	WHERE 
		depth = $2
	ORDER BY 
		depth DESC
	LIMIT 1`

	if err := s.db.QueryRowContext(ctx, q, hash[:], depth).Scan(
		&pastHash,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("can't get hash %d before block %s: %w", depth, hash.String(), err)
		}
		return nil, err
	}
	ph, err := chainhash.NewHash(pastHash)
	if err != nil {
		return nil, fmt.Errorf("failed to convert pastHash: %w", err)
	}
	return ph, nil
}
