package sql

import (
	"context"
	"database/sql"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/libsv/go-bt/v2/chainhash"
)

func (s *SQL) GetBlockIsMined(ctx context.Context, blockHash *chainhash.Hash) (bool, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetBlockIsMined")
	defer deferFn()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		SELECT
		  b.mined_set
		FROM blocks b
		WHERE hash = $1
	`

	var isMined bool

	err := s.db.QueryRowContext(ctx, q, blockHash[:]).Scan(&isMined)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, errors.NewBlockNotFoundError("[GetBlockIsMined][%s] block not found", blockHash.String())
		}

		return false, err
	}

	return isMined, nil
}
