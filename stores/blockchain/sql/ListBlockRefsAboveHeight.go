package sql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// ListBlockRefsAboveHeight returns every block with height > minHeight across
// all branches, ordered by (height DESC, id DESC) so callers can safely
// delete in FK-respecting order.
func (s *SQL) ListBlockRefsAboveHeight(ctx context.Context, minHeight uint32) ([]model.BlockRef, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:ListBlockRefsAboveHeight")
	defer deferFn()

	q := `SELECT id, hash, height FROM blocks WHERE height > $1 ORDER BY height DESC, id DESC`

	rows, err := s.db.QueryContext(ctx, q, minHeight)
	if err != nil {
		return nil, errors.NewStorageError("failed to list blocks above height", err)
	}
	defer rows.Close()

	out := make([]model.BlockRef, 0, 64)
	for rows.Next() {
		var (
			id     uint32
			hashBs []byte
			height uint32
		)
		if err = rows.Scan(&id, &hashBs, &height); err != nil {
			return nil, errors.NewStorageError("failed to scan block row", err)
		}
		h, err := chainhash.NewHash(hashBs)
		if err != nil {
			return nil, errors.NewProcessingError("failed to parse block hash", err)
		}
		out = append(out, model.BlockRef{ID: id, Hash: h, Height: height})
	}
	if err = rows.Err(); err != nil {
		return nil, errors.NewStorageError("error reading block rows", err)
	}

	return out, nil
}
