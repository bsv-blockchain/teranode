package sql

import (
	"context"
	"database/sql"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// GetBlockHeadersByParentLinks returns up to numberOfHeaders headers by walking parent_id links
// back from blockHashFrom, newest first.
//
// It exists beside GetBlockHeaders for one reason. That call takes a fast path selected by the
// on_main_chain flag whenever the start block is flagged, and the flag is repaired only across
// the last 200 blocks after a chain switch. A flag left wrong deeper than that makes the fast
// path return a chain that fails hash linkage, and a caller that proves the result by linkage
// then fails on every attempt. The pruner's stamp reads 288 to 575 blocks deep, and a fetch it
// cannot ever prove would stop a containment window from stamping, so from dropping, so the
// disk would grow without limit. This walk cannot be steered by the flag at all.
//
// It also takes no cache and fills none. GetBlockHeaders caches every answer under the start
// hash, and under the in-memory chain check that cache keeps entries for ten minutes and is not
// cleared when a block is stored. A caller that anchors on a different hash every time would
// only fill it with entries nobody reads twice.
//
// A short result is not an error: the walk stops at genesis, or at a hash the store does not
// hold, which returns nothing. The caller proves the length it needed. The row stream's own
// error IS checked, which the other header fetches do not do: a stream that fails part way
// would otherwise arrive as a short list with a nil error, and the caller would read it as a
// chain that ended early.
func (s *SQL) GetBlockHeadersByParentLinks(ctx context.Context, blockHashFrom *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlockHeadersByParentLinks",
		tracing.WithDebugLogMessage(s.logger, "[GetBlockHeadersByParentLinks][%s] called for %d headers", blockHashFrom.String(), numberOfHeaders),
	)
	defer deferFn()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q, args := parentLinkHeadersQuery(blockHashFrom, numberOfHeaders)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil
		}

		return nil, nil, errors.NewStorageError("failed to get headers by parent links", err)
	}

	defer rows.Close()

	h, m, err := s.processBlockHeadersRows(rows, numberOfHeaders, false)
	if err != nil {
		return nil, nil, err
	}

	if err := rows.Err(); err != nil {
		return nil, nil, errors.NewStorageError("header stream failed part way", err)
	}

	return h, m, nil
}
