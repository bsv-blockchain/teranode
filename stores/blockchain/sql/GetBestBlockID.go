package sql

import (
	"context"
	"database/sql"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// bestBlockIDResult is the cached value for getBestBlockID.
type bestBlockIDResult struct {
	id   uint32
	hash *chainhash.Hash
}

// getBestBlockID returns the database ID and hash of the best (most-work) valid block.
// This is a lightweight alternative to GetBestBlockHeader for code paths that only need
// the block's identity (chain-membership checks, fork detection, off-chain set rebuild)
// without the full BlockHeader or BlockHeaderMeta.
//
// The query mirrors GetBestBlockHeader's ordering (chain_work DESC, peer_id ASC, id ASC)
// to guarantee identical tie-breaking. Results are cached in responseCache and
// automatically invalidated by ResetResponseCache().
func (s *SQL) getBestBlockID(ctx context.Context) (uint32, *chainhash.Hash, error) {
	cacheID := chainhash.HashH([]byte("getBestBlockID"))
	cacheOp := s.responseCache.Begin(cacheID)

	if cached := cacheOp.Get(); cached != nil {
		if r, ok := cached.Value().(bestBlockIDResult); ok {
			return r.id, r.hash, nil
		}
	}

	q := `
		SELECT b.id, b.hash
		FROM blocks b
		WHERE invalid = false
		ORDER BY chain_work DESC, peer_id ASC, id ASC
		LIMIT 1
	`

	var (
		id        uint32
		hashBytes []byte
	)

	s.bestBlockIDQueries.Add(1)

	if err := s.db.QueryRowContext(ctx, q).Scan(&id, &hashBytes); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil, errors.NewBlockNotFoundError("no valid blocks found", err)
		}
		return 0, nil, errors.NewStorageError("getBestBlockID query failed", err)
	}

	hash, err := chainhash.NewHash(hashBytes)
	if err != nil {
		return 0, nil, errors.NewStorageError("getBestBlockID: invalid hash bytes", err)
	}

	cacheOp.Set(bestBlockIDResult{id: id, hash: hash}, s.cacheTTL)

	return id, hash, nil
}
