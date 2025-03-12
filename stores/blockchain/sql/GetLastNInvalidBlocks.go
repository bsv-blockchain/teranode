package sql

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/libsv/go-bt/v2/chainhash"
)

// GetLastNInvalidBlocks retrieves the last n blocks that were marked as invalid.
// These blocks can be on any fork in the blockchain.
func (s *SQL) GetLastNInvalidBlocks(ctx context.Context, n int64) ([]*model.BlockInfo, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetLastNInvalidBlocks")
	defer deferFn()

	// Create a unique cache ID for this query
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetLastNInvalidBlocks-%d", n)))

	// Check if we have a cached result
	cached := s.responseCache.Get(cacheID)
	if cached != nil && cached.Value() != nil {
		if cacheData, ok := cached.Value().([]*model.BlockInfo); ok && cacheData != nil {
			s.logger.Debugf("GetLastNInvalidBlocks cache hit")
			return cacheData, nil
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Query to get the last n invalid blocks, ordered by inserted_at descending (most recent first)
	q := `
		SELECT
		 b.version
		,b.block_time
		,b.n_bits
		,b.nonce
		,b.previous_hash
		,b.merkle_root
		,b.tx_count
		,b.size_in_bytes
		,b.coinbase_tx
		,b.height
		,b.inserted_at
		FROM blocks b
		WHERE invalid = true
		ORDER BY height DESC
		LIMIT $1
	`

	rows, err := s.db.QueryContext(ctx, q, n)
	if err != nil {
		return nil, errors.NewStorageError("failed to get invalid blocks", err)
	}
	defer rows.Close()

	// Process the query results using the common helper function
	blockInfos, err := s.processBlockRows(rows)
	if err != nil {
		return nil, err
	}

	// Cache the result
	s.responseCache.Set(cacheID, blockInfos, s.cacheTTL)

	return blockInfos, nil
}
