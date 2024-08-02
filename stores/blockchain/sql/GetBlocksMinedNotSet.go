package sql

import (
	"context"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/tracing"
)

func (s *SQL) GetBlocksMinedNotSet(ctx context.Context) ([]*model.Block, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetBlocksMinedNotSet")
	defer deferFn()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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
		,b.subtree_count
		,b.subtrees
		,b.height
		FROM blocks b
		WHERE mined_set = false
		ORDER BY height ASC
	`

	return s.getBlocksWithQuery(ctx, q)
}
