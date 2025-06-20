// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlocksMinedNotSet method, which retrieves blocks that
// have been stored in the blockchain database but have not yet had their mining status
// properly recorded. This functionality is important for mining operations and blockchain
// analytics, ensuring that all blocks are properly tagged with their mining origin.
// The implementation uses a straightforward SQL query to identify blocks with the mined_set
// flag set to false, which typically indicates blocks that were received from the network
// but whose mining status needs to be verified or updated. This method supports Teranode's
// mining infrastructure by ensuring accurate tracking of locally mined blocks versus
// those received from the network.
package sql

import (
	"context"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/util/tracing"
)

// GetBlocksMinedNotSet retrieves blocks whose mining status has not been properly recorded.
// This implements a specialized blockchain query method not directly defined in the Store interface.
//
// In blockchain systems like Teranode that support mining operations, it's important to
// accurately track which blocks were mined locally versus those received from the network.
// This method identifies blocks that have been stored in the blockchain database but have
// not yet had their mining status properly recorded (mined_set flag is false).
//
// This functionality serves several important purposes:
//   - Supports post-processing of blocks to update their mining status
//   - Enables accurate mining statistics and analytics
//   - Helps identify blocks that may need additional verification
//   - Facilitates mining pool integration by ensuring proper attribution of mined blocks
//
// The implementation uses a straightforward SQL query to retrieve blocks with mined_set=false,
// ordered by height to process them in chronological order. It leverages the shared
// getBlocksWithQuery helper method to handle the common block reconstruction logic used
// across multiple query methods in the package.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//
// Returns:
//   - []*model.Block: An array of complete block objects whose mining status needs to be set
//   - error: Any error encountered during retrieval, specifically:
//   - StorageError for database errors or processing failures
func (s *SQL) GetBlocksMinedNotSet(ctx context.Context) ([]*model.Block, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlocksMinedNotSet")
	defer deferFn()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	q := `
		SELECT
		 b.ID
	  ,b.version
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
