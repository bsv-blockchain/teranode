// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetBlockHeaders method, which retrieves a sequence of consecutive
// block headers starting from a specified block hash. This functionality is essential for
// blockchain synchronization, where nodes need to efficiently retrieve chains of headers
// to validate and update their local blockchain state.
//
// The implementation uses a hybrid query strategy:
//
//  1. An in-memory response/chain-walk cache to short-circuit repeated requests.
//  2. A fast path that filters by the on_main_chain partial index and a height
//     range derived from the start block — used whenever the start hash is on
//     the main chain and no rebuild is in flight. This replaces an O(N)
//     recursive parent_id walk with a single backward index scan.
//  3. A recursive Common Table Expression (CTE) fallback that walks
//     parent_id pointers — used for fork tips, unknown hashes, and while a
//     main-chain rebuild is in flight, so the CTE remains authoritative for
//     reorg / fork scenarios.
//
// In Teranode's high-throughput architecture, efficient header retrieval is critical for
// maintaining synchronization with the network, especially during initial block download
// or when recovering from network partitions. The method is designed to work efficiently
// with both PostgreSQL and SQLite database backends.
package sql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/model/time"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// GetBlockHeaders retrieves a sequence of consecutive block headers starting from a specified block hash.
// This implements the blockchain.Store.GetBlockHeaders interface method.
//
// This method is a cornerstone of blockchain synchronization, enabling nodes to efficiently
// retrieve chains of headers to validate and update their local blockchain state. In Bitcoin's
// headers-first synchronization model, nodes first download and validate headers before
// requesting the corresponding block data, making this method critical for maintaining
// network consensus.
//
// The implementation follows a tiered approach to optimize performance:
//  1. First checks the blocks cache for the requested headers sequence.
//  2. If not found in cache, takes the on_main_chain fast path when the
//     start hash is on the main chain and no rebuild is in flight: a single
//     backward index scan over (height) where on_main_chain = true,
//     restricted to the height range derived from the start block.
//  3. Otherwise (fork tips, unknown hashes, mid-rebuild), falls back to a
//     recursive CTE that walks parent_id pointers from the start block
//     backwards.
//  4. For each block, constructs both a BlockHeader object containing the
//     core consensus fields and a BlockHeaderMeta object containing
//     additional metadata.
//
// The SQL implementation uses database-specific optimizations for both PostgreSQL and
// SQLite to ensure efficient execution of both the fast path and the CTE
// fallback. The method also handles special cases such as chain
// reorganizations and invalid blocks, ensuring that only valid headers are
// returned.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - blockHashFrom: The hash of the starting block for header retrieval
//   - numberOfHeaders: Maximum number of consecutive headers to retrieve
//
// Returns:
//   - []*model.BlockHeader: Slice of consecutive block headers starting from the specified block
//   - []*model.BlockHeaderMeta: Slice of metadata for the corresponding block headers
//   - error: Any error encountered during retrieval, specifically:
//   - StorageError for database access or query execution errors
//   - ProcessingError for errors during header reconstruction
//   - nil if the operation was successful (even if fewer headers than requested were found)
func (s *SQL) GetBlockHeaders(ctx context.Context, blockHashFrom *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:GetBlockHeaders",
		tracing.WithDebugLogMessage(s.logger, "[GetBlockHeaders][%s] called for %d headers", blockHashFrom.String(), numberOfHeaders),
	)
	defer deferFn()

	// Use chain walk cache when in-memory mode is on (survives StoreBlock wipes),
	// otherwise fall back to response cache (original behavior).
	cache := s.responseCache
	cacheTTL := s.cacheTTL
	if s.useInMemoryChainCheck {
		cache = s.chainWalkCache
		cacheTTL = chainWalkCacheTTL
	}

	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetBlockHeaders-%s-%d", blockHashFrom.String(), numberOfHeaders)))
	cacheOp := cache.Begin(cacheID)

	cached := cacheOp.Get()
	if cached != nil {
		if result, ok := cached.Value().([2]interface{}); ok {
			if h, ok := result[0].([]*model.BlockHeader); ok {
				if m, ok := result[1].([]*model.BlockHeaderMeta); ok {
					return h, m, nil
				}
			}
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Try the on_main_chain fast path when the start hash is itself on the main
	// chain and no rebuild is in flight. The fast path replaces an O(N) recursive
	// parent_id walk with a single backward index scan over idx_on_main_chain_height
	// — measured ~3-6× faster on small datasets and 10-20× on production-sized DBs.
	// Fork tips, unknown hashes, or DB errors fall back to the recursive CTE so the
	// CTE remains the authoritative path. Same TOCTOU caveats apply as in
	// GetLatestBlockHeaderFromBlockLocator: the guard check and main query are
	// non-atomic, but the store's single-writer model bounds staleness to one call.
	q, args := s.buildGetBlockHeadersQuery(ctx, blockHashFrom, numberOfHeaders)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []*model.BlockHeader{}, []*model.BlockHeaderMeta{}, nil
		}

		return nil, nil, errors.NewStorageError("failed to get headers", err)
	}

	defer rows.Close()

	h, m, err := s.processBlockHeadersRows(rows, numberOfHeaders, false)
	if err != nil {
		return nil, nil, err
	}

	cacheOp.Set([2]interface{}{h, m}, cacheTTL)

	return h, m, nil
}

// buildGetBlockHeadersQuery returns the SQL query and args for GetBlockHeaders.
// The fast path uses the on_main_chain partial index when the start hash is on
// the main chain. Otherwise the recursive CTE walks parent_id pointers and is
// authoritative for fork tips and rebuilds.
func (s *SQL) buildGetBlockHeadersQuery(ctx context.Context, blockHashFrom *chainhash.Hash, numberOfHeaders uint64) (string, []interface{}) {
	const blockColumns = `
			 b.version
			,b.block_time
			,b.nonce
			,b.previous_hash
			,b.merkle_root
			,b.n_bits
			,b.id
			,b.height
			,b.tx_count
			,b.size_in_bytes
			,b.peer_id
			,b.block_time
			,b.inserted_at
			,b.chain_work
			,b.mined_set
			,b.subtrees_set
			,b.invalid
			,b.processed_at
			,b.median_time_past`

	if s.mainChainRebuilding.Load() == 0 {
		var (
			onMain      bool
			startHeight uint32
		)
		// Resolve start-block height in the probe so the main query binds it as
		// a literal parameter. This (a) lets the planner pick the
		// idx_on_main_chain_height partial index for the height range, and
		// (b) eliminates the intra-query race that a same-query subquery
		// evaluated twice would have. Treat any error / missing row / off-main-chain
		// as "not eligible" and fall through to the CTE.
		if scanErr := s.db.QueryRowContext(ctx,
			`SELECT COALESCE(on_main_chain, false), COALESCE(height, 0)
			 FROM blocks WHERE hash = $1 LIMIT 1`,
			blockHashFrom[:],
		).Scan(&onMain, &startHeight); scanErr == nil && onMain {
			fastPath := `
		SELECT` + blockColumns + `
		FROM blocks b
		WHERE b.on_main_chain = true
		  AND b.height <= $1
		  AND b.height > $1 - $2
		ORDER BY b.height DESC
		LIMIT $2
	`
			return fastPath, []interface{}{startHeight, numberOfHeaders}
		}
	}

	cte := `
		WITH RECURSIVE ChainBlocks AS (
			SELECT id, parent_id, 1 AS depth
			FROM blocks
			WHERE hash = $1
			UNION ALL
			SELECT bb.id, bb.parent_id, cb.depth + 1
			FROM blocks bb
			JOIN ChainBlocks cb ON bb.id = cb.parent_id
			WHERE bb.id != cb.id
			  AND cb.depth < $2
		)
		SELECT` + blockColumns + `
		FROM blocks b
		JOIN ChainBlocks cb ON b.id = cb.id
		ORDER BY b.height DESC
		LIMIT $2
	`
	return cte, []interface{}{blockHashFrom[:], numberOfHeaders}
}

func (s *SQL) processBlockHeadersRows(rows *sql.Rows, numberOfHeaders uint64, hasCoinbaseColumn bool) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	var (
		hashPrevBlock  []byte
		hashMerkleRoot []byte
		nBits          []byte
		insertedAt     time.CustomTime
		coinbaseBytes  []byte
		processedAt    *time.CustomTime
	)

	blockHeaders := make([]*model.BlockHeader, 0, numberOfHeaders)
	blockHeaderMetas := make([]*model.BlockHeaderMeta, 0, numberOfHeaders)

	for rows.Next() {
		blockHeader := &model.BlockHeader{}
		blockHeaderMeta := &model.BlockHeaderMeta{}

		// Create scan targets
		scanTargets := []interface{}{
			&blockHeader.Version,
			&blockHeader.Timestamp,
			&blockHeader.Nonce,
			&hashPrevBlock,
			&hashMerkleRoot,
			&nBits,
			&blockHeaderMeta.ID,
			&blockHeaderMeta.Height,
			&blockHeaderMeta.TxCount,
			&blockHeaderMeta.SizeInBytes,
			&blockHeaderMeta.PeerID,
			&blockHeaderMeta.BlockTime,
			&insertedAt,
			&blockHeaderMeta.ChainWork,
			&blockHeaderMeta.MinedSet,
			&blockHeaderMeta.SubtreesSet,
			&blockHeaderMeta.Invalid,
			&processedAt,
			&blockHeaderMeta.MedianTimePast,
		}

		// Add coinbase_tx if it's in the query
		if hasCoinbaseColumn {
			scanTargets = append(scanTargets, &coinbaseBytes)
		}

		if err := rows.Scan(scanTargets...); err != nil {
			return nil, nil, errors.NewStorageError("failed to scan row", err)
		}

		// If miner is empty but we have coinbase_tx, extract miner from it
		if blockHeaderMeta.Miner == "" && len(coinbaseBytes) > 0 {
			coinbaseTx, err := bt.NewTxFromBytes(coinbaseBytes)
			if err == nil {
				extractedMiner, err := util.ExtractCoinbaseMinerRaw(coinbaseTx, s.rawMinerTag)
				if err == nil && extractedMiner != "" {
					blockHeaderMeta.Miner = extractedMiner
				}
			}
		}

		bits, _ := model.NewNBitFromSlice(nBits)
		blockHeader.Bits = *bits

		var err error

		blockHeader.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock)
		if err != nil {
			return nil, nil, errors.NewProcessingError("failed to convert hashPrevBlock", err)
		}

		blockHeader.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot)
		if err != nil {
			return nil, nil, errors.NewProcessingError("failed to convert hashMerkleRoot", err)
		}

		insertedAtUint32, err := safeconversion.Int64ToUint32(insertedAt.Unix())
		if err != nil {
			return nil, nil, errors.NewProcessingError("failed to convert insertedAt", err)
		}

		blockHeaderMeta.Timestamp = insertedAtUint32

		// Set the block time to the timestamp in the meta
		blockHeaderMeta.BlockTime = blockHeader.Timestamp

		if processedAt != nil {
			blockHeaderMeta.ProcessedAt = &processedAt.Time
		}

		blockHeaders = append(blockHeaders, blockHeader)
		blockHeaderMetas = append(blockHeaderMetas, blockHeaderMeta)
	}

	return blockHeaders, blockHeaderMetas, nil
}
