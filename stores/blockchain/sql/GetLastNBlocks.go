// Package sql implements the blockchain.Store interface using SQL database backends.
// It provides concrete SQL-based implementations for all blockchain operations
// defined in the interface, with support for different SQL engines.
//
// This file implements the GetLastNBlocks method, which retrieves information about the
// most recent blocks in the blockchain. This functionality is essential for blockchain
// explorers, monitoring tools, and diagnostic interfaces that need to display recent
// blockchain activity. The implementation includes efficient caching to optimize performance
// for repeated queries, support for including or excluding orphaned blocks, and filtering
// by maximum height. It also includes custom time handling to accommodate differences
// between PostgreSQL and SQLite timestamp representations, ensuring consistent behavior
// across different database backends.
package sql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/util/tracing"
	"github.com/libsv/go-bt/v2/chainhash"
)

// GetLastNBlocks retrieves information about the most recent blocks in the blockchain.
// This implements the blockchain.Store.GetLastNBlocks interface method.
//
// The method retrieves detailed information about the N most recent blocks, with options
// to include orphaned blocks and filter by maximum height. This functionality is essential
// for blockchain explorers, monitoring tools, and diagnostic interfaces that need to display
// recent blockchain activity. In Teranode's high-throughput architecture, efficient access
// to recent block information is critical for monitoring system health and performance.
//
// The implementation uses a response cache to optimize performance for repeated queries,
// which is particularly important for frequently accessed recent block data. It constructs
// SQL queries dynamically based on the provided parameters, with different query paths for
// including or excluding orphaned blocks. The method also handles database engine differences
// between PostgreSQL and SQLite, particularly for timestamp handling.
//
// Parameters:
//   - ctx: Context for the database operation, allowing for cancellation and timeouts
//   - n: The number of most recent blocks to retrieve
//   - includeOrphans: If true, includes orphaned blocks (blocks not in the main chain);
//     if false, only includes blocks in the main chain
//   - fromHeight: Optional maximum height filter; if greater than 0, only blocks with
//     height less than or equal to this value will be included
//
// Returns:
//   - []*model.BlockInfo: An array of block information structures containing details
//     such as hash, height, timestamp, transaction count, and size for each block
//   - error: Any error encountered during retrieval, specifically:
//     - StorageError for database errors or processing failures
func (s *SQL) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "sql:GetLastNBlocks")
	defer deferFn()

	// the cache will be invalidated by the StoreBlock function when a new block is added, or after cacheTTL seconds
	cacheID := chainhash.HashH([]byte(fmt.Sprintf("GetLastNBlocks-%d-%t-%d", n, includeOrphans, fromHeight)))

	cached := s.responseCache.Get(cacheID)
	if cached != nil && cached.Value() != nil {
		if cacheData, ok := cached.Value().([]*model.BlockInfo); ok && cacheData != nil {
			s.logger.Debugf("GetLastNBlocks cache hit")
			return cacheData, nil
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fromHeightQuery := ""
	if fromHeight > 0 {
		fromHeightQuery = fmt.Sprintf("WHERE height <= %d", fromHeight)
	}

	var q string

	if includeOrphans {
		q = `
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
		WHERE invalid = false
		ORDER BY height DESC
	  LIMIT $1
	`
	} else {
		q = `
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
		WHERE id IN (
			SELECT id FROM blocks
			WHERE id IN (
				WITH RECURSIVE ChainBlocks AS (
					SELECT id, parent_id, height
					FROM blocks
					WHERE invalid = false
					AND hash = (
						SELECT b.hash
						FROM blocks b
						WHERE b.invalid = false
						ORDER BY chain_work DESC, peer_id ASC, id ASC
						LIMIT 1
					)
					UNION ALL
					SELECT bb.id, bb.parent_id, bb.height
					FROM blocks bb
					JOIN ChainBlocks cb ON bb.id = cb.parent_id
					WHERE bb.id != cb.id
					  AND bb.invalid = false
				)
				SELECT id FROM ChainBlocks
				` + fromHeightQuery + `
				LIMIT $1
			)
		)
		ORDER BY height DESC
	`
	}

	rows, err := s.db.QueryContext(ctx, q, n)
	if err != nil {
		return nil, errors.NewStorageError("failed to get blocks", err)
	}
	defer rows.Close()

	// Process the query results using the common helper function
	blockInfos, err := s.processBlockRows(rows)
	if err != nil {
		return nil, err
	}

	s.responseCache.Set(cacheID, blockInfos, s.cacheTTL)

	return blockInfos, nil
}

// The following code implements custom time handling to accommodate differences between
// database engines. SQLite stores timestamps as TEXT fields while PostgreSQL uses native
// TIMESTAMP fields. This abstraction layer ensures consistent behavior across different
// database backends, which is essential for Teranode's database engine agnostic design.

const SQLiteTimestampFormat = "2006-01-02 15:04:05"

// CustomTime is a wrapper around time.Time that implements custom database serialization.
// This type provides database engine abstraction by handling the different timestamp
// formats used by PostgreSQL (native TIMESTAMP) and SQLite (TEXT fields).
type CustomTime struct {
	time.Time
}

// Scan implements the sql.Scanner interface for CustomTime.
//
// This method provides database engine abstraction by handling different timestamp
// representations from various SQL backends. It supports scanning time values from:
// - Native time.Time objects (used by PostgreSQL)
// - String representations (used by SQLite)
// - Byte array representations
//
// The method parses string and byte array values using the SQLiteTimestampFormat constant,
// ensuring consistent behavior regardless of the underlying database engine. This abstraction
// is critical for Teranode's database-agnostic design, allowing the same code to work with
// both PostgreSQL and SQLite backends without modification.
//
// Parameters:
//   - value: The database value to scan, which may be a time.Time, []byte, or string
//
// Returns:
//   - error: Any error encountered during scanning or parsing, specifically:
//     - ProcessingError for unsupported value types
//     - Time parsing errors if the string format is invalid
func (ct *CustomTime) Scan(value interface{}) error {
	switch v := value.(type) {
	case time.Time:
		ct.Time = v
		return nil
	case []byte:
		t, err := time.Parse(SQLiteTimestampFormat, string(v))
		if err != nil {
			return err
		}

		ct.Time = t

		return nil
	case string:
		t, err := time.Parse(SQLiteTimestampFormat, v)
		if err != nil {
			return err
		}

		ct.Time = t

		return nil
	}

	return errors.NewProcessingError("unsupported type: %T", value)
}

// Value implements the driver.Valuer interface for CustomTime.
//
// This method provides the database serialization functionality for CustomTime values,
// converting them to a format that can be stored in the database. It returns the underlying
// time.Time value, which will be handled appropriately by the database driver:
// - PostgreSQL driver will store it as a native TIMESTAMP
// - SQLite driver will convert it to a string using the appropriate format
//
// This abstraction is essential for Teranode's database-agnostic design, ensuring that
// timestamp values are consistently handled regardless of the underlying database engine.
//
// Returns:
//   - driver.Value: The time.Time value to be stored in the database
//   - error: Any error encountered during conversion (always nil for this implementation)
func (ct CustomTime) Value() (driver.Value, error) {
	return ct.Time, nil
}
