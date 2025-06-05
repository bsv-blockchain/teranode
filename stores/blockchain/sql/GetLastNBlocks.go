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

/* The following code exists to be able to handle the fact that the inserted_at is a TEXT field in
   sqlite and a TIMESTAMP field in postgres. This is because the sqlite driver does not support
	 TIMESTAMP fields.
*/

const SQLiteTimestampFormat = "2006-01-02 15:04:05"

type CustomTime struct {
	time.Time
}

// Scan implements the sql.Scanner interface.
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

// Value implements the driver.Valuer interface.
func (ct CustomTime) Value() (driver.Value, error) {
	return ct.Time, nil
}
