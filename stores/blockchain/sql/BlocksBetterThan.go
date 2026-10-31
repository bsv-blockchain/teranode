package sql

import (
	"bytes"
	"context"
	"database/sql"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/model/time"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// blocksBetterThanColumns are the columns BlocksBetterThan reads, in scan order.
const blocksBetterThanColumns = `
		 b.id
		,b.version
		,b.block_time
		,b.n_bits
		,b.nonce
		,b.previous_hash
		,b.merkle_root
		,b.size_in_bytes
		,b.coinbase_tx
		,b.peer_id
		,b.height
		,b.tx_count
		,b.chain_work
		,b.mined_set
		,b.subtrees_set
		,b.invalid
		,b.inserted_at
		,b.processed_at
		,b.median_time_past
`

// sortsBetterThanAfter is "sorts better than ($2, $3, $4)" in the best-tip order, chain_work
// DESC, peer_id ASC, id ASC. It is written as ORs rather than a row comparison because the
// order mixes directions, which a row comparison cannot express.
const sortsBetterThanAfter = `(
			b.chain_work > $2
			OR (b.chain_work = $2 AND b.peer_id < $3)
			OR (b.chain_work = $2 AND b.peer_id = $3 AND b.id < $4)
		)`

// sortsAtLeastFrom is "sorts better than or level with ($5, $6, $7)".
const sortsAtLeastFrom = `(
			b.chain_work > $5
			OR (b.chain_work = $5 AND b.peer_id < $6)
			OR (b.chain_work = $5 AND b.peer_id = $6 AND b.id <= $7)
		)`

// blocksBetterThanQuery reads the valid blocks that sort better than a key, worst first. The
// redundant b.chain_work >= $1 bounds the scan of idx_chain_work_valid, which the planner
// cannot derive from the ORs; the ORDER BY is that index read backwards, so no sort is needed.
// $1 is the lower of the two chain works when a from key is given.
const blocksBetterThanQuery = `
		SELECT` + blocksBetterThanColumns + `
		FROM blocks b
		WHERE b.invalid = false
		AND b.chain_work >= $1
		AND ` + sortsBetterThanAfter + `
		AND b.height >= $5
		ORDER BY b.chain_work ASC, b.peer_id DESC, b.id DESC
		LIMIT $6
`

// blocksBetterThanOrFromQuery is blocksBetterThanQuery that also returns the blocks at or above
// the from key.
const blocksBetterThanOrFromQuery = `
		SELECT` + blocksBetterThanColumns + `
		FROM blocks b
		WHERE b.invalid = false
		AND b.chain_work >= $1
		AND (` + sortsBetterThanAfter + ` OR ` + sortsAtLeastFrom + `)
		AND b.height >= $8
		ORDER BY b.chain_work ASC, b.peer_id DESC, b.id DESC
		LIMIT $9
`

// bestTipKeyQuery reads the key of the best tip, by the order GetBestBlockHeader uses.
const bestTipKeyQuery = `
		SELECT id, hash, chain_work, peer_id
		FROM blocks
		WHERE invalid = false
		ORDER BY chain_work DESC, peer_id ASC, id ASC
		LIMIT 1
`

// BlocksBetterThan returns the valid blocks that sort better than after in the order the store
// picks its best tip by (chain_work DESC, peer_id ASC, id ASC), worst first, at or above
// minHeight, at most limit of them. When from is given it also returns the blocks that sort
// better than or level with from, so a reader can move its position down to a low-water mark
// without comparing keys itself: peer ids compare by the database's collation, which the reader
// does not know.
//
// Without an invalidation the best tip only ever moves to a block that sorts better, so every
// block that was the best tip after after is in the result, in the order it could have become
// the tip. Blocks that sorted better but were never the tip are in it too.
func (s *SQL) BlocksBetterThan(ctx context.Context, after model.BestTipKey, from *model.BestTipKey, minHeight, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error) {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:BlocksBetterThan")
	defer deferFn()

	var (
		rows *sql.Rows
		err  error
	)

	if from == nil {
		rows, err = s.db.QueryContext(ctx, blocksBetterThanQuery,
			after.ChainWork, after.ChainWork, after.PeerID, int64(after.ID), minHeight, limit)
	} else {
		lowest := after.ChainWork
		if bytes.Compare(from.ChainWork, lowest) < 0 {
			lowest = from.ChainWork
		}

		rows, err = s.db.QueryContext(ctx, blocksBetterThanOrFromQuery,
			lowest, after.ChainWork, after.PeerID, int64(after.ID),
			from.ChainWork, from.PeerID, int64(from.ID), minHeight, limit)
	}

	if err != nil {
		return nil, nil, errors.NewStorageError("failed to get blocks better than %s", after.Hash, err)
	}

	defer rows.Close()

	headers := make([]*model.BlockHeader, 0, preallocFor(uint64(limit)))
	metas := make([]*model.BlockHeaderMeta, 0, preallocFor(uint64(limit)))

	for rows.Next() {
		header := &model.BlockHeader{}
		meta := &model.BlockHeaderMeta{}

		var (
			hashPrevBlock  []byte
			hashMerkleRoot []byte
			nBits          []byte
			coinbaseBytes  []byte
			insertedAt     time.CustomTime
			processedAt    *time.CustomTime
		)

		if err = rows.Scan(
			&meta.ID,
			&header.Version,
			&header.Timestamp,
			&nBits,
			&header.Nonce,
			&hashPrevBlock,
			&hashMerkleRoot,
			&meta.SizeInBytes,
			&coinbaseBytes,
			&meta.PeerID,
			&meta.Height,
			&meta.TxCount,
			&meta.ChainWork,
			&meta.MinedSet,
			&meta.SubtreesSet,
			&meta.Invalid,
			&insertedAt,
			&processedAt,
			&meta.MedianTimePast,
		); err != nil {
			return nil, nil, errors.NewStorageError("failed to scan block better than %s", after.Hash, err)
		}

		bits, bitsErr := model.NewNBitFromSlice(nBits)
		if bitsErr != nil {
			return nil, nil, errors.NewStorageError("error in BlocksBetterThan: malformed n_bits (length %d, expected 4) for block height %d", len(nBits), meta.Height, bitsErr)
		}

		header.Bits = *bits

		if header.HashPrevBlock, err = chainhash.NewHash(hashPrevBlock); err != nil {
			return nil, nil, errors.NewProcessingError("failed to convert hashPrevBlock", err)
		}

		if header.HashMerkleRoot, err = chainhash.NewHash(hashMerkleRoot); err != nil {
			return nil, nil, errors.NewProcessingError("failed to convert hashMerkleRoot", err)
		}

		if processedAt != nil {
			meta.ProcessedAt = &processedAt.Time
		}

		meta.Timestamp = uint32(insertedAt.Unix()) //nolint:gosec // block insert time fits uint32 until 2106
		meta.BlockTime = header.Timestamp

		if len(coinbaseBytes) > 0 {
			if coinbaseTx, txErr := bt.NewTxFromBytes(coinbaseBytes); txErr == nil {
				if miner, minerErr := util.ExtractCoinbaseMinerRaw(coinbaseTx, s.rawMinerTag); minerErr == nil {
					meta.Miner = miner
				}
			}
		}

		headers = append(headers, header)
		metas = append(metas, meta)
	}

	if err = rows.Err(); err != nil {
		return nil, nil, errors.NewStorageError("failed to read blocks better than %s", after.Hash, err)
	}

	return headers, metas, nil
}

// ClearBestTipLowWater deletes the best tip's low-water mark if it still holds expected, the
// bytes the caller read. A drop recorded since changes the bytes, so the delete is then a no-op
// and the caller sees the newer mark on its next read.
func (s *SQL) ClearBestTipLowWater(ctx context.Context, expected []byte) error {
	ctx, _, deferFn := tracing.Tracer("blockchain").Start(ctx, "sql:ClearBestTipLowWater")
	defer deferFn()

	if _, err := s.db.ExecContext(ctx, `DELETE FROM state WHERE key = $1 AND data = $2`, model.BestTipLowWaterStateKey, expected); err != nil {
		return errors.NewStorageError("failed to clear best tip low-water mark", err)
	}

	return nil
}

// bestTipKey reads the key of the best tip through q. ok is false when no valid block exists.
func bestTipKey(ctx context.Context, q execQuerier) (key model.BestTipKey, ok bool, err error) {
	var (
		id   int64
		hash []byte
	)

	if err = q.QueryRowContext(ctx, bestTipKeyQuery).Scan(&id, &hash, &key.ChainWork, &key.PeerID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.BestTipKey{}, false, nil
		}

		return model.BestTipKey{}, false, err
	}

	h, err := chainhash.NewHash(hash)
	if err != nil {
		return model.BestTipKey{}, false, err
	}

	key.ID = uint32(id) //nolint:gosec // block ids are uint32 everywhere in the model
	key.Hash = *h

	return key, true, nil
}

// recordBestTipDrop records in the low-water mark that the best tip moved from before to the
// current best, when it moved. Only a write that can make the best tip sort worse calls it, so a
// move is a drop. It runs inside the transaction that changed the tip, and the caller holds
// slowPathMu, so two drops never merge into the mark at once.
//
// The mark keeps the lower of its key and the new best's, and adds the block that was the best
// tip before to its dropped tips. It gets a new generation either way, so a reader that read the
// old mark does not clear this drop with it. The keys are compared by the database, under the
// same collation the best-tip order uses.
func (s *SQL) recordBestTipDrop(ctx context.Context, tx *sql.Tx, before model.BestTipKey, hadBefore bool) error {
	after, ok, err := bestTipKey(ctx, tx)
	if err != nil || !ok || (hadBefore && after.ID == before.ID) {
		return err
	}

	mark := model.BestTipLowWater{Key: after}

	var existing []byte

	switch err = tx.QueryRowContext(ctx, `SELECT data FROM state WHERE key = $1`, model.BestTipLowWaterStateKey).Scan(&existing); {
	case errors.Is(err, sql.ErrNoRows):
	case err != nil:
		return err
	default:
		old, decodeErr := model.NewBestTipLowWaterFromBytes(existing)
		if decodeErr != nil {
			// A mark this binary cannot read is replaced. The new best is a drop on its own,
			// so recording it still makes the reader replay from it. The tips the old mark
			// listed are lost with it, so the overflow flag makes the reader reset anyway.
			s.logger.Warnf("[recordBestTipDrop] replacing unreadable best tip low-water mark: %v", decodeErr)

			mark.DroppedTipsOverflow = true
		} else {
			mark.Generation = old.Generation + 1
			mark.DroppedTips = old.DroppedTips
			mark.DroppedTipsOverflow = old.DroppedTipsOverflow

			lower, cmpErr := s.sortsWorse(ctx, tx, old.Key, after)
			if cmpErr != nil {
				return cmpErr
			}

			if lower {
				mark.Key = old.Key
			}
		}
	}

	// The block that was the best tip may never have been applied by block assembly, and once
	// it is invalid or deleted no read of valid blocks returns it, so the mark names it.
	if hadBefore {
		mark.AddDroppedTip(before.Hash)
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO state (key, data)
		VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET
			data = EXCLUDED.data,
			updated_at = CURRENT_TIMESTAMP
	`, model.BestTipLowWaterStateKey, mark.Bytes())

	return err
}

// sortsWorse reports whether a sorts worse than b in the best-tip order. The database compares
// them, so peer ids compare under the collation the blocks index uses.
func (s *SQL) sortsWorse(ctx context.Context, q execQuerier, a, b model.BestTipKey) (bool, error) {
	// The placeholders first appear in order, which SQLite needs to bind them by position.
	query := `SELECT ($1 < $2) OR ($1 = $2 AND ($3 > $4 OR ($3 = $4 AND $5 > $6)))`
	if s.engine == util.Postgres {
		query = `SELECT ($1::bytea < $2::bytea) OR ($1::bytea = $2::bytea AND ($3::text > $4::text OR ($3::text = $4::text AND $5::bigint > $6::bigint)))`
	}

	var worse bool

	if err := q.QueryRowContext(ctx, query, a.ChainWork, b.ChainWork, a.PeerID, b.PeerID, int64(a.ID), int64(b.ID)).Scan(&worse); err != nil {
		return false, err
	}

	return worse, nil
}
