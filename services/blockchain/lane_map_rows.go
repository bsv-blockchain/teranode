package blockchain

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// laneMapRowsToProto carries a block's lane map rows on AddBlock. The bytes are shared, not copied.
func laneMapRowsToProto(rows []lane.MapRow) []*blockchain_api.LaneMapRow {
	if len(rows) == 0 {
		return nil
	}

	out := make([]*blockchain_api.LaneMapRow, len(rows))

	for i, r := range rows {
		out[i] = &blockchain_api.LaneMapRow{
			Lane:      uint32(r.Lane),
			First:     r.First,
			Last:      r.Last,
			FirstHole: r.FirstHole,
			Tag:       uint32(r.Tag),
			HoleData:  r.HoleData,
		}
	}

	return out
}

// laneMapRowsFromProto reads a block's lane map rows off AddBlock. It checks only that each field
// fits; the store checks the rows themselves before it inserts them.
func laneMapRowsFromProto(rows []*blockchain_api.LaneMapRow) ([]lane.MapRow, error) {
	if len(rows) == 0 {
		return nil, nil
	}

	out := make([]lane.MapRow, len(rows))

	for i, r := range rows {
		if r.GetLane() > lane.MaxLane || r.GetTag() > 255 {
			return nil, errors.NewInvalidArgumentError("lane map row %d: lane %d or tag %d out of range", i, r.GetLane(), r.GetTag())
		}

		out[i] = lane.MapRow{
			Lane:      uint8(r.GetLane()), //nolint:gosec // checked above
			First:     r.GetFirst(),
			Last:      r.GetLast(),
			FirstHole: r.GetFirstHole(),
			Tag:       lane.RowTag(r.GetTag()), //nolint:gosec // checked above
			HoleData:  r.GetHoleData(),
		}
	}

	return out, nil
}

// insertStoredBlockLaneMapRows stores the lane map rows of the block hash names under the ID the
// store gave it. The block must be stored, and not as invalid: StoreBlock gives an invalid block no
// rows either. The store's insert is insert-only, so rows that say what is stored already write
// nothing and rows that differ fail.
func insertStoredBlockLaneMapRows(ctx context.Context, store blockchain_store.Store, hash *chainhash.Hash, rows []lane.MapRow) error {
	_, meta, err := store.GetBlockHeader(ctx, hash)
	if err != nil {
		return err
	}

	if meta.Invalid {
		return errors.NewInvalidArgumentError("block %s is stored as invalid, so it takes no lane map rows", hash.String())
	}

	return store.InsertLaneMapRows(ctx, meta.ID, rows)
}
