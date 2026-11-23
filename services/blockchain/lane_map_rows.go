package blockchain

import (
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
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
