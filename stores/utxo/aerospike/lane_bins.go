package aerospike

import (
	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// The two lane list bins, fields.SpendingLanePositions and fields.ParentLanePositions, hold lane
// positions as Aerospike integers: signed 64-bit, the packed value bit for bit (lane.ToSigned), so
// lanes 128 to 255 are negative. Aerospike has no schema to install, so these are the bins'
// format, which every writer and parser must share: the Go codec below, and the Lua, C module,
// native and expression writers when the step that fills the bins adds them. Nothing in this build
// writes either bin.

// spendingLanePositionsBin builds the slot lane-position bin of a record holding n output slots,
// every slot unspent.
func spendingLanePositionsBin(n int) *aerospike.Bin {
	return aerospike.NewBin(fields.SpendingLanePositions.String(), make([]interface{}, n))
}

// parentLanePositionsBin builds a child's parent lane-position bin, checking every position.
func parentLanePositionsBin(parents []uint64) (*aerospike.Bin, error) {
	list := make([]interface{}, len(parents))

	for i, p := range parents {
		if err := lane.CheckPosition(p); err != nil {
			return nil, errors.NewProcessingError("parent %d", i, err)
		}

		list[i] = lane.ToSigned(p)
	}

	return aerospike.NewBin(fields.ParentLanePositions.String(), list), nil
}

// decodeSpendingLanePositions parses the slot lane-position bin of a record holding n output
// slots. A slot not spent reads 0 (a nil or zero element); a spent slot's position must be valid.
// A list of another length or with a non-integer element is an error.
func decodeSpendingLanePositions(v interface{}, n int) ([]uint64, error) {
	list, err := laneList(v, fields.SpendingLanePositions, n)
	if err != nil {
		return nil, err
	}

	out := make([]uint64, n)

	for i, e := range list {
		if e == nil {
			continue
		}

		p, err := laneListElement(e, fields.SpendingLanePositions, i)
		if err != nil {
			return nil, err
		}

		if p != lane.None {
			if err = lane.CheckPosition(p); err != nil {
				return nil, errors.NewProcessingError("%s slot %d", fields.SpendingLanePositions, i, err)
			}
		}

		out[i] = p
	}

	return out, nil
}

// decodeParentLanePositions parses a child's parent lane-position bin for a transaction with
// parentCount distinct parents. Every position is required and must be valid.
func decodeParentLanePositions(v interface{}, parentCount int) ([]uint64, error) {
	list, err := laneList(v, fields.ParentLanePositions, parentCount)
	if err != nil {
		return nil, err
	}

	out := make([]uint64, parentCount)

	for i, e := range list {
		p, err := laneListElement(e, fields.ParentLanePositions, i)
		if err != nil {
			return nil, err
		}

		if err = lane.CheckPosition(p); err != nil {
			return nil, errors.NewProcessingError("%s parent %d", fields.ParentLanePositions, i, err)
		}

		out[i] = p
	}

	return out, nil
}

func laneList(v interface{}, bin fields.FieldName, want int) ([]interface{}, error) {
	list, ok := v.([]interface{})
	if !ok {
		return nil, errors.NewProcessingError("%s bin is %T, not a list", bin, v)
	}

	if len(list) != want {
		return nil, errors.NewProcessingError("%s bin holds %d entries, want %d", bin, len(list), want)
	}

	return list, nil
}

func laneListElement(e interface{}, bin fields.FieldName, i int) (uint64, error) {
	switch t := e.(type) {
	case int:
		return lane.FromSigned(int64(t)), nil
	case int64:
		return lane.FromSigned(t), nil
	default:
		return 0, errors.NewProcessingError("%s entry %d is %T, not an integer", bin, i, e)
	}
}
