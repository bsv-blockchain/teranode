package aerospike

import (
	"encoding/binary"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util/lane"
)

// The two lane bins are the formats every writer and parser shares: the Go codec below, and
// teranode.lua and the C module's UDF and native spend, which write the slot bin. Aerospike has no
// schema to install, so these definitions are the format.
//
// fields.SpendingLanePositions is the slot bin: a byte string beside the utxos bin of the master or
// page record that holds the outputs, eight little-endian bytes per output in output order, the
// packed position bit for bit. The position of output offset is bytes offset*8 to offset*8+7. A
// fresh spend that carries a lane position writes it in the same record write as the utxos change,
// growing the bin with zeros to cover that output; a spend that carries none writes nothing, so a
// record spent with lane positions off has no such bin. Zero, or an output past the end of the bin,
// means no position. Unspend zeroes the output's eight bytes. It is the shape of the spendHeights
// bin, so the C module handles both alike.
//
// fields.ParentLanePositions is the child's copy of its parents' positions: a list on the master
// record of Aerospike integers, signed 64-bit, the packed value bit for bit (lane.ToSigned), so
// lanes 128 to 255 are negative.

// slotLanePositionSize is the bytes per output in the slot bin.
const slotLanePositionSize = lane.PositionSize

// appendSlotLanePositions encodes positions as the slot bin holds them, one per output in order, 0
// for an output with none. Only the servers write the bin; this is the format's reference, for tests
// and readers.
func appendSlotLanePositions(dst []byte, positions []uint64) []byte {
	return lane.AppendPositions(dst, positions)
}

// decodeSpendingLanePositions parses the slot bin of a record holding n outputs into one position
// per output, 0 for none. An absent bin is every output without one. A bin that is not a byte
// string, is not a whole number of positions, covers more than n outputs, or holds an invalid
// non-zero position is an error.
func decodeSpendingLanePositions(v interface{}, n int) ([]uint64, error) {
	out := make([]uint64, n)

	if v == nil {
		return out, nil
	}

	b, ok := v.([]byte)
	if !ok {
		return nil, errors.NewProcessingError("%s bin is %T, not bytes", fields.SpendingLanePositions, v)
	}

	if len(b)%slotLanePositionSize != 0 || len(b)/slotLanePositionSize > n {
		return nil, errors.NewProcessingError("%s bin is %d bytes, not a position for each of at most %d outputs",
			fields.SpendingLanePositions, len(b), n)
	}

	for i := 0; i*slotLanePositionSize < len(b); i++ {
		p := binary.LittleEndian.Uint64(b[i*slotLanePositionSize:])
		if p != lane.None {
			if err := lane.CheckPosition(p); err != nil {
				return nil, errors.NewProcessingError("%s output %d", fields.SpendingLanePositions, i, err)
			}
		}

		out[i] = p
	}

	return out, nil
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

// laneShadow reports whether lane positions are on (utxostore_laneMigration shadow or later). Only
// then does a create carry its parents' positions and a spend its spender's, and only then does the
// store count lane position coverage.
func (s *Store) laneShadow() bool {
	return s.settings != nil && s.settings.UtxoStore.LaneMigration.Stamps()
}

// countLaneSpends counts n spent outputs of one spend. They count as carried only when the spend
// carried the spender's lane position and its route records it: the Lua and native spends do, on a
// server the start-up probe found writing it (laneSlotWrites); the expression spend does not.
func (s *Store) countLaneSpends(n int, carriedPosition bool) {
	if carriedPosition && s.laneSlotWrites.Load() && !s.useExpressionSpend() {
		utxo.CountLaneSpends(laneStoreName, n, 0)
		return
	}

	utxo.CountLaneSpends(laneStoreName, 0, n)
}

// laneStoreName labels this store's lane position counters.
const laneStoreName = "aerospike"
