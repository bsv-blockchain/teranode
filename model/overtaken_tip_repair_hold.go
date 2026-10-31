package model

import (
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// OvertakenTipRepairHoldKey is the blockchain state key under which block validation records
// the overtaken blocks whose repair has not finished. While the record lists any, the pruner
// deletes nothing: their transactions may look mined while off the longest chain, so the
// pruner cannot tell which parents they still need. An absent record, or one listing no
// blocks, holds nothing.
const OvertakenTipRepairHoldKey = "OvertakenTipRepairHold"

// heldBlockSize is the encoded size of one HeldBlock: hash, id, height.
const heldBlockSize = chainhash.HashSize + 4 + 4

// overtakenTipRepairHoldVersion is the first byte of every record. It also keeps a record with
// no blocks from being empty, which the gRPC client would send as nil and the SQL state table
// would refuse.
const overtakenTipRepairHoldVersion = 1

// HeldBlock is an overtaken block whose repair has not finished.
type HeldBlock struct {
	Hash   chainhash.Hash
	ID     uint32
	Height uint32
}

// EncodeOvertakenTipRepairHold encodes held blocks for OvertakenTipRepairHoldKey: a version
// byte, then the blocks.
func EncodeOvertakenTipRepairHold(held []HeldBlock) []byte {
	data := make([]byte, 1, 1+len(held)*heldBlockSize)
	data[0] = overtakenTipRepairHoldVersion

	for _, b := range held {
		data = append(data, b.Hash[:]...)
		data = binary.LittleEndian.AppendUint32(data, b.ID)
		data = binary.LittleEndian.AppendUint32(data, b.Height)
	}

	return data
}

// DecodeOvertakenTipRepairHold decodes a record written by EncodeOvertakenTipRepairHold.
func DecodeOvertakenTipRepairHold(data []byte) ([]HeldBlock, error) {
	if len(data) == 0 || data[0] != overtakenTipRepairHoldVersion {
		return nil, errors.NewProcessingError("invalid overtaken-tip repair hold: not a version %d record", overtakenTipRepairHoldVersion)
	}

	data = data[1:]

	if len(data)%heldBlockSize != 0 {
		return nil, errors.NewProcessingError("invalid overtaken-tip repair hold: %d bytes is not a whole number of %d-byte entries", len(data), heldBlockSize)
	}

	held := make([]HeldBlock, 0, len(data)/heldBlockSize)

	for off := 0; off < len(data); off += heldBlockSize {
		var b HeldBlock

		copy(b.Hash[:], data[off:off+chainhash.HashSize])
		b.ID = binary.LittleEndian.Uint32(data[off+chainhash.HashSize:])
		b.Height = binary.LittleEndian.Uint32(data[off+chainhash.HashSize+4:])
		held = append(held, b)
	}

	return held, nil
}
