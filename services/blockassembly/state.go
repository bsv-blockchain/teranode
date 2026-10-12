package blockassembly

import (
	"encoding/binary"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
)

// StateKey is the key under which the BlockAssembler persists its checkpoint
// (best block header + height) in the blockchain store's state table. External
// tools that seed or rewind the node (e.g. cmd/seeder, cmd/rewindblockchain)
// must use this key and the Encode/DecodeState helpers so the on-disk format
// never drifts from what initState reads back on startup.
const StateKey = "BlockAssembler"

// EncodeState serialises a BlockAssembler checkpoint into its persisted form:
// a 4-byte little-endian height followed by the 80-byte block header.
func EncodeState(header *model.BlockHeader, height uint32) []byte {
	headerBytes := header.Bytes()

	state := make([]byte, 4+len(headerBytes))
	binary.LittleEndian.PutUint32(state[:4], height)
	copy(state[4:], headerBytes)

	return state
}

// DecodeState parses a persisted BlockAssembler checkpoint produced by
// EncodeState, returning the block header and its height.
func DecodeState(data []byte) (*model.BlockHeader, uint32, error) {
	if len(data) < 4 {
		return nil, 0, errors.NewProcessingError("invalid BlockAssembler state: expected at least 4 bytes, got %d", len(data))
	}

	height := binary.LittleEndian.Uint32(data[:4])

	header, err := model.NewBlockHeaderFromBytes(data[4:])
	if err != nil {
		return nil, 0, err
	}

	return header, height, nil
}
