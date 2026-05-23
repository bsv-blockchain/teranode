// Package txparse provides streaming parsers for Bitcoin transaction wire formats.
// These parsers extract only the data needed for specific operations, avoiding
// full deserialization of potentially large transactions.
package txparse

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// ParseInputReferencesFromExtendedTx parses an Extended Format Bitcoin transaction from a reader
// to extract only input references (prevTxID + prevOutIndex), skipping all scripts and Extended
// Format metadata to minimize memory usage and bandwidth.
//
// Extended Format has: version(4) + marker(6: 0x00 0x00 0x00 0x00 0x00 0xEF) + inputs + outputs.
// Each input in Extended Format includes PreviousTxSatoshis(8) and PreviousTxScript(varint+bytes)
// after the standard fields, which are skipped.
//
// The reader is consumed only up to the end of inputs — outputs are never read from disk/network.
func ParseInputReferencesFromExtendedTx(reader io.Reader) ([]*bt.Input, error) {
	// Skip version (4 bytes)
	if _, err := io.CopyN(io.Discard, reader, 4); err != nil {
		return nil, errors.NewProcessingError("failed to skip version", err)
	}

	// Extended Format: Verify the marker (6 bytes: 0x00 0x00 0x00 0x00 0x00 0xEF)
	extendedMarker := make([]byte, 6)
	if _, err := io.ReadFull(reader, extendedMarker); err != nil {
		return nil, errors.NewProcessingError("failed to read extended format marker", err)
	}

	expectedMarker := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0xEF}
	if !bytes.Equal(extendedMarker, expectedMarker) {
		return nil, errors.NewProcessingError("transaction is not in extended format")
	}

	// Parse input count
	var inputCountVarInt bt.VarInt
	if _, err := inputCountVarInt.ReadFrom(reader); err != nil {
		return nil, errors.NewProcessingError("failed to read input count", err)
	}
	inputCount := int(inputCountVarInt)

	inputs := make([]*bt.Input, inputCount)

	for i := 0; i < inputCount; i++ {
		// Read previous tx ID (32 bytes)
		prevTxID := make([]byte, 32)
		if _, err := io.ReadFull(reader, prevTxID); err != nil {
			return nil, errors.NewProcessingError("failed to read prevTxID for input %d/%d", i, inputCount, err)
		}

		// Read previous output index (4 bytes)
		var prevOutIndex uint32
		if err := binary.Read(reader, binary.LittleEndian, &prevOutIndex); err != nil {
			return nil, errors.NewProcessingError("failed to read prevOutIndex for input %d/%d", i, inputCount, err)
		}

		// Skip unlocking script (varint length + script bytes)
		var scriptLenVarInt bt.VarInt
		if _, err := scriptLenVarInt.ReadFrom(reader); err != nil {
			return nil, errors.NewProcessingError("failed to read script length for input %d/%d", i, inputCount, err)
		}
		if _, err := io.CopyN(io.Discard, reader, int64(scriptLenVarInt)); err != nil {
			return nil, errors.NewProcessingError("failed to skip script for input %d/%d", i, inputCount, err)
		}

		// Skip sequence number (4 bytes)
		if _, err := io.CopyN(io.Discard, reader, 4); err != nil {
			return nil, errors.NewProcessingError("failed to skip sequence for input %d/%d", i, inputCount, err)
		}

		// Extended Format: skip PreviousTxSatoshis (8 bytes)
		if _, err := io.CopyN(io.Discard, reader, 8); err != nil {
			return nil, errors.NewProcessingError("failed to skip previous satoshis for input %d/%d", i, inputCount, err)
		}

		// Extended Format: skip PreviousTxScript (varint length + script bytes)
		var prevScriptLenVarInt bt.VarInt
		if _, err := prevScriptLenVarInt.ReadFrom(reader); err != nil {
			return nil, errors.NewProcessingError("failed to read previous script length for input %d/%d", i, inputCount, err)
		}
		if _, err := io.CopyN(io.Discard, reader, int64(prevScriptLenVarInt)); err != nil {
			return nil, errors.NewProcessingError("failed to skip previous script for input %d/%d", i, inputCount, err)
		}

		// Create minimal Input with only fields needed for parent updates
		prevHash, err := chainhash.NewHash(prevTxID)
		if err != nil {
			return nil, errors.NewProcessingError("invalid previous tx id for input %d/%d", i, inputCount, err)
		}

		inputs[i] = &bt.Input{
			PreviousTxOutIndex: prevOutIndex,
		}
		if err := inputs[i].PreviousTxIDAdd(prevHash); err != nil {
			return nil, errors.NewProcessingError("failed to set previous tx id for input %d/%d", i, inputCount, err)
		}
	}

	// STOP - don't parse outputs
	return inputs, nil
}
