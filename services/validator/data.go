/*
Package validator implements Bitcoin SV transaction validation functionality.

This file implements the data structures and methods for handling transaction
validation data, particularly focusing on the serialization and deserialization
of transaction validation requests.
*/
package validator

import (
	"encoding/binary"

	"github.com/bitcoin-sv/teranode/errors"
)

// TxValidationData encapsulates the data required for transaction validation
// This structure combines the transaction bytes with the block height at which
// the transaction should be validated.
type TxValidationData struct {
	// Tx contains the raw transaction bytes
	// This field holds the serialized transaction data to be validated
	Tx []byte

	// Height represents the block height at which the transaction should be validated
	// This is crucial for applying the correct validation rules based on protocol upgrades
	Height uint32

	// validation options
	Options *Options
}

func NewTxValidationData(tx []byte, height uint32, options *Options) *TxValidationData {
	return &TxValidationData{
		Tx:      tx,
		Height:  height,
		Options: options,
	}
}

// NewTxValidationDataFromBytes deserializes a byte slice into a TxValidationData structure
// The byte format is:
// [4 bytes for height in little-endian][remaining bytes for transaction data]
//
// Parameters:
//   - bytes: Raw byte slice containing serialized validation data
//
// Returns:
//   - *TxValidationData: Deserialized transaction validation data
//   - error: Any errors encountered during deserialization
//
// Error cases:
//   - Input bytes too short (less than 4 bytes)
//   - Invalid height encoding
func NewTxValidationDataFromBytes(bytes []byte) (*TxValidationData, error) {
	// Check minimum length requirement for height field
	if len(bytes) < 4 {
		return nil, errors.New(errors.ERR_ERROR, "input bytes too short")
	}

	d := &TxValidationData{}

	// Extract height from first 4 bytes (little-endian)
	d.Height = binary.LittleEndian.Uint32(bytes[:4])

	// Extract validation options from the 5th byte
	d.Options = NewDefaultOptions()
	validationOptionsByte := bytes[4]

	// set the first bit to true if skipUtxoCreation is true
	d.Options.skipUtxoCreation = validationOptionsByte&0x01 == 0x01

	// set the second bit to true if addTXToBlockAssembly is true
	d.Options.addTXToBlockAssembly = validationOptionsByte&0x02 == 0x02

	// set the third bit to true if skipPolicyChecks is true
	d.Options.skipPolicyChecks = validationOptionsByte&0x04 == 0x04

	// set the fourth bit to true if createConflicting is true
	d.Options.createConflicting = validationOptionsByte&0x08 == 0x08

	// read remaining bytes as tx (if present)
	if len(bytes) > 4 {
		d.Tx = make([]byte, len(bytes[5:]))
		copy(d.Tx, bytes[5:])
	}

	return d, nil
}

// Bytes serializes the TxValidationData structure into a byte slice
// The resulting byte format is:
// [4 bytes for height in little-endian][transaction bytes]
//
// Returns:
//   - []byte: Serialized validation data
//
// Format:
//   - First 4 bytes: Block height in little-endian
//   - Remaining bytes: Transaction data
//
// Note: The method performs a defensive copy of the transaction data
// to ensure immutability of the original data.
func (d *TxValidationData) Bytes() []byte {
	// Calculate total size needed
	bytes := make([]byte, 0, 4+len(d.Tx))

	// Write height (4 bytes) in little-endian format
	b32 := make([]byte, 4)
	binary.LittleEndian.PutUint32(b32, d.Height)
	bytes = append(bytes, b32...)

	// write the validation options as a byte slice
	validationOptionsByte := byte(0x00)

	// set the first but to 1 if skipUtxoCreation is true
	if d.Options.skipUtxoCreation {
		validationOptionsByte |= 0x01
	}

	// set the second bit to 1 if addTXToBlockAssembly is true
	if d.Options.addTXToBlockAssembly {
		validationOptionsByte |= 0x02
	}

	// set the third bit to 1 if skipPolicyChecks is true
	if d.Options.skipPolicyChecks {
		validationOptionsByte |= 0x04
	}

	// set the fourth bit to 1 if createConflicting is true
	if d.Options.createConflicting {
		validationOptionsByte |= 0x08
	}

	// Append validation options byte
	bytes = append(bytes, validationOptionsByte)

	// Append transaction data
	bytes = append(bytes, d.Tx...)

	return bytes
}
