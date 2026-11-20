package model

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/bsv-blockchain/teranode/errors"
)

// UTXOValue represents the spendable value and conditions for a UTXO.
//
// Contains the monetary value in satoshis and the locking script that must be
// satisfied to spend the output.
type UTXOValue struct {
	// Value represents the amount in satoshis
	Value uint64
	// Locktime represents the transaction locktime
	Locktime uint32
	// Script contains the locking script
	Script []byte
}

// NewUTXOValue creates a new UTXOValue with the specified parameters.
func NewUTXOValue(value uint64, locktime uint32, script []byte) *UTXOValue {
	return &UTXOValue{
		Value:    value,
		Locktime: locktime,
		Script:   script,
	}
}

// maxExactScriptAlloc is the largest script length NewUTXOValueFromReader
// trusts enough to allocate up front; longer scripts use a bounded, growing
// read. This mirrors services/utxopersister, the production (and hardened)
// copy of this UTXO wire format.
const maxExactScriptAlloc = 64 * 1024

// NewUTXOValueFromBytes creates a UTXOValue from a byte slice.
//
// b is untrusted: it may be truncated or crafted. The fixed header is 16 bytes
// (8-byte value + 4-byte locktime + 4-byte script length) followed by the
// script. Both the header and the declared script length are bounds-checked so
// a short or hostile buffer returns an error instead of panicking with a
// slice-out-of-range.
func NewUTXOValueFromBytes(b []byte) (*UTXOValue, error) {
	if len(b) < 16 {
		return nil, errors.NewProcessingError("utxo value too short: need at least 16 header bytes, got %d", len(b))
	}

	u := new(UTXOValue)

	u.Value = binary.LittleEndian.Uint64(b[:8])
	u.Locktime = binary.LittleEndian.Uint32(b[8:12])

	// Read the script length from the next 4 bytes
	length := binary.LittleEndian.Uint32(b[12:16])

	// length is an untrusted value read from b; guard the slice so a bogus
	// length cannot drive an out-of-range panic. uint64 arithmetic avoids any
	// overflow of 16+length.
	if uint64(len(b)) < 16+uint64(length) {
		return nil, errors.NewProcessingError("utxo value script truncated: declared length %d but only %d script bytes available", length, len(b)-16)
	}

	u.Script = b[16 : 16+length]

	return u, nil
}

// NewUTXOValueFromReader creates a UTXOValue by reading from an io.Reader.
func NewUTXOValueFromReader(r io.Reader) (*UTXOValue, error) {
	// Read the marker
	marker := make([]byte, 1)
	if _, err := io.ReadFull(r, marker); err != nil {
		return nil, err
	}

	if marker[0] == 0x00 {
		return nil, nil
	}

	u := new(UTXOValue)

	// Read the value
	if err := binary.Read(r, binary.LittleEndian, &u.Value); err != nil {
		return nil, err
	}

	// if u.Value != 100 {
	// 	return nil, errors.NewError("invalid value, expected %d, got %d", 100, u.Value)
	// }

	// Read the locktime
	if err := binary.Read(r, binary.LittleEndian, &u.Locktime); err != nil {
		return nil, err
	}

	// if u.Locktime != 0 {
	// 	return nil, errors.NewError("invalid locktime, expected %d, got %d", 0, u.Locktime)
	// }

	// Read the script length
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	// if length != 25 {
	// 	return nil, errors.NewError("invalid script length: %d", length)
	// }

	// length is read straight from r and is untrusted. A truncated or crafted
	// record can declare a script length of up to 4 GB with no bytes behind
	// it; do NOT do make([]byte, length) on that. Mirror
	// services/utxopersister.NewUTXOFromReader: small scripts get one
	// exactly-sized allocation, but a large declared length is streamed via
	// io.CopyN (bytes.Buffer.ReadFrom over an io.LimitReader), which grows to
	// the bytes actually present and stops at EOF rather than pre-allocating
	// the claimed size.
	if length <= maxExactScriptAlloc {
		u.Script = make([]byte, length)
		if _, err := io.ReadFull(r, u.Script); err != nil {
			return nil, err
		}

		return u, nil
	}

	var script bytes.Buffer
	if _, err := io.CopyN(&script, r, int64(length)); err != nil {
		return nil, errors.NewProcessingError("error reading utxo script (%d bytes)", length, err)
	}

	u.Script = script.Bytes()

	// expected, _ := hex.DecodeString("76a914df2fd021d3db1504cca2d63e082665171bda420288ac")
	// if !bytes.Equal(u.Script, expected) {
	// 	return nil, errors.NewError("invalid script: %x", u.Script)
	// }

	return u, nil
}

// Bytes returns the byte representation of the UTXOValue.
func (u *UTXOValue) Bytes() []byte {
	if u == nil {
		return nil
	}

	b := make([]byte, 8+4+4+len(u.Script))

	// Write the value to the first 8 bytes
	binary.LittleEndian.PutUint64(b[:8], u.Value)

	// Write the locktime to the next 4 bytes
	binary.LittleEndian.PutUint32(b[8:12], u.Locktime)

	// Write the script length to the next 4 bytes
	binary.LittleEndian.PutUint32(b[12:16], uint32(len(u.Script)))

	// Write the script to the remaining bytes
	copy(b[16:], u.Script)

	return b
}

// Write writes the UTXOValue to an io.Writer.
func (u *UTXOValue) Write(w io.Writer) error {
	if u == nil {
		if _, err := w.Write([]byte{0x00}); err != nil {
			return errors.NewProcessingError("error writing nil marker", err)
		}

		return nil
	}

	if _, err := w.Write([]byte{0x01}); err != nil {
		return errors.NewProcessingError("error writing not nil marker", err)
	}

	// Write the value
	if err := binary.Write(w, binary.LittleEndian, u.Value); err != nil {
		return errors.NewProcessingError("error writing value", err)
	}

	// Write the locktime
	if err := binary.Write(w, binary.LittleEndian, u.Locktime); err != nil {
		return errors.NewProcessingError("error writing locktime", err)
	}

	// Write the length of the script
	if err := binary.Write(w, binary.LittleEndian, uint32(len(u.Script))); err != nil {
		return errors.NewProcessingError("error writing script length", err)
	}

	// Write the script
	if _, err := w.Write(u.Script); err != nil {
		return errors.NewProcessingError("error writing script", err)
	}

	return nil
}

// Equal compares this UTXOValue with another for equality.
func (u *UTXOValue) Equal(other *UTXOValue) bool {
	return u.Value == other.Value && u.Locktime == other.Locktime && bytes.Equal(u.Script, other.Script)
}

// String returns a string representation of the UTXOValue.
func (u *UTXOValue) String() string {
	return fmt.Sprintf("%10d / %8d - %x", u.Value, u.Locktime, u.Script)
}
