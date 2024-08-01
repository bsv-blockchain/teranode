package model

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/bitcoin-sv/ubsv/errors"
	"io"
)

type UTXOValue struct {
	Value    uint64
	Locktime uint32
	Script   []byte
}

func NewUTXOValue(value uint64, locktime uint32, script []byte) *UTXOValue {
	return &UTXOValue{
		Value:    value,
		Locktime: locktime,
		Script:   script,
	}
}

func NewUTXOValueFromBytes(b []byte) *UTXOValue {
	u := new(UTXOValue)

	u.Value = binary.LittleEndian.Uint64(b[:8])
	u.Locktime = binary.LittleEndian.Uint32(b[8:12])

	// Read the script length from the next 4 bytes
	length := binary.LittleEndian.Uint32(b[12:16])

	u.Script = b[16 : 16+length]

	return u
}

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

	u.Script = make([]byte, length)
	if _, err := io.ReadFull(r, u.Script); err != nil {
		return nil, err
	}

	// expected, _ := hex.DecodeString("76a914df2fd021d3db1504cca2d63e082665171bda420288ac")
	// if !bytes.Equal(u.Script, expected) {
	// 	return nil, errors.NewError("invalid script: %x", u.Script)
	// }

	return u, nil
}

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

func (u *UTXOValue) Equal(other *UTXOValue) bool {
	return u.Value == other.Value && u.Locktime == other.Locktime && bytes.Equal(u.Script, other.Script)
}

func (u *UTXOValue) String() string {
	return fmt.Sprintf("%10d / %8d - %x", u.Value, u.Locktime, u.Script)
}
