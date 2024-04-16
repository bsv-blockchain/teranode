package model

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
	u := new(UTXOValue)

	// Read the value
	if err := binary.Read(r, binary.LittleEndian, &u.Value); err != nil {
		return nil, err
	}

	// Read the locktime
	if err := binary.Read(r, binary.LittleEndian, &u.Locktime); err != nil {
		return nil, err
	}

	// Read the script length
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	u.Script = make([]byte, length)
	if _, err := r.Read(u.Script); err != nil {
		return nil, err
	}

	return u, nil
}

func (u *UTXOValue) Bytes() []byte {
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
	// Write the value
	if err := binary.Write(w, binary.LittleEndian, u.Value); err != nil {
		return fmt.Errorf("error writing value: %w", err)
	}

	// Write the locktime
	if err := binary.Write(w, binary.LittleEndian, u.Locktime); err != nil {
		return fmt.Errorf("error writing locktime: %w", err)
	}

	// Write the length of the script
	if err := binary.Write(w, binary.LittleEndian, uint32(len(u.Script))); err != nil {
		return fmt.Errorf("error writing script length: %w", err)
	}

	// Write the script
	if _, err := w.Write(u.Script); err != nil {
		return fmt.Errorf("error writing script: %w", err)
	}

	return nil
}

func (u *UTXOValue) Equal(other *UTXOValue) bool {
	return u.Value == other.Value && u.Locktime == other.Locktime && bytes.Equal(u.Script, other.Script)
}
