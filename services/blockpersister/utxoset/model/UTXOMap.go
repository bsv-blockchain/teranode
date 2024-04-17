package model

import (
	"encoding/binary"
	"fmt"
	"io"
)

// UTXOKey represents a bitcoin transaction outpoint, i.e. the transaction ID and the output index.
// It is used as a key in the UTXOMap and is hashable.
type UTXOMap struct {
	genericMap[UTXOKey, *UTXOValue]
}

func newUTXOMap() UTXOMap {
	return UTXOMap{
		newSplitSwissMap[UTXOKey, *UTXOValue](1024),
	}
}

func (um *UTXOMap) Read(r io.Reader) error {
	// Read the number of UTXOs
	var num uint32
	if err := binary.Read(r, binary.LittleEndian, &num); err != nil {
		return fmt.Errorf("error reading number of UTXOs: %w", err)
	}

	for i := uint32(0); i < num; i++ {
		key, err := NewUTXOKeyFromReader(r)
		if err != nil {
			return err
		}

		val, err := NewUTXOValueFromReader(r)
		if err != nil {
			return err
		}

		um.Put(*key, val)
	}

	return nil
}
