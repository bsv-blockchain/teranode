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
			return fmt.Errorf("Error reading record key %d: %w", i, err)
		}

		val, err := NewUTXOValueFromReader(r)
		if err != nil {
			return fmt.Errorf("Error reading record value %d: %w", i, err)
		}

		um.Put(*key, val)
	}

	return nil
}

func (um *UTXOMap) Write(w io.Writer) error {
	// Write the number of UTXOs
	num := uint32(um.Length())

	if err := binary.Write(w, binary.LittleEndian, num); err != nil {
		return fmt.Errorf("error writing number of UTXOs: %w", err)
	}

	var err error
	var count int

	// Write each UTXO
	um.Iter(func(key UTXOKey, val *UTXOValue) (stop bool) {
		if err = key.Write(w); err != nil {
			stop = true
			return
		}

		if err = val.Write(w); err != nil {
			stop = true
			return
		}

		count++
		return
	})

	if err != nil {
		return fmt.Errorf("Failed to write UTXO map: %w", err)
	}

	if count != um.Length() {
		return fmt.Errorf("Failed to write all UTXOs: %d != %d", count, um.Length())
	}

	return nil
}
