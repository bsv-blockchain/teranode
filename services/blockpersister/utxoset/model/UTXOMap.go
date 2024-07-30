package model

import (
	"encoding/binary"
	"github.com/bitcoin-sv/ubsv/errors"
	"io"
)

// UTXOMap represents a bitcoin transaction outpoint, i.e. the transaction ID and the output index.
// It is used as a key in the UTXOMap and is hashable.
type UTXOMap struct {
	genericMap[UTXOKey, *UTXOValue]
}

func newUTXOMap() UTXOMap {
	return UTXOMap{
		NewSplitSwissMap[UTXOKey, *UTXOValue](1024),
	}
}

func (um *UTXOMap) Read(r io.Reader) error {
	// Read the number of UTXOs
	var num uint32
	if err := binary.Read(r, binary.LittleEndian, &num); err != nil {
		return errors.NewProcessingError("error reading number of UTXOs", err)
	}

	for i := uint32(0); i < num; i++ {
		key, err := NewUTXOKeyFromReader(r)
		if err != nil {
			return errors.NewProcessingError("Error reading record key %d", i, err)
		}

		val, err := NewUTXOValueFromReader(r)
		if err != nil {
			return errors.NewProcessingError("Error reading a value record %d", i, err)
		}

		um.Put(*key, val)
	}

	return nil
}

func (um *UTXOMap) Write(w io.Writer) error {
	// Write the number of UTXOs
	num := uint32(um.Length())

	if err := binary.Write(w, binary.LittleEndian, num); err != nil {
		return errors.NewProcessingError("error writing number of UTXOs", err)
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
		return errors.NewProcessingError("Failed to write UTXO map", err)
	}

	if count != um.Length() {
		return errors.NewProcessingError("Failed to write all UTXOs: %d != %d", count, um.Length())
	}

	return nil
}
