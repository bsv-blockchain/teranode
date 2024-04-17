package model

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/libsv/go-bt/v2/chainhash"
)

// UTXODiff is a map of UTXOs.
type UTXODiff struct {
	blockHash   chainhash.Hash // This is the block hash that is the last block in the chain with these UTXOs.
	blockHeight uint32
	added       UTXOMap
	removed     UTXOMap
}

// NewUTXOMap creates a new UTXODiff.
func NewUTXODiff(blockHash *chainhash.Hash, blockHeight uint32) *UTXODiff {
	return &UTXODiff{
		blockHash:   *blockHash,
		blockHeight: blockHeight,
		added:       newUTXOMap(),
		removed:     newUTXOMap(),
	}
}

// Add adds a UTXO to the map.
func (us *UTXODiff) Add(txID chainhash.Hash, index uint32, value uint64, locktime uint32, script []byte) {
	uk := NewUTXOKey(txID, index)
	uv := NewUTXOValue(value, locktime, script)

	us.added.Put(*uk, uv)
}

func (us *UTXODiff) Delete(txID chainhash.Hash, index uint32) {
	uk := NewUTXOKey(txID, index)

	if us.added.Exists(*uk) {
		us.added.Delete(*uk)
	} else {
		us.removed.Put(*uk, nil)
	}
}

func NewUTXODiffFromReader(r io.Reader) (*UTXODiff, error) {
	blockHash := new(chainhash.Hash)

	if _, err := r.Read(blockHash[:]); err != nil {
		return nil, fmt.Errorf("error reading block hash: %w", err)
	}

	var blockHeight uint32
	if err := binary.Read(r, binary.LittleEndian, &blockHeight); err != nil {
		return nil, fmt.Errorf("error reading block height: %w", err)
	}

	us := NewUTXODiff(blockHash, blockHeight)

	if err := us.removed.Read(r); err != nil {
		return nil, err
	}

	if err := us.added.Read(r); err != nil {
		return nil, err
	}

	return us, nil
}

func (us *UTXODiff) Persist(folder string) error {
	filename := path.Join(folder, fmt.Sprintf("%s_%d.utxodiff", us.blockHash.String(), us.blockHeight))
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer f.Close()

	// Create a buffered writer
	w := bufio.NewWriter(f)

	return us.Write(w)
}

func (us *UTXODiff) Write(w io.Writer) error {
	if _, err := w.Write(us.blockHash[:]); err != nil {
		return fmt.Errorf("error writing block hash: %w", err)
	}

	// Write the block height
	if err := binary.Write(w, binary.LittleEndian, us.blockHeight); err != nil {
		return fmt.Errorf("error writing block height: %w", err)
	}

	// Write the number of removed UTXOs
	if err := binary.Write(w, binary.LittleEndian, uint32(us.removed.Length())); err != nil {
		return fmt.Errorf("error writing number of removed UTXOs: %w", err)
	}

	us.removed.Iter(func(uk UTXOKey, uv *UTXOValue) (stop bool) {
		if err := uk.Write(w); err != nil {
			stop = true
			return
		}

		if err := uv.Write(w); err != nil {
			stop = true
			return
		}

		return
	})

	// Write the number of added UTXOs
	if err := binary.Write(w, binary.LittleEndian, uint32(us.added.Length())); err != nil {
		return fmt.Errorf("error writing number of added UTXOs: %w", err)
	}

	us.added.Iter(func(uk UTXOKey, uv *UTXOValue) (stop bool) {
		if err := uk.Write(w); err != nil {
			stop = true
			return
		}

		if err := uv.Write(w); err != nil {
			stop = true
			return
		}

		return
	})

	return nil
}
