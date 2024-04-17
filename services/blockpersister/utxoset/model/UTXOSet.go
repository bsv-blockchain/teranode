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

// UTXOSet is a map of UTXOs.
type UTXOSet struct {
	blockHash   chainhash.Hash // This is the block hash that is the last block in the chain with these UTXOs.
	blockHeight uint32
	m           UTXOMap
}

// NewUTXOMap creates a new UTXOMap.
func NewUTXOSet(blockHash *chainhash.Hash, blockHeight uint32) *UTXOSet {
	return &UTXOSet{
		blockHash:   *blockHash,
		blockHeight: blockHeight,
		m:           newUTXOMap(),
	}
}

// Add adds a UTXO to the map.
func (us *UTXOSet) Add(txID chainhash.Hash, index uint32, value uint64, locktime uint32, script []byte) {
	uk := NewUTXOKey(txID, index)
	uv := NewUTXOValue(value, locktime, script)

	us.m.Put(*uk, uv)
}

func (us *UTXOSet) Get(txID chainhash.Hash, index uint32) (*UTXOValue, bool) {
	uk := NewUTXOKey(txID, index)

	return us.m.Get(*uk)
}

func (us *UTXOSet) Delete(txID chainhash.Hash, index uint32) {
	uk := NewUTXOKey(txID, index)

	us.m.Delete(*uk)
}

func NewUTXOSetFromReader(r io.Reader) (*UTXOSet, error) {
	blockHash := new(chainhash.Hash)

	if _, err := r.Read(blockHash[:]); err != nil {
		return nil, fmt.Errorf("error reading block hash: %w", err)
	}

	var blockHeight uint32
	if err := binary.Read(r, binary.LittleEndian, &blockHeight); err != nil {
		return nil, fmt.Errorf("error reading block height: %w", err)
	}

	us := NewUTXOSet(blockHash, blockHeight)

	if err := us.m.Read(r); err != nil {
		return nil, err
	}

	return us, nil
}

func (us *UTXOSet) Persist(folder string) error {
	filename := path.Join(folder, fmt.Sprintf("%s_%d.utxoset", us.blockHash.String(), us.blockHeight))
	f, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer f.Close()

	// Create a buffered writer
	w := bufio.NewWriter(f)

	return us.Write(w)
}

func (us *UTXOSet) Write(w io.Writer) error {
	if _, err := w.Write(us.blockHash[:]); err != nil {
		return fmt.Errorf("error writing block hash: %w", err)
	}

	// Write the block height
	if err := binary.Write(w, binary.LittleEndian, us.blockHeight); err != nil {
		return fmt.Errorf("error writing block height: %w", err)
	}

	// Write the number of UTXOs
	if err := binary.Write(w, binary.LittleEndian, uint32(us.m.Length())); err != nil {
		return fmt.Errorf("error writing number of UTXOs: %w", err)
	}

	us.m.Iter(func(uk UTXOKey, uv *UTXOValue) (stop bool) {
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
