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
	BlockHash chainhash.Hash // This is the block hash that is the last block in the chain with these UTXOs.
	Current   UTXOMap
}

// NewUTXOMap creates a new UTXOMap.
func NewUTXOSet(blockHash *chainhash.Hash) *UTXOSet {
	return &UTXOSet{
		BlockHash: *blockHash,
		Current:   newUTXOMap(),
	}
}

func NewUTXOSetFromPrevious(blockHash *chainhash.Hash, previousUTXOSet *UTXOSet) *UTXOSet {
	us := &UTXOSet{
		BlockHash: *blockHash,
		Current:   newUTXOMap(),
	}

	previousUTXOSet.Current.Iter(func(uk UTXOKey, uv *UTXOValue) (stop bool) {
		us.Current.Put(uk, uv)
		return
	})

	return us
}

// Add adds a UTXO to the map.
func (us *UTXOSet) Add(uk UTXOKey, uv *UTXOValue) {
	us.Current.Put(uk, uv)
}

func (us *UTXOSet) Get(uk UTXOKey) (*UTXOValue, bool) {
	return us.Current.Get(uk)
}

func (us *UTXOSet) Delete(uk UTXOKey) {
	us.Current.Delete(uk)
}

func NewUTXOSetFromReader(r io.Reader) (*UTXOSet, error) {
	blockHash := new(chainhash.Hash)

	if _, err := io.ReadFull(r, blockHash[:]); err != nil {
		return nil, fmt.Errorf("error reading block hash: %w", err)
	}

	us := NewUTXOSet(blockHash)

	if err := us.Current.Read(r); err != nil {
		return nil, err
	}

	return us, nil
}

func LoadUTXOSet(folder string, blockHash chainhash.Hash) (*UTXOSet, error) {
	// Load the UTXO set from disk
	filename := path.Join(folder, fmt.Sprintf("%s.utxoset", blockHash.String()))

	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer f.Close()

	// Use a buffered reader
	r := bufio.NewReader(f)

	return NewUTXOSetFromReader(r)
}

func (us *UTXOSet) Persist(filename string) error {
	var err error
	var f *os.File

	tmpFilename := filename + ".tmp"

	f, err = os.Create(tmpFilename)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}

	defer func() {
		_ = f.Close()

		if err != nil {
			_ = os.Rename(tmpFilename, filename+".error")
		} else {
			_ = os.Rename(tmpFilename, filename)
		}
	}()

	// Create a buffered writer
	w := bufio.NewWriter(f)
	defer w.Flush()

	return us.Write(w)
}

func (us *UTXOSet) Write(w io.Writer) error {
	if _, err := w.Write(us.BlockHash[:]); err != nil {
		return fmt.Errorf("error writing block hash: %w", err)
	}

	// Write the number of UTXOs
	if err := binary.Write(w, binary.LittleEndian, uint32(us.Current.Length())); err != nil {
		return fmt.Errorf("error writing number of UTXOs: %w", err)
	}

	var err error

	us.Current.Iter(func(uk UTXOKey, uv *UTXOValue) (stop bool) {
		if err = uk.Write(w); err != nil {
			stop = true
			return
		}

		if err = uv.Write(w); err != nil {
			stop = true
			return
		}

		return
	})

	if err != nil {
		return fmt.Errorf("Failed to write UTXO set: %w", err)
	}

	return nil
}
