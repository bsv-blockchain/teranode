package model

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// UTXODiff is a map of UTXOs.
type UTXODiff struct {
	BlockHash chainhash.Hash // This is the block hash that is the last block in the chain with these UTXOs.
	Added     UTXOMap
	Removed   UTXOMap
}

// NewUTXOMap creates a new UTXODiff.
func NewUTXODiff(blockHash *chainhash.Hash) *UTXODiff {
	return &UTXODiff{
		BlockHash: *blockHash,
		Added:     newUTXOMap(),
		Removed:   newUTXOMap(),
	}
}

// Add adds a UTXO to the map.
func (us *UTXODiff) Add(txID chainhash.Hash, index uint32, value uint64, locktime uint32, script []byte) {
	uk := NewUTXOKey(txID, index)
	uv := NewUTXOValue(value, locktime, script)

	us.Added.Put(uk, uv)
}

func (us *UTXODiff) Delete(txID chainhash.Hash, index uint32) {
	uk := NewUTXOKey(txID, index)

	if us.Added.Exists(uk) {
		us.Added.Delete(uk)
	} else {
		us.Removed.Put(uk, nil)
	}
}

func (us *UTXODiff) ProcessTx(tx *bt.Tx) {
	if !tx.IsCoinbase() {
		for _, input := range tx.Inputs {
			us.Delete(*input.PreviousTxIDChainHash(), input.PreviousTxOutIndex)
		}
	}

	for i, output := range tx.Outputs {
		if output.LockingScript.IsData() {
			continue
		}

		us.Add(*tx.TxIDChainHash(), uint32(i), output.Satoshis, tx.LockTime, *output.LockingScript)
	}
}

func NewUTXODiffFromReader(r io.Reader) (*UTXODiff, error) {
	blockHash := new(chainhash.Hash)

	if _, err := io.ReadFull(r, blockHash[:]); err != nil {
		return nil, fmt.Errorf("error reading block hash: %w", err)
	}

	us := NewUTXODiff(blockHash)

	if err := us.Removed.Read(r); err != nil {
		return nil, err
	}

	if err := us.Added.Read(r); err != nil {
		return nil, err
	}

	return us, nil
}

func (us *UTXODiff) Persist(filename string) error {
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
	defer func() {
		_ = w.Flush()
	}()

	if err := us.Write(w); err != nil {
		return err
	}

	return nil
}

func (us *UTXODiff) Write(w io.Writer) error {
	if _, err := w.Write(us.BlockHash[:]); err != nil {
		return fmt.Errorf("error writing block hash: %w", err)
	}

	if err := us.Removed.Write(w); err != nil {
		return fmt.Errorf("error writing removed UTXOs: %w", err)
	}

	if err := us.Added.Write(w); err != nil {
		return fmt.Errorf("error writing added UTXOs: %w", err)
	}

	return nil
}
