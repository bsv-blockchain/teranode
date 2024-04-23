package model

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// UTXODiff is a map of UTXOs.
type UTXODiff struct {
	logger    ulogger.Logger
	BlockHash chainhash.Hash // This is the block hash that is the last block in the chain with these UTXOs.
	Added     UTXOMap
	Removed   UTXOMap
}

// NewUTXOMap creates a new UTXODiff.
func NewUTXODiff(logger ulogger.Logger, blockHash *chainhash.Hash) *UTXODiff {
	return &UTXODiff{
		logger:    logger,
		BlockHash: *blockHash,
		Added:     newUTXOMap(),
		Removed:   newUTXOMap(),
	}
}

// Add adds a UTXO to the map.
func (ud *UTXODiff) Add(txID chainhash.Hash, index uint32, value uint64, locktime uint32, script []byte) {
	uk := NewUTXOKey(txID, index)
	uv := NewUTXOValue(value, locktime, script)

	ud.Added.Put(uk, uv)
}

func (ud *UTXODiff) Delete(txID chainhash.Hash, index uint32) {
	uk := NewUTXOKey(txID, index)

	if ud.Added.Exists(uk) {
		ud.Added.Delete(uk)
	} else {
		ud.Removed.Put(uk, nil)
	}
}

func (ud *UTXODiff) ProcessTx(tx *bt.Tx) {
	if !tx.IsCoinbase() {
		for _, input := range tx.Inputs {
			ud.Delete(*input.PreviousTxIDChainHash(), input.PreviousTxOutIndex)
		}
	}

	for i, output := range tx.Outputs {
		if output.LockingScript.IsData() {
			continue
		}

		ud.Add(*tx.TxIDChainHash(), uint32(i), output.Satoshis, tx.LockTime, *output.LockingScript)
	}
}

func NewUTXODiffFromReader(logger ulogger.Logger, r io.Reader) (*UTXODiff, error) {
	blockHash := new(chainhash.Hash)

	if _, err := io.ReadFull(r, blockHash[:]); err != nil {
		return nil, fmt.Errorf("error reading block hash: %w", err)
	}

	ud := NewUTXODiff(logger, blockHash)

	if err := ud.Removed.Read(r); err != nil {
		return nil, err
	}

	if err := ud.Added.Read(r); err != nil {
		return nil, err
	}

	return ud, nil
}

func (ud *UTXODiff) Persist(ctx context.Context, store blob.Store) error {
	reader, writer := io.Pipe()

	bufferedWriter := bufio.NewWriter(writer)

	go func() {
		defer func() {
			// Flush the buffer and close the writer with error handling
			if err := bufferedWriter.Flush(); err != nil {
				ud.logger.Errorf("error flushing writer: %v", err)
			}

			if err := writer.CloseWithError(nil); err != nil {
				ud.logger.Errorf("error closing writer: %v", err)
			}
		}()

		if err := ud.Write(bufferedWriter); err != nil {
			ud.logger.Errorf("error writing UTXO diff: %v", err)
			writer.CloseWithError(err)
			return
		}
	}()

	return store.SetFromReader(ctx, ud.BlockHash[:], reader, options.WithFileExtension("utxodiff"), options.WithPrefixDirectory(10))
}

func (ud *UTXODiff) Write(w io.Writer) error {
	if _, err := w.Write(ud.BlockHash[:]); err != nil {
		return fmt.Errorf("error writing block hash: %w", err)
	}

	if err := ud.Removed.Write(w); err != nil {
		return fmt.Errorf("error writing removed UTXOs: %w", err)
	}

	if err := ud.Added.Write(w); err != nil {
		return fmt.Errorf("error writing added UTXOs: %w", err)
	}

	return nil
}
