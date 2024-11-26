package model

import (
	"io"

	"github.com/bitcoin-sv/ubsv/errors"
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
		return nil, errors.NewStorageError("error reading block hash", err)
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

func (ud *UTXODiff) Write(w io.Writer) error {
	if _, err := w.Write(ud.BlockHash[:]); err != nil {
		return errors.NewProcessingError("error writing block hash", err)
	}

	if err := ud.Removed.Write(w); err != nil {
		return errors.NewProcessingError("error writing removed UTXOs", err)
	}

	if err := ud.Added.Write(w); err != nil {
		return errors.NewProcessingError("error writing added UTXOs", err)
	}

	return nil
}

/* Trim removes any UTXOs that are in both the Added and Removed maps. */
/* This can occur when processing multiple subtrees in parallel. */
func (ud *UTXODiff) Trim() {
	var keysToDelete []UTXOKey

	ud.Added.Iter(func(uk UTXOKey, uv *UTXOValue) (stop bool) {
		if ud.Removed.Exists(uk) {
			keysToDelete = append(keysToDelete, uk)
		}

		return false
	})

	for _, uk := range keysToDelete {
		ud.Added.Delete(uk)
		ud.Removed.Delete(uk)
	}
}
