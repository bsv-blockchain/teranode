package model

import (
	"encoding/binary"
	"io"

	"github.com/bitcoin-sv/ubsv/errors"

	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
)

// UTXOSet is a map of UTXOs.
type UTXOSet struct {
	logger    ulogger.Logger
	BlockHash chainhash.Hash // This is the block hash that is the last block in the chain with these UTXOs.
	Current   UTXOMap
}

// NewUTXOMap creates a new UTXOMap.
func NewUTXOSet(logger ulogger.Logger, blockHash *chainhash.Hash) *UTXOSet {
	return &UTXOSet{
		logger:    logger,
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

func NewUTXOSetFromReader(logger ulogger.Logger, r io.Reader) (*UTXOSet, error) {
	blockHash := new(chainhash.Hash)

	if _, err := io.ReadFull(r, blockHash[:]); err != nil {
		return nil, errors.NewProcessingError("error reading block hash", err)
	}

	us := NewUTXOSet(logger, blockHash)

	if err := us.Current.Read(r); err != nil {
		return nil, err
	}

	return us, nil
}

func (us *UTXOSet) Write(w io.Writer) error {
	if _, err := w.Write(us.BlockHash[:]); err != nil {
		return errors.NewProcessingError("error writing block hash", err)
	}

	// Write the number of UTXOs
	// nolint:gosec
	if err := binary.Write(w, binary.LittleEndian, uint32(us.Current.Length())); err != nil {
		return errors.NewProcessingError("error writing number of UTXOs", err)
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
		return errors.NewProcessingError("failed to write UTXO set", err)
	}

	return nil
}
