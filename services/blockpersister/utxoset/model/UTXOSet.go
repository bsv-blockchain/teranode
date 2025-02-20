package model

import (
	"encoding/binary"
	"io"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2/chainhash"
)

// UTXOSet represents a complete set of UTXOs at a specific block.
type UTXOSet struct {
	// logger provides logging functionality
	logger ulogger.Logger
	// BlockHash is the hash of the block this UTXO set represents
	BlockHash chainhash.Hash
	// Current contains the current set of UTXOs
	Current UTXOMap
}

// NewUTXOSet creates a new UTXOSet for a specific block.
func NewUTXOSet(logger ulogger.Logger, blockHash *chainhash.Hash) *UTXOSet {
	return &UTXOSet{
		logger:    logger,
		BlockHash: *blockHash,
		Current:   newUTXOMap(),
	}
}

// NewUTXOSetFromPrevious creates a new UTXOSet based on a previous set.
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

// Get retrieves a UTXO from the set.
func (us *UTXOSet) Get(uk UTXOKey) (*UTXOValue, bool) {
	return us.Current.Get(uk)
}

// Delete removes a UTXO from the set.
func (us *UTXOSet) Delete(uk UTXOKey) {
	us.Current.Delete(uk)
}

// NewUTXOSetFromReader creates a UTXOSet by reading from an io.Reader.
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

// Write writes the UTXOSet to an io.Writer.
func (us *UTXOSet) Write(w io.Writer) error {
	var err error

	if _, err = w.Write(us.BlockHash[:]); err != nil {
		return errors.NewProcessingError("error writing block hash", err)
	}

	var lengthUint32 uint32

	lengthUint32, err = util.SafeIntToUint32(us.Current.Length())
	if err != nil {
		return err
	}

	// Write the number of UTXOs
	if err = binary.Write(w, binary.LittleEndian, lengthUint32); err != nil {
		return errors.NewProcessingError("error writing number of UTXOs", err)
	}

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
