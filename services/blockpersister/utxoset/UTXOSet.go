package utxoset

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset/model"
	"github.com/libsv/go-bt/v2/chainhash"
)

// UTXOMap is a map of UTXOs.
type UTXOMap struct {
	blockHash chainhash.Hash
	m         map[model.Outpoint]*model.UTXO
}

// NewUTXOMap creates a new UTXOMap.
func NewUTXOMap(blockHash *chainhash.Hash) *UTXOMap {
	return &UTXOMap{
		blockHash: *blockHash,
		m:         make(map[model.Outpoint]*model.UTXO),
	}
}

// Add adds a UTXO to the map.
func (um *UTXOMap) Add(txID chainhash.Hash, index uint32, value uint64, locktime uint32, script []byte) {
	outpoint := model.NewOutpoint(txID, index)
	utxo := model.NewUTXO(value, locktime, script)

	um.m[*outpoint] = utxo
}

func (um *UTXOMap) Get(txID chainhash.Hash, index uint32) *model.UTXO {
	outpoint := model.NewOutpoint(txID, index)

	return um.m[*outpoint]
}

func (um *UTXOMap) Delete(txID chainhash.Hash, index uint32) {
	outpoint := model.NewOutpoint(txID, index)

	delete(um.m, *outpoint)
}

func NewUTXOMapFromReader(r io.Reader) (*UTXOMap, error) {
	um := new(UTXOMap)
	um.m = make(map[model.Outpoint]*model.UTXO)

	if _, err := r.Read(um.blockHash[:]); err != nil {
		return nil, fmt.Errorf("error reading block hash: %w", err)
	}

	// Read the number of UTXOs
	var num uint32
	if err := binary.Read(r, binary.LittleEndian, &num); err != nil {
		return nil, fmt.Errorf("error reading number of UTXOs: %w", err)
	}

	for i := uint32(0); i < num; i++ {
		outpoint, err := model.NewOutpointFromReader(r)
		if err != nil {
			return nil, err
		}

		utxo, err := model.NewUTXOFromReader(r)
		if err != nil {
			return nil, err
		}

		um.m[*outpoint] = utxo
	}

	return um, nil
}

func (um *UTXOMap) Write(w io.Writer) error {
	if _, err := w.Write(um.blockHash[:]); err != nil {
		return fmt.Errorf("error writing block hash: %w", err)
	}

	// Write the number of UTXOs
	if err := binary.Write(w, binary.LittleEndian, uint32(len(um.m))); err != nil {
		return fmt.Errorf("error writing number of UTXOs: %w", err)
	}

	for outpoint, utxo := range um.m {
		if err := outpoint.Write(w); err != nil {
			return err
		}
		if err := utxo.Write(w); err != nil {
			return err
		}
	}
	return nil
}

// UTXOMapHolder is a map of UTXOMaps (forks).  The key of the map is the block hash of the block that the UTXOMap is for.
type UTXOMapHolder map[chainhash.Hash]UTXOMap
