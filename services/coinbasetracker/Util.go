package coinbasetracker

import (
	"errors"

	smodel "github.com/TAAL-GmbH/ubsv/db/model"
	nmodel "github.com/TAAL-GmbH/ubsv/model"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

func IsGenesisBlock(block *smodel.Block) bool {
	if block == nil {
		return false
	}
	previousBlock := &chainhash.Hash{}
	// merkleRoot := "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"
	// Note: genesis block hash is different from merkleRoot
	// Need to know the genesis block hash.
	return block.Height == 0 &&
		// block.BlockHash == merkleRoot &&
		block.PrevBlockHash == previousBlock.String()
}

func NetworkBlockToStoreBlock(nblock *nmodel.Block) (*smodel.Block, error) {
	if nblock == nil {
		return nil, errors.New("nil network block")
	}
	return &smodel.Block{
		// TODO: Height is missing. How can we know the height?
		BlockHash:     nblock.Hash().String(),
		PrevBlockHash: nblock.Header.HashPrevBlock.String(),
	}, nil
}
