package sql

import (
	"context"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/libsv/go-bt/v2/chainhash"
)

// LocateBlockHashes returns the hashes of the blocks after the first known block in
// the locator until the provided stop hash is reached, or up to the provided
// max number of block hashes.
//
// In addition, there are two special cases:
//   - When no locators are provided, the stop hash is treated as a request for
//     that block, so it will either return the stop hash itself if it is known,
//     or nil if it is unknown
//   - When locators are provided, but none of them are known, hashes starting
//     after the genesis block will be returned
func (s *SQL) LocateBlockHashes(ctx context.Context, locator []*chainhash.Hash, hashStop *chainhash.Hash,
	maxHashes uint32) ([]*chainhash.Hash, error) {

	if maxHashes == 0 {
		return nil, errors.New(errors.ERR_INVALID_ARGUMENT, "maxHashes must be greater than 0")
	}

	var foundBlock *chainhash.Hash
	if len(locator) == 0 {
		foundBlock = hashStop
	} else {
		for _, hash := range locator {
			_, err := s.GetBlockExists(ctx, hash)
			if err != nil {
				continue
			}
			foundBlock = hash
			break
		}
	}

	if foundBlock == nil {
		// if no locators are known, start from the genesis block
		foundBlock = model.GenesisBlockHeader.Hash()
	}

	// get the headers starting from the found block
	blockHeaders, _, err := s.GetBlockHeaders(ctx, foundBlock, uint64(maxHashes))
	if err != nil {
		return nil, err
	}
	hashes := make([]*chainhash.Hash, 0, len(blockHeaders))
	for _, header := range blockHeaders {
		hashes = append(hashes, header.Hash())
		if hashStop != nil && header.Hash().IsEqual(hashStop) {
			break
		}
	}

	return hashes, nil
}
