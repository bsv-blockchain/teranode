package catchup

import (
	"context"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/services/blockchain"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// CommonAncestorFinderWithLocator wraps the catchup.CommonAncestorFinder with blockchain client functionality
type CommonAncestorFinderWithLocator struct {
	finder           *CommonAncestorFinder
	blockchainClient blockchain.ClientI
	logger           ulogger.Logger
	locatorHashes    []*chainhash.Hash
}

// NewCommonAncestorFinderWithLocator creates a new common ancestor finder with blockchain client
func NewCommonAncestorFinderWithLocator(blockchainClient blockchain.ClientI, logger ulogger.Logger, locatorHashes []*chainhash.Hash) *CommonAncestorFinderWithLocator {
	// Create a lookup function that uses the blockchain client
	lookupFunc := func(hash *chainhash.Hash) (*model.BlockHeader, bool) {
		ctx := context.Background()
		header, _, err := blockchainClient.GetBlockHeader(ctx, hash)
		if err != nil {
			return nil, false
		}

		return header, true
	}

	finder := NewCommonAncestorFinder(lookupFunc)

	return &CommonAncestorFinderWithLocator{
		finder:           finder,
		blockchainClient: blockchainClient,
		logger:           logger,
		locatorHashes:    locatorHashes,
	}
}

// FindCommonAncestorWithForkDepth finds the common ancestor and calculates fork depth
func (caf *CommonAncestorFinderWithLocator) FindCommonAncestorWithForkDepth(ctx context.Context, remoteHeaders []*model.BlockHeader, currentHeight uint32) (*chainhash.Hash, *model.BlockHeaderMeta, uint32, error) {
	// Debug logging
	caf.logger.Debugf("[common-ancestor] Looking for common ancestor with %d locator hashes and %d remote headers", len(caf.locatorHashes), len(remoteHeaders))

	if len(caf.locatorHashes) > 0 && len(remoteHeaders) > 0 {
		caf.logger.Debugf("[common-ancestor] First locator hash: %s", caf.locatorHashes[0].String())
		caf.logger.Debugf("[common-ancestor] First remote header hash: %s", remoteHeaders[0].Hash().String())

		if len(remoteHeaders) > 1 {
			caf.logger.Debugf("[common-ancestor] Last remote header hash: %s", remoteHeaders[len(remoteHeaders)-1].Hash().String())
		}
	}

	// Use the wrapped finder to find the common ancestor
	ancestor, index, err := caf.finder.FindCommonAncestor(caf.locatorHashes, remoteHeaders)
	if err != nil {
		caf.logger.Debugf("[common-ancestor] Failed to find common ancestor: %v", err)
		return nil, nil, 0, err
	}

	// Get metadata for the common ancestor
	_, meta, err := caf.blockchainClient.GetBlockHeader(ctx, ancestor.Hash())
	if err != nil {
		return nil, nil, 0, errors.NewProcessingError("failed to get metadata for common ancestor: %v", err)
	}

	// Calculate fork depth
	forkDepth := uint32(0)
	if meta != nil && meta.Height < currentHeight {
		forkDepth = currentHeight - meta.Height
	}

	// Log the result
	caf.logger.Debugf("[common-ancestor] found at height %d (index %d in remote headers), fork depth: %d", meta.Height, index, forkDepth)

	return ancestor.Hash(), meta, forkDepth, nil
}
