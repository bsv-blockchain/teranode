package catchup

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
)

// CommonAncestorFinder provides methods for finding common ancestors between chains
type CommonAncestorFinder struct {
	// LocalChainLookup is a function that looks up a block header by hash in the local chain
	LocalChainLookup func(hash *chainhash.Hash) (*model.BlockHeader, bool)
}

// NewCommonAncestorFinder creates a new common ancestor finder
func NewCommonAncestorFinder(lookupFunc func(hash *chainhash.Hash) (*model.BlockHeader, bool)) *CommonAncestorFinder {
	return &CommonAncestorFinder{
		LocalChainLookup: lookupFunc,
	}
}

// FindCommonAncestor finds the common ancestor between local chain and remote headers
// It uses the block locator approach similar to Bitcoin's getheaders protocol
func (caf *CommonAncestorFinder) FindCommonAncestor(locatorHashes []*chainhash.Hash, remoteHeaders []*model.BlockHeader) (*model.BlockHeader, int, error) {
	if len(locatorHashes) == 0 || len(remoteHeaders) == 0 {
		return nil, -1, ErrNoCommonAncestor
	}

	// Create a map of remote headers for quick lookup
	remoteMap := make(map[chainhash.Hash]int)
	for i, header := range remoteHeaders {
		remoteMap[*header.Hash()] = i
	}

	// Check each locator hash to see if it exists in remote headers
	for _, hash := range locatorHashes {
		if idx, exists := remoteMap[*hash]; exists {
			// Found common ancestor in remote headers
			return remoteHeaders[idx], idx, nil
		}
	}

	// If no direct match, walk back through the chain from each locator
	for _, hash := range locatorHashes {
		currentHash := hash
		// Walk back through the chain up to a reasonable depth
		for depth := 0; depth < 1000; depth++ {
			// Check if current hash is in remote headers
			if idx, exists := remoteMap[*currentHash]; exists {
				return remoteHeaders[idx], idx, nil
			}

			// Get the header for this hash
			header, exists := caf.LocalChainLookup(currentHash)
			if !exists {
				// Can't continue walking back from this point
				break
			}

			// Move to the previous block
			if header.HashPrevBlock == nil || header.HashPrevBlock.IsEqual(&chainhash.Hash{}) {
				// Reached genesis
				break
			}
			currentHash = header.HashPrevBlock
		}
	}

	// No common ancestor found
	return nil, -1, ErrNoCommonAncestor
}

// FindCommonAncestorByHeaders finds common ancestor by comparing two header chains
func (caf *CommonAncestorFinder) FindCommonAncestorByHeaders(chain1, chain2 []*model.BlockHeader) (*model.BlockHeader, int, int) {
	if len(chain1) == 0 || len(chain2) == 0 {
		return nil, -1, -1
	}

	// Create maps for quick lookup
	chain1Map := make(map[chainhash.Hash]int)
	for i, header := range chain1 {
		chain1Map[*header.Hash()] = i
	}

	// Find first common header
	for i, header := range chain2 {
		if idx, exists := chain1Map[*header.Hash()]; exists {
			return header, idx, i
		}
	}

	return nil, -1, -1
}

// ValidateChainConnection validates that a chain of headers properly connects
func (caf *CommonAncestorFinder) ValidateChainConnection(headers []*model.BlockHeader, startHash *chainhash.Hash) bool {
	if len(headers) == 0 {
		return true
	}

	// First header should connect to startHash if provided
	if startHash != nil && !headers[0].HashPrevBlock.IsEqual(startHash) {
		return false
	}

	// Each subsequent header should connect to the previous
	for i := 1; i < len(headers); i++ {
		if !headers[i].HashPrevBlock.IsEqual(headers[i-1].Hash()) {
			return false
		}
	}

	return true
}

// GetBlockLocator creates a block locator from a starting point
// Block locator is a list of block hashes going backwards with exponentially increasing gaps
func GetBlockLocator(startHash *chainhash.Hash, startHeight uint32, lookupFunc func(height uint32) *chainhash.Hash) []*chainhash.Hash {
	locator := make([]*chainhash.Hash, 0, 32)

	// Add the starting hash
	if startHash != nil {
		locator = append(locator, startHash)
	}

	// Step backwards with exponentially increasing gaps
	step := 1
	height := int(startHeight)

	for height > 0 && len(locator) < 10 {
		height -= step
		if height < 0 {
			height = 0
		}

		if hash := lookupFunc(uint32(height)); hash != nil {
			locator = append(locator, hash)
		}

		// Exponentially increase step size
		if len(locator) > 5 {
			step *= 2
		}
	}

	// Always include genesis if we haven't already
	if height > 0 {
		if hash := lookupFunc(0); hash != nil {
			locator = append(locator, hash)
		}
	}

	return locator
}

// CommonAncestorResult contains the result of finding a common ancestor
type CommonAncestorResult struct {
	Header      *model.BlockHeader
	Height      uint32
	LocalIndex  int // Index in local chain (-1 if not found)
	RemoteIndex int // Index in remote chain (-1 if not found)
	Depth       int // How far back from tip
}

// FindBestCommonAncestor finds the best (highest) common ancestor between chains
func (caf *CommonAncestorFinder) FindBestCommonAncestor(
	localTip *model.BlockHeader,
	localHeight uint32,
	remoteHeaders []*model.BlockHeader,
	maxDepth int,
) (*CommonAncestorResult, error) {
	if len(remoteHeaders) == 0 {
		return nil, ErrNoCommonAncestor
	}

	// Start from the local tip and work backwards
	currentHash := localTip.Hash()
	currentHeight := localHeight
	depth := 0

	// Create map of remote headers
	remoteMap := make(map[chainhash.Hash]int)
	for i, header := range remoteHeaders {
		remoteMap[*header.Hash()] = i
	}

	// Walk back through local chain
	for depth < maxDepth && currentHeight > 0 {
		// Check if current hash exists in remote chain
		if remoteIdx, exists := remoteMap[*currentHash]; exists {
			// Found common ancestor
			if header, exists := caf.LocalChainLookup(currentHash); exists {
				return &CommonAncestorResult{
					Header:      header,
					Height:      currentHeight,
					LocalIndex:  depth,
					RemoteIndex: remoteIdx,
					Depth:       depth,
				}, nil
			}
		}

		// Move to previous block
		if header, exists := caf.LocalChainLookup(currentHash); exists {
			currentHash = header.HashPrevBlock
			currentHeight--
			depth++
		} else {
			break
		}
	}

	return nil, ErrNoCommonAncestor
}
