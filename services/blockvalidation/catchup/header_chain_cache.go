// Package catchup provides a chain-aware header cache for efficient block validation during catchup.
// The cache pre-computes validation header sets to avoid repeated database lookups during block processing.
package catchup

import (
	"sync"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// HeaderChainCache provides efficient access to block headers during catchup operations.
// It maintains the chain relationship between headers and pre-computes validation header sets.
//
// Key features:
// - Pre-computes validation headers for each block (previous N headers)
// - Validates chain integrity before caching
// - Thread-safe for concurrent reads after building
// - Immutable after construction - built once then read-only
//
// This significantly reduces database queries during block validation.
type HeaderChainCache struct {
	mu sync.RWMutex

	// validationHeaders maps each block hash to the headers needed for its validation
	// These are the previous N headers in the chain, stored in the order expected by ValidateBlock
	validationHeaders map[chainhash.Hash][]*model.BlockHeader

	// chainIndex provides quick lookup of headers by their hash
	// Used to verify chain relationships and check for existing headers
	chainIndex map[chainhash.Hash]*model.BlockHeader

	// headerSequence maintains the headers in chain order (oldest to newest)
	// This preserves the exact sequence as provided during cache construction
	headerSequence []*model.BlockHeader

	// logger for debugging chain validation issues
	logger ulogger.Logger
}

// NewHeaderChainCache creates a new header chain cache.
//
// Parameters:
//   - logger: Logger for debugging cache operations
//
// Returns:
//   - *HeaderChainCache: Empty cache ready to be built with headers
func NewHeaderChainCache(logger ulogger.Logger) *HeaderChainCache {
	return &HeaderChainCache{
		validationHeaders: make(map[chainhash.Hash][]*model.BlockHeader),
		chainIndex:        make(map[chainhash.Hash]*model.BlockHeader),
		headerSequence:    make([]*model.BlockHeader, 0),
		logger:            logger,
	}
}

// BuildFromHeaders constructs the cache from a sequence of headers.
//
// Parameters:
//   - headers: Block headers in chain order (oldest to newest)
//   - previousHeaderCount: Number of previous headers to cache for validation
//
// Returns:
//   - error: If headers don't form a valid chain
//
// The headers must form a continuous chain where each header's HashPrevBlock
// matches the previous header's hash.
func (hc *HeaderChainCache) BuildFromHeaders(headers []*model.BlockHeader, previousHeaderCount uint64) error {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	if len(headers) == 0 {
		return nil // Empty cache is valid
	}

	// First, validate the chain integrity
	if err := hc.validateChain(headers); err != nil {
		return errors.NewProcessingError("invalid header chain: %v", err)
	}

	// Clear any existing data
	hc.validationHeaders = make(map[chainhash.Hash][]*model.BlockHeader)
	hc.chainIndex = make(map[chainhash.Hash]*model.BlockHeader)
	hc.headerSequence = make([]*model.BlockHeader, len(headers))
	copy(hc.headerSequence, headers)

	// Build the chain index
	for _, header := range headers {
		hc.chainIndex[*header.Hash()] = header
	}

	// Pre-compute validation header sets for each block
	for i, header := range headers {
		validationHeaders := hc.collectPreviousHeaders(headers, i, int(previousHeaderCount))
		// Always store the validation headers, even if empty (for first block)
		hc.validationHeaders[*header.Hash()] = validationHeaders
		hc.logger.Debugf("Cached %d validation headers for block %s", len(validationHeaders), header.Hash().String())
	}

	hc.logger.Infof("Built header chain cache with %d headers, %d validation sets", len(headers), len(hc.validationHeaders))

	return nil
}

// validateChain ensures all headers form a valid chain.
//
// Parameters:
//   - headers: Headers to validate
//
// Returns:
//   - error: If chain is broken at any point
//
// Verifies each header's HashPrevBlock matches the previous header's hash.
func (hc *HeaderChainCache) validateChain(headers []*model.BlockHeader) error {
	for i := 1; i < len(headers); i++ {
		prevHash := headers[i-1].Hash()
		if !headers[i].HashPrevBlock.IsEqual(prevHash) {
			return errors.NewProcessingError("chain broken at position %d: block %s expects prev %s but previous block is %s",
				i, headers[i].Hash().String(), headers[i].HashPrevBlock.String(), prevHash.String())
		}
	}
	return nil
}

// collectPreviousHeaders collects up to 'count' headers before the header at position 'index'.
//
// Parameters:
//   - headers: Full header sequence
//   - index: Position of header needing validation headers
//   - count: Maximum number of previous headers to collect
//
// Returns:
//   - []*model.BlockHeader: Previous headers in reverse order (newest first)
//
// Returns headers in reverse order to match blockchain service expectations.
func (hc *HeaderChainCache) collectPreviousHeaders(headers []*model.BlockHeader, index int, count int) []*model.BlockHeader {
	if index == 0 {
		return nil // No previous headers for the first block
	}

	// Calculate how many headers we can actually collect
	start := index - count
	if start < 0 {
		start = 0
	}

	// Collect headers from start to index-1
	result := make([]*model.BlockHeader, 0, index-start)
	for i := start; i < index; i++ {
		result = append(result, headers[i])
	}

	// Reverse to get newest first (matching GetBlockHeaders behavior)
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return result
}

// GetValidationHeaders returns the cached headers needed to validate a specific block.
// Returns nil, false if the block is not in the cache.
func (hc *HeaderChainCache) GetValidationHeaders(blockHash *chainhash.Hash) ([]*model.BlockHeader, bool) {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	headers, exists := hc.validationHeaders[*blockHash]
	if !exists {
		return nil, false
	}

	// Return a copy to prevent modification
	result := make([]*model.BlockHeader, len(headers))
	copy(result, headers)
	return result, true
}

// HasBlock checks if a block hash exists in the chain index.
func (hc *HeaderChainCache) HasBlock(blockHash *chainhash.Hash) bool {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	_, exists := hc.chainIndex[*blockHash]
	return exists
}

// VerifyChainConnection checks if a new header would connect properly to the cached chain.
// This is useful for validating catchup responses.
func (hc *HeaderChainCache) VerifyChainConnection(newHeader *model.BlockHeader) error {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	// Check if the previous block exists in our chain
	prevBlock, exists := hc.chainIndex[*newHeader.HashPrevBlock]
	if !exists {
		return errors.NewProcessingError("previous block %s not found in chain", newHeader.HashPrevBlock.String())
	}

	// Verify the connection is valid
	if !prevBlock.Hash().IsEqual(newHeader.HashPrevBlock) {
		return errors.NewProcessingError("chain connection invalid: block %s expects prev %s but found %s",
			newHeader.Hash().String(), newHeader.HashPrevBlock.String(), prevBlock.Hash().String())
	}

	return nil
}

// GetChainLength returns the number of headers in the cached chain.
//
// Returns:
//   - int: Number of headers in the chain
//
// Thread-safe for concurrent access.
func (hc *HeaderChainCache) GetChainLength() int {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	return len(hc.headerSequence)
}

// Clear removes all cached data.
// Use when resetting for a new catchup operation.
// Thread-safe but should only be called when cache is not in use.
func (hc *HeaderChainCache) Clear() {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	hc.validationHeaders = make(map[chainhash.Hash][]*model.BlockHeader)
	hc.chainIndex = make(map[chainhash.Hash]*model.BlockHeader)
	hc.headerSequence = hc.headerSequence[:0]
}

// GetCacheStats returns statistics about the cache for monitoring.
//
// Returns:
//   - totalHeaders: Total number of headers in the cache
//   - validationSets: Number of pre-computed validation header sets
//
// Thread-safe for concurrent access.
func (hc *HeaderChainCache) GetCacheStats() (totalHeaders int, validationSets int) {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	return len(hc.chainIndex), len(hc.validationHeaders)
}
