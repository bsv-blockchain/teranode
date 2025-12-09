// Package blockvalidation implements block validation for Bitcoin SV nodes in Teranode.
//
// This package provides the core functionality for validating Bitcoin blocks, managing block subtrees,
// and processing transaction metadata. It is designed for high-performance operation at scale,
// supporting features like:
//
// - Concurrent block validation with optimistic mining support
// - Subtree-based block organization and validation
// - Transaction metadata caching and management
// - Automatic chain catchup when falling behind
// - Integration with Kafka for distributed operation
//
// The package exposes gRPC interfaces for block validation operations,
// making it suitable for use in distributed Teranode deployments.
package blockvalidation

import (
	"context"
	"net/http"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
)

// Interface defines the contract for block validation functionality in Teranode.
// It provides methods for validating blocks, managing subtrees, and handling transaction metadata.
type Interface interface {
	// Health checks the health status of the block validation service.
	// If checkLiveness is true, only checks service liveness.
	// If false, performs full dependency checking.
	Health(ctx context.Context, checkLiveness bool) (int, string, error)

	// BlockFound notifies the service about a newly discovered block.
	// The baseUrl parameter indicates where to fetch block data if needed.
	// If waitToComplete is true, waits for validation to complete before returning.
	BlockFound(ctx context.Context, blockHash *chainhash.Hash, baseURL string, waitToComplete bool) error

	// ProcessBlock validates and processes a complete block at the specified height.
	ProcessBlock(ctx context.Context, block *model.Block, blockHeight uint32, peerID, baseURL string) error

	// ValidateBlock validates a block using the provided request, but does not update any state or database tables.
	// This is useful for validating blocks without committing them to the database.
	// The options parameter allows control over validation behavior, including revalidation of invalid blocks.
	ValidateBlock(ctx context.Context, block *model.Block, options *ValidateBlockOptions) error

	// RevalidateBlock forces revalidation of a block identified by its hash.
	// This is used to do a full revalidation of a block, that was previously marked as invalid.
	RevalidateBlock(ctx context.Context, blockHash chainhash.Hash) error

	// GetCatchupStatus returns the current status of blockchain catchup operations.
	GetCatchupStatus(ctx context.Context) (*CatchupStatus, error)
}

var _ Interface = &MockBlockValidation{}

type MockBlockValidation struct{}

func (mv *MockBlockValidation) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return http.StatusOK, "OK", nil
}

func (mv *MockBlockValidation) BlockFound(ctx context.Context, blockHash *chainhash.Hash, baseURL string, waitToComplete bool) error {
	return nil
}

func (mv *MockBlockValidation) ProcessBlock(ctx context.Context, block *model.Block, blockHeight uint32, peerID, baseURL string) error {
	return nil
}

func (mv *MockBlockValidation) ValidateBlock(ctx context.Context, block *model.Block, options *ValidateBlockOptions) error {
	return nil
}

func (mv *MockBlockValidation) RevalidateBlock(ctx context.Context, blockHash chainhash.Hash) error {
	return nil
}

func (mv *MockBlockValidation) GetCatchupStatus(ctx context.Context) (*CatchupStatus, error) {
	return &CatchupStatus{IsCatchingUp: false}, nil
}
