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
	"time"

	"github.com/bitcoin-sv/teranode/model"
	"github.com/bitcoin-sv/teranode/stores/blob/options"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/libsv/go-bt/v2/chainhash"
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
	ProcessBlock(ctx context.Context, block *model.Block, blockHeight uint32) error

	// Get retrieves a subtree by its hash.
	Get(ctx context.Context, subtreeHash []byte) ([]byte, error)

	// Exists checks if a subtree exists by its hash.
	Exists(ctx context.Context, subtreeHash []byte) (bool, error)

	// Set stores a key-value pair with optional options.
	Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error

	// SetTTL sets a time-to-live duration for a key.
	SetTTL(ctx context.Context, key []byte, ttl time.Duration) error

	// SetTxMeta stores transaction metadata.
	SetTxMeta(ctx context.Context, txMetaData []*meta.Data) error

	// DelTxMeta deletes transaction metadata for a given hash.
	DelTxMeta(ctx context.Context, hash *chainhash.Hash) error

	// SetMinedMulti marks multiple transactions as mined in a specific block.
	SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error
}

var _ Interface = &MockBlockValidation{}

type MockBlockValidation struct{}

func (mv *MockBlockValidation) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return http.StatusOK, "OK", nil
}

func (mv *MockBlockValidation) BlockFound(ctx context.Context, blockHash *chainhash.Hash, baseURL string, waitToComplete bool) error {
	return nil
}

func (mv *MockBlockValidation) ProcessBlock(ctx context.Context, block *model.Block, blockHeight uint32) error {
	return nil
}

func (mv *MockBlockValidation) Get(ctx context.Context, subtreeHash []byte) ([]byte, error) {
	return nil, nil
}

func (mv *MockBlockValidation) Exists(ctx context.Context, subtreeHash []byte) (bool, error) {
	return false, nil
}

func (mv *MockBlockValidation) Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error {
	return nil
}

func (mv *MockBlockValidation) SetTTL(ctx context.Context, key []byte, ttl time.Duration) error {
	return nil
}

func (mv *MockBlockValidation) SetTxMeta(ctx context.Context, txMetaData []*meta.Data) error {
	return nil
}

func (mv *MockBlockValidation) DelTxMeta(ctx context.Context, hash *chainhash.Hash) error {
	return nil
}

func (mv *MockBlockValidation) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error {
	return nil
}
