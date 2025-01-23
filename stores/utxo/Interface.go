// Package utxo provides UTXO (Unspent Transaction Output) management for the Bitcoin SV Teranode implementation.
//
// The package implements a UTXO store interface that handles:
//   - UTXO creation, retrieval, and deletion
//   - Transaction spending and unspending operations
//   - UTXO freezing for alert system functionality
//   - Transaction metadata management
//   - Block height and median time tracking
//
// # UTXO States
//
// UTXOs can exist in several states:
//   - OK: The UTXO is valid and spendable
//   - SPENT: The UTXO has been spent in a transaction
//   - LOCKED: The UTXO is temporarily locked (e.g., coinbase maturity)
//   - FROZEN: The UTXO has been frozen by the alert system
//
// # Usage Example
//
//	store := // initialize your UTXO store implementation
//
//	// Create UTXOs from a transaction
//	metadata, err := store.Create(ctx, transaction, blockHeight)
//
//	// Spend UTXOs
//	spends := []*Spend{
//	    {
//	        TxID: txID,
//	        Vout: 0,
//	        UTXOHash: utxoHash,
//	        SpendingTxID: spendingTxID,
//	    },
//	}
//	err = store.Spend(ctx, spends, blockHeight)
package utxo

import (
	"context"

	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// ReAssignedUtxoSpendableAfterBlocks is the number of blocks that must pass
// before a reassigned UTXO becomes spendable.
const ReAssignedUtxoSpendableAfterBlocks = 1_000

// Spend represents a UTXO spending operation, containing both the UTXO being spent
// and the transaction that spends it.
type Spend struct {
	// TxID is the transaction ID that created this UTXO
	TxID *chainhash.Hash `json:"txId"`

	// Vout is the output index in the creating transaction
	Vout uint32 `json:"vout"`

	// UTXOHash is the unique identifier of this UTXO
	UTXOHash *chainhash.Hash `json:"utxoHash"`

	// SpendingTxID is the transaction ID that spends this UTXO
	// This will be nil if the UTXO is unspent
	SpendingTxID *chainhash.Hash `json:"spendingTxId,omitempty"`

	// ConflictingTxID is the transaction ID that conflicts with this UTXO
	ConflictingTxID *chainhash.Hash `json:"conflictingTxId,omitempty"`

	// error is the error that occurred during the spend operation
	Err error `json:"err,omitempty"`
}

var (
	// MetaFields defines the standard set of metadata fields that can be queried.
	MetaFields = []string{"locktime", "fee", "sizeInBytes", "parentTxHashes", "blockIDs", "isCoinbase", "frozen", "conflicting"}
	// MetaFieldsWithTx defines the set of metadata fields including the transaction data.
	MetaFieldsWithTx = []string{"tx", "locktime", "fee", "sizeInBytes", "parentTxHashes", "blockIDs", "isCoinbase", "frozen", "conflicting"}
)

// UnresolvedMetaData represents a transaction's metadata that needs to be resolved.
// It is used by the BatchDecorate function to efficiently fetch metadata for multiple transactions.
// It is struct that holds the hash of a tx and the index in the original list
// of hashes that was passed to the MetaBatchDecorate function. It also holds the optional fields
// that should be fetched and the error that was returned when fetching the data.
type UnresolvedMetaData struct {
	// Hash is the transaction hash
	Hash chainhash.Hash
	// Idx is the index in the original list of hashes passed to BatchDecorate
	Idx int
	// Data holds the fetched metadata, nil until fetched
	Data *meta.Data
	// Fields specifies which metadata fields should be fetched
	Fields []string
	// Err holds any error encountered while fetching the metadata
	Err error
}

// CreateOption is a function type that modifies CreateOptions.
// It follows the functional options pattern for configuring UTXO creation.
type CreateOption func(*CreateOptions)

// CreateOptions holds optional parameters for UTXO creation.
type CreateOptions struct {
	BlockIDs    []uint32
	TxID        *chainhash.Hash
	IsCoinbase  *bool
	Frozen      bool
	Conflicting bool
}

// WithBlockIDs returns a CreateOption that sets the block IDs for a UTXO.
// Multiple block IDs can be specified in case of a transaction that appears in multiple blocks.
func WithBlockIDs(blockIDs ...uint32) CreateOption {
	return func(o *CreateOptions) {
		o.BlockIDs = blockIDs
	}
}

// WithTXID returns a CreateOption that sets a custom transaction ID for a UTXO.
func WithTXID(txID *chainhash.Hash) CreateOption {
	return func(o *CreateOptions) {
		o.TxID = txID
	}
}

// WithSetCoinbase returns a CreateOption that marks a UTXO as coming from a coinbase transaction.
func WithSetCoinbase(b bool) CreateOption {
	return func(o *CreateOptions) {
		o.IsCoinbase = &b
	}
}

// WithFrozen returns a CreateOption that marks a UTXO as frozen.
func WithFrozen(b bool) CreateOption {
	return func(o *CreateOptions) {
		o.Frozen = b
	}
}

// WithConflicting returns a CreateOption that marks a UTXO as conflicting with another transaction.
func WithConflicting(b bool) CreateOption {
	return func(o *CreateOptions) {
		o.Conflicting = b
	}
}

// Store defines the interface for UTXO management operations.
// Implementations must be thread-safe as they will be accessed concurrently.
type Store interface {
	// Health checks the health status of the UTXO store.
	// If checkLiveness is true, it performs additional liveness checks.
	// Returns status code, status message and any error encountered.
	Health(ctx context.Context, checkLiveness bool) (int, string, error)

	// Create stores a new transaction's outputs as UTXOs and returns associated metadata.
	// The blockHeight parameter is used to determine coinbase maturity.
	// Additional options can be specified using CreateOption functions.
	Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, error)

	// Get retrieves UTXO metadata for a given transaction hash.
	// The fields parameter can be used to specify which metadata fields to retrieve.
	// If fields is empty, all fields will be retrieved.
	Get(ctx context.Context, hash *chainhash.Hash, fields ...[]string) (*meta.Data, error)

	// Delete removes a UTXO and its associated metadata from the store.
	Delete(ctx context.Context, hash *chainhash.Hash) error

	GetSpend(ctx context.Context, spend *Spend) (*SpendResponse, error)    // Remove? Only used in tests
	GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) // Remove?

	// Blockchain specific functions

	// Spend marks all the UTXOs of the transaction as spent.
	Spend(ctx context.Context, tx *bt.Tx) ([]*Spend, error)

	// UnSpend reverses a previous spend operation, marking UTXOs as unspent.
	// This is used during blockchain reorganizations.
	UnSpend(ctx context.Context, spends []*Spend) error

	// SetMinedMulti updates the block ID for multiple transactions that have been mined.
	SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error

	// these functions are not pure as they will update the data object in place

	// BatchDecorate efficiently fetches metadata for multiple transactions.
	// The fields parameter specifies which metadata fields to retrieve.
	BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*UnresolvedMetaData, fields ...string) error

	// PreviousOutputsDecorate fetches information about transaction inputs' previous outputs.
	PreviousOutputsDecorate(ctx context.Context, outpoints []*meta.PreviousOutput) error

	// functions related to Alert System

	// FreezeUTXOs marks UTXOs as frozen, preventing them from being spent.
	// This is used by the alert system to prevent spending of UTXOs.
	FreezeUTXOs(ctx context.Context, spends []*Spend, tSettings *settings.Settings) error

	// UnFreezeUTXOs removes the frozen status from UTXOs, allowing them to be spent again.
	UnFreezeUTXOs(ctx context.Context, spends []*Spend, tSettings *settings.Settings) error

	// ReAssignUTXO reassigns a UTXO to a new transaction output.
	// The UTXO will become spendable after ReAssignedUtxoSpendableAfterBlocks blocks.
	ReAssignUTXO(ctx context.Context, utxo *Spend, newUtxo *Spend, tSettings *settings.Settings) error

	// internal state functions

	// SetBlockHeight updates the current block height in the store.
	SetBlockHeight(height uint32) error

	// GetBlockHeight returns the current block height from the store.
	GetBlockHeight() uint32

	// SetMedianBlockTime updates the median block time in the store.
	SetMedianBlockTime(height uint32) error

	// GetMedianBlockTime returns the current median block time from the store.
	GetMedianBlockTime() uint32
}

var _ Store = &MockUtxostore{}

// MockUtxostore provides a mock implementation of the Store interface for testing purposes.
// All methods return nil values and no errors, except for PreviousOutputsDecorate which sets
// default values for testing.
type MockUtxostore struct{}

func (mu *MockUtxostore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return 0, "Validator test", nil
}
func (mu *MockUtxostore) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, error) {
	options := &CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

	txMeta, err := util.TxMetaDataFromTx(tx)
	if err != nil {
		return nil, err
	}

	return txMeta, nil
}

func (mu *MockUtxostore) Get(ctx context.Context, hash *chainhash.Hash, fields ...[]string) (*meta.Data, error) {
	return nil, nil
}
func (mu *MockUtxostore) Delete(ctx context.Context, hash *chainhash.Hash) error {
	return nil
}

func (mu *MockUtxostore) GetSpend(ctx context.Context, spend *Spend) (*SpendResponse, error) {
	return nil, nil
}
func (mu *MockUtxostore) GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	return nil, nil
}

func (mu *MockUtxostore) Spend(ctx context.Context, tx *bt.Tx) ([]*Spend, error) {
	return nil, nil
}
func (mu *MockUtxostore) UnSpend(ctx context.Context, spends []*Spend) error {
	return nil
}
func (mu *MockUtxostore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error {
	return nil
}

func (mu *MockUtxostore) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*UnresolvedMetaData, fields ...string) error {
	return nil
}
func (mu *MockUtxostore) PreviousOutputsDecorate(ctx context.Context, outpoints []*meta.PreviousOutput) error {
	for _, outpoint := range outpoints {
		outpoint.LockingScript = []byte{}
		outpoint.Satoshis = 32_280_613_550
	}

	return nil
}

func (mu *MockUtxostore) FreezeUTXOs(ctx context.Context, spends []*Spend, tSettings *settings.Settings) error {
	return nil
}

func (mu *MockUtxostore) UnFreezeUTXOs(ctx context.Context, spends []*Spend, tSettings *settings.Settings) error {
	return nil
}

func (mu *MockUtxostore) ReAssignUTXO(ctx context.Context, utxo *Spend, newUtxo *Spend, tSettings *settings.Settings) error {
	return nil
}

func (mu *MockUtxostore) SetBlockHeight(_ uint32) error {
	return nil
}
func (mu *MockUtxostore) GetBlockHeight() uint32 {
	return 0
}
func (mu *MockUtxostore) SetMedianBlockTime(_ uint32) error {
	return nil
}
func (mu *MockUtxostore) GetMedianBlockTime() uint32 {
	return 10 * 60
}
