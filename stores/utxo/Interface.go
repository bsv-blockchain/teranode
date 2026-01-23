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

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/spend"
)

// ReAssignedUtxoSpendableAfterBlocks is the number of blocks that must pass
// before a reassigned UTXO becomes spendable.
const ReAssignedUtxoSpendableAfterBlocks = 1_000

// BlockState represents an atomic snapshot of blockchain state containing
// both block height and median block time. This ensures consistency between
// these values during validation, preventing race conditions that could occur
// when reading them separately.
type BlockState struct {
	Height     uint32 // Current block height
	MedianTime uint32 // Median time of recent blocks
}

// Spend represents a UTXO spending operation, containing both the UTXO being spent
// and the transaction that spends it.
type Spend struct {
	// TxID is the transaction ID that created this UTXO
	TxID *chainhash.Hash `json:"txId"`

	// Vout is the output index in the creating transaction
	Vout uint32 `json:"vout"`

	// UTXOHash is the unique identifier of this UTXO
	UTXOHash *chainhash.Hash `json:"utxoHash"`

	// SpendingData contains information about the transaction that spends this UTXO
	// This will be nil if the UTXO is unspent
	SpendingData *spend.SpendingData `json:"spendingData,omitempty"`

	// ConflictingTxID is the transaction ID that conflicts with this UTXO
	ConflictingTxID *chainhash.Hash `json:"conflictingTxId,omitempty"`

	// BlockIDs is the list of blocks the transaction has been mined into
	BlockIDs []uint32 `json:"blockIDs,omitempty"`

	// error is the error that occurred during the spend operation
	Err error `json:"err,omitempty"`
}

// Clone creates a deep copy of the Spend struct.
// Returns nil if the receiver is nil.
func (s *Spend) Clone() *Spend {
	if s == nil {
		return nil
	}

	clone := &Spend{
		Vout: s.Vout,
		Err:  s.Err,
	}

	if s.TxID != nil {
		clone.TxID = &chainhash.Hash{}
		*clone.TxID = *s.TxID
	}

	if s.UTXOHash != nil {
		clone.UTXOHash = &chainhash.Hash{}
		*clone.UTXOHash = *s.UTXOHash
	}

	if s.SpendingData != nil {
		clone.SpendingData = s.SpendingData.Clone()
	}

	if s.ConflictingTxID != nil {
		clone.ConflictingTxID = &chainhash.Hash{}
		*clone.ConflictingTxID = *s.ConflictingTxID
	}

	if s.BlockIDs != nil {
		clone.BlockIDs = make([]uint32, len(s.BlockIDs))
		copy(clone.BlockIDs, s.BlockIDs)
	}

	return clone
}

// IgnoreFlags controls which UTXO states should be ignored during spend operations.
type IgnoreFlags struct {
	IgnoreConflicting bool
	IgnoreLocked      bool
}

var (
	// MetaFields defines the standard set of metadata fields that can be queried.
	MetaFields = []fields.FieldName{fields.LockTime, fields.Fee, fields.SizeInBytes, fields.TxInpoints, fields.BlockIDs, fields.IsCoinbase, fields.Conflicting, fields.Locked, fields.Creating}
	// MetaFieldsWithTx defines the set of metadata fields including the transaction data.
	MetaFieldsWithTx = append(MetaFields, fields.Tx)
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
	Fields []fields.FieldName
	// Err holds any error encountered while fetching the metadata
	Err error
}

// CreateOption is a function type that modifies CreateOptions.
// It follows the functional options pattern for configuring UTXO creation.
type CreateOption func(*CreateOptions)

// CreateOptions holds optional parameters for UTXO creation.
type CreateOptions struct {
	MinedBlockInfos []MinedBlockInfo
	TxID            *chainhash.Hash
	IsCoinbase      *bool
	Frozen          bool
	Conflicting     bool
	Locked          bool
}

// WithMinedBlockInfo returns a CreateOption that sets the block IDs for a UTXO.
// Multiple block IDs can be specified in case of a transaction that appears in multiple blocks.
func WithMinedBlockInfo(minedBlockInfos ...MinedBlockInfo) CreateOption {
	return func(o *CreateOptions) {
		if o.MinedBlockInfos == nil {
			o.MinedBlockInfos = make([]MinedBlockInfo, 0)
		}

		o.MinedBlockInfos = append(o.MinedBlockInfos, minedBlockInfos...)
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

// WithConflicting marks a transaction as conflicting with another transaction.
func WithConflicting(b bool) CreateOption {
	return func(o *CreateOptions) {
		o.Conflicting = b
	}
}

// WithLocked sets the transactions as locked and not spendable on creation
func WithLocked(b bool) CreateOption {
	return func(o *CreateOptions) {
		o.Locked = b
	}
}

type MinedBlockInfo struct {
	BlockID        uint32
	BlockHeight    uint32
	SubtreeIdx     int
	OnLongestChain bool
	UnsetMined     bool // if true, the mined info will be removed from the tx
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
	Get(ctx context.Context, hash *chainhash.Hash, fields ...fields.FieldName) (*meta.Data, error)

	// Delete removes a UTXO and its associated metadata from the store.
	Delete(ctx context.Context, hash *chainhash.Hash) error

	GetSpend(ctx context.Context, spend *Spend) (*SpendResponse, error) // Remove? Only used in tests
	GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error

	// Blockchain specific functions

	// Spend marks all the UTXOs of the transaction as spent.
	Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...IgnoreFlags) ([]*Spend, error)

	// Unspend reverses a previous spend operation, marking UTXOs as unspent.
	// This is used during blockchain reorganizations.
	Unspend(ctx context.Context, spends []*Spend, flagAsLocked ...bool) error

	// SetMinedMulti updates the block ID for multiple transactions that have been mined.
	SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo MinedBlockInfo) (map[chainhash.Hash][]uint32, error)

	// GetUnminedTxIterator returns an iterator for all unmined transactions in the store.
	GetUnminedTxIterator(fullScan bool) (UnminedTxIterator, error)

	// QueryOldUnminedTransactions returns transaction hashes for unmined transactions older than the cutoff height.
	// This method is used by the store-agnostic cleanup implementation.
	QueryOldUnminedTransactions(ctx context.Context, cutoffBlockHeight uint32) ([]chainhash.Hash, error)

	// PreserveTransactions marks transactions to be preserved from deletion until a specific block height.
	// This clears any existing DeleteAtHeight and sets PreserveUntil to the specified height.
	// Used to protect parent transactions when cleaning up unmined transactions.
	PreserveTransactions(ctx context.Context, txIDs []chainhash.Hash, preserveUntilHeight uint32) error

	// ProcessExpiredPreservations handles transactions whose preservation period has expired.
	// For each transaction with PreserveUntil <= currentHeight, it sets an appropriate DeleteAtHeight
	// and clears the PreserveUntil field.
	ProcessExpiredPreservations(ctx context.Context, currentHeight uint32) error

	// these functions are not pure as they will update the data object in place

	// BatchDecorate efficiently fetches metadata for multiple transactions.
	// The fields parameter specifies which metadata fields to retrieve.
	BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*UnresolvedMetaData, fields ...fields.FieldName) error

	// PreviousOutputsDecorate fetches information about transaction inputs' previous outputs.
	PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error

	// functions related to Alert System

	// FreezeUTXOs marks UTXOs as frozen, preventing them from being spent.
	// This is used by the alert system to prevent spending of UTXOs.
	FreezeUTXOs(ctx context.Context, spends []*Spend, tSettings *settings.Settings) error

	// UnFreezeUTXOs removes the frozen status from UTXOs, allowing them to be spent again.
	UnFreezeUTXOs(ctx context.Context, spends []*Spend, tSettings *settings.Settings) error

	// ReAssignUTXO reassigns a UTXO to a new transaction output.
	// The UTXO will become spendable after ReAssignedUtxoSpendableAfterBlocks blocks.
	ReAssignUTXO(ctx context.Context, utxo *Spend, newUtxo *Spend, tSettings *settings.Settings) error

	// GetCounterConflicting returns the counter conflicting transactions for a given transaction hash.
	GetCounterConflicting(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error)

	// GetConflictingChildren returns the children of the given conflicting transaction
	GetConflictingChildren(ctx context.Context, txHash chainhash.Hash) ([]chainhash.Hash, error)

	// SetConflicting marks transactions as conflicting or not conflicting and returns the affected spends.
	SetConflicting(ctx context.Context, txHashes []chainhash.Hash, value bool) ([]*Spend, []chainhash.Hash, error)

	// SetLocked marks transactions as locked for spending.
	SetLocked(ctx context.Context, txHashes []chainhash.Hash, value bool) error

	// MarkTransactionsOnLongestChain marks transactions as being on the longest chain or not.
	// When onLongestChain is true, the unminedSince field is unset (transaction is mined).
	// When onLongestChain is false, the unminedSince field is set to the current block height.
	MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error

	// internal state functions

	// SetBlockHeight updates the current block height in the store.
	SetBlockHeight(height uint32) error

	// GetBlockHeight returns the current block height from the store.
	GetBlockHeight() uint32

	// SetMedianBlockTime updates the median block time in the store.
	SetMedianBlockTime(height uint32) error

	// GetMedianBlockTime returns the current median block time from the store.
	GetMedianBlockTime() uint32

	// GetBlockState returns an atomic snapshot of both block height and median block time.
	// This prevents race conditions that could occur when reading these values separately,
	// ensuring consistency during validation operations.
	GetBlockState() BlockState
}
