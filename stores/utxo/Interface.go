// Package utxo provides UTXO (Unspent Transaction Output) management for the BSV Blockchain Teranode implementation.
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
//	// Spend the transaction's inputs and create its outputs in one operation
//	metadata, spends, err := store.SpendAndCreate(ctx, transaction, blockHeight)
//
//	// Only create the transaction's outputs (e.g. coinbase, no inputs to spend)
//	metadata, _, err = store.SpendAndCreate(ctx, coinbaseTx, blockHeight, WithCreateOnly())
//
//	// Only spend the transaction's inputs
//	_, spends, err = store.SpendAndCreate(ctx, transaction, blockHeight, WithSpendOnly())
package utxo

import (
	"bytes"
	"context"
	"encoding/binary"
	"sort"

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

// ConflictIntentKind identifies the direction of a conflict-resolution
// operation recorded in the write-ahead log.
type ConflictIntentKind string

const (
	// ConflictIntentForward records a ProcessConflicting invocation.
	ConflictIntentForward ConflictIntentKind = "forward"

	// ConflictIntentReverse records a ReverseProcessConflicting invocation.
	ConflictIntentReverse ConflictIntentKind = "reverse"
)

// ConflictIntent is a write-ahead-log record describing one in-flight
// conflict-resolution operation (ProcessConflicting or
// ReverseProcessConflicting). It is persisted durably BEFORE the operation's
// first state mutation and removed once the operation's terminal step
// completes, so a process kill between any two steps can be detected and the
// operation replayed on restart. See ProcessConflicting / ReverseProcessConflicting.
type ConflictIntent struct {
	// Kind is the operation direction: forward = ProcessConflicting,
	// reverse = ReverseProcessConflicting.
	Kind ConflictIntentKind

	// BlockHeight is the block height the operation was invoked with.
	BlockHeight uint32

	// BlockHash is the hash of the block whose movement triggered the operation
	// — the moved-forward block for a forward intent, the moved-back block for a
	// reverse intent. Startup replay gates on this block's chain membership so a
	// stale intent (whose block was reorged out from under it) is discarded rather
	// than blindly re-applied, which would undo a later, valid reorg.
	BlockHash chainhash.Hash

	// TxHashes is the operation's input hash slice — conflictingTxHashes for
	// forward, demotedTxHashes for reverse.
	TxHashes []chainhash.Hash

	// StartedAt is the unix-nanosecond timestamp the intent was recorded.
	StartedAt int64
}

// IntentID derives a deterministic identifier for the intent from its kind,
// block hash, block height and the (sorted) set of tx hashes. Two invocations
// with the same (kind, block, height, hashes) yield the same id, which makes
// BeginConflictIntent idempotent across a crash-retry of the same operation
// and lets startup replay deduplicate naturally. The hash ordering is
// normalised so callers need not pre-sort.
func (ci ConflictIntent) IntentID() chainhash.Hash {
	sorted := make([]chainhash.Hash, len(ci.TxHashes))
	copy(sorted, ci.TxHashes)
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i][:], sorted[j][:]) < 0
	})

	buf := make([]byte, 0, len(ci.Kind)+chainhash.HashSize+4+len(sorted)*chainhash.HashSize)
	buf = append(buf, []byte(ci.Kind)...)
	buf = append(buf, ci.BlockHash[:]...)

	var heightBytes [4]byte
	binary.BigEndian.PutUint32(heightBytes[:], ci.BlockHeight)
	buf = append(buf, heightBytes[:]...)

	for i := range sorted {
		buf = append(buf, sorted[i][:]...)
	}

	return chainhash.HashH(buf)
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

	return clone
}

// IgnoreFlags controls which UTXO states should be ignored during spend operations.
type IgnoreFlags struct {
	IgnoreConflicting bool
	IgnoreLocked      bool
	// SkipUTXOHashCheck disables the per-input utxo-hash integrity comparison during Spend.
	// Set ONLY on the gated below-checkpoint outpoint-only path (spec §3.2 Seam 1). Default
	// false — above-checkpoint and steady-state spends always enforce the hash.
	SkipUTXOHashCheck bool
}

// ConflictingChildRemoval identifies one (parent, child) pair that should be
// scrubbed from the parent's conflictingChildren list.
// Used with utxo.Store.RemoveFromConflictingChildren.
type ConflictingChildRemoval struct {
	ParentHash *chainhash.Hash
	ChildHash  *chainhash.Hash
}

// BlockIDsRemoval identifies the set of block IDs to strip from one
// transaction's blockIDs membership.
// Used with utxo.Store.RemoveBlockIDs.
type BlockIDsRemoval struct {
	TxHash   *chainhash.Hash
	BlockIDs []uint32
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
	MinedBlockInfos    []MinedBlockInfo
	TxID               *chainhash.Hash
	IsCoinbase         *bool
	Frozen             bool
	Conflicting        bool
	Locked             bool
	SkipExtendedInputs bool

	// SpendAndCreate-specific options.
	IgnoreFlags IgnoreFlags // spend-phase flags (ignored with CreateOnly)
	CreateOnly  bool        // skip the spend phase
	SpendOnly   bool        // skip the create phase
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

// WithSkipExtendedInputs marks a create as minimal: compute meta with fee=0 (no GetFees) and
// persist per-input parent script/satoshis as empty/zero, while ALWAYS retaining the per-input
// outpoint and every output. Set ONLY on the gated below-checkpoint path (spec §3.2 Seam 3, §3.3).
func WithSkipExtendedInputs(b bool) CreateOption {
	return func(o *CreateOptions) {
		o.SkipExtendedInputs = b
	}
}

// WithIgnoreConflicting makes the spend phase of SpendAndCreate ignore the
// conflicting flag on the UTXOs being spent.
func WithIgnoreConflicting(b bool) CreateOption {
	return func(o *CreateOptions) {
		o.IgnoreFlags.IgnoreConflicting = b
	}
}

// WithIgnoreLocked makes the spend phase of SpendAndCreate ignore the locked
// flag on the UTXOs being spent.
func WithIgnoreLocked(b bool) CreateOption {
	return func(o *CreateOptions) {
		o.IgnoreFlags.IgnoreLocked = b
	}
}

// WithSkipUTXOHashCheck disables the per-input utxo-hash integrity comparison in
// the spend phase of SpendAndCreate. Set ONLY on the gated below-checkpoint
// outpoint-only path (see IgnoreFlags.SkipUTXOHashCheck). The Aerospike store
// does not implement the outpoint-only fast path and treats this flag as a
// no-op (see SupportsOutpointOnlySpend).
func WithSkipUTXOHashCheck(b bool) CreateOption {
	return func(o *CreateOptions) {
		o.IgnoreFlags.SkipUTXOHashCheck = b
	}
}

// WithCreateOnly makes SpendAndCreate skip the spend phase: only the
// transaction's outputs and metadata are stored (coinbase, seeding, and batch
// flows whose inputs are spent elsewhere).
func WithCreateOnly() CreateOption {
	return func(o *CreateOptions) {
		o.CreateOnly = true
	}
}

// WithSpendOnly makes SpendAndCreate skip the create phase: only the
// transaction's inputs are spent (reorg/conflict helpers and batch flows whose
// outputs are created elsewhere).
func WithSpendOnly() CreateOption {
	return func(o *CreateOptions) {
		o.SpendOnly = true
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

	// SupportsOutpointOnlySpend reports whether this store correctly honours the
	// below-checkpoint outpoint-only fast path — i.e. whether it acts on the
	// CreateOptions.SkipExtendedInputs and IgnoreFlags.SkipUTXOHashCheck flags rather
	// than silently ignoring them. Callers that intend to skip decorate (and hence
	// hand un-decorated inputs to Create/Spend) MUST consult this first: a store that
	// returns false would still try to derive the UTXO hash from absent parent data
	// and hard-error on every transaction. SQL stores return true; stores without
	// fast-path support (e.g. Aerospike, pending Stage B) return false. Decorators
	// delegate to the wrapped store.
	SupportsOutpointOnlySpend() bool

	// Close drains any in-flight batched writes (Create, Spend, Get, Unlock,
	// or any other batched operations the implementation owns) and releases
	// backing resources (connection pools, file handles, batcher workers).
	//
	// After Close returns, no further Store operations are valid.
	//
	// Unless the supplied context expires first, implementations MUST wait
	// for outstanding batched writes to complete before returning. Returning
	// before pending writes have committed risks silently losing UTXO state:
	// callers (block validation, legacy sync) will have already received
	// successful responses for those writes and will have committed the
	// parent block, but on restart the UTXOs will be missing — breaking
	// subsequent blocks that spend them.
	//
	// The context bounds the drain. If it expires before the drain completes,
	// implementations should return its error; the underlying drain and
	// resource release may continue best-effort in the background, but the
	// caller must treat a context error as "drain not confirmed complete".
	Close(ctx context.Context) error

	// Get retrieves UTXO metadata for a given transaction hash.
	// The fields parameter can be used to specify which metadata fields to retrieve.
	// If fields is empty, all fields will be retrieved.
	Get(ctx context.Context, hash *chainhash.Hash, fields ...fields.FieldName) (*meta.Data, error)

	// Delete removes a UTXO and its associated metadata from the store.
	Delete(ctx context.Context, hash *chainhash.Hash) error

	GetSpend(ctx context.Context, spend *Spend) (*SpendResponse, error) // Remove? Only used in tests
	GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error

	// Blockchain specific functions

	// SpendAndCreate spends tx's inputs and creates its outputs + metadata as one
	// logical operation. Implementations SHOULD make this atomic (followup work);
	// the current shared sequential implementation spends, then creates, and
	// unspends on create failure (except ErrTxExists — see below).
	//
	// Semantics (contract for all implementations):
	//   - Default: spend inputs, then create outputs. On create failure other than
	//     ErrTxExists, successful spends are rolled back before returning.
	//   - ErrTxExists from the create phase is returned to the caller WITH the
	//     spends left in place; the caller decides what to do with the existing tx.
	//   - WithCreateOnly(): skip the spend phase (coinbase, seeding, batch flows
	//     whose inputs are spent elsewhere). Returned []*Spend is nil.
	//   - WithSpendOnly(): skip the create phase (reorg/conflict helpers, batch
	//     flows whose outputs are created elsewhere). Returned *meta.Data is nil.
	//   - On spend failure the returned []*Spend carries per-input Err values for
	//     caller inspection (conflict detection).
	//   - When the create phase fails and the spends were rolled back, the
	//     returned []*Spend is nil; a non-nil slice alongside a non-nil error
	//     means the spends are still in effect (ErrTxExists, or rollback failure).
	//   - Transactions with no inputs to spend (synthesized seed txs) must pass
	//     WithCreateOnly(); backend behaviour for a default-mode spend of a
	//     zero-input tx is undefined.
	SpendAndCreate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, []*Spend, error)

	// Unspend reverses a previous spend operation, marking UTXOs as unspent.
	// This is used during blockchain reorganizations.
	Unspend(ctx context.Context, spends []*Spend, flagAsLocked ...bool) error

	// SetMinedMulti marks transactions as mined in the block described by minedBlockInfo.
	//
	// Postcondition (when minedBlockInfo.UnsetMined is false and a nil error is returned):
	//   - Every hash in `hashes` MUST appear as a key in the returned map.
	//   - Every returned slice MUST contain minedBlockInfo.BlockID.
	// Implementations that cannot prove this MUST return a non-nil error.
	//
	// When minedBlockInfo.UnsetMined is true, missing or empty entries are tolerated:
	// the call may no-op for transactions that no longer exist.
	SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, minedBlockInfo MinedBlockInfo) (map[chainhash.Hash][]uint32, error)

	// GetUnminedTxIterator returns an iterator for unmined transactions in the store.
	// Uses the unmined_since secondary index to query only transactions with unmined_since set,
	// and does NOT scan all records. For full consistency checking that scans all records
	// regardless of unmined_since, see ScanInconsistentUnminedTxs.
	GetUnminedTxIterator() (UnminedTxIterator, error)

	// ScanInconsistentUnminedTxs returns a lightweight iterator that scans all records
	// to detect unmined_since inconsistencies (mined txs with unmined_since still set).
	// Only fetches txid, block_ids, and unmined_since — no heavy data like TxInpoints.
	ScanInconsistentUnminedTxs() (ConsistencyScanIterator, error)

	// GetPrunableUnminedTxIterator returns a lightweight iterator optimized for the pruner's needs.
	// Unlike GetUnminedTxIterator, this iterator:
	// - Filters server-side for only unmined transactions with unminedSince <= cutoffBlockHeight
	// - Fetches only the bins needed by the pruner (txID, unminedSince, external, inputs)
	// This reduces bandwidth by 90-99%+ compared to the full iterator when the mempool is large.
	GetPrunableUnminedTxIterator(cutoffBlockHeight uint32) (UnminedTxIterator, error)

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

	// BatchPreviousOutputsDecorate fetches previous output information for inputs across
	// multiple transactions in bulk. This is more efficient than calling PreviousOutputsDecorate
	// per-transaction because it reduces database round-trips.
	// Inputs that are already decorated (PreviousTxScript != nil) are skipped.
	BatchPreviousOutputsDecorate(ctx context.Context, txs []*bt.Tx) error

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

	// RemoveFromConflictingChildren removes each child hash from its parent's
	// conflictingChildren list. Used by repair tooling when child transactions
	// are deleted and must no longer appear in any surviving parent's
	// conflictingChildren field. The call is idempotent — missing parents,
	// missing list bins, and missing list entries are silently tolerated.
	// Implementations must use the backend's batch API (e.g. Aerospike
	// BatchOperate) so large removals stay fast.
	RemoveFromConflictingChildren(ctx context.Context, removals []ConflictingChildRemoval) error

	// RemoveBlockIDs trims the supplied block IDs from each transaction's
	// blockIDs membership without deleting the transaction record. Used by
	// repair tooling when transactions are referenced by multiple blocks and
	// only a subset is being removed. Idempotent.
	// Implementations must batch across removals using the backend's batch
	// API.
	RemoveBlockIDs(ctx context.Context, removals []BlockIDsRemoval) error

	// GetConflictingTxIterator returns an iterator over transactions currently
	// marked conflicting=true. Complements GetUnminedTxIterator, which filters
	// out conflicting records. Used by repair tooling to purge losing-side
	// transactions during a rewind.
	GetConflictingTxIterator() (UnminedTxIterator, error)

	// SetLocked marks transactions as locked for spending.
	SetLocked(ctx context.Context, txHashes []chainhash.Hash, value bool) error

	// conflict-resolution write-ahead log (crash safety for ProcessConflicting /
	// ReverseProcessConflicting — see #861)

	// BeginConflictIntent durably records a conflict-resolution intent BEFORE the
	// operation's first state mutation. It MUST be committed durably before
	// returning. Recording the same intent id more than once is idempotent (the
	// id is deterministic over kind+height+hashes), so a crash-retry of the same
	// operation does not create a duplicate. A non-nil error MUST abort the
	// caller — the operation must not proceed without a durable intent record.
	BeginConflictIntent(ctx context.Context, intent ConflictIntent) error

	// CompleteConflictIntent deletes the intent record identified by intentID
	// after the operation's terminal step has committed. Removing an
	// already-absent intent is idempotent (no error).
	CompleteConflictIntent(ctx context.Context, intentID chainhash.Hash) error

	// PendingConflictIntents returns every intent that was begun but not yet
	// completed — i.e. operations that may have been interrupted by a crash.
	// Called once at BlockAssembler startup to drive replay.
	PendingConflictIntents(ctx context.Context) ([]ConflictIntent, error)

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
