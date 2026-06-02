/*
Package validator implements BSV Blockchain transaction validation functionality.

This file implements option patterns for both general validation options and
transaction validator-specific options, providing flexible configuration for
validation operations.
*/
package validator

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// ParentTxMetadata holds metadata about a parent transaction needed for validation
// This allows the validator to skip UTXO store lookups for in-block parents
type ParentTxMetadata struct {
	BlockHeight uint32 // The block height where this transaction was mined
}

// Options defines the configuration options for validation operations
type Options struct {
	// SkipUtxoCreation determines whether UTXO creation should be skipped
	// When true, the validator won't create new UTXOs for transaction outputs
	SkipUtxoCreation bool

	// AddTXToBlockAssembly determines whether transactions should be added to block assembly
	// When true, validated transactions are forwarded to the block assembly process
	AddTXToBlockAssembly bool

	// SkipPolicyChecks determines whether policy checks should be skipped
	// this is done when validating transaction from a block that has been mined
	SkipPolicyChecks bool

	// CreateConflicting determines whether to allow conflicting transactions
	// this is done when validating transaction from a block that has been mined
	CreateConflicting bool

	// IgnoreConflicting determines whether to ignore transactions marked as conflicting when spending
	IgnoreConflicting bool

	// IgnoreLocked determines whether to ignore transactions marked as locked when spending
	IgnoreLocked bool

	// ParentMetadata provides pre-fetched metadata for parent transactions
	// When provided, the validator will check this map before calling utxoStore.Get()
	// This enables validation to proceed without UTXO store lookups for in-block parents
	// Key: parent transaction hash, Value: metadata (block height)
	ParentMetadata map[chainhash.Hash]*ParentTxMetadata

	// SkipTxMetaPublishing determines whether txmeta should be published to Kafka
	// When true, the validator won't publish transaction metadata to the txmeta Kafka topic
	// Used during legacy catchup (quickValidationMode) where no consumer needs the data
	SkipTxMetaPublishing bool

	// SkipScriptValidation determines whether BDK transaction/script validation should be skipped.
	// When true, the validator skips the BDK ValidateTransaction call (script execution,
	// sigops, standardness, consensus checks). Intended for legacy catchup
	// (quickValidationMode) where the block is anchored to a hard-coded checkpoint and
	// PoW + checkpoint linkage already establish the chain as canonical — re-running
	// scripts is pure overhead. MUST NOT be set on non-trusted validation paths.
	SkipScriptValidation bool

	// CandidateBlockTime carries the candidate block's own header timestamp
	// (block.Header.Timestamp) for block-validation finality on pre-CSV blocks.
	// Only consumed when SkipPolicyChecks=true AND blockHeight < CSVHeight, which
	// matches bitcoin-sv's ContextualCheckBlock comparing nLockTime against the
	// candidate block header time (src/validation.cpp:6020-6022) before CSV.
	// Policy mode and post-CSV consensus do not read this field — leaving it at
	// zero in those contexts is harmless.
	CandidateBlockTime uint32

	// CandidateParentMedianTime carries the equivalent of bitcoin-sv's
	// pindexPrev->GetMedianTimePast() for the candidate block being validated.
	// Only consumed when SkipPolicyChecks=true AND blockHeight >= CSVHeight,
	// matching bitcoin-sv's BIP113 activation at ContextualCheckBlock
	// (src/validation.cpp:6001), where post-activation finality is checked
	// against the parent's MTP rather than the candidate block's own timestamp.
	//
	// Required field on the consensus path. Block-validation callers MUST
	// populate this. selectFinalityComparisonTime returns a ProcessingError
	// when it is missing — there is no tip-MTP soft-fall. blockState.MedianTime
	// is updated asynchronously from blockchain notifications, so a tip
	// advance / reorg between the caller's snapshot and the validator's read
	// would silently swap the comparison time source; the hard-error stance
	// makes a forgotten populate-callsite fail fast at validation time
	// instead of silently degrading.
	//
	// Sourcing on the caller side: bitcoin-sv's pindexPrev->GetMedianTimePast()
	// for a candidate at height H is the median of timestamps at heights
	// [H-11, H-1] — i.e. the parent block's own timestamp AND its 10 previous
	// ancestors along the parent's actual chain (the bitcoin-sv routine
	// iterates `this` then `pprev` 10 times). Teranode's height-based MTP
	// lookups (GetMedianTimePastForHeights, GetMedianTimePastRange) are
	// restricted to the current main chain — wrong for a side-chain
	// candidate. The correct primitive is
	// blockchainClient.GetBlockHeaders(parentHash, 11), whose SQL fallback
	// recursively walks parent_id when the start hash is off main chain;
	// callers then take the median of the 11 returned headers' timestamps
	// (see services/legacy/netsync and services/subtreevalidation for the
	// reference implementations).
	CandidateParentMedianTime uint32
}

// Option defines a function type for setting options
// This follows the functional options pattern for flexible configuration
type Option func(*Options)

// NewDefaultOptions creates a new Options instance with default settings
// Default configuration:
//   - skipUtxoCreation: false (UTXOs will be created)
//   - addTXToBlockAssembly: true (transactions will be added to block assembly)
//
// Returns:
//   - *Options: New options instance with default settings
func NewDefaultOptions() *Options {
	return &Options{
		SkipUtxoCreation:     false,
		AddTXToBlockAssembly: true,
		SkipPolicyChecks:     false,
		CreateConflicting:    false,
	}
}

// ProcessOptions applies the provided options to a new Options instance
// Parameters:
//   - opts: Variable number of Option functions to apply
//
// Returns:
//   - *Options: Configured options instance
func ProcessOptions(opts ...Option) *Options {
	options := NewDefaultOptions()
	for _, o := range opts {
		o(options)
	}

	return options
}

// WithSkipUtxoCreation creates an option to control UTXO creation
// Parameters:
//   - skip: When true, UTXO creation will be skipped
//
// Returns:
//   - Option: Function that sets the skipUtxoCreation option
func WithSkipUtxoCreation(skip bool) Option {
	return func(o *Options) {
		o.SkipUtxoCreation = skip
	}
}

// WithAddTXToBlockAssembly creates an option to control block assembly integration (allows the transaction to be added to the block assembly or not)
// Parameters:
//   - add: When true, transactions will be added to block assembly
//
// Returns:
//   - Option: Function that sets the addTXToBlockAssembly option
func WithAddTXToBlockAssembly(add bool) Option {
	return func(o *Options) {
		o.AddTXToBlockAssembly = add
	}
}

// WithSkipPolicyChecks creates an option to control policy checks
// Parameters:
//   - skip: When true, policy checks will be skipped
//
// Returns:
//   - Option: Function that sets the skipPolicyChecks option
func WithSkipPolicyChecks(skip bool) Option {
	return func(o *Options) {
		o.SkipPolicyChecks = skip
	}
}

// WithCreateConflicting creates an option to control whether a conflicting transaction is created
// Parameters:
//   - create: When true, a conflicting transaction will be created
//
// Returns:
//   - Option: Function that sets the createConflicting option
func WithCreateConflicting(create bool) Option {
	return func(o *Options) {
		o.CreateConflicting = create
	}
}

// WithIgnoreConflicting creates an option to control whether a conflicting transaction is ignored
// Parameters:
//   - ignore: When true, a conflicting transaction will be ignored
//
// Returns:
//   - Option: Function that sets the ignoreConflicting option
func WithIgnoreConflicting(ignore bool) Option {
	return func(o *Options) {
		o.IgnoreConflicting = ignore
	}
}

// WithIgnoreLocked creates an option to control whether the locked flag will be ignored when spending UTXOs
// Parameters:
//   - ignoreLocked: When true, transactions marked as locked will also be processed
//
// Returns:
//   - Option: Function that sets the ignoreLocked option
func WithIgnoreLocked(ignoreLocked bool) Option {
	return func(o *Options) {
		o.IgnoreLocked = ignoreLocked
	}
}

// WithSkipTxMetaPublishing creates an option to control txmeta Kafka publishing
// Parameters:
//   - skip: When true, txmeta will not be published to Kafka
//
// Returns:
//   - Option: Function that sets the skipTxMetaPublishing option
func WithSkipTxMetaPublishing(skip bool) Option {
	return func(o *Options) {
		o.SkipTxMetaPublishing = skip
	}
}

// WithSkipScriptValidation creates an option to skip the BDK transaction/script
// validation step. See Options.SkipScriptValidation for safety constraints.
func WithSkipScriptValidation(skip bool) Option {
	return func(o *Options) {
		o.SkipScriptValidation = skip
	}
}

// WithParentMetadata creates an option to provide pre-fetched parent transaction metadata
// Parameters:
//   - metadata: Map of parent transaction hashes to their metadata (block height, etc.)
//
// Returns:
//   - Option: Function that sets the parentMetadata option
func WithParentMetadata(metadata map[chainhash.Hash]*ParentTxMetadata) Option {
	return func(o *Options) {
		o.ParentMetadata = metadata
	}
}

// WithCandidateBlockTime creates an option carrying the candidate block's
// own header timestamp. Required by block-validation callers when validating
// pre-CSV blocks; ignored in other contexts. See Options.CandidateBlockTime.
//
// Parameters:
//   - timestamp: The candidate block's Header.Timestamp value
//
// Returns:
//   - Option: Function that sets the candidateBlockTime option
func WithCandidateBlockTime(timestamp uint32) Option {
	return func(o *Options) {
		o.CandidateBlockTime = timestamp
	}
}

// WithCandidateParentMedianTime creates an option carrying the candidate
// block's parent-chain MTP (the equivalent of bitcoin-sv's
// pindexPrev->GetMedianTimePast()). Required by block-validation callers on
// every post-CSV consensus request — selectFinalityComparisonTime returns
// a ProcessingError when this field is missing on that path (no tip-MTP
// soft-fall). See Options.CandidateParentMedianTime for the rationale of
// the hard-error stance.
//
// Parameters:
//   - mtp: The candidate-parent-chain MTP computed at the caller — i.e. the
//     median of timestamps at [H-11, H-1] following the parent's actual
//     chain (use blockchainClient.GetBlockHeaders(parentHash, 11) which has
//     a fork-aware fallback, then take the median of the returned headers'
//     timestamps).
//
// Returns:
//   - Option: Function that sets the candidateParentMedianTime option
func WithCandidateParentMedianTime(mtp uint32) Option {
	return func(o *Options) {
		o.CandidateParentMedianTime = mtp
	}
}

// TxValidatorOptions defines configuration options specific to transaction validation
type TxValidatorOptions struct {
	skipPolicyChecks bool
}

// NewTxValidatorOptions creates a new TxValidatorOptions instance with the provided options applied.
func NewTxValidatorOptions(opts ...TxValidatorOption) *TxValidatorOptions {
	options := &TxValidatorOptions{}
	for _, opt := range opts {
		opt(options)
	}

	return options
}

// TxValidatorOption defines a function type for setting transaction validator options
// This follows the functional options pattern for flexible configuration
type TxValidatorOption func(*TxValidatorOptions)

// WithTxValidatorSkipPolicyChecks creates an option to skip policy checks during validation.
func WithTxValidatorSkipPolicyChecks(skip bool) TxValidatorOption {
	return func(o *TxValidatorOptions) {
		o.skipPolicyChecks = skip
	}
}
