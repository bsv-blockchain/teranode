/*
Package validator implements BSV Blockchain transaction validation functionality.

This file implements option patterns for both general validation options and
transaction validator-specific options, providing flexible configuration for
validation operations.
*/
package validator

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

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

	// SkipTxMetaPublishing determines whether txmeta should be published to Kafka
	// When true, the validator won't publish transaction metadata to the txmeta Kafka topic
	// Used during legacy catchup (quickValidationMode) where no consumer needs the data
	SkipTxMetaPublishing bool

	// InBlock marks provenance: the transaction arrived as part of a block or
	// announced subtree (block validation, subtree validation, legacy sync)
	// rather than via mempool submission. Published on the txmeta topic so
	// relay consumers never announce such transactions. Explicit by design —
	// not inferred from SkipPolicyChecks, which external submitters may set
	// on genuinely fresh transactions.
	InBlock bool

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

	// UnconfirmedParentsAtCandidateHeight makes the unconfirmedParentHeight
	// sentinel resolve to the candidate block height instead of failing closed
	// (consensus-mode sentinel → MEMPOOL_HEIGHT → BDK rejects with
	// bad-txns-unconfirmed-input-in-block).
	//
	// Exists for block validation: a tx in a mined block that spends a
	// same-block parent finds that parent in the UTXO store with empty
	// BlockHeights (SetMinedMulti only runs after block acceptance), so the
	// validator stamps the sentinel and BDK rejects a legitimate block. On
	// those paths the candidate height IS the parent's true height, so the
	// substitution is exact — it feeds BDK's per-input protocol-era flag
	// selection and the BIP68/MTP lookups with the height the parent will be
	// mined at.
	//
	// Why "unconfirmed implies same-block" holds on the legacy path: the
	// sentinel fires for ANY empty-BlockHeights parent, which in general also
	// covers a parent mined in block N−1 whose asynchronous SetMinedMulti has
	// not landed yet (it would wrongly resolve to N instead of N−1). Legacy
	// netsync closes that window before validation starts:
	// waitForPreviousBlockMined (services/legacy/netsync/handle_block.go)
	// blocks until the previous block's mined-set completes, and since every
	// block waits on its parent, all ancestors' BlockHeights are recorded by
	// induction. Callers on other paths do NOT automatically get this
	// invariant and must establish it themselves before setting the flag.
	//
	// CONSENSUS SAFETY — fail-open, gate carefully. With this set, a parent
	// that is genuinely unconfirmed-and-NOT-in-the-block (a mempool floater)
	// is no longer rejected at tx level. The floater backstop is NOT here and
	// is NOT a synchronous checkParentsExistOnChain rejection before acceptance:
	// such a parent surfaces from block.Valid as ErrBlockIncomplete (model's
	// getParentTxMetaBlockIDs returns BlockIncompleteError for empty-BlockIDs),
	// which is NOT ErrBlockInvalid, and under optimistic mining the block has
	// already been added by the time block.Valid runs in the background. The
	// real backstop is block-validation's FSM-gated ErrBlockIncomplete handling
	// (BlockValidation.go: the optimistic background goroutine, the
	// non-optimistic path, and reValidateBlock, all via isCaughtUp): in a
	// caught-up state the floater block is invalidated/rolled back, in
	// CATCHINGBLOCKS it stays incomplete and is retried
	// (preserving #1031). Setting this flag is therefore only sound when ALL of:
	//   - the tx comes from a locally-held, PoW-checked block (not a peer
	//     announcement), AND
	//   - the FSM-gated ErrBlockIncomplete backstop in block validation will
	//     run (it always does, on every block.Valid path), AND
	//   - mempool/block-assembly contamination vectors are absent or
	//     acceptable (sync states run with block assembly disabled; in
	//     RUNNING the candidate-height substitution matches the everyday
	//     mempool policy-mode behaviour at tip+1).
	//
	// Block assembly: the flag is compatible with AddTXToBlockAssembly=true
	// (an earlier revision hard-errored on the combination; that broke
	// RUNNING-state legacy catch-up, where assembly must stay enabled for
	// reorg resilience). A floater child blessed at the candidate height
	// and added to assembly is the same tx policy-mode admission would have
	// accepted into assembly — policy substitutes tip+1 for unconfirmed
	// parents, equal to the candidate height at the tip, and era flags
	// cannot differ post-Genesis. Accepted-block txs are mined-removed from
	// assembly as always.
	//
	// The intended setters are subtreevalidation's checkSubtreeFromBlock
	// legacy branch and CheckBlockSubtrees (the block-validation path, which
	// runs after ValidateBlock's PoW checks). MUST NOT be set on peer-facing
	// subtree handling or mempool-admission paths.
	UnconfirmedParentsAtCandidateHeight bool

	// PrefetchedParents supplies parent-transaction metadata already read in
	// bulk by the caller (the per-level bulk reader on the catchup path), keyed
	// by parent tx hash. When a parent is present here the validator uses it
	// instead of issuing a per-parent utxoStore.Get, which deduplicates the
	// many reads of a shared parent in fan-out blocks and batches the rest.
	//
	// It is a pure read-source swap: the entry must carry exactly what a Get
	// would return for the requested fields (BlockIDs, BlockHeights, and Tx
	// when the tx needs extending). The unconfirmed-parent sentinel logic is
	// unchanged — an entry with empty BlockHeights still resolves to
	// unconfirmedParentHeight. The validator falls back to a store Get for any
	// parent absent from this map, or present but missing the Tx needed for
	// extension, so the prefetch can never reduce correctness. nil = always
	// read from the store (the non-catchup default).
	PrefetchedParents map[chainhash.Hash]*meta.Data

	// OutpointOnlySpend enables the below-checkpoint fast path:
	//   - Parent Get (block-heights + extend) is skipped entirely — utxoHeights
	//     is left nil, which is safe because SkipScriptValidation must also be
	//     set on this path, bypassing the BDK/BIP68 code that consumes heights.
	//   - Spend is issued with IgnoreFlags.SkipUTXOHashCheck=true so the SQL
	//     store builds spends via outpoint lookup (no UTXO-hash comparison).
	//   - Create is issued with utxo.WithSkipExtendedInputs(true) so fee
	//     computation is skipped for un-extended inputs.
	//   - The SkipUtxoCreation fallback calls util.TxMetaDataFromTxNoFee
	//     instead of util.TxMetaDataFromTx (which calls GetFees and errors on
	//     un-decorated inputs).
	//
	// MUST be paired with WithSkipScriptValidation(true). Default OFF — every
	// code path when false is byte-identical to the pre-existing behaviour.
	// The checkpoint gate is enforced by the caller (Stage C task C2); this
	// option simply honours the flag.
	OutpointOnlySpend bool
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

// WithInBlock creates an option marking the transaction as having arrived as
// part of a block or announced subtree rather than via mempool submission.
// Set this at every block-context validation call site; it controls whether
// relay consumers of the txmeta Kafka topic may announce the transaction.
//
// Parameters:
//   - inBlock: When true, the transaction is marked as block-originated
//
// Returns:
//   - Option: Function that sets the inBlock option
func WithInBlock(inBlock bool) Option {
	return func(o *Options) {
		o.InBlock = inBlock
	}
}

// WithSkipScriptValidation creates an option to skip the BDK transaction/script
// validation step. See Options.SkipScriptValidation for safety constraints.
func WithSkipScriptValidation(skip bool) Option {
	return func(o *Options) {
		o.SkipScriptValidation = skip
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

// WithUnconfirmedParentsAtCandidateHeight resolves unconfirmed-parent heights
// to the candidate block height instead of failing closed in consensus mode.
// See Options.UnconfirmedParentsAtCandidateHeight for the consensus-safety
// contract — only the legacy block-sync path may set this.
func WithUnconfirmedParentsAtCandidateHeight(enabled bool) Option {
	return func(o *Options) {
		o.UnconfirmedParentsAtCandidateHeight = enabled
	}
}

// WithOutpointOnlySpend enables the below-checkpoint outpoint-only fast path.
// See Options.OutpointOnlySpend for the full contract.
func WithOutpointOnlySpend(b bool) Option {
	return func(o *Options) {
		o.OutpointOnlySpend = b
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
