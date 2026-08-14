/*
Package validator implements BSV Blockchain transaction validation functionality.

This package provides comprehensive transaction validation for BSV Blockchain nodes,
including BDK transaction validation, UTXO management, and policy enforcement.
*/
package validator

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/batchermetrics"
	"github.com/bsv-blockchain/teranode/util/health"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/cespare/xxhash/v2"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// Constants defining key validation parameters and limits for Bitcoin consensus rules.
// These constants establish the fundamental constraints that govern transaction and block validation,
// ensuring compliance with Bitcoin protocol specifications and network consensus requirements.
const (
	// MaxSatoshis defines the maximum number of satoshis that can exist in the Bitcoin SV ecosystem (21M BSV).
	// This represents the absolute monetary supply limit, with each BSV consisting of 100,000,000 satoshis.
	// Any transaction that would create more satoshis than this limit violates consensus rules and must be
	// rejected to maintain the integrity of the monetary system and prevent inflation attacks.
	MaxSatoshis = 21_000_000_00_000_000

	// maxAggregatedSpendErrs caps how many per-spend errors are wrapped into the
	// aggregate attached to a failed-validation error. The failure count scales
	// with the tx's input count; an uncapped chain makes error construction and
	// every errors.Is on it quadratic. See errors.JoinCapped.
	maxAggregatedSpendErrs = 10

	// kafkaShedRetryBackoff is the pause between in-place block-assembly handoff
	// retries on the Kafka ingest path when the queue is full. Short so the
	// transaction reaches a mining template promptly once the queue drains,
	// while the loop selects on the request context so shutdown aborts it.
	kafkaShedRetryBackoff = 5 * time.Millisecond

	// defaultBlockAssemblyShedRetryTimeout bounds the in-place handoff retry when
	// Validator.BlockAssemblyShedRetryTimeout is unset. It mirrors the setting's
	// documented default so a Settings struct built directly in a test still gets
	// a bounded retry rather than none at all.
	defaultBlockAssemblyShedRetryTimeout = 2 * time.Second

	// shedUnwindVerifyAttempts and shedUnwindVerifyBackoff bound the read-back that
	// confirms Delete actually deleted. A single read conflates "the record
	// survived" with "the read failed", and only the first justifies aborting;
	// retrying converts most transient store errors into a definitive answer before
	// the unwind gives up. Small on purpose: this runs on a path that only executes
	// when the node is already shedding.
	shedUnwindVerifyAttempts = 3
	shedUnwindVerifyBackoff  = 5 * time.Millisecond

	// defaultHandoffRoundTripSlack is the allowance added to block assembly's own
	// bounded queue wait when sizing the per-attempt hand-off deadline. It covers the
	// gRPC round trip, the batcher dispatch and the server's queue-full poll
	// granularity.
	//
	// Deliberately small, because it is subtracted from the ingest retry window (see
	// Validator.handoffFloor). Operators size it with validator_handoffRoundTripSlack;
	// this is the fallback for a Settings struct that never went through the loader.
	defaultHandoffRoundTripSlack = 500 * time.Millisecond

	// defaultShedUnwindTimeout is the fallback for validator_shedUnwindTimeout, which
	// bounds the whole unwind. The context reaching the unwind is detached from the
	// caller by design, so this is what stops a wedged store from parking an ingest
	// goroutine on a best-effort cleanup path. It sits OUTSIDE the hand-off budget —
	// the unwind only starts once the hand-off has given up — so an ingest goroutine's
	// total retention is BlockAssemblyShedRetryTimeout + ShedUnwindTimeout.
	//
	// It is ONE budget for the whole sequence, not a per-phase one: the Delete, up to
	// shedUnwindVerifyAttempts verify reads with shedUnwindVerifyBackoff between them,
	// and the Unspend all share it. Under a slow store the verify read therefore gets
	// fewer attempts than shedUnwindVerifyAttempts before the context short-circuits
	// it, and the unwind fails closed having effectively tried once — which is why the
	// setting's longdesc carries a sizing rule keyed on the store's P99 rather than
	// just a default.
	//
	// 2s covers one Delete, up to shedUnwindVerifyAttempts verify reads and one Unspend
	// against a store whose healthy latency is sub-millisecond.
	defaultShedUnwindTimeout = 2 * time.Second
)

// twoPhaseCommitTimeout bounds the 2PC unlock. The context reaching it is detached
// from the caller by design, so this is what stops a wedged store from parking an
// ingest goroutine on post-acceptance bookkeeping. It does NOT widen the retention
// figure the validator_blockAssemblyShedRetryTimeout longdesc quotes: the shed
// unwind and the 2PC unlock are mutually exclusive (a shed returns at Validate's
// send-failure arm and never reaches the commit), so the worst case is
// max(ShedUnwindTimeout, twoPhaseCommitTimeout), not their sum.
//
// A var only so tests can shrink it.
var twoPhaseCommitTimeout = 2 * time.Second

const (

	// coinbaseTxID represents the special transaction ID used for coinbase transactions.
	// Coinbase transactions are the first transaction in each block and create new bitcoins as mining rewards.
	// This constant is used to identify and handle coinbase transactions differently from regular transactions
	// during validation, as they have special rules and don't spend existing UTXOs.
	coinbaseTxID = "0000000000000000000000000000000000000000000000000000000000000000"

	// DustLimit defines the minimum output value in satoshis (1 satoshi)
	// Outputs with less than this value are considered dust unless they are
	// not spendable (OP_FALSE OP_RETURN).  This applies to outputs after the
	// Genesis upgrade.
	DustLimit = uint64(1)

	// unconfirmedParentHeight is the teranode-internal sentinel written into
	// utxoHeights when a parent transaction is not present in the UTXO store
	// with recorded block heights (i.e. the parent UTXO is not yet confirmed).
	//
	// Chosen as 0xFFFFFFFF — an impossible block height (no real chain reaches
	// 4.29 billion blocks) — so it cannot collide with any value produced by
	// the only other height-population branch (a UTXO-store hit, which uses the
	// real stored height). The collision matters because in mainline block
	// validation `blockState.Height + 1` equals the candidate height, making
	// height-based identification of unconfirmed slots ambiguous.
	//
	// It is **distinct** from BDK / svnode's MEMPOOL_HEIGHT = 0x7FFFFFFF on
	// purpose: that constant is a BDK-adapter concept and lives only inside
	// ScriptVerifierGoBDK.ValidateTransaction, which translates this sentinel
	// outward (→ MEMPOOL_HEIGHT in consensus mode so BDK rejects with
	// bad-txns-unconfirmed-input-in-block; → the candidate block height in
	// policy mode, matching svnode's GetInputScriptBlockHeight conversion at
	// bitcoin-sv/src/validation.cpp:2668).
	unconfirmedParentHeight uint32 = 0xFFFFFFFF
)

// Txmeta Kafka wire-format constants live in stores/txmetacache (see wire.go
// in that package). They are imported here as the single source of truth
// shared between the producer (this package) and all consumers
// (services/subtreevalidation, services/legacy/netsync, ...).

// txmetaBatchItem represents an item to be batched for TxMeta Kafka messages.
type txmetaBatchItem struct {
	hash      *chainhash.Hash
	metaBytes []byte
	isDelete  bool
}

// Validator implements comprehensive BSV Blockchain transaction validation and manages the complete lifecycle
// of transactions from initial validation through block assembly integration. This struct serves as the
// primary validation engine, coordinating between multiple components to ensure transaction validity
// according to Bitcoin consensus rules and policy constraints.
//
// The Validator orchestrates the validation process by:
// - Performing structural and semantic transaction validation
// - Executing Bitcoin scripts and verifying signatures
// - Managing UTXO state transitions and double-spend prevention
// - Coordinating with block assembly for transaction inclusion
// - Handling both individual and batch validation scenarios

type Validator struct {
	// logger provides structured logging capabilities for the validator, enabling comprehensive
	// monitoring and debugging of validation operations. All validation activities, errors, and
	// performance metrics are logged through this component for operational visibility and troubleshooting.
	logger ulogger.Logger

	// settings contains the complete configuration for the validator, including consensus parameters,
	// policy rules, network settings, and operational thresholds. These settings control the behavior
	// of all validation operations and determine how strictly various rules are enforced.
	settings *settings.Settings

	// txValidator performs the core transaction-specific validation checks including structure validation,
	// input/output verification, script execution, and consensus rule enforcement. This component
	// implements the detailed validation logic that determines transaction validity.
	txValidator TxValidatorI

	// utxoStore manages the UTXO set and transaction metadata, providing access to unspent transaction
	// outputs for input validation and double-spend prevention. This store maintains the current state
	// of all UTXOs and enables efficient lookup and verification of transaction inputs.
	utxoStore utxo.Store

	// blockAssembler handles block template creation and transaction ordering for mining operations.
	// This component coordinates with the validator to include validated transactions in block templates
	// and manages the prioritization and selection of transactions for block inclusion.
	blockAssembler blockassembly.Store

	// blockchainClient provides access to the blockchain service for block-related operations,
	// including block height retrieval, chain state verification, and FSM synchronization.
	// This client is used to ensure the validator service remains synchronized with the blockchain.
	blockchainClient blockchain.ClientI

	// stats tracks validator performance metrics
	stats *gocore.Stat

	// txmetaKafkaProducerClient publishes transaction metadata events
	txmetaKafkaProducerClient kafka.KafkaAsyncProducerI

	// rejectedTxKafkaProducerClient publishes rejected transaction events
	rejectedTxKafkaProducerClient kafka.KafkaAsyncProducerI

	// policyRejectedTxKafkaProducerClient publishes consensus-valid transactions that were
	// rejected by local policy (ErrTxPolicy). This is a separate topic from
	// rejectedTxKafkaProducerClient for two reasons:
	//   1. Different message schema: this topic carries the full raw tx bytes
	//      (KafkaTxPolicyRejectedTopicMessage.RawTx) so that consumers can reconstruct
	//      the transaction without an extra HTTP roundtrip. The rejected-tx topic only
	//      carries {TxHash, Reason, PeerId} and is not suitable for raw-byte delivery.
	//   2. Different consumers: subtree validation pods consume this topic to populate a
	//      local cache of policy-rejected txs; the rejected-tx topic is consumed by P2P
	//      gossip components that only need the hash and rejection reason.
	// Merging the two topics would require either sending raw bytes for every rejection
	// (wasted bandwidth) or adding a type tag that consumers must filter (added complexity).
	policyRejectedTxKafkaProducerClient kafka.KafkaAsyncProducerI

	// txmetaKafkaBatcher batches TxMeta Kafka messages for efficient publishing
	txmetaKafkaBatcher *batcher.Batcher[txmetaBatchItem]

	// mtpStore is a dense in-memory array of Median Time Past values indexed by block height.
	// mtpStore[h] = MTP for block h. Loaded from height 0 up to (blockHeight - 1) before
	// each block's transactions are validated, then extended on demand as new heights arrive.
	//
	// MTP values are immutable once a block is persisted, so entries never need invalidation.
	// Memory cost: ~4 MB per million blocks (one uint32 per block), negligible for any
	// foreseeable chain length.
	//
	// mtpMu guards concurrent access to mtpStore.
	//   - EnsureMTPLoaded acquires the write lock for the duration of the fetch + append +
	//     in-place overlap patch. Concurrent EnsureMTPLoaded callers serialise; the second
	//     one fast-paths out after acquiring the lock if the first already populated the
	//     range it needs.
	//   - validateTransaction acquires the read lock around its MTP lookups. This protects
	//     against the cross-block case where block N's per-tx goroutines are still reading
	//     while block N+1's EnsureMTPLoaded is appending or patching overlap entries (the
	//     append re-allocates the backing array; the in-place patch mutates indices that
	//     readers may be addressing).
	// Same-block contention is negligible: EnsureMTPLoaded runs once per block before per-tx
	// goroutines start, and per-tx readers only contend with each other on the read lock.
	mtpMu    sync.RWMutex
	mtpStore []uint32
}

// New creates a new Validator instance with the provided configuration.
// It initializes the validator with the given logger, UTXO store, and Kafka producers.
// Returns an error if initialization fails.
func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, store utxo.Store,
	txMetaKafkaProducerClient kafka.KafkaAsyncProducerI, rejectedTxKafkaProducerClient kafka.KafkaAsyncProducerI,
	policyRejectedTxKafkaProducerClient kafka.KafkaAsyncProducerI,
	blockAssemblyClient blockassembly.ClientI, blockchainClient blockchain.ClientI) (Interface, error) {
	initPrometheusMetrics()

	var ba blockassembly.Store

	if !tSettings.BlockAssembly.Disabled {
		ba = blockAssemblyClient
	}

	v := &Validator{
		logger:                              logger,
		settings:                            tSettings,
		txValidator:                         NewTxValidator(logger, tSettings),
		utxoStore:                           store,
		blockAssembler:                      ba,
		stats:                               gocore.NewStat("validator"),
		txmetaKafkaProducerClient:           txMetaKafkaProducerClient,
		rejectedTxKafkaProducerClient:       rejectedTxKafkaProducerClient,
		policyRejectedTxKafkaProducerClient: policyRejectedTxKafkaProducerClient,
		blockchainClient:                    blockchainClient,
	}

	// The ingest hand-off subtracts the per-attempt floor from the retry window, so a
	// window that cannot accommodate even one attempt leaves no room to retry at all.
	// That is a legal configuration and it stays honoured — a shed must always be
	// given the remote's full queue wait or it comes back misclassified — but the
	// effective bound is then the floor rather than the configured window, which an
	// operator should not have to derive from the source.
	if floor := v.handoffFloor(); floor >= v.shedRetryTimeout() {
		logger.Warnf("[Validator] validator_blockAssemblyShedRetryTimeout=%s is not greater than the block-assembly handoff floor %s (blockassembly_queueFullWaitTimeout=%s plus validator_handoffRoundTripSlack=%s), so a queue-full handoff makes one bounded attempt with no retries and the effective ingest stall bound is %s, not the configured window; to restore retries either raise validator_blockAssemblyShedRetryTimeout above the floor or lower validator_handoffRoundTripSlack", v.shedRetryTimeout(), floor, tSettings.BlockAssembly.QueueFullWaitTimeout, v.handoffRoundTripSlack(), floor)
	}

	txmetaKafkaURL := v.settings.Kafka.TxMetaConfig
	if txmetaKafkaURL == nil {
		return nil, errors.NewConfigurationError("missing Kafka URL for txmeta")
	}

	if v.txmetaKafkaProducerClient != nil { // tests may not set this
		v.txmetaKafkaProducerClient.Start(ctx, make(chan *kafka.Message, 10_000))
	}

	if v.rejectedTxKafkaProducerClient != nil { // tests may not set this
		v.rejectedTxKafkaProducerClient.Start(ctx, make(chan *kafka.Message, 10_000))
	}

	if v.policyRejectedTxKafkaProducerClient != nil {
		v.policyRejectedTxKafkaProducerClient.Start(ctx, make(chan *kafka.Message, 10_000))
	}

	// Initialize TxMeta Kafka batcher if batch size is configured
	txmetaKafkaBatchSize := tSettings.Validator.TxMetaKafkaBatchSize
	txmetaKafkaBatchTimeout := tSettings.Validator.TxMetaKafkaBatchTimeoutMs
	if txmetaKafkaBatchSize > 0 && v.txmetaKafkaProducerClient != nil {
		duration := time.Duration(txmetaKafkaBatchTimeout) * time.Millisecond
		sendBatch := func(batch []*txmetaBatchItem) {
			v.sendTxMetaBatch(batch)
		}
		b := batcher.NewWithPool(txmetaKafkaBatchSize, duration, sendBatch, true,
			batcher.WithName("validator_txmeta_kafka"),
			batcher.WithLogger(logger),
			batcher.WithMetrics(batchermetrics.Provider()),
			batcher.WithTracer(tracing.Tracer("validator").OTelTracer()),
		)
		if ms := tSettings.Validator.TxMetaKafkaBatchTickerIntervalMillis; ms > 0 {
			b.SetTickInterval(time.Duration(ms) * time.Millisecond)
		}
		v.txmetaKafkaBatcher = b
		logger.Infof("TxMeta Kafka batching enabled: batchSize=%d, timeout=%dms", txmetaKafkaBatchSize, txmetaKafkaBatchTimeout)
	}

	return v, nil
}

// Close releases the resources the Validator owns: the tx-meta batcher and the
// three async Kafka producers (txmeta / rejectedTx / policyRejectedTx). It
// mirrors validator.Server.Stop()'s ordering — drain the tx-meta batcher FIRST
// so queued tx-meta flushes INTO the producer, THEN stop the producers so their
// final flush runs during shutdown.
//
// This is the teardown for the local-validator path (UseLocalValidator=true),
// where the daemon owns the *Validator directly and closes it during shutdown;
// the Server-wrapped path drives the same drain+stop from Server.Stop().
//
// Idempotent and nil-guarded: the batcher Close (go-batcher v2.0.4) and each
// producer Stop are safe to call more than once, so a repeated Close — or an
// overlap with Server.Stop() — does no harm. The drain is bounded by
// DefaultBatcherDrainTimeout so a wedged flush cannot stall shutdown. Close()
// takes no ctx, so each producer Stop() is raced against an internal timeout of
// the same bound: a wedged broker flush can't block shutdown, and the outstanding
// Stop() finishes the flush later if it can.
func (v *Validator) Close() error {
	if v.txmetaKafkaBatcher != nil {
		util.DrainBatcher(v.logger, "validator_txmeta_batcher", util.DefaultBatcherDrainTimeout, v.txmetaKafkaBatcher.Close)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), util.DefaultBatcherDrainTimeout)
	defer cancel()

	kafka.StopProducerCtx(stopCtx, v.logger, "validator txmeta", v.txmetaKafkaProducerClient)
	kafka.StopProducerCtx(stopCtx, v.logger, "validator rejectedTx", v.rejectedTxKafkaProducerClient)
	kafka.StopProducerCtx(stopCtx, v.logger, "validator policy-rejected tx", v.policyRejectedTxKafkaProducerClient)

	return nil
}

// Health performs health checks on the validator and its dependencies.
// When checkLiveness is true, only checks service liveness.
// When false, performs full readiness check including dependencies.
// Returns HTTP status code, status message, and error if any.
func (v *Validator) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if checkLiveness {
		// Add liveness checks here. Don't include dependency checks.
		// If the service is stuck return http.StatusServiceUnavailable
		// to indicate a restart is needed
		return http.StatusOK, "OK", nil
	}

	// Add readiness checks here. Include dependency checks.
	// If any dependency is not ready, return http.StatusServiceUnavailable
	// If all dependencies are ready, return http.StatusOK
	// A failed dependency check does not imply the service needs restarting
	start, stat, _ := tracing.NewStatFromContext(ctx, "Health", v.stats)
	defer stat.AddTime(start)

	checkBlockHeight := func(ctx context.Context, checkLiveness bool) (int, string, error) {
		var (
			sb  strings.Builder
			err error
		)

		blockHeight := v.GetBlockHeight()

		switch {
		case blockHeight == 0:
			err := errors.NewProcessingError("error getting blockHeight from validator: 0")
			_, _ = sb.WriteString(fmt.Sprintf("BlockHeight: BAD: %v,", err))
		case blockHeight <= 0:
			err = errors.NewProcessingError("blockHeight <= 0")
			_, _ = sb.WriteString(fmt.Sprintf("BlockHeight: BAD: %d,", blockHeight))
		default:
			_, _ = sb.WriteString(fmt.Sprintf("BlockHeight: GOOD: %d,", blockHeight))
		}

		if err != nil {
			return http.StatusFailedDependency, sb.String(), err
		}

		return http.StatusOK, sb.String(), nil
	}

	var brokersURL []string
	if v.rejectedTxKafkaProducerClient != nil { // tests may not set this
		brokersURL = v.rejectedTxKafkaProducerClient.BrokersURL()
	}

	checks := make([]health.Check, 0, 3)
	checks = append(checks, health.Check{Name: "Kafka", Check: kafka.HealthChecker(ctx, brokersURL)})
	checks = append(checks, health.Check{Name: "BlockHeight", Check: checkBlockHeight})

	if v.utxoStore != nil {
		checks = append(checks, health.Check{Name: "UTXOStore", Check: v.utxoStore.Health})
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

// GetBlockHeight returns the current block height from the UTXO store.
func (v *Validator) GetBlockHeight() uint32 {
	return v.utxoStore.GetBlockHeight()
}

// GetMedianBlockTime returns the median block time from the UTXO store.
func (v *Validator) GetMedianBlockTime() uint32 {
	return v.utxoStore.GetMedianBlockTime()
}

// GetBlockState returns the block height and median block time from the UTXO
// store as one snapshot: the store keeps the pair behind a single atomic
// pointer, so a reader can never pair a new height with a stale median time
// mid-read (issue 1443). Whether both values also describe the same chain tip
// depends on the writer — the blockchain notification listener publishes them
// together with SetBlockState, which is the production path; the single-field
// setters update one field at a time and carry the other forward.
func (v *Validator) GetBlockState() utxo.BlockState {
	return v.utxoStore.GetBlockState()
}

// selectFinalityComparisonTime returns the time value to compare nLockTime
// against, plus a flag indicating that finality should be skipped entirely
// for this combination of context.
//
//	Policy mode (!SkipPolicyChecks): tip MTP in all eras. Matches bitcoin-sv's
//	TxnValidation calling StandardNonFinalVerifyFlags (src/policy/policy.h),
//	which unconditionally sets LOCKTIME_MEDIAN_TIME_PAST — no Genesis / CSV
//	gating, no GetAdjustedTime() fallback.
//
//	Consensus mode (SkipPolicyChecks=true):
//	- blockHeight < CSVHeight  → candidate block header time, supplied by the
//	  caller via Options.CandidateBlockTime. Matches bitcoin-sv
//	  ContextualCheckBlock at src/validation.cpp:6020-6022, which uses
//	  block.GetBlockTime() for pre-CSV blocks. When the caller does not
//	  supply a value (zero), this returns skipFinality=true rather than
//	  fabricating one — block-context callers that haven't migrated yet
//	  keep their previous skip-finality behaviour, no regression.
//	- blockHeight >= CSVHeight → candidate-parent MTP (equivalent to
//	  bitcoin-sv's pindexPrev->GetMedianTimePast() at src/validation.cpp:6001
//	  once BIP113 activates), supplied by the caller via
//	  Options.CandidateParentMedianTime. All block-validation callers MUST
//	  populate this field — there is no tip-MTP fallback. Missing values
//	  return a ProcessingError so a forgotten populate-callsite cannot
//	  silently degrade to blockState.MedianTime (which is updated
//	  asynchronously from blockchain notifications and would race with tip
//	  advance / reorg during validation). The hard-error stance replaces an
//	  earlier doc-only contract that proved fragile under review.
func selectFinalityComparisonTime(opts *Options, blockHeight uint32, csvHeight uint32, blockState utxo.BlockState) (comparisonTime uint32, skipFinality bool, err error) {
	switch {
	case !opts.SkipPolicyChecks:
		if blockState.MedianTime == 0 {
			return 0, false, errors.NewProcessingError("utxo store not ready, block height: %d, median block time: %d", blockHeight, blockState.MedianTime)
		}

		return blockState.MedianTime, false, nil
	case blockHeight < csvHeight:
		if opts.CandidateBlockTime == 0 {
			return 0, true, nil
		}

		return opts.CandidateBlockTime, false, nil
	default:
		// blockHeight >= csvHeight: use the caller-supplied candidate-parent MTP.
		// No tip-MTP soft-fall — a missing value is a caller-side bug and we
		// surface it instead of silently picking blockState.MedianTime (which
		// races with asynchronous tip-advance / reorg updates).
		if opts.CandidateParentMedianTime == 0 {
			return 0, false, errors.NewProcessingError("post-CSV consensus path requires Options.CandidateParentMedianTime, got zero (block height: %d, csv height: %d)", blockHeight, csvHeight)
		}

		return opts.CandidateParentMedianTime, false, nil
	}
}

// Validate performs comprehensive validation of a transaction.
// It checks transaction finality, validates inputs and outputs, updates the UTXO set,
// and optionally adds the transaction to block assembly.
// Returns error if validation fails.
func (v *Validator) Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...Option) (txMeta *meta.Data, err error) {
	return v.ValidateWithOptions(ctx, tx, blockHeight, ProcessOptions(opts...))
}

// ValidateWithOptions performs comprehensive validation of a transaction with explicit options.
// This method is the core transaction validation entry point that implements the full Bitcoin
// validation ruleset. It delegates to validateInternal for the actual validation logic and
// handles rejected transaction reporting via Kafka when validation fails.
//
// The validation process includes:
// - Script signature verification
// - Double-spend detection
// - Transaction format validation
// - UTXO existence verification
// - Fee calculation and policy enforcement
// - Block assembly integration (if enabled)
//
// When validation fails with errors other than storage or service errors, the transaction
// is reported to the rejected transaction Kafka topic for monitoring and analysis.
//
// Parameters:
//   - ctx: Context for the validation operation, used for tracing and cancellation
//   - tx: Transaction to validate, must be properly initialized
//   - blockHeight: Current blockchain height to validate against
//   - validationOptions: Options controlling validation behavior and policy enforcement
//
// Returns:
//   - *meta.Data: Transaction metadata if validation succeeds, includes fee calculations
//   - error: Detailed validation error if validation fails, nil on success
func (v *Validator) ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (txMetaData *meta.Data, err error) {
	// Use context-aware logger for trace correlation
	ctxLogger := v.logger.WithTraceContext(ctx)
	ctxLogger.Debugf("[ValidateWithOptions] Validate tx %s", tx.TxID())

	// Configurable retry for TX_LOCKED and TX_CREATING errors with exponential backoff.
	// Both occur when a parent and child tx arrive nearly simultaneously and the
	// parent hasn't finished its own two-phase commit yet: TX_LOCKED is the normal
	// path, TX_CREATING is what a large, multi-record parent returns while it is
	// still being written. Either way the child is spending an output of a parent
	// that is still committing, and the condition clears on its own (or once the
	// parent is mined) rather than needing distinct handling. The retry budget is
	// shared and left as the existing validator_txlocked_maxRetries setting rather
	// than a new one, since both cases need the same short tolerance for the same
	// underlying race. Set maxRetries to 0 to disable and return the error immediately
	// to the caller.
	maxRetries := v.settings.Validator.TxLockedMaxRetries
	if maxRetries < 0 {
		ctxLogger.Errorf("[ValidateWithOptions] invalid TxLockedMaxRetries (%d); clamping to 0", maxRetries)
		maxRetries = 0
	}
	const maxSafeRetries = 10 // cap to prevent excessive backoff (2^10 * 10ms ≈ 10s max single sleep)
	if maxRetries > maxSafeRetries {
		ctxLogger.Warnf("[ValidateWithOptions] TxLockedMaxRetries (%d) exceeds safe limit; clamping to %d", maxRetries, maxSafeRetries)
		maxRetries = maxSafeRetries
	}
	const baseBackoff = 10 * time.Millisecond

	// Loop runs maxRetries+1 times: 1 initial attempt + maxRetries retries.
	// e.g. maxRetries=3 → attempts 0,1,2,3 → 1 initial + 3 retries with 10/20/40ms backoff.
	for attempt := 0; attempt <= maxRetries; attempt++ {
		txMetaData, err = v.validateInternal(ctx, tx, blockHeight, validationOptions)

		// If no error, or the error is neither TX_LOCKED nor TX_CREATING, break immediately (don't retry)
		locked := errors.Is(err, errors.ErrTxLocked)
		creating := errors.Is(err, errors.ErrTxCreating)
		if err == nil || (!locked && !creating) {
			break
		}

		condition := "TX_LOCKED"
		if creating {
			condition = "TX_CREATING"
		}

		// TX_LOCKED/TX_CREATING error on the last attempt — give up. This is the
		// alertable signal that the budget is too short for this node's load: BSV has no
		// mempool, so on the client-facing paths the transaction is simply gone — the
		// propagation surfaces hand the submitter a 409 and keep nothing, and the Kafka
		// intake runs WithLogErrorAndMoveOn. The one path that survives it is legacy p2p
		// relay, which parks the tx in netsync's orphan pool and revalidates it once when
		// that entry is evicted.
		if attempt >= maxRetries {
			prometheusValidatorParentCommitExhausted.WithLabelValues(condition).Inc()
			ctxLogger.Warnf("[ValidateWithOptions] %s for tx %s after %d retries, giving up: %v", condition, tx.TxID(), attempt, err)

			break
		}

		// Exponential backoff: 10ms, 20ms, 40ms, ...
		// Counted rather than logged at Info: the give-up path above warns, but without the
		// succeeded-after-retrying side there is no denominator, and at Teranode's
		// transaction rates a log line per retry would be a flood.
		prometheusValidatorParentCommitRetries.WithLabelValues(condition).Inc()

		backoff := time.Duration(1<<uint(attempt)) * baseBackoff
		ctxLogger.Debugf("[ValidateWithOptions] %s for tx %s, retrying in %v (retry %d/%d): %v", condition, tx.TxID(), backoff, attempt+1, maxRetries, err)

		select {
		case <-ctx.Done():
			return txMetaData, ctx.Err()
		case <-time.After(backoff):
		}
	}

	if err != nil {
		if v.rejectedTxKafkaProducerClient != nil { // tests may not set this
			// TODO should this also announce transactions with missing parents etc.?
			if errors.Is(err, errors.ErrTxInvalid) {
				if v.blockchainClient != nil {
					var (
						state *blockchain.FSMStateType
						err1  error
					)

					if state, err1 = v.blockchainClient.GetFSMCurrentState(ctx); err1 != nil {
						ctxLogger.Errorf("[ValidateWithOptions] failed to publish rejected tx - error getting blockchain FSM state: %v", err1)

						return
					}

					if *state == blockchain_api.FSMStateType_CATCHINGBLOCKS {
						// ignore notifications while syncing or catching up
						return
					}
				}

				startKafka := time.Now()

				txID := tx.TxIDChainHash().String()

				m := &kafkamessage.KafkaRejectedTxTopicMessage{
					TxHash: txID,
					Reason: err.Error(),
					PeerId: "", // Empty peer_id indicates internal rejection
				}

				value, marshalErr := proto.Marshal(m)
				if marshalErr != nil {
					ctxLogger.Errorf("[ValidateWithOptions] failed to marshal rejected tx message: %v", marshalErr)
				} else {
					v.rejectedTxKafkaProducerClient.Publish(&kafka.Message{
						Key:   []byte(txID),
						Value: value,
					})
				}

				prometheusValidatorSendToP2PKafka.Observe(float64(time.Since(startKafka).Microseconds()) / 1_000_000)
			}
		}

		// Publish consensus-valid but policy-rejected transactions so subtree validation
		// pods can cache the raw tx bytes and avoid HTTP roundtrips to other miners.
		if errors.Is(err, errors.ErrTxPolicy) {
			v.publishPolicyRejectedTx(ctx, ctxLogger, tx, err)
		}
	}

	return txMetaData, err
}

// publishPolicyRejectedTx publishes the raw bytes of a policy-rejected transaction to
// the KAFKA_TX_POLICY_REJECTED topic. Subtree validation pods consume from this topic
// to populate a local cache, avoiding expensive HTTP fetches when a subtree from another
// miner contains transactions our node rejected on policy grounds.
func (v *Validator) publishPolicyRejectedTx(ctx context.Context, ctxLogger ulogger.Logger, tx *bt.Tx, validationErr error) {
	if v.policyRejectedTxKafkaProducerClient == nil {
		return
	}

	// Stay quiet while catching up, mirroring the rejected-tx producer above.
	// During CATCHINGBLOCKS the node replays large volumes of historical
	// transactions; publishing a policy-rejected message for every one would flood the
	// topic with cache entries that subtree validation does not need yet.
	if v.blockchainClient != nil {
		state, err := v.blockchainClient.GetFSMCurrentState(ctx)
		if err != nil {
			ctxLogger.Errorf("[publishPolicyRejectedTx] failed to get blockchain FSM state: %v", err)
			return
		}

		if *state == blockchain_api.FSMStateType_CATCHINGBLOCKS {
			return
		}
	}

	// Skip oversized transactions before serializing (tx.Size() is computed, not
	// allocated): the broker rejects messages over message.max.bytes, and consumers
	// skip txs over maxCachedTxBytes anyway. Skipping is lossless — subtree validation
	// falls back to the HTTP fetch path on a cache miss. Same pattern as propagation's
	// large-tx HTTP fallback (see PropagationServer.ProcessTransaction).
	if maxBytes := v.settings.Validator.KafkaMaxMessageBytes; maxBytes > 0 && tx.Size() > maxBytes {
		ctxLogger.Debugf("[publishPolicyRejectedTx] skipping tx %s: size %d exceeds validator_kafka_maxMessageBytes %d", tx.TxIDChainHash().String(), tx.Size(), maxBytes)
		return
	}

	txHash := tx.TxIDChainHash()

	m := &kafkamessage.KafkaTxPolicyRejectedTopicMessage{
		TxHash: txHash.CloneBytes(),
		RawTx:  tx.SerializeBytes(),
		Reason: validationErr.Error(),
	}

	value, marshalErr := proto.Marshal(m)
	if marshalErr != nil {
		ctxLogger.Errorf("[publishPolicyRejectedTx] proto marshal error for tx %s: %v", txHash.String(), marshalErr)
		return
	}

	// Non-blocking publish: this runs on the validation hot path, and the
	// policy-rejected cache is strictly best-effort (a drop just falls back to the HTTP
	// fetch path on the consumer side). Blocking here on Kafka back-pressure would stall
	// validateTransaction, so a full producer buffer drops the message instead.
	if !v.policyRejectedTxKafkaProducerClient.TryPublish(&kafka.Message{
		Key:   txHash.CloneBytes(),
		Value: value,
	}) {
		ctxLogger.Debugf("[publishPolicyRejectedTx] dropped tx %s: policy-rejected producer buffer full", txHash.String())
	}
}

// validateInternal performs the core validation logic for a transaction.
// This method contains the detailed step-by-step transaction validation workflow and manages
// the entire lifecycle of a transaction from initial validation through UTXO updates and
// optional block assembly integration. It is the heart of the validation engine and
// implements the full Bitcoin consensus and policy rules.
//
// The validation process follows these key steps:
// 1. Initialize tracing and performance monitoring
// 2. Extend transaction with previous output data for validation
// 3. Validate transaction format, structure, and basic policy rules
// 4. Spend referenced UTXOs, checking for double-spends
// 5. Generate and store transaction metadata
// 6. Validate transaction scripts (signature verification)
// 7. Perform two-phase commit to finalize UTXO state changes
// 8. Optionally send to block assembly for mining consideration
//
// The method includes extensive error handling and rollback capability in case
// any validation step fails, ensuring UTXO database consistency even during partial
// validation failures.
//
// Parameters:
//   - ctx: Context for the validation operation, used for tracing and cancellation
//   - tx: Transaction to validate, must be properly initialized
//   - blockHeight: Current blockchain height to validate against
//   - validationOptions: Options controlling validation behavior and policy enforcement
//
// Returns:
//   - *meta.Data: Transaction metadata if validation succeeds, includes fee calculations
//   - error: Detailed validation error with specific reason if validation fails
//
//gocognit:ignore
func (v *Validator) validateInternal(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (txMetaData *meta.Data, err error) {
	// this caches the tx hash in the object for the duration of all operations. It's immutable, so not a problem
	tx.SetTxHash(tx.TxIDChainHash())
	txID := tx.TxIDChainHash().String()

	ctx, span, deferFn := tracing.Tracer("validator").Start(
		ctx,
		"validateInternal",
		tracing.WithParentStat(v.stats),
		tracing.WithHistogram(prometheusTransactionValidateTotal),
		tracing.WithTag("txid", txID),
	)

	defer func() {
		deferFn(err)
	}()

	if v.settings.Validator.VerboseDebug {
		v.logger.Debugf("[Validator:ValidateInternal] called for %s", txID)

		defer func() {
			v.logger.Debugf("[Validator:ValidateInternal] called for %s DONE", txID)
		}()
	}

	var spentUtxos []*utxo.Spend

	// Get atomic block state to prevent race conditions between height and median time reads
	blockState := v.GetBlockState()

	if blockHeight == 0 {
		blockHeight = blockState.Height + 1
	}

	// Reject coinbase first, matching bitcoin-sv CheckRegularTransaction
	// (src/validation.cpp:601-603) which short-circuits before any contextual
	// (finality / MTP) check.
	if tx.IsCoinbase() {
		err = errors.NewProcessingError("[Validate][%s] coinbase transactions are not supported", txID)
		span.RecordError(err)

		return nil, err
	}

	if validationOptions.OutpointOnlySpend && !validationOptions.SkipScriptValidation {
		err = errors.NewProcessingError("[Validate][%s] OutpointOnlySpend requires SkipScriptValidation", txID)
		span.RecordError(err)

		return nil, err
	}

	// Defence-in-depth: OutpointOnlySpend is only ever legitimate at or below the
	// highest hardcoded checkpoint (the callers gate on this). Reject it above the
	// checkpoint independently of the caller so a buggy or misconfigured caller
	// cannot spend-by-outpoint (hash check off, BIP68 skipped) on a steady-state
	// block. Mirrors the blockvalidation I4 guard; uses the same single-source
	// HighestCheckpointHeight so the bound cannot drift.
	if validationOptions.OutpointOnlySpend && blockHeight > blockchain.HighestCheckpointHeight(v.settings.ChainCfgParams.Checkpoints) {
		err = errors.NewProcessingError("[Validate][%s] OutpointOnlySpend must not be used above the highest checkpoint (height %d)", txID, blockHeight)
		span.RecordError(err)

		return nil, err
	}

	// Fail closed on a store that does not support the fast path: OutpointOnlySpend
	// relies on SkipUTXOHashCheck / SkipExtendedInputs, which such a store ignores —
	// it would then derive the UTXO hash from absent parent data and hard-error on the
	// un-decorated inputs, stalling IBD. Ask the store directly (the capability lives on
	// the store, not a settings scheme guess) so a misconfigured caller cannot reach an
	// unsupported store on the fast path.
	if validationOptions.OutpointOnlySpend && !v.utxoStore.SupportsOutpointOnlySpend() {
		err = errors.NewProcessingError("[Validate][%s] OutpointOnlySpend requires a UTXO store that supports it", txID)
		span.RecordError(err)

		return nil, err
	}

	comparisonTime, skipFinality, finalityErr := selectFinalityComparisonTime(validationOptions, blockHeight, uint32(v.settings.ChainCfgParams.CSVHeight), blockState)
	if finalityErr != nil {
		err = finalityErr
		span.RecordError(err)

		return nil, err
	}

	if !skipFinality {
		// this function should be moved into go-bt
		if err = util.IsTransactionFinal(tx, blockHeight, comparisonTime); err != nil {
			err = errors.NewUtxoNonFinalError("[Validate][%s] transaction is not final", txID, err)
			span.RecordError(err)

			return nil, err
		}
	}

	var utxoHeights []uint32

	// OutpointOnlySpend: skip parent reads entirely. utxoHeights stays nil.
	// Safe because (a) SkipScriptValidation short-circuits BDK before it indexes
	// utxoHeights, and (b) the OutpointOnlySpend guard in validateTransaction's
	// phase-2 explicitly skips BIP68 (the only remaining utxoHeights consumer).
	if !validationOptions.OutpointOnlySpend {
		// check whether the transaction is extended, extend it if not
		// we also get the block heights of the inputs of the transaction since we are doing a DB lookup
		if !tx.IsExtended() {
			// get the block heights of all inputs of the transaction and extend the inputs of not extended transaction.
			// utxoHeights is a slice of block heights for each input
			// txInpoints is a struct containing the parent tx hashes and the vout indexes of each input
			if utxoHeights, err = v.getTransactionInputBlockHeightsAndExtendTx(ctx, tx, txID, validationOptions); err != nil {
				err = errors.NewProcessingError("[Validate][%s] error getting transaction input block heights", txID, err)
				span.RecordError(err)

				return nil, err
			}
		}

		// if the transaction was extended, we still need to get the block heights of the inputs
		// since that processing did not happen before extending the transaction
		// This must be done BEFORE validateTransaction to ensure BIP68 sequence lock validation has the required heights
		if len(utxoHeights) == 0 {
			if utxoHeights, err = v.getTransactionInputBlockHeightsAndExtendTx(ctx, tx, txID, validationOptions); err != nil {
				err = errors.NewProcessingError("[Validate][%s] error getting transaction input block heights", txID, err)
				span.RecordError(err)

				return nil, err
			}
		}
	}

	// Run Teranode-owned checks and BDK transaction validation.
	if err = v.validateTransaction(ctx, tx, blockHeight, utxoHeights, validationOptions); err != nil {
		err = errors.NewProcessingError("[Validate][%s] error validating transaction", txID, err)
		span.RecordError(err)

		return nil, err
	}

	// The post-decision store work must not be cancellable by the caller: the
	// hand-off, its unwind and the two-phase commit all have to run to a decision even
	// if the client has already given up, or a shed leaves the transaction spent,
	// created and Locked with nothing to recover it. context.WithoutCancel is what
	// guarantees that. DecoupleTracingSpan alone does not: its documented fast path
	// for tracing disabled returns the caller's context unchanged, and tracing is
	// disabled by default, so the guarantee was contingent on a config flag.
	// WithoutCancel also keeps context values, which the tracing-enabled path (built
	// on context.Background) drops.
	//
	// Detached does not mean unbounded — but the bounding is deliberately PARTIAL, so
	// read this as a list rather than a guarantee.
	//
	// Bounded, each with its own hard deadline:
	//
	//   - the block-assembly hand-off (see handoffFloor), in both the unary and the
	//     batched block-assembly client;
	//   - the shed unwind (see shedUnwindTimeout);
	//   - the two-phase-commit unlock (see twoPhaseCommitTimeout).
	//
	// NOT bounded here, deliberately, and therefore reliant on the store's own
	// client-level timeouts: the primary SpendAndCreate below, and the store reads and
	// writes on the error and duplicate paths — the GetMeta after ErrTxExists, the
	// conflicting-path CreateInUtxoStore and its GetMeta, MarkConflictingRecursively,
	// and the GetMeta on the not-found path.
	//
	// The exposure is real rather than theoretical. Before this context was detached,
	// process shutdown could eventually interrupt those calls; now nothing outside the
	// store can. Aerospike mitigates with its own TotalTimeout/SocketTimeout, but the
	// SQL/Postgres store applies no per-operation deadline of its own, so a wedged
	// non-Aerospike store can retain an ingest goroutine indefinitely on those seams.
	//
	// SpendAndCreate is not bounded here because it is the acceptance operation, not
	// bookkeeping: a deadline on it changes acceptance semantics under load — a
	// slow-but-healthy store during a burst would start failing transactions — and it
	// would fire mid-operation into the store's own spend-rollback-on-create-failure
	// path. Sizing that deadline needs a load trace, so it is recorded here as a known
	// gap rather than guessed at.
	//
	// The Kafka retry loop below still honours caller cancellation, deliberately and
	// explicitly, through its own select on ctx.
	decoupledCtx, _, deferFn := tracing.DecoupleTracingSpan(context.WithoutCancel(ctx), "validator", "decoupledSpan")
	defer deferFn()

	/*
		Scenario where store is done before adding to assembly:
		Parent -> spent -> tx meta -> stored                                                  -> block assembly
		Child                                 -> spent -> tx meta -> stored -> block assembly

		Scenario where store is done after adding to assembly:
		Parent -> spent -> tx meta -> block assembly -> stored
		Child                                                  -> spent -> tx meta -> stored -> block assembly
	*/

	var (
		tErr       *errors.Error
		utxoMapErr error
	)

	// the option blockAssemblyDisabled is false by default
	blockAssemblyEnabled := !v.settings.BlockAssembly.Disabled
	addToBlockAssembly := blockAssemblyEnabled && validationOptions.AddTXToBlockAssembly

	// spend the tx's inputs and create its outputs + metadata in one store
	// operation; the store reverses the spends if the create fails (except on
	// ErrTxExists, which leaves the spends in place and is handled below)
	if txMetaData, spentUtxos, err = v.spendAndCreateInUtxoStore(decoupledCtx, tx, blockHeight, addToBlockAssembly, validationOptions); err != nil {
		if errors.Is(err, errors.ErrTxExists) {
			// create phase: the tx already exists in the store
			v.logger.Debugf("[Validate][%s] tx already exists in store, not sending to block assembly: %v", txID, err)

			txMetaData = &meta.Data{}
			if err = v.utxoStore.GetMeta(decoupledCtx, tx.TxIDChainHash(), txMetaData); err != nil {
				return nil, errors.NewProcessingError("[Validate][%s] failed to get tx meta data from store", txID, err)
			}

			// The transaction is already durably present; a resubmit is an
			// idempotent success and must not mutate lock state. It does not
			// re-drive the block-assembly handoff.
			//
			// A queue-full shed no longer leaves a record behind (see unwindShed),
			// so reaching here means a genuine duplicate rather than shed residue.
			// A record that IS locked and unmined here belongs to something a
			// resubmit must not touch: an in-flight conflict resolution, which
			// locks honest losing parents without marking them conflicting; or a
			// submission interrupted between the create and the hand-off (shutdown
			// mid-retry, or an unwind that itself failed). Those causes are
			// bit-for-bit indistinguishable in the store, and the unmined reload on
			// the next block-assembly start recovers all of them — so the state is
			// counted and logged rather than answered differently.
			if txMetaData.Locked && len(txMetaData.BlockIDs) == 0 && !txMetaData.Conflicting {
				prometheusValidatorExistingTxLockedUnmined.Inc()

				v.logger.Warnf("[Validate][%s] resubmit of an existing transaction that is locked and unmined; returning success, recovery is the unmined reload (cause is indistinguishable between conflict resolution and an interrupted submission)", txID)
			}

			return txMetaData, nil
		}

		if errors.Is(err, errors.ErrUtxoError) {
			saveAsConflicting := false

			// Collect failed spends and attach a capped aggregate to the
			// returned error. The failure count scales with the tx's input
			// count; an uncapped chain makes every subsequent errors.Is on it
			// walk the full chain (mainnet IBD stall, block 820116).
			failedSpends := make([]error, 0, 8)

			for _, spend := range spentUtxos {
				if spend.Err != nil {
					if validationOptions.CreateConflicting && (errors.Is(spend.Err, errors.ErrSpent) || errors.Is(spend.Err, errors.ErrTxConflicting)) {
						saveAsConflicting = true
					}

					failedSpends = append(failedSpends, spend.Err)
				}
			}

			if len(failedSpends) > 0 {
				if errors.As(err, &tErr) {
					tErr.SetWrappedErr(errors.JoinCapped(maxAggregatedSpendErrs, failedSpends...))
				}
			}

			if saveAsConflicting {
				// On the outpoint-only fast path the tx is deliberately un-decorated, so a full
				// create would call GetFees on zero parent satoshis and error. Thread the same
				// minimal-create option the primary create below uses, so the conflicting
				// fallback (reached during legacy below-checkpoint catchup on a stale/double
				// spend — see handle_block.go PreValidateTransactions, which sets both
				// CreateConflicting and OutpointOnlySpend) does not hard-fail the block.
				var conflictingCreateOpts []utxo.CreateOption
				if validationOptions.OutpointOnlySpend {
					conflictingCreateOpts = append(conflictingCreateOpts, utxo.WithSkipExtendedInputs(true))
				}

				if txMetaData, utxoMapErr = v.CreateInUtxoStore(decoupledCtx, tx, blockHeight, true, false, conflictingCreateOpts...); utxoMapErr != nil {
					if errors.Is(utxoMapErr, errors.ErrTxExists) {
						txMetaData = &meta.Data{}
						if err = v.utxoStore.GetMeta(decoupledCtx, tx.TxIDChainHash(), txMetaData); err != nil {
							err = errors.NewProcessingError("[Validate][%s] CreateInUtxoStore failed - tx exists but unable to get meta data", txID, err)
							span.RecordError(err)

							return nil, err
						}

						// Tx already exists — ensure it and all its spending descendants are marked conflicting.
						// NOTE: cascaded descendants may still be in the subtree processor's in-memory template
						// until the next reset/reload — this path has no subtreeProcessor handle to evict them.
						if !txMetaData.Conflicting {
							if _, _, setErr := utxo.MarkConflictingRecursively(decoupledCtx, v.utxoStore, []chainhash.Hash{*tx.TxIDChainHash()}); setErr != nil {
								err = errors.NewProcessingError("[Validate][%s] failed to mark existing tx as conflicting", txID, setErr)
								span.RecordError(err)

								return nil, err
							}
						}

						err = errors.NewTxConflictingError("[Validate][%s] tx is conflicting (already exists)", txID, err)
						span.RecordError(err)

						return txMetaData, err
					}

					err = errors.NewProcessingError("[Validate][%s] CreateInUtxoStore failed: %v", txID, utxoMapErr)
					span.RecordError(err)

					return txMetaData, err
				}

				// We successfully added the tx to the utxo store as a conflicting tx,
				// so we can return a conflicting error
				err = errors.NewTxConflictingError("[Validate][%s] tx is conflicting", txID, err)
				span.RecordError(err)

				return txMetaData, err
			}
		} else if errors.Is(err, errors.ErrTxNotFound) {
			// PHASE ASSUMPTION: this branch (and the ErrUtxoError branch above) assumes the
			// error originated in the SPEND phase of SpendAndCreate — today no create path
			// emits ErrTxNotFound or ErrUtxoError, and no spend path emits ErrTxExists.
			// An atomic SpendAndCreate implementation (Postgres tx, Aerospike-native) whose
			// create phase can surface ErrTxNotFound (e.g. an internal parent lookup) MUST
			// NOT let it escape untagged, or a failed create gets misread here as a
			// DAH-evicted parent and the tx is wrongly treated as blessed.
			//
			// The parent transaction was not found. This can legitimately happen when the parent has been DAH-evicted
			// long after the child was mined. Only short-circuit if the stored metadata confirms prior full validation:
			//   - tx has been included in at least one block (BlockIDs non-empty), AND
			//   - tx is NOT marked conflicting, AND
			//   - tx is NOT locked
			// Otherwise, surface the original ErrTxNotFound — a "tx exists in store" alone is not proof of validation
			// (a re-org or DAH window could expose a stale or mid-flight record).
			txMetaData = &meta.Data{}
			if metaErr := v.utxoStore.GetMeta(decoupledCtx, tx.TxIDChainHash(), txMetaData); metaErr == nil {
				if len(txMetaData.BlockIDs) > 0 && !txMetaData.Conflicting && !txMetaData.Locked {
					v.logger.Warnf("[Validate][%s] parent tx DAH-evicted, child already mined and not conflicting/locked, assuming blessed (BlockIDs=%v)", txID, txMetaData.BlockIDs)

					return txMetaData, nil
				}
			}
		}

		err = errors.NewProcessingError("[Validate][%s] error spending utxos", txID, err)
		span.RecordError(err)

		return nil, err
	}

	if validationOptions.SkipUtxoCreation {
		// create the tx meta needed for the block assembly
		if validationOptions.OutpointOnlySpend {
			txMetaData, err = util.TxMetaDataFromTxNoFee(tx)
		} else {
			txMetaData, err = util.TxMetaDataFromTx(tx)
		}
		if err != nil {
			return nil, errors.NewProcessingError("[Validate][%s] failed to get tx meta data", txID, err)
		}
	}

	if addToBlockAssembly {
		var txInpoints subtree.TxInpoints

		if txMetaData.TxInpoints.ParentTxHashes != nil {
			txInpoints = txMetaData.TxInpoints
		} else {
			txInpoints, err = subtree.NewTxInpointsFromTx(tx)
			if err != nil {
				return nil, errors.NewProcessingError("[Validate][%s] error getting tx inpoints: %v", txID, err)
			}
		}

		blockAssemblyData := &blockassembly.Data{
			TxIDChainHash: *tx.TxIDChainHash(),
			Fee:           txMetaData.Fee,
			Size:          uint64(tx.Size()), // nolint:gosec
			TxInpoints:    txInpoints,
		}

		// On the Kafka ingest path a queue-full shed is worth waiting out rather
		// than dropping: ErrThresholdExceeded is a queue-depth condition
		// independent of this transaction, and block assembly drains a batch per
		// loop iteration, so a full queue normally clears in milliseconds (a
		// genuinely invalid transaction fails earlier with a different,
		// non-retried error). Retry the handoff in place, then fall through to the
		// txmeta publish and the 2PC unlock.
		//
		// The wait is BOUNDED by BlockAssemblyShedRetryTimeout. Parking here for
		// the whole duration of a stall relocated the growth the block-assembly
		// queue cap removed into the validator's Kafka consumer — the puller does
		// not wait for the per-partition goroutines, so every parked goroutine
		// keeps holding its fetched record batch — and it held this transaction's
		// parent Locked long enough for children on other partitions to exhaust
		// their TX_LOCKED retries and be committed past. Retention is now
		// proportional to a window this node controls, not to how long an operator
		// takes to notice.
		//
		// The budget STARTS HERE, before the first hand-off, not after it: the
		// fetched record batch this goroutine holds is pinned from the first attempt,
		// so a budget measured from after it under-reports retention by a whole
		// hand-off. Subtracting handoffFloor leaves room for one final in-flight send
		// inside the window, which is what keeps the total within
		// BlockAssemblyShedRetryTimeout rather than overshooting it by two
		// unaccounted attempts. A negative result means the configured window cannot
		// accommodate even one attempt, so the deadline is already past and the
		// hand-off makes exactly one bounded try — the condition New warns about at
		// startup.
		//
		// Synchronous callers leave WaitForBlockAssembly false and surface the
		// shed immediately.
		var retryDeadline time.Time
		if validationOptions.WaitForBlockAssembly {
			retryDeadline = time.Now().Add(v.shedRetryTimeout() - v.handoffFloor())
		}

		// send the tx to the block assembler
		err = v.sendToBlockAssembler(decoupledCtx, blockAssemblyData, spentUtxos)

		if err != nil && validationOptions.WaitForBlockAssembly && errors.Is(err, errors.ErrThresholdExceeded) {
			// One timer for the whole retry, reset per iteration. At the 2s/5ms
			// defaults this loop runs a few hundred times per shed, on a path that by
			// definition runs under pressure, and time.After allocates a timer per
			// iteration.
			backoff := time.NewTimer(kafkaShedRetryBackoff)
			defer backoff.Stop()

			for err != nil && errors.Is(err, errors.ErrThresholdExceeded) && time.Now().Before(retryDeadline) {
				select {
				case <-ctx.Done():
					// Deliberate asymmetry: the LOOP honours caller cancellation even
					// though the store operations inside it do not (see the detached
					// context above). This is the only cancellation path on the ingest
					// hand-off, and it must stay.
					//
					// Shutdown: do NOT unwind. The record must survive so the
					// redelivered Kafka message (the offset is left uncommitted by
					// WithLogErrorAndMoveOn's cancelled carve-out) or the unmined
					// reload can recover it.
					err = errors.NewProcessingError("[Validate][%s] context cancelled while waiting for block assembly", txID, ctx.Err())
					span.RecordError(err)

					return nil, err
				case <-backoff.C:
					// Resetting straight after the receive is safe: the channel is
					// drained by it.
					backoff.Reset(kafkaShedRetryBackoff)
				}

				// Re-check after the wait, not only before it. The budget can expire
				// DURING the backoff, and starting a hand-off then would overshoot the
				// advertised bound by the backoff plus a whole handoffFloor — the loop
				// condition alone cannot see that, because it ran before the wait.
				if !time.Now().Before(retryDeadline) {
					break
				}

				err = v.sendToBlockAssembler(decoupledCtx, blockAssemblyData, spentUtxos)
			}
		}

		if err != nil {
			// Preserve a queue-full shed's resource-exhausted classification so a
			// first-submission shed surfaces to a synchronous caller as
			// ResourceExhausted (queue full, retryable) rather than being masked
			// as Internal by a ProcessingError wrapper. Every other send failure
			// keeps its processing wrapping.
			if errors.Is(err, errors.ErrThresholdExceeded) {
				// A shed leaves no trace: the transaction is either fully accepted
				// (spent, created, handed off, unlocked) or not accepted at all. Undo
				// this call's store work so a resubmit is an ordinary first
				// submission rather than an already-exists success for a transaction
				// that is in no subtree and no template, and so descendants get a
				// clean missing-parent answer instead of TX_LOCKED.
				//
				// The outcome is logged and metered inside unwindShed; the error the
				// caller receives stays the shed either way, because an unwind failure
				// degrades to today's behaviour (record left Locked, recovered by the
				// unmined reload) rather than changing the answer.
				_ = v.unwindShed(decoupledCtx, tx, txID, spentUtxos, !validationOptions.SkipUtxoCreation)

				// On the ingest path the submitter was already told the transaction was
				// accepted (propagation returns success before the validator sees it),
				// so this drop is silent from the client's point of view. Count it
				// separately from a synchronous shed, which does surface a retryable
				// status to its caller.
				if validationOptions.WaitForBlockAssembly {
					prometheusValidatorShedDroppedTotal.Inc()
					v.logger.Warnf("[Validate][%s] dropping transaction after the bounded block-assembly handoff retry (%s); the submitter was already told it was accepted", txID, v.shedRetryTimeout())
				}
			} else {
				// A hand-off that failed on OUR OWN deadline is not a shed and must not
				// be unwound: the deadline can fire after block assembly enqueued the
				// transaction, so deleting the record could remove one that is already
				// in a subtree or a template. The transaction is left Locked for the
				// unmined reload, which is the pre-existing behaviour for every non-shed
				// send failure.
				//
				// Detection is exact because decoupledCtx is detached from the caller:
				// nothing outside this function can cancel it, so a DeadlineExceeded
				// here can only be the hand-off deadline. The likeliest cause is a
				// split-deployment settings skew — this process's copy of
				// blockassembly_queueFullWaitTimeout under-reporting what the
				// block-assembly process actually enforces — which makes the deadline
				// fire before the shed arrives, costing the shed classification and the
				// unwind. Counted and named so an operator can act on it rather than
				// seeing an unexplained hand-off failure.
				if errors.Is(err, context.DeadlineExceeded) {
					prometheusValidatorHandoffDeadlineTotal.Inc()
					v.logger.Warnf("[Validate][%s] block-assembly handoff hit its own deadline (%s) so the transaction is left locked for the unmined reload, not unwound; if this recurs, blockassembly_queueFullWaitTimeout as seen by this process may under-report the block-assembly process's value - align it across both settings contexts, or raise validator_handoffRoundTripSlack, which is the term that actually widens this deadline (raising validator_blockAssemblyShedRetryTimeout buys more attempts but each still deadlines at the same floor)", txID, v.handoffFloor())
				}

				err = errors.NewProcessingError("[Validate][%s] error sending tx to block assembler", txID, err)
			}

			span.RecordError(err)

			return nil, err
		}
	}

	// Serialize and enqueue txmeta for the subtree validation kafka topic.
	// If this fails (e.g. serialization error), log but continue to the two-phase commit
	// so the tx doesn't remain locked. A missing txmeta message is recoverable; a stuck
	// lock is not. We intentionally do NOT return this error to the caller: the tx has
	// been validated, spent, and created in the UTXO store — returning an error would
	// cause callers to treat an accepted tx as failed and trigger duplicate retries.
	if v.txmetaKafkaProducerClient != nil && !validationOptions.SkipTxMetaPublishing {
		// InBlock is set explicitly by block-context callers (block validation,
		// subtree validation, legacy sync) whose transactions arrived as part
		// of a block or announced subtree rather than via mempool submission.
		// Mark the published txmeta so relay consumers (legacy netsync) don't
		// announce it as a fresh mempool tx.
		txMetaData.InBlock = validationOptions.InBlock

		if txMetaErr := v.sendTxMetaToKafka(txMetaData, tx.TxIDChainHash()); txMetaErr != nil {
			v.logger.Errorf("[Validate][%s] failed to serialize/enqueue txmeta for kafka, continuing to 2PC: %v", txID, txMetaErr)
		}
	}

	if txMetaData.Locked {
		if err = v.twoPhaseCommitTransaction(decoupledCtx, tx, txID); err != nil {
			v.logger.Warnf("[Validate][%s] error during two phase commit, transaction will be marked as spendable on next block: %v", txID, err)

			return txMetaData, err
		}

		txMetaData.Locked = false
	}

	return txMetaData, nil
}

// getTransactionInputBlockHeights returns the block heights for each input of the transaction
func (v *Validator) getTransactionInputBlockHeightsAndExtendTx(ctx context.Context, tx *bt.Tx, txID string, validationOptions *Options) ([]uint32, error) {
	ctx, span, endSpan := tracing.Tracer("validator").Start(ctx, "getTransactionInputBlockHeightsAndExtendTx",
		tracing.WithHistogram(getTransactionInputBlockHeights),
	)
	defer endSpan()

	// get the utxo heights for each input
	utxoHeights, err := v.getUtxoBlockHeightsAndExtendTx(ctx, tx, txID, validationOptions.PrefetchedParents)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	return utxoHeights, nil
}

// twoPhaseCommitTransaction marks the transaction as spendable.
//
// Bounded by twoPhaseCommitTimeout: the context arriving here is detached from the
// caller (see Validate), so without a deadline of its own a wedged store would park
// this goroutine on what is post-acceptance bookkeeping. A timeout costs nothing the
// code does not already handle — the failure arm below is explicit that the unlock is
// recovered by the next block the transaction is mined into.
func (v *Validator) twoPhaseCommitTransaction(ctx context.Context, tx *bt.Tx, txID string) error {
	ctx, span, endSpan := tracing.Tracer("validator").Start(ctx, "twoPhaseCommitTransaction",
		tracing.WithHistogram(prometheusTransaction2PhaseCommit),
	)
	defer endSpan()

	ctx, cancel := context.WithTimeout(ctx, twoPhaseCommitTimeout)
	defer cancel()

	// the tx was marked as locked on creation, we have added it successfully to block assembly
	// so we can now mark it as spendable again
	if err := v.utxoStore.SetLocked(ctx, []chainhash.Hash{*tx.TxIDChainHash()}, false); err != nil {
		// this is not a fatal error, since the transaction will we marked as spendable on the next block it's mined into
		err = errors.NewProcessingError("[Validate][%s] error marking tx as spendable", txID, err)
		span.RecordError(err)

		return err
	}

	return nil
}

// getUtxoBlockHeightsAndExtendTx returns the block heights for each input of the transaction.
// prefetched, when non-nil, supplies parent metadata already read in bulk so per-parent
// store Gets can be skipped (see Options.PrefetchedParents).
func (v *Validator) getUtxoBlockHeightsAndExtendTx(ctx context.Context, tx *bt.Tx, txID string, prefetched map[chainhash.Hash]*meta.Data) ([]uint32, error) {
	// get the block heights of the input transactions of the transaction
	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(v.logger, g, v.settings.UtxoStore.GetBatcherSize)

	parentTxHashes := make(map[chainhash.Hash][]int)
	utxoHeights := make([]uint32, len(tx.Inputs))

	for inputIdx, input := range tx.Inputs {
		parentTxHash := input.PreviousTxIDChainHash()

		if _, ok := parentTxHashes[*parentTxHash]; !ok {
			parentTxHashes[*parentTxHash] = make([]int, 0)
		}

		parentTxHashes[*parentTxHash] = append(parentTxHashes[*parentTxHash], inputIdx)
	}

	extend := !tx.IsExtended() // if the tx is not extended, we need to extend it with the parent tx hashes

	for parentTxHash, idxs := range parentTxHashes {
		parentTxHash := parentTxHash
		inputIdxs := idxs

		g.Go(func() error {
			if err := v.getUtxoBlockHeightAndExtendForParentTx(gCtx, parentTxHash, inputIdxs, utxoHeights, tx, extend, prefetched); err != nil {
				if errors.Is(err, errors.ErrTxNotFound) {
					return errors.NewTxMissingParentError("[Validate][%s] error getting parent transaction %s", txID, parentTxHash, err)
				}

				return errors.NewProcessingError("[Validate][%s] error getting parent transaction %s", txID, parentTxHash, err)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return utxoHeights, nil
}

// getUtxoBlockHeightAndExtendForParentTx retrieves the block height for a parent transaction
// and extends the inputs of the transaction if it is not already extended.
//
// Two height-population branches exist; exactly one writes utxoHeights[idx]
// for any given parent:
//
//  1. UTXO-store hit with non-empty BlockHeights (confirmed prior-block parent)
//     — writes the real stored block height.
//  2. UTXO-store fallback with empty BlockHeights (parent in the store but not
//     yet mined into a block, e.g. an in-block parent of the candidate block)
//     — writes the unconfirmedParentHeight sentinel. The sentinel is later
//     resolved by Options.UnconfirmedParentsAtCandidateHeight (block-validation
//     paths substitute the candidate height before BDK/BIP68 consume the
//     heights) or translated at the BDK boundary: MEMPOOL_HEIGHT in consensus
//     (BDK rejects with bad-txns-unconfirmed-input-in-block) or the candidate
//     height in policy mode.
func (v *Validator) getUtxoBlockHeightAndExtendForParentTx(gCtx context.Context, parentTxHash chainhash.Hash, idxs []int,
	utxoHeights []uint32, tx *bt.Tx, extend bool, prefetched map[chainhash.Hash]*meta.Data) error {
	// Validate every target index up front, before any utxoHeights[idx] (the
	// height loops below) or tx.Inputs[idx] (the extend loop) dereference. idxs
	// are positions in tx.Inputs, and the caller sizes utxoHeights to
	// len(tx.Inputs), so an out-of-range index would otherwise panic in the
	// height loop before reaching the extend path. Unreachable today (idxs
	// derive from range tx.Inputs), so this is purely defensive hardening.
	for _, idx := range idxs {
		if idx < 0 || idx >= len(tx.Inputs) || idx >= len(utxoHeights) {
			return errors.NewProcessingError("[Validate][%s] input index %d out of bounds (%d inputs, %d height slots)",
				tx.TxIDChainHash().String(), idx, len(tx.Inputs), len(utxoHeights))
		}
	}

	f := []fields.FieldName{fields.BlockIDs, fields.BlockHeights}

	if extend {
		// add the parent tx outputs to the fields, to be able to extend the transaction
		f = append(f, fields.Tx)
	}

	// Use a bulk-prefetched parent if the caller supplied one that carries
	// everything we need (the parent tx outputs too, when extending). This is a
	// read-source swap only: the height/sentinel logic below is unchanged, and
	// any parent not prefetched — or prefetched without the Tx needed for
	// extension — falls back to a store Get, so correctness is never reduced.
	var txMeta *meta.Data
	if pf, ok := prefetched[parentTxHash]; ok && pf != nil && (!extend || pf.Tx != nil) {
		txMeta = pf
	} else {
		var err error
		if txMeta, err = v.utxoStore.Get(gCtx, &parentTxHash, f...); err != nil {
			return err
		}
	}

	if len(txMeta.BlockHeights) == 0 {
		// Parent is in the UTXO store but has no block heights recorded — i.e.
		// the parent UTXO is not yet confirmed. Mark each slot with the
		// teranode-internal sentinel so the BDK adapter can translate it at
		// the boundary: MEMPOOL_HEIGHT in consensus (BDK rejects with
		// bad-txns-unconfirmed-input-in-block) or the candidate height in
		// policy mode (matching svnode's GetInputScriptBlockHeight). See
		// ScriptVerifierGoBDK.ValidateTransaction for the translation.
		for _, idx := range idxs {
			utxoHeights[idx] = unconfirmedParentHeight
		}
	} else {
		for _, idx := range idxs {
			utxoHeights[idx] = txMeta.BlockHeights[0]
		}
	}

	if extend {
		// extend the transaction inputs with the parent tx outputs (idx bounds
		// already validated at the top of the function)
		for _, idx := range idxs {
			// PreviousTxOutIndex comes from the (untrusted) child transaction, so
			// bound it against the parent's output count before indexing.
			// Otherwise a tx referencing a real parent but a non-existent vout
			// (e.g. vout 99 on a 2-output parent) panics here with index out of
			// range and crashes the validator. Mirrors the guard in
			// stores/utxo/aerospike/get.go.
			vout := tx.Inputs[idx].PreviousTxOutIndex
			if txMeta.Tx == nil || txMeta.Tx.Outputs == nil ||
				int(vout) >= len(txMeta.Tx.Outputs) || txMeta.Tx.Outputs[vout] == nil {
				return errors.NewProcessingError("[Validate][%s] parent transaction %s has no output for index %d",
					tx.TxIDChainHash().String(), parentTxHash.String(), vout)
			}

			// extend the input with the parent tx outputs
			tx.Inputs[idx].PreviousTxSatoshis = txMeta.Tx.Outputs[vout].Satoshis
			tx.Inputs[idx].PreviousTxScript = txMeta.Tx.Outputs[vout].LockingScript
		}
	}

	return nil
}

func (v *Validator) TriggerBatcher() {
	// Noop
}

// CreateInUtxoStore stores transaction metadata in the UTXO store without
// spending any inputs (SpendAndCreate with WithCreateOnly).
// Returns transaction metadata and error if storage fails.
// Extra create options (e.g. utxo.WithSkipExtendedInputs) may be passed via extraOpts
// for specialised call sites; the zero-arg call is byte-identical to the original.
func (v *Validator) CreateInUtxoStore(ctx context.Context, tx *bt.Tx, blockHeight uint32, markAsConflicting bool,
	markAsLocked bool, extraOpts ...utxo.CreateOption) (*meta.Data, error) {
	ctx, _, deferFn := tracing.Tracer("validator").Start(ctx, "storeTxInUtxoMap",
		tracing.WithHistogram(prometheusValidatorSetTxMeta),
	)
	defer deferFn()

	createOptions := []utxo.CreateOption{
		utxo.WithCreateOnly(),
		utxo.WithConflicting(markAsConflicting),
	}

	if markAsLocked {
		createOptions = append(createOptions, utxo.WithLocked(true))
	}

	createOptions = append(createOptions, extraOpts...)

	txMetaData, _, err := v.utxoStore.SpendAndCreate(ctx, tx, blockHeight, createOptions...)
	if err != nil {
		return nil, err
	}

	return txMetaData, nil
}

func (v *Validator) sendTxMetaToKafka(data *meta.Data, txHash *chainhash.Hash) error {
	startKafka := time.Now()

	metaBytes, err := data.MetaBytes()
	if err != nil {
		return errors.NewProcessingError("error serializing tx meta data for tx %s", txHash.String(), err)
	}

	if len(metaBytes) > 2048 {
		v.logger.Warnf("stored tx meta maybe too big for txmeta cache, size: %d, parent hash count: %d", len(metaBytes), len(data.TxInpoints.ParentTxHashes))
	}

	// Use batcher if available, otherwise send directly
	if v.txmetaKafkaBatcher != nil {
		v.txmetaKafkaBatcher.Put(&txmetaBatchItem{
			hash:      txHash,
			metaBytes: metaBytes,
			isDelete:  false,
		})
	} else {
		// Fallback: send single item as batch format for consistency.
		item := &txmetaBatchItem{
			hash:      txHash,
			metaBytes: metaBytes,
			isDelete:  false,
		}
		if v.settings.Validator.TxMetaWireFormat == "v2" {
			v.sendTxMetaBatchV2([]*txmetaBatchItem{item})
		} else {
			value := serializeTxMetaBatch([]*txmetaBatchItem{item})
			// Hash key spreads single-item fallback messages evenly across partitions
			// instead of bunching on franz-go's StickyKeyPartitioner default for nil keys.
			v.txmetaKafkaProducerClient.Publish(&kafka.Message{
				Key:   txHash[:],
				Value: value,
			})
		}
	}

	prometheusValidatorSendToBlockValidationKafka.Observe(float64(time.Since(startKafka).Microseconds()) / 1_000_000)

	return nil
}

// sendTxMetaBatch serializes and publishes a batch of TxMeta items to Kafka.
//
// The Kafka message key is set to the first item's tx hash. With franz-go's default
// StickyKeyPartitioner this hashes onto a single partition deterministically, which:
//  1. Distributes traffic evenly across the topic's partitions (tx hashes are uniform).
//  2. Keeps every record from one batch on the same partition (preserves any
//     intra-batch ordering the consumer might rely on).
//
// Previously Key was nil, which makes StickyKeyPartitioner equivalent to a
// StickyPartitioner — bunching consecutive batches onto the same partition until
// linger expires. That created bursty partition usage and the observed Kafka-read
// throughput oscillation on the consumer side.
func (v *Validator) sendTxMetaBatch(batch []*txmetaBatchItem) {
	if len(batch) == 0 {
		return
	}

	if v.settings.Validator.TxMetaWireFormat == "v2" {
		v.sendTxMetaBatchV2(batch)
		return
	}

	value := serializeTxMetaBatch(batch)

	v.txmetaKafkaProducerClient.Publish(&kafka.Message{
		Key:   batch[0].hash[:],
		Value: value,
	})
}

// serializeTxMetaBatch serializes a batch of TxMeta items to raw bytes.
// Format:
// [4 bytes]  - entry count (uint32, little-endian)
// For each entry:
//
//	[32 bytes] - tx hash (raw bytes)
//	[1 byte]   - action (0=ADD, 1=DELETE)
//	[4 bytes]  - content length (uint32, little-endian) - 0 for DELETE
//	[N bytes]  - content (metaBytes) - only for ADD
func serializeTxMetaBatch(batch []*txmetaBatchItem) []byte {
	// Calculate total size
	size := 4 // entry count
	for _, item := range batch {
		size += 32 + 1 + 4 // hash + action + length
		if !item.isDelete {
			size += len(item.metaBytes)
		}
	}

	buf := make([]byte, size)
	offset := 0

	// Write entry count
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(batch)))
	offset += 4

	// Write each entry
	for _, item := range batch {
		// Write hash (32 bytes)
		copy(buf[offset:], item.hash[:])
		offset += 32

		// Write action (1 byte)
		if item.isDelete {
			buf[offset] = txmetacache.WireActionDELETE
		} else {
			buf[offset] = txmetacache.WireActionADD
		}
		offset++

		// Write content length (4 bytes)
		if item.isDelete {
			binary.LittleEndian.PutUint32(buf[offset:], 0)
			offset += 4
		} else {
			binary.LittleEndian.PutUint32(buf[offset:], uint32(len(item.metaBytes)))
			offset += 4
			// Write content
			copy(buf[offset:], item.metaBytes)
			offset += len(item.metaBytes)
		}
	}

	return buf
}

// txmetaItemWithHash bundles a batch item with its pre-computed xxhash so
// per-partition grouping and serialization don't re-hash.
type txmetaItemWithHash struct {
	item *txmetaBatchItem
	h    uint64
}

// serializeTxMetaBatchV2 writes a v2-format txmeta Kafka payload for a set of
// items that have already been grouped into a single Kafka partition.
//
// Layout (see services/subtreevalidation/txmetaHandler.go for the symmetric
// parser):
//
//	[1 byte]    magic = 0xFF
//	[1 byte]    version = 0x02
//	[2 bytes]   reserved (zero)
//	[4 bytes]   entry count (uint32 LE)
//	per entry:
//	  [8 bytes]  xxhash(tx hash) (uint64 LE)
//	  [32 bytes] tx hash
//	  [1 byte]   action (0=ADD, 1=DELETE)
//	  [4 bytes]  content length (uint32 LE)
//	  [N bytes]  content (only for ADD)
//
// Putting the pre-computed xxhash on the wire lets the receiver skip its own
// xxhash on every entry — a small per-entry saving that compounds at the
// production rates this is designed for.
func serializeTxMetaBatchV2(items []txmetaItemWithHash) []byte {
	size := 8 // header: magic + version + 2 reserved + count
	for _, it := range items {
		size += 8 + 32 + 1 + 4
		if !it.item.isDelete {
			size += len(it.item.metaBytes)
		}
	}

	buf := make([]byte, size)
	buf[0] = txmetacache.WireV2Magic
	buf[1] = txmetacache.WireV2Version
	binary.LittleEndian.PutUint32(buf[4:], uint32(len(items)))
	off := 8

	for _, it := range items {
		binary.LittleEndian.PutUint64(buf[off:], it.h)
		off += 8
		copy(buf[off:], it.item.hash[:])
		off += 32
		if it.item.isDelete {
			buf[off] = txmetacache.WireActionDELETE
			off++
			binary.LittleEndian.PutUint32(buf[off:], 0)
			off += 4
		} else {
			buf[off] = txmetacache.WireActionADD
			off++
			binary.LittleEndian.PutUint32(buf[off:], uint32(len(it.item.metaBytes)))
			off += 4
			copy(buf[off:], it.item.metaBytes)
			off += len(it.item.metaBytes)
		}
	}

	return buf
}

// sendTxMetaBatchV2 splits the batch into per-partition sub-batches keyed by
// xxhash(tx hash) and emits one Kafka record per non-empty partition with the
// partition number set explicitly on the record (requires the txmeta producer
// to have been built with kafka.KafkaProducerConfig.ManualPartitioning=true).
//
// Routing rule:
//
//	bucketIdx           = xxhash(hash) % BucketsCount
//	bucketsPerPartition = BucketsCount / NumPartitions
//	partition           = bucketIdx / bucketsPerPartition
//
// Each partition therefore owns a contiguous, disjoint range of receiver
// cache buckets. The subtreevalidation handler can write its partition's
// records to the cache without taking locks contended by any other
// partition's records (modulo the cache's own bucket-lock granularity).
// txmetaPartitionsScratch is the per-call scratch held in
// txmetaPartitionsScratchPool. partitions[p] is the per-partition group of
// items being assembled before serialization. The outer slice header and the
// per-partition inner slices' backing arrays are both reused across calls;
// only newly-required capacity (e.g. when a hot partition gets a bigger
// group than any prior call) triggers a fresh allocation. The byte buffer
// produced by serializeTxMetaBatchV2 is NOT pooled — it is handed to
// franz-go via Publish and we have no callback hook for safe return.
type txmetaPartitionsScratch struct {
	partitions [][]txmetaItemWithHash
}

var txmetaPartitionsScratchPool = sync.Pool{
	New: func() any { return &txmetaPartitionsScratch{} },
}

func (v *Validator) sendTxMetaBatchV2(batch []*txmetaBatchItem) {
	if len(batch) == 0 {
		return
	}

	numPartitions := v.settings.Validator.TxMetaNumPartitions
	if numPartitions <= 0 {
		numPartitions = 1
	}
	bucketsPerPartition := txmetacache.BucketsCount / numPartitions
	if bucketsPerPartition < 1 {
		bucketsPerPartition = 1
	}

	scratch := txmetaPartitionsScratchPool.Get().(*txmetaPartitionsScratch)
	// Ensure outer slice has the right shape, growing only if needed.
	if cap(scratch.partitions) < numPartitions {
		scratch.partitions = make([][]txmetaItemWithHash, numPartitions)
	} else {
		scratch.partitions = scratch.partitions[:numPartitions]
	}
	// Reset every per-partition slice's length to 0 but retain capacity for
	// reuse on the next pool hit. We do NOT nil the elements: they're past
	// len, GC can still collect the txmetaBatchItem pointers when no live
	// slice header references them, and they get overwritten the next time
	// this partition is hit.
	for i := range scratch.partitions {
		scratch.partitions[i] = scratch.partitions[i][:0]
	}
	defer txmetaPartitionsScratchPool.Put(scratch)

	for _, item := range batch {
		h := xxhash.Sum64(item.hash[:])
		bucket := int(h % uint64(txmetacache.BucketsCount))
		p := bucket / bucketsPerPartition
		if p >= numPartitions {
			// Defensive cap; only fires if BucketsCount is not an exact
			// multiple of NumPartitions, which is documented as a constraint.
			p = numPartitions - 1
		}
		scratch.partitions[p] = append(scratch.partitions[p], txmetaItemWithHash{item: item, h: h})
	}

	for p, items := range scratch.partitions {
		if len(items) == 0 {
			continue
		}
		v.txmetaKafkaProducerClient.Publish(&kafka.Message{
			Partition: int32(p), //nolint:gosec // p < numPartitions, bounded by setting
			Value:     serializeTxMetaBatchV2(items),
		})
	}
}

// spendAndCreateInUtxoStore spends the UTXOs referenced by the transaction's
// inputs and stores the transaction's outputs + metadata in one store operation.
// The store rolls the spends back when the create phase fails with anything
// other than ErrTxExists. With Options.SkipUtxoCreation only the spend phase
// runs and the returned metadata is nil.
//
// CONTRACT (below-checkpoint outpoint-only fast path): when OutpointOnlySpend is
// set the tx is deliberately un-decorated (no parent satoshis/scripts), so EVERY
// UTXO-store create reachable on this path MUST thread WithSkipExtendedInputs(true)
// — otherwise the store runs GetFees over zero parent satoshis and hard-fails the
// block. This coupling is NOT enforced by a runtime guard (unlike the
// OutpointOnlySpend => SkipScriptValidation precondition checked earlier), so any
// new create seam added on this path must repeat it. The conflicting-fallback
// create in validateInternal threads the same option for this reason.
// SkipUTXOHashCheck must likewise be set on that path so the store resolves
// spends via outpoint lookup without a UTXO-hash comparison.
func (v *Validator) spendAndCreateInUtxoStore(ctx context.Context, tx *bt.Tx, blockHeight uint32,
	addToBlockAssembly bool, validationOptions *Options) (*meta.Data, []*utxo.Spend, error) {
	// NOTE: prometheusTransactionSpendUtxos keeps its name for dashboard
	// continuity but now measures the combined spend+create (and any rollback),
	// not just the spend. The primary path no longer records
	// prometheusValidatorSetTxMeta (storeTxInUtxoMap) — that histogram only sees
	// the conflicting-fallback create via CreateInUtxoStore.
	ctx, span, deferFn := tracing.Tracer("validator").Start(ctx, "spendAndCreateUtxos",
		tracing.WithHistogram(prometheusTransactionSpendUtxos),
	)
	defer deferFn()

	opts := []utxo.CreateOption{
		utxo.WithIgnoreLocked(validationOptions.IgnoreLocked),
	}

	if validationOptions.OutpointOnlySpend {
		opts = append(opts, utxo.WithSkipUTXOHashCheck(true), utxo.WithSkipExtendedInputs(true))
	}

	if validationOptions.SkipUtxoCreation {
		opts = append(opts, utxo.WithSpendOnly())
	} else if addToBlockAssembly {
		// mark the tx as locked, since we are going to add it to the block assembly
		opts = append(opts, utxo.WithLocked(true))
	}

	txMetaData, spends, err := v.utxoStore.SpendAndCreate(ctx, tx, blockHeight, opts...)
	if err != nil {
		span.RecordError(err)

		return txMetaData, spends, errors.NewProcessingError("validator: UTXO Store spend and create failed for %s", tx.TxIDChainHash().String(), err)
	}

	return txMetaData, spends, nil
}

// sendToBlockAssembler sends validated transaction data to the block assembler.
// Returns error if block assembly integration fails.
//
// The call is bounded by handoffFloor, in BOTH the unary and the batched mode of the
// block-assembly client. The context arriving here is deliberately detached from the
// caller (see Validate), so without a deadline of its own nothing would stop a wedged
// block-assembly server from parking this goroutine — and, on the Kafka ingest path,
// the record batch it holds — indefinitely. Applying it here rather than at each call
// site covers the first hand-off and every retry from one place.
//
// The two modes are bounded, but they are not semantically identical:
//
//   - Unary mode: the deadline cancels the in-flight gRPC call, so the transaction was
//     not accepted by block assembly.
//   - Batch mode (blockassembly_sendBatchSize > 0, the shipped default): the deadline
//     abandons the WAIT but not the item. The batcher may still dispatch it, so the
//     transaction MAY STILL REACH block assembly.
//
// That difference is why a deadline failure is deliberately never treated as a shed
// and never unwound — deleting the record of a transaction that is still in flight
// could remove one already in a subtree or a template. See the context.DeadlineExceeded
// arm in Validate, which leaves it Locked for the unmined reload and counts it.
func (v *Validator) sendToBlockAssembler(ctx context.Context, bData *blockassembly.Data, reservedUtxos []*utxo.Spend) error {
	ctx, span, deferFn := tracing.Tracer("validator").Start(ctx, "sendToBlockAssembler",
		tracing.WithHistogram(prometheusValidatorSendToBlockAssembly),
	)
	defer deferFn()

	ctx, cancel := context.WithTimeout(ctx, v.handoffFloor())
	defer cancel()

	_ = reservedUtxos

	// if v.settings.Validator.VerboseDebug {
	v.logger.Debugf("[Validator] sending tx %s to block assembler", bData.TxIDChainHash.String())
	// }

	if _, err := v.blockAssembler.Store(ctx, &bData.TxIDChainHash, bData.Fee, bData.Size, bData.TxInpoints); err != nil {
		// A queue-full shed must keep its resource-exhausted classification so it
		// surfaces as ResourceExhausted (retry gate skips it) rather than being
		// masked as a generic service fault (Internal). Every other failure is a
		// genuine service fault.
		if errors.Is(err, errors.ErrThresholdExceeded) {
			span.RecordError(err)

			return err
		}

		e := errors.NewServiceError("error calling blockAssembler Store()", err)
		span.RecordError(e)

		return e
	}

	return nil
}

// shedRetryTimeout returns the bound on the in-place block-assembly handoff
// retry, falling back to the documented default when the setting is unset or
// nonsensical (tests build Settings structs directly, and a zero value would turn
// the bounded retry into no retry at all).
func (v *Validator) shedRetryTimeout() time.Duration {
	if t := v.settings.Validator.BlockAssemblyShedRetryTimeout; t > 0 {
		return t
	}

	return defaultBlockAssemblyShedRetryTimeout
}

// shedUnwindTimeout returns the bound on the whole shed unwind, falling back to the
// documented default when the setting is unset or nonsensical. Same reason as
// shedRetryTimeout: tests build Settings structs directly, and a zero value here
// would make the unwind's context already expired rather than unbounded.
func (v *Validator) shedUnwindTimeout() time.Duration {
	if t := v.settings.Validator.ShedUnwindTimeout; t > 0 {
		return t
	}

	return defaultShedUnwindTimeout
}

// handoffRoundTripSlack returns the allowance added to block assembly's own queue
// wait when sizing a single hand-off attempt, falling back to the documented default
// when the setting is unset or nonsensical. A zero value would drop the margin the
// shed classification depends on (see handoffFloor), so the fallback is not optional.
func (v *Validator) handoffRoundTripSlack() time.Duration {
	if t := v.settings.Validator.HandoffRoundTripSlack; t > 0 {
		return t
	}

	return defaultHandoffRoundTripSlack
}

// handoffFloor is the ceiling on a SINGLE block-assembly hand-off attempt.
//
// Two constraints, in priority order:
//
//  1. It must never fire before block assembly's own bounded queue wait can answer.
//     That handler waits up to blockassembly_queueFullWaitTimeout and only then sheds,
//     and it classifies caller cancellation as explicitly NOT a shed. So a deadline
//     shorter than that wait turns every queue-full shed into a context error: the
//     resource-exhausted classification is lost, the shed unwind never runs, and the
//     transaction is stranded locked. Hence the floor is the remote's wait plus
//     handoffRoundTripSlack (validator_handoffRoundTripSlack).
//  2. Subject to (1), the ingest hand-off should not outlast the retry window that
//     BlockAssemblyShedRetryTimeout advertises. Validate subtracts this floor from
//     that window, so the window and one final in-flight attempt together stay within
//     it.
//
// (1) wins when they conflict, because losing the shed classification is a correctness
// failure while overshooting the window is a bounded resource overshoot on a path
// already documented as a time bound rather than a memory bound. New logs a warning
// when the configured window cannot accommodate one attempt, so the effective bound is
// never a surprise.
//
// Cross-process caveat: BlockAssembly.QueueFullWaitTimeout read here is THIS process's
// copy of a block-assembly setting, and the two settings contexts are independent. It
// sizes a safety margin only and never feeds a control decision, which is why the
// queue-stats RPC's self-reporting treatment (used for DoubleSpendWindow, where a
// wrong value could invert the decision) is not replicated for it. If the local copy
// under-reports, the margin is short and a late shed degrades to an ambiguous hand-off
// failure — stranded locked, recovered by the unmined reload, counted by
// prometheusValidatorHandoffDeadlineTotal — never to a wrong unwind.
func (v *Validator) handoffFloor() time.Duration {
	// Clamped defensively: the settings loader already rejects negatives, but tests
	// build Settings structs directly and bypass it.
	wait := v.settings.BlockAssembly.QueueFullWaitTimeout
	if wait < 0 {
		wait = 0
	}

	return wait + v.handoffRoundTripSlack()
}

// unwindShed reverses this call's UTXO-store work after a queue-full shed, so a
// shed leaves no trace: the transaction is either fully accepted (spent, created,
// handed off, unlocked) or not accepted at all, and a resubmit is then an
// ordinary first submission rather than an already-exists success for a
// transaction that reached no subtree and no template.
//
// # Ordering is load-bearing: Delete the record FIRST, then Unspend the inputs
//
//	Unspend -> Delete   inputs are free while the record T still exists and is
//	                    Locked. If the Delete then fails, a competing double-spend
//	                    T' can take those inputs while T survives as a record the
//	                    unmined reload can still lift into a mining template. That
//	                    is an invalid template: consensus-visible.
//	Delete -> Unspend   record T is gone while its inputs still read spent-by-T.
//	                    If the Unspend then fails those UTXOs are temporarily
//	                    unspendable, but no record can re-enter a template. An
//	                    operator can recover from the logged outpoints. Never
//	                    consensus-visible.
//
// # Verify-after-delete
//
// The ordering argument only holds if Delete actually deleted. This method holds
// a utxo.Store INTERFACE, and utxo.Store.Delete is not a trustworthy contract:
// TxMetaCache advertises the full interface but implements Delete as a cache
// eviction that never reaches the underlying record, while its spend and unspend
// DO delegate. Trusting a nil return there would convert the safe intermediate
// state into the dangerous one, and believe it succeeded. So the record is read
// back, and anything other than "genuinely gone" aborts before unspending.
//
// # Why an inconclusive read fails CLOSED
//
// A read that errors does not establish that the record survived — but it equally
// does not establish that it is gone, and the two candidate outcomes are not
// symmetric:
//
//	fail open (unspend anyway)   worst case: the record survived AND its inputs are
//	                             free, so a competing double-spend takes them while
//	                             the survivor can still be lifted into a template.
//	                             Consensus-visible, network-visible.
//	fail closed (abort)          worst case: the record is gone and its inputs read
//	                             spent by a transaction that no longer exists. Those
//	                             outpoints are unspendable until an operator acts on
//	                             the log line below, which names every one of them.
//	                             Node-local, never consensus-visible.
//
// Under uncertainty the branch whose worst case is node-local and recoverable wins,
// and that does not change because the probability shifts. What does change is the
// size of the window: the read is retried (shedUnwindVerifyAttempts) before being
// treated as inconclusive, which converts most transient store errors into a
// definitive answer.
//
// Aborting leaves the transaction exactly as the shed found it (present, Locked,
// inputs spent), which is the pre-change behaviour with the unmined reload as its
// backstop. That makes the unwind correct through every decorator in the stack,
// including ones not yet written, at the cost of one GetMeta on a path that only
// runs when the node is already shedding.
//
// # Preconditions (all hold by construction on this path)
//
//   - Only this call's own work is ever undone: the already-exists branch returns
//     before the hand-off, so an existing record from another submitter is
//     unreachable from here.
//   - No descendant can have spent T's outputs: T is Locked for its whole
//     lifetime up to this point and a locked transaction cannot be spent.
//   - Only the addToBlockAssembly branch reaches this, which is the only path
//     that can produce a shed. Block-context callers (block validation, subtree
//     validation, legacy sync) set AddTXToBlockAssembly=false and can never
//     unwind a transaction that arrived as part of a block.
//   - createdRecord is false on the spend-only shape (SkipUtxoCreation), where
//     there is no record to delete; Unspend of an empty spend set is a no-op.
//
// Failure is best-effort and never fatal: every arm logs the txid AND the outpoints,
// meters, and falls back to leaving the transaction Locked. The error returned to the
// validation caller stays the shed in every case, so the 503 mapping is unchanged. The
// returned error is for callers that want to observe the unwind outcome; the shed path
// deliberately ignores it.
//
// The context is bounded by shedUnwindTimeout (validator_shedUnwindTimeout), because
// the one it inherits is detached from the caller and a wedged store would otherwise
// park an ingest goroutine on a best-effort cleanup path. That bound is ONE budget for
// the delete, the verify reads and the unspend together, so a slow store spends it on
// the earlier phases and the verify read can get fewer attempts than
// shedUnwindVerifyAttempts before failing closed.
func (v *Validator) unwindShed(ctx context.Context, tx *bt.Tx, txID string, spentUtxos []*utxo.Spend, createdRecord bool) error {
	prometheusValidatorShedUnwindTotal.Inc()

	ctx, cancel := context.WithTimeout(ctx, v.shedUnwindTimeout())
	defer cancel()

	txHash := tx.TxIDChainHash()

	if createdRecord {
		if err := v.utxoStore.Delete(ctx, txHash); err != nil {
			prometheusValidatorShedUnwindFailures.Inc()
			v.logger.Errorf("[unwindShed][%s] failed to delete the shed transaction record; leaving it locked for the unmined reload, outpoints %s deliberately stay spent so no competing spend can take the inputs of a surviving record: %v", txID, unwindOutpoints(spentUtxos), err)

			return err
		}

		// Verify-after-delete: only unspend once the record is provably gone.
		gone, verifyErr := v.verifyRecordDeleted(ctx, txHash)

		switch {
		case gone:
			// Provably deleted; fall through to the unspend.
		case verifyErr == nil:
			prometheusValidatorShedUnwindAborted.Inc()
			v.logger.Errorf("[unwindShed][%s] store reported a successful delete but the record is still readable; aborting before unspend, outpoints %s deliberately stay spent to avoid freeing the inputs of a surviving record", txID, unwindOutpoints(spentUtxos))

			return errors.NewProcessingError("[unwindShed][%s] record survived a successful delete", txID)
		default:
			prometheusValidatorShedUnwindUnverified.Inc()
			v.logger.Errorf("[unwindShed][%s] could not confirm the shed transaction record was deleted after %d attempts; aborting before unspend (fail closed), outpoints %s may be unspendable and need operator recovery: %v", txID, shedUnwindVerifyAttempts, unwindOutpoints(spentUtxos), verifyErr)

			return verifyErr
		}
	}

	if len(spentUtxos) == 0 {
		return nil
	}

	if err := v.utxoStore.Unspend(ctx, spentUtxos); err != nil {
		prometheusValidatorShedUnwindFailures.Inc()
		v.logger.Errorf("[unwindShed][%s] failed to unspend %d input(s) of a shed transaction; outpoints %s are temporarily unspendable and need operator recovery: %v", txID, len(spentUtxos), unwindOutpoints(spentUtxos), err)

		return err
	}

	return nil
}

// verifyRecordDeleted reports whether the record is provably gone, retrying a failed
// read a bounded number of times before giving up.
//
// Return shapes:
//
//	(true, nil)    ErrTxNotFound: provably deleted, safe to unspend.
//	(false, nil)   the record is readable, so the store did not honour Delete
//	               (TxMetaCache's cache-only Delete is the known case). Conclusive, so
//	               it is not retried.
//	(false, err)   inconclusive after every attempt. The caller fails closed.
//
// The wait between attempts selects on ctx.Done() so shedUnwindTimeout actually cuts
// the retry short rather than being carried and ignored.
func (v *Validator) verifyRecordDeleted(ctx context.Context, txHash *chainhash.Hash) (bool, error) {
	var lastErr error

	for attempt := 0; attempt < shedUnwindVerifyAttempts; attempt++ {
		if attempt > 0 {
			timer := time.NewTimer(shedUnwindVerifyBackoff)

			select {
			case <-ctx.Done():
				timer.Stop()

				return false, ctx.Err()
			case <-timer.C:
			}
		}

		err := v.utxoStore.GetMeta(ctx, txHash, &meta.Data{})

		switch {
		case err == nil:
			return false, nil
		case errors.Is(err, errors.ErrTxNotFound):
			return true, nil
		default:
			lastErr = err
		}
	}

	return false, lastErr
}

// unwindOutpoints renders a spend set as a single-line, comma-separated
// txid:vout list for the recovery log line an operator acts on.
func unwindOutpoints(spends []*utxo.Spend) string {
	parts := make([]string, 0, len(spends))

	for _, s := range spends {
		if s == nil || s.TxID == nil {
			continue
		}

		parts = append(parts, fmt.Sprintf("%s:%d", s.TxID.String(), s.Vout))
	}

	return strings.Join(parts, ",")
}

// extendTransaction adds previous output information to transaction inputs.
// Returns error if required parent transaction data cannot be found.
func (v *Validator) extendTransaction(ctx context.Context, tx *bt.Tx) error {
	ctx, span, deferFn := tracing.Tracer("validator").Start(ctx, "extendTransaction",
		tracing.WithHistogram(prometheusTransactionExtend),
	)
	defer deferFn()

	if tx.IsCoinbase() {
		return nil
	}

	if err := v.utxoStore.PreviousOutputsDecorate(ctx, tx); err != nil {
		if errors.Is(err, errors.ErrTxNotFound) {
			err = errors.NewTxMissingParentError("error extending transaction, parent tx not found", err)
			span.RecordError(err)

			return err
		}

		err = errors.NewProcessingError("can't extend transaction %s", tx.TxIDChainHash().String(), err)
		span.RecordError(err)

		return err
	}

	tx.SetExtended(true)
	return nil
}

// mtpReorgOverlap is the number of already-stored MTP values that EnsureMTPLoaded
// re-fetches on every extension call to detect and repair reorg-invalidated entries.
//
// A block reorg at depth D invalidates MTP values for the following 11 heights
// (one full MTP window). Overlapping by D+11 therefore catches any reorg of depth D.
// BSV reorgs are extremely shallow in practice (depth ≤ 1–2), so 12 is a safe,
// cheap constant that covers the realistic worst case.
const mtpReorgOverlap = 12

// EnsureMTPLoaded pre-warms the in-memory MTP store up to (blockHeight - 1).
// This must be called once per block, before concurrent per-transaction goroutines start,
// so that BIP68 MTP lookups inside each goroutine are pure array reads with no gRPC calls.
//
// If BIP68 is not yet active (blockHeight < CSVHeight) or no blockchain client is
// configured, this is a no-op.
//
// When the store already covers the needed range this is a fast O(1) no-op.
// When new heights extend beyond the loaded range, the fetch includes a backward
// overlap of mtpReorgOverlap heights. Any already-stored values that differ from
// the freshly fetched ones (reorg-invalidated) are corrected in-place before the
// new tail is appended.
func (v *Validator) EnsureMTPLoaded(ctx context.Context, blockHeight uint32) error {
	csvHeight := uint32(v.settings.ChainCfgParams.CSVHeight)
	if v.blockchainClient == nil || blockHeight == 0 || blockHeight < csvHeight {
		return nil
	}

	// The highest MTP index we guarantee is blockHeight:
	//   - blockMTPHeight = blockHeight: GetMedianTimePastRange computes stored_mtp(N)
	//     on the fly for the not-yet-persisted block N from block_time values [N-11, N-1].
	//   - utxoHeights *may* exceed blockHeight: unconfirmed parents are stamped with the
	//     unconfirmedParentHeight sentinel (0xFFFFFFFF). In consensus mode BDK rejects
	//     before BIP68 runs; in policy mode BIP68 is gated out — so readMTPsLocked never
	//     actually sees the sentinel, but its `h >= storeLen` clamp still protects.

	needed := blockHeight

	v.mtpMu.Lock()
	defer v.mtpMu.Unlock()

	// Fast path: store already covers the needed height.  A concurrent EnsureMTPLoaded
	// that won the lock may have already populated the store; re-checking here avoids a
	// redundant gRPC fetch.
	currentLen := uint32(len(v.mtpStore))
	if currentLen > needed {
		return nil
	}

	// Compute the fetch start, extending back by mtpReorgOverlap so we re-check
	// recently stored values. This repairs any MTP entries that were invalidated by
	// a chain reorg: a reorg at depth D corrupts stored MTP values for the next 11
	// heights, so overlapping by 12 catches reorgs of depth ≤ 1 (the realistic case).
	var fromHeight uint32
	if currentLen > mtpReorgOverlap {
		fromHeight = currentLen - mtpReorgOverlap
	}

	isInitialLoad := currentLen == 0
	start := time.Now()

	fetched, err := v.blockchainClient.GetMedianTimePastRange(ctx, fromHeight, needed)
	if err != nil {
		return errors.NewProcessingError("[Validator][EnsureMTPLoaded] failed to fetch MTPs from height %d to %d", fromHeight, needed, err)
	}

	expected := needed - fromHeight + 1
	if uint32(len(fetched)) != expected {
		return errors.NewProcessingError("[Validator][EnsureMTPLoaded] MTP count mismatch: expected %d, got %d", expected, len(fetched))
	}

	// Patch any overlap values that changed (reorg-invalidated entries).
	for i := fromHeight; i < currentLen; i++ {
		if v.mtpStore[i] != fetched[i-fromHeight] {
			v.mtpStore[i] = fetched[i-fromHeight]
		}
	}

	// Append the new tail beyond the previously loaded range.
	v.mtpStore = append(v.mtpStore, fetched[currentLen-fromHeight:]...)

	if isInitialLoad {
		v.logger.Infof("[Validator][EnsureMTPLoaded] initial MTP store loaded: %d entries (heights 0..%d) in %s", len(v.mtpStore), needed, time.Since(start))
	} else {
		v.logger.Debugf("[Validator][EnsureMTPLoaded] extended MTP store to height %d (+%d entries) in %s", needed, needed-currentLen+1, time.Since(start))
	}

	return nil
}

// validateTransaction performs Teranode-owned transaction checks, BDK
// transaction validation, and BIP68 sequence-lock validation.
//
// Phase 1 keeps checks that need local node context, including fee policy and
// cache-size limits, and runs BDK transaction validation.
//
// Phase 2 is BIP68 sequence-lock validation (block context only) via
// txValidator.ValidateBIP68.
//
// Phase 2 is only executed when phase 1 succeeds and SkipPolicyChecks is true (block context).
// This avoids the cost of MTP lookups when a transaction fails normal validation.
// MTP values are read from v.mtpStore, pre-loaded by EnsureMTPLoaded before concurrent
// goroutines start, so no gRPC calls or locking are needed here.
func (v *Validator) validateTransaction(ctx context.Context, tx *bt.Tx, blockHeight uint32, utxoHeights []uint32, validationOptions *Options) error {
	ctx, span, deferFn := tracing.Tracer("validator").Start(ctx, "validateTransaction",
		tracing.WithHistogram(prometheusTransactionValidate),
	)
	defer deferFn()

	// 0) Check whether we have a complete transaction in extended format, with all input information
	//    we cannot check the satoshi input, OP_RETURN is allowed 0 satoshis
	//
	// OutpointOnlySpend (below-checkpoint fast path): the tx is intentionally left
	// un-extended — validateInternal deliberately skipped the parent read (see ~735).
	// Do NOT re-extend here: OutpointOnlySpend requires SkipScriptValidation, so BDK
	// never consumes the extension, BIP68 reads parent heights (not scripts), and the
	// spend is outpoint-only. Without this guard validateTransaction re-issues the exact
	// per-parent PreviousOutputsDecorate reads the fast path exists to eliminate.
	if !validationOptions.OutpointOnlySpend && !tx.IsExtended() {
		if err := v.extendTransaction(ctx, tx); err != nil {
			// error is already wrapped in our errors package
			span.RecordError(err)

			return err
		}
	}

	// Legacy block-sync resolution: substitute the unconfirmedParentHeight
	// sentinel with the candidate block height BEFORE any consumer sees it —
	// both the BDK call in phase 1 (per-input era-flag selection, where the
	// sentinel would otherwise translate to MEMPOOL_HEIGHT and reject with
	// bad-txns-unconfirmed-input-in-block) and the BIP68/MTP lookups in
	// phase 2. On the legacy path an unconfirmed parent IS a same-block
	// parent, so the candidate height is its true height. See
	// Options.UnconfirmedParentsAtCandidateHeight for the consensus-safety
	// contract; the floater backstop is block validation's
	// checkParentsExistOnChain.
	// No AddTXToBlockAssembly guard here, deliberately (an earlier revision
	// hard-errored on flag+assembly): the legacy branch must set this flag in
	// EVERY FSM state — a restarted node with FSM restored to RUNNING catches
	// up over the legacy bridge and wedges without it — while assembly stays
	// enabled in RUNNING for reorg resilience. The combination is safe: a
	// floater child blessed at the candidate height and added to assembly is
	// the same tx policy-mode admission would have accepted into assembly
	// (policy substitutes tip+1 for unconfirmed parents — equal to the
	// candidate height at the tip; era flags cannot differ post-Genesis), and
	// accepted-block txs are mined-removed from assembly as always.
	if validationOptions.UnconfirmedParentsAtCandidateHeight {
		utxoHeights = resolveUnconfirmedParentsAtCandidateHeight(utxoHeights, blockHeight)
	}

	// Phase 1: run Teranode-owned checks and BDK transaction validation.
	if err := v.txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, validationOptions); err != nil {
		span.RecordError(err)
		return err
	}

	// Phase 2: BIP68 sequence-lock validation — only for block context
	// (SkipPolicyChecks == true) and only when BIP68 is active
	// (blockHeight >= CSVHeight). Performed after phase 1 so that MTP lookups
	// are skipped for invalid transactions.
	//
	// Policy mode (peer-received txs) deliberately does NOT run BIP68 — this
	// is a stable design decision, not a missing check. Two reasons:
	//
	//  1. Post-Genesis, BIP68 short-circuits to no-op anyway. BSV Genesis
	//     restored the original Bitcoin nSequence semantics (RBF signalling
	//     only, no relative lock-time enforcement); see the post-Genesis
	//     early-return in TxValidator.sequenceLocks. Running BIP68 in
	//     current-mainnet policy mode would do zero observable work.
	//
	//  2. Pre-Genesis policy mode is only reachable in regtest / synthetic
	//     test scenarios. Mainnet IBD validates historical pre-Genesis
	//     blocks via consensus mode (SkipPolicyChecks=true), which already
	//     runs BIP68 below — peer-received txs never arrive in a
	//     pre-Genesis state on a real mainnet node.
	//
	// Benefits of confining BIP68 to consensus mode:
	//  - Keeps the peer-tx admission hot path simple — no MTP plumbing.
	//  - Keeps the MTP store and EnsureMTPLoaded pre-warming entirely out
	//    of the policy path; MTP infrastructure exists solely for
	//    block-validation batching.
	//  - Per-tx policy-mode MTP lookups (synchronous gRPC / DB I/O per
	//    peer tx) are avoided. Consensus mode amortises a single
	//    EnsureMTPLoaded call across an entire block of txs validated
	//    concurrently; policy mode would have to either pay that cost
	//    per-tx or keep the MTP cache always warm regardless of need.
	// OutpointOnlySpend (below-checkpoint fast path): BIP68 is a consensus validity
	// check already certified by the pinned hardcoded checkpoint, and validateInternal
	// intentionally left utxoHeights empty — so skip BIP68 here (its only consumer of
	// utxoHeights). Same basis as skipping script validation below checkpoint.
	if validationOptions.OutpointOnlySpend || !validationOptions.SkipPolicyChecks || v.blockchainClient == nil || blockHeight < uint32(v.settings.ChainCfgParams.CSVHeight) {
		return nil
	}

	// Build utxoMTPs and blockMTP from the pre-loaded mtpStore (populated by EnsureMTPLoaded).
	//
	// Teranode stores MTP(H) = median of block timestamps [H-11, H-1].
	// BSV's GetMedianTimePast() at block H = median of [H-11, H-1] (per BIP113, block H
	// itself is never included), so BSV MTP(H) == Teranode stored_mtp(H).
	//
	// For UTXO coin time: BSV uses GetAncestor(nCoinHeight-1)->GetMedianTimePast()
	//   = median of [nCoinHeight-11, nCoinHeight-1]
	//   = Teranode stored_mtp(nCoinHeight) → use utxoHeight directly.
	//
	// For block time: BSV uses block.GetPrev()->GetMedianTimePast()
	//   = median of [blockHeight-11, blockHeight-1]
	//   = Teranode stored_mtp(blockHeight). Block N is not yet persisted during
	//   validation, so stored_mtp(N) is not in the DB; GetMedianTimePastRange
	//   computes it on the fly from the block_time values of [N-11, N-1] which
	//   ARE in the DB, and EnsureMTPLoaded stores the result at mtpStore[blockHeight].
	blockMTPHeight := blockHeight

	// Hold the read lock only for the MTP lookups themselves, not for the subsequent
	// ValidateBIP68 call which works on the copied utxoMTPs / blockMTP values. This
	// serialises against EnsureMTPLoaded writers (append + in-place overlap patch) for
	// the cross-block case (block N+1 extending mtpStore while block N's per-tx
	// goroutines read it) without holding the lock through ECDSA / sequence-lock
	// arithmetic. RLock is uncontended in the steady-state path where EnsureMTPLoaded
	// has already populated the range.
	utxoMTPs, blockMTP, err := v.readMTPsLocked(blockMTPHeight, utxoHeights)
	if err != nil {
		span.RecordError(err)
		return err
	}

	return v.txValidator.ValidateBIP68(tx, blockHeight, utxoHeights, utxoMTPs, blockMTP)
}

// readMTPsLocked returns the per-input MTP values and the block MTP for use by
// validateTransaction. It takes the mtpStore read lock for the duration of the
// reads only and releases it before returning. The caller is free to use the
// returned slice / value without further synchronisation.
func (v *Validator) readMTPsLocked(blockMTPHeight uint32, utxoHeights []uint32) ([]uint32, uint32, error) {
	v.mtpMu.RLock()
	defer v.mtpMu.RUnlock()

	// Guard against a missing EnsureMTPLoaded call. In normal operation this cannot
	// happen because Server.go calls EnsureMTPLoaded before spawning goroutines.
	if uint32(len(v.mtpStore)) <= blockMTPHeight {
		return nil, 0, errors.NewProcessingError("[Validator][validateTransaction] MTP store not loaded up to height %d (store length %d); EnsureMTPLoaded must be called before block validation", blockMTPHeight, len(v.mtpStore))
	}

	storeLen := uint32(len(v.mtpStore))
	utxoMTPs := make([]uint32, len(utxoHeights))

	for i, h := range utxoHeights {
		if h >= storeLen {
			utxoMTPs[i] = v.mtpStore[blockMTPHeight]
		} else {
			utxoMTPs[i] = v.mtpStore[h]
		}
	}

	return utxoMTPs, v.mtpStore[blockMTPHeight], nil
}
