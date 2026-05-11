/*
Package validator implements BSV Blockchain transaction validation functionality.

This package provides comprehensive transaction validation for BSV Blockchain nodes,
including script verification, UTXO management, and policy enforcement. It supports
multiple script interpreters and implements the full Bitcoin transaction validation ruleset.
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
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// Constants defining key validation parameters and limits for Bitcoin consensus rules.
// These constants establish the fundamental constraints that govern transaction and block validation,
// ensuring compliance with Bitcoin protocol specifications and network consensus requirements.
const (
	// MaxBlockSize defines the maximum allowed size of a block in bytes (4GB).
	// This limit governs the maximum amount of transaction data that can be included in a single block,
	// directly impacting network throughput and scalability. Blocks exceeding this size are rejected
	// as invalid by the consensus rules, ensuring network stability and preventing resource exhaustion.
	MaxBlockSize = 4 * 1024 * 1024 * 1024

	// MaxTxSizeConsensusBeforeGenesis defines the consensus limit for transaction size before Genesis (1 MB).
	// This matches C++ bitcoin-sv: MAX_TX_SIZE_CONSENSUS_BEFORE_GENESIS in consensus/consensus.h
	// Transactions exceeding this size are invalid by consensus rules pre-Genesis.
	MaxTxSizeConsensusBeforeGenesis = 1_000_000 // 1 MB

	// MaxTxSizeConsensusAfterGenesis defines the consensus limit for transaction size after Genesis (1 GB).
	// This matches C++ bitcoin-sv: MAX_TX_SIZE_CONSENSUS_AFTER_GENESIS in consensus/consensus.h
	// Transactions exceeding this size are invalid by consensus rules post-Genesis.
	MaxTxSizeConsensusAfterGenesis = 1_000_000_000 // 1 GB

	// MaxSatoshis defines the maximum number of satoshis that can exist in the Bitcoin SV ecosystem (21M BSV).
	// This represents the absolute monetary supply limit, with each BSV consisting of 100,000,000 satoshis.
	// Any transaction that would create more satoshis than this limit violates consensus rules and must be
	// rejected to maintain the integrity of the monetary system and prevent inflation attacks.
	MaxSatoshis = 21_000_000_00_000_000

	// coinbaseTxID represents the special transaction ID used for coinbase transactions.
	// Coinbase transactions are the first transaction in each block and create new bitcoins as mining rewards.
	// This constant is used to identify and handle coinbase transactions differently from regular transactions
	// during validation, as they have special rules and don't spend existing UTXOs.
	coinbaseTxID = "0000000000000000000000000000000000000000000000000000000000000000"

	// MaxTxSigopsCountPolicyAfterGenesis defines the maximum number of signature
	// operations allowed in a transaction after the Genesis upgrade (UINT32_MAX).
	MaxTxSigopsCountPolicyAfterGenesis = ^uint32(0)

	// DustLimit defines the minimum output value in satoshis (1 satoshi)
	// Outputs with less than this value are considered dust unless they are
	// not spendable (OP_FALSE OP_RETURN).  This applies to outputs after the
	// Genesis upgrade.
	DustLimit = uint64(1)

	// txmetaActionADD represents the ADD action for txmeta batch messages
	txmetaActionADD = byte(0)
	// txmetaActionDELETE represents the DELETE action for txmeta batch messages
	txmetaActionDELETE = byte(1)
)

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
	blockAssemblyClient blockassembly.ClientI, blockchainClient blockchain.ClientI) (Interface, error) {
	initPrometheusMetrics()

	var ba blockassembly.Store

	if !tSettings.BlockAssembly.Disabled {
		ba = blockAssemblyClient
	}

	v := &Validator{
		logger:                        logger,
		settings:                      tSettings,
		txValidator:                   NewTxValidator(logger, tSettings),
		utxoStore:                     store,
		blockAssembler:                ba,
		stats:                         gocore.NewStat("validator"),
		txmetaKafkaProducerClient:     txMetaKafkaProducerClient,
		rejectedTxKafkaProducerClient: rejectedTxKafkaProducerClient,
		blockchainClient:              blockchainClient,
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
		v.txmetaKafkaBatcher = b
		logger.Infof("TxMeta Kafka batching enabled: batchSize=%d, timeout=%dms", txmetaKafkaBatchSize, txmetaKafkaBatchTimeout)
	}

	return v, nil
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

// GetBlockState returns an atomic snapshot of both block height and median block time
// from the UTXO store. This prevents race conditions that could occur when reading
// these values separately, ensuring consistency during validation.
func (v *Validator) GetBlockState() utxo.BlockState {
	return v.utxoStore.GetBlockState()
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

	// Configurable retry for TX_LOCKED errors with exponential backoff.
	// TX_LOCKED occurs when a parent and child tx arrive nearly simultaneously and the
	// parent hasn't finished its 2-phase commit (unlock). This is a short-lived race
	// condition that resolves once the parent's lock clears. Set maxRetries to 0 to
	// disable and return TX_LOCKED immediately to the caller.
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

		// If no error or not a TX_LOCKED error, break immediately (don't retry)
		if err == nil || !errors.Is(err, errors.ErrTxLocked) {
			break
		}

		// TX_LOCKED error on the last attempt — give up
		if attempt >= maxRetries {
			ctxLogger.Warnf("[ValidateWithOptions] TX_LOCKED for tx %s after %d retries, giving up: %v", tx.TxID(), attempt, err)
			break
		}

		// Exponential backoff: 10ms, 20ms, 40ms, ...
		backoff := time.Duration(1<<uint(attempt)) * baseBackoff
		ctxLogger.Debugf("[ValidateWithOptions] TX_LOCKED for tx %s, retrying in %v (retry %d/%d): %v", tx.TxID(), backoff, attempt+1, maxRetries, err)

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

					if *state == blockchain_api.FSMStateType_CATCHINGBLOCKS || *state == blockchain_api.FSMStateType_LEGACYSYNCING {
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

				value, err := proto.Marshal(m)
				if err != nil {
					return nil, err
				}

				v.rejectedTxKafkaProducerClient.Publish(&kafka.Message{
					Key:   []byte(txID),
					Value: value,
				})

				prometheusValidatorSendToP2PKafka.Observe(float64(time.Since(startKafka).Microseconds()) / 1_000_000)
			}
		}
	}

	return txMetaData, err
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

	// We do not check IsFinal for transactions before BIP113 change (block height 419328)
	// This is an exception for transactions before the media block time was used
	if blockHeight > v.settings.ChainCfgParams.CSVHeight {

		utxoStoreMedianBlockTime := blockState.MedianTime
		if utxoStoreMedianBlockTime == 0 {
			err = errors.NewProcessingError("utxo store not ready, block height: %d, median block time: %d", blockHeight, utxoStoreMedianBlockTime)
			span.RecordError(err)

			return nil, err
		}

		// this function should be moved into go-bt
		if err = util.IsTransactionFinal(tx, blockHeight, utxoStoreMedianBlockTime); err != nil {
			err = errors.NewUtxoNonFinalError("[Validate][%s] transaction is not final", txID, err)
			span.RecordError(err)

			return nil, err
		}
	}

	if tx.IsCoinbase() {
		err = errors.NewProcessingError("[Validate][%s] coinbase transactions are not supported", txID)
		span.RecordError(err)

		return nil, err
	}

	var utxoHeights []uint32

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

	// validate the transaction format, consensus rules etc.
	// this does not validate the signatures in the transaction yet
	if err = v.validateTransaction(ctx, tx, blockHeight, utxoHeights, validationOptions); err != nil {
		err = errors.NewProcessingError("[Validate][%s] error validating transaction", txID, err)
		span.RecordError(err)

		return nil, err
	}

	// validate the transaction scripts and signatures
	if err = v.validateTransactionScripts(ctx, tx, blockHeight, utxoHeights, validationOptions); err != nil {
		err = errors.NewProcessingError("[Validate][%s] error validating transaction scripts", txID, err)
		span.RecordError(err)

		return nil, err
	}

	// decouple the tracing context to not cancel the context when finalize the block assembly
	decoupledCtx, _, deferFn := tracing.DecoupleTracingSpan(ctx, "validator", "decoupledSpan")
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

	// this will reverse the spends if there is an error
	if spentUtxos, err = v.spendUtxos(decoupledCtx, tx, blockHeight, validationOptions.IgnoreLocked); err != nil {
		if errors.Is(err, errors.ErrUtxoError) {
			saveAsConflicting := false

			var spendErrs *errors.Error

			for _, spend := range spentUtxos {
				if spend.Err != nil {
					if validationOptions.CreateConflicting && (errors.Is(spend.Err, errors.ErrSpent) || errors.Is(spend.Err, errors.ErrTxConflicting)) {
						saveAsConflicting = true
					}

					var spendErr *errors.Error
					if errors.As(spend.Err, &spendErr) {
						if spendErrs == nil {
							spendErrs = errors.New(spendErr.Code(), spendErr.Message(), spendErr)
						} else {
							spendErrs = errors.New(spendErrs.Code(), spendErrs.Message(), spendErr)
						}
					}
				}
			}

			if spendErrs != nil {
				if errors.As(err, &tErr) {
					tErr.SetWrappedErr(spendErrs)
				}
			}

			if saveAsConflicting {
				if txMetaData, utxoMapErr = v.CreateInUtxoStore(decoupledCtx, tx, blockHeight, true, false); utxoMapErr != nil {
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
			// the parent transaction was not found, this can happen when the parent tx has been DAH'd and removed from
			// the utxo store. We can check whether the tx already exists, which means it has been validated and
			// blessed. In this case we can just return early.
			txMetaData = &meta.Data{}
			if err = v.utxoStore.GetMeta(decoupledCtx, tx.TxIDChainHash(), txMetaData); err == nil {
				v.logger.Warnf("[Validate][%s] parent tx not found, but tx already exists in store, assuming already blessed", txID)

				return txMetaData, nil
			}
		}

		err = errors.NewProcessingError("[Validate][%s] error spending utxos", txID, err)
		span.RecordError(err)

		return nil, err
	}

	// the option blockAssemblyDisabled is false by default
	blockAssemblyEnabled := !v.settings.BlockAssembly.Disabled
	addToBlockAssembly := blockAssemblyEnabled && validationOptions.AddTXToBlockAssembly

	if !validationOptions.SkipUtxoCreation {
		// store the transaction in the UTXO store, marking it as locked if we are going to add it to the block assembly
		txMetaData, err = v.CreateInUtxoStore(decoupledCtx, tx, blockHeight, false, addToBlockAssembly)
		if err != nil {
			if errors.Is(err, errors.ErrTxExists) {
				v.logger.Debugf("[Validate][%s] tx already exists in store, not sending to block assembly: %v", txID, err)

				txMetaData = &meta.Data{}
				if err = v.utxoStore.GetMeta(decoupledCtx, tx.TxIDChainHash(), txMetaData); err != nil {
					return nil, errors.NewProcessingError("[Validate][%s] failed to get tx meta data from store", txID, err)
				}

				return txMetaData, nil
			}

			v.logger.Errorf("[Validate][%s] error registering tx in metaStore: %v", txID, err)

			if reverseErr := v.reverseSpends(decoupledCtx, spentUtxos); reverseErr != nil {
				err = errors.NewProcessingError("[Validate][%s] error reversing utxo spends: %v", txID, reverseErr, err)
			}

			return nil, errors.NewProcessingError("[Validate][%s] error registering tx in metaStore", txID, err)
		}
	} else {
		// create the tx meta needed for the block assembly
		txMetaData, err = util.TxMetaDataFromTx(tx)
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

		// send the tx to the block assembler
		if err = v.sendToBlockAssembler(decoupledCtx, &blockassembly.Data{
			TxIDChainHash: *tx.TxIDChainHash(),
			Fee:           txMetaData.Fee,
			Size:          uint64(tx.Size()), // nolint:gosec
			TxInpoints:    txInpoints,
		}, spentUtxos); err != nil {
			err = errors.NewProcessingError("[Validate][%s] error sending tx to block assembler", txID, err)
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
	utxoHeights, err := v.getUtxoBlockHeightsAndExtendTx(ctx, tx, txID, validationOptions)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	return utxoHeights, nil
}

// twoPhaseCommitTransaction marks the transaction as spendable
func (v *Validator) twoPhaseCommitTransaction(ctx context.Context, tx *bt.Tx, txID string) error {
	ctx, span, endSpan := tracing.Tracer("validator").Start(ctx, "twoPhaseCommitTransaction",
		tracing.WithHistogram(prometheusTransaction2PhaseCommit),
	)
	defer endSpan()

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

// getUtxoBlockHeightsAndExtendTx returns the block heights for each input of the transaction
func (v *Validator) getUtxoBlockHeightsAndExtendTx(ctx context.Context, tx *bt.Tx, txID string, validationOptions *Options) ([]uint32, error) {
	// get the block heights of the input transactions of the transaction
	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(g, v.settings.UtxoStore.GetBatcherSize)

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
			if err := v.getUtxoBlockHeightAndExtendForParentTx(gCtx, parentTxHash, inputIdxs, utxoHeights, tx, extend, validationOptions); err != nil {
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
func (v *Validator) getUtxoBlockHeightAndExtendForParentTx(gCtx context.Context, parentTxHash chainhash.Hash, idxs []int,
	utxoHeights []uint32, tx *bt.Tx, extend bool, validationOptions *Options) error {

	// OPTIMIZATION: Check if parent metadata is provided in options (for in-block parents)
	// This allows validation without UTXO store lookups for in-block parent transactions
	// SAFETY: Parent metadata only includes transactions that successfully validated AND created UTXOs
	// (see check_block_subtrees.go:buildParentMetadata which filters by successful validations)
	if validationOptions != nil && validationOptions.ParentMetadata != nil {
		if parentMeta, found := validationOptions.ParentMetadata[parentTxHash]; found {
			// Use pre-fetched metadata instead of UTXO store lookup
			// Safe because metadata only includes transactions that completed full validation+storage
			for _, idx := range idxs {
				utxoHeights[idx] = parentMeta.BlockHeight
			}

			// If transaction is already extended, we have all the data we need
			// The parent metadata optimization works best with pre-extended transactions
			if !extend {
				return nil
			}
			// Otherwise fall through to UTXO store to get full transaction for extending
		}
	}

	f := []fields.FieldName{fields.BlockIDs, fields.BlockHeights}

	if extend {
		// add the parent tx outputs to the fields, to be able to extend the transaction
		f = append(f, fields.Tx)
	}

	txMeta, err := v.utxoStore.Get(gCtx, &parentTxHash, f...)
	if err != nil {
		return err
	}

	if len(txMeta.BlockHeights) == 0 {
		// Get atomic block state to ensure consistency
		blockState := v.utxoStore.GetBlockState()
		for _, idx := range idxs {
			utxoHeights[idx] = blockState.Height + 1
		}
	} else {
		for _, idx := range idxs {
			utxoHeights[idx] = txMeta.BlockHeights[0]
		}
	}

	if extend {
		// extend the transaction inputs with the parent tx outputs
		for _, idx := range idxs {
			if idx > len(tx.Inputs) {
				return errors.NewProcessingError("[Validate][%s] input index %d out of bounds for transaction with %d inputs",
					tx.TxIDChainHash().String(), idx, len(tx.Inputs))
			}

			if txMeta.Tx == nil || txMeta.Tx.Outputs == nil || txMeta.Tx.Outputs[tx.Inputs[idx].PreviousTxOutIndex] == nil {
				return errors.NewProcessingError("[Validate][%s] parent transaction %s does not have outputs for input index %d",
					tx.TxIDChainHash().String(), parentTxHash.String(), idx)
			}

			// extend the input with the parent tx outputs
			tx.Inputs[idx].PreviousTxSatoshis = txMeta.Tx.Outputs[tx.Inputs[idx].PreviousTxOutIndex].Satoshis
			tx.Inputs[idx].PreviousTxScript = txMeta.Tx.Outputs[tx.Inputs[idx].PreviousTxOutIndex].LockingScript
		}
	}

	return nil
}

func (v *Validator) TriggerBatcher() {
	// Noop
}

// CreateInUtxoStore stores transaction metadata in the UTXO store.
// Returns transaction metadata and error if storage fails.
func (v *Validator) CreateInUtxoStore(ctx context.Context, tx *bt.Tx, blockHeight uint32, markAsConflicting bool,
	markAsLocked bool) (*meta.Data, error) {
	ctx, _, deferFn := tracing.Tracer("validator").Start(ctx, "storeTxInUtxoMap",
		tracing.WithHistogram(prometheusValidatorSetTxMeta),
	)
	defer deferFn()

	createOptions := []utxo.CreateOption{
		utxo.WithConflicting(markAsConflicting),
	}

	if markAsLocked {
		createOptions = append(createOptions, utxo.WithLocked(true))
	}

	txMetaData, err := v.utxoStore.Create(ctx, tx, blockHeight, createOptions...)
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
		// Fallback: send single item as batch format for consistency
		value := serializeTxMetaBatch([]*txmetaBatchItem{{
			hash:      txHash,
			metaBytes: metaBytes,
			isDelete:  false,
		}})

		v.txmetaKafkaProducerClient.Publish(&kafka.Message{
			Key:   nil,
			Value: value,
		})
	}

	prometheusValidatorSendToBlockValidationKafka.Observe(float64(time.Since(startKafka).Microseconds()) / 1_000_000)

	return nil
}

// sendTxMetaBatch serializes and publishes a batch of TxMeta items to Kafka.
func (v *Validator) sendTxMetaBatch(batch []*txmetaBatchItem) {
	if len(batch) == 0 {
		return
	}

	value := serializeTxMetaBatch(batch)

	v.txmetaKafkaProducerClient.Publish(&kafka.Message{
		Key:   nil,
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
			buf[offset] = txmetaActionDELETE
		} else {
			buf[offset] = txmetaActionADD
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

// spendUtxos attempts to spend the UTXOs referenced by transaction inputs.
// Returns the spent UTXOs and error if spending fails.
func (v *Validator) spendUtxos(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreLocked bool) ([]*utxo.Spend, error) {
	ctx, span, deferFn := tracing.Tracer("validator").Start(ctx, "spendUtxos",
		tracing.WithHistogram(prometheusTransactionSpendUtxos),
	)
	defer deferFn()

	var (
		err error
	)

	spends, err := v.utxoStore.Spend(ctx, tx, blockHeight, utxo.IgnoreFlags{
		IgnoreConflicting: false,
		IgnoreLocked:      ignoreLocked,
	})
	if err != nil {
		span.RecordError(err)

		return spends, errors.NewProcessingError("validator: UTXO Store spend failed for %s", tx.TxIDChainHash().String(), err)
	}

	return spends, nil
}

// sendToBlockAssembler sends validated transaction data to the block assembler.
// Returns error if block assembly integration fails.
func (v *Validator) sendToBlockAssembler(ctx context.Context, bData *blockassembly.Data, reservedUtxos []*utxo.Spend) error {
	ctx, span, deferFn := tracing.Tracer("validator").Start(ctx, "sendToBlockAssembler",
		tracing.WithHistogram(prometheusValidatorSendToBlockAssembly),
	)
	defer deferFn()

	_ = reservedUtxos

	// if v.settings.Validator.VerboseDebug {
	v.logger.Debugf("[Validator] sending tx %s to block assembler", bData.TxIDChainHash.String())
	// }

	if _, err := v.blockAssembler.Store(ctx, &bData.TxIDChainHash, bData.Fee, bData.Size, bData.TxInpoints); err != nil {
		e := errors.NewServiceError("error calling blockAssembler Store()", err)
		span.RecordError(e)

		return e
	}

	return nil
}

// reverseSpends reverses previously spent UTXOs in case of validation failure.
// Attempts up to 3 retries with exponential backoff.
// Returns error if UTXO reversal fails.
func (v *Validator) reverseSpends(ctx context.Context, spentUtxos []*utxo.Spend) error {
	ctx, span, deferFn := tracing.Tracer("validator").Start(ctx, "reverseSpends")
	defer deferFn()

	for retries := uint(0); retries < 3; retries++ {
		if errReset := v.utxoStore.Unspend(ctx, spentUtxos); errReset != nil {
			if retries < 2 {
				backoff := time.Duration(1<<retries) * time.Second
				v.logger.Errorf("error resetting utxos, retrying in %s: %v", backoff.String(), errReset)
				time.Sleep(backoff)
			} else {
				span.RecordError(errReset)
				return errors.NewProcessingError("error resetting utxos", errReset)
			}
		} else {
			break
		}
	}

	return nil
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
	//   - utxoHeights *may* exceed blockHeight when the chain tip advances during
	//     validation (unconfirmed parents get blockState.Height+1); validateTransaction
	//     clamps those lookups to blockMTPHeight.
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

// validateTransaction performs transaction-level validation checks in two phases:
//  1. Full transaction validation (structure, scripts, fees) via txValidator.ValidateTransaction.
//  2. BIP68 sequence-lock validation (block context only) via txValidator.ValidateBIP68.
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
	if !tx.IsExtended() {
		if err := v.extendTransaction(ctx, tx); err != nil {
			// error is already wrapped in our errors package
			span.RecordError(err)

			return err
		}
	}

	// Phase 1: run the internal tx validation, checking policies, scripts, signatures etc.
	if err := v.txValidator.ValidateTransaction(tx, blockHeight, utxoHeights, validationOptions); err != nil {
		span.RecordError(err)
		return err
	}

	// Phase 2: BIP68 sequence-lock validation — only for block context (SkipPolicyChecks == true)
	// and only when BIP68 is active (blockHeight >= CSVHeight).
	// Performed after phase 1 so that MTP lookups are skipped for invalid transactions.
	if !validationOptions.SkipPolicyChecks || v.blockchainClient == nil || blockHeight < uint32(v.settings.ChainCfgParams.CSVHeight) {
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

// validateTransactionScripts performs script validation for a transaction
// Returns error if validation fails
func (v *Validator) validateTransactionScripts(ctx context.Context, tx *bt.Tx, blockHeight uint32, utxoHeights []uint32,
	validationOptions *Options) error {
	ctx, span, deferFn := tracing.Tracer("validator").Start(ctx, "validateTransactionScripts",
		tracing.WithHistogram(prometheusTransactionValidateScripts),
	)
	defer deferFn()

	// 0) Check whether we have a complete transaction in extended format, with all input information
	//    we cannot check the satoshi input, OP_RETURN is allowed 0 satoshis
	if !tx.IsExtended() {
		err := v.extendTransaction(ctx, tx)
		if err != nil {
			// error is already wrapped in our errors package
			span.RecordError(err)
			return err
		}
	}

	// run the internal tx validation, checking policies, scripts, signatures etc.
	return v.txValidator.ValidateTransactionScripts(tx, blockHeight, utxoHeights, validationOptions)
}
