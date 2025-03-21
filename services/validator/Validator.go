/*
Package validator implements Bitcoin SV transaction validation functionality.

This package provides comprehensive transaction validation for Bitcoin SV nodes,
including script verification, UTXO management, and policy enforcement. It supports
multiple script interpreters and implements the full Bitcoin transaction validation ruleset.
*/
package validator

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/blockassembly"
	"github.com/bitcoin-sv/teranode/settings"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/stores/utxo/meta"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util"
	"github.com/bitcoin-sv/teranode/util/health"
	"github.com/bitcoin-sv/teranode/util/kafka"
	kafkamessage "github.com/bitcoin-sv/teranode/util/kafka/kafka_message"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

// Constants defining key validation parameters and limits.
const (
	// MaxBlockSize defines the maximum allowed size of a block in bytes (4GB).
	MaxBlockSize = 4 * 1024 * 1024 * 1024

	// MaxSatoshis defines the maximum number of satoshis that can exist (21M BSV).
	MaxSatoshis = 21_000_000_00_000_000

	// coinbaseTxID represents the transaction ID for coinbase transactions.
	coinbaseTxID = "0000000000000000000000000000000000000000000000000000000000000000"

	// MaxTxSigopsCountPolicyAfterGenesis defines the maximum number of signature
	// operations allowed in a transaction after the Genesis upgrade (UINT32_MAX).
	MaxTxSigopsCountPolicyAfterGenesis = ^uint32(0)
)

// Validator implements Bitcoin SV transaction validation and manages the lifecycle
// of transactions from validation through block assembly.
type Validator struct {
	// logger handles structured logging for the validator
	logger ulogger.Logger

	settings *settings.Settings

	// txValidator performs transaction-specific validation checks
	txValidator TxValidatorI

	// utxoStore manages the UTXO set and transaction metadata
	utxoStore utxo.Store

	// blockAssembler handles block template creation and transaction ordering
	blockAssembler blockassembly.Store

	// saveInParallel indicates if UTXOs should be saved concurrently
	saveInParallel bool

	// stats tracks validator performance metrics
	stats *gocore.Stat

	// txmetaKafkaProducerClient publishes transaction metadata events
	txmetaKafkaProducerClient kafka.KafkaAsyncProducerI

	// rejectedTxKafkaProducerClient publishes rejected transaction events
	rejectedTxKafkaProducerClient kafka.KafkaAsyncProducerI
}

// New creates a new Validator instance with the provided configuration.
// It initializes the validator with the given logger, UTXO store, and Kafka producers.
// Returns an error if initialization fails.
func New(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, store utxo.Store, txMetaKafkaProducerClient kafka.KafkaAsyncProducerI, rejectedTxKafkaProducerClient kafka.KafkaAsyncProducerI) (Interface, error) {
	initPrometheusMetrics()

	var ba blockassembly.Store

	var err error

	if !tSettings.BlockAssembly.Disabled {
		ba, err = blockassembly.NewClient(ctx, logger, tSettings)
		if err != nil {
			return nil, errors.NewServiceError("failed to create block assembly client", err)
		}
	}

	v := &Validator{
		logger:                        logger,
		settings:                      tSettings,
		txValidator:                   NewTxValidator(logger, tSettings),
		utxoStore:                     store,
		blockAssembler:                ba,
		saveInParallel:                true,
		stats:                         gocore.NewStat("validator"),
		txmetaKafkaProducerClient:     txMetaKafkaProducerClient,
		rejectedTxKafkaProducerClient: rejectedTxKafkaProducerClient,
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

// Validate performs comprehensive validation of a transaction.
// It checks transaction finality, validates inputs and outputs, updates the UTXO set,
// and optionally adds the transaction to block assembly.
// Returns error if validation fails.
func (v *Validator) Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...Option) (txMeta *meta.Data, err error) {
	return v.ValidateWithOptions(ctx, tx, blockHeight, ProcessOptions(opts...))
}

// ValidateWithOptions performs comprehensive validation of a transaction.
// It checks transaction finality, validates inputs and outputs, updates the UTXO set,
// and optionally adds the transaction to block assembly.
// Returns error if validation fails.
func (v *Validator) ValidateWithOptions(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (txMetaData *meta.Data, err error) {
	if txMetaData, err = v.validateInternal(ctx, tx, blockHeight, validationOptions); err != nil {
		v.logger.Errorf("[ValidateWithOptions] failed to validate transaction: %v", err)

		if v.rejectedTxKafkaProducerClient != nil { // tests may not set this
			// TODO which errors should we be sending here?
			if !errors.Is(err, errors.ErrStorageError) && !errors.Is(err, errors.ErrServiceError) {
				startKafka := time.Now()

				m := &kafkamessage.KafkaRejectedTxTopicMessage{
					TxHash: tx.TxIDChainHash().CloneBytes(),
					Reason: err.Error(),
				}

				value, err := proto.Marshal(m)
				if err != nil {
					return nil, err
				}

				v.rejectedTxKafkaProducerClient.Publish(&kafka.Message{
					Value: value,
				})

				prometheusValidatorSendToP2PKafka.Observe(float64(time.Since(startKafka).Microseconds()) / 1_000_000)
			}
		}
	}

	return txMetaData, err
}

// validateInternal performs the core validation logic for a transaction.
// It handles UTXO spending, transaction metadata storage, and block assembly integration.
// Returns error if any validation step fails.
//
//gocognit:ignore
func (v *Validator) validateInternal(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (txMetaData *meta.Data, err error) {
	txID := tx.TxID()
	ctx, _, deferFn := tracing.StartTracing(ctx, "Validator:Validate",
		tracing.WithParentStat(v.stats),
		tracing.WithHistogram(prometheusTransactionValidateTotal),
		tracing.WithTag("txid", txID),
	)

	defer func() {
		deferFn(err)
	}()

	// this cached the tx hash in the object for the duration of all operations. It's immutable, so not a problem
	tx.SetTxHash(tx.TxIDChainHash())

	if v.settings.Validator.VerboseDebug {
		v.logger.Debugf("[Validator:Validate] called for %s", txID)

		defer func() {
			v.logger.Debugf("[Validator:Validate] called for %s DONE", txID)
		}()
	}

	var spentUtxos []*utxo.Spend

	if blockHeight == 0 {
		// set block height from the utxo store
		blockHeight = v.GetBlockHeight()
	}

	// We do not check IsFinal for transactions before BIP113 change (block height 419328)
	// This is an exception for transactions before the media block time was used
	if blockHeight > v.settings.ChainCfgParams.CSVHeight {
		utxoStoreMedianBlockTime := v.GetMedianBlockTime()
		if utxoStoreMedianBlockTime == 0 {
			return nil, errors.NewProcessingError("utxo store not ready, block height: %d, median block time: %d", blockHeight, utxoStoreMedianBlockTime)
		}

		// this function should be moved into go-bt
		if err = util.IsTransactionFinal(tx, blockHeight+1, utxoStoreMedianBlockTime); err != nil {
			return nil, errors.NewUtxoNonFinalError("[Validate][%s] transaction is not final", txID, err)
		}
	}

	if tx.IsCoinbase() {
		return nil, errors.NewProcessingError("[Validate][%s] coinbase transactions are not supported", txID)
	}

	// validate the transaction format, consensus rules etc.
	// this does not validate the signatures in the transaction yet
	if err = v.validateTransaction(ctx, tx, blockHeight, validationOptions); err != nil {
		return nil, errors.NewProcessingError("[Validate][%s] error validating transaction", txID, err)
	}

	// get the utxo heights for each input
	utxoHeights, err := v.getUtxoBlockHeights(ctx, tx, txID)
	if err != nil {
		return nil, err
	}

	// validate the transaction scripts and signatures
	if err = v.validateTransactionScripts(ctx, tx, blockHeight, utxoHeights, validationOptions); err != nil {
		return nil, errors.NewProcessingError("[Validate][%s] error validating transaction scripts", txID, err)
	}

	// decouple the tracing context to not cancel the context when finalize the block assembly
	setSpan := tracing.DecoupleTracingSpan(ctx, "decoupledSpan")
	defer setSpan.Finish()

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
	if spentUtxos, err = v.spendUtxos(setSpan, tx, validationOptions.IgnoreUnspendable); err != nil {
		if errors.Is(err, errors.ErrTxInvalid) {
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
							spendErrs = errors.New(spendErr.Code(), spendErr.Message())
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
				if txMetaData, utxoMapErr = v.CreateInUtxoStore(setSpan, tx, blockHeight, true, false); utxoMapErr != nil {
					return txMetaData, utxoMapErr
				}

				// We successfully added the tx to the utxo store as a conflicting tx,
				// so we can return a conflicting error
				return txMetaData, errors.NewTxConflictingError("[Validate][%s] tx is conflicting", txID, err)
			}
		} else if errors.Is(err, errors.ErrTxNotFound) {
			// the parent transaction was not found, this can happen when the parent tx has been ttl'd and removed from
			// the utxo store. We can check whether the tx already exists, which means it has been validated and
			// blessed. In this case we can just return early.
			if txMetaData, err = v.utxoStore.Get(setSpan.Ctx, tx.TxIDChainHash()); err == nil {
				v.logger.Warnf("[Validate][%s] parent tx not found, but tx already exists in store, assuming already blessed", txID)
				return txMetaData, nil
			}
		}

		return nil, errors.NewProcessingError("[Validate][%s] error spending utxos", txID, err)
	}

	// the option blockAssemblyDisabled is false by default
	blockAssemblyEnabled := !v.settings.BlockAssembly.Disabled
	addToBlockAssembly := blockAssemblyEnabled && validationOptions.AddTXToBlockAssembly

	if !validationOptions.SkipUtxoCreation {
		// store the transaction in the UTXO store, marking it as unspendable if we are going to add it to the block assembly
		txMetaData, err = v.CreateInUtxoStore(setSpan, tx, blockHeight, false, addToBlockAssembly)
		if err != nil {
			if errors.Is(err, errors.ErrTxExists) {
				v.logger.Debugf("[Validate][%s] tx already exists in store, not sending to block assembly: %v", txID, err)

				return txMetaData, nil
			}

			v.logger.Errorf("[Validate][%s] error registering tx in metaStore: %v", txID, err)

			if reverseErr := v.reverseSpends(setSpan, spentUtxos); reverseErr != nil {
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
		parentTxHashes := make([]chainhash.Hash, len(tx.Inputs))
		for i, input := range tx.Inputs {
			parentTxHashes[i] = *input.PreviousTxIDChainHash()
		}

		// first we send the tx to the block assembler
		if err = v.sendToBlockAssembler(setSpan, &blockassembly.Data{
			TxIDChainHash: tx.TxIDChainHash(),
			Fee:           txMetaData.Fee,
			Size:          uint64(tx.Size()),
		}, spentUtxos); err != nil {
			err = errors.NewProcessingError("[Validate][%s] error sending tx to block assembler", txID, err)

			if reverseErr := v.reverseTxMetaStore(setSpan, tx.TxIDChainHash()); reverseErr != nil {
				// add reverseErr to the message, wrap the error
				err = errors.NewProcessingError("[Validate][%s] error reversing tx meta utxoStore: %v", txID, reverseErr, err)
			}

			if reverseErr := v.reverseSpends(setSpan, spentUtxos); reverseErr != nil {
				// add reverseErr to the message, wrap the err
				err = errors.NewProcessingError("[Validate][%s] error reversing utxo spends: %v", txID, reverseErr, err)
			}

			setSpan.RecordError(err)

			return nil, err
		}
	}

	// send the txMetaData over to the subtree validation kafka topic
	if v.txmetaKafkaProducerClient != nil {
		if err = v.sendTxMetaToKafka(txMetaData, tx.TxIDChainHash()); err != nil {
			return nil, err
		}
	}

	if txMetaData.Unspendable {
		// the tx was marked as unspendable on creation, we have added it successfully to block assembly
		// so we can now mark it as spendable again
		if err = v.utxoStore.SetUnspendable(setSpan.Ctx, []chainhash.Hash{*tx.TxIDChainHash()}, false); err != nil {
			// this is not a fatal error, since the transaction will we marked as spendable on the next block it's mined into
			return nil, errors.NewProcessingError("[Validate][%s] error marking tx as spendable", txID, err)
		}

		txMetaData.Unspendable = false
	}

	return txMetaData, nil
}

func (v *Validator) getUtxoBlockHeights(ctx context.Context, tx *bt.Tx, txID string) ([]uint32, error) {
	// get the block heights of the input transactions of the transaction
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(v.settings.UtxoStore.GetBatcherSize)

	parentTxHashes := make(map[chainhash.Hash][]int)
	utxoHeights := make([]uint32, len(tx.Inputs))

	for idx, input := range tx.Inputs {
		parentTxHash := input.PreviousTxIDChainHash()

		if _, ok := parentTxHashes[*parentTxHash]; !ok {
			parentTxHashes[*parentTxHash] = make([]int, 0)
		}

		parentTxHashes[*parentTxHash] = append(parentTxHashes[*parentTxHash], idx)
	}

	for parentTxHash, idxs := range parentTxHashes {
		parentTxHash := parentTxHash
		idxs := idxs

		g.Go(func() error {
			txMeta, err := v.utxoStore.Get(gCtx, &parentTxHash, fields.BlockIDs, fields.BlockHeights)
			if err != nil {
				return errors.NewProcessingError("[Validate][%s] error getting parent transaction %s", txID, parentTxHash, err)
			}

			if len(txMeta.BlockHeights) == 0 {
				// the parent has not been mined yet, which means it's recent
				for _, idx := range idxs {
					utxoHeights[idx] = v.utxoStore.GetBlockHeight()
				}
			} else {
				for _, idx := range idxs {
					utxoHeights[idx] = txMeta.BlockHeights[0]
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return utxoHeights, nil
}

func (v *Validator) TriggerBatcher() {
	// Noop
}

func (v *Validator) reverseTxMetaStore(setSpan tracing.Span, txHash *chainhash.Hash) (err error) {
	for retries := 0; retries < 3; retries++ {
		if metaErr := v.utxoStore.Delete(setSpan.Ctx, txHash); metaErr != nil {
			if retries < 2 {
				backoff := time.Duration(2^retries) * time.Second
				v.logger.Errorf("error deleting tx %s from tx meta utxoStore, retrying in %s: %v", txHash.String(), backoff.String(), metaErr)
				time.Sleep(backoff)
			} else {
				err = errors.NewStorageError("error deleting tx %s from tx meta utxoStore", txHash.String(), metaErr)
			}
		} else {
			break
		}
	}

	if v.txmetaKafkaProducerClient != nil { // tests may not set this
		startKafka := time.Now()

		m := &kafkamessage.KafkaTxMetaTopicMessage{
			TxHash: txHash.CloneBytes(),
			Action: kafkamessage.KafkaTxMetaActionType_DELETE,
		}

		value, err := proto.Marshal(m)
		if err != nil {
			return err
		}

		v.txmetaKafkaProducerClient.Publish(&kafka.Message{
			Value: value,
		})

		prometheusValidatorSendToBlockValidationKafka.Observe(float64(time.Since(startKafka).Microseconds()) / 1_000_000)
	}

	return err
}

// CreateInUtxoStore stores transaction metadata in the UTXO store.
// Returns transaction metadata and error if storage fails.
func (v *Validator) CreateInUtxoStore(traceSpan tracing.Span, tx *bt.Tx, blockHeight uint32, markAsConflicting bool,
	markAsUnspendable bool) (*meta.Data, error) {
	ctx, _, deferFn := tracing.StartTracing(traceSpan.Ctx, "storeTxInUtxoMap",
		tracing.WithHistogram(prometheusValidatorSetTxMeta),
	)
	defer deferFn()

	createOptions := []utxo.CreateOption{
		utxo.WithConflicting(markAsConflicting),
	}

	if markAsUnspendable {
		createOptions = append(createOptions, utxo.WithUnspendable(true))
	}

	data, err := v.utxoStore.Create(ctx, tx, blockHeight, createOptions...)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (v *Validator) sendTxMetaToKafka(data *meta.Data, txHash *chainhash.Hash) error {
	startKafka := time.Now()

	metaBytes := data.MetaBytes()

	if len(metaBytes) > 2048 {
		v.logger.Warnf("stored tx meta maybe too big for txmeta cache, size: %d, parent hash count: %d", len(metaBytes), len(data.ParentTxHashes))
	}

	value, err := proto.Marshal(&kafkamessage.KafkaTxMetaTopicMessage{
		TxHash:  txHash[:],
		Action:  kafkamessage.KafkaTxMetaActionType_ADD,
		Content: metaBytes,
	})
	if err != nil {
		return err
	}

	v.txmetaKafkaProducerClient.Publish(&kafka.Message{
		Value: value,
	})

	prometheusValidatorSendToBlockValidationKafka.Observe(float64(time.Since(startKafka).Microseconds()) / 1_000_000)

	return nil
}

// spendUtxos attempts to spend the UTXOs referenced by transaction inputs.
// Returns the spent UTXOs and error if spending fails.
func (v *Validator) spendUtxos(traceSpan tracing.Span, tx *bt.Tx, ignoreUnspendable bool) ([]*utxo.Spend, error) {
	ctx, _, deferFn := tracing.StartTracing(traceSpan.Ctx, "spendUtxos",
		tracing.WithHistogram(prometheusTransactionSpendUtxos),
	)
	defer deferFn()

	utxoSpan := tracing.Start(ctx, "SpendUtxos")
	defer func() {
		utxoSpan.Finish()
	}()

	var (
		err error
	)

	spends, err := v.utxoStore.Spend(ctx, tx, ignoreUnspendable)
	if err != nil {
		traceSpan.RecordError(err)

		return spends, errors.NewProcessingError("validator: UTXO Store spend failed for %s", tx.TxIDChainHash().String(), err)
	}

	return spends, nil
}

// sendToBlockAssembler sends validated transaction data to the block assembler.
// Returns error if block assembly integration fails.
func (v *Validator) sendToBlockAssembler(traceSpan tracing.Span, bData *blockassembly.Data, reservedUtxos []*utxo.Spend) error {
	ctx, _, deferFn := tracing.StartTracing(traceSpan.Ctx, "sendToBlockAssembler",
		tracing.WithHistogram(prometheusValidatorSendToBlockAssembly),
	)
	defer deferFn()

	_ = reservedUtxos

	span := tracing.Start(ctx, "sendToBlockAssembler")
	defer span.Finish()

	if v.settings.Validator.VerboseDebug {
		v.logger.Debugf("[Validator] sending tx %s to block assembler", bData.TxIDChainHash.String())
	}

	if _, err := v.blockAssembler.Store(ctx, bData.TxIDChainHash, bData.Fee, bData.Size); err != nil {
		e := errors.NewServiceError("error calling blockAssembler Store()", err)
		traceSpan.RecordError(e)

		return e
	}

	return nil
}

// reverseSpends reverses previously spent UTXOs in case of validation failure.
// Attempts up to 3 retries with exponential backoff.
// Returns error if UTXO reversal fails.
func (v *Validator) reverseSpends(traceSpan tracing.Span, spentUtxos []*utxo.Spend) error {
	ctx, _, deferFn := tracing.StartTracing(traceSpan.Ctx, "reverseSpends")
	defer deferFn()

	reverseUtxoSpan := tracing.Start(ctx, "reverseSpends")
	defer reverseUtxoSpan.Finish()

	for retries := 0; retries < 3; retries++ {
		if errReset := v.utxoStore.Unspend(ctx, spentUtxos); errReset != nil {
			if retries < 2 {
				backoff := time.Duration(2^retries) * time.Second
				v.logger.Errorf("error resetting utxos, retrying in %s: %v", backoff.String(), errReset)
				time.Sleep(backoff)
			} else {
				reverseUtxoSpan.RecordError(errReset)
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
	ctx, _, deferFn := tracing.StartTracing(ctx, "extendTransaction",
		tracing.WithHistogram(prometheusTransactionValidate),
	)
	defer deferFn()

	if tx.IsCoinbase() {
		return nil
	}

	outpoints := make([]*meta.PreviousOutput, len(tx.Inputs))

	for i, input := range tx.Inputs {
		outpoints[i] = &meta.PreviousOutput{
			PreviousTxID: *input.PreviousTxIDChainHash(),
			Vout:         input.PreviousTxOutIndex,
		}
	}

	if err := v.utxoStore.PreviousOutputsDecorate(ctx, outpoints); err != nil {
		if errors.Is(err, errors.ErrTxNotFound) {
			return errors.NewTxMissingParentError("error extending transaction, parent tx not found")
		}

		return errors.NewProcessingError("can't extend transaction %s", tx.TxIDChainHash().String(), err)
	}

	for i, input := range tx.Inputs {
		input.PreviousTxScript = bscript.NewFromBytes(outpoints[i].LockingScript)
		input.PreviousTxSatoshis = outpoints[i].Satoshis
	}

	return nil
}

// validateTransaction performs transaction-level validation checks.
// Ensures transaction is properly extended and meets all validation rules.
// Returns error if validation fails.
func (v *Validator) validateTransaction(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "validateTransaction",
		tracing.WithHistogram(prometheusTransactionValidate),
	)
	defer deferFn()

	// 0) Check whether we have a complete transaction in extended format, with all input information
	//    we cannot check the satoshi input, OP_RETURN is allowed 0 satoshis
	if !tx.IsExtended() {
		err := v.extendTransaction(ctx, tx)
		if err != nil {
			// error is already wrapped in our errors package
			return err
		}
	}

	// run the internal tx validation, checking policies, scripts, signatures etc.
	return v.txValidator.ValidateTransaction(tx, blockHeight, validationOptions)
}

// validateTransactionScripts performs script validation for a transaction
// Returns error if validation fails
func (v *Validator) validateTransactionScripts(ctx context.Context, tx *bt.Tx, blockHeight uint32, utxoHeights []uint32,
	validationOptions *Options) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "validateTransactionScripts",
		tracing.WithHistogram(prometheusTransactionValidateScripts),
	)
	defer deferFn()

	// 0) Check whether we have a complete transaction in extended format, with all input information
	//    we cannot check the satoshi input, OP_RETURN is allowed 0 satoshis
	if !tx.IsExtended() {
		err := v.extendTransaction(ctx, tx)
		if err != nil {
			// error is already wrapped in our errors package
			return err
		}
	}

	// run the internal tx validation, checking policies, scripts, signatures etc.
	return v.txValidator.ValidateTransactionScripts(tx, blockHeight, utxoHeights, validationOptions)
}

// feesToBtFeeQuote converts a minimum mining fee rate to a bt.FeeQuote structure.
// The fee rate is specified in BSV/KB and converted to satoshis/KB.
func feesToBtFeeQuote(minMiningFee float64) *bt.FeeQuote {
	satoshisPerKB := int(minMiningFee * 1e8)

	btFeeQuote := bt.NewFeeQuote()

	for _, feeType := range []bt.FeeType{bt.FeeTypeStandard, bt.FeeTypeData} {
		btFeeQuote.AddQuote(feeType, &bt.Fee{
			MiningFee: bt.FeeUnit{
				Satoshis: satoshisPerKB,
				Bytes:    1000,
			},
			RelayFee: bt.FeeUnit{
				Satoshis: satoshisPerKB,
				Bytes:    1000,
			},
		})
	}

	return btFeeQuote
}
