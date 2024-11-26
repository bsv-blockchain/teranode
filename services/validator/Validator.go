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

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/settings"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/health"
	"github.com/bitcoin-sv/ubsv/util/kafka"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
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

	// txValidator performs transaction-specific validation checks
	txValidator TxValidatorI

	// utxoStore manages the UTXO set and transaction metadata
	utxoStore utxo.Store

	// blockAssembler handles block template creation and transaction ordering
	blockAssembler blockassembly.Store

	// saveInParallel indicates if UTXOs should be saved concurrently
	saveInParallel bool

	// blockAssemblyDisabled toggles whether validated transactions are sent to block assembly
	blockAssemblyDisabled bool

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
func New(ctx context.Context, logger ulogger.Logger, store utxo.Store, txMetaKafkaProducerClient kafka.KafkaAsyncProducerI, rejectedTxKafkaProducerClient kafka.KafkaAsyncProducerI) (Interface, error) {
	initPrometheusMetrics()

	ba, err := blockassembly.NewClient(ctx, logger)
	if err != nil {
		return nil, errors.NewServiceError("failed to create block assembly client", err)
	}

	v := &Validator{
		logger:                        logger,
		txValidator:                   NewTxValidator(logger, settings.NewPolicySettings(), chaincfg.GetChainParamsFromConfig()),
		utxoStore:                     store,
		blockAssembler:                ba,
		saveInParallel:                true,
		stats:                         gocore.NewStat("validator"),
		txmetaKafkaProducerClient:     txMetaKafkaProducerClient,
		rejectedTxKafkaProducerClient: rejectedTxKafkaProducerClient,
	}

	v.blockAssemblyDisabled = gocore.Config().GetBool("blockassembly_disabled", false)

	txmetaKafkaURL, err, ok := gocore.Config().GetURL("kafka_txmetaConfig")
	if err != nil {
		return nil, errors.NewConfigurationError("failed to get Kafka URL for txmeta: %v", err)
	}

	if !ok || txmetaKafkaURL == nil {
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

	checks := []health.Check{
		{Name: "BlockHeight", Check: checkBlockHeight},
		{Name: "UTXOStore", Check: v.utxoStore.Health},
		{Name: "Kafka", Check: kafka.HealthChecker(ctx, brokersURL)},
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
func (v *Validator) Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...Option) (err error) {
	// apply options
	validationOptions := ProcessOptions(opts...)

	if err = v.validateInternal(ctx, tx, blockHeight, validationOptions); err != nil {
		if v.rejectedTxKafkaProducerClient != nil { // tests may not set this
			startKafka := time.Now()

			v.rejectedTxKafkaProducerClient.Publish(&kafka.Message{
				Value: append(tx.TxIDChainHash().CloneBytes(), err.Error()...),
			})

			prometheusValidatorSendToP2PKafka.Observe(float64(time.Since(startKafka).Microseconds()) / 1_000_000)
		}
	}

	return err
}

// validateInternal performs the core validation logic for a transaction.
// It handles UTXO spending, transaction metadata storage, and block assembly integration.
// Returns error if any validation step fails.
//
//gocognit:ignore
func (v *Validator) validateInternal(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (err error) {
	txID := tx.TxID()
	ctx, _, deferFn := tracing.StartTracing(ctx, "Validator:Validate",
		tracing.WithParentStat(v.stats),
		tracing.WithHistogram(prometheusTransactionValidateTotal),
		tracing.WithTag("txid", txID),
	)

	defer func() {
		deferFn(err)
	}()

	if gocore.Config().GetBool("validator_verbose_debug", false) {
		v.logger.Debugf("[Validator:Validate] called for %s", txID)

		defer func() {
			v.logger.Debugf("[Validator:Validate] called for %s DONE", txID)
		}()
	}

	var spentUtxos []*utxo.Spend

	// this should be updated automatically by the utxo store
	utxoStoreBlockHeight := v.GetBlockHeight()
	// We do not check IsFinal for transactions before BIP113 change (block height 419328)
	// This is an exception for transactions before the media block time was used
	if utxoStoreBlockHeight > util.LockTimeBIP113 {
		utxoStoreMedianBlockTime := v.GetMedianBlockTime()
		if utxoStoreMedianBlockTime == 0 {
			return errors.NewProcessingError("utxo store not ready, block height: %d, median block time: %d", utxoStoreBlockHeight, utxoStoreMedianBlockTime)
		}

		// this function should be moved into go-bt
		if err = util.IsTransactionFinal(tx, utxoStoreBlockHeight+1, utxoStoreMedianBlockTime); err != nil {
			return errors.NewUtxoNonFinalError("[Validate][%s] transaction is not final", txID, err)
		}
	}

	if tx.IsCoinbase() {
		return errors.NewProcessingError("[Validate][%s] coinbase transactions are not supported", txID)
	}

	if err = v.validateTransaction(ctx, tx, blockHeight); err != nil {
		return errors.NewProcessingError("[Validate][%s] error validating transaction", txID, err)
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

	// this will reverse the spends if there is an error
	// TODO make this stricter, checking whether this utxo was already spent by the same tx and return early if so
	//      do not allow any utxo be spent more than once
	if spentUtxos, err = v.spendUtxos(setSpan, tx, blockHeight); err != nil {
		if errors.Is(err, errors.ErrSpent) {
			// get the data from the spend
			var errData *errors.UtxoSpentErrData
			if errors.AsData(err, &errData) {
				s := errData.SpendingTxHash
				// freeze both transactions and remove from block assembly
				_ = s
			}
		} else if errors.Is(err, errors.ErrTxNotFound) {
			// the parent transaction was not found, this can happen when the parent tx has been ttl'd and removed from
			// the utxo store. We can check whether the tx already exists, which means it has been validated and
			// blessed. In this case we can just return early.
			if _, err = v.utxoStore.Get(setSpan.Ctx, tx.TxIDChainHash()); err == nil {
				v.logger.Warnf("[Validate][%s] parent tx not found, but tx already exists in store, assuming already blessed", txID)
				return nil
			}
		}

		return errors.NewProcessingError("[Validate][%s] error spending utxos", txID, err)
	}

	var txMetaData *meta.Data
	if !validationOptions.skipUtxoCreation {
		// TODO do we need to make this a 2 phase commit?
		//      if the block assembly addition fails, the utxo should not be spendable yet
		txMetaData, err = v.StoreTxInUtxoMap(setSpan, tx, blockHeight)
		if err != nil {
			if errors.Is(err, errors.ErrTxExists) {
				// stop all processing, this transaction has already been validated and passed into the block assembly
				// buf := make([]byte, 1024)
				// runtime.Stack(buf, false)
				v.logger.Debugf("[Validate][%s] tx already exists in store, not sending to block assembly: %v", txID, err)
				// v.logger.Debugf("[Validate][%s] stack: %s", tx.TxIDChainHash().String(), string(buf))

				return nil
			}

			v.logger.Errorf("[Validate][%s] error registering tx in metaStore: %v", txID, err)

			if reverseErr := v.reverseSpends(setSpan, spentUtxos); reverseErr != nil {
				err = errors.NewProcessingError("error reversing utxo spends: %v", reverseErr, err)
			}

			return errors.NewProcessingError("error registering tx in metaStore", err)
		}
	} else {
		// create the tx meta needed for the block assembly
		txMetaData, err = util.TxMetaDataFromTx(tx)
		if err != nil {
			return errors.NewProcessingError("failed to get tx meta data", err)
		}
	}

	// the option blockAssemblyDisabled is false by default
	blockAssemblyEnabled := !v.blockAssemblyDisabled

	if blockAssemblyEnabled && validationOptions.addTXToBlockAssembly {
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
			err = errors.NewProcessingError("error sending tx to block assembler", err)

			if reverseErr := v.reverseTxMetaStore(setSpan, tx.TxIDChainHash()); err != nil {
				// add reverseErr to the message, wrap the err
				err = errors.NewProcessingError("error reversing tx meta utxoStore: %v", reverseErr, err)
			}

			if reverseErr := v.reverseSpends(setSpan, spentUtxos); reverseErr != nil {
				// add reverseErr to the message, wrap the err
				err = errors.NewProcessingError("error reversing utxo spends: %v", reverseErr, err)
			}

			setSpan.RecordError(err)

			return err
		}
	}

	return nil
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

		v.txmetaKafkaProducerClient.Publish(&kafka.Message{
			Value: append(txHash.CloneBytes(), []byte("delete")...),
		})

		prometheusValidatorSendToBlockValidationKafka.Observe(float64(time.Since(startKafka).Microseconds()) / 1_000_000)
	}

	return err
}

// storeTxInUtxoMap stores transaction metadata in the UTXO store.
// Returns transaction metadata and error if storage fails.
func (v *Validator) StoreTxInUtxoMap(traceSpan tracing.Span, tx *bt.Tx, blockHeight uint32) (*meta.Data, error) {
	ctx, _, deferFn := tracing.StartTracing(traceSpan.Ctx, "storeTxInUtxoMap",
		tracing.WithHistogram(prometheusValidatorSetTxMeta),
	)
	defer deferFn()

	data, err := v.utxoStore.Create(ctx, tx, blockHeight)
	if err != nil {
		return nil, err
	}

	if v.txmetaKafkaProducerClient != nil { // tests may not set this
		startKafka := time.Now()

		v.txmetaKafkaProducerClient.Publish(&kafka.Message{
			Value: append(tx.TxIDChainHash().CloneBytes(), data.MetaBytes()...),
		})

		prometheusValidatorSendToBlockValidationKafka.Observe(float64(time.Since(startKafka).Microseconds()) / 1_000_000)
	}

	return data, nil
}

// spendUtxos attempts to spend the UTXOs referenced by transaction inputs.
// Returns the spent UTXOs and error if spending fails.
func (v *Validator) spendUtxos(traceSpan tracing.Span, tx *bt.Tx, blockHeight uint32) ([]*utxo.Spend, error) {
	ctx, _, deferFn := tracing.StartTracing(traceSpan.Ctx, "spendUtxos",
		tracing.WithHistogram(prometheusTransactionSpendUtxos),
	)
	defer deferFn()

	utxoSpan := tracing.Start(ctx, "SpendUtxos")
	defer func() {
		utxoSpan.Finish()
	}()

	var (
		err  error
		hash *chainhash.Hash
	)

	// check the utxos
	txIDChainHash := tx.TxIDChainHash()

	spends := make([]*utxo.Spend, 0, len(tx.Inputs))

	for _, input := range tx.Inputs {
		if input.PreviousTxSatoshis == 0 {
			continue // There are some old transactions (e.g. d5a13dcb1ad24dbffab91c3c2ffe7aea38d5e84b444c0014eb6c7c31fe8e23fc) that have 0 satoshis
		}

		hash, err = util.UTXOHashFromInput(input)
		if err != nil {
			utxoSpan.RecordError(err)
			return nil, errors.NewProcessingError("error getting input utxo hash", err)
		}

		// v.logger.Debugf("spending utxo %s:%d -> %s", input.PreviousTxIDChainHash().String(), input.PreviousTxOutIndex, hash.String())
		spends = append(spends, &utxo.Spend{
			TxID:         input.PreviousTxIDChainHash(),
			Vout:         input.PreviousTxOutIndex,
			UTXOHash:     hash,
			SpendingTxID: txIDChainHash,
		})
	}

	err = v.utxoStore.Spend(ctx, spends, blockHeight)
	if err != nil {
		traceSpan.RecordError(err)

		// check whether this is a double spend error

		if errors.Is(err, errors.ErrSpent) {
			// remove the spending tx from the block assembly and freeze it
			// TODO implement freezing in utxo store
			if err := v.blockAssembler.RemoveTx(ctx, txIDChainHash); err != nil {
				v.logger.Errorf("validator: UTXO Store remove tx failed: %v", err)
			}
		}

		return nil, errors.NewProcessingError("validator: UTXO Store spend failed for %s", tx.TxIDChainHash().String(), err)
	}

	return spends, nil
}

// sendToBlockAssembler sends validated transaction data to the block assembler.
// Returns error if block assembly integration fails.
func (v *Validator) sendToBlockAssembler(traceSpan tracing.Span, bData *blockassembly.Data, reservedUtxos []*utxo.Spend) error { //nolint:gosec
	ctx, _, deferFn := tracing.StartTracing(traceSpan.Ctx, "sendToBlockAssembler",
		tracing.WithHistogram(prometheusValidatorSendToBlockAssembly),
	)
	defer deferFn()

	span := tracing.Start(ctx, "sendToBlockAssembler")
	defer span.Finish()

	if gocore.Config().GetBool("validator_verbose_debug", false) {
		v.logger.Debugf("[Validator] sending tx %s to block assembler", bData.TxIDChainHash.String())
	}

	if _, err := v.blockAssembler.Store(ctx, bData.TxIDChainHash, bData.Fee, bData.Size); err != nil {
		e := errors.NewStorageError("error calling blockAssembler Store()", err)
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
		if errReset := v.utxoStore.UnSpend(ctx, spentUtxos); errReset != nil {
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
func (v *Validator) validateTransaction(ctx context.Context, tx *bt.Tx, blockHeight uint32) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "validateTransaction",
		tracing.WithHistogram(prometheusTransactionValidate),
	)
	defer deferFn()

	// 0) Check whether we have a complete transaction in extended format, with all input information
	//    we cannot check the satoshi input, OP_RETURN is allowed 0 satoshis
	if !util.IsExtended(tx, blockHeight) {
		err := v.extendTransaction(ctx, tx)
		if err != nil {
			// error is already wrapped in our errors package
			return err
		}
	}

	// run the internal tx validation, checking policies, scripts, signatures etc.
	return v.txValidator.ValidateTransaction(tx, blockHeight)
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
