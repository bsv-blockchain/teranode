package validator

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/bitcoin-sv/ubsv/chaincfg"
	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/bitcoin-sv/ubsv/util/health"
	"github.com/bitcoin-sv/ubsv/util/kafka"
	"github.com/bitcoin-sv/ubsv/util/retry"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
)

const (
	MaxBlockSize                       = 4 * 1024 * 1024 * 1024
	MaxSatoshis                        = 21_000_000_00_000_000
	coinbaseTxID                       = "0000000000000000000000000000000000000000000000000000000000000000"
	MaxTxSigopsCountPolicyAfterGenesis = ^uint32(0) // UINT32_MAX
)

type Validator struct {
	logger                        ulogger.Logger
	txValidator                   TxValidator
	utxoStore                     utxo.Store
	blockAssembler                blockassembly.Store
	saveInParallel                bool
	blockAssemblyDisabled         bool
	stats                         *gocore.Stat
	txMetaKafkaProducerClient     *kafka.KafkaAsyncProducer
	rejectedTxKafkaProducerClient *kafka.KafkaAsyncProducer
	kafkaHealthURL                *url.URL
}

func New(ctx context.Context, logger ulogger.Logger, store utxo.Store) (Interface, error) {
	initPrometheusMetrics()

	ba, err := blockassembly.NewClient(ctx, logger)
	if err != nil {
		return nil, errors.NewServiceError("failed to create block assembly client", err)
	}

	// Get the type of verificator from config
	scriptValidator, ok := gocore.Config().Get("validator_scriptVerificationLibrary", VerificatorGoBT)
	if !ok {
		scriptValidator = VerificatorGoBT
	}

	v := &Validator{
		logger:         logger,
		txValidator:    NewTxValidator(scriptValidator, logger, NewPolicySettings(), chaincfg.GetChainParamsFromConfig()),
		utxoStore:      store,
		blockAssembler: ba,
		saveInParallel: true,
		stats:          gocore.NewStat("validator"),
	}

	v.blockAssemblyDisabled = gocore.Config().GetBool("blockassembly_disabled", false)

	txmetaKafkaURL, _, found := gocore.Config().GetURL("kafka_txmetaConfig")
	if found {
		workers, _ := gocore.Config().GetInt("blockvalidation_kafkaWorkers", 100)
		// only start the kafka producer if there are workers listening
		// this can be used to disable the kafka producer, by just setting workers to 0
		if workers > 0 {
			v.kafkaHealthURL = txmetaKafkaURL
			v.txMetaKafkaProducerClient, err = retry.Retry(ctx, logger, func() (*kafka.KafkaAsyncProducer, error) {
				return kafka.NewKafkaAsyncProducer(v.logger, txmetaKafkaURL, make(chan []byte, 10000))
			}, retry.WithMessage("[Validator] error starting kafka producer for txMeta"))

			if err != nil {
				v.logger.Fatalf("[Validator] failed to start kafka producer for txMeta: %v", err)
				return nil, err
			}

			go v.txMetaKafkaProducerClient.Start(ctx)

			logger.Infof("[Validator] connected to kafka at %s", txmetaKafkaURL.Host)
		}
	}

	rejectedTxKafkaURL, _, found := gocore.Config().GetURL("kafka_rejectedTxConfig")
	if found {
		workers, _ := gocore.Config().GetInt("validator_kafkaWorkers", 100)
		// only start the kafka producer if there are workers listening
		// this can be used to disable the kafka producer, by just setting workers to 0
		if workers > 0 {
			v.kafkaHealthURL = rejectedTxKafkaURL
			v.rejectedTxKafkaProducerClient, err = retry.Retry(ctx, logger, func() (*kafka.KafkaAsyncProducer, error) {
				return kafka.NewKafkaAsyncProducer(v.logger, rejectedTxKafkaURL, make(chan []byte, 10000))
			}, retry.WithMessage("[Validator] error starting kafka producer for rejected Txs"))

			if err != nil {
				v.logger.Fatalf("[Validator] failed to start kafka producer for rejected Txs: %v", err)
				return nil, err
			}

			go v.rejectedTxKafkaProducerClient.Start(ctx)

			logger.Infof("[Validator] connected to kafka at %s", rejectedTxKafkaURL.Host)
		}
	}

	return v, nil
}

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

	checks := []health.Check{
		{Name: "BlockHeight", Check: checkBlockHeight},
		{Name: "UTXOStore", Check: v.utxoStore.Health},
		{Name: "Kafka", Check: kafka.HealthChecker(ctx, v.kafkaHealthURL)},
	}

	return health.CheckAll(ctx, checkLiveness, checks)
}

func (v *Validator) GetBlockHeight() uint32 {
	return v.utxoStore.GetBlockHeight()
}

func (v *Validator) GetMedianBlockTime() uint32 {
	return v.utxoStore.GetMedianBlockTime()
}

// Validate validates a transaction
func (v *Validator) Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...Option) (err error) {
	// apply options
	validationOptions := ProcessOptions(opts...)

	if err = v.validateInternal(ctx, tx, blockHeight, validationOptions); err != nil {
		if v.rejectedTxKafkaProducerClient != nil {
			startKafka := time.Now()
			v.rejectedTxKafkaProducerClient.PublishChannel <- append(tx.TxIDChainHash().CloneBytes(), err.Error()...)

			prometheusValidatorSendToP2PKafka.Observe(float64(time.Since(startKafka).Microseconds()) / 1_000_000)
		}
	}

	return err
}

//gocognit:ignore
func (v *Validator) validateInternal(ctx context.Context, tx *bt.Tx, blockHeight uint32, validationOptions *Options) (err error) {
	txID := tx.TxID()
	ctx, _, deferFn := tracing.StartTracing(ctx, "Validator:Validate",
		tracing.WithParentStat(v.stats),
		tracing.WithHistogram(prometheusTransactionValidateTotal),
		tracing.WithDebugLogMessage(v.logger, "[Validator:Validate] called for %s", txID),
		tracing.WithTag("txid", txID),
	)

	defer func() {
		deferFn(err)
	}()

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
			return errors.NewNonFinalError("[Validate][%s] transaction is not final", txID, err)
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
		if errors.Is(err, errors.ErrTxNotFound) {
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
		txMetaData, err = v.storeTxInUtxoMap(setSpan, tx, blockHeight)
		if err != nil {
			if errors.Is(err, errors.ErrTxAlreadyExists) {
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

	if !v.blockAssemblyDisabled {
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

	if v.txMetaKafkaProducerClient != nil {
		startKafka := time.Now()
		v.txMetaKafkaProducerClient.PublishChannel <- append(txHash.CloneBytes(), []byte("delete")...)

		prometheusValidatorSendToBlockValidationKafka.Observe(float64(time.Since(startKafka).Microseconds()) / 1_000_000)
	}

	return err
}

func (v *Validator) storeTxInUtxoMap(traceSpan tracing.Span, tx *bt.Tx, blockHeight uint32) (*meta.Data, error) {
	ctx, _, deferFn := tracing.StartTracing(traceSpan.Ctx, "storeTxInUtxoMap",
		tracing.WithHistogram(prometheusValidatorSetTxMeta),
	)
	defer deferFn()

	data, err := v.utxoStore.Create(ctx, tx, blockHeight)
	if err != nil {
		return nil, err
	}

	if v.txMetaKafkaProducerClient != nil {
		startKafka := time.Now()
		v.txMetaKafkaProducerClient.PublishChannel <- append(tx.TxIDChainHash().CloneBytes(), data.MetaBytes()...)

		prometheusValidatorSendToBlockValidationKafka.Observe(float64(time.Since(startKafka).Microseconds()) / 1_000_000)
	}

	return data, nil
}

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

	spends := make([]*utxo.Spend, len(tx.Inputs))

	for idx, input := range tx.Inputs {
		if input.PreviousTxSatoshis == 0 {
			continue // There are some old transactions (e.g. d5a13dcb1ad24dbffab91c3c2ffe7aea38d5e84b444c0014eb6c7c31fe8e23fc) that have 0 satoshis
		}

		hash, err = util.UTXOHashFromInput(input)
		if err != nil {
			utxoSpan.RecordError(err)
			return nil, errors.NewProcessingError("error getting input utxo hash", err)
		}

		// v.logger.Debugf("spending utxo %s:%d -> %s", input.PreviousTxIDChainHash().String(), input.PreviousTxOutIndex, hash.String())
		spends[idx] = &utxo.Spend{
			TxID:         input.PreviousTxIDChainHash(),
			Vout:         input.PreviousTxOutIndex,
			UTXOHash:     hash,
			SpendingTxID: txIDChainHash,
		}
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

func (v *Validator) sendToBlockAssembler(traceSpan tracing.Span, bData *blockassembly.Data, reservedUtxos []*utxo.Spend) error {
	ctx, _, deferFn := tracing.StartTracing(traceSpan.Ctx, "sendToBlockAssembler",
		tracing.WithHistogram(prometheusValidatorSendToBlockAssembly),
	)
	defer deferFn()

	span := tracing.Start(ctx, "sendToBlockAssembler")
	defer span.Finish()

	v.logger.Debugf("[Validator] sending tx %s to block assembler", bData.TxIDChainHash.String())

	if _, err := v.blockAssembler.Store(ctx, bData.TxIDChainHash, bData.Fee, bData.Size); err != nil {
		e := errors.NewStorageError("error calling blockAssembler Store()", err)
		traceSpan.RecordError(e)

		return e
	}

	return nil
}

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

	return v.txValidator.VerifyScript(tx, blockHeight)
}

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
