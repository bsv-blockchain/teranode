package validator

import (
	"context"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/util/retry"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
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
	logger                 ulogger.Logger
	txValidator            TxValidator
	utxoStore              utxo.Store
	blockAssembler         blockassembly.Store
	saveInParallel         bool
	blockAssemblyDisabled  bool
	blockassemblyKafkaChan chan []byte
	txMetaKafkaChan        chan []byte
	rejectedTxKafkaChan    chan []byte
	stats                  *gocore.Stat
}

func New(ctx context.Context, logger ulogger.Logger, store utxo.Store) (Interface, error) {
	initPrometheusMetrics()

	ba := blockassembly.NewClient(ctx, logger)

	v := &Validator{
		logger: logger,
		txValidator: TxValidator{
			policy: NewPolicySettings(),
		},
		utxoStore:      store,
		blockAssembler: ba,
		saveInParallel: true,
		stats:          gocore.NewStat("validator"),
	}

	v.blockAssemblyDisabled = gocore.Config().GetBool("blockassembly_disabled", false)

	txsKafkaURL, _, found := gocore.Config().GetURL("kafka_txsConfig")
	if found {
		workers, _ := gocore.Config().GetInt("blockassembly_kafkaWorkers", 100)
		// only start the kafka producer if there are workers listening
		// this can be used to disable the kafka producer, by just setting workers to 0
		if workers > 0 {
			v.blockassemblyKafkaChan = make(chan []byte, 10000)
			go func() {
				_, err := retry.Retry(ctx, logger, func() (interface{}, error) {
					return nil, util.StartAsyncProducer(v.logger, txsKafkaURL, v.blockassemblyKafkaChan)
				}, retry.WithMessage("[Validator] error starting kafka producer for BlockAssembly Txs"))
				if err != nil {
					v.logger.Fatalf("[Validator] failed to start kafka producer for BlockAssembly Txs: %v", err)
					return
				}
			}()

			logger.Infof("[Validator] connected to kafka at %s", txsKafkaURL.Host)
		}
	}

	txmetaKafkaURL, _, found := gocore.Config().GetURL("kafka_txmetaConfig")
	if found {
		workers, _ := gocore.Config().GetInt("blockvalidation_kafkaWorkers", 100)
		// only start the kafka producer if there are workers listening
		// this can be used to disable the kafka producer, by just setting workers to 0
		if workers > 0 {
			v.txMetaKafkaChan = make(chan []byte, 10000)
			go func() {
				_, err := retry.Retry(ctx, logger, func() (interface{}, error) {
					return nil, util.StartAsyncProducer(v.logger, txmetaKafkaURL, v.txMetaKafkaChan)
				}, retry.WithMessage("[Validator] error starting kafka producer for txMeta"))
				if err != nil {
					v.logger.Fatalf("[Validator] failed to start kafka producer for txMeta: %v", err)
					return
				}
			}()

			logger.Infof("[Validator] connected to kafka at %s", txmetaKafkaURL.Host)
		}
	}

	rejectedTxKafkaURL, _, found := gocore.Config().GetURL("kafka_rejectedTxConfig")
	if found {
		workers, _ := gocore.Config().GetInt("validator_kafkaWorkers", 100)
		// only start the kafka producer if there are workers listening
		// this can be used to disable the kafka producer, by just setting workers to 0
		if workers > 0 {
			v.rejectedTxKafkaChan = make(chan []byte, 10000)
			go func() {
				_, err := retry.Retry(ctx, logger, func() (interface{}, error) {
					return nil, util.StartAsyncProducer(v.logger, rejectedTxKafkaURL, v.rejectedTxKafkaChan)
				}, retry.WithMessage("[Validator] error starting kafka producer for rejected Txs"))
				if err != nil {
					v.logger.Fatalf("[Validator] failed to start kafka producer for rejected Txs: %v", err)
					return
				}
			}()

			logger.Infof("[Validator] connected to kafka at %s", rejectedTxKafkaURL.Host)
		}
	}

	return v, nil
}

func (v *Validator) Health(cntxt context.Context) (int, string, error) {
	start, stat, _ := tracing.NewStatFromContext(cntxt, "Health", v.stats)
	defer stat.AddTime(start)

	return 0, "LocalValidator", nil
}

func (v *Validator) GetBlockHeight() uint32 {
	return v.utxoStore.GetBlockHeight()
}

func (v *Validator) GetMedianBlockTime() uint32 {
	return v.utxoStore.GetMedianBlockTime()
}

// Validate validates a transaction
func (v *Validator) Validate(ctx context.Context, tx *bt.Tx, blockHeight uint32) (err error) {
	if err = v.validateInternal(ctx, tx, blockHeight); err != nil {
		if v.rejectedTxKafkaChan != nil {
			startKafka := time.Now()
			v.rejectedTxKafkaChan <- append(tx.TxIDChainHash().CloneBytes(), err.Error()...)
			prometheusValidatorSendToP2PKafka.Observe(float64(time.Since(startKafka).Microseconds()) / 1_000_000)
		}

	}

	return err
}

func (v *Validator) validateInternal(ctx context.Context, tx *bt.Tx, blockHeight uint32) (err error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "Validator:Validate",
		tracing.WithParentStat(v.stats),
		tracing.WithHistogram(prometheusTransactionValidateTotal),
		tracing.WithDebugLogMessage(v.logger, "[Validator:Validate] called for %s", tx.TxID()),
	)
	defer deferFn()

	var spentUtxos []*utxo.Spend

	// this should be updated automatically by the utxo store
	utxoStoreBlockHeight := v.GetBlockHeight()
	utxoStoreMedianBlockTime := v.GetMedianBlockTime()
	if utxoStoreBlockHeight == 0 || utxoStoreMedianBlockTime == 0 {
		return errors.NewProcessingError("utxo store not ready, block height: %d, median block time: %d", utxoStoreBlockHeight, utxoStoreMedianBlockTime)
	}

	// this function should be moved into go-bt
	if !util.IsTransactionFinal(tx, utxoStoreBlockHeight+1, utxoStoreMedianBlockTime) {
		return errors.NewNonFinalError("[Validate][%s] transaction is not final", tx.TxIDChainHash().String())
	}

	if tx.IsCoinbase() {
		return errors.NewProcessingError("[Validate][%s] coinbase transactions are not supported", tx.TxIDChainHash().String())
	}

	if err = v.validateTransaction(ctx, tx, blockHeight); err != nil {
		return errors.NewProcessingError("[Validate][%s] error validating transaction", tx.TxID(), err)
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
		return errors.NewProcessingError("[Validate][%s] error spending utxos", tx.TxID(), err)
	}

	// TODO do we need to make this a 2 phase commit?
	//      if the block assembly addition fails, the utxo should not be spendable yet
	txMetaData, err := v.storeTxInUtxoMap(setSpan, tx, blockHeight)
	if err != nil {
		if errors.Is(err, errors.ErrTxAlreadyExists) {
			// stop all processing, this transaction has already been validated and passed into the block assembly
			// buf := make([]byte, 1024)
			// runtime.Stack(buf, false)

			v.logger.Debugf("[Validate][%s] tx already exists in store, not sending to block assembly: %v", tx.TxIDChainHash().String(), err)
			// v.logger.Debugf("[Validate][%s] stack: %s", tx.TxIDChainHash().String(), string(buf))
			return nil
		}

		v.logger.Errorf("[Validate][%s] error registering tx in metaStore: %v", tx.TxIDChainHash().String(), err)
		if reverseErr := v.reverseSpends(setSpan, spentUtxos); reverseErr != nil {
			err = errors.NewProcessingError("error reversing utxo spends: %v", reverseErr, err)
		}
		return errors.NewProcessingError("error registering tx in metaStore", err)
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

func (v *Validator) reverseTxMetaStore(setSpan tracing.Span, txID *chainhash.Hash) (err error) {
	for retries := 0; retries < 3; retries++ {
		if metaErr := v.utxoStore.Delete(setSpan.Ctx, txID); metaErr != nil {
			if retries < 2 {
				backoff := time.Duration(2^retries) * time.Second
				v.logger.Errorf("error deleting tx %s from tx meta utxoStore, retrying in %s: %v", txID.String(), backoff.String(), metaErr)
				time.Sleep(backoff)
			} else {
				err = errors.NewStorageError("error deleting tx %s from tx meta utxoStore", txID.String(), metaErr)
			}
		} else {
			break
		}
	}

	if v.txMetaKafkaChan != nil {
		startKafka := time.Now()
		v.txMetaKafkaChan <- append(txID.CloneBytes(), []byte("delete")...)
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

	if v.txMetaKafkaChan != nil {
		startKafka := time.Now()
		v.txMetaKafkaChan <- append(tx.TxIDChainHash().CloneBytes(), data.MetaBytes()...)
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

	var err error
	var hash *chainhash.Hash

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

	if v.blockassemblyKafkaChan != nil {
		start := time.Now()
		v.blockassemblyKafkaChan <- bData.Bytes()
		prometheusValidatorSendToBlockAssemblyKafka.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
	} else {
		if _, err := v.blockAssembler.Store(ctx, bData.TxIDChainHash, bData.Fee, bData.Size); err != nil {
			e := errors.NewStorageError("error calling blockAssembler Store()", err)
			traceSpan.RecordError(e)
			return e
		}
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
		return errors.NewStorageError("can't extend transaction %s", tx.TxIDChainHash().String(), err)
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

	basicSpan := tracing.Start(ctx, "BasicValidation")
	defer func() {
		basicSpan.Finish()
	}()

	// 0) Check whether we have a complete transaction in extended format, with all input information
	//    we cannot check the satoshi input, OP_RETURN is allowed 0 satoshis
	if !util.IsExtended(tx, blockHeight) {
		err := v.extendTransaction(ctx, tx)
		if err != nil {
			return errors.NewProcessingError("can't extend transaction %s", tx.TxIDChainHash(), err)
		}
	}

	return v.txValidator.ValidateTransaction(tx, blockHeight)
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
