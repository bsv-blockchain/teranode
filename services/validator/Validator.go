package validator

import (
	"context"
	"time"

	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/opentracing/opentracing-go"
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
				// TODO add retry
				if err := util.StartAsyncProducer(v.logger, txsKafkaURL, v.blockassemblyKafkaChan); err != nil {
					v.logger.Errorf("[Validator] error starting kafka producer: %v", err)
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
				// TODO add retry
				if err := util.StartAsyncProducer(v.logger, txmetaKafkaURL, v.txMetaKafkaChan); err != nil {
					v.logger.Errorf("[Validator] error starting kafka producer: %v", err)
					return
				}
			}()

			logger.Infof("[Validator] connected to kafka at %s", txmetaKafkaURL.Host)
		}
	}

	return v, nil
}

func (v *Validator) Health(cntxt context.Context) (int, string, error) {
	start, stat, _ := tracing.NewStatFromContext(cntxt, "Health", v.stats)
	defer stat.AddTime(start)

	return 0, "LocalValidator", nil
}

func (v *Validator) GetBlockHeight() (height uint32, err error) {
	return v.utxoStore.GetBlockHeight()
}

// TODO try to break this
func (v *Validator) Validate(cntxt context.Context, tx *bt.Tx, blockHeight uint32) (err error) {
	start, stat, ctx := tracing.NewStatFromContext(cntxt, "Validate", v.stats)
	defer func() {
		stat.AddTime(start)
		prometheusTransactionValidateTotal.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
	}()

	traceSpan := tracing.Start(ctx, "Validator:Validate")
	var spentUtxos []*utxo.Spend

	defer func(reservedUtxos *[]*utxo.Spend) {
		traceSpan.Finish()

		//if r := recover(); r != nil {
		//	buf := make([]byte, 1024)
		//	runtime.Stack(buf, false)
		//
		//	//if reservedUtxos != nil && len(*reservedUtxos) > 0 {
		//	//	// TODO is this correct in the recover? should we be reversing the utxos?
		//	//	spanCtx := tracing.Start(ctx, "Validator:Validate:Recover")
		//	//	if reverseErr := v.reverseSpends(spanCtx, *reservedUtxos); reverseErr != nil {
		//	//		v.logger.Errorf("[Validate][%s] error reversing utxos: %v", tx.TxID(), reverseErr)
		//	//	}
		//	//}
		//
		//	v.logger.Errorf("[Validate][%s] Validate recover [stack=%s]: %v", tx.TxID(), string(buf), r)
		//}
	}(&spentUtxos)

	if tx.IsCoinbase() {
		return errors.NewProcessingError("[Validate][%s] coinbase transactions are not supported", tx.TxIDChainHash().String())
	}

	if err = v.validateTransaction(traceSpan, tx, blockHeight); err != nil {
		return errors.NewProcessingError("[Validate][%s] error validating transaction", tx.TxID(), err)
	}

	// decouple the tracing context to not cancel the context when finalize the block assembly
	callerSpan := opentracing.SpanFromContext(traceSpan.Ctx)
	setCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)
	setCtx = tracing.CopyStatFromContext(traceSpan.Ctx, setCtx)
	setSpan := tracing.Start(setCtx, "Validator:sendToBlockAssembly")
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
	start, stat, ctx := tracing.StartStatFromContext(traceSpan.Ctx, "registerTxInMetaStore")
	defer func() {
		stat.AddTime(start)
		prometheusValidatorSetTxMeta.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
	}()

	txMetaSpan := tracing.Start(ctx, "Validator:Validate:StoreTxMeta")
	defer txMetaSpan.Finish()

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
	start, stat, ctx := tracing.StartStatFromContext(traceSpan.Ctx, "spendUtxos")
	defer func() {
		stat.AddTime(start)
		prometheusTransactionSpendUtxos.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
	}()

	utxoSpan := tracing.Start(ctx, "Validator:Validate:SpendUtxos")
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
	startTime, stat, ctx := tracing.StartStatFromContext(traceSpan.Ctx, "sendToBlockAssembler")
	defer func() {
		stat.AddTime(startTime)
		prometheusValidatorSendToBlockAssembly.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
	}()

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
	start, stat, ctx := tracing.StartStatFromContext(traceSpan.Ctx, "reverseSpends")
	defer stat.AddTime(start)

	reverseUtxoSpan := tracing.Start(ctx, "Validator:Validate:reverseSpends")
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

func (v *Validator) validateTransaction(traceSpan tracing.Span, tx *bt.Tx, blockHeight uint32) error {
	start, stat, ctx := tracing.StartStatFromContext(traceSpan.Ctx, "validateTransaction")
	defer func() {
		stat.AddTime(start)
		prometheusTransactionValidate.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
	}()

	basicSpan := tracing.Start(ctx, "Validator:Validate:Basic")
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
