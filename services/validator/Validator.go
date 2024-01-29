package validator

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	defaultvalidator "github.com/TAAL-GmbH/arc/validator/default" // TODO move this to UBSV repo - add recover to validation
	"github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/go-bitcoin"
	"github.com/ordishs/go-utils/batcher"
	"github.com/ordishs/gocore"
)

var (
	ErrBadRequest = errors.New("VALIDATOR_BAD_REQUEST")
	ErrInternal   = errors.New("VALIDATOR_INTERNAL")

	blockValidationStat = gocore.NewStat("Validator_sendTxMetaBatchToBlockValidator", true)
)

type blockValidationTxMetaClient interface {
	SetTxMeta(context.Context, []*txmeta.Data) error
	DelTxMeta(context.Context, *chainhash.Hash) error
}

type Validator struct {
	logger                        ulogger.Logger
	utxoStore                     utxostore.Interface
	blockAssembler                blockassembly.Store
	txMetaStore                   txmeta.Store
	blockValidationClient         blockValidationTxMetaClient
	blockValidationBatcher        batcher.Batcher[txmeta.Data]
	kafkaProducer                 sarama.SyncProducer
	kafkaTopic                    string
	kafkaPartitions               int
	saveInParallel                bool
	blockAssemblyDisabled         bool
	blockValidationBatcherEnabled bool
}

func New(ctx context.Context, logger ulogger.Logger, store utxostore.Interface, txMetaStore txmeta.Store, blockValidationClient blockValidationTxMetaClient) (Interface, error) {
	initPrometheusMetrics()

	ba := blockassembly.NewClient(ctx, logger)
	enabled := gocore.Config().GetBool("blockvalidation_txMetaCacheBatcherEnabled", true)

	validator := &Validator{
		logger:                        logger,
		utxoStore:                     store,
		blockAssembler:                ba,
		txMetaStore:                   txMetaStore,
		blockValidationClient:         blockValidationClient,
		saveInParallel:                true,
		blockValidationBatcherEnabled: enabled,
	}

	if blockValidationClient != nil && validator.blockValidationBatcherEnabled {
		sendBatch := func(batch []*txmeta.Data) {
			startTime := gocore.CurrentTime()
			defer func() {
				blockValidationStat.AddTime(startTime)
				prometheusValidatorSetTxMetaCache.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
			}()

			// add data to block validation cache
			if err := validator.blockValidationClient.SetTxMeta(ctx, batch); err != nil {
				validator.logger.Errorf("error sending tx meta batch to block validation cache: %v", err)
			}
		}
		batchSize, _ := gocore.Config().GetInt("blockvalidation_txMetaCacheBatchSize", 100)
		batchTimeOut, _ := gocore.Config().GetInt("blockvalidation_txMetaCacheBatchTimeoutMillis", 10)
		validator.blockValidationBatcher = *batcher.New[txmeta.Data](batchSize, time.Duration(batchTimeOut)*time.Millisecond, sendBatch, false)
	}

	validator.blockAssemblyDisabled = gocore.Config().GetBool("blockassembly_disabled", false)

	kafkaURL, _, found := gocore.Config().GetURL("blockassembly_kafkaBrokers")
	if found {
		_, producer, err := util.ConnectToKafka(kafkaURL)
		if err != nil {
			return nil, fmt.Errorf("unable to connect to kafka: %v", err)
		}

		//defer func() {
		//	_ = clusterAdmin.Close()
		//	_ = producer.Close()
		//}()

		validator.kafkaProducer = producer
		validator.kafkaTopic = kafkaURL.Path[1:]
		validator.kafkaPartitions, err = strconv.Atoi(kafkaURL.Query().Get("partitions"))
		if err != nil {
			return nil, fmt.Errorf("unable to parse partitions: %v", err)
		}

		logger.Infof("[VALIDATOR] connected to kafka at %s", kafkaURL.Host)
	}

	return validator, nil
}

func (v *Validator) Health(cntxt context.Context) (int, string, error) {
	start, stat, _ := util.NewStatFromContext(cntxt, "Health", stats)
	defer stat.AddTime(start)

	return 0, "LocalValidator", nil
}

func (v *Validator) GetBlockHeight() (height uint32, err error) {
	return v.utxoStore.GetBlockHeight()
}

func (v *Validator) Validate(cntxt context.Context, tx *bt.Tx) (err error) {
	start, stat, ctx := util.NewStatFromContext(cntxt, "Validate", stats)
	defer func() {
		stat.AddTime(start)
		prometheusTransactionValidateTotal.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
	}()

	traceSpan := tracing.Start(ctx, "Validator:Validate")
	var spentUtxos []*utxostore.Spend

	defer func(reservedUtxos *[]*utxostore.Spend) {
		traceSpan.Finish()

		if r := recover(); r != nil {
			buf := make([]byte, 1024)
			runtime.Stack(buf, false)

			if len(*reservedUtxos) > 0 {
				// TODO is this correct in the recover? should we be reversing the utxos?
				spanCtx := tracing.Start(ctx, "Validator:Validate:Recover")
				if reverseErr := v.reverseSpends(spanCtx, *reservedUtxos); reverseErr != nil {
					v.logger.Errorf("[Validate][%s] error reversing utxos: %v", tx.TxID(), reverseErr)
				}
			}

			v.logger.Errorf("[Validate][%s] Validate recover [stack=%s]: %v", tx.TxID(), string(buf), r)
		}
	}(&spentUtxos)

	if tx.IsCoinbase() {
		return errors.Join(ErrBadRequest, fmt.Errorf("[Validate][%s] coinbase transactions are not supported", tx.TxIDChainHash().String()))
	}

	if err = v.validateTransaction(traceSpan, tx); err != nil {
		return errors.Join(ErrBadRequest, fmt.Errorf("[Validate][%s] error validating transaction: %v", tx.TxID(), err))
	}

	// this will reverse the spends if there is an error
	// TODO make this stricter, checking whether this utxo was already spent by the same tx and return early if so
	//      do not allow any utxo be spent more than once
	if spentUtxos, err = v.spendUtxos(traceSpan, tx); err != nil {
		return errors.Join(ErrInternal, fmt.Errorf("[Validate][%s] error spending utxos: %v", tx.TxID(), err))
	}

	// decouple the tracing context to not cancel the context when finalize the block assembly
	callerSpan := opentracing.SpanFromContext(traceSpan.Ctx)
	setCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)
	setCtx = util.CopyStatFromContext(traceSpan.Ctx, setCtx)
	setSpan := tracing.Start(setCtx, "Validator:sendToBlockAssembly")
	defer setSpan.Finish()

	// then we store the new utxos from the tx
	err = v.storeUtxos(setSpan.Ctx, tx)
	if err != nil {
		if reverseErr := v.reverseSpends(setSpan, spentUtxos); reverseErr != nil {
			err = errors.Join(ErrInternal, err, fmt.Errorf("error reversing utxo spends: %v", reverseErr))
		}

		if err = v.reverseTxMetaStore(setSpan, tx.TxIDChainHash()); err != nil {
			err = errors.Join(ErrInternal, err, fmt.Errorf("error reversing tx meta utxoStore: %v", err))
		}

		setSpan.RecordError(err)
		return err
	}

	txMetaData, err := v.registerTxInMetaStore(setSpan, tx, spentUtxos)
	if err != nil {
		if errors.Is(err, txmeta.NewErrTxmetaAlreadyExists(tx.TxIDChainHash())) {
			// stop all processing, this transaction has already been validated and passed into the block assembly
			v.logger.Debugf("[Validate][%s] tx already exists in meta utxoStore, not sending to block assembly: %v", tx.TxIDChainHash().String(), err)
			return nil
		}

		if reverseErr := v.reverseSpends(setSpan, spentUtxos); reverseErr != nil {
			err = errors.Join(err, fmt.Errorf("error reversing utxo spends: %v", reverseErr))
		}
		return errors.Join(ErrInternal, fmt.Errorf("error registering tx in meta utxoStore: %v", err))
	}

	if !v.blockAssemblyDisabled {
		// first we send the tx to the block assembler
		if err = v.sendToBlockAssembler(setSpan, &blockassembly.Data{
			TxIDChainHash: tx.TxIDChainHash(),
			Fee:           txMetaData.Fee,
			Size:          uint64(tx.Size()),
			LockTime:      tx.LockTime,
		}, spentUtxos); err != nil {
			err = errors.Join(ErrInternal, fmt.Errorf("error sending tx to block assembler: %v", err))

			if reverseErr := v.reverseStores(setSpan, tx); reverseErr != nil {
				err = errors.Join(err, fmt.Errorf("error reversing utxo stores: %v", reverseErr))
			}

			if reverseErr := v.reverseSpends(setSpan, spentUtxos); reverseErr != nil {
				err = errors.Join(err, fmt.Errorf("error reversing utxo spends: %v", reverseErr))
			}

			if metaErr := v.txMetaStore.Delete(setSpan.Ctx, tx.TxIDChainHash()); metaErr != nil {
				err = errors.Join(err, fmt.Errorf("error deleting tx %s from tx meta utxoStore: %v", tx.TxIDChainHash().String(), metaErr))
			}

			setSpan.RecordError(err)
			return err
		}
	}

	return nil
}

func (v *Validator) reverseTxMetaStore(setSpan tracing.Span, txID *chainhash.Hash) (err error) {
	if metaErr := v.txMetaStore.Delete(setSpan.Ctx, txID); metaErr != nil {
		err = errors.Join(err, fmt.Errorf("error deleting tx %s from tx meta utxoStore: %v", txID.String(), metaErr))
	}

	if v.blockValidationClient != nil {
		if bvErr := v.blockValidationClient.DelTxMeta(setSpan.Ctx, txID); bvErr != nil {
			err = errors.Join(err, fmt.Errorf("error deleting tx %s from block validation cache: %v", txID.String(), bvErr))
		}
	}

	return err
}

func (v *Validator) storeUtxos(ctx context.Context, tx *bt.Tx) error {
	start, stat, ctx := util.StartStatFromContext(ctx, "storeUtxos")
	storeUtxosSpan := tracing.Start(ctx, "Validator:storeUtxos")
	defer func() {
		stat.AddTime(start)
		storeUtxosSpan.Finish()
		prometheusTransactionStoreUtxos.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
	}()

	err := v.utxoStore.Store(storeUtxosSpan.Ctx, tx)
	if err != nil {

		// TODO #144
		// add the tx to the fail queue and process ASAP?

		// TODO remove from tx meta store?
		// TRICKY - we've sent the tx to block assembly - we can't undo that?
		// the reverseSpends need to be given the outputs not the spends
		// v.reverseSpends(traceSpan, spentUtxos)
		return fmt.Errorf("error storing tx %s in utxo utxoStore: %v", tx.TxIDChainHash().String(), err)
	}

	return nil
}

func (v *Validator) registerTxInMetaStore(traceSpan tracing.Span, tx *bt.Tx, spentUtxos []*utxostore.Spend) (*txmeta.Data, error) {
	start, stat, ctx := util.StartStatFromContext(traceSpan.Ctx, "registerTxInMetaStore")
	defer func() {
		stat.AddTime(start)
		prometheusValidatorSetTxMeta.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
	}()

	txMetaSpan := tracing.Start(ctx, "Validator:Validate:StoreTxMeta")
	defer txMetaSpan.Finish()

	data, err := v.txMetaStore.Create(ctx, tx)
	if err != nil {
		if errors.Is(err, txmeta.NewErrTxmetaAlreadyExists(tx.TxIDChainHash())) {
			// this does not need to be a warning, it's just a duplicate validation request
			return nil, txmeta.NewErrTxmetaAlreadyExists(tx.TxIDChainHash())
		}

		if reverseErr := v.reverseSpends(txMetaSpan, spentUtxos); reverseErr != nil {
			err = errors.Join(err, fmt.Errorf("error reversing utxos: %v", reverseErr))
		}
		return data, errors.Join(fmt.Errorf("error sending tx %s to tx meta utxoStore", tx.TxIDChainHash().String()), err)
	}

	if v.blockValidationClient != nil && v.blockValidationBatcherEnabled {
		go func() {
			v.blockValidationBatcher.Put(data)
		}()
	}

	return data, nil
}

func (v *Validator) validateTransaction(traceSpan tracing.Span, tx *bt.Tx) error {
	start, stat, ctx := util.StartStatFromContext(traceSpan.Ctx, "validateTransaction")
	defer func() {
		stat.AddTime(start)
		prometheusTransactionValidate.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
	}()

	basicSpan := tracing.Start(ctx, "Validator:Validate:Basic")
	defer func() {
		basicSpan.Finish()
	}()

	// check all the basic stuff
	// TODO this is using the ARC validator, but should be moved into a separate package or imported to this one
	validator := defaultvalidator.New(&bitcoin.Settings{})
	// this will also check whether the transaction is in extended format

	return validator.ValidateTransaction(tx)
}

func (v *Validator) spendUtxos(traceSpan tracing.Span, tx *bt.Tx) ([]*utxostore.Spend, error) {
	start, stat, ctx := util.StartStatFromContext(traceSpan.Ctx, "spendUtxos")
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
	spends := make([]*utxostore.Spend, len(tx.Inputs))
	for idx, input := range tx.Inputs {
		hash, err = util.UTXOHashFromInput(input)
		if err != nil {
			utxoSpan.RecordError(err)
			return nil, fmt.Errorf("error getting input utxo hash: %s", err.Error())
		}

		// v.logger.Debugf("spending utxo %s:%d -> %s", input.PreviousTxIDChainHash().String(), input.PreviousTxOutIndex, hash.String())
		spends[idx] = &utxostore.Spend{
			TxID:         input.PreviousTxIDChainHash(),
			Vout:         input.PreviousTxOutIndex,
			Hash:         hash,
			SpendingTxID: txIDChainHash,
		}
	}

	err = v.utxoStore.Spend(ctx, spends)
	if err != nil {
		traceSpan.RecordError(err)

		// check whether this is a double spend error
		var spentErr *utxostore.ErrSpent
		ok := errors.As(err, &spentErr)
		if ok {
			// remove the spending tx from the block assembly and freeze it
			// TODO implement freezing in utxo store
			err = v.blockAssembler.RemoveTx(ctx, spentErr.SpendingTxID)
			if err != nil {
				v.logger.Errorf("validator: UTXO Store remove tx failed: %v", err)
			}
		}

		return nil, errors.Join(fmt.Errorf("validator: UTXO Store spend failed for %s", tx.TxIDChainHash().String()), err)
	}

	return spends, nil
}

func (v *Validator) sendToBlockAssembler(traceSpan tracing.Span, bData *blockassembly.Data, reservedUtxos []*utxostore.Spend) error {
	startTime, stat, ctx := util.StartStatFromContext(traceSpan.Ctx, "sendToBlockAssembler")
	defer func() {
		stat.AddTime(startTime)
		prometheusValidatorSendToBlockAssembly.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
	}()

	if v.kafkaProducer != nil {
		if err := v.publishToKafka(traceSpan, bData); err != nil {
			if reverseErr := v.reverseSpends(traceSpan, reservedUtxos); reverseErr != nil {
				err = errors.Join(err, fmt.Errorf("error reversing utxos: %v", reverseErr))
			}
			traceSpan.RecordError(err)
			return fmt.Errorf("error sending tx to kafka: %v", err)
		}
	} else {
		if _, err := v.blockAssembler.Store(ctx, bData.TxIDChainHash, bData.Fee, bData.Size, bData.LockTime, bData.UtxoHashes); err != nil {
			e := fmt.Errorf("error calling blockAssembler Store(): %v", err)
			if reverseErr := v.reverseSpends(traceSpan, reservedUtxos); reverseErr != nil {
				e = errors.Join(e, fmt.Errorf("error reversing utxos: %v", reverseErr))
			}
			traceSpan.RecordError(e)
			return e
		}
	}

	return nil
}

func (v *Validator) reverseSpends(traceSpan tracing.Span, spentUtxos []*utxostore.Spend) error {
	start, stat, ctx := util.StartStatFromContext(traceSpan.Ctx, "reverseSpends")
	defer stat.AddTime(start)

	reverseUtxoSpan := tracing.Start(ctx, "Validator:Validate:ReverseUtxos")
	defer reverseUtxoSpan.Finish()

	// decouple the tracing context to not cancel the context when the tx is being saved in the background
	callerSpan := opentracing.SpanFromContext(reverseUtxoSpan.Ctx)
	setCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)
	_, _, ctx = util.StartStatFromContext(setCtx, "reverseSpends")

	if errReset := v.utxoStore.UnSpend(ctx, spentUtxos); errReset != nil {
		// TODO on error add to a queue to be processed later
		reverseUtxoSpan.RecordError(errReset)
		return fmt.Errorf("error resetting utxos %v", errReset)
	}

	return nil
}

func (v *Validator) reverseStores(traceSpan tracing.Span, tx *bt.Tx) error {
	start, stat, ctx := util.StartStatFromContext(traceSpan.Ctx, "reverseStores")
	defer stat.AddTime(start)

	reverseUtxoSpan := tracing.Start(ctx, "Validator:Validate:reverseStores")
	defer reverseUtxoSpan.Finish()

	// decouple the tracing context to not cancel the context when the tx is being saved in the background
	callerSpan := opentracing.SpanFromContext(reverseUtxoSpan.Ctx)
	setCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)
	_, _, ctx = util.StartStatFromContext(setCtx, "reverseStores")

	if errReverse := v.utxoStore.Delete(ctx, tx); errReverse != nil {
		reverseUtxoSpan.RecordError(errReverse)
		return fmt.Errorf("error reversing utxo stores %v", errReverse)
	}

	return nil
}

func (v *Validator) publishToKafka(traceSpan tracing.Span, bData *blockassembly.Data) error {
	start, stat, ctx := util.StartStatFromContext(traceSpan.Ctx, "publishToKafka")
	defer stat.AddTime(start)

	kafkaSpan := tracing.Start(ctx, "Validator:Validate:publishToKafka")
	defer kafkaSpan.Finish()

	// partition is the first byte of the txid - max 2^8 partitions = 256
	partition := binary.LittleEndian.Uint32(bData.TxIDChainHash[:]) % uint32(v.kafkaPartitions)
	_, _, err := v.kafkaProducer.SendMessage(&sarama.ProducerMessage{
		Topic:     v.kafkaTopic,
		Partition: int32(partition),
		Key:       sarama.ByteEncoder(bData.TxIDChainHash[:]),
		Value:     sarama.ByteEncoder(bData.Bytes()),
	})
	if err != nil {
		return err
	}

	return nil
}
