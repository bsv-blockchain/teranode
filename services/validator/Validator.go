package validator

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"

	"github.com/Shopify/sarama"
	defaultvalidator "github.com/TAAL-GmbH/arc/validator/default" // TODO move this to UBSV repo - add recover to validation
	"github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/go-bitcoin"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Validator struct {
	logger                utils.Logger
	utxoStore             utxostore.Interface
	blockAssembler        blockassembly.Store
	txMetaStore           txmeta.Store
	kafkaProducer         sarama.SyncProducer
	kafkaTopic            string
	kafkaPartitions       int
	saveInParallel        bool
	blockAssemblyDisabled bool
}

func New(ctx context.Context, logger utils.Logger, store utxostore.Interface, txMetaStore txmeta.Store) (Interface, error) {
	ba := blockassembly.NewClient(ctx, logger)

	validator := &Validator{
		logger:         logger,
		utxoStore:      store,
		blockAssembler: ba,
		txMetaStore:    txMetaStore,
		saveInParallel: true,
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
	defer func() {
		stat.AddTime(start)
	}()

	return 0, "LocalValidator", nil
}

func (v *Validator) Validate(cntxt context.Context, tx *bt.Tx) (err error) {
	start, stat, ctx := util.NewStatFromContext(cntxt, "Validate", stats)
	defer func() {
		stat.AddTime(start)
	}()

	traceSpan := tracing.Start(ctx, "Validator:Validate")
	var spentUtxos []*utxostore.Spend
	defer func(reservedUtxos *[]*utxostore.Spend) {
		traceSpan.Finish()
		if r := recover(); r != nil {
			if len(*reservedUtxos) > 0 {
				// TODO is this correct in the recover? should we be reversing the utxos?
				spanCtx := tracing.Start(ctx, "Validator:Validate:Recover")
				v.reverseSpends(spanCtx, *reservedUtxos)
			}

			v.logger.Errorf("[Validate][%s] Validate recover: %v", tx.TxID(), r)
		}
	}(&spentUtxos)

	if tx.IsCoinbase() {
		return fmt.Errorf("[Validate][%s] coinbase transactions are not supported", tx.TxIDChainHash().String())
	}

	if err = v.validateTransaction(traceSpan, tx); err != nil {
		return fmt.Errorf("[Validate][%s] error validating transaction: %v", tx.TxID(), err)
	}

	// this will reverse the spends if there is an error
	// TODO make this stricter, checking whether this utxo was already spent by the same tx and return early if so
	//      do not allow any utxo be spent more than once
	if spentUtxos, err = v.spendUtxos(traceSpan, tx); err != nil {
		return fmt.Errorf("[Validate][%s] error spending utxos: %v", tx.TxID(), err)
	}

	txMetaData, err := v.registerTxInMetaStore(traceSpan, tx, spentUtxos)
	if err != nil {
		if errors.Is(err, txmeta.ErrAlreadyExists) {
			// stop all processing, this transaction has already been validated and passed into the block assembly
			v.logger.Debugf("[Validate][%s] tx already exists in meta utxoStore, not sending to block assembly: %v", tx.TxIDChainHash().String(), err)
			return nil
		}

		v.reverseSpends(traceSpan, spentUtxos)
		return fmt.Errorf("error registering tx in meta utxoStore: %v", err)
	}

	if !v.blockAssemblyDisabled {
		// first we send the tx to the block assembler
		if err = v.sendToBlockAssembler(traceSpan, &blockassembly.Data{
			TxIDChainHash: tx.TxIDChainHash(),
			Fee:           txMetaData.Fee,
			Size:          uint64(tx.Size()),
			LockTime:      tx.LockTime,
		}, spentUtxos); err != nil {
			// TODO remove from tx meta store
			v.reverseSpends(traceSpan, spentUtxos)
			traceSpan.RecordError(err)
			return fmt.Errorf("error sending tx to block assembler: %v", err)
		}
	}

	storeStart := gocore.CurrentNanos()
	storeStat := stat.NewStat("utxo.Store", true)
	defer func() {
		storeStat.AddTime(storeStart)
	}()

	// then we store the new utxos from the tx
	err = v.utxoStore.Store(traceSpan.Ctx, tx)
	if err != nil {
		// TODO remove from tx meta store
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
	}()

	txMetaSpan := tracing.Start(ctx, "Validator:Validate:StoreTxMeta")
	defer txMetaSpan.Finish()

	data, err := v.txMetaStore.Create(ctx, tx)
	if err != nil {
		if errors.Is(err, txmeta.ErrAlreadyExists) {
			// this does not need to be a warning, it's just a duplicate validation request
			return nil, txmeta.ErrAlreadyExists
		}

		v.reverseSpends(traceSpan, spentUtxos)
		return data, errors.Join(fmt.Errorf("error sending tx %s to tx meta utxoStore", tx.TxIDChainHash().String()), err)
	}

	return data, nil
}

func (v *Validator) validateTransaction(traceSpan tracing.Span, tx *bt.Tx) error {
	start, stat, ctx := util.StartStatFromContext(traceSpan.Ctx, "validateTransaction")
	defer func() {
		stat.AddTime(start)
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

	// TODO Should we be doing this in a batch?
	err = v.utxoStore.Spend(ctx, spends)
	if err != nil {
		traceSpan.RecordError(err)
		return nil, fmt.Errorf("validator: UTXO Store spend failed: %v", err)
	}

	return spends, nil
}

func (v *Validator) sendToBlockAssembler(traceSpan tracing.Span, bData *blockassembly.Data, reservedUtxos []*utxostore.Spend) error {
	start, stat, ctx := util.StartStatFromContext(traceSpan.Ctx, "sendToBlockAssembler")
	defer func() {
		stat.AddTime(start)
	}()

	if v.kafkaProducer != nil {
		if err := v.publishToKafka(traceSpan, bData); err != nil {
			v.reverseSpends(traceSpan, reservedUtxos)
			traceSpan.RecordError(err)
			return fmt.Errorf("error sending tx to kafka: %v", err)
		}
	} else {
		if _, err := v.blockAssembler.Store(ctx, bData.TxIDChainHash, bData.Fee, bData.Size, bData.LockTime, bData.UtxoHashes); err != nil {
			v.reverseSpends(traceSpan, reservedUtxos)
			traceSpan.RecordError(err)
			return fmt.Errorf("error sending tx to block assembler: %v", err)
		}
	}

	return nil
}

func (v *Validator) reverseSpends(traceSpan tracing.Span, spentUtxos []*utxostore.Spend) {
	start, stat, ctx := util.StartStatFromContext(traceSpan.Ctx, "reverseSpends")
	defer func() {
		stat.AddTime(start)
	}()

	reverseUtxoSpan := tracing.Start(ctx, "Validator:Validate:ReverseUtxos")
	defer reverseUtxoSpan.Finish()

	// decouple the tracing context to not cancel the context when the tx is being saved in the background
	callerSpan := opentracing.SpanFromContext(reverseUtxoSpan.Ctx)
	setCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)
	_, _, ctx = util.StartStatFromContext(setCtx, "reverseSpends")

	if errReset := v.utxoStore.UnSpend(ctx, spentUtxos); errReset != nil {
		reverseUtxoSpan.RecordError(errReset)
		v.logger.Errorf("error resetting utxos %v", errReset)
	}
}

func (v *Validator) publishToKafka(traceSpan tracing.Span, bData *blockassembly.Data) error {
	start, stat, ctx := util.StartStatFromContext(traceSpan.Ctx, "publishToKafka")
	defer func() {
		stat.AddTime(start)
	}()

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
