package validator

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/TAAL-GmbH/arc/api"
	"github.com/TAAL-GmbH/arc/validator" // TODO move this to UBSV repo - add recover to validation
	"github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/bscript/interpreter"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/go-bitcoin"
	"github.com/ordishs/gocore"
)

const (
	MaxBlockSize                       = 4 * 1024 * 1024 * 1024
	MaxSatoshis                        = 21_000_000_00_000_000
	coinbaseTxID                       = "0000000000000000000000000000000000000000000000000000000000000000"
	MaxTxSigopsCountPolicyAfterGenesis = ^uint32(0) // UINT32_MAX
)

var (
	ErrBadRequest = errors.New("VALIDATOR_BAD_REQUEST")
	ErrInternal   = errors.New("VALIDATOR_INTERNAL")
)

type Validator struct {
	logger                 ulogger.Logger
	utxoStore              utxostore.Interface
	blockAssembler         blockassembly.Store
	txMetaStore            txmeta.Store
	saveInParallel         bool
	blockAssemblyDisabled  bool
	blockassemblyKafkaChan chan []byte
	txMetaKafkaChan        chan []byte
	stats                  *gocore.Stat
}

func New(ctx context.Context, logger ulogger.Logger, store utxostore.Interface, txMetaStore txmeta.Store) (Interface, error) {
	initPrometheusMetrics()

	ba := blockassembly.NewClient(ctx, logger)

	v := &Validator{
		logger:         logger,
		utxoStore:      store,
		blockAssembler: ba,
		txMetaStore:    txMetaStore,
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
	start, stat, _ := util.NewStatFromContext(cntxt, "Health", v.stats)
	defer stat.AddTime(start)

	return 0, "LocalValidator", nil
}

func (v *Validator) GetBlockHeight() (height uint32, err error) {
	return v.utxoStore.GetBlockHeight()
}

// TODO try to break this
func (v *Validator) Validate(cntxt context.Context, tx *bt.Tx, blockHeight uint32) (err error) {
	start, stat, ctx := util.NewStatFromContext(cntxt, "Validate", v.stats)
	defer func() {
		stat.AddTime(start)
		prometheusTransactionValidateTotal.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
	}()

	traceSpan := tracing.Start(ctx, "Validator:Validate")
	var spentUtxos []*utxostore.Spend

	defer func(reservedUtxos *[]*utxostore.Spend) {
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
		return errors.Join(ErrBadRequest, fmt.Errorf("[Validate][%s] coinbase transactions are not supported", tx.TxIDChainHash().String()))
	}

	if err = v.validateTransaction(traceSpan, tx, blockHeight); err != nil {
		return errors.Join(ErrBadRequest, fmt.Errorf("[Validate][%s] error validating transaction: %v", tx.TxID(), err))
	}

	// decouple the tracing context to not cancel the context when finalize the block assembly
	callerSpan := opentracing.SpanFromContext(traceSpan.Ctx)
	setCtx := opentracing.ContextWithSpan(context.Background(), callerSpan)
	setCtx = util.CopyStatFromContext(traceSpan.Ctx, setCtx)
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
	if spentUtxos, err = v.spendUtxos(setSpan, tx); err != nil {
		return errors.Join(ErrInternal, fmt.Errorf("[Validate][%s] error spending utxos: %v", tx.TxID(), err))
	}

	txMetaData, err := v.registerTxInMetaStore(setSpan, tx, spentUtxos)
	if err != nil {
		if errors.Is(err, txmeta.NewErrTxmetaAlreadyExists(tx.TxIDChainHash())) {
			// stop all processing, this transaction has already been validated and passed into the block assembly
			v.logger.Debugf("[Validate][%s] tx already exists in metaStore, not sending to block assembly: %v", tx.TxIDChainHash().String(), err)
			return nil
		}

		v.logger.Errorf("[Validate][%s] error registering tx in metaStore: %v", tx.TxIDChainHash().String(), err)
		if reverseErr := v.reverseSpends(setSpan, spentUtxos); reverseErr != nil {
			err = errors.Join(err, fmt.Errorf("error reversing utxo spends: %v", reverseErr))
		}
		return errors.Join(ErrInternal, fmt.Errorf("error registering tx in metaStore: %v", err))
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
			err = errors.Join(ErrInternal, fmt.Errorf("error sending tx to block assembler: %v", err))

			if reverseErr := v.reverseTxMetaStore(setSpan, tx.TxIDChainHash()); err != nil {
				err = errors.Join(err, fmt.Errorf("error reversing tx meta utxoStore: %v", reverseErr))
			}

			if reverseErr := v.reverseSpends(setSpan, spentUtxos); reverseErr != nil {
				err = errors.Join(err, fmt.Errorf("error reversing utxo spends: %v", reverseErr))
			}

			setSpan.RecordError(err)
			return err
		}
	}

	// if the block assembly creates utxos, then we don't need to do it here
	// then we store the new utxos from the tx
	err = v.storeUtxos(setSpan.Ctx, tx)
	if err != nil {
		v.logger.Errorf("[Validate][%s] error storing tx in utxo utxoStore: %v", tx.TxIDChainHash().String(), err)

		// TODO We need to make sure that these actions are actually completed
		//      Push into a queue to be processed later?

		if err = v.blockAssembler.RemoveTx(setSpan.Ctx, tx.TxIDChainHash()); err != nil {
			err = errors.Join(ErrInternal, err, fmt.Errorf("error removing tx from block assembly: %v", err))
		}

		if err = v.reverseTxMetaStore(setSpan, tx.TxIDChainHash()); err != nil {
			err = errors.Join(ErrInternal, err, fmt.Errorf("error reversing tx meta utxoStore: %v", err))
		}

		if reverseErr := v.reverseSpends(setSpan, spentUtxos); reverseErr != nil {
			err = errors.Join(ErrInternal, err, fmt.Errorf("error reversing utxo spends: %v", reverseErr))
		}

		setSpan.RecordError(err)
		return err
	}

	return nil
}

func (v *Validator) reverseTxMetaStore(setSpan tracing.Span, txID *chainhash.Hash) (err error) {
	for retries := 0; retries < 3; retries++ {
		if metaErr := v.txMetaStore.Delete(setSpan.Ctx, txID); metaErr != nil {
			if retries < 2 {
				backoff := time.Duration(2^retries) * time.Second
				v.logger.Errorf("error deleting tx %s from tx meta utxoStore, retrying in %s: %v", txID.String(), backoff.String(), metaErr)
				time.Sleep(backoff)
			} else {
				err = fmt.Errorf("error deleting tx %s from tx meta utxoStore: %v", txID.String(), metaErr)
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
		return data, fmt.Errorf("error sending tx %s to txmetaStore: %w", tx.TxIDChainHash().String(), err)
	}

	if v.txMetaKafkaChan != nil {
		startKafka := time.Now()
		v.txMetaKafkaChan <- append(tx.TxIDChainHash().CloneBytes(), data.MetaBytes()...)
		prometheusValidatorSendToBlockValidationKafka.Observe(float64(time.Since(startKafka).Microseconds()) / 1_000_000)
	}

	return data, nil
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
		if input.PreviousTxSatoshis == 0 {
			continue // There are some old transactions (e.g. d5a13dcb1ad24dbffab91c3c2ffe7aea38d5e84b444c0014eb6c7c31fe8e23fc) that have 0 satoshis
		}

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
			if spentErr.SpendingTxID != nil {
				err = v.blockAssembler.RemoveTx(ctx, spentErr.SpendingTxID)
				if err != nil {
					v.logger.Errorf("validator: UTXO Store remove tx failed: %v", err)
				}
			}
		}

		return nil, fmt.Errorf("validator: UTXO Store spend failed for %s: %w", tx.TxIDChainHash().String(), err)
	}

	return spends, nil
}

func (v *Validator) sendToBlockAssembler(traceSpan tracing.Span, bData *blockassembly.Data, reservedUtxos []*utxostore.Spend) error {
	startTime, stat, ctx := util.StartStatFromContext(traceSpan.Ctx, "sendToBlockAssembler")
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
			e := fmt.Errorf("error calling blockAssembler Store(): %v", err)
			traceSpan.RecordError(e)
			return e
		}
	}

	return nil
}

func (v *Validator) reverseSpends(traceSpan tracing.Span, spentUtxos []*utxostore.Spend) error {
	start, stat, ctx := util.StartStatFromContext(traceSpan.Ctx, "reverseSpends")
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
				return fmt.Errorf("error resetting utxos %v", errReset)
			}
		} else {
			break
		}
	}

	return nil
}

func (v *Validator) validateTransaction(traceSpan tracing.Span, tx *bt.Tx, blockHeight uint32) error {
	start, stat, ctx := util.StartStatFromContext(traceSpan.Ctx, "validateTransaction")
	defer func() {
		stat.AddTime(start)
		prometheusTransactionValidate.Observe(float64(time.Since(start).Microseconds()) / 1_000_000)
	}()

	basicSpan := tracing.Start(ctx, "Validator:Validate:Basic")
	defer func() {
		basicSpan.Finish()
	}()

	// TODO read policy from config
	policy := &bitcoin.Settings{}
	//
	// Each node will verify every transaction against a long checklist of criteria:
	//
	txSize := tx.Size()

	// fmt.Println(hex.EncodeToString(tx.ExtendedBytes()))

	// 0) Check whether we have a complete transaction in extended format, with all input information
	//    we cannot check the satoshi input, OP_RETURN is allowed 0 satoshis
	if !util.IsExtended(tx, blockHeight) {
		return fmt.Errorf("transaction is not in extended format")
	}

	// 1) Neither lists of inputs or outputs are empty
	if len(tx.Inputs) == 0 || len(tx.Outputs) == 0 {
		return fmt.Errorf("transaction has no inputs or outputs")
	}

	// 2) The transaction size in bytes is less than maxtxsizepolicy.
	if err := v.checkTxSize(txSize, policy); err != nil {
		return err
	}

	// 3) check that each input value, as well as the sum, are in the allowed range of values (less than 21m coins)
	// 5) None of the inputs have hash=0, N=–1 (coinbase transactions should not be relayed)
	if err := v.checkInputs(tx); err != nil {
		return err
	}

	// 4) Each output value, as well as the total, must be within the allowed range of values (less than 21m coins,
	//    more than the dust threshold if 1 unless it's OP_RETURN, which is allowed to be 0)
	if err := v.checkOutputs(tx, blockHeight); err != nil {
		return err
	}

	// 6) nLocktime is equal to INT_MAX, or nLocktime and nSequence values are satisfied according to MedianTimePast
	//    => checked by the node, we do not want to have to know the current block height

	// 7) The transaction size in bytes is greater than or equal to 100
	if txSize < 100 {
		return fmt.Errorf("transaction size in bytes is less than 100 bytes")
	}

	// 8) The number of signature operations (SIGOPS) contained in the transaction is less than the signature operation limit
	if err := v.sigOpsCheck(tx, policy); err != nil {
		return err
	}

	// SAO - https://bitcoin.stackexchange.com/questions/83805/did-the-introduction-of-verifyscript-cause-a-backwards-incompatible-change-to-co
	if blockHeight != 163685 {
		// 9) The unlocking script (scriptSig) can only push numbers on the stack
		if err := v.pushDataCheck(tx); err != nil {
			return err
		}
	}

	// 10) Reject if the sum of input values is less than sum of output values
	// 11) Reject if transaction fee would be too low (minRelayTxFee) to get into an empty block.
	if err := v.checkFees(tx, api.FeesToBtFeeQuote(policy.MinMiningTxFee)); err != nil {
		return err
	}

	// 12) The unlocking scripts for each input must validate against the corresponding output locking scripts
	if err := v.checkScripts(tx, blockHeight); err != nil {
		return err
	}

	// everything checks out
	return nil
}

func (v *Validator) checkTxSize(txSize int, policy *bitcoin.Settings) error {
	maxTxSizePolicy := policy.MaxTxSizePolicy
	if maxTxSizePolicy == 0 {
		// no policy found for tx size, use max block size
		maxTxSizePolicy = MaxBlockSize
	}
	if txSize > maxTxSizePolicy {
		return fmt.Errorf("transaction size in bytes is greater than max tx size policy %d", maxTxSizePolicy)
	}

	return nil
}

func (v *Validator) checkOutputs(tx *bt.Tx, blockHeight uint32) error {
	total := uint64(0)

	minOutput := uint64(0)
	if blockHeight >= util.GenesisActivationHeight {
		minOutput = bt.DustLimit
	}

	for index, output := range tx.Outputs {
		isData := output.LockingScript.IsData()
		switch {
		case !isData && (output.Satoshis > MaxSatoshis || output.Satoshis < minOutput):
			return validator.NewError(fmt.Errorf("transaction output %d satoshis is invalid", index), api.ErrStatusOutputs)
		case isData && output.Satoshis != 0:
			return validator.NewError(fmt.Errorf("transaction output %d has non 0 value op return", index), api.ErrStatusOutputs)
		}
		total += output.Satoshis
	}

	if total > MaxSatoshis {
		return validator.NewError(fmt.Errorf("transaction output total satoshis is too high"), api.ErrStatusOutputs)
	}

	return nil
}

func (v *Validator) checkInputs(tx *bt.Tx) error {
	total := uint64(0)
	for index, input := range tx.Inputs {
		if hex.EncodeToString(input.PreviousTxID()) == coinbaseTxID {
			return validator.NewError(fmt.Errorf("transaction input %d is a coinbase input", index), api.ErrStatusInputs)
		}
		/* lots of our valid test transactions have this sequence number, is this not allowed?
		if input.SequenceNumber == 0xffffffff {
			fmt.Printf("input %d has sequence number 0xffffffff, txid = %s", index, tx.TxID())
			return validator.NewError(fmt.Errorf("transaction input %d sequence number is invalid", index), arc.ErrStatusInputs)
		}
		*/
		if input.PreviousTxSatoshis > MaxSatoshis {
			return validator.NewError(fmt.Errorf("transaction input %d satoshis is too high", index), api.ErrStatusInputs)
		}
		total += input.PreviousTxSatoshis
	}
	if total > MaxSatoshis {
		return validator.NewError(fmt.Errorf("transaction input total satoshis is too high"), api.ErrStatusInputs)
	}

	return nil
}

func (v *Validator) checkFees(tx *bt.Tx, feeQuote *bt.FeeQuote) error {
	feesOK, err := tx.IsFeePaidEnough(feeQuote)
	if err != nil {
		return err
	}

	if !feesOK {
		return fmt.Errorf("transaction fee is too low")
	}

	return nil
}

func (v *Validator) sigOpsCheck(tx *bt.Tx, policy *bitcoin.Settings) error {
	maxSigOps := policy.MaxTxSigopsCountsPolicy

	if maxSigOps == 0 {
		maxSigOps = int64(MaxTxSigopsCountPolicyAfterGenesis)
	}

	numSigOps := int64(0)
	for _, input := range tx.Inputs {
		parser := interpreter.DefaultOpcodeParser{}
		parsedUnlockingScript, err := parser.Parse(input.PreviousTxScript)
		if err != nil {
			return err
		}

		for _, op := range parsedUnlockingScript {
			if op.Value() == bscript.OpCHECKSIG || op.Value() == bscript.OpCHECKSIGVERIFY {
				numSigOps++
				if numSigOps > maxSigOps {
					return fmt.Errorf("transaction unlocking scripts have too many sigops (%d)", numSigOps)
				}
			}
		}
	}

	return nil
}

func (v *Validator) pushDataCheck(tx *bt.Tx) error {
	for index, input := range tx.Inputs {
		if input.UnlockingScript == nil {
			return fmt.Errorf("transaction input %d unlocking script is empty", index)
		}
		parser := interpreter.DefaultOpcodeParser{}
		parsedUnlockingScript, err := parser.Parse(input.UnlockingScript)
		if err != nil {
			return err
		}
		if !parsedUnlockingScript.IsPushOnly() {
			return fmt.Errorf("transaction input %d unlocking script is not push only", index)
		}
	}

	return nil
}

func (v *Validator) checkScripts(tx *bt.Tx, blockHeight uint32) error {
	for i, in := range tx.Inputs {
		prevOutput := &bt.Output{
			Satoshis:      in.PreviousTxSatoshis,
			LockingScript: in.PreviousTxScript,
		}

		opts := make([]interpreter.ExecutionOptionFunc, 0, 3)
		opts = append(opts, interpreter.WithTx(tx, i, prevOutput))

		if blockHeight >= util.ForkIDActivationHeight {
			opts = append(opts, interpreter.WithForkID())
		}

		if blockHeight >= util.GenesisActivationHeight {
			opts = append(opts, interpreter.WithAfterGenesis())
		}

		// opts = append(opts, interpreter.WithDebugger(&LogDebugger{}),

		if err := interpreter.NewEngine().Execute(opts...); err != nil {
			return fmt.Errorf("script execution failed: %w", err)
		}
	}

	return nil
}
