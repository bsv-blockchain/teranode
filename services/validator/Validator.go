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

func (v *Validator) Validate(ctx context.Context, tx *bt.Tx) (err error) {
	traceSpan := tracing.Start(ctx, "Validator:Validate")
	var spentUtxos []*utxostore.Spend
	defer func(reservedUtxos *[]*utxostore.Spend) {
		traceSpan.Finish()
		if r := recover(); r != nil {
			if len(*reservedUtxos) > 0 {
				// TODO is this correct in the recover? should we be reversing the utxos?
				v.reverseSpends(tracing.Start(ctx, "Validator:Validate:Recover"), *reservedUtxos)
			}

			v.logger.Errorf("[VALIDATOR] Validate recover: %v", r)
		}
	}(&spentUtxos)

	if tx.IsCoinbase() {
		return fmt.Errorf("coinbase transactions are not supported: %s", tx.TxIDChainHash().String())
	}

	// get the fees and utxoHashes, before we spend the utxos
	fees, parentTxHashes, err := v.getFeesAndUtxoHashes(tx)
	if err != nil {
		return fmt.Errorf("error getting fees and utxo hashes: %v", err)
	}

	if err = v.validateTransaction(traceSpan, tx); err != nil {
		return err
	}

	// this will reverse the spends if there is an error
	// TODO make this stricter, checking whether this utxo was already spent by the same tx and return early if so
	//      do not allow any utxo be spent more than once
	if spentUtxos, err = v.spendUtxos(traceSpan, tx); err != nil {
		return err
	}

	// TODO should this be here? or should it be in block assembly?
	if err = v.registerTxInMetaStore(traceSpan, tx, fees, parentTxHashes, spentUtxos); err != nil {
		v.reverseSpends(traceSpan, spentUtxos)
		return fmt.Errorf("error registering tx in meta utxoStore: %v", err)
	}

	if v.blockAssemblyDisabled {
		// block assembly is disabled, which means we are running in non-mining mode
		utxoSpan := tracing.Start(traceSpan.Ctx, "Validator:Validate:UpdateSpendable")
		defer utxoSpan.Finish()

		// store the new utxos from the tx
		if err = v.utxoStore.Store(traceSpan.Ctx, tx); err != nil {
			v.reverseSpends(traceSpan, spentUtxos)
			return fmt.Errorf("error storing tx %s in utxo utxoStore: %v", tx.TxIDChainHash().String(), err)
		}

		return nil
	}

	// first we send the tx to the block assembler
	if err = v.sendToBlockAssembler(traceSpan, &blockassembly.Data{
		TxIDChainHash: tx.TxIDChainHash(),
		Fee:           fees,
		Size:          uint64(tx.Size()),
		LockTime:      tx.LockTime,
	}, spentUtxos); err != nil {
		v.reverseSpends(traceSpan, spentUtxos)
		traceSpan.RecordError(err)
		return fmt.Errorf("error sending tx to block assembler: %v", err)
	}

	// then we store the new utxos from the tx
	if err = v.utxoStore.Store(traceSpan.Ctx, tx); err != nil {
		v.reverseSpends(traceSpan, spentUtxos)
		return fmt.Errorf("error storing tx %s in utxo utxoStore: %v", tx.TxIDChainHash().String(), err)
	}

	return nil
}

func (v *Validator) registerTxInMetaStore(traceSpan tracing.Span, tx *bt.Tx, fees uint64, parentTxHashes []*chainhash.Hash, reservedUtxos []*utxostore.Spend) error {
	txMetaSpan := tracing.Start(traceSpan.Ctx, "Validator:Validate:StoreTxMeta")
	defer txMetaSpan.Finish()

	if err := v.txMetaStore.Create(txMetaSpan.Ctx, tx.TxIDChainHash(), fees, uint64(tx.Size()), parentTxHashes, nil, tx.LockTime); err != nil {
		if errors.Is(err, txmeta.ErrAlreadyExists) {
			// this does not need to be a warning, it's just a duplicate validation request
			return nil
		}

		v.reverseSpends(traceSpan, reservedUtxos)
		return fmt.Errorf("error sending tx %s to tx meta utxoStore: %v", tx.TxIDChainHash().String(), err)
	}

	return nil
}

func (v *Validator) validateTransaction(traceSpan tracing.Span, tx *bt.Tx) error {
	basicSpan := tracing.Start(traceSpan.Ctx, "Validator:Validate:Basic")
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
	utxoSpan := tracing.Start(traceSpan.Ctx, "Validator:Validate:SpendUtxos")
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
	err = v.utxoStore.Spend(utxoSpan.Ctx, spends)
	if err != nil {
		traceSpan.RecordError(err)
		return nil, fmt.Errorf("validator: UTXO Store spend failed: %v", err)
	}

	return spends, nil
}

func (v *Validator) getFeesAndUtxoHashes(tx *bt.Tx) (uint64, []*chainhash.Hash, error) {
	var fees uint64
	utxoHashes := make([]*chainhash.Hash, 0, len(tx.Outputs))

	for _, input := range tx.Inputs {
		fees += input.PreviousTxSatoshis
	}

	for i, output := range tx.Outputs {
		if output.Satoshis > 0 {
			fees -= output.Satoshis

			utxoHash, utxoErr := util.UTXOHashFromOutput(tx.TxIDChainHash(), output, uint32(i))
			if utxoErr != nil {
				return 0, nil, fmt.Errorf("error getting output utxo hash: %s", utxoErr.Error())
			}

			utxoHashes = append(utxoHashes, utxoHash)
		}
	}

	return fees, utxoHashes, nil
}

func (v *Validator) sendToBlockAssembler(traceSpan tracing.Span, bData *blockassembly.Data, reservedUtxos []*utxostore.Spend) error {

	if v.kafkaProducer != nil {
		if err := v.publishToKafka(traceSpan, bData); err != nil {
			v.reverseSpends(traceSpan, reservedUtxos)
			traceSpan.RecordError(err)
			return fmt.Errorf("error sending tx to kafka: %v", err)
		}
	} else {
		if _, err := v.blockAssembler.Store(traceSpan.Ctx, bData.TxIDChainHash, bData.Fee, bData.Size, bData.LockTime, bData.UtxoHashes); err != nil {
			v.reverseSpends(traceSpan, reservedUtxos)
			traceSpan.RecordError(err)
			return fmt.Errorf("error sending tx to block assembler: %v", err)
		}
	}

	return nil
}

func (v *Validator) reverseSpends(traceSpan tracing.Span, spentUtxos []*utxostore.Spend) {
	reverseUtxoSpan := tracing.Start(traceSpan.Ctx, "Validator:Validate:ReverseUtxos")
	defer reverseUtxoSpan.Finish()

	if errReset := v.utxoStore.UnSpend(reverseUtxoSpan.Ctx, spentUtxos); errReset != nil {
		reverseUtxoSpan.RecordError(errReset)
		v.logger.Errorf("error resetting utxos %v", errReset)
	}
}

func (v *Validator) publishToKafka(traceSpan tracing.Span, bData *blockassembly.Data) error {
	kafkaSpan := tracing.Start(traceSpan.Ctx, "Validator:Validate:publishToKafka")
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
