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
	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-bitcoin"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
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

func (v *Validator) Validate(ctx context.Context, tx *bt.Tx) error {
	traceSpan := tracing.Start(ctx, "Validator:Validate")
	var reservedUtxos []*chainhash.Hash
	defer func(reservedUtxos *[]*chainhash.Hash) {
		traceSpan.Finish()
		if r := recover(); r != nil {
			if len(*reservedUtxos) > 0 {
				// TODO is this correct in the recover? should we be reversing the utxos?
				v.reverseSpends(tracing.Start(ctx, "Validator:Validate:Recover"), *reservedUtxos)
			}

			v.logger.Errorf("[VALIDATOR] Validate recover: %v", r)
		}
	}(&reservedUtxos)

	if tx.IsCoinbase() {
		return fmt.Errorf("coinbase transactions are not supported: %s", tx.TxIDChainHash().String())
	}

	// get the fees and utxoHashes, before we spend the utxos
	fees, utxoHashes, err := v.getFeesAndUtxoHashes(tx)
	if err != nil {
		return fmt.Errorf("error getting fees and utxo hashes: %v", err)
	}

	if err = v.validateTransaction(traceSpan, tx); err != nil {
		return err
	}

	g, _ := errgroup.WithContext(ctx)

	var parentTxHashes []*chainhash.Hash
	g.Go(func() error {
		// this will reverse the spends if there is an error
		if reservedUtxos, parentTxHashes, err = v.spendUtxos(traceSpan, tx); err != nil {
			return err
		}
		return nil
	})

	if v.blockAssemblyDisabled {
		// block assembly is disabled, which means we are running in non-mining mode
		utxoSpan := tracing.Start(traceSpan.Ctx, "Validator:Validate:StoreUtxos")
		defer utxoSpan.Finish()

		// wait for the utxos to be spent before we store the new ones
		if err = g.Wait(); err != nil {
			return err
		}

		// we must create the new utxos in the utxo utxoStore and will stop processing after that
		for _, hash := range utxoHashes {
			if _, err = v.utxoStore.Store(utxoSpan.Ctx, hash, tx.LockTime); err != nil {
				v.reverseSpends(traceSpan, reservedUtxos)
				utxoSpan.RecordError(err)
				return fmt.Errorf("error storing utxo: %v", err)
			}
		}

		return nil
	}

	// register transaction in tx status utxoStore
	duplicateValidationRequest := false
	g.Go(func() error {
		if duplicateValidationRequest, err = v.registerTxInMetaStore(traceSpan, tx, fees, parentTxHashes, reservedUtxos); err != nil {
			return err
		}
		return nil
	})

	// this allows the spending of the utxo and storing in the meta store to happen in parallel
	if err = g.Wait(); err != nil {
		_ = v.txMetaStore.Delete(traceSpan.Ctx, tx.TxIDChainHash())
		return err
	}

	if duplicateValidationRequest {
		// this was a duplicate validation request, because the tx already existed in the tx meta store
		// we can just return here and will not send the tx to the block assembler
		return nil
	}

	return v.sendToBlockAssembler(traceSpan, &blockassembly.Data{
		TxIDChainHash: tx.TxIDChainHash(),
		Fee:           fees,
		Size:          uint64(tx.Size()),
		LockTime:      tx.LockTime,
		UtxoHashes:    utxoHashes,
	}, reservedUtxos)
}

func (v *Validator) registerTxInMetaStore(traceSpan tracing.Span, tx *bt.Tx, fees uint64, parentTxHashes []*chainhash.Hash, reservedUtxos []*chainhash.Hash) (bool, error) {
	txMetaSpan := tracing.Start(traceSpan.Ctx, "Validator:Validate:StoreTxMeta")
	defer txMetaSpan.Finish()

	if err := v.txMetaStore.Create(txMetaSpan.Ctx, tx.TxIDChainHash(), fees, uint64(tx.Size()), parentTxHashes, nil, tx.LockTime); err != nil {
		if errors.Is(err, txmeta.ErrAlreadyExists) {
			// this does not need to be a warning, it's just a duplicate validation request
			return true, nil
		}

		v.reverseSpends(traceSpan, reservedUtxos)
		return false, fmt.Errorf("error sending tx %s to tx meta utxoStore: %v", tx.TxIDChainHash().String(), err)
	}

	return false, nil
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

func (v *Validator) spendUtxos(traceSpan tracing.Span, tx *bt.Tx) ([]*chainhash.Hash, []*chainhash.Hash, error) {
	utxoSpan := tracing.Start(traceSpan.Ctx, "Validator:Validate:SpendUtxos")
	defer func() {
		utxoSpan.Finish()
	}()

	var err error
	var hash *chainhash.Hash
	var utxoResponse *utxostore.UTXOResponse
	var parentTxHash *chainhash.Hash

	reservedUtxos := make([]*chainhash.Hash, 0, len(tx.Inputs))
	parentTxHashes := make([]*chainhash.Hash, 0, len(tx.Inputs))

	// check the utxos
	txIDChainHash := tx.TxIDChainHash()
	for _, input := range tx.Inputs {
		hash, err = util.UTXOHashFromInput(input)
		if err != nil {
			utxoSpan.RecordError(err)
			return nil, nil, fmt.Errorf("error getting input utxo hash: %s", err.Error())
		}

		// TODO Should we be doing this in a batch?
		utxoResponse, err = v.utxoStore.Spend(utxoSpan.Ctx, hash, txIDChainHash)
		if err != nil {
			utxoSpan.RecordError(err)
			break
		}
		if utxoResponse == nil {
			err = fmt.Errorf("utxoResponse %s is empty, recovered", hash.String())
			utxoSpan.RecordError(err)
			break
		}
		if utxoResponse.Status != int(utxostore_api.Status_OK) {
			err = fmt.Errorf("utxo %d of %s (%s) is not spendable: %s", input.PreviousTxOutIndex, input.PreviousTxIDStr(), hash.String(), utxostore_api.Status(utxoResponse.Status))
			utxoSpan.RecordError(err)
			break
		}

		reservedUtxos = append(reservedUtxos, hash)

		parentTxHash, err = chainhash.NewHash(bt.ReverseBytes(input.PreviousTxID()))
		parentTxHashes = append(parentTxHashes, parentTxHash)
	}

	if err != nil {
		v.logger.Debugf("reverse %d utxos for %s", len(reservedUtxos), txIDChainHash.String())
		defer func() {
			utxoSpan.Finish()
		}()

		v.reverseSpends(traceSpan, reservedUtxos)
		traceSpan.RecordError(err)
		return nil, nil, fmt.Errorf("validator: UTXO Store spend failed: %v", err)
	}

	return reservedUtxos, parentTxHashes, nil
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

func (v *Validator) sendToBlockAssembler(traceSpan tracing.Span, bData *blockassembly.Data, reservedUtxos []*chainhash.Hash) error {

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

func (v *Validator) reverseSpends(traceSpan tracing.Span, reservedUtxos []*chainhash.Hash) {
	reverseUtxoSpan := tracing.Start(traceSpan.Ctx, "Validator:Validate:ReverseUtxos")
	defer reverseUtxoSpan.Finish()

	for _, hash := range reservedUtxos {
		if _, errReset := v.utxoStore.Reset(reverseUtxoSpan.Ctx, hash); errReset != nil {
			reverseUtxoSpan.RecordError(errReset)
			v.logger.Errorf("error resetting utxo %s: %v", hash.String(), errReset)
		}
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
