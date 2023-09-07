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
)

type Validator struct {
	logger          utils.Logger
	store           utxostore.Interface
	blockAssembler  blockassembly.Store
	txMetaStore     txmeta.Store
	kafkaProducer   sarama.SyncProducer
	kafkaTopic      string
	kafkaPartitions int
	saveInParallel  bool
}

func New(ctx context.Context, logger utils.Logger, store utxostore.Interface, txMetaStore txmeta.Store) (Interface, error) {
	ba := blockassembly.NewClient(ctx)

	validator := &Validator{
		logger:         logger,
		store:          store,
		blockAssembler: ba,
		txMetaStore:    txMetaStore,
		saveInParallel: true,
	}

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
	var reservedUtxos []*chainhash.Hash
	defer func(reservedUtxos *[]*chainhash.Hash) {
		if r := recover(); r != nil {
			if len(*reservedUtxos) > 0 {
				// TODO is this correct in the recover? should we be reversing the utxos?
				v.reverseSpends(*reservedUtxos, tracing.Start(ctx, "Validator:Validate:Recover"))
			}

			v.logger.Errorf("[VALIDATOR] Validate recover: %v", r)
		}
	}(&reservedUtxos)

	traceSpan := tracing.Start(ctx, "Validator:Validate")
	defer traceSpan.Finish()

	if tx.IsCoinbase() {
		return fmt.Errorf("coinbase transactions are not supported: %s", tx.TxIDChainHash().String())
	}

	if err := v.validateTransaction(traceSpan, tx); err != nil {
		return err
	}

	// get the fees and utxoHashes, before we spend the utxos
	fees, utxoHashes, err := v.getFeesAndUtxoHashes(tx)
	if err != nil {
		return fmt.Errorf("error getting fees and utxo hashes: %v", err)
	}

	var parentTxHashes []*chainhash.Hash
	if reservedUtxos, parentTxHashes, err = v.spendUtxos(traceSpan, tx); err != nil {
		return err
	}

	// register transaction in tx status store
	if err = v.registerTxInMetaStore(ctx, traceSpan, tx, fees, parentTxHashes, utxoHashes, reservedUtxos); err != nil {
		return err
	}

	return v.sendToBlockAssembler(ctx, tx.TxIDChainHash(), traceSpan, reservedUtxos)
}

func (v *Validator) registerTxInMetaStore(ctx context.Context, traceSpan tracing.Span, tx *bt.Tx, fees uint64, parentTxHashes []*chainhash.Hash, utxoHashes []*chainhash.Hash, reservedUtxos []*chainhash.Hash) error {
	if err := v.txMetaStore.Create(ctx, tx.TxIDChainHash(), fees, uint64(tx.Size()), parentTxHashes, utxoHashes, tx.LockTime); err != nil {
		if errors.Is(err, txmeta.ErrAlreadyExists) {
			// this does not need to be a warning, it's just a duplicate validation request
			return nil
		}

		v.reverseSpends(reservedUtxos, traceSpan)
		return fmt.Errorf("error sending tx %s to tx meta store: %v", tx.TxIDChainHash().String(), err)
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

func (v *Validator) spendUtxos(traceSpan tracing.Span, tx *bt.Tx) ([]*chainhash.Hash, []*chainhash.Hash, error) {
	utxoSpan := tracing.Start(traceSpan.Ctx, "Validator:Validate:CheckUtxos")
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
		utxoResponse, err = v.store.Spend(utxoSpan.Ctx, hash, txIDChainHash)
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

		v.reverseSpends(reservedUtxos, traceSpan)
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

func (v *Validator) sendToBlockAssembler(ctx context.Context, txIDChainHash *chainhash.Hash, traceSpan tracing.Span, reservedUtxos []*chainhash.Hash) error {
	if v.kafkaProducer != nil {
		if err := v.publishToKafka(txIDChainHash); err != nil {
			v.reverseSpends(reservedUtxos, traceSpan)
			return fmt.Errorf("error sending tx to kafka: %v", err)
		}
	} else {
		if _, err := v.blockAssembler.Store(ctx, txIDChainHash); err != nil {
			v.reverseSpends(reservedUtxos, traceSpan)
			return fmt.Errorf("error sending tx to block assembler: %v", err)
		}
	}

	return nil
}

func (v *Validator) reverseSpends(reservedUtxos []*chainhash.Hash, traceSpan tracing.Span) {
	reverseUtxoSpan := tracing.Start(traceSpan.Ctx, "Validator:Validate:ReverseUtxos")
	defer func() { reverseUtxoSpan.Finish() }()

	for _, hash := range reservedUtxos {
		if _, errReset := v.store.Reset(reverseUtxoSpan.Ctx, hash); errReset != nil {
			reverseUtxoSpan.RecordError(errReset)
			v.logger.Errorf("error resetting utxo %s: %v", hash.String(), errReset)
		}
	}
}

func (v *Validator) publishToKafka(txIDBytes *chainhash.Hash) error {
	// partition is the first byte of the txid - max 2^8 partitions = 256
	partition := binary.LittleEndian.Uint32(txIDBytes[:]) % uint32(v.kafkaPartitions)
	_, _, err := v.kafkaProducer.SendMessage(&sarama.ProducerMessage{
		Topic:     v.kafkaTopic,
		Partition: int32(partition),
		Key:       sarama.ByteEncoder(txIDBytes[:]),
		Value:     sarama.ByteEncoder(txIDBytes[:]),
	})
	if err != nil {
		return err
	}

	return nil
}
