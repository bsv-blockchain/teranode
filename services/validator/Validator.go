package validator

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/Shopify/sarama"
	defaultvalidator "github.com/TAAL-GmbH/arc/validator/default"
	"github.com/TAAL-GmbH/ubsv/services/blockassembly"
	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	"github.com/TAAL-GmbH/ubsv/stores/txstatus"
	utxostore "github.com/TAAL-GmbH/ubsv/stores/utxo"
	"github.com/TAAL-GmbH/ubsv/tracing"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-bitcoin"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
)

type Validator struct {
	logger          utils.Logger
	store           utxostore.Interface
	blockAssembler  blockassembly.Store
	txStatus        txstatus.Store
	kafkaProducer   sarama.SyncProducer
	kafkaTopic      string
	kafkaPartitions int
	saveInParallel  bool
}

func New(logger utils.Logger, store utxostore.Interface, txStatus txstatus.Store) (Interface, error) {
	ba := blockassembly.NewClient()

	validator := &Validator{
		logger:         logger,
		store:          store,
		blockAssembler: ba,
		txStatus:       txStatus,
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
	traceSpan := tracing.Start(ctx, "Validator:Validate")
	defer traceSpan.Finish()

	if tx.IsCoinbase() {
		coinbaseSpan := tracing.Start(traceSpan.Ctx, "Validator:Validate:IsCoinbase")
		defer coinbaseSpan.Finish()

		// TODO what checks do we need to do on a coinbase tx?
		// not just anyone should be able to send a coinbase tx through the system
		txid, err := chainhash.NewHash(bt.ReverseBytes(tx.TxIDBytes()))
		if err != nil {
			return err
		}

		hash, err := util.UTXOHashFromOutput(txid, tx.Outputs[0], 0)
		if err != nil {
			return err
		}

		// store the coinbase utxo
		// TODO this should be marked as spendable only after 100 blocks
		_, err = v.store.Store(coinbaseSpan.Ctx, hash)
		if err != nil {
			return err
		}

		return nil
	}

	basicSpan := tracing.Start(traceSpan.Ctx, "Validator:Validate:Basic")

	// check all the basic stuff
	// TODO this is using the ARC validator, but should be moved into a separate package or imported to this one
	validator := defaultvalidator.New(&bitcoin.Settings{})
	// this will also check whether the transaction is in extended format

	if err := validator.ValidateTransaction(tx); err != nil {
		basicSpan.Finish()
		return err
	}
	basicSpan.Finish()

	utxoSpan := tracing.Start(traceSpan.Ctx, "Validator:Validate:CheckUtxos")

	// check the utxos
	txIDBytes := bt.ReverseBytes(tx.TxIDBytes())
	txIDChainHash, err := chainhash.NewHash(txIDBytes)
	if err != nil {
		return err
	}

	var hash *chainhash.Hash
	var utxoResponse *utxostore.UTXOResponse
	var parentTxHash *chainhash.Hash

	reservedUtxos := make([]*chainhash.Hash, 0, len(tx.Inputs))
	parentTxHashes := make([]*chainhash.Hash, 0, len(tx.Inputs))

	for idx, input := range tx.Inputs {
		hash, err = util.UTXOHashFromInput(input)
		if err != nil {
			utxoSpan.RecordError(err)
			return err
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
			err = fmt.Errorf("utxo %d of %s is not spendable", idx, input.PreviousTxIDStr())
			utxoSpan.RecordError(err)
			break
		}

		reservedUtxos = append(reservedUtxos, hash)

		parentTxHash, err = chainhash.NewHash(bt.ReverseBytes(input.PreviousTxID()))
		parentTxHashes = append(parentTxHashes, parentTxHash)
	}

	if err != nil {
		reverseUtxoSpan := tracing.Start(traceSpan.Ctx, "Validator:Validate:ReverseUtxos")
		defer func() {
			reverseUtxoSpan.Finish()
			utxoSpan.Finish()
		}()

		// Revert all the spends
		for _, hash = range reservedUtxos {
			if _, err = v.store.Reset(reverseUtxoSpan.Ctx, hash); err != nil {
				reverseUtxoSpan.RecordError(err)
			}
		}

		return err
	}
	utxoSpan.Finish()

	// process the outputs of the transaction into new spendable outputs
	storeUtxoSpan := tracing.Start(traceSpan.Ctx, "Validator:Validate:StoreUtxos")
	defer storeUtxoSpan.Finish()

	var fees uint64
	utxoHashes := make([]*chainhash.Hash, 0, len(tx.Outputs))

	for _, input := range tx.Inputs {
		fees += input.PreviousTxSatoshis
	}

	for i, output := range tx.Outputs {
		if output.Satoshis > 0 {
			fees -= output.Satoshis

			utxoHash, utxoErr := util.UTXOHashFromOutput(txIDChainHash, output, uint32(i))
			if utxoErr != nil {
				fmt.Printf("error getting output utxo hash: %s", utxoErr.Error())
				//return err
			}

			utxoHashes = append(utxoHashes, utxoHash)
		}
	}

	// TODO what if one of these fails?
	// we should probably recover and add it to a retry queue

	// register transaction in tx status store
	if err = v.txStatus.Create(ctx, txIDChainHash, fees, parentTxHashes, utxoHashes); err != nil {
		v.logger.Errorf("error sending tx to txstatus: %v", err)
	}

	if v.kafkaProducer != nil {
		if err = v.publishToKafka(txIDChainHash); err != nil {
			v.logger.Errorf("error sending tx to kafka: %v", err)
		}
	} else {
		if _, err = v.blockAssembler.Store(ctx, txIDChainHash); err != nil {
			v.logger.Errorf("error sending tx to block assembler: %v", err)
		}
	}

	// if v.saveInParallel {
	// 	var wg sync.WaitGroup
	// 	for i, output := range tx.Outputs {
	// 		if output.Satoshis > 0 {
	// 			i := i
	// 			output := output
	// 			wg.Add(1)
	// 			go func() {
	// 				defer wg.Done()

	// 				utxoHash, utxoErr := util.UTXOHashFromOutput(txIDChainHash, output, uint32(i))
	// 				if utxoErr != nil {
	// 					fmt.Printf("error getting output utxo hash: %s", utxoErr.Error())
	// 					//return err
	// 				}

	// 				_, utxoErr = v.store.Client(storeUtxoSpan.Ctx, utxoHash)
	// 				if utxoErr != nil {
	// 					fmt.Printf("error storing utxo: %s\n", utxoErr.Error())
	// 				}
	// 			}()
	// 		}
	// 	}
	// 	wg.Wait()
	// } else {
	// 	for i, output := range tx.Outputs {
	// 		if output.Satoshis > 0 {
	// 			hash, err = util.UTXOHashFromOutput(txIDChainHash, output, uint32(i))
	// 			if err != nil {
	// 				return err
	// 			}

	// 			_, err = v.store.Client(storeUtxoSpan.Ctx, hash)
	// 			if err != nil {
	// 				break
	// 			}
	// 		}
	// 	}
	// }

	return nil
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
