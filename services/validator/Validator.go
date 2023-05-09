package validator

import (
	"context"
	"fmt"
	"sync"

	defaultvalidator "github.com/TAAL-GmbH/arc/validator/default"
	"github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	"github.com/TAAL-GmbH/ubsv/tracing"
	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-bitcoin"
)

type Validator struct {
	store          store.UTXOStore
	saveInParallel bool
}

func New(store store.UTXOStore) Interface {
	return &Validator{
		store:          store,
		saveInParallel: true,
	}
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
	var utxoResponse *store.UTXOResponse
	reservedUtxos := make([]*chainhash.Hash, 0, len(tx.Inputs))
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

	if v.saveInParallel {
		var wg sync.WaitGroup
		for i, output := range tx.Outputs {
			if output.Satoshis > 0 {
				i := i
				output := output
				wg.Add(1)
				go func() {
					defer wg.Done()

					utxoHash, utxoErr := util.UTXOHashFromOutput(txIDChainHash, output, uint32(i))
					if utxoErr != nil {
						fmt.Printf("error getting output utxo hash: %s", utxoErr.Error())
						//return err
					}

					_, utxoErr = v.store.Store(storeUtxoSpan.Ctx, utxoHash)
					if utxoErr != nil {
						fmt.Printf("error storing utxo: %s\n", utxoErr.Error())
					}
				}()
			}
		}
		wg.Wait()
	} else {
		for i, output := range tx.Outputs {
			if output.Satoshis > 0 {
				hash, err = util.UTXOHashFromOutput(txIDChainHash, output, uint32(i))
				if err != nil {
					return err
				}

				_, err = v.store.Store(storeUtxoSpan.Ctx, hash)
				if err != nil {
					break
				}
			}
		}
	}

	return nil
}
