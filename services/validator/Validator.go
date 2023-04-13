package validator

import (
	"context"
	"fmt"

	defaultvalidator "github.com/TAAL-GmbH/arc/validator/default"
	"github.com/TAAL-GmbH/ubsv/services/utxo"
	"github.com/TAAL-GmbH/ubsv/services/utxo/store"
	"github.com/TAAL-GmbH/ubsv/services/utxo/utxostore_api"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-bitcoin"
)

type Validator struct {
	store store.UTXOStore
}

func New(store store.UTXOStore) Interface {
	return &Validator{
		store: store,
	}
}

func (v *Validator) Validate(tx *bt.Tx) error {
	if tx.IsCoinbase() {
		// TODO what checks do we need to do on a coinbase tx?
		// not just anyone should be able to send a coinbase tx through the system
		hash, err := utxo.GetOutputUtxoHash(bt.ReverseBytes(tx.TxIDBytes()), tx.Outputs[0], 0)
		if err != nil {
			return err
		}

		// store the coinbase utxo
		// TODO this should be marked as spendable only after 100 blocks
		_, err = v.store.Store(context.Background(), hash)
		if err != nil {
			return err
		}

		return nil
	}

	// check all the basic stuff
	// TODO this is using the ARC validator, but should be moved into a separate package or imported to this one
	validator := defaultvalidator.New(&bitcoin.Settings{})
	// this will also check whether the transaction is in extended format

	if err := validator.ValidateTransaction(tx); err != nil {
		return err
	}

	// check the utxos
	txIDBytes := bt.ReverseBytes(tx.TxIDBytes())
	txIDChainHash, err := chainhash.NewHash(txIDBytes)
	if err != nil {
		return err
	}

	var hash *chainhash.Hash
	var utxoResponse *store.UTXOResponse
	reservedUtxos := make([]*chainhash.Hash, 0, len(tx.Inputs))
	for _, input := range tx.Inputs {
		hash, err = utxo.GetInputUtxoHash(input)
		if err != nil {
			return err
		}

		// TODO Should we be doing this in a batch?
		utxoResponse, err = v.store.Spend(context.Background(), hash, txIDChainHash)
		if err != nil {
			break
		}
		if utxoResponse.Status != int(utxostore_api.Status_OK) {
			err = fmt.Errorf("utxo %x is not spendable", bt.ReverseBytes(hash[:]))
			break
		}

		reservedUtxos = append(reservedUtxos, hash)
	}

	if err != nil {
		// Revert all the spends
		for _, hash = range reservedUtxos {
			_, err = v.store.Reset(context.Background(), hash)
		}

		return err
	}

	// process the outputs of the transaction into new spendable outputs
	for i, output := range tx.Outputs {
		if output.Satoshis > 0 {
			hash, err = utxo.GetOutputUtxoHash(txIDBytes, output, uint64(i))
			if err != nil {
				return err
			}

			_, err = v.store.Store(context.Background(), hash)
			if err != nil {
				break
			}
		}
	}

	return nil
}
