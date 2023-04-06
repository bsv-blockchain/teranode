package validator

import (
	"context"
	"fmt"

	defaultvalidator "github.com/TAAL-GmbH/arc/validator/default"
	store "github.com/TAAL-GmbH/ubsv/services/validator/utxostore"
	"github.com/libsv/go-bk/crypto"
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
		hash, err := getOutputUtxoHash(tx.TxIDBytes(), tx.Outputs[0], 0)
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
	reservedUtxos := make([]*chainhash.Hash, 0, len(tx.Inputs))
	for _, input := range tx.Inputs {
		hash, err = getInputUtxoHash(input)
		if err != nil {
			return err
		}

		// TODO Should we be doing this in a batch?
		_, err = v.store.Spend(context.Background(), hash, txIDChainHash)
		if err != nil {
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

	return nil
}

func getInputUtxoHash(input *bt.Input) (*chainhash.Hash, error) {
	voutBytes := bt.VarInt(input.PreviousTxOutIndex).Bytes()

	if input.PreviousTxScript == nil || len(*input.PreviousTxScript) == 0 {
		return nil, fmt.Errorf("previous tx script is nil")
	}
	previousScript := []byte(*input.PreviousTxScript)

	if input.PreviousTxSatoshis == 0 {
		return nil, fmt.Errorf("previous tx satoshis is 0")
	}
	previousSatoshis := bt.VarInt(input.PreviousTxSatoshis).Bytes()

	utxoHashBytes := append(input.PreviousTxID(), voutBytes...)
	utxoHashBytes = append(utxoHashBytes, previousScript...)
	utxoHashBytes = append(utxoHashBytes, previousSatoshis...)

	hash := crypto.Sha256(utxoHashBytes)
	chHash, err := chainhash.NewHash(hash)
	if err != nil {
		return nil, err
	}

	return chHash, nil
}

func getOutputUtxoHash(txID []byte, output *bt.Output, vOut uint64) (*chainhash.Hash, error) {
	voutBytes := bt.VarInt(vOut).Bytes()

	if output.LockingScript == nil || len(*output.LockingScript) == 0 {
		return nil, fmt.Errorf("output script is nil")
	}
	outputScript := []byte(*output.LockingScript)

	if output.Satoshis == 0 {
		return nil, fmt.Errorf("satoshis is 0")
	}
	satoshiBytes := bt.VarInt(output.Satoshis).Bytes()

	utxoHashBytes := append(txID, voutBytes...)
	utxoHashBytes = append(utxoHashBytes, outputScript...)
	utxoHashBytes = append(utxoHashBytes, satoshiBytes...)

	hash := crypto.Sha256(utxoHashBytes)
	chHash, err := chainhash.NewHash(hash)
	if err != nil {
		return nil, err
	}

	return chHash, nil
}
