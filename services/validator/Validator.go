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
	"go.opentelemetry.io/otel"
)

type Validator struct {
	store store.UTXOStore
}

func New(store store.UTXOStore) Interface {
	return &Validator{
		store: store,
	}
}

func (v *Validator) Validate(ctx context.Context, tx *bt.Tx) error {
	ctx, span := otel.Tracer("").Start(ctx, "Validator:Validate")
	defer span.End()

	if tx.IsCoinbase() {
		coinbaseCtx, coinbaseSpan := otel.Tracer("").Start(ctx, "Validator:Validate:IsCoinbase")
		defer coinbaseSpan.End()

		// TODO what checks do we need to do on a coinbase tx?
		// not just anyone should be able to send a coinbase tx through the system
		hash, err := utxo.GetOutputUtxoHash(bt.ReverseBytes(tx.TxIDBytes()), tx.Outputs[0], 0)
		if err != nil {
			return err
		}

		// store the coinbase utxo
		// TODO this should be marked as spendable only after 100 blocks
		_, err = v.store.Store(coinbaseCtx, hash)
		if err != nil {
			return err
		}

		return nil
	}

	_, basicSpan := otel.Tracer("").Start(ctx, "Validator:Validate:Basic")

	// check all the basic stuff
	// TODO this is using the ARC validator, but should be moved into a separate package or imported to this one
	validator := defaultvalidator.New(&bitcoin.Settings{})
	// this will also check whether the transaction is in extended format

	if err := validator.ValidateTransaction(tx); err != nil {
		basicSpan.End()
		return err
	}
	basicSpan.End()

	utxoCtx, utxoSpan := otel.Tracer("").Start(ctx, "Validator:Validate:CheckUtxos")

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
		hash, err = utxo.GetInputUtxoHash(input)
		if err != nil {
			utxoSpan.RecordError(err)
			return err
		}

		// TODO Should we be doing this in a batch?
		utxoResponse, err = v.store.Spend(utxoCtx, hash, txIDChainHash)
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
		reverseUtxoCtx, reverseUtxoSpan := otel.Tracer("").Start(ctx, "Validator:Validate:ReverseUtxos")
		defer func() {
			reverseUtxoSpan.End()
			utxoSpan.End()
		}()

		// Revert all the spends
		for _, hash = range reservedUtxos {
			if _, err = v.store.Reset(reverseUtxoCtx, hash); err != nil {
				reverseUtxoSpan.RecordError(err)
			}
		}

		return err
	}
	utxoSpan.End()

	// process the outputs of the transaction into new spendable outputs
	storeUtxoCtx, storeUtxoSpan := otel.Tracer("").Start(ctx, "Validator:Validate:StoreUtxos")
	defer storeUtxoSpan.End()

	for i, output := range tx.Outputs {
		if output.Satoshis > 0 {
			hash, err = utxo.GetOutputUtxoHash(txIDBytes, output, uint64(i))
			if err != nil {
				return err
			}

			_, err = v.store.Store(storeUtxoCtx, hash)
			if err != nil {
				break
			}
		}
	}

	return nil
}
