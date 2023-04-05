package validator

import (
	store "github.com/TAAL-GmbH/ubsv/services/validator/utxostore"
	"github.com/libsv/go-bt/v2"
)

type Validator struct {
	store store.UTXOStore
}

func New(store store.UTXOStore) *Validator {
	return &Validator{
		store: store,
	}
}

func (v *Validator) Validate(tx bt.Tx) error {
	return nil
}
