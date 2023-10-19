package utxo

import (
	"errors"
	"fmt"

	"github.com/libsv/go-bt/v2/chainhash"
)

var (
	ErrNotFound      = errors.New("utxo not found")
	ErrAlreadyExists = errors.New("utxo already exists")
	ErrSpent         = errors.New("utxo already spent")
	ErrLockTime      = errors.New("utxo not spendable yet, due to lock time")
	ErrChainHash     = errors.New("utxo chain hash could not be calculated")
	ErrStore         = errors.New("utxo store error")
)

type ErrSpentExtra struct {
	Err          error
	SpendingTxID *chainhash.Hash
}

func NewErrSpentExtra(spendingTxID *chainhash.Hash) *ErrSpentExtra {
	return &ErrSpentExtra{
		Err:          ErrSpent,
		SpendingTxID: spendingTxID,
	}
}
func (e *ErrSpentExtra) Error() string {
	if e.SpendingTxID == nil {
		return e.Err.Error()
	}
	return fmt.Sprintf("%s: %s", e.Err, e.SpendingTxID.String())
}

func (e *ErrSpentExtra) Unwrap() error {
	return e.Err
}
