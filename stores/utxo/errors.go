package utxo

import (
	"fmt"
	"time"

	"github.com/bitcoin-sv/ubsv/ubsverrors"
	"github.com/libsv/go-bt/v2/chainhash"
)

const (
	isoFormat = "2006-01-02T15:04:05Z"
)

var (
	ErrNotFound      = ubsverrors.New(ubsverrors.ErrorConstants_NOT_FOUND, "utxo not found")
	ErrAlreadyExists = ubsverrors.New(0, "utxo already exists")
	ErrTypeSpent     = &ErrSpent{}
	ErrTypeLockTime  = &ErrLockTime{}
	ErrChainHash     = ubsverrors.New(0, "utxo chain hash could not be calculated")
	ErrStore         = ubsverrors.New(0, "utxo store error")
)

type ErrSpent struct {
	SpendingTxID *chainhash.Hash
}

func NewErrSpent(spendingTxID *chainhash.Hash, optionalErrs ...error) error {
	errSpent := &ErrSpent{
		SpendingTxID: spendingTxID,
	}

	e := ubsverrors.New(0, errSpent.Error(), ErrTypeSpent)
	return e
}

func (e *ErrSpent) Error() string {
	if e.SpendingTxID == nil {
		return "utxo already spent (invalid use of ErrSpent as spendingTxID is not set)"
	}
	return fmt.Sprintf("utxo already spent by txid %s", e.SpendingTxID.String())
}

type ErrLockTime struct {
	lockTime    uint32
	blockHeight uint32
}

func NewErrLockTime(lockTime uint32, blockHeight uint32, optionalErrs ...error) error {
	errLockTime := &ErrLockTime{
		lockTime:    lockTime,
		blockHeight: blockHeight,
	}

	return ubsverrors.New(0, errLockTime.Error(), ErrTypeLockTime)
}
func (e *ErrLockTime) Error() string {
	if e.lockTime == 0 {
		return "utxo is locked (invalid use of ErrLockTime as locktime is zero)"
	}

	if e.lockTime >= 500_000_000 {
		// This is a timestamp based locktime
		spendableAt := time.Unix(int64(e.lockTime), 0)
		return fmt.Sprintf("utxo is locked until %s", spendableAt.UTC().Format(isoFormat))
	}
	return fmt.Sprintf("utxo is locked until block %d (height check: %d)", e.lockTime, e.blockHeight)
}
