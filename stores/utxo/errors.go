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
	ErrNotFound      = ubsverrors.NewErrString("utxo not found")
	ErrAlreadyExists = ubsverrors.NewErrString("utxo already exists")
	ErrTypeSpent     = &ErrSpent{}
	ErrTypeLockTime  = &ErrLockTime{}
	ErrChainHash     = ubsverrors.NewErrString("utxo chain hash could not be calculated")
	ErrStore         = ubsverrors.NewErrString("utxo store error")
)

type ErrSpent struct {
	spendingTxID *chainhash.Hash
}

func NewErrSpent(spendingTxID *chainhash.Hash, optionalErrs ...error) error {
	e := ubsverrors.Wrap(&ErrSpent{
		spendingTxID: spendingTxID,
	}, optionalErrs...)

	return e
}

func (e *ErrSpent) Error() string {
	if e.spendingTxID == nil {
		return fmt.Sprintf("utxo already spent (invalid use of ErrSpent as spendingTxID is not set)")
	}
	return fmt.Sprintf("utxo already spent by txid %s", e.spendingTxID.String())
}

type ErrLockTime struct {
	lockTime    uint32
	blockHeight uint32
}

func NewErrLockTime(lockTime uint32, blockHeight uint32, optionalErrs ...error) error {
	return ubsverrors.Wrap(&ErrLockTime{
		lockTime:    lockTime,
		blockHeight: blockHeight,
	}, optionalErrs...)
}
func (e *ErrLockTime) Error() string {
	if e.lockTime == 0 {
		return fmt.Sprintf("utxo is locked (invalid use of ErrLockTime as locktime is zero)")
	}

	if e.lockTime >= 500000000 {
		// This is a timestamp based locktime
		spendableAt := time.Unix(int64(e.lockTime), 0)
		return fmt.Sprintf("utxo is locked until %s", spendableAt.UTC().Format(isoFormat))
	}
	return fmt.Sprintf("utxo is locked until block %d (height check: %d)", e.lockTime, e.blockHeight)
}
