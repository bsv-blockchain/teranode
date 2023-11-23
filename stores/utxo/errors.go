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
	ErrNotFound      = ubsverrors.New("utxo not found")
	ErrAlreadyExists = ubsverrors.New("utxo already exists")
	ErrTypeSpent     = ubsverrors.New("utxo already spent")
	ErrTypeLockTime  = ubsverrors.New("utxo is locked")
	ErrChainHash     = ubsverrors.New("utxo chain hash could not be calculated")
	ErrStore         = ubsverrors.New("utxo store error")
)

type ErrSpent struct {
	err          *ubsverrors.Error
	spendingTxID *chainhash.Hash
}

func NewErrSpent(spendingTxID *chainhash.Hash, optionalErrs ...error) error {

	e := &ErrSpent{
		err:          ErrTypeSpent,
		spendingTxID: spendingTxID,
	}

	_ = e.err.Wrap(optionalErrs...)

	return e
}

func (e *ErrSpent) Error() string {
	if e.spendingTxID == nil {
		return fmt.Sprintf("%s (invalid use of ErrSpent as spendingTxID is not set)", e.err.Error())
	}
	return fmt.Sprintf("%s by txid %s", e.err, e.spendingTxID.String())
}

func (e *ErrSpent) Unwrap() error {
	return e.err
}

type ErrLockTime struct {
	err         *ubsverrors.Error
	lockTime    uint32
	blockHeight uint32
}

func NewErrLockTime(lockTime uint32, blockHeight uint32) error {
	return &ErrLockTime{
		err:         ErrTypeLockTime,
		lockTime:    lockTime,
		blockHeight: blockHeight,
	}
}
func (e *ErrLockTime) Error() string {
	if e.lockTime == 0 {
		return fmt.Sprintf("%s (invalid use of ErrLockTime as locktime is zero)", e.err.Error())
	}

	if e.lockTime >= 500000000 {
		// This is a timestamp based locktime
		spendableAt := time.Unix(int64(e.lockTime), 0)
		return fmt.Sprintf("%s until %s", e.err, spendableAt.UTC().Format(isoFormat))
	}
	return fmt.Sprintf("%s until block %d (height check: %d)", e.err, e.lockTime, e.blockHeight)
}

func (e *ErrLockTime) Unwrap() error {
	return e.err
}
