package utxo

import (
	"errors"
	"fmt"
	"time"

	"github.com/libsv/go-bt/v2/chainhash"
)

const (
	isoFormat = "2006-01-02T15:04:05Z"
)

var (
	ErrNotFound      = errors.New("utxo not found")
	ErrAlreadyExists = errors.New("utxo already exists")
	ErrSpent         = errors.New("utxo already spent")
	ErrLockTime      = errors.New("utxo not spendable")
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
		return fmt.Sprintf("%s (invalid use of ErrSpentExtra as spendingTxID is not set)", e.Err.Error())
	}
	return fmt.Sprintf("%s by txid %s", e.Err, e.SpendingTxID.String())
}

func (e *ErrSpentExtra) Unwrap() error {
	return e.Err
}

type ErrLockTimeExtra struct {
	Err         error
	LockTime    uint32
	BlockHeight uint32
}

func NewErrLockTimeExtra(lockTime uint32, blockHeight uint32) *ErrLockTimeExtra {
	return &ErrLockTimeExtra{
		Err:         ErrLockTime,
		LockTime:    lockTime,
		BlockHeight: blockHeight,
	}
}
func (e *ErrLockTimeExtra) Error() string {
	if e.LockTime == 0 {
		return fmt.Sprintf("%s (invalid use of ErrLockTimeExtra as locktime is zero)", e.Err.Error())
	}

	if e.LockTime >= 500000000 {
		// This is a timestamp based locktime
		spendableAt := time.Unix(int64(e.LockTime), 0)
		return fmt.Sprintf("%s until %s", e.Err, spendableAt.UTC().Format(isoFormat))
	}
	return fmt.Sprintf("%s until block %d (height check: %d)", e.Err, e.LockTime, e.BlockHeight)
}

func (e *ErrLockTimeExtra) Unwrap() error {
	return e.Err
}
