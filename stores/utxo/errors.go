package utxo

import (
	"fmt"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
)

const (
	isoFormat = "2006-01-02T15:04:05Z"
	nilString = "nil"
)

func NewErrLockTime(lockTime uint32, blockHeight uint32, optionalErrs ...error) error {
	var errorString string

	switch {
	case lockTime == 0:
		errorString = "ErrLockTime"
	case lockTime < 500_000_000:
		// This is a timestamp based locktime
		spendableAt := time.Unix(int64(lockTime), 0)
		errorString = fmt.Sprintf("utxo is locked until %s", spendableAt.UTC().Format(isoFormat))
	default:
		errorString = fmt.Sprintf("utxo is locked until block %d (height check: %d)", lockTime, blockHeight)
	}

	return errors.NewTxLockTimeError(errorString)
}
