package utxo

import (
	"fmt"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/libsv/go-bt/v2/chainhash"
)

const (
	isoFormat = "2006-01-02T15:04:05Z"
)

func NewErrSpent(txID *chainhash.Hash, vOut uint32, utxoHash, spendingTxID *chainhash.Hash, optionalErrs ...error) error {
	txIDString := "nil"
	if txID != nil {
		txIDString = txID.String()
	}
	utxoHashString := "nil"
	if utxoHash != nil {
		utxoHashString = utxoHash.String()
	}
	spendingTxString := "nil"
	if spendingTxID != nil {
		spendingTxString = spendingTxID.String()
	}
	return errors.NewSpentError("%s:$%d utxo %s already spent by txid %s", txIDString, vOut, utxoHashString, spendingTxString)
}

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

	return errors.NewLockTimeError(errorString)
}
