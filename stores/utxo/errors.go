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
	ErrNotFound      = ubsverrors.New(ubsverrors.ERR_NOT_FOUND, "utxo not found")
	ErrAlreadyExists = ubsverrors.New(0, "utxo already exists")
	ErrTypeSpent     = &ErrSpent{}
	ErrTypeLockTime  = &ErrLockTime{}
	ErrChainHash     = ubsverrors.New(0, "utxo chain hash could not be calculated")
	ErrStore         = ubsverrors.New(0, "utxo store error")
)

type ErrSpent struct {
	TxID         *chainhash.Hash
	VOut         uint32
	UtxoHash     *chainhash.Hash
	SpendingTxID *chainhash.Hash
}

func NewErrSpent(txID *chainhash.Hash, vOut uint32, utxoHash, spendingTxID *chainhash.Hash, optionalErrs ...error) error {
	errSpent := &ErrSpent{
		TxID:         txID,
		VOut:         vOut,
		UtxoHash:     utxoHash,
		SpendingTxID: spendingTxID,
	}

	e := ubsverrors.New(0, errSpent.Error(), ErrTypeSpent)
	return e
}

func (e *ErrSpent) Error() string {
	return fmt.Sprintf("%s:$%d utxo %s already spent by txid %s", e.TxID.String(), e.VOut, e.UtxoHash.String(), e.SpendingTxID.String())
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
		return "ErrLockTime"
	}

	if e.lockTime >= 500_000_000 {
		// This is a timestamp based locktime
		spendableAt := time.Unix(int64(e.lockTime), 0)
		return fmt.Sprintf("utxo is locked until %s", spendableAt.UTC().Format(isoFormat))
	}
	return fmt.Sprintf("utxo is locked until block %d (height check: %d)", e.lockTime, e.blockHeight)
}
