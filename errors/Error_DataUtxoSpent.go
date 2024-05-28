package errors

import (
	"fmt"
	"time"

	"github.com/libsv/go-bt/v2/chainhash"
)

type UtxoSpentErrData struct {
	Hash           chainhash.Hash
	SpendingTxHash chainhash.Hash
	Time           time.Time
}

func (e *UtxoSpentErrData) Error() string {
	return fmt.Sprintf("utxo %s already spent by %s at %s", e.Hash, e.SpendingTxHash, e.Time)
}

func NewUtxoSpentErr(txID chainhash.Hash, spendingTxID chainhash.Hash, t time.Time, err error) *Error {
	utxoSpentErrStruct := &UtxoSpentErrData{
		Hash:           txID,
		SpendingTxHash: spendingTxID,
		Time:           t,
	}

	e := New(ERR_TX_ALREADY_EXISTS, "utxoSpentErrStruct.Error()", err)
	e.Data = utxoSpentErrStruct
	return e
}
