package errors

import (
	"encoding/json"
	"fmt"

	spendpkg "github.com/bitcoin-sv/teranode/stores/utxo/spend"
	"github.com/libsv/go-bt/v2/chainhash"
)

type UtxoSpentErrData struct {
	Hash         chainhash.Hash         `json:"hash"`
	Vout         uint32                 `json:"vout"`
	UtxoHash     chainhash.Hash         `json:"utxo_hash"`
	SpendingData *spendpkg.SpendingData `json:"spending_data"`
}

func (e *UtxoSpentErrData) SetData(key string, value interface{}) {
	switch key {
	case "hash":
		e.Hash = value.(chainhash.Hash)
	case "vout":
		e.Vout = value.(uint32)
	case "utxo_hash":
		e.UtxoHash = value.(chainhash.Hash)
	case "spending_data":
		e.SpendingData = value.(*spendpkg.SpendingData)
	}
}

func (e *UtxoSpentErrData) GetData(key string) interface{} {
	switch key {
	case "hash":
		return e.Hash
	case "vout":
		return e.Vout
	case "utxo_hash":
		return e.UtxoHash
	case "spending_data":
		return e.SpendingData
	}

	return nil
}

func (e *UtxoSpentErrData) Error() string {
	return fmt.Sprintf("utxo %s already spent by %v", e.Hash, e.SpendingData)
}

func (e *UtxoSpentErrData) EncodeErrorData() []byte {
	// marshal the data to a byte slice using the encoding/json package
	data, err := json.Marshal(e)
	if err != nil {
		// Note: Check if we should log this
		return []byte{}
	}

	return data
}

func NewUtxoSpentError(txID chainhash.Hash, vOut uint32, utxoHash chainhash.Hash, spendingData *spendpkg.SpendingData) *Error {
	utxoSpentErrStruct := &UtxoSpentErrData{
		Hash:         txID,
		Vout:         vOut,
		UtxoHash:     utxoHash,
		SpendingData: spendingData,
	}

	utxoSpentError := New(ERR_UTXO_SPENT, "%s (%d): %s:%d utxo already spent by tx %v", ERR_UTXO_SPENT.Enum(), ERR_UTXO_SPENT, txID.String(), vOut, spendingData)
	utxoSpentError.data = utxoSpentErrStruct

	return utxoSpentError
}
