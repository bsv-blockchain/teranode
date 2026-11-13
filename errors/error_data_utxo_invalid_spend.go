package errors

import (
	"encoding/json"
	"fmt"

	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
)

// UtxoInvalidSpendErrData is the error data of a replayed spend that the store
// rejects as invalid. SpendingData is the stored owner, which equals the claim
// that was rejected.
type UtxoInvalidSpendErrData struct {
	SpendingData *spendpkg.SpendingData `json:"spending_data"`
}

// SetData sets the data for the UtxoInvalidSpendErrData structure.
func (e *UtxoInvalidSpendErrData) SetData(key string, value interface{}) {
	if key == "spending_data" {
		e.SpendingData, _ = value.(*spendpkg.SpendingData)
	}
}

// GetData retrieves the data for the UtxoInvalidSpendErrData structure based on the key.
func (e *UtxoInvalidSpendErrData) GetData(key string) interface{} {
	if key == "spending_data" {
		return e.SpendingData
	}

	return nil
}

// Error returns a string representation of the UtxoInvalidSpendErrData error.
func (e *UtxoInvalidSpendErrData) Error() string {
	return fmt.Sprintf("invalid spend, stored owner %v", e.SpendingData)
}

// EncodeErrorData encodes the UtxoInvalidSpendErrData to a byte slice using JSON encoding.
func (e *UtxoInvalidSpendErrData) EncodeErrorData() []byte {
	data, err := json.Marshal(e)
	if err != nil {
		return []byte{}
	}

	return data
}

// NewUtxoInvalidSpendError creates a UTXO error that carries the stored owner of the outpoint.
func NewUtxoInvalidSpendError(spendingData *spendpkg.SpendingData, message string, params ...interface{}) *Error {
	invalidSpendError := NewUtxoError(message, params...)
	invalidSpendError.data = &UtxoInvalidSpendErrData{SpendingData: spendingData}

	return invalidSpendError
}
