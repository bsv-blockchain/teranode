package errors

import (
	"encoding/json"
	"fmt"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	spendpkg "github.com/bsv-blockchain/teranode/stores/utxo/spend"
)

// UtxoSpentErrData is the error data structure for UTXO spent errors.
type UtxoSpentErrData struct {
	Hash         chainhash.Hash         `json:"hash"`
	SpendingData *spendpkg.SpendingData `json:"spending_data"`
	UtxoHash     chainhash.Hash         `json:"utxo_hash"`
	Vout         uint32                 `json:"vout"`
}

// SetData sets the data for the UtxoSpentErrData structure.
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

// GetData retrieves the data for the UtxoSpentErrData structure based on the key.
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

// Error returns a string representation of the UtxoSpentErrData error.
func (e *UtxoSpentErrData) Error() string {
	return fmt.Sprintf("utxo %s already spent by %v", e.Hash, e.SpendingData)
}

// EncodeErrorData encodes the UtxoSpentErrData to a byte slice using JSON encoding.
func (e *UtxoSpentErrData) EncodeErrorData() []byte {
	// marshal the data to a byte slice using the encoding/json package
	data, err := json.Marshal(e)
	if err != nil {
		// Note: Check if we should log this
		return []byte{}
	}

	return data
}

// NewUtxoSpentError creates a new UTXO spent error with the given transaction ID, output index, UTXO hash, and spending data.
func NewUtxoSpentError(txID chainhash.Hash, vOut uint32, utxoHash chainhash.Hash, spendingData *spendpkg.SpendingData) *Error {
	utxoSpentErrStruct := &UtxoSpentErrData{
		Hash:         txID,
		Vout:         vOut,
		UtxoHash:     utxoHash,
		SpendingData: spendingData,
	}

	// No code prefix here. Error() and UserMessage already prepend errCodeMsgFmt,
	// so rendering it into the message too produced "UTXO_SPENT (70): UTXO_SPENT
	// (70): ..." and, worse, made the code part of Message() rather than around
	// it - which put an internal enum on the JSON-RPC wire, where the boundary
	// reads Message() precisely because it never renders codes.
	utxoSpentError := New(ERR_UTXO_SPENT, "%s:%d utxo already spent by tx %v", txID.String(), vOut, spendingData)
	utxoSpentError.data = utxoSpentErrStruct

	return utxoSpentError
}
