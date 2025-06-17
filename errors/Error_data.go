package errors

import (
	"encoding/json"
	"fmt"
)

// ErrDataI is an interface for error data that can be set, retrieved, and encoded.
type ErrDataI interface {
	EncodeErrorData() []byte
	Error() string
	GetData(key string) interface{}
	SetData(key string, value interface{})
}

// ErrData is a generic error data structure that implements the ErrDataI interface.
type ErrData map[string]interface{}

// Error returns a string representation of the error data.
func (e *ErrData) Error() string {
	return fmt.Sprintf(" %v", *e)
}

// SetData sets a key-value pair in the error data.
func (e *ErrData) SetData(key string, value interface{}) {
	if e == nil {
		return
	}

	(*e)[key] = value
}

// GetData retrieves the value associated with a key in the error data.
func (e *ErrData) GetData(key string) interface{} {
	if e == nil {
		return nil
	}

	return (*e)[key]
}

// EncodeErrorData encodes the error data to a byte slice using JSON encoding.
func (e *ErrData) EncodeErrorData() []byte {
	// marshal the data to a byte slice using the encoding/json package
	data, err := json.Marshal(e)
	if err != nil {
		// Note: Check if we should log this
		return []byte{}
	}

	return data
}

// GetErrorData retrieves error data based on the error code and unmarshals it from a byte slice.
func GetErrorData(code ERR, dataBytes []byte) (ErrDataI, error) {
	var errData ErrDataI

	switch code {
	case ERR_UTXO_SPENT:
		errData = &UtxoSpentErrData{}
		// unmarshall the data from the byte slice using the encoding/json package
		err := json.Unmarshal(dataBytes, errData)
		if err != nil {
			return errData, err
		}

	default:
		// get generic error data
		errData = &ErrData{}

		// unmarshall the data from the byte slice using the encoding/json package
		err := json.Unmarshal(dataBytes, errData)
		if err != nil {
			return errData, err
		}
	}

	return errData, nil
}
