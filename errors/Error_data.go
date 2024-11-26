package errors

import (
	"encoding/json"
	"fmt"
)

type ErrDataI interface {
	Error() string
	SetData(key string, value interface{})
	GetData(key string) interface{}
	EncodeErrorData() []byte
}

type ErrData map[string]interface{}

func (e *ErrData) Error() string {
	return fmt.Sprintf(" %v", *e)
}

func (e *ErrData) SetData(key string, value interface{}) {
	if e == nil {
		return
	}

	(*e)[key] = value
}

func (e *ErrData) GetData(key string) interface{} {
	if e == nil {
		return nil
	}

	return (*e)[key]
}

func (e *ErrData) EncodeErrorData() []byte {
	// marshal the data to a byte slice using the encoding/json package
	data, err := json.Marshal(e)
	if err != nil {
		// Note: Check if we should log this
		return []byte{}
	}

	return data
}

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
