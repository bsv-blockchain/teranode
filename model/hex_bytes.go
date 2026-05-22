package model

import (
	"encoding/hex"

	"github.com/bsv-blockchain/teranode/errors"
)

// HexBytes is a []byte that marshals to/from JSON as a hex-encoded string.
type HexBytes []byte

func (h HexBytes) MarshalJSON() ([]byte, error) {
	return []byte(`"` + hex.EncodeToString(h) + `"`), nil
}

func (h *HexBytes) UnmarshalJSON(data []byte) error {
	if len(data) < 2 || data[0] != '"' || data[len(data)-1] != '"' {
		return errors.NewInvalidArgumentError("HexBytes must be a JSON string")
	}
	b, err := hex.DecodeString(string(data[1 : len(data)-1]))
	if err != nil {
		return err
	}
	*h = b
	return nil
}
