// Package utxo provides UTXO (Unspent Transaction Output) management for the Bitcoin SV Teranode implementation.
package utxo

import (
	"encoding/binary"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/stores/utxo/spend"
	"github.com/bitcoin-sv/teranode/util"
)

// SpendResponse contains the response from querying (GetSpend) a UTXO's spend status.
type SpendResponse struct {
	// Status indicates the current state of the UTXO
	Status int `json:"status"`

	// SpendingData contains information about the transaction that spent this UTXO, if any
	SpendingData *spend.SpendingData `json:"spendingData,omitempty"`

	// LockTime is the block height or timestamp until which this UTXO is locked
	LockTime uint32 `json:"lockTime,omitempty"`
}

// Bytes serializes the SpendResponse into a byte slice.
// The format is: [8 bytes status][4 bytes locktime][4 bytes spendingTxVin][32 bytes spendingTxID (optional)]
func (sr *SpendResponse) Bytes() []byte {
	buf := make([]byte, 0, 8+4+4+32)

	uint32Bytes := make([]byte, 4)
	uint64Bytes := make([]byte, 8)

	// write status to bytes
	//nolint:gosec
	binary.LittleEndian.PutUint64(uint64Bytes, uint64(sr.Status))
	buf = append(buf, uint64Bytes...)

	// write locktime to bytes
	binary.LittleEndian.PutUint32(uint32Bytes, sr.LockTime)
	buf = append(buf, uint32Bytes...)

	// write spendingData to bytes
	if sr.SpendingData != nil {
		buf = append(buf, sr.SpendingData.Bytes()...)
	}

	return buf
}

// FromBytes deserializes a SpendResponse from a byte slice.
// Returns an error if the byte slice is invalid or too short.
func (sr *SpendResponse) FromBytes(b []byte) (err error) {
	if len(b) < 12 {
		return errors.NewInvalidArgumentError("invalid byte length")
	}

	intFirstEightBytes, err := util.SafeUint64ToInt(binary.LittleEndian.Uint64(b[:8]))
	if err != nil {
		return errors.NewProcessingError("failed to convert first eight bytes", err)
	}

	sr.Status = intFirstEightBytes

	sr.LockTime = binary.LittleEndian.Uint32(b[8:12])

	if len(b) > 12 {
		sr.SpendingData, err = spend.NewSpendingDataFromBytes(b[12:])
		if err != nil {
			return errors.NewProcessingError("failed to convert spending data", err)
		}
	}

	return nil
}
