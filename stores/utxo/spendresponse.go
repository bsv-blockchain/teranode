package utxo

import (
	"encoding/binary"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/libsv/go-bt/v2/chainhash"
)

// SpendResponse is a struct that holds the response from the GetSpend function
type SpendResponse struct {
	Status       int             `json:"status"`
	SpendingTxID *chainhash.Hash `json:"spendingTxId,omitempty"`
	LockTime     uint32          `json:"lockTime,omitempty"`
}

func (sr *SpendResponse) Bytes() []byte {
	buf := make([]byte, 0, 8+32+4)

	uint32Bytes := make([]byte, 4)
	uint64Bytes := make([]byte, 8)

	// write status to bytes
	//nolint:gosec
	binary.LittleEndian.PutUint64(uint64Bytes, uint64(sr.Status))
	buf = append(buf, uint64Bytes...)

	// write locktime to bytes
	binary.LittleEndian.PutUint32(uint32Bytes, sr.LockTime)
	buf = append(buf, uint32Bytes...)

	// write spending txid to bytes
	if sr.SpendingTxID != nil {
		buf = append(buf, sr.SpendingTxID.CloneBytes()...)
	}

	return buf
}

func (sr *SpendResponse) FromBytes(b []byte) (err error) {
	if len(b) < 12 {
		return errors.NewInvalidArgumentError("invalid byte length")
	}

	//nolint:gosec
	sr.Status = int(binary.LittleEndian.Uint64(b[:8]))
	sr.LockTime = binary.LittleEndian.Uint32(b[8:12])

	if len(b) > 12 {
		sr.SpendingTxID, err = chainhash.NewHash(b[12:])
		if err != nil {
			return err
		}
	}

	return nil
}
