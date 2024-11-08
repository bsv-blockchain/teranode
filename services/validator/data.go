package validator

import (
	"encoding/binary"

	"github.com/bitcoin-sv/ubsv/errors"
)

type TxValidationData struct {
	Tx     []byte
	Height uint32
}

func NewTxValidationDataFromBytes(bytes []byte) (*TxValidationData, error) {
	if len(bytes) < 4 {
		return nil, errors.New(errors.ERR_ERROR, "input bytes too short")
	}

	d := &TxValidationData{}

	// read first 4 bytes as height
	d.Height = binary.LittleEndian.Uint32(bytes[:4])

	// read remaining bytes as tx
	if len(bytes) > 4 {
		d.Tx = make([]byte, len(bytes[4:]))
		copy(d.Tx, bytes[4:])
	}

	return d, nil
}

func (d *TxValidationData) Bytes() []byte {
	bytes := make([]byte, 0, 4+len(d.Tx))

	// write 4 bytes for height
	b32 := make([]byte, 4)
	binary.LittleEndian.PutUint32(b32, uint32(d.Height))
	bytes = append(bytes, b32...)

	// write tx
	bytes = append(bytes, d.Tx...)

	return bytes
}
