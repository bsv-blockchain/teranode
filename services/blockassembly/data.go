package blockassembly

import (
	"encoding/binary"

	"github.com/libsv/go-bt/v2/chainhash"
)

type Data struct {
	TxIDChainHash *chainhash.Hash
	Fee           uint64
	Size          uint64
}

func NewFromBytes(bytes []byte) (*Data, error) {
	d := &Data{}

	// read first 32 bytes as txIDChainHash
	d.TxIDChainHash = &chainhash.Hash{}
	copy(d.TxIDChainHash[:], bytes[:32])

	// read next 8 bytes as fee
	d.Fee = binary.LittleEndian.Uint64(bytes[32:40])

	// read next 8 bytes as size
	d.Size = binary.LittleEndian.Uint64(bytes[40:48])

	return d, nil
}

func (d *Data) Bytes() []byte {
	bytes := make([]byte, 0, 256)

	// write txIDChainHash
	bytes = append(bytes, d.TxIDChainHash[:]...)

	// write 8 bytes for fee
	b64 := make([]byte, 8)
	binary.LittleEndian.PutUint64(b64, d.Fee)
	bytes = append(bytes, b64...)

	// write 8 bytes for size
	binary.LittleEndian.PutUint64(b64, d.Size)
	bytes = append(bytes, b64...)

	return bytes
}
