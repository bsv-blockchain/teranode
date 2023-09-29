package blockassembly

import (
	"encoding/binary"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

type Data struct {
	TxIDChainHash *chainhash.Hash
	Fee           uint64
	Size          uint64
	LockTime      uint32
	UtxoHashes    []*chainhash.Hash
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

	// read next 4 bytes as locktime
	d.LockTime = binary.LittleEndian.Uint32(bytes[48:52])

	// read varint as utxoHashes length
	varint, bytesRead := bt.NewVarIntFromBytes(bytes[52:])

	// read the utxoHashes
	d.UtxoHashes = make([]*chainhash.Hash, varint)
	for i := 0; i < int(varint); i++ {
		h := &chainhash.Hash{}
		copy(h[:], bytes[52+bytesRead+(i*32):])
		d.UtxoHashes[i] = h
	}

	return d, nil
}

func (d *Data) Bytes() []byte {
	bytes := make([]byte, 0)

	// write txIDChainHash
	bytes = append(bytes, d.TxIDChainHash[:]...)

	// write 8 bytes for fee
	b64 := make([]byte, 8)
	binary.LittleEndian.PutUint64(b64, d.Fee)
	bytes = append(bytes, b64...)

	// write 8 bytes for size
	binary.LittleEndian.PutUint64(b64, d.Size)
	bytes = append(bytes, b64...)

	// write 4 bytes for locktime
	b32 := make([]byte, 4)
	binary.LittleEndian.PutUint32(b32, d.LockTime)
	bytes = append(bytes, b32...)

	// write length in varint
	bytes = append(bytes, bt.VarInt(uint64(len(d.UtxoHashes))).Bytes()...)

	// write utxoHashes
	for _, h := range d.UtxoHashes {
		bytes = append(bytes, h[:]...)
	}

	return bytes
}
