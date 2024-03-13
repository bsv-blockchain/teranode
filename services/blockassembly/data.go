package blockassembly

import (
	"encoding/binary"

	"github.com/libsv/go-bt/v2/chainhash"
)

type Data struct {
	TxIDChainHash  *chainhash.Hash
	Fee            uint64
	Size           uint64
	LockTime       uint32
	UtxoHashes     []chainhash.Hash
	ParentTxHashes []chainhash.Hash
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
	utxoLen := binary.LittleEndian.Uint32(bytes[52:56])

	// read the utxoHashes
	pos := 56
	d.UtxoHashes = make([]chainhash.Hash, utxoLen)
	for i := 0; i < int(utxoLen); i++ {
		d.UtxoHashes[i] = chainhash.Hash(bytes[pos : pos+chainhash.HashSize])
		pos += chainhash.HashSize
	}

	parenTxLen := binary.LittleEndian.Uint32(bytes[pos : pos+4])
	pos += 4

	// read the utxoHashes
	d.ParentTxHashes = make([]chainhash.Hash, parenTxLen)
	for i := 0; i < int(parenTxLen); i++ {
		d.ParentTxHashes[i] = chainhash.Hash(bytes[pos : pos+chainhash.HashSize])
		pos += chainhash.HashSize
	}

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

	// write 4 bytes for locktime
	b32 := make([]byte, 4)
	binary.LittleEndian.PutUint32(b32, d.LockTime)
	bytes = append(bytes, b32...)

	// write length of utxo hashes
	binary.LittleEndian.PutUint32(b32, uint32(len(d.UtxoHashes)))
	bytes = append(bytes, b32...)

	// write utxoHashes
	for _, h := range d.UtxoHashes {
		bytes = append(bytes, h[:]...)
	}

	// write length of parent tx hashes
	binary.LittleEndian.PutUint32(b32, uint32(len(d.ParentTxHashes)))
	bytes = append(bytes, b32...)

	// write parentTxHashes
	for _, h := range d.ParentTxHashes {
		bytes = append(bytes, h[:]...)
	}

	return bytes
}
