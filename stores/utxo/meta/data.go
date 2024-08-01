package meta

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// Data struct for the transaction metadata
// do not change order, has been optimized for size: https://golangprojectstructure.com/how-to-make-go-structs-more-efficient/
type Data struct {
	Tx             *bt.Tx           `json:"tx"`
	ParentTxHashes []chainhash.Hash `json:"parentTxHashes"`
	BlockIDs       []uint32         `json:"blockIDs"`
	Fee            uint64           `json:"fee"`
	SizeInBytes    uint64           `json:"sizeInBytes"`
	IsCoinbase     bool             `json:"isCoinbase"`
	LockTime       uint32           `json:"lockTime"` // lock time can be different from the transaction lock time, for instance in coinbase transactions
}

type PreviousOutput struct {
	PreviousTxID  chainhash.Hash
	Vout          uint32
	Idx           int
	LockingScript []byte
	Satoshis      uint64
}

// NewMetaDataFromBytes creates a new Data object from a byte slice
// the byte slice should be in the format:
//
//	8 bytes for Fee
//	8 bytes for SizeInBytes
//	1 byte for IsCoinbase
//	8 bytes for the number of ParentTxHashes
//	32 bytes for each ParentTxHash
func NewMetaDataFromBytes(dataBytes *[]byte, d *Data) {
	// read the numbers
	d.Fee = binary.LittleEndian.Uint64((*dataBytes)[:8])
	d.SizeInBytes = binary.LittleEndian.Uint64((*dataBytes)[8:16])
	d.IsCoinbase = (*dataBytes)[16] == 1
	parentTxHashesLen := binary.LittleEndian.Uint64((*dataBytes)[17:25])
	d.ParentTxHashes = make([]chainhash.Hash, parentTxHashesLen)

	for i := uint64(0); i < parentTxHashesLen; i++ {
		d.ParentTxHashes[i] = chainhash.Hash((*dataBytes)[25+i*32 : 25+(i+1)*32])
	}
}

// NewDataFromBytes creates a new Data object from a byte slice
// the byte slice should be in the format:
// 8 bytes for Fee
// 8 bytes for SizeInBytes
// 1 byte for IsCoinbase
// 8 bytes for the number of ParentTxHashes
// 32 bytes for each ParentTxHash
// the transaction data as a serialized byte slice
// the remainder of the byte slice is the block hashes as 4 byte integers
func NewDataFromBytes(dataBytes []byte) (*Data, error) {
	if len(dataBytes) < 24 {
		return nil, errors.NewProcessingError("dataBytes too short, expected at least 24 bytes, got %d", len(dataBytes))
	}

	d := &Data{}

	// read the numbers
	d.Fee = binary.LittleEndian.Uint64(dataBytes[:8])
	d.SizeInBytes = binary.LittleEndian.Uint64(dataBytes[8:16])
	d.IsCoinbase = dataBytes[16] == byte(1)
	parentTxHashesLen := binary.LittleEndian.Uint64(dataBytes[17:25])

	buf := bytes.NewReader(dataBytes[25:])

	// read the parent tx hashes
	var hashBytes [32]byte
	d.ParentTxHashes = make([]chainhash.Hash, parentTxHashesLen)
	for i := uint64(0); i < parentTxHashesLen; i++ {
		_, err := io.ReadFull(buf, hashBytes[:])
		if err != nil {
			return nil, errors.NewProcessingError("could not read hash bytes", err)
		}
		d.ParentTxHashes[i] = chainhash.Hash(hashBytes[:])
	}

	// read the tx
	d.Tx = &bt.Tx{}
	_, err := d.Tx.ReadFrom(buf)
	if err != nil {
		return nil, errors.NewProcessingError("could not read transaction", err)
	}

	// read the block hashes as the remainder data
	var blockBytes [4]byte
	d.BlockIDs = make([]uint32, 0)
	for {
		_, err = io.ReadFull(buf, blockBytes[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, errors.NewProcessingError("could not read block bytes", err)
		}
		d.BlockIDs = append(d.BlockIDs, binary.LittleEndian.Uint32(blockBytes[:]))
	}

	return d, nil
}

// Bytes returns the Data object as a byte slice
// the byte slice is in the format:
// 8 bytes for Fee
// 8 bytes for SizeInBytes
// 1 byte for IsCoinbase
// 8 bytes for the number of ParentTxHashes
// 32 bytes for each ParentTxHash
// the transaction data as a serialized byte slice
// the remainder of the byte slice is the block hashes as 4 byte integers
func (d *Data) Bytes() []byte {
	buf := make([]byte, 25, 1024) // 8 for Fee, 8 for SizeInBytes

	binary.LittleEndian.PutUint64(buf[:8], d.Fee)
	binary.LittleEndian.PutUint64(buf[8:16], d.SizeInBytes)
	if d.IsCoinbase {
		buf[16] = byte(1)
	} else {
		buf[16] = byte(0)
	}

	binary.LittleEndian.PutUint64(buf[17:25], uint64(len(d.ParentTxHashes)))
	for _, parentTxHash := range d.ParentTxHashes {
		buf = append(buf, parentTxHash.CloneBytes()...)
	}

	// write the tx data
	if d.Tx != nil {
		buf = append(buf, d.Tx.ExtendedBytes()...)
	}

	// write a varint for the length and then all the block hashes
	var blockBytes [4]byte
	for _, blockID := range d.BlockIDs {
		binary.LittleEndian.PutUint32(blockBytes[:], blockID)
		buf = append(buf, blockBytes[:]...)
	}

	return buf
}

// MetaBytes returns the Data object as a byte slice
// the byte slice is in the format:
// 8 bytes for Fee
// 8 bytes for SizeInBytes
// 1 byte for IsCoinbase
// 8 bytes for the number of ParentTxHashes
// 32 bytes for each ParentTxHash
func (d *Data) MetaBytes() []byte {
	buf := make([]byte, 25, 1024) // 8 for Fee, 8 for SizeInBytes

	binary.LittleEndian.PutUint64(buf[:8], d.Fee)
	binary.LittleEndian.PutUint64(buf[8:16], d.SizeInBytes)
	if d.IsCoinbase {
		buf[16] = byte(1)
	} else {
		buf[16] = byte(0)
	}

	binary.LittleEndian.PutUint64(buf[17:25], uint64(len(d.ParentTxHashes)))
	for _, parentTxHash := range d.ParentTxHashes {
		buf = append(buf, parentTxHash.CloneBytes()...)
	}

	return buf
}
