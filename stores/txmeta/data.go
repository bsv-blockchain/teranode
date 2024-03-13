package txmeta

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

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
}

type MetaData struct {
	Fee         uint64 `json:"fee"`
	SizeInBytes uint64 `json:"sizeInBytes"`
}

func NewMetaDataFromBytes(dataBytes *[]byte, d *Data) {
	// read the numbers
	d.Fee = binary.LittleEndian.Uint64((*dataBytes)[:8])
	d.SizeInBytes = binary.LittleEndian.Uint64((*dataBytes)[8:16])
	parentTxHashesLen := binary.LittleEndian.Uint64((*dataBytes)[16:24])
	d.ParentTxHashes = make([]chainhash.Hash, parentTxHashesLen)

	for i := uint64(0); i < parentTxHashesLen; i++ {
		d.ParentTxHashes[i] = chainhash.Hash((*dataBytes)[24+i*32 : 24+(i+1)*32])
	}
}

func NewDataFromBytes(dataBytes []byte) (*Data, error) {
	if len(dataBytes) < 24 {
		return nil, fmt.Errorf("dataBytes too short, expected at least 24 bytes, got %d", len(dataBytes))
	}

	d := &Data{}

	// read the numbers
	d.Fee = binary.LittleEndian.Uint64(dataBytes[:8])
	d.SizeInBytes = binary.LittleEndian.Uint64(dataBytes[8:16])
	parentTxHashesLen := binary.LittleEndian.Uint64(dataBytes[16:24])

	buf := bytes.NewReader(dataBytes[24:])

	// read the parent tx hashes
	var hashBytes [32]byte
	d.ParentTxHashes = make([]chainhash.Hash, parentTxHashesLen)
	for i := uint64(0); i < parentTxHashesLen; i++ {
		_, err := io.ReadFull(buf, hashBytes[:])
		if err != nil {
			return nil, err
		}
		d.ParentTxHashes[i] = chainhash.Hash(hashBytes[:])
	}

	// read the tx
	d.Tx = &bt.Tx{}
	_, err := d.Tx.ReadFrom(buf)
	if err != nil {
		return nil, err
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
			return nil, err
		}
		d.BlockIDs = append(d.BlockIDs, binary.LittleEndian.Uint32(blockBytes[:]))
	}

	return d, nil
}

func (d *Data) Bytes() []byte {
	buf := make([]byte, 24, 1024) // 8 for Fee, 8 for SizeInBytes

	binary.LittleEndian.PutUint64(buf[:8], d.Fee)
	binary.LittleEndian.PutUint64(buf[8:16], d.SizeInBytes)

	binary.LittleEndian.PutUint64(buf[16:24], uint64(len(d.ParentTxHashes)))
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

func (d *Data) MetaBytes() []byte {
	buf := make([]byte, 24, 1024) // 8 for Fee, 8 for SizeInBytes

	binary.LittleEndian.PutUint64(buf[:8], d.Fee)
	binary.LittleEndian.PutUint64(buf[8:16], d.SizeInBytes)

	binary.LittleEndian.PutUint64(buf[16:24], uint64(len(d.ParentTxHashes)))
	for _, parentTxHash := range d.ParentTxHashes {
		buf = append(buf, parentTxHash.CloneBytes()...)
	}

	return buf
}
