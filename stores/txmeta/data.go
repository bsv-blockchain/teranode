package txmeta

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/gcash/bchd/wire"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// Data struct for the transaction metadata
// do not change order, has been optimized for size: https://golangprojectstructure.com/how-to-make-go-structs-more-efficient/
type Data struct {
	Tx             *bt.Tx            `json:"tx"`
	ParentTxHashes []*chainhash.Hash `json:"parentTxHashes"`
	BlockHashes    []*chainhash.Hash `json:"blockHashes"` // TODO change this to use the db ids instead of the hashes
	Fee            uint64            `json:"fee"`
	SizeInBytes    uint64            `json:"sizeInBytes"`
}

type MetaData struct {
	Fee         uint64 `json:"fee"`
	SizeInBytes uint64 `json:"sizeInBytes"`
}

func NewMetaDataFromBytes(dataBytes []byte) (*Data, error) {
	d := &Data{}

	// read the numbers
	d.Fee = binary.LittleEndian.Uint64(dataBytes[:8])
	d.SizeInBytes = binary.LittleEndian.Uint64(dataBytes[8:16])

	buf := bytes.NewReader(dataBytes[16:])

	// read the parent tx hashes
	var hashBytes [32]byte
	parentTxHashesLen, _ := wire.ReadVarInt(buf, 0)
	d.ParentTxHashes = make([]*chainhash.Hash, parentTxHashesLen)
	for i := uint64(0); i < parentTxHashesLen; i++ {
		_, err := io.ReadFull(buf, hashBytes[:])
		if err != nil {
			return nil, err
		}
		if d.ParentTxHashes[i], err = chainhash.NewHash(hashBytes[:]); err != nil {
			return nil, err
		}
	}

	return d, nil
}

func NewDataFromBytes(dataBytes []byte) (*Data, error) {
	d := &Data{}

	// read the numbers
	d.Fee = binary.LittleEndian.Uint64(dataBytes[:8])
	d.SizeInBytes = binary.LittleEndian.Uint64(dataBytes[8:16])

	buf := bytes.NewReader(dataBytes[16:])

	// read the parent tx hashes
	var hashBytes [32]byte
	parentTxHashesLen, _ := wire.ReadVarInt(buf, 0)
	d.ParentTxHashes = make([]*chainhash.Hash, parentTxHashesLen)
	for i := uint64(0); i < parentTxHashesLen; i++ {
		_, err := io.ReadFull(buf, hashBytes[:])
		if err != nil {
			return nil, err
		}
		if d.ParentTxHashes[i], err = chainhash.NewHash(hashBytes[:]); err != nil {
			return nil, err
		}
	}

	// read the tx
	d.Tx = &bt.Tx{}
	_, err := d.Tx.ReadFrom(buf)
	if err != nil {
		return nil, err
	}

	// read the block hashes as the remainder data
	var hash *chainhash.Hash
	d.BlockHashes = make([]*chainhash.Hash, 0)
	for {
		_, err = io.ReadFull(buf, hashBytes[:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		hash, err = chainhash.NewHash(hashBytes[:])
		if err != nil {
			return nil, err
		}

		d.BlockHashes = append(d.BlockHashes, hash)
	}

	return d, nil
}

func (d *Data) Bytes() []byte {
	buf := make([]byte, 16) // 8 for Fee, 8 for SizeInBytes

	binary.LittleEndian.PutUint64(buf[:8], d.Fee)
	binary.LittleEndian.PutUint64(buf[8:16], d.SizeInBytes)

	// write a varint for the length and then all the parent tx hashes
	buf = append(buf, bt.VarInt(uint64(len(d.ParentTxHashes))).Bytes()...)
	for _, parentTxHash := range d.ParentTxHashes {
		buf = append(buf, parentTxHash.CloneBytes()...)
	}

	// write the tx data
	if d.Tx != nil {
		buf = append(buf, d.Tx.ExtendedBytes()...)
	}

	// write a varint for the length and then all the block hashes
	for _, blockHash := range d.BlockHashes {
		buf = append(buf, blockHash.CloneBytes()...)
	}

	return buf
}
