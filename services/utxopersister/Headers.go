package utxopersister

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/libsv/go-bt/v2/chainhash"
)

type BlockIndex struct {
	Hash        *chainhash.Hash    `json:"hash"`
	Height      uint32             `json:"height"`
	TxCount     uint64             `json:"txCount"`
	BlockHeader *model.BlockHeader `json:"blockHeader"`
}

func (bi *BlockIndex) Serialise(writer io.Writer) error {
	if _, err := writer.Write(bi.Hash[:]); err != nil {
		return err
	}

	var heightBytes [4]byte

	binary.LittleEndian.PutUint32(heightBytes[:], bi.Height)

	if _, err := writer.Write(heightBytes[:]); err != nil {
		return err
	}

	var TxCountBytes [8]byte

	binary.LittleEndian.PutUint64(TxCountBytes[:], bi.TxCount)

	if _, err := writer.Write(TxCountBytes[:]); err != nil {
		return err
	}

	return bi.BlockHeader.ToWireBlockHeader().Serialize(writer)
}

func NewUTXOHeaderFromReader(reader io.Reader) (*BlockIndex, error) {
	hash := &chainhash.Hash{}
	if n, err := io.ReadFull(reader, hash[:]); err != nil || n != 32 {
		return nil, errors.NewProcessingError("Expected 32 bytes, got %d", n, err)
	}

	if bytes.Equal(hash[:], EOFMarker) {
		// We return an empty BlockIndex and io.EOF to signal the end of the stream
		// The empty BlockIndex indicates an EOF where the eofMarker was written
		return &BlockIndex{}, io.EOF
	}

	var heightBytes [4]byte
	if n, err := io.ReadFull(reader, heightBytes[:]); err != nil || n != 4 {
		return nil, errors.NewProcessingError("Expected 4 bytes, got %d", n, err)
	}

	height := binary.LittleEndian.Uint32(heightBytes[:])

	var txCountBytes [8]byte
	if n, err := io.ReadFull(reader, txCountBytes[:]); err != nil || n != 8 {
		return nil, errors.NewProcessingError("Expected 8 bytes, got %d", n, err)
	}

	txCount := binary.LittleEndian.Uint64(txCountBytes[:])

	var header [80]byte
	if n, err := io.ReadFull(reader, header[:]); err != nil || n != 80 {
		return nil, errors.NewProcessingError("Expected 80 bytes, got %d", n, err)
	}

	blockHeader, err := model.NewBlockHeaderFromBytes(header[:])
	if err != nil {
		return nil, errors.NewProcessingError("Error creating block header from bytes", err)
	}

	return &BlockIndex{
		Hash:        hash,
		Height:      height,
		TxCount:     txCount,
		BlockHeader: blockHeader,
	}, nil
}

func (bi *BlockIndex) String() string {
	return fmt.Sprintf("Hash: %s, Height: %d, TxCount: %d, ComputedHash: %s", bi.Hash.String(), bi.Height, bi.TxCount, bi.BlockHeader.String())
}
