package utxopersister

import (
	"encoding/binary"
	"fmt"
	"io"

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
	if _, err := io.ReadFull(reader, hash[:]); err != nil {
		return nil, err
	}

	var heightBytes [4]byte
	if _, err := io.ReadFull(reader, heightBytes[:]); err != nil {
		return nil, err
	}

	height := binary.LittleEndian.Uint32(heightBytes[:])

	var txCountBytes [8]byte
	if _, err := io.ReadFull(reader, txCountBytes[:]); err != nil {
		return nil, err
	}

	txCount := binary.LittleEndian.Uint64(txCountBytes[:])

	var header [80]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return nil, err
	}

	blockHeader, err := model.NewBlockHeaderFromBytes(header[:])
	if err != nil {
		return nil, err
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
