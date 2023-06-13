package model

import (
	"bytes"
	"errors"
	"io"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/libsv/go-p2p/wire"
)

var (
	ErrInvalidPOW = errors.New("invalid proof of work")
)

type Block struct {
	Header           *BlockHeader
	CoinbaseTx       *bt.Tx
	TransactionCount uint64
	Subtrees         []*chainhash.Hash

	// local
	hash          *chainhash.Hash
	subtreeLength uint64
}

func NewBlock(header *BlockHeader, coinbase *bt.Tx, subtrees []*chainhash.Hash) (*Block, error) {
	return &Block{
		Header:     header,
		CoinbaseTx: coinbase,
		Subtrees:   subtrees,
	}, nil
}

func NewBlockFromBytes(blockBytes []byte) (*Block, error) {
	block := &Block{}

	var err error

	// read the first 80 bytes as the block header
	blockHeaderBytes := blockBytes[:80]
	block.Header, err = NewBlockHeaderFromBytes(blockHeaderBytes)
	if err != nil {
		return nil, err
	}

	// create new buffer reader for the block bytes
	buf := bytes.NewReader(blockBytes[80:])

	// read the length of the subtree list
	block.subtreeLength, err = wire.ReadVarInt(buf, 0)
	if err != nil {
		return nil, err
	}

	// read the subtree list
	var hashBytes [32]byte
	var subtreeHash *chainhash.Hash
	for i := uint64(0); i < block.subtreeLength; i++ {
		_, err = io.ReadFull(buf, hashBytes[:])
		if err != nil {
			return nil, err
		}

		subtreeHash, err = chainhash.NewHash(hashBytes[:])
		if err != nil {
			return nil, err
		}

		block.Subtrees = append(block.Subtrees, subtreeHash)
	}

	coinbaseTxBytes, _ := io.ReadAll(buf) // read the rest of the bytes as the coinbase tx
	block.CoinbaseTx, err = bt.NewTxFromBytes(coinbaseTxBytes)
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (b *Block) Hash() *chainhash.Hash {
	if b.hash != nil {
		return b.hash
	}

	hash := b.Header.Hash()
	b.hash = &hash

	return b.hash
}

func (b *Block) SubTreeBytes() ([]byte, error) {
	// write the subtree list
	buf := bytes.NewBuffer(nil)
	err := wire.WriteVarInt(buf, 0, uint64(len(b.Subtrees)))
	if err != nil {
		return nil, err
	}
	for _, subTree := range b.Subtrees {
		_, err = buf.Write(subTree[:])
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (b *Block) SubTreesFromBytes(subtreesBytes []byte) error {
	buf := bytes.NewBuffer(subtreesBytes)
	subTreeCount, err := wire.ReadVarInt(buf, 0)
	if err != nil {
		return err
	}

	var subtreeBytes [32]byte
	var subtreeHash *chainhash.Hash
	for i := uint64(0); i < subTreeCount; i++ {
		_, err = buf.Read(subtreeBytes[:])
		if err != nil {
			return err
		}
		subtreeHash, err = chainhash.NewHash(subtreeBytes[:])
		if err != nil {
			return err
		}
		b.Subtrees = append(b.Subtrees, subtreeHash)
	}

	b.subtreeLength = subTreeCount

	return err
}

func (b *Block) Bytes() ([]byte, error) {
	// TODO not tested, due to discussion around storing subtrees in the block

	// write the header
	hash := b.Header.Hash()
	buf := bytes.NewBuffer(hash[:])

	// write the subtree list
	subtreeBytes, err := b.SubTreeBytes()
	if err != nil {
		return nil, err
	}
	buf.Write(subtreeBytes)

	// write the coinbase tx
	_, err = buf.Write(b.CoinbaseTx.Bytes())
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
