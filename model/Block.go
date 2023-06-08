package model

import (
	"bytes"
	"errors"
	"io"

	"github.com/TAAL-GmbH/ubsv/util"
	"github.com/libsv/go-bc"
	"github.com/libsv/go-bk/crypto"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/libsv/go-p2p/wire"
)

var (
	ErrInvalidPOW = errors.New("invalid proof of work")
)

type Block struct {
	Header     *bc.BlockHeader
	CoinbaseTx *bt.Tx
	SubTrees   []*util.SubTree
	// Height uint32

	// local
	hash          *chainhash.Hash
	subTreeLength uint64
}

func NewBlock(header *bc.BlockHeader, coinbase *bt.Tx, subTrees []*util.SubTree) (*Block, error) {
	return &Block{
		Header:     header,
		CoinbaseTx: coinbase,
		SubTrees:   subTrees,
	}, nil
}

func NewBlockFromBytes(blockBytes []byte) (*Block, error) {
	block := &Block{}

	var err error

	// read the first 80 bytes as the block header
	blockHeaderBytes := blockBytes[:80]
	block.Header, err = bc.NewBlockHeaderFromBytes(blockHeaderBytes)
	if err != nil {
		return nil, err
	}

	// create new buffer reader for the block bytes
	buf := bytes.NewReader(blockBytes[80:])

	// read the length of the subtree list
	block.subTreeLength, err = wire.ReadVarInt(buf, 0)
	if err != nil {
		return nil, err
	}

	// read the subtree list
	var hashBytes [32]byte
	var subTreeHash *chainhash.Hash
	for i := uint64(0); i < block.subTreeLength; i++ {
		_, err = io.ReadFull(buf, hashBytes[:])
		if err != nil {
			return nil, err
		}

		subTreeHash, err = chainhash.NewHash(hashBytes[:])
		if err != nil {
			return nil, err
		}

		_ = subTreeHash

		//
		// TODO load the full subtree from the store ???
		//
		block.SubTrees = append(block.SubTrees, util.NewTree(20))
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

	hash, err := chainhash.NewHash(bt.ReverseBytes(crypto.Sha256d(b.Header.Bytes())))
	if err == nil {
		b.hash = hash
	}

	return hash
}

func (b *Block) Bytes() ([]byte, error) {
	// TODO not tested, due to discussion around storing subtrees in the block

	// write the header
	blockBytes := b.Header.Bytes()

	// write the subtree list
	buf := bytes.NewBuffer(blockBytes)
	err := wire.WriteVarInt(buf, 0, uint64(len(b.SubTrees)))
	if err != nil {
		return nil, err
	}

	for _, subTree := range b.SubTrees {
		hash := subTree.RootHash()
		_, err = buf.Write(hash[:])
		if err != nil {
			return nil, err
		}
	}

	// write the coinbase tx
	_, err = buf.Write(b.CoinbaseTx.Bytes())
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
