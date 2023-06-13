package model

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/libsv/go-bc"
	"github.com/libsv/go-bk/crypto"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type BlockHeader struct {
	// Version of the block.  This is not the same as the protocol version.
	Version uint32

	// Hash of the previous block header in the blockchain.
	HashPrevBlock *chainhash.Hash

	// Merkle tree reference to hash of all transactions for the block.
	HashMerkleRoot *chainhash.Hash

	// Time the block was created im unix time.
	Timestamp uint32

	// Difficulty target for the block.
	Bits []byte

	// Nonce used to generate the block.
	Nonce uint32
}

func NewBlockHeaderFromBytes(headerBytes []byte) (*BlockHeader, error) {
	if len(headerBytes) != 80 {
		return nil, fmt.Errorf("block header should be 80 bytes long")
	}

	hashPrevBlock, err := chainhash.NewHash(headerBytes[4:36])
	if err != nil {
		return nil, fmt.Errorf("error creating previous block hash from bytes: %s", err.Error())
	}
	hashMerkleRoot, err := chainhash.NewHash(headerBytes[36:68])
	if err != nil {
		return nil, fmt.Errorf("error creating merkle root hash from bytes: %s", err.Error())
	}

	return &BlockHeader{
		Version:        binary.LittleEndian.Uint32(headerBytes[:4]),
		HashPrevBlock:  hashPrevBlock,
		HashMerkleRoot: hashMerkleRoot,
		Timestamp:      binary.LittleEndian.Uint32(headerBytes[68:72]),
		Bits:           bt.ReverseBytes(headerBytes[72:76]),
		Nonce:          binary.LittleEndian.Uint32(headerBytes[76:]),
	}, nil
}

func NewBlockHeaderFromString(headerHex string) (*BlockHeader, error) {
	headerBytes, err := hex.DecodeString(headerHex)
	if err != nil {
		return nil, fmt.Errorf("error decoding hex string to bytes: %s", err.Error())
	}

	return NewBlockHeaderFromBytes(headerBytes)
}

func (bh *BlockHeader) Hash() *chainhash.Hash {
	hash := chainhash.DoubleHashH(bh.Bytes())
	return &hash
}

func (bh *BlockHeader) Valid() bool {
	target, err := bc.ExpandTargetFromAsInt(hex.EncodeToString(bh.Bits))
	if err != nil {
		return false
	}

	digest := bt.ReverseBytes(crypto.Sha256d(bh.Bytes()))
	var bn = big.NewInt(0)
	bn.SetBytes(digest)

	return bn.Cmp(target) < 0
}

func (bh *BlockHeader) Bytes() []byte {
	var blockHeaderBytes []byte
	blockHeaderBytes = append(blockHeaderBytes, bc.UInt32ToBytes(bh.Version)...)
	blockHeaderBytes = append(blockHeaderBytes, bh.HashPrevBlock.CloneBytes()...)
	blockHeaderBytes = append(blockHeaderBytes, bh.HashMerkleRoot.CloneBytes()...)
	blockHeaderBytes = append(blockHeaderBytes, bc.UInt32ToBytes(bh.Timestamp)...)
	blockHeaderBytes = append(blockHeaderBytes, bt.ReverseBytes(bh.Bits)...)
	blockHeaderBytes = append(blockHeaderBytes, bc.UInt32ToBytes(bh.Nonce)...)

	return blockHeaderBytes
}
