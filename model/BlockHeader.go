package model

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/libsv/go-bc"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
)

type BlockHeader struct {
	// Version of the block.  This is not the same as the protocol version.
	Version uint32 // When the block header bytes are built, this will be represented as 4 bytes in little endian.

	// Hash of the previous block header in the blockchain.
	HashPrevBlock *chainhash.Hash

	// Merkle tree reference to hash of all transactions for the block.
	HashMerkleRoot *chainhash.Hash

	// Time the block was created im unix time.
	Timestamp uint32 // When the block header bytes are built, this will be represented as 4 bytes in little endian.

	// Difficulty target for the block.
	Bits NBit // This is the target threshold in little endian - this is the way it is store in a bitcoin block.

	// Nonce used to generate the block.
	Nonce uint32 // When the block header bytes are built, this will be represented as 4 bytes in little endian.
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
		Bits:           NewNBitFromSlice(headerBytes[72:76]),
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

func (bh *BlockHeader) Valid() (bool, error) {
	target := bh.Bits.CalculateTarget()

	var bn = big.NewInt(0)
	bn.SetString(bh.Hash().String(), 16)

	compare := bn.Cmp(target)
	if compare < 0 {
		return true, nil
	}

	return false, fmt.Errorf("block header does not meet target %d: %s >? %s ", compare, target.String(), bn.String())
}

func (bh *BlockHeader) Bytes() []byte {
	var blockHeaderBytes []byte
	blockHeaderBytes = append(blockHeaderBytes, bc.UInt32ToBytes(bh.Version)...)
	blockHeaderBytes = append(blockHeaderBytes, bh.HashPrevBlock.CloneBytes()...)
	blockHeaderBytes = append(blockHeaderBytes, bh.HashMerkleRoot.CloneBytes()...)
	blockHeaderBytes = append(blockHeaderBytes, bc.UInt32ToBytes(bh.Timestamp)...)
	blockHeaderBytes = append(blockHeaderBytes, bh.Bits.CloneBytes()...)
	blockHeaderBytes = append(blockHeaderBytes, bc.UInt32ToBytes(bh.Nonce)...)

	return blockHeaderBytes
}
