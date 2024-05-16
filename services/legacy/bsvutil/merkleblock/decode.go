// Copyright (c) 2013-2016 The btcsuite developers
// Copyright (c) 2018 The bitcoinsv developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package merkleblock

import (
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/libsv/go-bt/v2/chainhash"
)

// MaxTxnCount defines the maximum number of transactions we will process before
// aborting merkle tree traversal operations.
//
// bitcoin core uses formula of max blocksize divided by segwit transaction
// size (240 bytes) to calculate max number of transactions that could fit
// in a block which at this time is 4000000/240=16666
//
// bitcoin ABC has removed this check and has been marked "FIXME".
//
// we have opted to use a similar calculation to core based on smallest
// possible transaction size spending OP_TRUE at 61 bytes with max block
// size variable
var MaxTxnCount = wire.MaxBlockPayload() / 61

// PartialBlock is used to house intermediate information needed to decode a
// wire.MsgMerkleBlock
type PartialBlock struct {
	numTx       uint64
	finalHashes []*chainhash.Hash
	bits        []byte
	// variables below used for traversal and extraction
	bad           bool
	bitsUsed      uint32
	hashesUsed    uint32
	matchedHashes []*chainhash.Hash
	matchedItems  []uint64
}

// NewMerkleBlockFromMsg returns a MerkleBlock from parsing a wire.MsgMerkleBlock
// which can be used for extracting transaction matches from for verification
// on a partial merkle tree.
//
// source code based off bitcoin c++ code at
// https://github.com/bitcoin/bitcoin/blob/master/src/merkleblock.cpp
// with protocol reference documentation at
// https://bitcoin.org/en/developer-examples#parsing-a-merkleblock
func NewMerkleBlockFromMsg(msg wire.MsgMerkleBlock) *PartialBlock {

	// get number of hashes in message
	numTx := uint64(msg.Transactions)

	// from the wire message Flags decode the bits
	bits := make([]byte, len(msg.Flags)*8)

	for i := uint32(0); i < uint32(len(bits)); i++ {
		if msg.Flags[i/8]&(1<<(i%8)) == 0 {
			bits[i] = byte(0)
		} else {
			bits[i] = byte(1)
		}
	}

	// Create merkle block using data from msg
	mBlock := &PartialBlock{
		// total number of transactions in block
		numTx: numTx,
		// hashes used for partial merkle tree
		finalHashes: msg.Hashes,
		// bit flags for our included hashes
		bits: bits,
		// initialse traversal variables
		bad:           false,
		bitsUsed:      0,
		hashesUsed:    0,
		matchedHashes: make([]*chainhash.Hash, 0),
		matchedItems:  make([]uint64, 0),
	}

	return mBlock
}

// GetMatches returns the transaction hashes matched in the partial merkle tree
func (m *PartialBlock) GetMatches() []*chainhash.Hash {
	return m.matchedHashes
}

// GetItems returns the item number of the matched transactions placement in the
// merkle block
func (m *PartialBlock) GetItems() []uint64 {
	return m.matchedItems
}

// BadTree returns status of partial merkle tree traversal
func (m *PartialBlock) BadTree() bool {
	return m.bad
}

// calcTreeWidth calculates and returns the the number of nodes (width) or a
// merkle tree at the given depth-first height.
func (m *PartialBlock) calcTreeWidth(height uint64) uint64 {
	return (m.numTx + (1 << height) - 1) >> height
}
