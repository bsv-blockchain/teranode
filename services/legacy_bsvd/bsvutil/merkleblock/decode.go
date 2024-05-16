// Copyright (c) 2013-2016 The btcsuite developers
// Copyright (c) 2018 The bitcoinsv developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package merkleblock

import (
	"github.com/bitcoin-sv/ubsv/services/legacy/blockchain"
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

// ExtractMatches traverses the partial merkle tree and returns the merkle root
// on successful traversal or nil if an error occured during traversal due to
// an invalid block being parsed
func (m *PartialBlock) ExtractMatches() *chainhash.Hash {

	// if block is empty then no extraction can be made
	if m.numTx == 0 {
		return nil
	}

	// check for excessively high number of transactions
	if m.numTx > MaxTxnCount {
		return nil
	}

	totalHashes := uint64(len(m.finalHashes))

	// check there are not more hashes than total number of transactions in a block
	if totalHashes > m.numTx {
		return nil
	}

	// there must be atleast one bit per node in the partial merkle tree and
	// atleast one node per hash
	if uint64(len(m.bits)) < totalHashes {
		return nil
	}

	// calculate the height of the merkle tree
	height := uint64(0)
	for m.calcTreeWidth(height) > 1 {
		height++
	}

	// traverse the partial merkle tree
	merkleRootHash := m.traverseAndExtract(height, 0)

	// check no problems occured during tree traversal
	if m.bad {
		return nil
	}

	// verify that all bits were consumed (except for the padding caused by
	// serialisation it as a byte sequence)
	if (m.bitsUsed+7)/8 != (uint32(len(m.bits))+7)/8 {
		return nil
	}

	// verify that all hashes were consumed
	if m.hashesUsed != uint32(len(m.finalHashes)) {
		return nil
	}

	// return merkle root
	return merkleRootHash
}

// traverseAndExtract traverses over a partial merkle tree and finds matched
// transaction hashes and their item position in the block
func (m *PartialBlock) traverseAndExtract(height, pos uint64) *chainhash.Hash {

	if m.bitsUsed >= uint32(len(m.bits)) {
		// bits array has overflowed
		m.bad = true
		return &chainhash.Hash{}
	}

	parent := m.bits[m.bitsUsed]
	m.bitsUsed++

	if height == 0 || parent == byte(0) {
		// at height 0 or bit not set of node, then do not descend tree,
		// just return hash
		if m.hashesUsed >= uint32(len(m.finalHashes)) {
			m.bad = true
			return &chainhash.Hash{}
		}

		hash := m.finalHashes[m.hashesUsed]
		m.hashesUsed++

		if height == 0 && parent == byte(1) {
			// at height 0 and we have a matched transaction
			m.matchedHashes = append(m.matchedHashes, hash)
			m.matchedItems = append(m.matchedItems, pos)
		}

		return hash
	}

	// descend into subtree to extract matched transactions
	left := m.traverseAndExtract(height-1, pos*2)

	var right *chainhash.Hash

	if pos*2+1 < m.calcTreeWidth(height-1) {
		right = m.traverseAndExtract(height-1, pos*2+1)

		if right.IsEqual(left) {
			// left and right branches should not be identical, as the
			// transaction hashes covered by them must each be unique
			m.bad = true
		}
	} else {
		right = left
	}

	// hash with double sha256 and return
	return blockchain.HashMerkleBranches(left, right)
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
