// Copyright (c) 2013-2016 The btcsuite developers
// Copyright (c) 2018 The bitcoinsv developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package merkleblock

import (
	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
	"github.com/bitcoin-sv/ubsv/services/legacy/wire"
	"github.com/libsv/go-bt/v2/chainhash"
)

// MerkleBlock is used to house intermediate information needed to generate a
// wire.MsgMerkleBlock
type MerkleBlock struct {
	numTx       uint32
	allHashes   []*chainhash.Hash
	finalHashes []*chainhash.Hash
	matchedBits []byte
	bits        []byte
}

// calcTreeWidth calculates and returns the the number of nodes (width) or a
// merkle tree at the given depth-first height.
func (m *MerkleBlock) calcTreeWidth(height uint32) uint32 {
	return (m.numTx + (1 << height) - 1) >> height
}

// calcHash returns the hash for a sub-tree given a depth-first height and
// node position.
func (m *MerkleBlock) calcHash(height, pos uint32) *chainhash.Hash {
	if height == 0 {
		return m.allHashes[pos]
	}

	var right *chainhash.Hash
	left := m.calcHash(height-1, pos*2)
	if pos*2+1 < m.calcTreeWidth(height-1) {
		right = m.calcHash(height-1, pos*2+1)
	} else {
		right = left
	}
	return hashMerkleBranches(left, right)
}

func hashMerkleBranches(left *chainhash.Hash, right *chainhash.Hash) *chainhash.Hash {
	// Concatenate the left and right nodes.
	var hash [chainhash.HashSize * 2]byte
	copy(hash[:chainhash.HashSize], left[:])
	copy(hash[chainhash.HashSize:], right[:])

	newHash := chainhash.DoubleHashH(hash[:])
	return &newHash
}

// traverseAndBuild builds a partial merkle tree using a recursive depth-first
// approach.  As it calculates the hashes, it also saves whether or not each
// node is a parent node and a list of final hashes to be included in the
// merkle block.
func (m *MerkleBlock) traverseAndBuild(height, pos uint32) {
	// Determine whether this node is a parent of a matched node.
	var isParent byte
	for i := pos << height; i < (pos+1)<<height && i < m.numTx; i++ {
		isParent |= m.matchedBits[i]
	}
	m.bits = append(m.bits, isParent)

	// When the node is a leaf node or not a parent of a matched node,
	// append the hash to the list that will be part of the final merkle
	// block.
	if height == 0 || isParent == 0x00 {
		m.finalHashes = append(m.finalHashes, m.calcHash(height, pos))
		return
	}

	// At this point, the node is an internal node and it is the parent of
	// of an included leaf node.

	// Descend into the left child and process its sub-tree.
	m.traverseAndBuild(height-1, pos*2)

	// Descend into the right child and process its sub-tree if
	// there is one.
	if pos*2+1 < m.calcTreeWidth(height-1) {
		m.traverseAndBuild(height-1, pos*2+1)
	}
}

// TxInSet checks if a given transaction is included in the given list of
// transactions
func TxInSet(tx *chainhash.Hash, set []*chainhash.Hash) bool {
	for _, next := range set {
		if *tx == *next {
			return true
		}
	}
	return false
}

// NewMerkleBlockWithTxnSet returns a new *wire.MsgMerkleBlock containing a
// partial merkle tree built using the list of transactions provided
func NewMerkleBlockWithTxnSet(block *bsvutil.Block, txnSet []*chainhash.Hash) (*wire.MsgMerkleBlock, []uint32) {

	numTx := uint32(len(block.Transactions()))
	mBlock := MerkleBlock{
		numTx:       numTx,
		allHashes:   make([]*chainhash.Hash, 0, numTx),
		matchedBits: make([]byte, 0, numTx),
	}

	// add all block transactions to merkle block and set bits for matching
	// transactions
	var matchedIndices []uint32
	for txIndex, tx := range block.Transactions() {
		if TxInSet(tx.Hash(), txnSet) {
			mBlock.matchedBits = append(mBlock.matchedBits, 0x01)
			matchedIndices = append(matchedIndices, uint32(txIndex))
		} else {
			mBlock.matchedBits = append(mBlock.matchedBits, 0x00)
		}
		mBlock.allHashes = append(mBlock.allHashes, tx.Hash())
	}

	return mBlock.calcBlock(block), matchedIndices
}

// calcBlock calculates the merkleBlock when created from either a TxnSet or
// by a bloom.Filter
func (m *MerkleBlock) calcBlock(block *bsvutil.Block) *wire.MsgMerkleBlock {

	// Calculate the number of merkle branches (height) in the tree.
	height := uint32(0)
	for m.calcTreeWidth(height) > 1 {
		height++
	}

	// Build the depth-first partial merkle tree.
	m.traverseAndBuild(height, 0)

	// Create and return the merkle block.
	msgMerkleBlock := wire.MsgMerkleBlock{
		Header:       block.MsgBlock().Header,
		Transactions: m.numTx,
		Hashes:       make([]*chainhash.Hash, 0, len(m.finalHashes)),
		Flags:        make([]byte, (len(m.bits)+7)/8),
	}
	for _, hash := range m.finalHashes {
		_ = msgMerkleBlock.AddTxHash(hash)
	}
	for i := uint32(0); i < uint32(len(m.bits)); i++ {
		msgMerkleBlock.Flags[i/8] |= m.bits[i] << (i % 8)
	}

	return &msgMerkleBlock
}
