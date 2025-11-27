# Merkle Proof Package

This package provides helper functions for constructing and verifying merkle proofs for transactions in the Teranode blockchain implementation.

## Overview

The merkle proof package supports Teranode's subtree architecture where transactions are organized into subtrees within blocks. A complete merkle proof consists of:
1. **Subtree Proof**: The path from a transaction to its subtree root
2. **Block Proof**: The path from the subtree root to the block's merkle root

## Usage

### Constructing a Merkle Proof

```go
import "github.com/bsv-blockchain/teranode/util/merkleproof"

// Implement the MerkleProofConstructor interface
type MyRepository struct {
    // ... your implementation
}

func (r *MyRepository) GetTxMeta(txHash *chainhash.Hash) (*merkleproof.TxMetaData, error) {
    // Return transaction metadata including block heights and subtree indexes
}

func (r *MyRepository) GetBlockByHeight(height uint32) (*model.Block, error) {
    // Return block data
}

func (r *MyRepository) GetBlockHeader(blockHash *chainhash.Hash) (*model.BlockHeader, error) {
    // Return block header
}

func (r *MyRepository) GetSubtree(subtreeHash *chainhash.Hash) (*subtree.Subtree, error) {
    // Return subtree data
}

// Construct the proof
repo := &MyRepository{}
txID, _ := chainhash.NewHashFromStr("your-transaction-id")
proof, err := merkleproof.ConstructMerkleProof(txID, repo)
if err != nil {
    // Handle error
}
```

### Verifying a Merkle Proof

```go
// Verify a merkle proof
valid, blockHash, err := merkleproof.VerifyMerkleProof(proof)
if err != nil {
    // Handle error
}

if valid {
    fmt.Printf("Transaction is in block %s\n", blockHash.String())
}

// For coinbase transactions
valid, blockHash, err = merkleproof.VerifyMerkleProofForCoinbase(proof)
```

## Data Structures

### MerkleProof

The `MerkleProof` struct contains all necessary information for SPV verification:

```go
type MerkleProof struct {
    TxID             chainhash.Hash   // Transaction being proven
    BlockHash        chainhash.Hash   // Block containing the transaction
    BlockHeight      uint32           // Block height
    MerkleRoot       chainhash.Hash   // Block's merkle root
    SubtreeIndex     int              // Index of subtree in block
    TxIndexInSubtree int              // Index of tx in subtree
    SubtreeRoot      chainhash.Hash   // Root of the containing subtree
    SubtreeProof     []chainhash.Hash // Path from tx to subtree root
    BlockProof       []chainhash.Hash // Path from subtree to block root
    Flags            []int            // Direction flags (0=left, 1=right)
}
```

### TxMetaData

Minimal transaction metadata needed for proof construction:

```go
type TxMetaData struct {
    BlockHeights []uint32 // Block heights where tx appears
    SubtreeIdxs  []int    // Subtree indexes where tx appears
}
```

## Features

- **SPV Compatible**: Follows Bitcoin's SPV (Simplified Payment Verification) protocol
- **Subtree Architecture**: Native support for Teranode's subtree-based block structure
- **Coinbase Handling**: Special handling for coinbase transactions (always at position 0)
- **Thread-Safe**: All operations are thread-safe
- **Comprehensive Testing**: Includes unit tests for various scenarios

## Testing

Run tests with:
```bash
go test -v ./util/merkleproof
```

Run with race detection:
```bash
go test -race ./util/merkleproof
```