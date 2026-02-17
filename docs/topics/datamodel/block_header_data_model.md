# Teranode Data Model - Block Header

The block header is the fundamental data structure that chains blocks together in a blockchain. It is a fixed 80-byte structure that gets hashed as part of the proof-of-work algorithm for mining. The block header contains cryptographic commitments to the previous block, all transactions in the current block (via Merkle root), and mining difficulty parameters.

Teranode uses the standard Bitcoin block header format, maintaining full compatibility with the Bitcoin protocol while optimizing block content organization through subtrees.

Source: `model/BlockHeader.go` (BlockHeader struct)

## Block Header Structure

| Field          | Type            | Size (bytes) | Byte Offset | Description                                                                                                                            |
|----------------|-----------------|--------------|-------------|----------------------------------------------------------------------------------------------------------------------------------------|
| Version        | uint32          | 4            | 0-3         | Version of the block structure (typically 1, 2, or 4). Not the same as protocol version. Stored in little-endian format.              |
| HashPrevBlock  | *chainhash.Hash | 32           | 4-35        | SHA-256 hash of the previous block header. This creates the blockchain "chain" by linking each block to its predecessor.              |
| HashMerkleRoot | *chainhash.Hash | 32           | 36-67       | Root hash of the Merkle tree containing all transactions. In Teranode, computed from subtree root hashes.                             |
| Timestamp      | uint32          | 4            | 68-71       | Unix epoch timestamp (seconds since January 1, 1970) when the miner started hashing the header. Stored in little-endian format.       |
| Bits           | NBit            | 4            | 72-75       | Compact representation of the difficulty target threshold. Uses specialized encoding (see NBit Encoding section below).                |
| Nonce          | uint32          | 4            | 76-79       | Counter value incremented by miners searching for a valid proof-of-work. Stored in little-endian format.                              |

**Total Size:** 80 bytes (constant, defined as `BlockHeaderSize` in `model/BlockHeader.go`)

### Serialization Format

The block header is serialized as a fixed 80-byte array with the following characteristics:

- **Numeric fields** (Version, Timestamp, Nonce): Encoded in little-endian format
- **Hash fields** (HashPrevBlock, HashMerkleRoot): 32 bytes each, stored in internal byte order
- **Bits field**: 4-byte compact encoding in little-endian format
- **No variable-length fields**: Always exactly 80 bytes

The serialized header is used for:

1. **Block identification**: Double SHA-256 hash of the 80 bytes produces the block ID
2. **Proof-of-work validation**: Hash must be below the target threshold
3. **P2P propagation**: Transmitted between nodes during block announcement
4. **Chain verification**: Links blocks via HashPrevBlock references

Implementation: `BlockHeader.Bytes()` method in `model/BlockHeader.go`

## Teranode Implementation

### Merkle Root Computation

While Teranode uses the standard Bitcoin block header format, the Merkle root calculation differs in organization:

**Traditional Bitcoin:** Merkle root is computed directly from individual transaction hashes in the block.

**Teranode:** Merkle root is computed from subtree root hashes, where each subtree represents a batch of transactions:

1. Transactions are organized into power-of-2 sized subtrees (16, 32, 64, etc.)
2. Each subtree computes its own Merkle root from its transaction set
3. Block Merkle root is computed from the subtree root hashes
4. This maintains Bitcoin protocol compatibility while enabling efficient block propagation

The resulting Merkle root is identical to what would be computed from individual transactions, just organized hierarchically.

#### Coinbase Placeholder Handling in Subtree 0

The first subtree in a block (the subtree in the 0'th position of the block) contains a placeholder transaction in position 0 of the subtree. This placeholder transaction exists to allow node operators to continue sharing subtrees maintaining a spot for the coinbase transaction to go when it exists as the block is mined.

The merkle root for the block is calculated when this placeholder transaction is replaced with the real coinbase transaction; but the subtree hash *does not change* within the block. This means to calculate the merkle root of a block, you cannot simply use subtree hashes as nodes in a merkle tree.

As a simple example using 2 subtrees `S0` and `S1` with a size of 2; the true merkle root for the block with 4 transactions `CT` (Coinbase Transaction), `T1`, `T2`, and `T3` is calculated via:
```text
        ROOT = H(S0 + S1)
       /                  \
   S0 = H(H0 + H1)      S1 = H(H2 + H3)
    /           \           /             \
  H0 = H(CT)  H1 = H(T1)  H2 = H(T2)  H3 = H(T3)
```

But, the root subtree hash for `S0` is actually calculated with placeholder transaction `FF`; so the subtree that was actually broadcasted has a hash that looks like:
```text
   S0 = H(H0 + H1)
    /           \
  H0 = H(FF)  H1 = H(T1)
```

To summarize, calculating the merkle root and/or calculating merkle proofs for transactions/subtrees in a block requires a user to replace Transaction 0 in Subtree 0 with the actual coinbase transaction and recompute the entire merkle tree.

### Block Header Usage in Teranode

Block headers flow through several stages in Teranode's distributed architecture:

**Block Assembly Service** (`services/blockassembly/`):

- Constructs block headers from validated subtrees
- Computes Merkle root from subtree hashes
- Sets timestamp and difficulty target (Bits)
- Mining pools iterate Nonce to find valid proof-of-work

**Block Validation Service** (`services/blockvalidation/`):

- Validates block header proof-of-work (hash < target)
- Verifies timestamp is within acceptable range
- Checks HashPrevBlock links to known block
- Validates Merkle root matches subtree set

**Blockchain Service** (`services/blockchain/`):

- Stores block headers in blockchain store (PostgreSQL)
- Maintains chain state via header links
- Tracks best chain by cumulative difficulty
- Provides header lookup by hash or height

### Integration with Block Structure

Block headers are embedded in the `Block` struct as the `Header` field:

```go
type Block struct {
    Header    *BlockHeader      // The 80-byte block header
    CoinbaseTx *bt.Tx           // Coinbase transaction
    Subtrees   []*chainhash.Hash // Subtree root hashes
    // ... additional fields
}
```

The header provides the cryptographic binding while subtrees contain the transaction organization.

Reference: `model/Block.go` (Block struct)

## Proof-of-Work

### Hash Computation

The block hash (block ID) is computed by:

1. Serialize the 80-byte block header (all 6 fields in order)
2. Compute SHA-256 hash of the serialized header
3. Compute SHA-256 hash of the result (double SHA-256)
4. The resulting 32-byte hash is the block ID

Implementation: `BlockHeader.Hash()` method in `model/BlockHeader.go`

### Difficulty Target Validation

For a block to be valid, its hash must be numerically less than the target threshold:

```text
block_hash < target_threshold
```

The target is derived from the Bits field (see NBit Encoding below). Miners increment the Nonce field (and potentially adjust Timestamp) until they find a hash that meets this condition.

Implementation: `BlockHeader.HasMetTargetDifficulty()` method in `model/BlockHeader.go`

### Mining Process

1. Construct block header with all fields except Nonce
2. Set Nonce = 0
3. Compute double SHA-256 hash of header
4. If hash < target: block is valid, broadcast to network
5. If hash >= target: increment Nonce, repeat from step 3
6. If Nonce overflows (exceeds 2^32): adjust Timestamp or coinbase transaction, rebuild Merkle root, retry

Modern ASIC miners can compute billions of hashes per second, iterating through Nonce values rapidly.

## NBit Encoding

The Bits field uses a specialized compact encoding to represent the 256-bit difficulty target in just 4 bytes.

### Compact Format

The NBit encoding uses an exponent-mantissa format:

- **Byte 0 (exponent)**: Specifies the length of the target in bytes
- **Bytes 1-3 (mantissa)**: Most significant 3 bytes of the target

**Formula:**

```text
target = mantissa × 256^(exponent - 3)
```

### Example

Genesis block Bits value: `0x1d00ffff`

- Exponent: 0x1d (29 decimal)
- Mantissa: 0x00ffff

Target calculation:

```text
target = 0x00ffff × 256^(29-3)
       = 0x00ffff × 256^26
       = 0x00ffff000000000000000000000000000000000000000000000000
```

This represents the maximum difficulty target (easiest difficulty). As blocks are mined, the difficulty adjusts, and the target decreases (Bits value changes).

### Implementation

```go
type NBit [4]byte  // 4-byte array in little-endian format

func (b NBit) CalculateTarget() *big.Int {
    // Decode exponent and mantissa
    // Compute target = mantissa × 256^(exponent-3)
}

func (b NBit) CalculateDifficulty() *big.Float {
    // Compute difficulty = genesis_target / current_target
}
```

Reference: `NBit` type and `NBit.CalculateTarget()` method in `model/NBit.go`

## Genesis Block Header

Teranode includes the hardcoded Bitcoin genesis block header (block 0) for chain initialization:

```go
GenesisBlockHeader = &BlockHeader{
    Version:        1,
    Timestamp:      1231006505,  // January 3, 2009 18:15:05 UTC
    Nonce:          2083236893,
    HashPrevBlock:  &chainhash.Hash{},  // All zeros (no previous block)
    HashMerkleRoot: merkleRoot,
    Bits:           *bits,  // 0x1d00ffff (maximum target)
}
```

The genesis block is the foundation of the blockchain. Its HashPrevBlock is all zeros since there is no previous block. The coinbase transaction famously includes the message:

> "The Times 03/Jan/2009 Chancellor on brink of second bailout for banks"

This timestamp proves the blockchain couldn't have been started before that date.

Reference: `GenesisBlockHeader` constant in `model/BlockHeader.go`

## Block Header Metadata

In addition to the 80-byte block header, Teranode maintains supplementary metadata in the `BlockHeaderMeta` struct for tracking block processing state:

Source: `model/BlockHeaderMeta.go` (BlockHeaderMeta struct)

| Field       | Type   | Description                                                                        |
|-------------|--------|------------------------------------------------------------------------------------|
| Height      | uint32 | Block height in the blockchain (0 for genesis, increments by 1 for each block)    |
| TxCount     | uint32 | Total number of transactions in the block (including coinbase)                     |
| SizeInBytes | uint64 | Total size of the block in bytes (header + coinbase + all transaction data)        |
| Miner       | string | Identifier of the miner who found the block (extracted from coinbase)              |

This metadata is stored alongside the header in the blockchain store but is not part of the consensus-critical 80-byte header structure.

## Version Field History

The Version field has evolved over Bitcoin's history:

| Version | Introduced | Purpose                                                  |
|---------|----------|----------------------------------------------------------|
| 1       | 2009     | Original Bitcoin genesis block format                    |
| 2       | 2012     | BIP-34: Block height in coinbase transaction             |
| 3       | 2015     | BIP-66: Strict DER signature encoding                    |
| 4       | 2015     | BIP-65: OP_CHECKLOCKTIMEVERIFY opcode                    |

BSV maintains version 4 for modern blocks. The version field also supports future soft-fork signaling through version bits.

## Timestamp Validation

The Timestamp field must satisfy certain validation rules:

1. **Median Time Past (MTP):** Timestamp must be greater than the median timestamp of the previous 11 blocks
2. **Network-Adjusted Time:** Timestamp must be less than the network-adjusted time + 2 hours
3. **Monotonic Increase:** Generally increases with each block (enforced via MTP rule)

These rules prevent miners from manipulating timestamps while allowing some flexibility for clock skew across the network.

## Comparison with Traditional Bitcoin

Teranode's block header implementation maintains full Bitcoin protocol compatibility:

**Identical:**

- 80-byte fixed structure and field layout
- Double SHA-256 hashing for block ID
- Proof-of-work validation using Bits/Nonce
- Chaining via HashPrevBlock references

**Enhanced:**

- **Merkle root computation:** Organized via subtrees rather than flat transaction list
- **Block propagation:** Headers propagated separately from block content
- **Storage:** Headers stored in PostgreSQL blockchain store with efficient indexing
- **Validation:** Distributed across Block Validation and Blockchain services

The key innovation is that while the header format remains unchanged, Teranode's subtree architecture enables processing blocks with millions of transactions while maintaining Bitcoin protocol compatibility.

## Implementation References

Core implementation files:

- `model/BlockHeader.go` - BlockHeader struct, NewBlockHeaderFromBytes(), Hash(), HasMetTargetDifficulty(), Bytes()
- `model/NBit.go` - NBit type, CalculateTarget(), CalculateDifficulty()
- `model/BlockHeaderMeta.go` - BlockHeaderMeta struct

## Additional Resources

- [Block Data Model](./block_data_model.md) - Complete block structure including subtrees
- [Subtree Data Model](./subtree_data_model.md) - How transactions are organized in subtrees
- [Transaction Data Model](./transaction_data_model.md) - Transaction format and validation
- [Blockchain Service Documentation](../services/blockchain.md) - Header storage and chain management
- [Block Validation Service Documentation](../services/blockValidation.md) - Header validation process
