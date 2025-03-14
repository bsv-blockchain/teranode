# Teranode Data Model - Block Header

The block header is a data structure that contains metadata about a block. It is used to connect blocks together in a blockchain. The block header is a structure that is hashed as part of the proof-of-work algorithm for mining. It contains the following fields:

| Field           | Type               | Description                                                                                                                            |
|-----------------|--------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| Version         | uint32             | Version of the block, different from the protocol version. Represented as 4 bytes in little endian when built into block header bytes.
| HashPrevBlock   | *chainhash.Hash    | Reference to the hash of the previous block header in the blockchain.                                                                  |
| HashMerkleRoot  | *chainhash.Hash    | Reference to the Merkle tree hash of all subtrees in the block.                                                                        |
| Timestamp       | uint32             | The time when the block was created, in Unix time. Represented as 4 bytes in little endian when built into block header bytes.         |
| Bits            | NBit               | Difficulty target for the block. Represented as a target threshold in little endian, the format used in a Bitcoin block.               |
| Nonce           | uint32             | Nonce used in generating the block. Represented as 4 bytes in little endian when built into block header bytes.                        |

## Additional Resources

- [Block Data Model](./block_data_model.md)
