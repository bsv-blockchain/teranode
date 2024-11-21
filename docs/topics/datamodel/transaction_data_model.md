# Teranode Data Model - Extended Transactions

Teranode Transactions (referred to as "Extended Transactions") include additional metadata to facilitate processing, and are broadcast to nodes as they occur.

Teranode is expected to receive new transactions using the extended format only, with the legacy format not supported. A wallet will be required to create new transactions in extended format in order to communicate with Teranode.


_Bitcoin Transaction format:_

| Field           | Description                                          | Size                                             |
|-----------------|------------------------------------------------------|--------------------------------------------------|
| Version no      | currently 2                                          | 4 bytes                                          |
| In-counter      | positive integer VI = [[VarInt]]                     | 1 - 9 bytes                                      |
| list of inputs  | Transaction Input  Structure                         | <in-counter> qty with variable length per input  |
| Out-counter     | positive integer VI = [[VarInt]]                     | 1 - 9 bytes                                      |
| list of outputs | Transaction Output Structure                         | <out-counter> qty with variable length per output |
| nLocktime       | if non-zero and sequence numbers are < 0xFFFFFFFF: block height or timestamp when transaction is final | 4 bytes                                          |

The Extended Format adds a marker to the transaction format:


| Field           | Description                                                                                            | Size                                              |
|-----------------|--------------------------------------------------------------------------------------------------------|---------------------------------------------------|
| Version no      | currently 2                                                                                            | 4 bytes                                           |
| **EF marker**   | **marker for extended format**                                                                         | **0000000000EF**                                  |
| In-counter      | positive integer VI = [[VarInt]]                                                                       | 1 - 9 bytes                                       |
| list of inputs  | **Extended Format** transaction Input Structure                                                        | <in-counter> qty with variable length per input   |
| Out-counter     | positive integer VI = [[VarInt]]                                                                       | 1 - 9 bytes                                       |
| list of outputs | Transaction Output Structure                                                                           | <out-counter> qty with variable length per output |
| nLocktime       | if non-zero and sequence numbers are < 0xFFFFFFFF: block height or timestamp when transaction is final | 4 bytes                                           |


The Extended Format marker allows a library that supports the format to recognize that it is dealing with a transaction in extended format, while a library that does not support extended format will read the transaction as having 0 inputs, 0 outputs and a future nLock time. This has been done to minimize the possible problems a legacy library will have when reading the extended format. It can in no way be recognized as a valid transaction.


The input structure is the only additional thing that is changed in the Extended Format. The original input structure looks like this:

| Field                     | Description                                                                                 | Size                          |
|---------------------------|---------------------------------------------------------------------------------------------|-------------------------------|
| Previous Transaction hash | TXID of the transaction the output was created in                                           | 32 bytes                      |
| Previous Txout-index      | Index of the output (Non negative integer)                                                  | 4 bytes                       |
| Txin-script length        | Non negative integer VI = VarInt                                                            | 1 - 9 bytes                   |
| Txin-script / scriptSig   | Script                                                                                      | <in-script length>-many bytes |
| Sequence_no               | Used to iterate inputs inside a payment channel. Input is final when nSequence = 0xFFFFFFFF | 4 bytes                       |

In the Extended Format, we extend the input structure to include the previous locking script and satoshi outputs:

| Field                          | Description                                                                                 | Size                            |
|--------------------------------|---------------------------------------------------------------------------------------------|---------------------------------|
| Previous Transaction hash      | TXID of the transaction the output was created in                                           | 32 bytes                        |
| Previous Txout-index           | Index of the output (Non negative integer)                                                  | 4 bytes                         |
| Txin-script length             | Non negative integer VI = VarInt                                                            | 1 - 9 bytes                     |
| Txin-script / scriptSig        | Script                                                                                      | <in-script length>-many bytes   |
| Sequence_no                    | Used to iterate inputs inside a payment channel. Input is final when nSequence = 0xFFFFFFFF | 4 bytes                         |
| **Previous TX satoshi output** | **Output value in satoshis of previous input**                                              | **8 bytes**                     |
| **Previous TX script length**  | **Non negative integer VI = VarInt**                                                        | **1 - 9 bytes**                 |
| **Previous TX locking script** | **Script**                                                                                  | **\<script length>-many bytes** |

The Extended Format is not backwards compatible, but has been designed in such a way that existing software should not read a transaction in Extend Format as a valid (partial) transaction. The Extended Format header (0000000000EF) will be read as an empty transaction with a future nLock time in a library that does not support the Extended Format.

## Comparison with the historical Bitcoin transactions

Bitcoin Transactions are broadcast and included in blocks as they are found.

_Current Transaction format:_

| Field           | Description                                          | Size                                             |
|-----------------|------------------------------------------------------|--------------------------------------------------|
| Version no      | currently 2                                          | 4 bytes                                          |
| In-counter      | positive integer VI = [[VarInt]]                     | 1 - 9 bytes                                      |
| list of inputs  | Transaction Input  Structure                         | <in-counter> qty with variable length per input  |
| Out-counter     | positive integer VI = [[VarInt]]                     | 1 - 9 bytes                                      |
| list of outputs | Transaction Output Structure                         | <out-counter> qty with variable length per output |
| nLocktime       | if non-zero and sequence numbers are < 0xFFFFFFFF: block height or timestamp when transaction is final | 4 bytes                                          |

As opposed to that, the Extended Format includes additional metadata, and is bundled within subtrees containers for efficient processing and propagation.

## Additional Resources

To know more about the Extended Transaction Format, please refer to the [Bitcoin Improvement Proposal 239 (09 November 2022)](https://github.com/bitcoin-sv/arc/blob/b6296d1f775e7f3568f915e13d8f03bfe8fd3c32/doc/BIP-239.md).

Other:

- [Overall System Design](../../../docs/architecture/teranode-overall-system-design.md)
- [Block](./block_data_model.md)
- [Subtree](./subtree_data_model.md)
