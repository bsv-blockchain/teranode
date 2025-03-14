# Teranode Data Model - Blocks

The Teranode BSV model introduces a novel approach to block propagation, optimizing the network for high transaction throughput.

Teranode Blocks contain lists of subtree identifiers, not transactions. This is practical for nodes because they have been processing subtrees continuously, this allows for quick validation of blocks.

![TERANODE_Block.svg](../architecture/img/TERANODE_Block.svg)


Each block is an abstraction which is a container of a group of subtrees. A block contains a variable number of subtrees, a coinbase transaction, and a header, called a block header, which includes the block ID of the previous block, effectively creating a chain.

| Field       | Type                  | Description                                                 |
|-------------|-----------------------|-------------------------------------------------------------|
| Header      | *BlockHeader          | The Block Header                                            |
| CoinbaseTx  | *bt.Tx                | The coinbase transaction.                                   |
| Subtrees    | []*chainhash.Hash     | An array of hashes, representing the subtrees of the block. |

This table provides an overview of each field in the `Block` struct, including the data type and a brief description of its purpose or contents.

For details about the block header, please check the `references` section below.

Please note that the use of subtrees within blocks represents a data abstraction for a more optimal propagation of transactions. The data model is still the same as Bitcoin, with blocks containing transactions. The subtrees are used to optimize the propagation of transactions and blocks.

## Advantages of the Teranode BSV Model

- **Faster Validation**: Since nodes process subtrees continuously, validating a block is quicker because it involves validating the presence and correctness of subtree identifiers rather than individual transactions.


- **Scalability**: The model supports a much higher transaction throughput (> 1M transactions per second).


## Network Behavior

- **Transactions**: They are broadcast network-wide, and each node further propagates the transactions.


- **Subtrees**: Nodes broadcast subtrees to indicate prepared batches of transactions for block inclusion, allowing other nodes to perform preliminary validations.


- **Block Propagation**: When a block is found, its validation is expedited due to the continuous processing of subtrees. If a node encounters a subtree within a new block that it is unaware of, it can request the details from the node that submitted the block.

This proactive approach with subtrees enables the network to handle a significantly higher volume of transactions while maintaining quick validation times. It also allows nodes to utilize their processing power more evenly over time, rather than experiencing idle times between blocks. This model ensures that Teranode BSV can scale effectively to meet high transaction demands without the bottlenecks experienced by the BTC network.

## Historical Bitcoin Block Model

Historically, Bitcoin blocks have contained transactions, with each block linked to the previous one by a cryptographic hash.

![Legacy_Bitcoin_Block.svg](../architecture/img/Legacy_Bitcoin_Block.svg)

Note how the Bitcoin block contains all transactions (including ALL transaction data) for each transaction it contains, not just the transaction Id. This means that the block size will be very large if many transactions were included. At scale, this is not practical, as the block size would be too large to propagate across the network in a timely manner.

The Teranode BSV model optimizes this by using subtrees to represent transactions in a block, allowing for more efficient propagation and validation of blocks.

## Additional Resources

- [Overall System Design](../architecture/teranode-overall-system-design.md)
- [Block Header](./block_header_data_model.md)
- [Subtree](./subtree_data_model.md)
