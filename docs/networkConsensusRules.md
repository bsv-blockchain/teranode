
## 📜 Network Consensus Rules

Bitcoin Nodes must adhere to the following network consensus rules. These rules are protocol level rules that are set in stone:

### Block Size Rule:

When a block is found, there is an economic limit applied to the block size which is imposed by nodes on the network.

This allows nodes to reach consensus on behavioural limits of the network. This limit is set to a large multiple of typical demand.

The Teranode Node system will be unbounded, so the block size limit will be purely economic, and the system will be capable of adding blocks of any size to the blockchain.

### Block Subsidy Rule:

Nodes receive a reward each time they add a new block to the blockchain. The reward consists of a Block Subsidy and transaction fees.

The Block Subsidy also acts to brings satoshis into circulation. The first Block Subsidies for the first 210,000 blocks added to the Bitcoin blockchain consisted of 5,000,000,000 or 50BSV per block.

Every 210,000, the value of the block subsidy is halved. The last halving took place in April 2024 at block height 840'000. As of April 2024, the  value of a Block Subsidy is 312,500,000 satoshis or 3.125BSV.

### Proof-of-Work Target Adjustment Rule:

The Target value to find a Proof-of-Work is adjusted to maintain a block discovery rate of approximately 10 minutes.

As of 2017, the Target value is adjusted every block. However, in the original release of Bitcoin, the Target value was adjusted every 14 days.

The Teranode Node system will maintain the current Target Adjustment schedule of every block, but the system may be updated in the future to return to the original Target Adjustment schedule (as part of Protocol restoration).

### Genesis Block Rule:

All new blocks must be added to the unbroken chain of Proof-of-Work leading back to the Genesis block or the initial block in the chain.

Since blocks are connected via new blocks including the hash of its previous block, and Merkle trees are used to keep track of transactions within a block, this rule can be satisfied using the chain of block headers. Each block header is only 80 bytes.

Given the chain of block headers can be used to adhere to the Genesis Block rules, the Initial Block Download or IBD where a new node joining the network downloads all block data including transaction data all the way back to the Genesis block used by current node implementations is not needed and will not be included in the Teranode Node system.

### Coinbase Maturity Rule:

Nodes cannot spend the output of a Coinbase Transaction unless 99 blocks have been added to the chain after it. In other words, for a node to spend its block reward, 99 more blocks must be added to the chain.

### Maximum Transaction Size Rule:

Bitcoin Nodes collectively set a practical limit for the size of transactions they are willing to timestamp into a block.

The Teranode Node project will ensure this limit is entirely economic by allowing unbounded scaling.

### nLockTime and nSequence Rules:

The nSequence fields of every transaction input and the nLockTime field of every transaction collectively determine the finality of a transaction.

If the value of nSequence of a transaction input is 0xFFFFFFFF then that input is a “final input”.

If the value of nSequence of a transaction input is not 0xFFFFFFFF then that input is a “non-final input”.

If all the inputs of a transaction are “final inputs” then the transaction is “final”, irrespective of the value of the nLockTime field.

If one or more of the inputs of a transaction are “non-final inputs” then:

- If the value of the transaction’s nLockTime field is less than 500,000,000 then the field represents a block height.
- If the node is working on a block whose height is greater or equal to the value of this field, then the transaction is “final”.
- Otherwise, the transaction is “non-final”.

- If the value of the transaction’s nLockTime field is greater or equal to 500,000,000 then the field is a UNIX epoch timestamp.
- If the median time passed of the last 11 blocks is greater or equal to the value of this field, then the transaction is “final”.
- Otherwise, the transaction is “non-final”.

A new transaction must replace a prior “non-final” transaction if it has the same inputs in the same order, every sequence number for every input in the new transaction is not less than the sequence number for the corresponding input in the prior transaction, and the sequence number of at least one input in the new transaction is greater than the sequence number for the corresponding input in the prior transaction.

If a new transaction is detected which does not fulfill all these requirements, then it must be rejected.

If a new transaction is detected which has inputs that conflict with the inputs of a “non-final” transaction, but which are not identical to the inputs of the

“non-final” transaction, then the “non-final” transaction is the “first seen” transaction and takes priority over the new transaction.

The sum value of the inputs to a transaction must be greater than or equal to the sum value of its outputs:

A new transaction must have valid inputs that provide a value that is greater than or equal to the value of its outputs.

If the value of the inputs is greater than the value of the outputs, the difference becomes part of the block reward.

If the value of the inputs is equal to the value of the outputs, the transaction has no fee, and is considered a “free” transaction.

The Teranode Node system will support free transactions up to a cumulative maximum size: initially, 2GB, then 5GB per block.

Free transactions must follow a standard transaction template that has been given the status of standard by the Technical Standards Committee.

For example, a standard transaction template type could be transactions that only have Pay-to-Public-Key-Hash scripts in their inputs and outputs.

### Transaction Format Rule:

Transactions must conform to the data formatting rules of the Bitcoin protocol.
