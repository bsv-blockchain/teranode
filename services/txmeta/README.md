# TX Meta store

The tx meta store is used to (temporarily) store information about a transaction for the purposes of
mining transactions into a block. This information is stored in a key-value store, where the key is
the transaction hash and the value is an object containing the following fields:
- hash: the transaction ID
- fee: the fee paid for the transaction
- size_in_bytes: the size of the transaction in bytes 
- parents: the transaction IDs of the parents of this transaction
- blocks: the hash of the blocks that contain this transaction
- lock_time: the lock time of the transaction

The tx meta store is used in the following services:
- Validator: to store information about transactions after they are validated 
- Block Assembler: to store information about transactions that are being mined into a block
- Block Validation: which uses this information to validate blocks

## Usage

### Validator
- `Create`: creates a new tx-meta object for a transaction (deprecated)

### Block Assembly
- `UpdateTxMinedStatus`: updates the tx meta store with the block hash of a transaction

### Block Validation
- `Create`: creates a new tx-meta object for a transaction coming from a competing miner
- `Get`: gets the tx-meta object for a transaction when validating a subtree and blessing transactions (all transactions)
- `UpdateTxMinedStatus`: updates the tx meta store with the block hash of a transaction

#### Block - in Block Validation
- validOrderAndBlessed (inactive): checks that the transactions in a block are ordered correctly and that all transactions are blessed, checks that the blocks the transaction has been in are not on our current chain
  - `Get` for all transactions && all parents !!!

## Use cases
- Tx comes in for the first time via propagation / validation
- Tx comes in for the second time via propagation / validation
- Tx comes in from a subtree from a competing miner, seen for the first time
- Tx comes in from a subtree from a competing miner, seen for the second time
- Tx comes in from a subtree from a competing miner, was in a previous block, but not in the current chain
- Tx comes in from a subtree from a competing miner, was in a previous block, and is in the current chain
- 
