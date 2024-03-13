## First findings

During our initial tests, that started Thursday February 22nd, the three Teranodes have been processing and validating transactions really well. They are easily able to handle over 1 million transactions per second, validating these transactions and building blocks.

Where we have encountered problems is in validating and processing blocks from other miners. For example, M1 may have had issues validating blocks produced by M2 (or vice versa).

### Block and subtree structure

The block structure in Teranode is exactly the same as in the original [Bitcoin design](https://wiki.bitcoinsv.io/index.php/Block). What Teranode does do differently, is the way a block is propagated between nodes. In Teranode a block has been abstracted into the block header and a list of subtrees which are made up of all the transaction ids in that block. For further details on subtrees, [this Coingeek article](https://coingeek.com/new-teranode-features-to-push-bsv-blockchain-capabilities-beyond-the-limits/) may be helpful reading.

This block header and list of subtrees is enough information for a miner to validate the proof of work on the block, since the transaction ids in the subtrees suffice to calculate the Merkle root of that block. The only thing that a miner needs to make sure of, is that the transactions in the subtrees are all valid and in the correct order.

This transaction validation happens every time a subtree is propagated by a competing miner, or around 1 subtree, with 1 million transaction ids, per second, per miner. 

### Latest issue: Cache warm-up leading to forks

Like mentioned above, everytime a subtree is created and propagated by a competing miner, the Teranode needs to validate the entire subtree of transaction ids. This process needs to be done very quickly, and for that reason caching layers are built into the validation.

When the cache is warmed up properly, the Teranode is able to validate the subtrees in a matter of milliseconds. However, when the cache is not warmed up properly, the Teranode is not able to validate the subtrees quickly enough and the subtrees start to pile up. This is what happened this weekend with the final result being that the Teranodes fork off each other and continue on their own chains.

All of the Teranodes could keep up with the 1 million transactions per second, they just all ended up on their own chain.

We are working on multiple avenues to mitigate this problem and will keep you updated.
