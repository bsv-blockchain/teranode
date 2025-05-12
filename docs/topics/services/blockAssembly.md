# 📦 Block Assembly Service

## Index


1. [Description](#1-description)
2. [Functionality](#2-functionality)
    - [2.1. Starting the Block Assembly Service](#21-starting-the-block-assembly-service)
    - [2.2. Receiving Transactions from the TX Validator Service](#22-receiving-transactions-from-the-tx-validator-service)
    - [2.3. Grouping Transactions into Subtrees](#23-grouping-transactions-into-subtrees)
    - [2.4. Creating Mining Candidates](#24-creating-mining-candidates)
    - [2.5. Submit Mining Solution](#25-submit-mining-solution)
    - [2.6. Processing Subtrees and Blocks from other Nodes and Handling Forks and Conflicts](#26-processing-subtrees-and-blocks-from-other-nodes-and-handling-forks-and-conflicts)
    - [2.6.1. The block received is the same as the current chaintip (i.e. the block we have already seen).](#261-the-block-received-is-the-same-as-the-current-chaintip-ie-the-block-we-have-already-seen)
    - [2.6.2. The block received is a new block, and it is the new chaintip.](#262-the-block-received-is-a-new-block-and-it-is-the-new-chaintip)
    - [2.6.3. The block received is a new block, but it represents a fork.](#263-the-block-received-is-a-new-block-but-it-represents-a-fork)
        - [Fork Detection and Assessment](#fork-detection-and-assessment)
        - [Chain Selection and Reorganization Process](#chain-selection-and-reorganization-process)
    - [2.7. Resetting the Block Assembly](#27-resetting-the-block-assembly)
3. [Data Model](#3-data-model)
4. [gRPC Protobuf Definitions](#4-grpc-protobuf-definitions)
5. [Technology](#5-technology)
6. [Directory Structure and Main Files](#6-directory-structure-and-main-files)
7. [How to run](#7-how-to-run)
8. [Configuration options (settings flags)](#8-configuration-options-settings-flags)
    - [Network and Communication Settings](#network-and-communication-settings)
    - [gRPC Client Settings](#grpc-client-settings)
    - [Subtree Management](#subtree-management)
    - [Block and Transaction Processing](#block-and-transaction-processing)
    - [Mining and Difficulty](#mining-and-difficulty)
    - [Service Control](#service-control)
9. [Other Resources](#9-other-resources)

## 1. Description


The Block Assembly Service is responsible for assembling new blocks and adding them to the blockchain.  The block assembly process involves the following steps:

1. **Receiving Transactions from the TX Validator Service**:
    - The block assembly module receives new transactions from the transaction validator service.

2. **Grouping Transactions into Subtrees**:
    - The received transactions are grouped into subtrees.
    - Subtrees represent a hierarchical structure that organizes transactions for more efficient processing and inclusion in a block.

3. **Broadcasting Subtrees to Other Nodes**:
    - Once subtrees are formed, they are broadcasted to other nodes in the network. This is initiated by the block assembly service, which sends a notification to the P2P service (via the Blockchain Service).
    - This step is crucial for maintaining network synchronization and ensuring all nodes have the latest set of subtrees, prior to receiving a block with those subtrees in them. The nodes can validate the subtrees and ensure that they are valid before they are included in a block.

4. **Creating Mining Candidates**:
    - The block assembly continuously creates mining candidates.
    - A mining candidate is essentially a potential block that includes all the subtrees known up to that time, built on top of the longest honest chain.
    - This candidate block is then submitted to the mining module of the node.

5. **Mining Process**:
    - The mining module attempts to find a solution to the cryptographic challenge (proof of work) associated with the mining candidate.
    - If the miner successfully solves the puzzle before other nodes in the network, the block is considered valid and ready to be added to the blockchain.

6. **Adding the Block to the Blockchain**:
    - Once a mining solution is found, the new block is added to the blockchain.

7. **Notifying Other Nodes of the New Block**:
    - After successfully adding the block, other nodes in the network are notified of the new block.

8. **Handling Forks and Conflicts**:
    - The node also handles the resolution of forks in the blockchain and conflicting subtrees or blocks mined by other nodes.
    - This involves choosing between different versions of the blockchain (in case of forks) and resolving conflicts in transactions and subtrees included in other nodes' blocks.

> **Note**: For information about how the Block Assembly service is initialized during daemon startup and how it interacts with other services, see the [Teranode Daemon Reference](../../references/teranodeDaemonReference.md#service-initialization-flow).

A high level diagram:

![Block_Assembly_Service_Container_Diagram.png](img%2FBlock_Assembly_Service_Container_Diagram.png)


Based on its settings, the Block Assembly receives TX notifications from the validator service via 3 different paths:

* A Kafka topic.
* A gRPC client.

The Block Assembly service also subscribes to the Blockchain service, and receives notifications when a new subtree or block is received from another node.

![Block_Assembly_Service_Component_Diagram.png](img%2FBlock_Assembly_Service_Component_Diagram.png)

Finally, note that the Block Assembly benefits of the use of Lustre Fs (filesystem). Lustre is a type of parallel distributed file system, primarily used for large-scale cluster computing. This filesystem is designed to support high-performance, large-scale data storage and workloads.

Specifically for Teranode, these volumes are meant to be temporary holding locations for short-lived file-based data that needs to be shared quickly between various services.

Teranode microservices make use of the Lustre file system in order to share subtree and tx data, eliminating the need for redundant propagation of subtrees over grpc or message queues. The services sharing Subtree data through this system can be seen here:

![lustre_fs.svg](img/plantuml/lustre_fs.svg)


## 2. Functionality

### 2.1. Starting the Block Assembly Service

![block_assembly_init.svg](img%2Fplantuml%2Fblockassembly%2Fblock_assembly_init.svg)

The Block Assembly service initialisation involves setting up internal communication channels and external communication channels, and instantiating the Subtree Processor and Job Store.

The SubTree Processor is the component that groups transactions into subtrees.

The Job Store is a temporary in-memory map that tracks information about the candidate blocks that the miners are attempting to find a solution for.

### 2.2. Receiving Transactions from the TX Validator Service

![block_assembly_add_tx.svg](img%2Fplantuml%2Fblockassembly%2Fblock_assembly_add_tx.svg)

- The TX Validator interacts with the Block Assembly Client. Based on configuration, we send either transactions in batches or individually. This communication can be done over gRPC or through Kafka.


- The Block Assembly client then delegates to the Server, which adds the transactions to the Subtree Processor.


- At a later stage, the Subtree Processor will group the transactions into subtrees, which will be used to create mining candidates.


### 2.3. Grouping Transactions into Subtrees

![block_assembly_add_tx_to_subtree.svg](img%2Fplantuml%2Fblockassembly%2Fblock_assembly_add_tx_to_subtree.svg)

- The Subtree Processor dequeues any transaction request (txReq) received in the previous section, and adds it to the latest (current) subtree.


- If the current subtree is complete (i.e. if it has reached the target length, say 1M transactions), it sends the subtree to the server through an internal Go channel (newSubtreeChan).


- The server then checks if the subtree already exists in the Subtree Store. Otherwise, the server persists the new subtree in the store with a specified (and settings-driven) TTL (Time-To-Live).


- Finally, the server sends a notification to the BlockchainClient to announce the new subtree. This will be propagated to other nodes via the P2P service.


### 2.3.1 Dynamic Subtree Size Adjustment


The Block Assembly service can dynamically adjust the subtree size based on real-time performance metrics when enabled via configuration:

- The system targets a rate of approximately one subtree per second under high throughput conditions
- If subtrees are being created too quickly, the size is automatically increased
- If subtrees are being created too slowly, the size is decreased
- Adjustments are always made to a power of 2 and constrained by minimum and maximum bounds
- Size increases are capped at 2x per block to prevent wild oscillations

Importantly, the system maintains a minimum subtree size threshold, configured via `minimum_merkle_items_per_subtree`. In low transaction volume scenarios, subtrees will only be created once enough transactions have accumulated to meet this minimum size requirement. This means that during periods of low network usage, the target rate of one subtree per second may not be achieved, as the system prioritizes reaching the minimum subtree size before sending.

![block_assembly_dynamic_subtree.svg](img%2Fplantuml%2Fblockassembly%2Fblock_assembly_dynamic_subtree.svg)

This self-tuning mechanism helps maintain consistent processing rates and optimal resource utilization during block assembly, automatically adapting to the node's current processing capabilities and transaction volumes.


### 2.4. Creating Mining Candidates

![block_assembly_get_mining_candidate.svg](img%2Fplantuml%2Fblockassembly%2Fblock_assembly_get_mining_candidate.svg)

- The "Miner" initiates the process by requesting a mining candidate (a block to mine) from the Block Assembly.


- The "Block Assembler" sub-component interacts with the Subtree Processor to obtain completed subtrees that can be included in the mining candidate. It must be noted that there is no subtree limit, Teranode has no restrictions on the maximum block size (hence, neither on the number of subtrees).


- The Block Assembler then calculates the coinbase value and merkle proof for the candidate block.


- The mining candidate, inclusive of the list of subtrees, a coinbase TX, a merkle proof, and associated fees, is returned back to the miner.


- The Block Assembly Server makes status announcements, using the Status Client, about the mining candidate's height and previous hash.


- Finally, the Server tracks the current candidate in the JobStore within a new "job" and its TTL. This information will be retrieved at a later stage, if and when the miner submits a solution to the mining challenge for this specific mining candidate.


### 2.5. Submit Mining Solution

Once a miner solves the mining challenge, it submits a solution to the Block Assembly Service. The solution includes the nonce required to solve the mining challenge.

![block_assembly_submit_mining_solution.svg](img%2Fplantuml%2Fblockassembly%2Fblock_assembly_submit_mining_solution.svg)

- The "Mining" service submits a mining solution (based on a previously provided "mining candidate") to the Block Assembly Service.


- The Block Assembly server adds the submission to a channel (blockSubmissionCh) and processes the submission (submitMiningSolution).


- The job item details are retrieved from the JobStore, and a new block is created with the miner's proof of work.


- The block is validated, and if valid, the coinbase transaction is persisted in the Tx Store.

- The block is added to the blockchain via the Blockchain Client. This will be propagated to other nodes via the P2P service.

- Subtree TTLs are removed, effectively setting the subtrees for removal from the Subtree Store.


- All jobs in the Job Store are deleted.


- In case of an error at any point in the process, the block is invalidated through the Blockchain Client.



### 2.6. Processing Subtrees and Blocks from other Nodes and Handling Forks and Conflicts

The block assembly service subscribes to the Blockchain service, and receives notifications (`model.NotificationType_Block`) when a new block is received from another node. The logic for processing these blocks can be found in the `BlockAssembler.go` file, `startChannelListeners` function.

Once a new block has been received from another node, there are 4 scenarios to consider:

### 2.6.1. The block received is the same as the current chaintip (i.e. the block we have already seen).

In this case, the block notification is redundant, and refers to a block that the service already considers the current chaintip. The service does nothing.

### 2.6.2. The block received is a new block, and it is the new chaintip.

In this scenario, another node has mined a new block that is now the new chaintip.

The service needs to "move up" the block. By this, we mean the process to identify transactions included in the new block (so we do not include them in future blocks) and to process the coinbase UTXOs (so we can include them in future blocks).

![block_assembly_move_up.svg](img%2Fplantuml%2Fblockassembly%2Fblock_assembly_move_up.svg)

1. **Checking for the Best Block Header**:
    - The `BlockAssembler` logs information indicating that the best block header (the header of the most recent block in the chain) is the same as the previous one. It then attempts to "move up" to this new block.

2. **Getting the Block from Blockchain Client**:
    - `b.blockchainClient.GetBlock(ctx, bestBlockchainBlockHeader.Hash())` fetches the block corresponding to the best blockchain block header.

3. **Processing the Block in SubtreeProcessor**:
    - `b.subtreeProcessor.MoveForwardBlock(block)` is called, which initiates the process of updating the subtree processor with the new block.

4. **SubtreeProcessor Handling**:
    - In `MoveForwardBlock`, a channel for error handling is set up and a `moveBlockRequest` is sent to `moveForwardBlockChan`.
    - This triggers the `case moveForwardReq := <-stp.moveForwardBlockChan` in `SubtreeProcessor`, which handles the request to move up a block.
    - `stp.moveForwardBlock(ctx, moveForwardReq.block, false)` is called, which is where the main logic of handling the new block is executed.

5. **MoveForwardBlock Functionality**:
    - This function cleans out transactions from the current subtrees that are also in the new block (to avoid duplication and maintain chain integrity).
    - When `moveForwardBlock` is invoked, it receives a `block` object as a parameter. The function begins with basic validation checks to ensure that the provided block is valid and relevant for processing.
    - The function handles the coinbase transaction (the first transaction in a block, used to reward miners). It processes the unspent transaction outputs (UTXOs) associated with the coinbase transaction.
    - The function then compares the list of transactions that are pending to be mined, as maintained by the Block Assembly, with the transactions included in the newly received block. It identifies transactions from the pending list that were not included in the new block.
    - A list of these remaining transactions is then created. These are the transactions that still require mining.
    - The Subtree Processor assigns these remaining transactions to the current subtree. This subtree represents the first set of transactions that will be included in the next block to be assembled on top of the newly received block. This ensures that pending transactions are carried over for inclusion in future blocks mined by this node.


### 2.6.3. The block received is a new block, but it represents a fork.

In this scenario, the function needs to handle a reorganization. A blockchain reorganization occurs when a node discovers a longer or more difficult chain different from the current local chain. This can happen due to network delays or forks in the blockchain network.

It is the responsibility of the block assembly to always build on top of the longest chain of work. For clarity, it is not the Block Validation or Blockchain services's responsibility to resolve forks. The Block Assembly is notified of the ongoing chains of work, and it makes sure to build on the longest one. If the longest chain of work is different from the current local chain the block assembly was working on, a reorganization will take place.

#### Fork Detection and Assessment
The Block Assembly service implements real-time fork detection through the following mechanisms:

- **Real-time Block Monitoring**: Implemented via `blockchainSubscriptionCh` in the Block Assembler, which continuously monitors for new blocks and chain updates.

- **Fork Detection Criteria**: The Block Assembly service uses three main criteria to detect and handle forks:

1. **Chain Height Tracking**:
    - Maintains current blockchain height through `bestBlockHeight`
    - Compares incoming block heights with current chain tip
    - Used to determine if incoming blocks represent a longer chain

2. **Block Hash Verification**:
    - Uses `HashPrevBlock` to verify block connectivity
    - Ensures each block properly references its predecessor
    - Helps identify where chains diverge

3. **Reorganization Size Protection**:
    - Monitors the size of potential chain reorganizations
    - If a reorganization would require moving more than 5 blocks either backwards or forwards
    - AND the current chain height is greater than 1000 blocks
    - Triggers a full reset of the block assembler as a safety measure against deep reorganizations

The `BlockAssembler` keeps the node synchronized with the network by identifying and switching to the strongest chain (the one with the most accumulated proof of work), ensuring all nodes in the network converge on the same transaction history.

#### Chain Selection and Reorganization Process


During a reorganization, the `BlockAssembler` performs two key operations:
1. Removes (rolls back) transactions from blocks in the current chain, starting from where the fork occurred
2. Applies transactions from the new chain's blocks, ensuring the node switches to the stronger chain


The service automatically manages chain selection through:

1. **Best Chain Detection**:
    - Continuously monitors for new best block headers
    - Compares incoming blocks against current chain tip
    - Automatically triggers reorganization when a better chain is detected

2. **Chain Selection Process**:
    - Accepts the chain with the most accumulated proof of work
    - Performs a safety check on reorganization depth:
        - If the reorganization involves more than 5 blocks in either direction
        - And the current chain height is greater than 1000
        - The block assembler will reset rather than attempt the reorganization
    - Block validation and transaction verification are handled by other services, not the Block Assembly

3. **Chain Switching Process**:
    - Identifies common ancestor between competing chains
    - Rolls back the current chain to a common point with the competing (and stronger) chain
    - Applies new blocks from the competing chain
    - Updates UTXO set and transaction pools accordingly



![block_assembly_reorg.svg](img/plantuml/blockassembly/block_assembly_reorg.svg)

The following diagram illustrates how the Block Assembly service handles a chain reorganization:

- `err = b.handleReorg(ctx, bestBlockchainBlockHeader)`:
    - Calls the `handleReorg` method, passing the current context (`ctx`) and the new best block header from the blockchain network.
    - The reorg process involves rolling back to the last common ancestor block and then adding the new blocks from the network to align the `BlockAssembler`'s blockchain state with the network's state.
    - **Getting Reorg Blocks**:
        - `moveBackBlocks, moveForwardBlocks, err := b.getReorgBlocks(ctx, header)`:
            - Calls `getReorgBlocks` to determine the blocks to move down (to revert) and move up (to apply) for aligning with the network's consensus chain.
            - `header` is the new block header that triggered the reorg.
            - This step involves finding the common ancestor and getting the blocks from the current chain (move down) and the new chain (move up).
    - **Performing Reorg in Subtree Processor**:
        - `b.subtreeProcessor.Reorg(moveBackBlocks, moveForwardBlocks)`:
            - Executes the actual reorg process in the `SubtreeProcessor`, responsible for managing the blockchain's data structure and state.
            - The function reverts the coinbase Txs associated to invalidated blocks (deleting their UTXOs).
            - This step involves reconciling the status of transactions from reverted and new blocks, and coming to a curated new current subtree(s) to include in the next block to mine.

Note: If other nodes propose blocks containing a transaction that Teranode has identified as a double-spend (based on the First-Seen rule), Teranode will only build on top of such blocks when the network has reached consensus on which transaction to accept, even if it differs from Teranode's initial first-seen assessment. For more information, please review the [Double Spend Detection documentation](../architecture/understandingDoubleSpends.md).

### 2.7. Resetting the Block Assembly


The Block Assembly service can be reset to the best block by calling the `ResetBlockAssembly` gRPC method.

1. **State Storage and Retrieval**:
    - `bestBlockchainBlockHeader, meta, err = b.blockchainClient.GetBestBlockHeader(ctx)`: Retrieves the best block header from the blockchain along with its metadata.

2. **Resetting Block Assembly**:
    - The block assembler resets to the new best block header with its height and details.
    - It then calculates which blocks need to be moved down or up to align with the new best block header (`getReorgBlocks`).

3. **Processing the Reorganization**:
    - It attempts to reset the `subtreeProcessor` with the new block headers. If there's an error during this reset, it logs the error, and the block header is re-set to match the `subtreeProcessor`'s current block header.

4. **Updating Assembly State**:
    - Updates internal state with the new best block header and adjusts the height of the best block based on how many blocks were moved up and down.
    - Attempts to set the new state and current blockchain chain.



![block_assembly_reset.svg](img/plantuml/blockassembly/block_assembly_reset.svg)


## 3. Data Model

- [Block Data Model](../datamodel/block_data_model.md): Contain lists of subtree identifiers.
- [Subtree Data Model](../datamodel/subtree_data_model.md): Contain lists of transaction IDs and their Merkle root.
- [UTXO Data Model](../datamodel/utxo_data_model.md): Include additional metadata to facilitate processing.

## 4. gRPC Protobuf Definitions

The Block Assembly Service uses gRPC for communication between nodes. The protobuf definitions used for defining the service methods and message formats can be seen [here](../../references/protobuf_docs/blockassemblyProto.md).


## 5. Technology

- **Go (Golang)**: The service is written in Go.

- **gRPC**: For communication between different services, gRPC is commonly used.

- **Kafka**: Used for tx message queuing and streaming, Kafka can efficiently handle the high throughput of transaction data in a distributed manner.

- **Configuration Management (gocore)**: Uses `gocore` for configuration management, allowing dynamic configuration of service parameters.

- **Networking and Protocol Buffers**: Handles network communications and serializes structured data using Protocol Buffers, a language-neutral, platform-neutral, extensible mechanism for serializing structured data.

## 6. Directory Structure and Main Files

```
/services/blockassembly
├── BlockAssembler.go              - Main logic for assembling blocks.
├── BlockAssembler_test.go         - Tests for BlockAssembler.go.
├── Client.go                      - Client-side logic for block assembly.
├── Interface.go                   - Interface definitions for block assembly.
├── Server.go                      - Server-side logic for block assembly.
├── Server_test.go                 - Tests for Server.go.
├── blockassembly_api              - Directory for block assembly API.
│   ├── blockassembly_api.pb.go    - Generated protobuf code.
│   ├── blockassembly_api.proto    - Protobuf definitions.
│   ├── blockassembly_api_grpc.pb.go - gRPC generated code.
├── blockassembly_system_test.go   - System-level integration tests.
├── data.go                        - Data structures used in block assembly.
├── data_test.go                   - Tests for data.go.
├── metrics.go                     - Metrics collection for block assembly.
├── mining                         - Directory for mining-related functionality.
├── remotettl.go                   - Management of remote TTL (Time To Live) values.
└── subtreeprocessor               - Directory for subtree processing.
    ├── SubtreeProcessor.go        - Main logic for processing subtrees.
    ├── SubtreeProcessor_test.go   - Tests for SubtreeProcessor.go.
    ├── metrics.go                 - Metrics specific to subtree processing.
    ├── options.go                 - Configuration options for subtree processing.
    ├── queue.go                   - Queue implementation for subtree processing.
    ├── queue_test.go              - Tests for queue.go.
    ├── testdata                   - Test data for subtree processor tests.
    └── txIDAndFee.go              - Handling transaction IDs and fees.
```

## 7. How to run

To run the Block Assembly Service locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -BlockAssembly=1
```

Please refer to the [Locally Running Services Documentation](../../howto/locallyRunningServices.md) document for more information on running the Block Assembly Service locally.


## 8. Configuration options (settings flags)

The Block Assembly service uses the following configuration options:

### Network and Communication Settings

1. **`blockassembly_grpcAddress`**: Specifies the gRPC address for the block assembly service.
2. **`network`**: Defines the network setting (e.g., "mainnet"). Default: "mainnet".

### gRPC Client Settings

3. **`blockassembly_grpcMaxRetries`**: Maximum number of gRPC retries. Default: 3.
4. **`blockassembly_grpcRetryBackoff`**: Backoff duration for gRPC retries. Default: 2 seconds.
5. **`blockassembly_sendBatchSize`**: Batch size for sending operations. Default: 0 (no batching).
6. **`blockassembly_sendBatchTimeout`**: Timeout for batch send operations in milliseconds. Default: 100.

### Subtree Management

7. **`blockassembly_subtreeTTL`**: Time-to-live (in minutes) for subtrees stored in the cache. Default: 120 minutes.
8. **`blockassembly_newSubtreeChanBuffer`**: Buffer size for the channel that handles new subtree processing. Default: 1,000.
9. **`blockassembly_subtreeRetryChanBuffer`**: Buffer size for the channel dedicated to retrying subtree storage operations. Default: 1,000.
10. **`blockassembly_subtreeTTLConcurrency`**: Concurrency level for subtree TTL operations. Default: 32.
11. **`initial_merkle_items_per_subtree`**: Initial number of items per subtree. Default: 1,048,576.
12. **`blockassembly_useDynamicSubtreeSize`**: Enables automatic adjustment of subtree size based on real-time performance. Default: false.
13. **`minimum_merkle_items_per_subtree`**: Minimum allowed size for subtrees (must be a power of 2). Default: 1,024.
14. **`maximum_merkle_items_per_subtree`**: Maximum allowed size for subtrees. Default: 1,048,576 (1024*1024).

### Block and Transaction Processing

12. **`blockassembly_maxBlockReorgRollback`**: Maximum number of blocks the service can roll back in the event of a blockchain reorganization. Default: 100.
13. **`blockassembly_maxBlockReorgCatchup`**: Maximum number of blocks to catch up during a blockchain reorganization. Default: 100.
14. **`tx_chan_buffer_size`**: Buffer size for transaction channel. Default: 0 (unlimited).
15. **`blockassembly_subtreeProcessorBatcherSize`**: Batch size for subtree processor. Default: 1000.
16. **`double_spend_window_millis`**: Window for detecting double spends in milliseconds. Default: 2000.
17. **`blockassembly_moveBackBlockConcurrency`**: Concurrency for moving down block operations. Default: 64.
18. **`blockassembly_processRemainderTxHashesConcurrency`**: Concurrency for processing remainder transaction hashes. Default: 64.
19. **`blockassembly_subtreeProcessorConcurrentReads`**: Number of concurrent reads for subtree processor. Default: 4.

### Mining and Difficulty

20. **`blockassembly_SubmitMiningSolution_waitForResponse`**: Whether to wait for response when submitting mining solutions. Default: true.

### Service Control

21. **`blockassembly_disabled`**: A toggle to enable or disable the block assembly functionality altogether. Useful for testing or maintenance scenarios. Default: false.
22. **`fsm_state_restore`**: Boolean flag for FSM (Finite State Machine) state restoration. When enabled, the service will attempt to restore its previous state after restart. Default: false.


## 9. Other Resources

- [Block Assembly Reference](../../references/services/blockassembly_reference.md)
- [Handling Double Spends](../architecture/understandingDoubleSpends.md)
