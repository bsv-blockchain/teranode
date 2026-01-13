# 🔍 Block Validation Service

## Index

1. [Description](#1-description)
2. [Functionality](#2-functionality)
    - [2.1. Receiving blocks for validation](#21-receiving-blocks-for-validation)
    - [2.2. Validating blocks](#22-validating-blocks)
    - [2.2.1. Overview](#221-overview)
    - [2.2.2. Catching up after a parent block is not found](#222-catching-up-after-a-parent-block-is-not-found)
    - [2.2.3. Quick Validation for Checkpointed Blocks](#223-quick-validation-for-checkpointed-blocks)
    - [2.2.4. Validating the Subtrees](#224-validating-the-subtrees)
    - [2.2.5. Block Data Validation](#225-block-data-validation)
    - [2.2.6. Transaction Re-presentation Detection](#226-transaction-re-presentation-detection)
    - [2.3. Marking Txs as mined](#23-marking-txs-as-mined)
3. [gRPC Protobuf Definitions](#3-grpc-protobuf-definitions)
4. [Data Model](#4-data-model)
5. [Technology](#5-technology)
6. [Directory Structure and Main Files](#6-directory-structure-and-main-files)
7. [How to run](#7-how-to-run)
8. [Configuration options (settings flags)](#8-configuration-options-settings-flags)
9. [Other Resources](#9-other-resources)

## 1. Description

The Block Validator is responsible for ensuring the integrity and consistency of each block before it is added to the blockchain. It performs several key functions:

1. **Validation of Block Structure**: Verifies that each block adheres to the defined structure and format, and that their subtrees are known and valid.

2. **Merkle Root Verification**: Confirms that the Merkle root in the block header correctly represents the subtrees in the block, ensuring data integrity.

3. **Block Header Verification**: Validates the block header, including the proof of work , timestamp, and reference to the previous block, maintaining the blockchain's unbroken chain.

![Block_Validation_Service_Container_Diagram.png](img/Block_Validation_Service_Container_Diagram.png)

The Block Validation Service:

- Receives new blocks from the Legacy Service. The Legacy Service has received them from other nodes on the network.
- Validates the blocks, after fetching them from the remote asset server.
- Updates stores, and notifies the blockchain service of the new block.

The Legacy Service communicates with the Block Validation over the gRPC protocol.

![Block_Validation_Service_Component_Diagram.png](img/Block_Validation_Service_Component_Diagram.png)

### Detailed Component Diagram

The detailed component diagram below shows the internal architecture of the Block Validation Service:

![Block_Validation_Component](img/plantuml/blockvalidation/Block_Validation_Component.svg)

> **Note**: For information about how the Block Validation service is initialized during daemon startup and how it interacts with other services, see the [Teranode Daemon Reference](../../references/teranodeDaemonReference.md#service-initialization-flow).

Finally, note that the Block Validation service benefits of the use of Lustre Fs (filesystem). Lustre is a type of parallel distributed file system, primarily used for large-scale cluster computing. This filesystem is designed to support high-performance, large-scale data storage and workloads.
Specifically for Teranode, these volumes are meant to be temporary holding locations for short-lived file-based data that needs to be shared quickly between various services
Teranode microservices make use of the Lustre file system in order to share subtree and tx data, eliminating the need for redundant propagation of subtrees over grpc or message queues. The services sharing Subtree data through this system can be seen here:

![lustre_fs.svg](img/plantuml/lustre_fs.svg)

## 2. Functionality

The block validator is a service that validates blocks. After validating them, it will update the relevant stores and blockchain accordingly.

### 2.1. Receiving blocks for validation

![block_validation_p2p_block_found.svg](img/plantuml/blockvalidation/block_validation_p2p_block_found.svg)

- The Legacy Service is responsible for receiving new blocks from the network. When a new block is found, it will notify the block validation service via the `BlockFound()` gRPC endpoint.
- The block validation service will then check if the block is already known. If not, it will start the validation process.
- The block is added to a channel for processing. The channel is used to ensure that the block validation process is asynchronous and non-blocking.

### 2.2. Validating blocks

#### 2.2.1. Overview

![block_validation_p2p_block_validation.svg](img/plantuml/blockvalidation/block_validation_p2p_block_validation.svg)

- As seen in the section 2.1, a new block is queued for validation in the blockFoundCh. The block validation server will pick it up and start the validation process.
- The server will request the block data from the remote node (`DoHTTPRequest()`).
- If the parent block is not known, it will be added to the catchupCh channel for processing. We stop at this point, as we can no longer proceed. The catchup process will be explained in the next section (section 2.2.2).
- If the parent is known, the block will be validated.
    - First, the service validates all the block subtrees.
        - For each subtree, we check if it is known. If not, we kick off a subtree validation process (see section 2.2.3 for more details).
    - The validator retrieves the last 100 block headers, which are used to validate the block data. We can see more about this specific step in the section 2.2.4.
    - The validator stores the coinbase Tx in the UTXO Store and the Tx Store.
    - The validator adds the block to the Blockchain.
    - For each Subtree in the block, the validator updates the TTL (Time To Live) to zero for the subtree. This allows the Store to clear out data the services will no longer use.
    - For each Tx for each Subtree, we set the Tx as mined in the UTXO Store. This allows the UTXO Store to know which block(s) the Tx is in.
    - Should an error occur during the validation process, the block will be invalidated and removed from the blockchain.

Note - there is a `optimisticMining` setting that allows to reverse the block validation and block addition to the blockchain steps.

- In the regular mode, the block is validated first, and, if valid, added to the block.
- If `optimisticMining` is on, the block is optimistically added to the blockchain right away, and then validated in the background next. If it was to be found invalid after validation, it would be removed from the blockchain. This mode is not recommended for production use, as it can lead to a temporary fork in the blockchain. It however can be useful for performance testing purposes.

#### 2.2.2. Catching up after a parent block is not found

![block_validation_p2p_block_catchup.svg](img/plantuml/blockvalidation/block_validation_p2p_block_catchup.svg)

When a block parent is not found in the local blockchain, the node will start a catchup process. The catchup process will iterate through the parent blocks until it finds a known block in the blockchain.

When a block is not known, it will be requested from the remote node. Once received, it will be queued for validation (effectively starting the process of validation for the parent block from the beginning, as seen in 2.3.1).

Notice that, when catching up, the Block Validator will set the machine state of the node to `CATCHING_UP`. This is done to prevent the node from processing new blocks while it is still catching up. The node will only assemble or process new blocks once it has caught up with the blockchain. For more information on this, please refer to the [State Management](../architecture/stateManagement.md)  documentation.

During the catchup process, the system tracks invalid blocks. If a block fails validation during catchup, it is marked as invalid in the blockchain store. This prevents invalid blocks from corrupting the chain state and allows the system to avoid reprocessing known invalid blocks. The system also maintains metrics on peer quality to identify and avoid peers that provide invalid blocks.

#### 2.2.3. Quick Validation for Checkpointed Blocks

For blocks that are below known checkpoints in the blockchain, the Block Validation service employs an optimized quick validation path that significantly improves synchronization performance. This mechanism is particularly effective during initial blockchain synchronization.

![quick_validation.svg](img/plantuml/blockvalidation/quick_validation.svg)

##### Quick Validation Process

The quick validation system operates in two distinct phases:

1. **UTXO Creation Phase**: All UTXOs for the block's transactions are created in parallel. The system:

    - Processes transactions concurrently using configurable parallelism (`blockvalidation_concurrency_createAllUTXOs`)
    - Creates UTXOs with the block's ID and mined status pre-set
    - Handles existing UTXO conflicts gracefully for recovery scenarios

2. **Transaction Spending Phase**: Validates transactions by spending their inputs in parallel. The system:

    - Uses optimized validator options that skip expensive policy checks for mined blocks
    - Processes transactions with configurable concurrency (`blockvalidation_concurrency_spendAllTransactions`)
    - Validates transaction relationships and dependencies efficiently

##### Checkpoint-Based Optimization

The quick validation path is only applied to blocks below verified checkpoints:

- Checkpoints are known valid block heights that serve as trust anchors
- Blocks below checkpoints skip full script validation since they are known to be valid
- The checkpoint height is configurable via `blockvalidation_quickValidationCheckpointHeight`

##### Performance Benefits

Quick validation provides substantial performance improvements:

- Approximately 10x faster processing for historical blocks below checkpoints
- Parallel UTXO operations maximize throughput
- Elimination of redundant policy checks for known valid blocks
- Reduced memory footprint through on-demand transaction extension

##### Subtree and Transaction Processing

During quick validation, the system also:

- Creates subtree metadata files (`.subtreeMeta`) automatically
- Reconstructs full subtree data (`.subtree`) with proper fee and size information
- Stores transactions in non-extended format to reduce storage overhead
- Extends transaction data on-demand only when needed for validation
    - Transactions received in standard Bitcoin format are automatically extended in-memory
    - Parent transaction data is retrieved from the UTXO store as needed
    - Extension is transparent and does not affect storage format

If quick validation encounters any errors, the system automatically falls back to normal validation to ensure correctness.

#### 2.2.4. Validating the Subtrees

Should the validation process for a block encounter a subtree it does not know about, it can request its processing off the Subtree Validation service.

![block_validation_subtree_validation_request.svg](img/plantuml/subtreevalidation/block_validation_subtree_validation_request.svg)

If any transaction under the subtree is also missing, the subtree validation process will kick off a recovery process for those transactions.

#### 2.2.5. Block Data Validation

As part of the overall block validation, the service will validate the block data, ensuring the format and integrity of the data, as well as confirming that coinbase tx, subtrees and transactions are valid. This is done in the `Valid()` method under the `Block` struct.

![block_data_validation.svg](img/plantuml/blockvalidation/block_data_validation.svg)

Effectively, the following validations are performed:

- The hash of the previous block must be known and valid. Teranode must always build a block on a previous block that it recognizes as the longest chain.

- The Proof of Work of a block must satisfy the difficulty target (Proof of Work higher than nBits in block header).

- The Merkle root of all transactions in a block must match the value of the Merkle root in the block header.

- A block must include at least one transaction, which is the Coinbase transaction.

- A block timestamp must not be too far in the past or the future.

    - The block time specified in the header must be larger than the Median-Time-Past (MTP) calculated from the previous block index. MTP is calculated by taking the timestamps of the last 11 blocks and finding the median (More details in BIP113).
    - The block time specified in the header must not be larger than the adjusted current time plus two hours ("maximum future block time").

- The first transaction in a block must be Coinbase. The transaction is Coinbase if the following requirements are satisfied:

    - The Coinbase transaction has exactly one input.
    - The input is null, meaning that the input's previous hash is 0000…0000 and the input's previous index is 0xFFFFFFFF.
    - The Coinbase transaction must start with the serialized block height, to ensure block and transaction uniqueness.

- The Coinbase transaction amount may not exceed block subsidy and all transaction fees (block reward).

#### 2.2.6. Transaction Re-presentation Detection

The Block Validation service implements a robust mechanism for detecting re-presented transactions using bloom filters. This mechanism, implemented in the `validOrderAndBlessed` function, is critical for preventing double-spending and ensuring transaction integrity in the blockchain.

![bloom_filter_validation.svg](img/plantuml/blockvalidation/bloom_filter_validation.svg)

##### Bloom Filter Implementation

Teranode maintains bloom filters for recent blocks to efficiently detect re-presented transactions:

- **Creation**: Each validated block generates a bloom filter containing all of its transaction hashes
- **Storage**: Bloom filters are stored in both memory (for active validation) and in the subtree store (for persistence)
- **Retention**: Filters are maintained for a configurable number of recent blocks (`blockvalidation_bloom_filter_retention_size`)
- **TTL Ordering**: The system enforces a strict TTL (Time-To-Live) ordering: txmetacache < utxo store < bloom filter
    - This ensures that even if a transaction is pruned from txmetacache, the bloom filter can still detect its re-presentation
    - The longer retention period for bloom filters provides an extended window for detecting re-presented transactions

##### The validOrderAndBlessed Mechanism

The `validOrderAndBlessed` function performs several critical validations during block processing:

1. **Transaction Ordering Validation**:

    - Ensures child transactions appear after their parent transactions within the same block
    - For each transaction, verifies that all of its parent transactions either appear earlier in the same block or exist in a previous block on the current chain

2. **Re-presented Transaction Detection**:

    - Efficiently checks if transactions have already been mined in the current chain using bloom filters
    - For potential matches in the bloom filter (which may include false positives), performs definitive verification against the txMetaStore
    - Rejects blocks containing transactions that have already been mined in the current chain

3. **Duplicate Input Prevention**:

    - Tracks all inputs being spent within the block to detect duplicate spends
    - If a transaction is found to be already mined in another block on the same chain, the new block is marked as invalid
    - Ensures no two transactions in the block spend the same input

4. **Orphaned Transaction Prevention**:

    - Verifies that parent transactions of each transaction either exist in the current block (before the child) or in a previous block on the current chain
    - Prevents situations where transactions depend on parents that don't exist or aren't accessible

This comprehensive validation mechanism operates with high concurrency (configurable via `block_validOrderAndBlessedConcurrency`) to maintain performance while ensuring the integrity of the blockchain by preventing double-spends and transaction re-presentations.

### 2.3. Marking Txs as mined

When a block is validated, the transactions in the block are marked as mined in the UTXO store. This process includes:

1. **Updating Transaction Status**: The Block Validation service marks each transaction as mined by setting its block information.

2. **Unsetting the Locked Flag**: For any transaction that still has the "locked" flag set, the flag is unset during the mined transaction update process.

3. **Storing Subtree Information**: The service also stores the subtree index in the block where the transaction was located, enabling more efficient transaction lookups.

The Block Validation service is exclusively responsible for marking block transactions as mined and ensuring their flags are properly updated, regardless of whether the transaction was mined by the local Block Assembly service or by another node in the network.

As a first step, either the `Block Validation` (after a remotely mined block is validated) or the `Block Assembly` (if a block is locally mined) marks the block subtrees as "set", by invoking the `Blockchain` `SetBlockSubtreesSet` gRPC call, as shown in the diagram below.

![blockchain_setblocksubtreesset.svg](img/plantuml/blockchain/blockchain_setblocksubtreesset.svg)

The `Blockchain` client then notifies subscribers (in this case, the `BlockValidation` service) of a new `NotificationType_BlockSubtreesSet` event.
The `BlockValidation` proceeds to mark all transactions within the block as "mined" in the `UTXOStore`. This allows to identify in which block a given tx was mined. See diagram below:

![block_validation_set_tx_mined.svg](img/plantuml/blockvalidation/block_validation_set_tx_mined.svg)

> **For a comprehensive explanation of the two-phase commit process across the entire system, including how Block Validation plays a role in the second phase, see the [Two-Phase Transaction Commit Process](../features/two_phase_commit.md) documentation.**
>
## 3. gRPC Protobuf Definitions

The Block Validation Service uses gRPC for communication between nodes. The protobuf definitions used for defining the service methods and message formats can be seen in the [Block Validation protobuf documentation](../../references/protobuf_docs/blockvalidationProto.md).

## 4. Data Model

- [Block Data Model](../datamodel/block_data_model.md): Contain lists of subtree identifiers.
- [Subtree Data Model](../datamodel/subtree_data_model.md): Contain lists of transaction IDs and their Merkle root.
- [Transaction Data Model](../datamodel/transaction_data_model.md): Comprehensive documentation covering both standard Bitcoin format and Extended Format (BIP-239), including automatic format conversion during block validation.
- [UTXO Data Model](../datamodel/utxo_data_model.md): UTXO and UTXO Metadata data models for managing unspent transaction outputs.

## 5. Technology

1. **Go Programming Language (Golang)**.

2. **gRPC (Google Remote Procedure Call)**:

    - Used for implementing server-client communication. gRPC is a high-performance, open-source framework that supports efficient communication between services.

3. **Blockchain Data Stores**:

    - Integration with various stores such as UTXO (Unspent Transaction Output) store, blob store, and transaction metadata store.

4. **Caching Mechanisms (ttlcache)**:

    - Uses `ttlcache`, a Go library for in-memory caching with time-to-live settings, to avoid redundant processing and improve performance.

5. **Configuration Management (gocore)**:

    - Uses `gocore` for configuration management, allowing dynamic configuration of service parameters.

6. **Networking and Protocol Buffers**:

    - Handles network communications and serializes structured data using Protocol Buffers, a language-neutral, platform-neutral, extensible mechanism for serializing structured data.

7. **Synchronization Primitives (sync)**:

    - Utilizes Go's `sync` package for synchronization primitives like mutexes, aiding in managing concurrent access to shared resources.

## 6. Directory Structure and Main Files

```text
./services/blockvalidation
│
├── BlockValidation.go             - Contains the core logic for block validation.
├── BlockValidation_test.go        - Unit tests for the `BlockValidation.go` functionalities.
├── Client.go                      - Client-side logic or API for interacting with the block validation service.
├── Interface.go                   - Defines an interface for the block validation service, outlining the methods that any implementation of the service should provide.
├── Server.go                      - Contains the server-side implementation for the block validation service, handling incoming requests and providing validation services.
├── Server_test.go                 - Unit tests for the `Server.go` functionalities,
├── blockvalidation_api
│   ├── blockvalidation_api.pb.go         - Auto-generated file from protobuf definitions, containing Go bindings for the API.
│   ├── blockvalidation_api.proto         - Protocol Buffers definition file for the block validation API.
│   └── blockvalidation_api_grpc.pb.go    - gRPC (Google's RPC framework) specific implementation file for the block validation API.
├── metrics.go                     - Metrics collection and monitoring of the block validation service's performance.
├── ttl_queue.go                   - Implements a time-to-live (TTL) queue, for managing caching within the service.
├── txmetacache.go                 - Transaction metadata cache, used to improve performance and efficiency in transaction data access.
└── txmetacache_test.go            - Unit tests for the `txmetacache.go` functionalities.
```

## 7. How to run

To run the Block Validation Service locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run -BlockValidation=1
```

Please refer to the [Locally Running Services Documentation](../../howto/locallyRunningServices.md) document for more information on running the Block Validation Service locally.

## 8. Configuration options (settings flags)

For comprehensive configuration documentation including all settings, defaults, and interactions, see the [block Validation Settings Reference](../../references/settings/services/blockvalidation_settings.md).

## 9. Other Resources

[Block Validation Reference](../../references/services/blockvalidation_reference.md)
