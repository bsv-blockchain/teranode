# 🔍 Block Validation Service

## Index

1. [Description](#1-description)
2. [Functionality](#2-functionality)
    - [2.1. Receiving blocks for validation](#21-receiving-blocks-for-validation)
    - [2.2. Validating blocks](#22-validating-blocks)
        - [2.2.1. Overview](#221-overview)
        - [2.2.2. Catching up after a parent block is not found](#222-catching-up-after-a-parent-block-is-not-found)
        - [2.2.3. Validating the Subtrees](#223-validating-the-subtrees)
        - [2.2.4. Block Data Validation](#224-block-data-validation)
    - [2.3. Marking Txs as mined](#23-marking-txs-as-mined)
3. [gRPC Protobuf Definitions](#3-grpc-protobuf-definitions)
4. [Data Model](#4-data-model)
5. [Technology](#5-technology)
6. [Directory Structure and Main Files](#6-directory-structure-and-main-files)
7. [How to run](#7-how-to-run)
8. [Configuration options (settings flags)](#8-configuration-options-settings-flags)
    - [Network and Communication Settings](#network-and-communication-settings)
    - [Kafka and Concurrency Settings](#kafka-and-concurrency-settings)
    - [Performance and Optimization](#performance-and-optimization)
    - [Storage and State Management](#storage-and-state-management)
9. [Other Resources](#9-other-resources)

## 1. Description

The Block Validator is responsible for ensuring the integrity and consistency of each block before it is added to the blockchain. It performs several key functions:

1. **Validation of Block Structure**: Verifies that each block adheres to the defined structure and format, and that their subtrees are known and valid.

2. **Merkle Root Verification**: Confirms that the Merkle root in the block header correctly represents the subtrees in the block, ensuring data integrity.

3. **Block Header Verification**: Validates the block header, including the proof of work , timestamp, and reference to the previous block, maintaining the blockchain's unbroken chain.

![Block_Validation_Service_Container_Diagram.png](img/Block_Validation_Service_Container_Diagram.png)

The Block Validation Service:

* Receives new blocks from the Legacy Service. The Legacy Service has received them from other nodes on the network.
* Validates the blocks, after fetching them from the remote asset server.
* Updates stores, and notifies the blockchain service of the new block.

The Legacy Service communicates with the Block Validation over the gRPC protocol.

![Block_Validation_Service_Component_Diagram.png](img/Block_Validation_Service_Component_Diagram.png)

> **Note**: For information about how the Block Validation service is initialized during daemon startup and how it interacts with other services, see the [Teranode Daemon Reference](../../references/teranodeDaemonReference.md#service-initialization-flow).

To improve performance, the Block Validation Service uses a caching mechanism for UTXO Meta data, called `Tx Meta Cache`. This prevents repeated fetch calls to the store by retaining recently loaded transactions in memory (for a limited time). This can be enabled or disabled via the `blockvalidation_txMetaCacheEnabled` setting. The caching mechanism is implemented in the `txmetacache` package, and is used by the Block Validation Service:

```go
	// create a caching tx meta store
	if gocore.Config().GetBool("blockvalidation_txMetaCacheEnabled", true) {
		logger.Infof("Using cached version of tx meta store")
		bVal.txMetaStore = newTxMetaCache(txMetaStore)
	} else {
		bVal.txMetaStore = txMetaStore
	}
```


Finally, note that the Block Validation service benefits of the use of Lustre Fs (filesystem). Lustre is a type of parallel distributed file system, primarily used for large-scale cluster computing. This filesystem is designed to support high-performance, large-scale data storage and workloads.
Specifically for Teranode, these volumes are meant to be temporary holding locations for short-lived file-based data that needs to be shared quickly between various services
Teranode microservices make use of the Lustre file system in order to share subtree and tx data, eliminating the need for redundant propagation of subtrees over grpc or message queues. The services sharing Subtree data through this system can be seen here:

![lustre_fs.svg](img/plantuml/lustre_fs.svg)


## 2. Functionality

The block validator is a service that validates blocks. After validating them, it will update the relevant stores and blockchain accordingly.


### 2.1. Receiving blocks for validation

![block_validation_p2p_block_found.svg](img/plantuml/blockvalidation/block_validation_p2p_block_found.svg)

* The Legacy Service is responsible for receiving new blocks from the network. When a new block is found, it will notify the block validation service via the `BlockFound()` gRPC endpoint.
* The block validation service will then check if the block is already known. If not, it will start the validation process.
* The block is added to a channel for processing. The channel is used to ensure that the block validation process is asynchronous and non-blocking.


### 2.2. Validating blocks

#### 2.2.1. Overview

![block_validation_p2p_block_validation.svg](img/plantuml/blockvalidation/block_validation_p2p_block_validation.svg)

* As seen in the section 2.1, a new block is queued for validation in the blockFoundCh. The block validation server will pick it up and start the validation process.
* The server will request the block data from the remote node (`DoHTTPRequest()`).
* If the parent block is not known, it will be added to the catchupCh channel for processing. We stop at this point, as we can no longer proceed. The catchup process will be explained in the next section (section 2.2.2).
* If the parent is known, the block will be validated.
  * First, the service validates all the block subtrees.
    * For each subtree, we check if it is known. If not, we kick off a subtree validation process (see section 2.2.3 for more details).
  * The validator retrieves the last 100 block headers, which are used to validate the block data. We can see more about this specific step in the section 2.2.4.
  * The validator stores the coinbase Tx in the UTXO Store and the Tx Store.
  * The validator adds the block to the Blockchain.
  * For each Subtree in the block, the validator updates the TTL (Time To Live) to zero for the subtree. This allows the Store to clear out data the services will no longer use.
  * For each Tx for each Subtree, we set the Tx as mined in the UTXO Store. This allows the UTXO Store to know which block(s) the Tx is in.
  * Should an error occur during the validation process, the block will be invalidated and removed from the blockchain.

Note - there is a `optimisticMining` setting that allows to reverse the block validation and block addition to the blockchain steps.
* In the regular mode, the block is validated first, and, if valid, added to the block.
* If `optimisticMining` is on, the block is optimistically added to the blockchain right away, and then validated in the background next. If it was to be found invalid after validation, it would be removed from the blockchain. This mode is not recommended for production use, as it can lead to a temporary fork in the blockchain. It however can be useful for performance testing purposes.


#### 2.2.2. Catching up after a parent block is not found

![block_validation_p2p_block_catchup.svg](img/plantuml/blockvalidation/block_validation_p2p_block_catchup.svg)

When a block parent is not found in the local blockchain, the node will start a catchup process. The catchup process will iterate through the parent blocks until it finds a known block in the blockchain.

When a block is not known, it will be requested from the remote node. Once received, it will be queued for validation (effectively starting the process of validation for the parent block from the beginning, as seen in 2.3.1).

Notice that, when catching up, the Block Validator will set the machine state of the node to `CATCHING_UP`. This is done to prevent the node from processing new blocks while it is still catching up. The node will only assemble or process new blocks once it has caught up with the blockchain. For more information on this, please refer to the [State Management](../architecture/stateManagement.md)  documentation.

#### 2.2.3. Validating the Subtrees

Should the validation process for a block encounter a subtree it does not know about, it can request its processing off the Subtree Validation service.

![block_validation_subtree_validation_request.svg](img/plantuml/subtreevalidation/block_validation_subtree_validation_request.svg)

If any transaction under the subtree is also missing, the subtree validation process will kick off a recovery process for those transactions.


#### 2.2.4. Block Data Validation

As part of the overall block validation, the service will validate the block data, ensuring the format and integrity of the data, as well as confirming that coinbase tx, subtrees and transactions are valid. This is done in the `Valid()` method under the `Block` struct.

![block_data_validation.svg](img/plantuml/blockvalidation/block_data_validation.svg)

Effectively, the following validations are performed:

- The hash of the previous block must be known and valid. Teranode must always build a block on a previous block that it recognizes as the longest chain.

- The Proof of Work of a block must satisfy the difficulty target (Proof of Work higher than nBits in block header).

- The Merkle root of all transactions in a block must match the value of the Merkle root in the block header.

- A block must include at least one transaction, which is the Coinbase transaction.

- A block timestamp must not be too far in the past or the future.
    - The block time specified in the header must be larger than the Median-Time-Past (MTP) calculated from the previous block index. MTP is calculated by taking the timestamps of the last 11 blocks and finding the median (More details in BIP113).
    - The block time specified in the header must not be larger than the adjusted current time plus two hours (“maximum future block time”).

- The first transaction in a block must be Coinbase. The transaction is Coinbase if the following requirements are satisfied:
    - The Coinbase transaction has exactly one input.
    - The input is null, meaning that the input’s previous hash is 0000…0000 and the input’s previous index is 0xFFFFFFFF.
    - The Coinbase transaction must start with the serialized block height, to ensure block and transaction uniqueness.

- The Coinbase transaction amount may not exceed block subsidy and all transaction fees (block reward).

### 2.3. Marking Txs as mined

When a block is validated, the transactions in the block are marked as mined in the UTXO store. This process includes:

1. **Updating Transaction Status**: The Block Validation service marks each transaction as mined by setting its block information.

2. **Unsetting the Unspendable Flag**: For any transaction that still has the "unspendable" flag set, the flag is unset during the mined transaction update process.

3. **Storing Subtree Information**: The service also stores the subtree index in the block where the transaction was located, enabling more efficient transaction lookups.

The Block Validation service is exclusively responsible for marking block transactions as mined and ensuring their flags are properly updated, regardless of whether the transaction was mined by the local Block Assembly service or by another node in the network.

As a first step, either the `Block Validation` (after a remotely mined block is validated) or the `Block Assembly` (if a block is locally mined) marks the block subtrees as "set", by invoking the `Blockchain` `SetBlockSubtreesSet` gRPC call, as shown in the diagram below.

![blockchain_setblocksubtreesset.svg](img/plantuml/blockchain/blockchain_setblocksubtreesset.svg)

The `Blockchain` client then notifies subscribers (in this case, the `BlockValidation` service) of a new `NotificationType_BlockSubtreesSet` event.
The `BlockValidation` proceeds to mark all transactions within the block as "mined" in the `UTXOStore`. This allows to identify in which block a given tx was mined. See diagram below:

![block_validation_set_tx_mined.svg](img/plantuml/blockvalidation/block_validation_set_tx_mined.svg)

> **For a comprehensive explanation of the two-phase commit process across the entire system, including how Block Validation plays a role in the second phase, see the [Two-Phase Transaction Commit Process](../features/two_phase_commit.md) documentation.**


## 3. gRPC Protobuf Definitions

The Block Validation Service uses gRPC for communication between nodes. The protobuf definitions used for defining the service methods and message formats can be seen [here](../../references/protobuf_docs/subtreevalidationProto.md).


## 4. Data Model

- [Block Data Model](../datamodel/block_data_model.md): Contain lists of subtree identifiers.
- [Subtree Data Model](../datamodel/subtree_data_model.md): Contain lists of transaction IDs and their Merkle root.
- [Extended Transaction Data Model](../datamodel/transaction_data_model.md): Includes additional metadata to facilitate processing.
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

```
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
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -BlockValidation=1
```

Please refer to the [Locally Running Services Documentation](../../howto/locallyRunningServices.md) document for more information on running the Block Validation Service locally.

## 8. Configuration options (settings flags)

The Block Validation service configuration can be adjusted through environment variables or command-line flags. This section provides a comprehensive overview of all available configuration options organized by functional category.

### Network and Communication Settings

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `blockvalidation_grpcAddress` | string | "localhost:8088" | Address that other services use to connect to this service | Affects how other services discover and communicate with the Block Validation service |
| `blockvalidation_grpcListenAddress` | string | ":8088" | Network interface and port the service listens on for gRPC connections | Controls network binding and accessibility of the service |

### Block Processing Pipeline

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `blockvalidation_blockFoundCh_buffer_size` | int | 1000 | Buffer size for the channel handling newly discovered blocks | Affects system's ability to handle bursts of new blocks without blocking |
| `blockvalidation_maxPreviousBlockHeadersToCheck` | uint64 | 100 | Maximum number of previous block headers to check during validation | Defines the depth of historical validation performed |
| `blockvalidation_validateBlockSubtreesConcurrency` | int | max(4, runtime.NumCPU()/2) | Number of concurrent goroutines for validating block subtrees | Higher values increase CPU utilization but improve throughput |
| `blockvalidation_bloom_filter_retention_size` | uint32 | 100 | Number of recent blocks to maintain bloom filters for | Affects memory usage and duplicate transaction detection efficiency |
| `blockvalidation_optimistic_mining` | bool | true | When enabled, blocks are conditionally accepted before full validation | Dramatically improves throughput at the cost of temporary chain inconsistency if validation fails |
| `blockvalidation_previous_block_header_count` | uint64 | 100 | Number of previous block headers to maintain in memory | Affects memory usage and validation performance |
| `blockvalidation_secret_mining_threshold` | uint32 | 10 | Threshold for detecting secret mining attacks | Lower values increase security at the cost of potential false positives |

### Transaction Processing

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `blockvalidation_localSetTxMinedConcurrency` | int | 8 | Concurrency level for marking transactions as mined | Higher values improve performance but increase memory usage |
| `blockvalidation_missingTransactionsBatchSize` | int | 5000 | Batch size for retrieving missing transactions | Larger batches improve throughput but increase memory usage |
| `blockvalidation_batch_missing_transactions` | bool | false | When enabled, missing transactions are fetched in batches | Improves network efficiency at the cost of slightly increased latency |
| `blockvalidation_txMetaCacheEnabled` | bool | true | Enables or disables transaction metadata caching | Dramatically improves performance at the cost of increased memory usage |
| `blockvalidation_processTxMetaUsingCache_BatchSize` | int | 1024 | Batch size for processing transaction metadata using cache | Affects performance and memory usage during cache operations |
| `blockvalidation_processTxMetaUsingCache_Concurrency` | int | 32 | Concurrency level for processing transaction metadata using cache | Controls parallel cache operations |
| `blockvalidation_processTxMetaUsingCache_MissingTxThreshold` | int | 1 | Threshold for switching to store-based processing when missing transactions | Controls fallback behavior when cache misses occur |
| `blockvalidation_processTxMetaUsingStore_BatchSize` | int | max(4, runtime.NumCPU()/2) | Batch size for processing transaction metadata using store | Affects storage I/O patterns and transaction processing throughput |
| `blockvalidation_processTxMetaUsingStore_Concurrency` | int | 32 | Concurrency level for processing transaction metadata using store | Controls parallel storage operations |
| `blockvalidation_processTxMetaUsingStore_MissingTxThreshold` | int | 1 | Maximum allowed missing transactions before failure | Controls error tolerance during transaction processing |

### Synchronization and Recovery

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `blockvalidation_useCatchupWhenBehind` | bool | false | Enables specialized catchup mode when node falls behind | Affects synchronization strategy and resource utilization |
| `blockvalidation_catchupConcurrency` | int | max(4, runtime.NumCPU()/2) | Concurrency level for catchup operations | Controls parallel processing during chain synchronization |
| `blockvalidation_catchupCh_buffer_size` | int | 10 | Buffer size for catchup operations channel | Affects performance during blockchain synchronization |
| `blockvalidation_subtreeBlockHeightRetention` | uint32 | (global setting) | How long to keep subtrees (in terms of block height) | Affects storage utilization and historical data availability |
| `blockvalidation_subtreeGroupConcurrency` | int | 1 | Maximum concurrent goroutines for processing subtree groups | Controls parallelism during subtree group processing |
| `blockvalidation_skipCheckParentMined` | bool | false | When enabled, skips parent block mining verification | Testing only: compromises chain integrity for performance |

### Error Handling and Resilience

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `blockvalidation_validation_max_retries` | int | 3 | Maximum retries for validation operations | Affects resilience to transient failures |
| `blockvalidation_validation_retry_sleep` | duration | 5s | Sleep duration between validation retries | Controls back-off behavior during retry attempts |
| `blockvalidation_isParentMined_retry_max_retry` | int | 20 | Maximum retries when checking if parent block is mined | Affects resilience during chain reorganizations |
| `blockvalidation_isParentMined_retry_backoff_multiplier` | int | 30 | Backoff multiplier for parent mining check retries | Controls exponential back-off during retries |
| `blockvalidation_check_subtree_from_block_timeout` | duration | 5m | Timeout for checking subtrees from a block | Limits how long the service will wait for subtree validation |
| `blockvalidation_check_subtree_from_block_retries` | int | 5 | Number of retries for checking subtrees | Affects resilience to transient subtree retrieval failures |
| `blockvalidation_check_subtree_from_block_retry_backoff_duration` | duration | 30s | Backoff duration between subtree check retries | Controls delay between retry attempts |
| `blockvalidation_subtree_validation_abandon_threshold` | int | 1 | Number of validation failures before abandoning a subtree | Controls resilience vs. resource conservation trade-off |

### Kafka Integration

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `kafka_blocksConfig` | string | (none) | Kafka configuration for block messages | Required for consuming blocks from Kafka |
| `blockvalidation_kafkaWorkers` | int | 0 (auto) | Number of Kafka consumer workers | Controls parallelism for Kafka-based block validation |

### Performance Monitoring

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `blockvalidation_validation_warmup_count` | int | 128 | Number of validation operations during warmup | Helps prime caches and establish performance baselines |

### Miscellaneous

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `excessiveblocksize` | int | 4GB | Maximum allowed block size | Limits resource consumption for extremely large blocks |
| `utxostore` | URL | (none) | URL for the UTXO store | Required for UTXO validation and updates |
| `fsm_state_restore` | bool | false | Enables FSM state restoration | Affects recovery behavior after service restart |

## Configuration Interactions and Dependencies

Many configuration settings interact with each other to affect overall system behavior. Understanding these interactions is crucial for optimal tuning.

### Optimistic Mining

The `blockvalidation_optimistic_mining` setting enables a performance optimization where blocks are conditionally accepted before full validation completes. This dramatically improves blockchain throughput but introduces a risk of temporary chain inconsistency if validation later fails.

When enabled:
- The system achieves higher throughput and lower latency
- Validation continues asynchronously after block acceptance
- If validation fails, a chain reorganization may be necessary

Related settings that affect this behavior include:
- `blockvalidation_validation_max_retries` - Controls resilience during validation
- `blockvalidation_validation_retry_sleep` - Affects backoff behavior during retries

### Transaction Processing Pipeline

The transaction processing pipeline is controlled by multiple settings that affect concurrency, batch sizing, and caching behavior:

- **Cache-based processing**: When `blockvalidation_txMetaCacheEnabled` is true, the service uses an in-memory cache for transaction metadata, controlled by `blockvalidation_processTxMetaUsingCache_*` settings
- **Store-based processing**: For transactions not in cache, the service uses store-based processing, controlled by `blockvalidation_processTxMetaUsingStore_*` settings
- **Batch size vs. Concurrency**: Increasing batch sizes improves efficiency but increases memory usage, while increasing concurrency improves throughput but increases CPU utilization

### Synchronization Strategy

When a node falls behind the blockchain tip, its synchronization strategy is controlled by:

- `blockvalidation_useCatchupWhenBehind` - Enables specialized catchup mode
- `blockvalidation_catchupConcurrency` - Controls parallel processing during catchup
- `blockvalidation_catchupCh_buffer_size` - Affects buffer capacity for catchup operations

Optimal settings depend on hardware capabilities and network conditions.

## Deployment Recommendations

### Development Environment

For development and testing environments, prioritize faster feedback cycles and debugging capabilities:

```properties
blockvalidation_optimistic_mining=false  # Disable for predictable validation behavior
blockvalidation_txMetaCacheEnabled=true  # Enable cache for better performance
blockvalidation_localSetTxMinedConcurrency=4  # Lower concurrency to reduce resource usage
blockvalidation_validateBlockSubtreesConcurrency=2  # Lower concurrency for reduced CPU usage
blockvalidation_batch_missing_transactions=true  # Enable for better network efficiency
```

### Production Environment

For production environments, prioritize reliability, resilience, and consistent performance:

```properties
blockvalidation_optimistic_mining=true  # Enable for better throughput
blockvalidation_blockFoundCh_buffer_size=2000  # Increased buffer for handling bursts
blockvalidation_txMetaCacheEnabled=true  # Enable cache for performance
blockvalidation_localSetTxMinedConcurrency=16  # Higher concurrency for better throughput
blockvalidation_validateBlockSubtreesConcurrency=8  # Balanced concurrency for validation
blockvalidation_processTxMetaUsingCache_Concurrency=64  # Higher concurrency for cache operations
blockvalidation_validation_max_retries=5  # Increased retries for better resilience
blockvalidation_useCatchupWhenBehind=true  # Enable efficient catchup when behind
```

### High-Performance Environment

For high-throughput environments with significant hardware resources:

```properties
blockvalidation_optimistic_mining=true  # Enable for maximum throughput
blockvalidation_blockFoundCh_buffer_size=5000  # Large buffer for handling extreme bursts
blockvalidation_txMetaCacheEnabled=true  # Enable cache for performance
blockvalidation_processTxMetaUsingCache_BatchSize=2048  # Larger batches for efficiency
blockvalidation_processTxMetaUsingCache_Concurrency=128  # High concurrency for cache operations
blockvalidation_localSetTxMinedConcurrency=32  # High concurrency for transaction marking
blockvalidation_validateBlockSubtreesConcurrency=16  # High concurrency for validation
blockvalidation_catchupConcurrency=16  # High concurrency for catchup operations
blockvalidation_useCatchupWhenBehind=true  # Enable efficient catchup when behind
```

> **Note**: These recommendations should be adjusted based on specific hardware capabilities, network conditions, and observed performance metrics. Regular monitoring and tuning are essential for optimal performance.


## 9. Other Resources

[Block Validation Reference](../../references/services/blockvalidation_reference.md)
