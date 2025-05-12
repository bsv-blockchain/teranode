# 🔍 Subtree Validation Service

## Index

1. [Description](#1-description)
2. [Functionality](#2-functionality)
    - [2.1. Receiving UTXOs and warming up the TXMeta Cache](#21-receiving-utxos-and-warming-up-the-txmeta-cache)
    - [2.2. Receving subtrees for validation](#22-receving-subtrees-for-validation)
    - [2.3. Validating the Subtrees](#23-validating-the-subtrees)
3. [gRPC Protobuf Definitions](#3-grpc-protobuf-definitions)
4. [Data Model](#4-data-model)
5. [Technology](#5-technology)
6. [Directory Structure and Main Files](#6-directory-structure-and-main-files)
    - [Key Changes and Additions:](#key-changes-and-additions)
7. [How to Run](#7-how-to-run)
8. [Configuration options (settings flags)](#8-configuration-options-settings-flags)
    - [gRPC Settings](#grpc-settings)
    - [Subtree Configuration](#subtree-configuration)
    - [Kafka Configuration](#kafka-configuration)
    - [Block Validation Settings](#block-validation-settings)
    - [Tx Metadata Processing](#tx-metadata-processing)
    - [UTXO Store Settings](#utxo-store-settings)
    - [Miscellaneous](#miscellaneous)
9. [Other Resources](#9-other-resources)

## 1. Description

The Subtree Validator is responsible for ensuring the integrity and consistency of each received subtree before it is added to the subtree store. It performs several key functions:

1. **Validation of Subtree Structure**: Verifies that each received subtree adheres to the defined structure and format, and that its transactions are known and valid.

2. **Transaction Legitimacy**: Ensures all transactions within subtrees are valid, including checks for double-spending.

3. **Decorates the Subtree with additional metadata**: Adds metadata to the subtree, to facilitate faster block validation at a later stage (by the Block Validation Service).
    - Specifically, the subtree metadata will contain all of the transaction parent hashes. This decorated subtree can be validated and processed faster by the Block Validation Service, preventing unnecessary round trips to the UTXO Store.

> **Note**: For information about how the Subtree Validation service is initialized during daemon startup and how it interacts with other services, see the [Teranode Daemon Reference](../../references/teranodeDaemonReference.md#service-initialization-flow).

![Subtree_Validation_Service_Container_Diagram.png](img/Subtree_Validation_Service_Container_Diagram.png)

The Subtree Validation Service:

* Receives new subtrees from the P2P Service. The P2P Service has received them from other nodes on the network.
* Validates the subtrees, after fetching them from the remote asset server.
* Decorates the subtrees with additional metadata, and stores them in the Subtree Store.

The P2P Service communicates with the Block Validation over either gRPC protocols.

![Subtree_Validation_Service_Component_Diagram.png](img/Subtree_Validation_Service_Component_Diagram.png)

### 1.1 Validator Integration

The Subtree Validation service interacts with the Validator service to validate transactions that might be missing during subtree processing. This interaction can happen in two different configurations:

1. **Local Validator**:
   - When `validator.useLocalValidator=true` (recommended for production)
   - The Validator is instantiated directly within the Subtree Validation service
   - Direct method calls are used without network overhead
   - This provides the best performance and lowest latency

2. **Remote Validator Service**:
   - When `validator.useLocalValidator=false`
   - The Subtree Validation service connects to a separate Validator service via gRPC
   - Useful for development, testing, or specialized deployment scenarios
   - Has higher latency due to additional network calls

This configuration is controlled by the settings passed to `GetValidatorClient()` in daemon.go.

To improve performance, the Subtree Validation Service uses a caching mechanism for UTXO meta data (called `TX Meta Cache` for historical reasons). This prevents repeated fetch calls to the store by retaining recently loaded transactions in memory (for a limited time). This can be enabled or disabled via the `subtreevalidation_txMetaCacheEnabled` setting. The caching mechanism is implemented in the `txmetacache` package, and is used by the Subtree Validation Service:

```go
	// create a caching tx meta store
    if gocore.Config().GetBool("subtreevalidation_txMetaCacheEnabled", true) {
        logger.Infof("Using cached version of tx meta store")
        u.utxoStore = txmetacache.NewTxMetaCache(ctx, ulogger.TestLogger{}, utxoStore)
    } else {
        u.utxoStore = utxoStore
    }
```

If this caching mechanism is enabled, the Subtree Validation Service will listen to the `kafka_txmetaConfig` Kafka topic, where the Transaction Validator posts new UTXO meta data. This data is then stored in the cache for quick access during subtree validation.

Finally, note that the Subtree Validation service benefits of the use of Lustre Fs (filesystem). Lustre is a type of parallel distributed file system, primarily used for large-scale cluster computing. This filesystem is designed to support high-performance, large-scale data storage and workloads.
Specifically for Teranode, these volumes are meant to be temporary holding locations for short-lived file-based data that needs to be shared quickly between various services
Teranode microservices make use of the Lustre file system in order to share subtree and tx data, eliminating the need for redundant propagation of subtrees over grpc or message queues. The services sharing Subtree data through this system can be seen here:

![lustre_fs.svg](img/plantuml/lustre_fs.svg)



## 2. Functionality

The subtree validator is a service that validates subtrees. After validating them, it will update the relevant stores accordingly.

### 2.1. Receiving UTXOs and warming up the TXMeta Cache

* The TX Validator service processes and validates new transactions.
* After validating transactions, The Tx Validator Service sends them (in UTXO Meta format) to the Subtree Validation Service via Kafka.
* The Subtree Validation Service stores these UTXO Meta Data in the Tx Meta Cache.
* At a later stage (next sections), the Subtree Validation Service will receive subtrees, composed of 1 million transactions. By having the Txs preloaded in a warmed up Tx Meta Cache, the Subtree Validation Service can quickly access the data required to validate the subtree.

![tx_validation_subtree_validation.svg](img/plantuml/validator/tx_validation_subtree_validation.svg)

### 2.2. Receving subtrees for validation

* The P2P service is responsible for receiving new subtrees from the network. When a new subtree is found, it will notify the subtree validation service via the Kafka `kafka_subtreesConfig` producer.

* The subtree validation service will then check if the subtree is already known. If not, it will start the validation process.
* Before validation, the service will "lock" the subtree, to avoid concurrent (and accidental) changes of the same subtree. To do this, the service will attempt to create a "lock" file in the shared subtree storage. If this succeeds, the subtree validation will then start.
* Once validated, we add it to the Subtree store, from where it will be retrieved later on (when a block using the subtrees gets validated).

Receiving subtrees for validation via Kafka:

![subtree_validation_kafka_subtree_found.svg](img/plantuml/subtreevalidation/subtree_validation_kafka_subtree_found.svg)


In addition to the P2P Service, the Block Validation service can also request for subtrees to be validated and added, together with its metadata, to the subtree store. Should the Block Validation service find, as part of the validation of a specific block, a subtree not known by the node, it can request its validation to the Subtree Validation service.

![block_validation_subtree_validation_request.svg](img/plantuml/subtreevalidation/block_validation_subtree_validation_request.svg)

The detail of how the subtree is validated will be described in the next section.

### 2.3. Validating the Subtrees

In the previous section, the process of validating a subtree was described. Here, we will go into more detail about the validation process.

![subtree_validation_detail.svg](img/plantuml/subtreevalidation/subtree_validation_detail.svg)


The validation process is as follows:

1. First, the Validator will check if the subtree already exists in the Subtree Store. If it does, the subtree will not be validated again.
2. If the subtree is not found in the Subtree Store, the Validator will fetch the subtree from the remote asset server.
3. The Validator will create a subtree metadata object.
4. Next, the Validator will decorate all Txs. To do this, it will try the following approaches (in order):
    - First, it will try to fetch the UTXO metadata from the tx metadata cache (in-memory).
    - If the tx metadata is not found, it will try to fetch the tx metadata from the UTXO store.
    - If the tx metadata is not found in the UTXO store, the Validator will fetch the UTXO from the remote asset server.
    - If the tx is not found, the tx will be marked as invalid, and the subtree validation will fail.

### 2.4. Subtree Locking Mechanism

To prevent concurrent validation of the same subtree, the service implements a file-based locking mechanism:

1. Before validation begins, the service attempts to create a "lock" file for the specific subtree.
2. If the lock file creation succeeds, the service proceeds with validation.
3. If the lock file already exists, the service assumes another instance is already validating the subtree.
4. This mechanism ensures efficient resource usage and prevents duplicate validation work.

The locking implementation is designed to be resilient across distributed systems by leveraging the shared filesystem.

## 3. gRPC Protobuf Definitions

The Subtree Validation Service uses gRPC for communication between nodes. The protobuf definitions used for defining the service methods and message formats can be seen [here](../../references/protobuf_docs/subtreevalidationProto.md).


## 4. Data Model

- [Subtree Data Model](../datamodel/subtree_data_model.md): Contain lists of transaction IDs and their Merkle root.
- [Extended Transaction Data Model](../datamodel/transaction_data_model.md): Include additional metadata to facilitate processing.
- [UTXO Data Model](../datamodel/utxo_data_model.md): Include additional metadata to facilitate processing.

## 5. Technology


1. **Go Programming Language (Golang)**.


2. **gRPC (Google Remote Procedure Call)**:
    - Used for implementing server-client communication. gRPC is a high-performance, open-source framework that supports efficient communication between services.

4. **Data Stores**:
    - Integration with various stores: blob store, and UTXO store.

5. **Caching Mechanisms (ttlcache)**:
    - Uses `ttlcache`, a Go library for in-memory caching with time-to-live settings, to avoid redundant processing and improve performance.

6. **Configuration Management (gocore)**:
    - Uses `gocore` for configuration management, allowing dynamic configuration of service parameters.

7. **Networking and Protocol Buffers**:
    - Handles network communications and serializes structured data using Protocol Buffers, a language-neutral, platform-neutral, extensible mechanism for serializing structured data.

8. **Synchronization Primitives (sync)**:
    - Utilizes Go's `sync` package for synchronization primitives like mutexes, aiding in managing concurrent access to shared resources.


## 6. Directory Structure and Main Files

```
./services/subtreevalidation
├── Client.go                               # Client-side implementation for gRPC subtree validation service interactions.
├── Interface.go                            # Defines interfaces related to subtree validation, facilitating abstraction and testing.
├── README.md                               # Project documentation including setup, usage, and examples.
├── Server.go                               # Server-side logic for the subtree validation service, handling RPC calls.
├── Server_test.go                          # Tests for the server implementation.
├── SubtreeValidation.go                    # Core logic for validating subtrees within a blockchain structure.
├── SubtreeValidation_test.go               # Unit tests for the subtree validation logic.
├── TryLockIfNotExists.go                   # Implementation of locking mechanism to avoid concurrent subtree validation.
├── metrics.go                              # Implementation of metrics collection for monitoring service performance.
├── processTxMetaUsingCache.go              # Logic for processing transaction metadata with a caching layer for efficiency.
├── processTxMetaUsingStore.go              # Handles processing of transaction metadata directly from storage, bypassing cache.
├── subtreeHandler.go                       # Handler for operations related to subtree processing and validation.
├── subtreeHandler_test.go                  # Unit tests for the subtree handler logic.
├── subtreevalidation_api                   # Directory containing Protocol Buffers definitions and generated code for the API.
│   ├── subtreevalidation_api.pb.go         # Generated Go code from .proto definitions, containing structs and methods.
│   ├── subtreevalidation_api.proto         # Protocol Buffers file defining the subtree validation service API.
│   └── subtreevalidation_api_grpc.pb.go    # Generated Go code for gRPC client and server interfaces from the .proto service.
├── txmetaHandler.go                        # Manages operations related to transaction metadata, including validation and caching.
└── txmetaHandler_test.go                   # Unit tests for transaction metadata handling.
```

## 7. How to Run

To run the Subtree Validation Service locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -SubtreeValidation=1
```

Please refer to the [Locally Running Services Documentation](../../howto/locallyRunningServices.md) document for more information on running the Subtree Validation Service locally.

## 8. Configuration Settings

The Subtree Validation service relies on a set of configuration settings that control its behavior, performance, and resource usage. This section provides a comprehensive overview of these settings, organized by functional category, along with their impacts, dependencies, and recommended configurations for different deployment scenarios.

### 8.1 Configuration Categories

Subtree Validation service settings can be organized into the following functional categories:

1. **Quorum & Coordination**: Settings that control how subtree validation is coordinated across the system
2. **Transaction Processing**: Settings that manage transaction retrieval and validation
3. **Performance & Scaling**: Settings that control concurrency, batch sizes, and resource usage
4. **Caching & Memory**: Settings that manage the transaction metadata cache
5. **Network & Communication**: Settings for network binding and service communication

### 8.2 Quorum & Coordination Settings

These settings control how subtree validation is coordinated to prevent duplicate work and ensure consistency.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `subtree_quorum_path` | string | `""` (empty) | Directory path where quorum data is stored for subtree validation | Critical - service will not initialize without this path configured |
| `subtree_quorum_absolute_timeout` | time.Duration | `30s` | Maximum time to wait for quorum operations to complete | Controls deadlock prevention and failure recovery during validation |
| `subtreevalidation_subtree_validation_abandon_threshold` | int | `1` | Number of sequential validation failures before abandoning validation attempts | Controls resilience and retry behavior for validation errors |

#### Quorum Interactions and Dependencies

The quorum mechanism ensures that subtree validation is coordinated across the system, preventing duplicate validation work:

- The quorum path specifies where lock files are stored to coordinate validation work
- The absolute timeout prevents deadlocks if a lock holder crashes or network issues occur
- Quorum locks are acquired per subtree hash to ensure only one validator processes each subtree

When a subtree fails validation multiple times (reaching the abandon threshold), it is marked as permanently invalid to prevent wasting resources on unrecoverable subtrees.

### 8.3 Transaction Processing Settings

These settings control how transactions are retrieved, validated, and processed during subtree validation.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `subtreevalidation_failfast_validation` | bool | `true` | Controls whether validation stops at the first error or continues | Affects validation performance and error reporting behavior |
| `subtreevalidation_validation_max_retries` | int | `30` | Maximum number of retry attempts for validation operations | Controls resilience and error handling during validation |
| `subtreevalidation_validation_retry_sleep` | string | `"5s"` | Time to wait between validation retry attempts | Controls backoff behavior during validation errors |
| `subtreevalidation_batch_missing_transactions` | bool | `true` | Controls whether missing transaction retrieval is batched | Affects network efficiency and transaction retrieval performance |
| `subtreevalidation_missingTransactionsBatchSize` | int | `16384` | Maximum number of transactions to retrieve in a single batch | Controls network efficiency and memory usage during retrieval |
| `subtreevalidation_percentageMissingGetFullData` | float64 | `20.0` | Percentage threshold for switching to full data retrieval | Controls when the service retrieves full transaction data based on missing rate |

#### Transaction Processing Interactions and Dependencies

The transaction processing settings work together to balance performance, reliability, and resource usage:

- When a subtree contains transactions not available locally, the service retrieves them from remote sources.
- Batch processing (`batch_missing_transactions`) improves network efficiency by grouping requests.
- The retry mechanism (`validation_max_retries` and `validation_retry_sleep`) handles transient failures.
- The percentage threshold (`percentageMissingGetFullData`) optimizes retrieval strategies based on the proportion of missing transactions.

### 8.4 Performance & Scaling Settings

These settings control concurrency, parallelism, and batch sizes to optimize performance and resource usage.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `subtreevalidation_getMissingTransactions` | int | max(4, numCPU/2) | Number of concurrent workers for fetching missing transactions | Controls parallelism for transaction retrieval operations |
| `subtreevalidation_subtreeFoundChConcurrency` | int | `1` | Number of concurrent workers for processing found subtrees | Controls throughput for subtree discovery and processing |
| `subtreevalidation_subtreeDAHConcurrency` | int | `8` | Number of concurrent workers for processing Direct Acyclic Hash operations | Controls parallelism for DAH computations during validation |
| `subtreevalidation_processTxMetaUsingStoreConcurrency` | int | `32` | Number of concurrent workers for store-based metadata processing | Controls parallelism and I/O load during validation |
| `subtreevalidation_processTxMetaUsingCacheConcurrency` | int | `32` | Number of concurrent workers for cached metadata processing | Controls parallelism and CPU utilization during validation |
| `subtreevalidation_spendBatcherSize` | int | `1024` | Batch size for processing spend operations | Controls I/O patterns and throughput for UTXO spend operations |
| `subtreevalidation_processTxMetaUsingStoreBatchSize` | int | `1024` | Batch size for processing transaction metadata from store | Affects I/O patterns and throughput for store-based metadata processing |
| `subtreevalidation_processTxMetaUsingCacheBatchSize` | int | `1024` | Batch size for processing transaction metadata using cache | Affects memory usage and throughput for cached metadata processing |

#### Performance & Scaling Interactions and Dependencies

These settings work together to optimize resource usage and throughput:

- Concurrency settings control how many operations can run in parallel, balancing CPU utilization against context switching overhead
- Batch size settings control memory usage and I/O efficiency by grouping related operations
- Different operations (retrieval, processing, validation) have separate concurrency controls to optimize each phase
- Default values are selected to balance performance against resource consumption, but may need tuning based on specific hardware and workloads

### 8.5 Caching & Memory Settings

These settings control the transaction metadata cache, which significantly impacts performance and memory usage.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `subtreevalidation_txMetaCacheEnabled` | bool | `true` | Controls whether transaction metadata caching is enabled | Significantly affects performance and memory usage patterns |
| `txMetaCacheMaxMB` | int | `256` | Maximum memory (in MB) to use for transaction metadata cache | Controls memory footprint and cache hit rates |
| `subtreevalidation_processTxMetaUsingCacheMissingTxThreshold` | int | `1` | Threshold for switching to full transaction data retrieval when using cache | Controls when the service fetches full transaction data vs. metadata |
| `subtreevalidation_processTxMetaUsingStoreMissingTxThreshold` | int | `1` | Threshold for switching to full transaction data retrieval when using store | Controls when the service fetches full transaction data vs. metadata |
| `subtreevalidation_txChanBufferSize` | int | `0` | Buffer size for transaction processing channels | Controls buffering behavior for transaction processing queues |

#### Caching & Memory Interactions and Dependencies

The caching system is critical for high-performance validation:

- When caching is enabled (`txMetaCacheEnabled=true`), a memory-based cache layer is created over the UTXO store
- The cache size (`txMetaCacheMaxMB`) directly impacts memory usage and performance - larger caches improve hit rates but consume more memory
- The missing transaction thresholds control when the service switches from metadata validation to full transaction validation, which is more resource-intensive but necessary in some cases
- Separate thresholds for cache and store operations allow optimizing each path independently

### 8.6 Network & Communication Settings

These settings control how the service communicates with other components in the system.

| Setting | Type | Default | Description | Impact |
|---------|------|---------|-------------|--------|
| `subtreevalidation_grpcAddress` | string | `"localhost:8089"` | Address for connecting to the subtree validation service | Affects how other services connect to this service |
| `subtreevalidation_grpcListenAddress` | string | `":8089"` | Address where the service listens for gRPC connections | Controls network binding for service communication |
| `subtreevalidation_subtreeValidationTimeout` | int | `1000` | Timeout (ms) for subtree validation operations | Controls error recovery and prevents hanging validation processes |
| `validator.useLocalValidator` | bool | `false` | Controls whether to use a local or remote validator | Affects system architecture and validation performance |

#### Network & Communication Interactions and Dependencies

These settings determine how the service integrates with the broader system:

- The gRPC address settings control how the service exposes its API and how other services connect to it
- The validation timeout prevents operations from hanging indefinitely, ensuring system resilience
- The validator deployment model (`useLocalValidator`) affects performance and system architecture, with local validation typically offering better performance but requiring more resources

### 8.7 Deployment Recommendations

#### Development Environment

```
subtree_quorum_path="/tmp/subtree_quorum"
subtreevalidation_txMetaCacheEnabled=true
txMetaCacheMaxMB=128
subtreevalidation_getMissingTransactions=4
subtreevalidation_processTxMetaUsingCacheConcurrency=8
subtreevalidation_processTxMetaUsingStoreConcurrency=8
subtreevalidation_grpcListenAddress=":8089"
validator.useLocalValidator=true
```

**Rationale**: Development environments benefit from simplified configuration with local validator and moderate concurrency. Caching is enabled but with lower memory limits to accommodate development machines. The quorum path is set to a temporary location for easy cleanup.

#### Production Environment

```
subtree_quorum_path="/var/teranode/subtree_quorum"
subtree_quorum_absolute_timeout="30s"
subtreevalidation_txMetaCacheEnabled=true
txMetaCacheMaxMB=512
subtreevalidation_getMissingTransactions=8
subtreevalidation_processTxMetaUsingCacheConcurrency=32
subtreevalidation_processTxMetaUsingStoreConcurrency=32
subtreevalidation_subtreeDAHConcurrency=16
subtreevalidation_grpcListenAddress=":8089"
validator.useLocalValidator=true
```

**Rationale**: Production environments should use higher concurrency values and larger cache sizes to handle increased load. The quorum path is set to a persistent location, and timeouts are carefully configured to balance responsiveness with reliability. Local validator is recommended for best performance.

#### High-Volume Environment

```
subtree_quorum_path="/var/teranode/subtree_quorum"
subtree_quorum_absolute_timeout="45s"
subtreevalidation_txMetaCacheEnabled=true
txMetaCacheMaxMB=2048
subtreevalidation_getMissingTransactions=16
subtreevalidation_processTxMetaUsingCacheConcurrency=64
subtreevalidation_processTxMetaUsingStoreConcurrency=64
subtreevalidation_subtreeDAHConcurrency=32
subtreevalidation_missingTransactionsBatchSize=32768
subtreevalidation_spendBatcherSize=2048
subtreevalidation_processTxMetaUsingStoreBatchSize=2048
subtreevalidation_processTxMetaUsingCacheBatchSize=2048
subtreevalidation_grpcListenAddress=":8089"
validator.useLocalValidator=true
```

**Rationale**: High-volume environments need maximum performance with significantly increased cache sizes, higher concurrency, and larger batch sizes. Timeout values are increased to accommodate larger processing volumes, and all performance-related settings are tuned for maximum throughput.

### 8.8 Configuration Best Practices

1. **Memory Management**: When enabling the transaction metadata cache, ensure the host system has sufficient memory. Monitor actual memory usage and adjust `txMetaCacheMaxMB` based on observed behavior.

2. **Concurrency Tuning**: Start with concurrency values approximately equal to the number of available CPU cores, then adjust based on observed CPU usage and throughput. Too much concurrency can lead to excessive context switching.

3. **Batch Size Optimization**: Larger batch sizes improve throughput but increase memory usage and latency. Find the optimal balance through testing with representative workloads.

4. **Quorum Path Persistence**: Use a persistent, reliable filesystem location for the quorum path to prevent data loss during restarts.

5. **Timeout Configuration**: Set timeouts based on network latency and expected processing times. Too short timeouts cause unnecessary retries, while too long timeouts can delay error recovery.

6. **Monitoring**: Implement comprehensive monitoring of cache hit rates, validation success rates, and processing times to identify configuration issues.

7. **Validator Deployment**: In production environments, use a local validator (`validator.useLocalValidator=true`) for best performance, unless network architecture specifically requires a remote validator.



## 9. Other Resources

- [Subtree Validation Reference](../../references/services/subtreevalidation_reference.md)
- [Handling Double Spends](../architecture/understandingDoubleSpends.md)
