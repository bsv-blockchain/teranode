# 🔍 Subtree Validation Service

## Index


1. [Description](#1-description)
2. [Functionality](#2-functionality)
- [2.1. Receiving Tx Meta and warming up the TXMeta Cache](#21-receiving-tx-meta-and-warming-up-the-txmeta-cache)
- [2.2. Receving subtrees for validation](#22-receving-subtrees-for-validation)
- [2.3. Validating the Subtrees](#23-validating-the-subtrees)
3. [gRPC Protobuf Definitions](#3-grpc-protobuf-definitions)
4. [Data Model](#4-data-model)
    - [4.1. Subtree Data Model](#41-subtree-data-model)
- [4.2. Transaction Data Model](#42-transaction-data-model)
- [4.3. Transaction Metadata Model](#43-transaction-metadata-model)
5. [Technology](#5-technology)
6. [Directory Structure and Main Files](#6-directory-structure-and-main-files)
7. [How to Run](#7-how-to-run)
8. [Configuration options (settings flags)](#8-configuration-options-settings-flags)


## 1. Description

The Subtree Validator is responsible for ensuring the integrity and consistency of each received subtree before it is added to the subtree store. It performs several key functions:

1. **Validation of Subtree Structure**: Verifies that each received subtree adheres to the defined structure and format, and that its transactions are known and valid.

2. **Transaction Legitimacy**: Ensures all transactions within subtrees are valid, including checks for double-spending.

3. **Decorates the Subtree with additional metadata**: Adds metadata to the subtree, to facilitate faster block validation at a later stage (by the Block Validation Service).
   - Specifically, the subtree metadata will contain all of the transaction parent hashes. This decorated subtree can be validated and processed faster by the Block Validation Service, preventing unnecessary round trips to the TX Meta Store.


![Subtree_Validation_Service_Container_Diagram.png](img%2FSubtree_Validation_Service_Container_Diagram.png)

The Subtree Validation Service:

* Receives new subtrees from the P2P Service. The P2P Service has received them from other nodes on the network.
* Validates the subtrees, after fetching them from the remote asset server.
* Decorates the subtrees with additional metadata, and stores them in the Subtree Store.

The P2P Service communicates with the Block Validation over either gRPC (Recommended) or fRPC (Experimental) protocols.

![Subtree_Validation_Service_Component_Diagram.png](img%2FSubtree_Validation_Service_Component_Diagram.png)

To improve performance, the Subtree Validation Service uses a caching mechanism for Tx MetaData. This prevents repeated fetch calls to the store by retaining recently loaded transactions in memory (for a limited time). This can be enabled or disabled via the `subtreevalidation_txMetaCacheEnabled` setting. The caching mechanism is implemented in the `txmetacache` package, and is used by the Subtree Validation Service:

```go
	// create a caching tx meta store
    if gocore.Config().GetBool("subtreevalidation_txMetaCacheEnabled", true) {
        logger.Infof("Using cached version of tx meta store")
        u.txMetaStore = txmetacache.NewTxMetaCache(context.Background(), ulogger.TestLogger{}, txMetaStore)
    } else {
        u.txMetaStore = txMetaStore
    }
```


Finally, note that the Subtree Validation service benefits of the use of Lustre Fs (filesystem). Lustre is a type of parallel distributed file system, primarily used for large-scale cluster computing. This filesystem is designed to support high-performance, large-scale data storage and workloads.
Specifically for Teranode, these volumes are meant to be temporary holding locations for short-lived file-based data that needs to be shared quickly between various services
Teranode microservices make use of the Lustre file system in order to share subtree and tx data, eliminating the need for redundant propagation of subtrees over grpc or message queues. The services sharing Subtree data through this system can be seen here:

![lustre_fs.svg](..%2Flustre_fs.svg)



## 2. Functionality

The subtree validator is a service that validates subtrees. After validating them, it will update the relevant stores accordingly.

### 2.1. Receiving Tx Meta and warming up the TXMeta Cache

* The TX Validator service processes and validates new transactions.
* After validating transactions, The Tx Validator Service sends them (in Tx Meta format) to the Subtree Validation Service via Kafka.
* The Subtree Validation Service stores these Tx Meta in a Tx Meta Cache.
* At a later stage (next sections), the Subtree Validation Service will receive subtrees, composed of 1 million transactions. By having the Txs preloaded in a warmed up Tx Meta Cache, the Subtree Validation Service can quickly access the data required to validate the subtree.

![tx_validation_subtree_validation.svg](img%2Fplantuml%2Fvalidator%2Ftx_validation_subtree_validation.svg)

### 2.2. Receving subtrees for validation

* The P2P service is responsible for receiving new subtrees from the network. When a new subtree is found, it will notify the subtree validation service either via the `SubtreeFound()` gRPC endpoint, or via the Kafka `kafka_subtreesConfig` producer (recommended for production).
* The subtree validation service will then check if the subtree is already known. If not, it will start the validation process.
* Before validation, the service will "lock" the subtree, to avoid concurrent (and accidental) changes of the same subtree. To do this, the service will attempt to create a "lock" file in the shared subtree storage. If this succeeds, the subtree validation will then start.
* Once validated, we add it to the Subtree store, from where it will be retrieved later on (when a block using the subtrees gets validated).

Receiving subtrees for validation via Kafka:

![subtree_validation_kafka_subtree_found.svg](img%2Fplantuml%2Fsubtreevalidation%2Fsubtree_validation_kafka_subtree_found.svg)

Receiving subtrees for validation via gRPC:

![subtree_validation_subtree_found.svg](img%2Fplantuml%2Fsubtreevalidation%2Fsubtree_validation_subtree_found.svg)


In addition to the P2P Service, the Block Validation service can also request for subtrees to be validated and added, together with its metadata, to the subtree store. Should the Block Validation service find, as part of the validation of a specific block, a subtree not known by the node, it can request its validation to the Subtree Validation service.

![block_validation_subtree_validation_request.svg](img%2Fplantuml%2Fsubtreevalidation%2Fblock_validation_subtree_validation_request.svg)

The detail of how the subtree is validated will be described in the next section.

### 2.3. Validating the Subtrees

In the previous section, the process of validating a subtree was described. Here, we will go into more detail about the validation process.

![subtree_validation_detail.svg](img%2Fplantuml%2Fsubtreevalidation%2Fsubtree_validation_detail.svg)

The validation process is as follows:

1. First, the Validator will check if the subtree already exists in the Substree Store. If it does, the subtree will not be validated again.
2. If the subtree is not found in the Subtree Store, the Validator will fetch the subtree from the remote asset server.
3. The Validator will create a subtree metadata object.
4. Next, the Validator will decorate all Txs. To do this, it will try 3 approaches (in order):
    - First, it will try to fetch the tx metadata from the tx metadata cache (in-memory).
    - If the tx metadata is not found, it will try to fetch the tx metadata from the tx metadata store.
    - If the tx metadata is not found in the tx metadata store, the Validator will fetch the Tx from the remote asset server.
    - If the tx is not found, the tx will be marked as invalid, and the subtree validation will fail.

## 3. gRPC Protobuf Definitions

The Subtree Validation Service uses gRPC for communication between nodes. The protobuf definitions used for defining the service methods and message formats can be seen [here](protobuf_docs/subtreevalidationProto.md).


## 4. Data Model

#### 4.1. Subtree Data Model

A subtree acts as an intermediate data structure to hold batches of transaction IDs (including metadata) and their corresponding Merkle root. Blocks are then built from a collection of subtrees.

More information on the subtree structure and purpose can be found in the [Architecture Documentation](docs/architecture/architecture.md).

Here's a table documenting the structure of the `Subtree` type:

| Field            | Type                  | Description                                                                     |
|------------------|-----------------------|---------------------------------------------------------------------------------|
| Height           | int                   | The height of the subtree within the blockchain.                                |
| Fees             | uint64                | Total fees associated with the transactions in the subtree.                     |
| SizeInBytes      | uint64                | The size of the subtree in bytes.                                               |
| FeeHash          | chainhash.Hash        | Hash representing the combined fees of the subtree.                             |
| Nodes            | []SubtreeNode         | An array of `SubtreeNode` objects, representing individual "nodes" within the subtree. |
| ConflictingNodes | []chainhash.Hash      | List of hashes representing nodes that conflict, requiring checks during block assembly. |

Here, a `SubtreeNode is a data structure representing a transaction hash, a fee, and the size in bytes of said TX.

### 4.2. Transaction Data Model

This refers to the extended transaction format, as seen below:

| Field           | Description                                                                                            | Size                                              |
|-----------------|--------------------------------------------------------------------------------------------------------|---------------------------------------------------|
| Version no      | currently 2                                                                                            | 4 bytes                                           |
| **EF marker**   | **marker for extended format**                                                                         | **0000000000EF**                                  |
| In-counter      | positive integer VI = [[VarInt]]                                                                       | 1 - 9 bytes                                       |
| list of inputs  | **Extended Format** transaction Input Structure                                                        | <in-counter> qty with variable length per input   |
| Out-counter     | positive integer VI = [[VarInt]]                                                                       | 1 - 9 bytes                                       |
| list of outputs | Transaction Output Structure                                                                           | <out-counter> qty with variable length per output |
| nLocktime       | if non-zero and sequence numbers are < 0xFFFFFFFF: block height or timestamp when transaction is final | 4 bytes                                           |

More information on the extended tx structure and purpose can be found in the [Architecture Documentation](docs/architecture/architecture.md).


### 4.3. Transaction Metadata Model

The TX Meta data model is defined in `stores/txmeta/data.go`:

| Field Name  | Description                                                     | Data Type             |
|-------------|-----------------------------------------------------------------|-----------------------|
| Hash        | Unique identifier for the transaction.                          | String/Hexadecimal    |
| Fee         | The fee associated with the transaction.                        | Decimal       |
| Size in Bytes | The size of the transaction in bytes.                        | Integer               |
| Parents     | List of hashes representing the parent transactions.            | Array of Strings/Hexadecimals |
| Blocks      | List of hashes of the blocks that include this transaction.     | Array of Strings/Hexadecimals |
| LockTime    | The earliest time or block number that this transaction can be included in the blockchain. | Integer/Timestamp or Block Number |

Note:

- **Parent Transactions**: 1 or more parent transaction hashes. For each input that our transaction has, we can have a different parent transaction. I.e. a TX can be spending UTXOs from multiple transactions.


- **Blocks**: 1 or more block hashes. Each block represents a block that mined the transaction.
    - Typically, a tx should only belong to one block. i.e. a) a tx is created (and its meta is stored in the tx meta store) and b) the tx is mined, and the mined block hash is tracked in the tx meta store for the given transaction.
    - However, in the case of a fork, a tx can be mined in multiple blocks by different nodes. In this case, the tx meta store will track multiple block hashes for the given transaction, until such time that the fork is resolved and only one block is considered valid.


## 5. Technology


1. **Go Programming Language (Golang)**.


2. **gRPC (Google Remote Procedure Call)**:
- Used for implementing server-client communication. gRPC is a high-performance, open-source framework that supports efficient communication between services.

4. **Data Stores**:
- Integration with various stores: blob store, and transaction metadata store.

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
├── Server_test.go                          # Tests for the server logic, ensuring the correctness of RPC implementations.
├── SubtreeValidation.go                    # Core logic for validating subtrees within a blockchain structure.
├── SubtreeValidation_test.go               # Unit tests for the subtree validation logic.
├── metrics.go                              # Implementation of metrics collection for monitoring service performance.
├── processTxMetaUsingCache.go              # Logic for processing transaction metadata with a caching layer for efficiency.
├── processTxMetaUsingStore.go              # Handles processing of transaction metadata directly from storage, bypassing cache.
├── subtreeHandler.go                       # Handler for operations related to subtree processing and validation.
├── subtreeHandler_test.go                  # Unit tests for the subtree handler logic.
├── subtreevalidation_api                   # Directory containing Protocol Buffers definitions and generated code for the API.
│   ├── subtreevalidation_api.pb.go         # Generated Go code from .proto definitions, containing structs and methods.
│   ├── subtreevalidation_api.proto         # Protocol Buffers file defining the subtree validation service API.
│   └── subtreevalidation_api_grpc.pb.go    # Generated Go code for gRPC client and server interfaces from the .proto service.
└── txmetaHandler.go                        # Manages operations related to transaction metadata, including validation and caching.
```

## 7. How to Run

To run the Subtree Validation Service locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -SubtreeValidation=1
```

Please refer to the [Locally Running Services Documentation](../locallyRunningServices.md) document for more information on running the Subtree Validation Service locally.

## 8. Configuration options (settings flags)


1. **`subtreevalidation_grpcAddress`**: Specifies the gRPC address for the subtree validation service.

2. **`use_open_tracing`**: A boolean flag that enables or disables OpenTracing for gRPC calls. OpenTracing is an open-source project that provides APIs for tracing the request flow across distributed systems.

3. **`use_prometheus_grpc_metrics`**: Enables or disables the collection of Prometheus metrics for gRPC calls. When set to true, the service collects metrics such as request counts, errors, and durations, which are useful for monitoring and debugging.

4. **`subtreevalidation_grpcListenAddress`**: Used by the server to determine the address on which to listen for incoming gRPC connections.

5. **`subtreevalidation_subtreeTTL`**: Configures the Time-To-Live (TTL) for subtrees stored in the service. This duration setting controls how long subtrees are retained before being considered stale or expired.

6. **`subtreevalidation_txMetaCacheEnabled`**: Controls whether a caching layer is used for tx metadata. Enabling this cache can significantly improve performance by reducing the need to repeatedly fetch tx metadata from the store.

7. **`kafka_subtreesConfig` and `kafka_txmetaConfig`**: Kafka configurations for consuming subtree and tx metadata messages.

8. **`subtreevalidation_kafkaSubtreeConcurrency`**: Determines the number of goroutines that will concurrently process subtree messages from Kafka. This setting allows for tuning the service's performance based on the available CPU resources and workload characteristics.

9. **`blockvalidation_fail_fast_validation`**: Allows the validation process to fail fast upon encountering certain conditions, potentially improving the system's responsiveness in error scenarios.

10. **`blockvalidation_subtree_validation_abandon_threshold`**: Specifies the threshold for the number of missing transactions at which the subtree validation process will be abandoned.

11. **`blockvalidation_validation_max_retries` and `blockvalidation_validation_retry_sleep`**: Control the retry behavior for subtree validation attempts, including the maximum number of retries and the sleep duration between retries.

12. **`blockvalidation_validation_warmup_count`**: Sets a warm-up count for subtree validations. It is used to delay the application of fail fast validations until a reasonable enough number of subtrees have been processed (128 by default).

13. **`blockvalidation_batchMissingTransactions`**: Controls whether missing transactions are processed in batches.

14. **`blockvalidation_missingTransactionsBatchSize`**: Defines the batch size for processing missing transactions.

15. **`blockvalidation_getMissingTransactions`**: Determines the number of goroutines that will concurrently process missing transactions.

16. **`blockvalidation_processTxMetaUsingStore_BatchSize`**: Defines the batch size for tx metadata store fetching.
