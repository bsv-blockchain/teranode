# 🔍 Block Validation Service

## Index


1. [Description](#1-description)
2. [Functionality](#2-functionality)
- [2.1. Receving subtrees for validation](#21-receving-subtrees-for-validation)
- [2.2. Receiving blocks for validation](#22-receiving-blocks-for-validation)
- [2.3. Validating blocks](#23-validating-blocks)
  - [2.3.1. Overview](#231-overview)
  - [2.3.2. Catching up after a parent block is not found](#232-catching-up-after-a-parent-block-is-not-found)
  - [2.3.3. Validating the Subtrees](#233-validating-the-subtrees)
  - [2.3.4. Block Data Validation](#234-block-data-validation)
3. [Data Model](#3-data-model)
4. [Technology](#4-technology)
5. [Directory Structure and Main Files](#5-directory-structure-and-main-files)
6. [How to run](#6-how-to-run)
7. [Configuration options (settings flags)](#7-configuration-options-settings-flags)


## 1. Description


The Block Validator is responsible for ensuring the integrity and consistency of each block before it is added to the blockchain. It performs several key functions:

1. **Validation of Subtree Structure**: Verifies that each received subtree adheres to the defined structure and format, and that its transactions are known and valid.

2. **Validation of Block Structure**: Verifies that each block adheres to the defined structure and format, and that their subtrees are known and valid.

3. **Transaction Legitimacy**: Ensures all transactions within subtrees are valid, including checks for double-spending.

4. **Merkle Root Verification**: Confirms that the Merkle root in the block header correctly represents the subtrees in the block, ensuring data integrity.

5. **Block Header Verification**: Validates the block header, including the proof of work , timestamp, and reference to the previous block, maintaining the blockchain's unbroken chain.



![Block_Validation_Service_Container_Diagram.png](img%2FBlock_Validation_Service_Container_Diagram.png)

The Block Validation Service:

* Receives new subtrees and blocks from the P2P Service. The P2P Service has received them from other nodes on the network.
* Validates the subtrees and blocks, after fetching them from the remote asset server.
* Updates stores, and notifies the blockchain service of the new block.

The P2P Service communicates with the Block Validation over either gRPC (Recommended) or fRPC (Experimental) protocols.

![Block_Validation_Service_Component_Diagram.png](img%2FBlock_Validation_Service_Component_Diagram.png)

To improve performance, the Block Validation Service uses a caching mechanism for Tx Meta data. This prevents repeated fetch calls to the store by retaining recently loaded transactions in memory (for a limited time). This can be enabled or disabled via the `blockvalidation_txMetaCacheEnabled` setting. The caching mechanism is implemented in the `txmetacache` package, and is used by the Block Validation Service:

```go
	// create a caching tx meta store
	if gocore.Config().GetBool("blockvalidation_txMetaCacheEnabled", true) {
		logger.Infof("Using cached version of tx meta store")
		bVal.txMetaStore = newTxMetaCache(txMetaStore)
	} else {
		bVal.txMetaStore = txMetaStore
	}
```



## 2. Functionality

The block validator is a service that validates blocks and their subtrees. After validating them, it will update the relevant stores and blockchain accordingly.


### 2.1. Receving subtrees for validation

* The P2P service is responsible for receiving new subtrees from the network. When a new subtree is found, it will notify the block validation service via the `SubtreeFound()` gRPC endpoint.
* The block validation service will then check if the subtree is already known. If not, it will start the validation process.
* Once validated, we add it to the Subtree store, from where it will be retrieved later on (when a block using the subtrees gets validated).

![block_validation_subtree_found.svg](img%2Fplantuml%2Fblockvalidation%2Fblock_validation_subtree_found.svg)


### 2.2. Receiving blocks for validation

![block_validation_p2p_block_found.svg](img%2Fplantuml%2Fblockvalidation%2Fblock_validation_p2p_block_found.svg)

* The P2P service is responsible for receiving new blocks from the network. When a new block is found, it will notify the block validation service via the `BlockFound()` gRPC endpoint.
* The block validation service will then check if the block is already known. If not, it will start the validation process.
* The block is added to a channel for processing. The channel is used to ensure that the block validation process is asynchronous and non-blocking.



### 2.3. Validating blocks

#### 2.3.1. Overview

![block_validation_p2p_block_validation.svg](img%2Fplantuml%2Fblockvalidation%2Fblock_validation_p2p_block_validation.svg)

Let's go through the different steps:

* As seen in the section 2.2, a new block is queued for validation in the blockFoundCh. The block validation server will pick it up and start the validation process.
* The server will request the block data from the remote node (`DoHTTPRequest()`).
* If the parent block is not known, it will be added to the catchupCh channel for processing. We stop at this point, as we can no longer proceed. The catchup process will be explained in the next section (section 2.2.2).
* If the parent is known, the block will be validated.
  * First, the service validates all the block subtrees.
    * For each subtree, we check if it is known. If not, we kick off a subtree validation process (see section 2.3.3 for more details).
  * The validator retrieves the last 100 block headers, which are used to validate the block data. We can see more about this specific step in the section 2.3.4.
  * The validator stores the coinbase Tx in the Tx Meta Store and the Tx Store.
  * The validator adds the block to the Blockchain.
  * For each Subtree in the block, the validator updates the TTL (Time To Live) to zero for the subtree. This allows the Store to clear out data the services will no longer use.
  * For each Tx for each Subtree, we set the Tx as mined in the Tx Meta Store. This allows the Tx Meta Store to know which blocks the Tx is in.
  * Should an error occur during the validation process, the block will be invalidated and removed from the blockchain.


#### 2.3.2. Catching up after a parent block is not found

![block_validation_p2p_block_catchup.svg](img%2Fplantuml%2Fblockvalidation%2Fblock_validation_p2p_block_catchup.svg)

When a block parent is not found in the local blockchain, the node will start a catchup process. The catchup process will iterate through the parent blocks until it finds a known block in the blockchain.

When a block is not known, it will be requested from the remote node. Once received, it will be queued for validation (effectively starting the process of validation for the parent block from the beginning, as seen in 2.3.1).

#### 2.3.3. Validating the Subtrees

Should the validation process for a block encounter a subtree it does not know about, it can request the subtree off the remote node.

![block_validation_p2p_block_subtree_retrieval.svg](img%2Fplantuml%2Fblockvalidation%2Fblock_validation_p2p_block_subtree_retrieval.svg)

If any transaction under the subtree is also missing, the subtree validation process will kick off a recovery process for those transactions.


#### 2.3.4. Block Data Validation

As part of the overall block validation, the service will validate the block data, ensuring the format and integrity of the data, as well as confirming that coinbase tx, subtrees and transactions are valid. This is done in the `Valid()` method under the `Block` struct.

![block_data_validation.svg](img%2Fplantuml%2Fblockvalidation%2Fblock_data_validation.svg)




## 3. Data Model

### 3.1. Block Data Model

Each block is an abstraction which is a container of a group of subtrees. A block contains a variable number of subtrees, a coinbase transaction, and a header, called a block header, which includes the block ID of the previous block, effectively creating a chain.

| Field       | Type                  | Description                                                 |
|-------------|-----------------------|-------------------------------------------------------------|
| Header      | *BlockHeader          | The Block Header                                            |
| CoinbaseTx  | *bt.Tx                | The coinbase transaction.                                   |
| Subtrees    | []*chainhash.Hash     | An array of hashes, representing the subtrees of the block. |

This table provides an overview of each field in the `Block` struct, including the data type and a brief description of its purpose or contents.

More information on the block structure and purpose can be found in the [Architecture Documentation](docs/architecture/architecture.md).


#### 3.2. Subtree Data Model

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

### 3.3. Transaction Data Model

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


### 3.4. Transaction Metadata Model

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


## 4. Technology


1. **Go Programming Language (Golang)**.

2. **gRPC (Google Remote Procedure Call)**:
  - Used for implementing server-client communication. gRPC is a high-performance, open-source framework that supports efficient communication between services.

3. **fRPC (Custom or Specific RPC Framework)**:
  - An alternative RPC (Remote Procedure Call) framework, used in experimental mode.

4. **Blockchain Data Stores**:
  - Integration with various stores such as UTXO (Unspent Transaction Output) store, blob store, and transaction metadata store.

5. **Caching Mechanisms (ttlcache)**:
  - Uses `ttlcache`, a Go library for in-memory caching with time-to-live settings, to avoid redundant processing and improve performance.

6. **Configuration Management (gocore)**:
  - Uses `gocore` for configuration management, allowing dynamic configuration of service parameters.

7. **Networking and Protocol Buffers**:
  - Handles network communications and serializes structured data using Protocol Buffers, a language-neutral, platform-neutral, extensible mechanism for serializing structured data.

8. **Synchronization Primitives (sync)**:
  - Utilizes Go's `sync` package for synchronization primitives like mutexes, aiding in managing concurrent access to shared resources.


## 5. Directory Structure and Main Files

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
│   ├── blockvalidation_api.frpc.go       - Specific implementation file for the fRPC framework for the block validation API.
│   ├── blockvalidation_api.pb.go         - Auto-generated file from protobuf definitions, containing Go bindings for the API.
│   ├── blockvalidation_api.proto         - Protocol Buffers definition file for the block validation API.
│   └── blockvalidation_api_grpc.pb.go    - gRPC (Google's RPC framework) specific implementation file for the block validation API.
├── frpc.go                        - Alternative RPC (Remote Procedure Call) framework implementation for the block validation service.
├── metrics.go                     - Metrics collection and monitoring of the block validation service's performance.
├── ttl_queue.go                   - Implements a time-to-live (TTL) queue, for managing caching within the service.
├── txmetacache.go                 - Transaction metadata cache, used to improve performance and efficiency in transaction data access.
└── txmetacache_test.go            - Unit tests for the `txmetacache.go` functionalities.
```

## 6. How to run

To run the Block Validation Service locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -BlockValidation=1
```

Please refer to the [Locally Running Services Documentation](../locallyRunningServices.md) document for more information on running the Block Validation Service locally.

## 7. Configuration options (settings flags)

In the `blockvalidation` service, the `gocore.Config().Get()` function is used to retrieve various configuration settings.  The specific configuration settings used in the service are as follows:

1. **`blockvalidation_grpcListenAddress`**:
  - Used in the `Enabled()` function to determine if a specific gRPC listen address is configured for the block validation service.

2. **`blockvalidation_frpcListenAddress`**:
  - Used in the `Start()` function to get the listen address for the fRPC server.

3. **`blockvalidation_txMetaCacheEnabled`**:
  - Used in the `New()` function to check if the transaction metadata cache is enabled.

4. **`blockvalidation_frpcConcurrency`**:
  - Used in the `frpcServer()` function to set the concurrency level for the fRPC server.
