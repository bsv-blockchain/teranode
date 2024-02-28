# 🗂️ Blob Server

## Index


1. [Description](#1-description)
2. [Architecture](#2-architecture)
3. [Technology ](#3-technology-)
- [3.1 Overview](#31-overview)
- [3.2 Store Options](#32-store-options)
4. [Data Model ](#4-data-model-)
- [4.1. Subtrees](#41-subtrees)
- [4.2. Txs](#42-txs)
5. [Use Cases](#5-use-cases)
- [5.1. Asset Server (HTTP) Get Transactions](#51-asset-server-http---get-transactions)
- [5.2. Asset Server (HTTP) Get Subtrees](#52-asset-server-http---get-subtrees)
- [5.3. Block Assembly](#53-block-assembly)
- [5.4. Block Validation](#54-block-validation)
- [5.5 Propagation TXStore Set()](#55-propagation---txstore-set)
6. [Directory Structure and Main Files](#6-directory-structure-and-main-files)
7. [Locally Running the store](#7-locally-running-the-store)
8. [Configuration options (settings flags)](#8-configuration-options-settings-flags)
- [8.1. TX Store Configuration](#81-tx-store-configuration)
- [8.2. SubTree Store Configuration](#82-subtree-store-configuration)

## 1. Description

The Blob Server is a generic datastore that can be used for any specific data model. In the current Teranode implementation, it is used to store transactions (extended tx) and subtrees.

The Blob Server provides a set of methods to interact with the TX and Subtree storage implementations.

1. **Health**: `Health(ctx)`
    - **Purpose**: Checks the health status of the Blob Server.


2. **Exists**: `Exists(ctx, key)`
    - **Purpose**: Determines if a given key exists in the store.


3. **Get**: `Get(ctx, key)`
    - **Purpose**: Retrieves the value associated with a given key.


4. **GetIoReader**: `GetIoReader(ctx, key)`
    - **Purpose**: Retrieves an `io.ReadCloser` for the value associated with a given key, useful for streaming large data.


5. **Set**: `Set(ctx, key, value, opts...)`
    - **Purpose**: Sets a key-value pair in the store.


6. **SetFromReader**: `SetFromReader(ctx, key, value, opts...)`
    - **Purpose**: Sets a key-value pair in the store from an `io.ReadCloser`, useful for streaming large data.


7. **SetTTL**: `SetTTL(ctx, key, ttl)`
    - **Purpose**: Sets a Time-To-Live for a given key.


8. **Del**: `Del(ctx, key)`
    - **Purpose**: Deletes a key and its associated value from the store.


9. **Close**: `Close(ctx)`
    - **Purpose**: Closes the Blob Server connection or any associated resources.


## 2. Architecture

The Blob Server is a store interface, with implementations for Tx Store and Subtree Store.

![Blob_Store_Component_Context_Diagram.png](..%2Fservices%2Fimg%2FBlob_Store_Component_Context_Diagram.png)

The Blob Server implementations for Tx Store and Subtree Store are injected into the various services that require them. They are initialised in the `main_stores.go` file and passed into the services as a dependency. See below:


```go

func getTxStore(logger ulogger.Logger) blob.Store {
	if txStore != nil {
		return txStore
	}

	txStoreUrl, err, found := gocore.Config().GetURL("txstore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("txstore config not found")
	}
	txStore, err = blob.NewStore(logger, txStoreUrl)
	if err != nil {
		panic(err)
	}

	return txStore
}

func getSubtreeStore(logger ulogger.Logger) blob.Store {
	if subtreeStore != nil {
		return subtreeStore
	}

	subtreeStoreUrl, err, found := gocore.Config().GetURL("subtreestore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("subtreestore config not found")
	}
	subtreeStore, err = blob.NewStore(logger, subtreeStoreUrl, options.WithPrefixDirectory(10))
	if err != nil {
		panic(err)
	}

	return subtreeStore
}

```

## 3. Technology

### 3.1 Overview

Key technologies involved:

1. **Go Programming Language (Golang)**:
    - A statically typed, compiled language known for its simplicity and efficiency, especially in concurrent operations and networked services.
    - The primary language used for implementing the service's logic.


### 3.2 Store Options

The Blob Server supports various backends, each suited to different storage requirements and environments.

- **Badger**: A high-performance key-value store. [BadgerDB Official](https://github.com/dgraph-io/badger)

- **Batcher**: Provides batch processing capabilities for storage operations.

- **File**: Utilizes the local file system for storage.

- **Google Cloud Storage (GCS)**: Integrates with Google Cloud Storage for cloud-based blob storage. [Google Cloud Storage](https://cloud.google.com/storage)

- **Kinesis S3**: Combines AWS Kinesis and S3 for streaming and storage solutions. [Amazon Kinesis](https://aws.amazon.com/kinesis/), [Amazon S3](https://aws.amazon.com/s3/)

- **Memory**: In-memory storage for temporary and fast data access.

- **MinIO**: A high-performance S3 compatible object storage service. [MinIO Official](https://min.io/)

- **Null**: A no-operation store for testing or disabling storage features.

- **Amazon S3**: Integration with Amazon Simple Storage Service (S3) for cloud storage. [Amazon S3](https://aws.amazon.com/s3/)

- **SeaweedFS**: A simple and highly scalable distributed file system. [SeaweedFS GitHub](https://github.com/chrislusf/seaweedfs)

- **SeaweedFS S3**: SeaweedFS with S3-compatible API support.

- **SQL**: SQL-based stores include PostgreSQL. [PostgreSQL Official](https://www.postgresql.org/)

**TODO** - which one is the production candidate?

## 4. Data Model

### 4.1. Subtrees

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

### 4.2. Txs

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


## 5. Use Cases


### 5.1. Asset Server (HTTP) - Get Transactions

![asset_server_http_get_transaction.svg](..%2Fservices%2Fimg%2Fplantuml%2Fassetserver%2Fasset_server_http_get_transaction.svg)

### 5.2. Asset Server (HTTP) - Get Subtrees

![asset_server_http_get_subtree.svg](..%2Fservices%2Fimg%2Fplantuml%2Fassetserver%2Fasset_server_http_get_subtree.svg)

### 5.3. Block Assembly

New Subtree and block mining scenarios:

![blob_server_block_assembly.svg](..%2Fservices%2Fimg%2Fplantuml%2Fblobserver%2Fblob_server_block_assembly.svg)

Reorganizing subtrees:

![asset_server_grpc_get_subtree.svg](..%2Fservices%2Fimg%2Fplantuml%2Fassetserver%2Fasset_server_grpc_get_subtree.svg)

### 5.4. Block Validation

Service:

![blob_server_block_validation.svg](..%2Fservices%2Fimg%2Fplantuml%2Fblobserver%2Fblob_server_block_validation.svg)


gRPC endpoints:

![blob_server_block_validation_addendum.svg](..%2Fservices%2Fimg%2Fplantuml%2Fblobserver%2Fblob_server_block_validation_addendum.svg)



### 5.5 Propagation - TXStore Set()

![txStore_propagation_set.svg](..%2Fservices%2Fimg%2Fplantuml%2Fassetserver%2FtxStore_propagation_set.svg)


## 6. Directory Structure and Main Files

```
./stores/blob/
├── Interface.go                # Interface definitions for the project.
├── badger                      # BadgerDB implementation.
│   ├── badger.go               # Main BadgerDB functionality.
│   └── badger_test.go          # Test cases for BadgerDB.
├── batcher                     # Batching functionality for efficient processing.
│   ├── batcher.go              # Main batcher functionality.
│   ├── queue.go                # Queue implementation for batcher.
│   └── queue_test.go           # Test cases for batcher queue.
├── factory.go                  # Factory methods for creating instances.
├── file                        # File system based implementations.
│   ├── file.go                 # File system handling.
│   └── file_test.go            # Test cases for file system functions.
├── gcs                         # Google Cloud Storage implementation.
│   └── gcs.go                  # GCS specific functionality.
├── kinesiss3                   # Amazon Kinesis and S3 integration.
│   └── kinesiss3.go            # Kinesis S3 specific functionality.
├── localttl                    # Local Time-to-Live functionality.
│   └── localttl.go             # Local TTL handling.
├── memory                      # In-memory implementation.
│   └── memory.go               # In-memory data handling.
├── minio                       # MinIO cloud storage implementation.
│   └── minio.go                # MinIO specific functionality.
├── null                        # Null implementation (no-op).
│   └── null.go                 # Null pattern implementation.
├── options                     # Options and configurations.
│   ├── Options.go              # General options for the project.
│   └── Reader.go               # Reader options and configurations.
├── s3                          # Amazon S3 cloud storage implementation.
│   └── s3.go                   # S3 specific functionality.
├── seaweedfs                   # SeaweedFS filesystem implementation.
│   └── seaweedfs-filer.go      # SeaweedFS filer specific functionality.
├── seaweedfss3                 # SeaweedFS with S3 compatibility.
│   └── seaweedfs-s3.go         # SeaweedFS S3 specific functionality.
└── sql                         # SQL database implementation.
   └── sql.go         # SQL specific functionality.
```

## 7. Locally Running the store

The Blob Server cannot be run independently. It is instantiated as part of the main.go initialization and directly used by the services that require it.

## 8. Configuration options (settings flags)


The Blob Server supports various backends for the TX Store and SubTree store. Below are configuration setting examples you can use, depending on your preferred storage backend:

### 8.1. TX Store Configuration


- **Null Store (No-op)**
  ```plaintext
  txstore.${YOUR_USERNAME}=null:///
  ```

- **BadgerDB**
  ```plaintext
  txstore.${YOUR_USERNAME}=badger:///data/txstore
  ```

- **Amazon S3**
  ```plaintext
  txstore.${YOUR_USERNAME}=s3:///eu-ubsv-txstore?region=eu-west-1&batch=true&writeKeys=true
  ```

- **File System**
  ```plaintext
  txstore.${YOUR_USERNAME}=file:///data/txstore?batch=true&writeKeys=true
  ```

- **SQLite In-Memory**
  ```plaintext
  txstore.${YOUR_USERNAME}=sqlitememory:///txstore
  ```

### 8.2. SubTree Store Configuration


- **SeaweedFS**
  ```plaintext
  subtreestore.${YOUR_USERNAME}=seaweedfs://localhost:9333/txs?filers=http://localhost:8888
  ```

- **Amazon S3 with Local TTL Store**
  ```plaintext
  subtreestore.${YOUR_USERNAME}=s3:///eu-ubsv-subtree-store?region=eu-west-1&localTTLStore=file&localTTLStorePath=/data/subtreestore-ttl
  ```

- **File System with Local TTL Store**
  ```plaintext
  subtreestore.${YOUR_USERNAME}=file:///data/subtreestore?localTTLStore=file&localTTLStorePath=./data/subtreestore-ttl
  ```

- **BadgerDB**
  ```plaintext
  subtreestore.${YOUR_USERNAME}=badger:///data/subtreestore
  ```

- **MinIO**
  ```plaintext
  subtreestore.${YOUR_USERNAME}=minio://minio:minio123@myminio-hl.minio.svc.cluster.local:9000/subtrees-lake?objectLocking=true&tempTTL=true
  ```

- **Null Store (No-op)**
  ```plaintext
  subtreestore.${YOUR_USERNAME}=null:///
  ```

- **SQLite In-Memory**
  ```plaintext
  subtreestore.${YOUR_USERNAME}=sqlitememory:///subtreestore
  ```
