# 🗂️ Blob Server

## Index

1. [Description](#1-description)
2. [Architecture](#2-architecture)
3. [Technology](#3-technology)
    - [3.1 Overview](#31-overview)
    - [3.2 Store Options](#32-store-options)
4. [Data Model](#4-data-model)
5. [Use Cases](#5-use-cases)
    - [5.1. Asset Server (HTTP): Get Transactions](#51-asset-server-http-get-transactions)
    - [5.2. Asset Server (HTTP): Get Subtrees](#52-asset-server-http-get-subtrees)
    - [5.3. Block Assembly](#53-block-assembly)
    - [5.4. Block Validation](#54-block-validation)
    - [5.5 Propagation: TXStore Set()](#55-propagation-txstore-set)
6. [Directory Structure and Main Files](#6-directory-structure-and-main-files)
7. [Locally Running the store](#7-locally-running-the-store)
8. [Configuration options (settings flags)](#8-configuration-options-settings-flags)
    - [8.1. TX Store Configuration](#81-tx-store-configuration)
    - [8.2. SubTree Store Configuration](#82-subtree-store-configuration)
9. [Other Resources](#9-other-resources)


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

- **Batcher**: Provides batch processing capabilities for storage operations.

- **File**: Utilizes the local file system for storage.

- **HTTP**: Implements an HTTP client for interacting with a remote blob storage server.

- **Local TTL**: Provides local Time-to-Live (TTL) functionality for managing data expiration.

- **Lustre**: Implements storage using the Lustre distributed file system. [Lustre Official](http://lustre.org/)

- **Memory**: In-memory storage for temporary and fast data access.

- **Null**: A no-operation store for testing or disabling storage features.

- **Amazon S3**: Integration with Amazon Simple Storage Service (S3) for cloud storage. [Amazon S3](https://aws.amazon.com/s3/)

Each store option is implemented in its respective subdirectory within the `stores/blob/` directory.

The system also includes a main server implementation (`server.go`) that provides an HTTP interface for blob storage operations.

Options for configuring these stores are managed through the `options` package.


## 4. Data Model

- [Subtree Data Model](../datamodel/subtree_data_model.md): Contain lists of transaction IDs and their Merkle root.

- [Extended Transaction Data Model](../datamodel/transaction_data_model.md): Include additional metadata to facilitate processing.

## 5. Use Cases

### 5.1. Asset Server (HTTP): Get Transactions

![asset_server_http_get_transaction.svg](..%2Fservices%2Fimg%2Fplantuml%2Fassetserver%2Fasset_server_http_get_transaction.svg)

### 5.2. Asset Server (HTTP): Get Subtrees

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



### 5.5 Propagation: TXStore Set()

![txStore_propagation_set.svg](..%2Fservices%2Fimg%2Fplantuml%2Fassetserver%2FtxStore_propagation_set.svg)


## 6. Directory Structure and Main Files

```
./stores/blob/
├── Interface.go                # Interface definitions for the project.
├── batcher                     # Batching functionality for efficient processing.
│   └── batcher.go              # Main batcher functionality.
├── factory.go                  # Factory methods for creating instances.
├── file                        # File system based implementations.
│   ├── file.go                 # File system handling.
│   └── file_test.go            # Test cases for file system functions.
├── http                        # HTTP client implementation for remote blob storage.
│   └── http.go                 # HTTP specific functionality.
├── localttl                    # Local Time-to-Live functionality.
│   └── localttl.go             # Local TTL handling.
├── lustre                      # Lustre filesystem implementation.
│   ├── lustre.go               # Lustre specific functionality.
│   └── lustre_test.go          # Test cases for Lustre functions.
├── memory                      # In-memory implementation.
│   └── memory.go               # In-memory data handling.
├── null                        # Null implementation (no-op).
│   └── null.go                 # Null pattern implementation.
├── options                     # Options and configurations.
│   ├── Options.go              # General options for the project.
│   └── Options_test.go         # Test cases for options.
├── s3                          # Amazon S3 cloud storage implementation.
│   └── s3.go                   # S3 specific functionality.
├── server.go                   # HTTP server implementation for blob storage.
└── server_test.go              # Test cases for the server implementation.
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

- **Amazon S3**
  ```plaintext
  txstore.${YOUR_USERNAME}=s3:///subtreestore?region=eu-west-1&batch=true&writeKeys=true
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


- **Null Store (No-op)**
  ```plaintext
  subtreestore.${YOUR_USERNAME}=null:///
  ```

- **Amazon S3 with Local TTL Store**
  ```plaintext
  subtreestore.${YOUR_USERNAME}=s3:///subtreestore?region=eu-west-1&localTTLStore=file&localTTLStorePath=/data/subtreestore-ttl
  ```

- **Lustre with S3 Backend**
  ```plaintext
  subtreestore.mainnet2=lustre://s3.${regionName}.amazonaws.com/teranode-${clientName}-block-store?region=${regionName}&localDir=data/subtreestore&localPersist=s3
  ```

- **File System with Local TTL Store**
  ```plaintext
  subtreestore.${YOUR_USERNAME}=file:///data/subtreestore?localTTLStore=file&localTTLStorePath=./data/subtreestore-ttl
  ```

- **SQLite In-Memory**
  ```plaintext
  subtreestore.${YOUR_USERNAME}=sqlitememory:///subtreestore
  ```


## 9. Other Resources

[Blob Server Reference](../../references/stores/blob_reference.md)
