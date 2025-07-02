# 🗂️ Blob Server

## Index

- [🗂️ Blob Server](#️-blob-server)
    - [Index](#index)
  - [1. Description](#1-description)
  - [2. Architecture](#2-architecture)
  - [3. Technology](#3-technology)
    - [3.1 Overview](#31-overview)
    - [3.2 Store Options](#32-store-options)
    - [3.3 Concurrent Access Patterns](#33-concurrent-access-patterns)
    - [3.4 HTTP REST API Server](#34-http-rest-api-server)
  - [4. Data Model](#4-data-model)
  - [5. Use Cases](#5-use-cases)
    - [5.1. Asset Server (HTTP): Get Transactions](#51-asset-server-http-get-transactions)
    - [5.2. Asset Server (HTTP): Get Subtrees](#52-asset-server-http-get-subtrees)
    - [5.3. Block Assembly](#53-block-assembly)
    - [5.4. Block Validation](#54-block-validation)
    - [5.5 Propagation: TXStore Set()](#55-propagation-txstore-set)
  - [6. Directory Structure and Main Files](#6-directory-structure-and-main-files)
  - [7. Locally Running the store](#7-locally-running-the-store)
  - [8. Configuration Options](#8-configuration-options)
    - [8.1. URL Format](#81-url-format)
    - [8.2. Store URL Settings](#82-store-url-settings)
      - [8.2.1. Common URL Parameters](#821-common-url-parameters)
      - [8.2.2. File Backend Parameters](#822-file-backend-parameters)
      - [8.2.3. S3 Backend Parameters](#823-s3-backend-parameters)
      - [8.2.4. HTTP Backend Parameters](#824-http-backend-parameters)
    - [8.3. Store Options](#83-store-options)
    - [8.4. File Operation Options](#84-file-operation-options)
    - [8.5. TX Store Configuration Examples](#85-tx-store-configuration-examples)
    - [8.6. SubTree Store Configuration Examples](#86-subtree-store-configuration-examples)
    - [8.7. Configuration Interactions](#87-configuration-interactions)
  - [9. Other Resources](#9-other-resources)


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

The Blob Server implementations for Tx Store and Subtree Store are injected into the various services that require them. They are initialised in the `daemon/daemon_stores.go` file and passed into the services as a dependency. See below:


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
	txStore, err = blob.NewStore(logger, txStoreUrl, options.WithHashPrefix(10))
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
	subtreeStore, err = blob.NewStore(logger, subtreeStoreUrl, options.WithHashPrefix(10))
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

- **Memory**: In-memory storage for temporary and fast data access.

- **Null**: A no-operation store for testing or disabling storage features.

- **Amazon S3**: Integration with Amazon Simple Storage Service (S3) for cloud storage. [Amazon S3](https://aws.amazon.com/s3/)

Each store option is implemented in its respective subdirectory within the `stores/blob/` directory.

The system also includes a main server implementation (`server.go`) that provides an HTTP interface for blob storage operations.

Options for configuring these stores are managed through the `options` package.

### 3.3 Concurrent Access Patterns

The Blob Server includes a `ConcurrentBlob` implementation that provides thread-safe access to blob storage operations with optimized concurrent access patterns. This feature is particularly important in high-concurrency environments where the same blob might be requested multiple times simultaneously.

#### Key Features:

- **Double-Checked Locking Pattern**: Ensures that only one fetch operation occurs at a time for each unique key, while allowing concurrent operations on different keys
- **Generic Type Support**: Parametrized by a key type K that must satisfy `chainhash.Hash` constraints for type-safe handling
- **Duplicate Operation Prevention**: Avoids duplicate network or disk operations when multiple goroutines request the same blob simultaneously
- **Efficient Resource Usage**: Other goroutines wait for completion rather than duplicating work

#### Usage Pattern:

```go
// Create a concurrent blob instance
concurrentBlob := blob.NewConcurrentBlob[chainhash.Hash](blobStore, options...)

// Get a blob with automatic fetching if not present
reader, err := concurrentBlob.Get(ctx, key, fileType, func() (io.ReadCloser, error) {
    // This function is called only if the blob doesn't exist
    return fetchBlobFromSource(key)
})
```

The `ConcurrentBlob` wrapper is particularly useful for services that need to fetch the same data concurrently, such as block validation or transaction processing services.

### 3.4 HTTP REST API Server

The Blob Server includes a comprehensive HTTP REST API server implementation (`HTTPBlobServer`) that provides full HTTP access to blob storage operations. This server implements the standard `http.Handler` interface and can be easily integrated into existing HTTP server infrastructure.

#### Supported HTTP Endpoints:

- **GET /health**: Health check endpoint returning server status
- **GET /{key}**: Retrieve blob by key with optional range support
- **POST /{key}**: Store new blob data
- **PUT /{key}**: Update existing blob data
- **DELETE /{key}**: Delete blob by key
- **GET /{key}/dah**: Get Delete-At-Height information for a blob
- **POST /{key}/dah**: Set Delete-At-Height for a blob

#### Usage Example:

```go
// Create HTTP blob server
httpServer := blob.NewHTTPBlobServer(blobStore, logger)

// Start HTTP server
http.Handle("/blob/", http.StripPrefix("/blob", httpServer))
log.Fatal(http.ListenAndServe(":8080", nil))
```

The HTTP server is particularly useful for external integrations, debugging, and providing web-based access to blob storage functionality.

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

## 8. Configuration Options

The Blob Server can be configured through URL-based settings and option parameters. This section provides a comprehensive reference for all configuration options.

### 8.1. URL Format

Blob store configurations use URLs that follow this general structure:

```
<scheme>://<path>[?<parameter1>=<value1>&<parameter2>=<value2>...]
```

Components:

- **Scheme**: Determines the storage backend type (file, memory, s3, http, null)
- **Path**: Specifies the storage location or identifier
- **Parameters**: Optional query parameters that modify behavior

### 8.2. Store URL Settings

#### 8.2.1. Common URL Parameters

| Parameter | Type | Default | Description | Impact |
|-----------|------|---------|-------------|--------|
| `batch` | Boolean | false | Enables batch operations | Improves performance by batching operations |
| `batch_size` | Integer | 1000 | Maximum items in a batch | Controls batch size; larger values increase throughput but use more memory |
| `batch_duration` | Integer (ms) | 50 | Maximum time before processing a batch | Balances throughput vs. latency |
| `localDAHStore` | Boolean | false | Enables Delete-At-Height functionality | Enables automatic expiry of data based on blockchain height |
| `localDAHStorePath` | String | "/tmp/localDAH" | Path for local DAH store metadata | Only used when localDAHStore=true |

#### 8.2.2. File Backend Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| Path component | String | None (required) | Base directory for file storage |

#### 8.2.3. S3 Backend Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| Path component | String | None (required) | S3 bucket name |
| `region` | String | "us-east-1" | AWS region for S3 bucket |
| `endpoint` | String | AWS S3 endpoint | Custom endpoint for S3-compatible storage |
| `forcePathStyle` | Boolean | false | Force path-style addressing |

#### 8.2.4. HTTP Backend Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| Host+Path | String | None (required) | Remote HTTP blob server endpoint |
| `timeout` | Integer (ms) | 30000 | HTTP request timeout |

### 8.3. Store Options

Additional configuration is provided through Store Options when creating a store:

| Option | Type | Description | Impact |
|--------|------|-------------|--------|
| `WithDefaultBlockHeightRetention` | uint32 | Sets block height retention | Controls automatic deletion after specified blockchain height |
| `WithDefaultSubDirectory` | String | Sets subdirectory within main path | Organizes data in storage hierarchy |
| `WithHashPrefix` | Integer | Number of characters from hash to use in filenames | Improves file organization and lookup performance |
| `WithSHA256Checksum` | Boolean | Enables SHA256 checksum generation | Adds data integrity verification |
| `WithLongtermStorage` | String, URL | Configures persistent storage options | Enables three-tier storage (memory, local, longterm) |

### 8.4. File Operation Options

These options can be specified per operation:

| Option | Type | Description | Impact |
|--------|------|-------------|--------|
| `WithDeleteAt` | uint32 | Sets blockchain height for automatic deletion | Controls per-file data retention |
| `WithFilename` | String | Sets specific filename | Overrides default hash-based naming |
| `WithFileExtension` | String | Sets file extension | Useful for identifying file types |
| `WithAllowOverwrite` | Boolean | Controls overwriting of existing files | Prevents accidental data loss when false |

### 8.5. TX Store Configuration Examples

- **Null Store (No-op)**
  ```plaintext
  txstore.${YOUR_USERNAME}=null:///
  ```

- **Amazon S3**
  ```plaintext
  txstore.${YOUR_USERNAME}=s3:///mytxstore?region=eu-west-1&batch=true&batch_size=2000&batch_duration=100
  ```

- **File System with DAH**
  ```plaintext
  txstore.${YOUR_USERNAME}=file:///data/txstore?batch=true&localDAHStore=true&localDAHStorePath=/data/txstore-dah
  ```

- **Memory Store**
  ```plaintext
  txstore.${YOUR_USERNAME}=memory:///
  ```

### 8.6. SubTree Store Configuration Examples

- **Null Store (No-op)**
  ```plaintext
  subtreestore.${YOUR_USERNAME}=null:///
  ```

- **Amazon S3 with Local DAH Store**
  ```plaintext
  subtreestore.${YOUR_USERNAME}=s3:///subtreestore?region=eu-west-1&localDAHStore=true&localDAHStorePath=/data/subtreestore-dah
  ```

- **File System with Local DAH Store**
  ```plaintext
  subtreestore.${YOUR_USERNAME}=file:///data/subtreestore?localDAHStore=true&localDAHStorePath=./data/subtreestore-dah
  ```

- **Memory Store**
  ```plaintext
  subtreestore.${YOUR_USERNAME}=memory:///
  ```

### 8.7. Configuration Interactions

#### 8.7.1. Storage Backend Selection

The URL scheme determines which backend is used, which in turn determines which parameters are relevant. For example, `region` is only relevant for S3 storage.

#### 8.7.2. Batching Configuration

When batching is enabled (`batch=true`), the `batch_size` and `batch_duration` parameters control batch behavior. Operations are batched until either the size limit is reached or the duration expires, whichever happens first.

#### 8.7.3. Delete-At-Height Functionality

When `localDAHStore` is enabled, the store maintains metadata about when items should be deleted based on blockchain height. This metadata is stored at the path specified by `localDAHStorePath`. Individual files can set their own DAH values using the `WithDeleteAt` option.

#### 8.7.4. Block Height Retention

The `BlockHeightRetention` setting (via `WithDefaultBlockHeightRetention`) interacts with the `DAH` setting (via `WithDeleteAt`). The store-level setting provides a default, while the file-level setting allows per-file control.


## 9. Other Resources

[Blob Server Reference](../../references/stores/blob_reference.md)
