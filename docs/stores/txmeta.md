# 📒 TXMeta Store

## Index

1. [Description ](#1-description-)
- [1.1 Purpose](#11-purpose)
2. [Architecture](#2-architecture)
3. [Data Model](#3-data-model)
4. [Use Cases](#4-use-cases)
- [4.1. Asset Server](#41-asset-server)
- [4.2. Transaction Validation Service](#42-transaction-validation-service)
- [4.3. Block Validation Service](#43-block-validation-service)
   - [4.3.1. Block Validation TX Meta Cache](#431-block-validation---tx-meta-cache)
   - [4.3.2. Block Validation Validate Subtrees](#432-block-validation---validate-subtrees)
   - [4.3.3. Block Validation Store Coinbase TX Meta Data](#433-block-validation---store-coinbase-tx-meta-data)
   - [4.3.4. Block Validation Update TX Meta as Mined](#434-block-validation---update-tx-meta-as-mined)
- [4.4. Block Assembly Service](#44-block-assembly-service)
5. [Technology ](#5-technology-)
- [5.1. Language and Libraries](#51-language-and-libraries)
- [5.2. Data Stores](#52-data-stores)
- [5.3. Data Purging](#53-data-purging)
6. [Directory Structure and Main Files](#6-directory-structure-and-main-files)
7. [How to run](#7-how-to-run)
- [ 7.1. How to run](#-71-how-to-run)
- [7.2. Configuration options (settings flags)](#72-configuration-options-settings-flags)
   - [Timeout Settings](#timeout-settings)
   - [Time-to-Live (TTL) Settings](#time-to-live-ttl-settings)
   - [Batch Processing Settings](#batch-processing-settings)
   - [Block Validation TX Meta Cache Settings ](#block-validation---tx-meta-cache-settings-)
   - [Data Store Examples](#data-store-examples)


## 1. Description

### 1.1 Purpose

The TX Meta Store provides a persistent storage mechanism for transaction metadata (TX Meta) in the Teranode node. The main reason to maintain this datastore is to provide fast lookup of relevant transaction metadata for tx / block validation and assembly processes.

The TX Meta (Transaction Metadata) is essentially additional information stored about each transaction that goes beyond the extended transaction data typically stored in the blockchain.

The TX Meta store offers the following functions:

1. **Create**: It creates a new transaction metadata record. It requires the original transaction, fees, size, parent transaction hashes, block hashes, and the lock time.

2. **Get**: It retrieves the metadata for a specific transaction using its unique identifier (hash).

3. **GetMeta**: It retrieves the metadata for a specific transaction using its unique identifier (hash), but only returns the basic metadata (fees, size, and the lock time).

4. **Delete**: It removes a transaction metadata record from the storage.

5. **SetMined**: It updates the metadata of a transaction to reflect that it has been mined and included in a block.

6. **SetMinedMulti**: It updates the metadata of multiple transactions to reflect that they have been mined and included in a block.


## 2. Architecture

The TX Meta Store is a microservice that is used by other microservices to retrieve or store / modify TX Meta data.

![TX_Meta_Store_Container_Context_Diagram.png](..%2Fservices%2Fimg%2FTX_Meta_Store_Container_Context_Diagram.png)


From the point of view of the components:

![TX_Meta_Store_Component_Context_Diagram.png](..%2Fservices%2Fimg%2FTX_Meta_Store_Component_Context_Diagram.png)



The TX Meta Service / Store uses a number of different datastores, either in-memory or persistent, to store the TX Meta data.

1. **AeroSpike**
2. **Badger**
3. **Memory**
4. **NullStore**
5. **Redis**
6. **SQL**
   * **Postgres**

The TX Meta configuration implementation (datastore type) is consistent within a Teranode node (every service connects to the same specific implementation), and it is defined via settings (`txmeta_store`), as it can be seen in `main_stores.go` (`getTxMetaStore()` method).

Notice how some of the databases are in-memory, while others are persistent (and shared with other services).

More details about the specific stores can be found in the [Technology](#5-technology-) section.

Notice how the architecture introduces an abstraction layer provided by the TX Meta Service, which then uses the TX Meta Store. Clients connect to the TX Meta Service through gRPC.

However, the client services can be configured to either use the TX Meta Service (via gRPC) or directly create an internal TX Meta Store instance. Independently of whether the client uses the gRPC client interface or the TX meta store, the same API methods are available. Once the settings are in place, the implementation and connection type are transparent to the client.

We can see in this excerpt from `main_stores.go` (`getTxMetaStore()` method) how the client can be configured to use the TX Meta Service or the TX Meta Store directly:

```go
func getTxMetaStore(logger *gocore.Logger) txmetastore.Store {
...
...
    if txMetaStoreURL.Scheme == "memory" {
		// the memory store is reached through a grpc client
		txMetaStore, err = txmeta.NewClient(context.Background(), logger)
		if err != nil {
			panic(err)
		}
	} else {
		txMetaStore, err = store.New(logger, txMetaStoreURL)
		if err != nil {
			panic(err)
		}
	}
	...

```

We can see here how a client works with either the TX Meta Service or the TX Meta Store:

![tx_meta_service_vs_store.svg](..%2Fservices%2Fimg%2Fplantuml%2Ftxmeta%2Ftx_meta_service_vs_store.svg)

Finally, note how the Block Validation implements a proxy cache for the TX Meta Store, which is used to cache the TX Meta data for a short period of time (15 minutes TTL by default). This is done to avoid repeated calls to the TX Meta Store for the same TX Meta data, which is a common scenario during block validation. Notice how this caching mechanism is implemented as part of the Block Validation service, and it is not a core feature of the TX Meta Store.

![Block_Validation_And_TX_Meta_Cache_Component_Context_Diagram.png](..%2Fservices%2Fimg%2FBlock_Validation_And_TX_Meta_Cache_Component_Context_Diagram.png)

The Block Validation TXMetaCache is implemented in `txmetacache.go`, and it offers the same API interfaces as the original TX Meta Store, in such a way that the Block Validation could use them indistinctly.

## 3. Data Model

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

## 4. Use Cases


### 4.1. Asset Server

![tx_meta_asset_service_diagram.svg](..%2Fservices%2Fimg%2Fplantuml%2Ftxmeta%2Ftx_meta_asset_service_diagram.svg)


The UI Dashboard, and any third party HTTP client, will access the Asset Server to retrieve both transactions and their meta data.
In this case, the tx meta store is used for both tx meta and transactions. This is because the transaction extended bytes are stored in the tx meta store.



### 4.2. Transaction Validation Service

![tx_meta_transaction_validation.svg](..%2Fservices%2Fimg%2Fplantuml%2Ftxmeta%2Ftx_meta_transaction_validation.svg)

The Transaction Validation Service will store the TX Meta when a transaction is validated, deleting it if there is any error sending the tx to the block assembler.

### 4.3. Block Validation Service


#### 4.3.1. Block Validation - TX Meta Cache

The Block Validation service uses a proxy cache for the TX Meta Store, which is used to cache the Tx Meta data for a short period of time (15 minutes TTL by default, which should be enough for the block to be mined). This is done to avoid repeated calls to the TX Meta Store for the same TX Meta data, which is a common scenario during block validation.

![tx_meta_caching.svg](..%2Fservices%2Fimg%2Fplantuml%2Ftxmeta%2Ftx_meta_caching.svg)


1. The `TX Meta Cache` checks if the metadata is in the cache.
    - If it's a cache miss, it fetches the data from the `TX Meta Store`, caches it, and returns the data.
    - If it's a cache hit and the data is not marked as mined, it returns the data directly.
2. If the cached data is marked as mined, the `TX Meta Cache` may need to update the cache with fresh data from the `TX Meta Store` to reflect any new state.

This caching mechanism is controlled via settings by the code excerpt below (`services/blockvalidation/Server.go`):

```go
// create a caching tx meta store
if gocore.Config().GetBool("blockvalidation_txMetaCacheEnabled", true) {
	logger.Infof("Using cached version of tx meta store")
	bVal.txMetaStore = newTxMetaCache(txMetaStore)
} else {
	bVal.txMetaStore = txMetaStore
}
```


#### 4.3.2. Block Validation - Validate Subtrees


When the Block Validation service validates a subtree, it might find unknown (not previously "blessed") subtrees. This will happen with the first subtree of any block (given the miner would have re-computed the merkle root after creating the coinbase tx), but also for subtrees the node might have not received or might have failed to validate in the past. In this case, the validating node will fetch the missing subtree from the node that sent the block, and it will then proceed to validate the received subtree.


As part of that missing subtree validation, it will retrieve the tx meta data for transactions in the new subtree from the TX Meta Store.


![tx_meta_validate_subtree.svg](..%2Fservices%2Fimg%2Fplantuml%2Ftxmeta%2Ftx_meta_validate_subtree.svg)


#### 4.3.3. Block Validation - Store Coinbase TX Meta Data


![tx_meta_transaction_validation_coinbase_tx.svg](..%2Fservices%2Fimg%2Fplantuml%2Ftxmeta%2Ftx_meta_transaction_validation_coinbase_tx.svg)

As part of the block validation, we store the coinbase tx meta data in the TX Meta Store.

#### 4.3.4. Block Validation - Update TX Meta as Mined

![tx_meta_transaction_validation_set_as_mined.svg](..%2Fservices%2Fimg%2Fplantuml%2Ftxmeta%2Ftx_meta_transaction_validation_set_as_mined.svg)

Once a block is validated, the TX Meta data for the transactions included in the block needs to be updated to reflect that they have been mined. This is done by the Block Validation service, which calls the `SetMinedMulti()` method of the TX Meta Store.

This action updates the list of block hashes for this Tx Meta data. A mined Tx Meta data should reflect at least one block hash (the block where it was mined), but it can also reflect multiple block hashes (in case of a fork).


### 4.4. Block Assembly Service


![tx_meta_block_assembly.svg](..%2Fservices%2Fimg%2Fplantuml%2Ftxmeta%2Ftx_meta_block_assembly.svg)


1. **Submit Mining Solution**: The `BlockAssembly` component calls `submitMiningSolution()` on itself. It then interacts with the `TxMetaStore` to create metadata for the `coinbaseTx`.

2. **Update Mined Status**: After submitting the mining solution, `BlockAssembly` calls `UpdateTxMinedStatus()` in `model/update-tx-mined.go`. This function then communicates with the `TxMetaStore` to set the mined status for multiple transactions (`setMinedMulti`), indicating that they are part of the newly mined block.


## 5. Technology


### 5.1. Language and Libraries

1. **Go Programming Language (Golang)**:

A statically typed, compiled language known for its simplicity and efficiency, especially in concurrent operations and networked services.
The primary language used for implementing the service's logic.

### 5.2. Data Stores

The following datastores are supported (either in development / experimental or production mode):

1. **Aerospike**:
    - A high-performance, NoSQL distributed database.
    - Suitable for environments requiring high throughput and low latency.
    - Handles large volumes of UTXO data with fast read/write capabilities.
    - https://aerospike.com.

2. **Memory (In-Memory Store)**:
    - Stores UTXOs directly in the application's memory.
    - Offers the fastest access times but lacks persistence; data is lost if the service restarts.
    - Useful for development or testing purposes.

3. **Nullstore**:
    - A dummy or placeholder implementation, used for testing (when no actual storage is needed).
    - Can be used to mock UTXO store functionality in a development or test environment.

4. **Redis**:
    - An in-memory data structure store, used as a database, cache, and message broker.
    - Offers fast data access and can persist data to disk.
    - Useful for scenarios requiring rapid access combined with the ability to handle volatile or transient data.
    - https://redis.com.

5. **Badger**:
    - BadgerDB is a fast, embeddable, persistent key-value (KV) database that is written in the Go programming language.
    - Suitable for write-heavy workloads.
    - https://github.com/dgraph-io/badger.

6. **SQL**:
    - SQL-based relational database implementations like PostgreSQL.
    - PostgreSQL: Offers robustness, advanced features, and strong consistency, suitable for complex queries and large datasets.
        - https://www.postgresql.org.

- The choice of implementation depends on the specific requirements of the BSV node, such as speed, data volume, persistence, and the operational environment.
- Memory-based stores (like in-memory and Redis) are typically faster but may require additional persistence mechanisms.
- Databases like Aerospike, Badger, and PostgreSQL provide a balance of speed and persistence, suitable for larger, more complex systems.
- Nullstore is more appropriate for testing, development, or lightweight applications.


### 5.3. Data Purging

Stored TX Meta data is automatically purged a certain TTL (Time To Live) period after it is mined. This is done to prevent the datastore from growing indefinitely and to ensure that only relevant data (i.e. data that can be mined or was recently mined) is kept in the store.


## 6. Directory Structure and Main Files



```
stores/txmeta/              # Directory for the txmeta store
│
├── Interface.go            # Interface definition for txmeta store
│
├── aerospike               # Aerospike-specific store implementation
│   └── aerospike.go        # Implementation file for Aerospike
│
├── aerospikemap            # Map utility based on Aerospike
│   └── aerospikemap.go     # Implementation file for aerospikemap
│
├── badger                  # BadgerDB-specific store implementation
│   └── badger.go           # Implementation file for BadgerDB
│
├── data.go                 # Data structures for txmeta store
│
├── memory                  # In-memory store implementation
│   └── memory.go           # Implementation file for in-memory store
│
├── nullstore               # Null (no-op) store implementation
│   └── nullstore.go        # Implementation file for nullstore
│
├── redis                   # Redis-specific store implementation
│   ├── redis.go            # Implementation file for Redis
│   └── update_blockhash.lua # Lua script for Redis operations
│
├── sql                     # SQL-specific store implementation
│   └── sql.go              # Implementation file for SQL store
│
└── tests                   # General tests for txmeta store implementations
    ├── data.go             # Data file for tests
    └── tests.go            # Test implementations

```


## 7. How to run


###  7.1. How to run

To run the TX Meta Store locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -TxMetaStore=1
```

Please refer to the [Locally Running Services Documentation](../locallyRunningServices.md) document for more information on running the Bootstrap Service locally.


### 7.2. Configuration options (settings flags)


#### Timeout Settings
- `txmeta_store_dbTimeoutMillis.{ENVIRONMENT}`: The database timeout setting in milliseconds. Default for all environments is 5000ms (5 seconds).

#### Time-to-Live (TTL) Settings
- `txmeta_store_ttl.{ENVIRONMENT}`: Time-to-live for the TX meta store entries in seconds. Default is set to 14400 seconds (4 hours).

#### Batch Processing Settings
- `txmeta_store_maxMinedBatchSize.{ENVIRONMENT}`: Maximum number of mined transactions processed in a batch. Default size is set to 1024.

- `txmeta_store_maxMinedRoutines.{ENVIRONMENT}`: Maximum number of concurrent goroutines for processing mined transactions. Default is set to 4.

#### Block Validation - TX Meta Cache Settings
- `blockvalidation_txMetaCacheEnabled`: Enables the TX Meta Cache for the Block Validation service. Default is set to `true`.

#### Data Store Examples
Different datastores can be configured for various environments. Below are the examples of how to set the `txmeta_store` for different datastores:

- For a null datastore:
```
txmeta_store.{ENVIRONMENT}=null:///
 ```

- For an in-memory datastore:
```
txmeta_store.{ENVIRONMENT}=memory://localhost:${TX_META_GRPC_PORT}
```

- For a PostgreSQL datastore:
```
txmeta_store.{ENVIRONMENT}=postgres://ubsv:ubsv@localhost:5432/ubsv
```

- For a Redis datastore:
```
txmeta_store.{ENVIRONMENT}=redis://redis-1-headless.redis-miner1.svc.cluster.local:${REDIS_PORT}
```


Replace `{ENVIRONMENT}` with the specific environment you are configuring, such as `development`, `staging`, `production`, your dev username, etc.

The `${TX_META_GRPC_PORT}` and `${REDIS_PORT}` are placeholders for the respective ports to be used for the gRPC server and Redis datastore, which should be replaced with actual port numbers.
