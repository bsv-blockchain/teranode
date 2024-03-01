# 🗃️ UTXO Store

## Index

1. [Description](#1-description)
2. [Architecture ](#2-architecture-)
3. [UTXO - Data Model](#3-utxo---data-model)
4. [Use Cases](#4-use-cases)
5. [Technology](#5-technology)
    - [5.1. Language and Libraries](#51-language-and-libraries)
    - [5.2. Data Stores](#52-data-stores)
6. [Directory Structure and Main Files](#-6-directory-structure-and-main-files)
7. [Running the Service Locally](#7-running-the-service-locally)


## 1. Description

The UTXO Store is responsible for tracking spendable UTXOs. These are UTXOs that can be used as inputs in new transactions. The UTXO Store is an internal datastore used by some of the services, such as the Asset Server, the TX Validator and the Block Assembly. The main purpose of this store is to maintain the UTXO data on behalf of other micro-services.

It handles the core functionalities of the UTXO Store:

* **Health**: Check the health status of the UTXO store service.
* **Get**: Retrieve a specific UTXO.
* **Store**: Add new UTXOs to the store.
* **Spend/UnSpend**: Mark UTXOs as spent or reverse such markings, respectively.
* **Delete**: Remove UTXOs from the store.
* **Block Height Management**: Set and retrieve the current blockchain height, which can be crucial for determining the spendability of certain UTXOs based on locktime conditions.

**Principles**:

- All operations are atomic.
- All data is shared across servers with standard sharing algorithms.
- In production, the data is stored in a Master and Replica configuration.
- No centralised broker - all clients know where each hash is stored.
- No cross-transaction state is stored.

## 2. Architecture

The UTXO Store is a micro-service that is used by other micro-services to retrieve or store / modify UTXOs.


![UTXO_Store_Container_Context_Diagram.png](..%2F..%2Fdocs%2Fservices%2Fimg%2FUTXO_Store_Container_Context_Diagram.png)


The UTXO Store uses a number of different datastores, either in-memory or persistent, to store the UTXOs.


![UTXO_Store_Component_Context_Diagram.png](..%2F..%2Fdocs%2Fservices%2Fimg%2FUTXO_Store_Component_Context_Diagram.png)

The UTXO store implementation is consistent within a Teranode node (every service connects to the same specific implementation), and it is defined via settings (`utxostore`), as it can be seen in the following code fragment (`main.go`):

```go

func getUtxoStore(ctx context.Context, logger ulogger.Logger) utxostore.Interface {
	if utxoStore != nil {
		return utxoStore
	}

	utxoStoreURL, err, found := gocore.Config().GetURL("utxostore")
	if err != nil {
		panic(err)
	}
	if !found {
		panic("no utxostore setting found")
	}
	utxoStore, err = utxo_factory.NewStore(ctx, logger, utxoStoreURL, "main")
	if err != nil {
		panic(err)
	}

	return utxoStore
}
```

The following datastores are supported (either in development / experimental or production mode):

1. **Aerospike**.

2. **Memory (In-Memory Store)**.

3. **Nullstore**.

4. **Redis**.

5. **SQL**:
    - SQL-based relational database implementations. At this point, only PostgreSQL is implemented.

Notice how some of the databases are in-memory, while others are persistent (and shared with other services).

More details about the specific stores can be found in the [Technology](#5-technology) section.


## 3. UTXO - Data Model

## 2.1. What is an UTXO?

The Teranode UTXO is no different from Bitcoin UTXO. The following is a description of the Bitcoin UTXO model, focusing on the BSV implementation:

- **Transaction Outputs**: When a transaction occurs on the blockchain, it creates "transaction outputs," which are essentially chunks of cryptocurrency value. Each output specifies an amount and a condition under which it can be spent (a cryptographic script key that the receiver owns).

Under the external library `github.com/ordishs/go-bt/output.go`, we can see the structure of a transaction output.

```go
type Output struct {
	Satoshis      uint64          `json:"satoshis"`
	LockingScript *bscript.Script `json:"locking_script"`
}
```

Components of the `Output` struct:

1. **Satoshis (`uint64`)**:
    - The amount of BSV cryptocurrency associated with this output.
    - The unit "Satoshis" refers to the smallest unit of Bitcoin (1 Bitcoin = 100 million Satoshis).

2. **LockingScript (`*bscript.Script`)**:
    - This field represents the conditions that must be met to spend the Satoshis in this output.
    - The `LockingScript`, often referred to as the "scriptPubKey" in Bitcoin's technical documentation, is a script written in Bitcoin's scripting language.
    - This script contains cryptographic conditions to unlock the funds.


Equally, we can see how a list of outputs is part of a transaction (`github.com/ordishs/go-bt/tx.go`):
```go
type Tx struct {
	Inputs   []*Input  `json:"inputs"`
	Outputs  []*Output `json:"outputs"`
	Version  uint32    `json:"version"`
	LockTime uint32    `json:"locktime"`
}
```

- **Unspent Transaction Outputs (UTXOs)**: A UTXO is a transaction output that hasn't been used as an input in a new transaction.

When a transaction occurs, it consumes one or more UTXOs as inputs and creates new UTXOs as outputs. The sum of the input UTXOs represents the total amount of Bitcoin being transferred, and the outputs represent the distribution of this amount after the transaction.

To "own" bitcoins means to control UTXOs on the blockchain that can be spent by the user (i.e., the user has the private key to unlock these UTXOs).

When a user creates a new transaction, the transaction references these UTXOs as inputs, proving his ownership by fulfilling the spending conditions set in these UTXOs (signing the transaction with the user's private key).

Independent UTXOs can be processed in parallel, potentially improving the efficiency of transaction validation.

To know more about UTXOs, please check https://bitcoin-association.gitbook.io/bitcoin-protocol-documentation/cJw8Rc8JxwBTZVOoBFC6/transaction-lifecycle/transaction-inputs-and-outputs.

### 3.2. How are UTXOs stored?


The UTXO data is stored in the following format:

| Field     | Description                                                   |
|-----------|---------------------------------------------------------------|
| hash      | The hash of the UTXO for identification purposes              |
| lock_time | The block number or timestamp at which this UTXO can be spent |
| tx_id     | The transaction ID where this UTXO was spent                  |


The UTXO will be first persisted with its hash (as key) and a lock time. Once the UTXO is spent, the spending tx_id will be saved.

The process can be summarised here:

![utxo_hash_computation.svg](..%2Fservices%2Fimg%2Fplantuml%2Futxo%2Futxo_hash_computation.svg)


- To compute the hash of the key, the caller must know the complete data and calculate the hash before calling the API.


- The hashing logic can be found in `UTXOHash.go`:

```go
func UTXOHash(previousTxid *chainhash.Hash, index uint32, lockingScript []byte, satoshis uint64) (*chainhash.Hash, error) {
	if len(lockingScript) == 0 {
		return nil, fmt.Errorf("locking script is nil")
	}

	if satoshis == 0 {
		return nil, fmt.Errorf("satoshis is 0")
	}

	utxoHash := make([]byte, 0, 256)
	utxoHash = append(utxoHash, previousTxid.CloneBytes()...)
	utxoHash = append(utxoHash, bt.VarInt(index).Bytes()...)
	utxoHash = append(utxoHash, lockingScript...)
	utxoHash = append(utxoHash, bt.VarInt(satoshis).Bytes()...)

	hash := crypto.Sha256(utxoHash)
	chHash, err := chainhash.NewHash(hash)
	if err != nil {
		return nil, err
	}

	return chHash, nil
}
```

- The existence of the key confirms the details of the UTXO are the same as what the caller has.



## 4. Use Cases

## 4.1. Asset Server:

![utxo_asset_server.svg](..%2Fservices%2Fimg%2Fplantuml%2Futxo%2Futxo_asset_server.svg)

1. The **UI Dashboard** sends a request to the AssetService for UTXO data.
2. The **AssetService** forwards this request to the **UTXO Store**.
3. The **UTXO Store** interacts with the underlying **Datastore** implementation to fetch the requested UTXO data and check the health status of the store.
4. The **Datastore** implementation returns the UTXO data and health status to the UTXO Store.
5. The UTXO Store sends this information back to the AssetService.
6. Finally, the AssetService responds back to the UI Dashboard.

To know more about the AssetService, please check its specific service documentation.

## 4.2. Block Assembly:


![utxo_block_assembly.svg](..%2Fservices%2Fimg%2Fplantuml%2Futxo%2Futxo_block_assembly.svg)

Coinbase Transaction creation (UTXO step):

1. The **Block Assembly** service (see `SubtreeProcessor.go`, `processCoinbaseUtxos` method) creates a new Coinbase transaction.
2. The **Block Assembly** service sends a request to the **UTXO Store** to store the Coinbase UTXO.
3. The **UTXO Store** interacts with the underlying **Datastore** implementation to store the Coinbase UTXO.

Coinbase Transaction deletion (UTXO step):

1. The **Block Assembly** service (see `SubtreeProcessor.go`, `` method)  deletes the Coinbase transaction. This is done when blocks are reorganised and previously tracked coinbase transactions lose validity.
2. The **Block Assembly** service sends a request to the **UTXO Store** to delete the Coinbase UTXO.
3. The **UTXO Store** interacts with the underlying **Datastore** implementation to delete the Coinbase UTXO.

To know more about the Block Assembly, please check its specific service documentation.

## 4.3. Transaction Validator.

![utxo_transaction_validator.svg](..%2Fservices%2Fimg%2Fplantuml%2Futxo%2Futxo_transaction_validator.svg)

The Transaction Validator uses the UTXO Store to perform a number of UTXO related operations:

1. Obtain the current block height from the **UTXO Store**.
2. Mark a UTXO as spent. If needed, it can also request to unspend (revert) a UTXO.
6. Store new UTXOs.

When marking a UTXO as spent, the store will check if the UTXO is known (by its hash), and whether it is spent or not. If it is spent, we will return one response message or another depending on whether the spending tx_id matches. See here:

![utxo_spend_process.svg](..%2Fservices%2Fimg%2Fplantuml%2Futxo%2Futxo_spend_process.svg)

On the other hand, we can see the process for unspending an UTXO here:

![utxo_unspend_process.svg](..%2Fservices%2Fimg%2Fplantuml%2Futxo%2Futxo_unspend_process.svg)

To know more about the Transaction Validator, please check its specific service documentation.



## 5. Technology

### 5.1. Language and Libraries

1. **Go Programming Language (Golang)**:

A statically typed, compiled language known for its simplicity and efficiency, especially in concurrent operations and networked services.
The primary language used for implementing the service's logic.

2. **Bitcoin Transaction (BT) GoLang library**: `github.com/ordishs/go-bt/` - a full featured Bitcoin transactions and transaction manipulation/functionality Go Library.

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

5. **SQL**:
    - SQL-based relational database implementations like PostgreSQL.
    - PostgreSQL: Offers robustness, advanced features, and strong consistency, suitable for complex queries and large datasets.
      - https://www.postgresql.org.

- The choice of implementation depends on the specific requirements of the BSV node, such as speed, data volume, persistence, and the operational environment.
- Memory-based stores (like in-memory and Redis) are typically faster but may require additional persistence mechanisms.
- Databases like Aerospike, and PostgreSQL provide a balance of speed and persistence, suitable for larger, more complex systems.
- Nullstore is more appropriate for testing, development, or lightweight applications.

- Aerospike or Redis are strongly recommended for Production usage.

### 5.3. Data Purging

Stored data is automatically purged a certain TTL (Time To Live) period after it is spent. This is done to prevent the datastore from growing indefinitely and to ensure that only relevant data (i.e. data that is spendable or recently spent) is kept in the store.

##  6. Directory Structure and Main Files

```
UTXO Store Package Structure (stores/utxo)
├── Interface.go
├── aerospike
│   ├── aerospike.go
├── memory
│   ├── memory.go
├── nullstore
│   └── nullstore.go
├── redis
│   ├── Redis.go
├── sql
│   ├── sql.go
├── tests
│   └── tests.go
└── utils.go
```

- **Interface.go**: Defines the interface for UTXO store operations.

#### aerospike Directory:
- **aerospike.go**: Aerospike UTXO store implementation.
- **aerospike_server_test.go**: Tests for the Aerospike UTXO store.

#### aerospikemap Directory:
- **aerospikemap.go**: An Aerospike map-based UTXO store implementation.

#### memory Directory:
- **memory.go**: Basic in-memory UTXO store.

#### nullstore Directory:
- **nullstore.go**: Implementation of a null or dummy UTXO store.

#### redis Directory:
- **Redis.go**: Redis UTXO store implementation.

#### sql Directory:
- **sql.go**: SQL-based implementation for the UTXO store.

#### tests Directory:
- **tests**: Contains shared or general tests for UTXO store implementations.

#### other:

- **utils.go**: Utility functions used across UTXO store implementations.


## 7. Running the Store Locally


###     How to run

To run the UTXO Store locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -UtxoStore=1
```

Please refer to the [Locally Running Services Documentation](../locallyRunningServices.md) document for more information on running the Bootstrap Service locally.

###     Settings

The `utxostore` setting must be set to pick a specific datastore implementation. The following values are supported:

- **aerospike**: Aerospike UTXO store implementation.

`utxostore.dev.[YOUR_USERNAME]=aerospike://xxxx.ubsv.dev:3000/ubsv-store?ConnectionQueueSize=5&LimitConnectionsToQueueSize=false`

- **memory**: Basic in-memory UTXO store.

`utxostore.dev.[YOUR_USERNAME]=memory://localhost:${UTXO_STORE_GRPC_PORT}/splitbyhash`

- **nullstore**: Implementation of a null or dummy UTXO store.

`utxostore.dev.[YOUR_USERNAME]=null:///`

- **redis**: Redis UTXO store implementation.

`utxostore.dev.[YOUR_USERNAME]=redis://localhost:${REDIS_PORT}`

- **sql**: SQL-based implementation for the UTXO store.

`utxostore.dev.[YOUR_USERNAME]=postgres://ubsv:ubsv@localhost:5432/ubsv`
