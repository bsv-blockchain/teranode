# 🌐 Coinbase Service

## Index

1. [Description](#1-description)
2. [Functionality](#2-functionality)
- [2.1. Block Processing ](#21-block-processing-)
- [2.2. Catching Up on Missing Parent Blocks](#22-catching-up-on-missing-parent-blocks)
- [2.2. gRPC Methods](#22-grpc-methods)
3. [gRPC Protobuf Definitions](#3-grpc-protobuf-definitions)
4. [Data Model](#4-data-model)
5. [Technology and specific Stores](#5-technology-and-specific-stores)
6. [Directory Structure and Main Files](#6-directory-structure-and-main-files)
7. [How to run](#7-how-to-run)
8. [Configuration options (settings flags)](#8-configuration-options-settings-flags)

## 1. Description

The Coinbase Service is designed to monitor the blockchain for new coinbase transactions, record them, track their maturity, and manage the spendability of the rewards miners earn.

In the Teranode context, the "coinbase transaction" is the first transaction in the first subtree of a block and is created by the Block Assembly. This transaction is unique in that it creates new coins from nothing as a reward for the miner's work in processing transactions and securing the network.

The Coinbase primary function is to monitor all blocks being mined, ensuring accurate tracking of the blocks that have been mined along with their Coinbase Unspent Transaction Outputs (UTXOs).

The container diagram can be seen here:


![Coinbase_Service_Container_Diagram.png](img%2FCoinbase_Service_Container_Diagram.png)

Please note that the Coinbase Service interacts with its own Blockchain datastore, which is operated separately from the Blockchain datastore used by other services, including the Blockchain Service. This separation enables the Coinbase Service to operate independently of the core node services.

As we can see in the more detailed diagram below, the Service also starts a gRPC server, which can be used to interact with the Coinbase Service (this is not used by any production service, but by testing and experimental applications).

![Coinbase_Service_Component_Diagram.png](img%2FCoinbase_Service_Component_Diagram.png)

The Coinbase Store is a SQL database that stores the Coinbase UTXOs and their maturity, such as PostgreSQL.


When a miner intends to spend one of their coins, they need to retrieve the corresponding UTXO from the Coinbase Service. Subsequently, they can generate a valid transaction and transmit this through the Coinbase Service. This action labels the Coinbase UTXO as spent.

In essence, the Coinbase Service operates as a straightforward Simplified Payment Verification (SPV) overlay node, custom-built to cater to the requirements of miners.


## 2. Functionality

The main purpose of the Coinbase Service is to monitor the blockchain for new blocks and extract the Coinbase transaction from each block.

The Coinbase transaction is then processed, and the Coinbase UTXOs are stored in the database.

The Coinbase Service also tracks the maturity of the Coinbase UTXOs, ensuring that they are not spendable until they have matured.

### 2.1. Block Processing

![coinbase_process_block.svg](img%2Fplantuml%2Fcoinbase%2Fcoinbase_process_block.svg)

1. The Coinbase Service subscribes to the Asset Service, which notifies the Coinbase Service when a new block is found.
2. The Coinbase Service then requests the block from the Asset Service.
3. The block is stored in the Coinbase Blockchain store.
4. The Coinbase Service then processes the Coinbase transaction, extracting the Coinbase UTXOs and storing them in the Coinbase store.
5. The Coinbase Service then checks if any prior Coinbase UTXOs have matured (typically after 100 blocks have been mined on top of a Coinbase Tx), and if so, it marks them as spendable.

### 2.2. Catching Up on Missing Parent Blocks

When a block parent is unknown by the Service, a catch up process is triggered. This process requests the missing blocks from the Asset Service, and processes them in order to ensure that the Coinbase UTXOs are tracked correctly.

![coinbase_catchup.svg](img%2Fplantuml%2Fcoinbase%2Fcoinbase_catchup.svg)

1. The Coinbase Service identifies the missing parent blocks.
2. Each block is processed, in order, by the Coinbase Service. The storeBlock() function was already described in the previous section.

### 2.2. gRPC Methods

The Coinbase Service offers a number of gRPC methods that can be used for miners to interact with the Coinbase Service. You can read more about them in the [gRPC Protobuf Definitions](#3-grpc-protobuf-definitions) section.


## 3. gRPC Protobuf Definitions

The Coinbase Service uses gRPC for communication between nodes. The protobuf definitions used for defining the service methods and message formats can be seen [here](protobuf_docs/coinbaseProto.md).


## 4. Data Model

The Service receives blocks and processes them, extracting the Coinbase transaction and storing it in the database. The Block and Transaction data model will not be covered in this document, as it is sufficiently covered in other documents (please refer to the [Architecture Overview](../architecture/teranode-architecture.md).

The Coinbase Service stores the Coinbase UTXOs in a database, along with their maturity. The maturity is the number of blocks that must be mined on top of the block that contains the Coinbase transaction before the UTXO can be spent.

In terms of the Coinbase specific data abstractions, the Coinbase Service uses the following data structures:


* **Coinbase UTXO tracking**

For every new Coinbase TX, the Coinbase Service will track the Coinbase TX UTXOs, including their maturity (locking_script).

In PostgreSQL, this is the data structure.


| Column Name    | Data Type    | Description                                                                          |
|----------------|--------------|--------------------------------------------------------------------------------------|
| inserted_at    | TIMESTAMPTZ  | Timestamp of when the record created.                                                |
| block_id       | BIGINT       | The ID of the block in which the UTXO was included.                                  |
| txid           | BYTEA        | The transaction ID of the UTXO, stored as binary data.                               |
| vout           | INTEGER      | The index of the UTXO in the transaction's output list.                              |
| locking_script | BYTEA        | The script that locks the UTXO, defining the conditions under which it can be spent. |
| satoshis       | BIGINT       | The amount of satoshis contained in the UTXO.                                        |
| processed_at   | TIMESTAMPTZ  | The timestamp of when the UTXO was processed (made spendable).                       |



* **Coinbase Spendable UTXOs**

Once a Coinbase UTXO has matured, it becomes spendable. The Coinbase Service will track the spendable Coinbase UTXOs in the database, so miners can claim them.

| Column Name    | Data Type    | Description                                                            |
|----------------|--------------|------------------------------------------------------------------------|
| inserted_at    | TIMESTAMPTZ  | Timestamp of when the spendable record was created.                    |
| txid           | BYTEA        | The transaction ID where the UTXO originated, stored in binary format. |
| vout           | INTEGER      | The output index number of the UTXO within the transaction.            |
| locking_script | BYTEA        | The script that defines the conditions needed to spend the UTXO.       |
| satoshis       | BIGINT       | The amount of satoshis that the UTXO represents.                       |


## 5. Technology and specific Stores

1. **Go (Golang)**:
  - The primary programming language used for the implementation.

2. **gRPC**:
  - A high-performance, open-source universal RPC framework that leverages HTTP/2 for transport, Protocol Buffers as the interface description language, and provides features such as authentication, load balancing, and more.

3. **Protocol Buffers (protobuf)**:
  - A language-neutral, platform-neutral, extensible mechanism for serializing structured data, similar to XML or JSON but smaller, faster, and simpler. It's used to define the API schema (`coinbase_api.proto`) and generate corresponding Go code.

4. **Bitcoin SV (BSV) Libraries**:
  - `github.com/libsv/go-bt`: A Go library used for building and signing Bitcoin transactions.
  - `github.com/libsv/go-bk`: Includes utilities for Bitcoin keys, used here for operations like WIF (Wallet Import Format) decoding, which is a common way to represent private keys in Bitcoin.
  - `github.com/libsv/go-bt/v2/bscript`: Used for handling Bitcoin scripts, which are part of the locking and unlocking mechanism that controls spending of bitcoins.

5. **PostgreSQL & SQLite**:
  - PostgreSQL is the production database recommendation for the Coinbase Service. SQLite can be optionally used for testing and development environments. The database engine being used executes SQL queries accordingly.

6. **gocore**:
  - A package for configuration management.


## 6. Directory Structure and Main Files

```
services/coinbase
│
├── Client.go
│   ├── Implementation of the client logic that interacts with the coinbase service, using gRPC protocol.
│
├── Coinbase.go
│   ├── Main business logic of the coinbase functionality, including processing transactions and managing UTXOs.
│
├── Interface.go
│   ├── Interfaces that the coinbase service implements, providing a contract for the service's functionality.
│
├── Server.go
│   ├── Contains the server-side logic, including the setup and management of the gRPC server, and the subscription to the Asset Server.
│
├── coinbase_api
│   │
│   ├── coinbase_api.pb.go
│   │   ├── Generated by the protocol buffer compiler, this file contains Go bindings for the messages defined in the `.proto` file.
│   │
│   ├── coinbase_api.proto
│   │   ├── The protocol buffer file defining the data structures and RPC services for the coinbase service's API.
│   │
│   ├── coinbase_api_extra.go
│   │   ├── Helper functions and extensions related to the generated code from `coinbase_api.pb.go`.
│   │
│   └── coinbase_api_grpc.pb.go
│       ├── Another auto-generated file that includes Go bindings specifically for gRPC, derived from the `.proto` service definitions.
│
└── metrics.go
    ├── Functionality for tracking and recording metrics, for monitoring and alerting purposes.
```

## 7. How to run

To run the Coinbase Service locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -Coinbase=1
```

Please refer to the [Locally Running Services Documentation](../locallyRunningServices.md) document for more information on running the Block Assembly Service locally.

## 8. Configuration options (settings flags)

### Server and Client Configuration
- **`coinbase_grpcListenAddress`**: Determines the gRPC server listen address for the Coinbase service. It is checked within the `Enabled()` function to decide if the Coinbase service should be active.
- **`coinbase_store`**: URL for the Coinbase store, required for initializing the Coinbase service by connecting to the specified database for storing and managing Coinbase transactions and UTXOs.
- **`coinbase_grpcAddress`**: Specifies the gRPC address for the Coinbase client, used for connecting to the Coinbase service from a client application.

### Coinbase Configuration
- **`coinbase_wallet_private_key`**: The private key associated with the Coinbase wallet, necessary for signing transactions and managing Coinbase UTXOs.
- **`distributor_backoff_duration`**: Configures the backoff duration for the distributor, which is used when retrying failed transaction distributions.
- **`distributor_max_retries`**: The maximum number of retry attempts for distributing transactions.
- **`distributor_failure_tolerance`**: The failure tolerance level for the distributor, indicating how many distribution failures are acceptable before marking a distribution as failed.
- **`blockchain_store_dbTimeoutMillis`**: The timeout in milliseconds for database operations within the blockchain store, impacting performance and responsiveness of data access and manipulation.
- **`propagation_grpcAddresses`**: A list of gRPC addresses for peer services, used for network communication and data propagation among peers.
- **`peerStatus_timeout`**: The timeout for peer status checks, ensuring timely validation of peer connectivity and synchronization status.
- **`coinbase_wait_for_peers`**: A boolean flag indicating whether the Coinbase service should wait for peers to be in sync before proceeding with its operations.
- **`coinbase_notification_threshold`**: The threshold for sending notifications about the Coinbase balance or UTXO count, used for monitoring and alerting purposes.
- **`coinbase_should_wait`** and **`coinbase_wait_until_block`**: These settings control whether the Coinbase service should delay its start until the blockchain reaches a certain block height, ensuring synchronization with the network.

### Miscellaneous Configuration
- **`coinbase_assetGrpcAddress`** and **`asset_grpcAddress`**: Addresses for connecting to the asset service via gRPC. These are used for fetching blockchain data, such as blocks and transactions, necessary for Coinbase operations.
- **`use_open_tracing`**: Indicates whether open tracing should be used for gRPC clients, aiding in monitoring and troubleshooting by tracing the request flow.
- **`use_prometheus_grpc_metrics`**: Specifies whether Prometheus metrics should be collected for gRPC operations, useful for performance monitoring and analysis.
