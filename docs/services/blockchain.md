# 🌐 Blockchain Service

## Index

1. [Description](#1-description)
2. [Functionality ](#2-functionality-)
- [2.1. Service initialization](#21-service-initialization)
- [2.2. Adding a new block to the blockchain](#22-adding-a-new-block-to-the-blockchain)
- [2.3. Getting a block from the blockchain](#23-getting-a-block-from-the-blockchain)
- [2.4. Getting the last N blocks from the blockchain](#24-getting-the-last-n-blocks-from-the-blockchain)
- [2.5. Checking if a Block Exists in the Blockchain](#25-checking-if-a-block-exists-in-the-blockchain)
- [2.6. Getting the Best Block Header](#26-getting-the-best-block-header)
- [2.7. Getting the Block Headers](#27-getting-the-block-headers)
- [2.8. Invalidating a Block](#28-invalidating-a-block)
- [2.9. Subscribing to Blockchain Events](#29-subscribing-to-blockchain-events)
- [2.10. Triggering a Subscription Notification](#210-triggering-a-subscription-notification)
3. [gRPC Protobuf Definitions](#3-grpc-protobuf-definitions)
4. [Data Model ](#4-data-model-)
- [4.1. Blocks](#41-blocks)
- [4.2. Block Database Data Structure](#42-block---database-data-structure)
5. [Technology](#5-technology)
6. [Directory Structure and Main Files](#6-directory-structure-and-main-files)
7. [How to run](#7-how-to-run)
8. [Configuration options (settings flags)](#8-configuration-options-settings-flags)


## 1. Description

This service implements a local Bitcoin SV (BSV) Blockchain service, maintaining the blockchain as understood by the node.

The service exposes various RPC methods such as `AddBlock`, `GetBlock`, `InvalidateBlock` and `Subscribe`.

The main features of the service are:

1. **Subscription Management**: The service can handle live subscriptions. Clients can subscribe to blockchain events, and the service will send them notifications. It manages new and dead subscriptions and sends out notifications accordingly.

2. **Adding a new Block to the Blockchain**: Allows adding a new block to the blockchain. It accepts a block request, processes it, and stores it in the blockchain store. Notifications are sent out about this new block.

3. **Block Retrieval**: Provides various methods to retrieve block information (`GetBlock`, `GetLastNBlocks`, `GetBlockExists` functions).

4. **Block Invalidation**: It allows to invalidate blocks (`InvalidateBlock` function), as part of a rollback process.

![Blockchain_Service_Container_Diagram.png](img%2FBlockchain_Service_Container_Diagram.png)

To fulfill its purpose, the service interfaces with a blockchain store for data persistence and retrieval.

![Blockchain_Service_Component_Diagram.png](img%2FBlockchain_Service_Component_Diagram.png)

## 2. Functionality

### 2.1. Service initialization

![blockchain_init.svg](img%2Fplantuml%2Fblockchain%2Fblockchain_init.svg)

Explanation of the sequence:

1. **New Method:**
  - The `Main` requests a new instance of the `Blockchain` service by calling the `New` function with a logger.
  - Inside the `New` method, the `Blockchain Service` performs initialization tasks including setting up the blockchain store, initializing various channels, and preparing the context and subscriptions.

2. **Start Method:**
  - The `Main` calls the `Start` method on the `Blockchain` service instance.
  - The `Blockchain Service` starts server operations, including channel listeners and the GRPC server.
  - The service enters a loop handling notifications and subscriptions.

### 2.2. Adding a new block to the blockchain

![blockchain_add_block.svg](img%2Fplantuml%2Fblockchain%2Fblockchain_add_block.svg)

Explanation of the sequence:

1. **Client Request:**
    - The `Client` calls the `AddBlock` method on the `Blockchain Service`, passing the block request.

2. **Parse Block Header:**
    - The `Blockchain Service` parses the block header from the request. If there's an error, it returns the error to the client and the process stops.

3. **Parse Coinbase Transaction:**
    - If the header parsing is successful, the service then parses the coinbase transaction. Again, if there's an error, it returns the error to the client.

4. **Parse Subtree Hashes:**
    - If the coinbase transaction parsing is successful, the service then parses the subtree hashes. If there's an error in parsing, it returns the error to the client.

5. **Store Block:**
    - If all parsing steps are successful, the `Blockchain Service` stores the block using the `Block Store`.

6. **Handle Storage Response:**
    - If there's an error in storing the block, the `Blockchain Service` returns the error to the client.
    - If the block is stored successfully, the `Blockchain Service` proceeds to send notifications.

7. **Send Notifications:**
    - The service sends notifications for the block.

There are 2 clients invoking this endpoint:

1. **The `Block Assembly` service:**
    - The `Block Assembly` service calls the `AddBlock` method on the `Blockchain Service` to add a new mined block to the blockchain.

2. **The `Block Validation` service:**
    - The `Block Validation` service calls the `AddBlock` method on the `Blockchain Service` to add a new block (received from another node) to the blockchain.

### 2.3. Getting a block from the blockchain

![blockchain_get_block.svg](img%2Fplantuml%2Fblockchain%2Fblockchain_get_block.svg)

Explanation of the sequence:

1. **Client Request:**
    - The `Client` calls the `GetBlock` method on the `Blockchain Service`, passing the context and the request.

2. **Parse Block Hash:**
    - The `Blockchain Service` uses the `Model` to parse the block hash from the request. If there's an error, it returns the error to the client.

3. **Retrieve Block from Store:**
    - If the hash parsing is successful, the service then retrieves the block from the `Store` using the parsed hash.

4. **Handle Store Response:**
    - If there's an error in retrieving the block, the service returns the error to the client.
    - If the block is successfully retrieved, the service proceeds to prepare the response.

5. **Prepare Response:**
    - The service loops through each subtree hash in the block, converting them to bytes.
    - The service creates a `GetBlockResponse` with the block's header, height, coinbase transaction, subtree hashes, transaction count, and size in bytes.

6. **Return Response:**
    - The `Blockchain Service` returns the prepared `GetBlockResponse` to the `Client Code`.

There are 2 clients invoking this endpoint:

1. **The `Asset Server` service:**
    - The `Asset Server` service calls the `GetBlock` method on the `Blockchain Service` to retrieve a block from the blockchain.

- **The `Block Assembly` service:**
    - The `Block Assembly` service calls the `GetBlock` method on the `Blockchain Service` to retrieve a block from the blockchain.

### 2.4. Getting the last N blocks from the blockchain

![blockchain_get_last_n_blocks.svg](img%2Fplantuml%2Fblockchain%2Fblockchain_get_last_n_blocks.svg)

Explanation of the sequence:

1. **Client Request:**
    - The `Client` initiates a call to the `GetLastNBlocks` method on the `Blockchain Service`, passing the context and the request.

2. **Retrieve Blocks from Store:**
    - The `Blockchain Service` then calls the `GetLastNBlocks` method on the `Store`, passing the number of blocks, orphan inclusion flag, and the starting height from the request.

3. **Handle Store Response:**
    - If there's an error in retrieving the last N blocks, the service returns the error to the client.
    - If the blocks are successfully retrieved, the service prepares the response.

4. **Prepare and Return Response:**
    - The `Blockchain Service` creates a `GetLastNBlocksResponse` containing the retrieved `blockInfo`.
    - It then returns this response to the `Client`.

The `Asset Server` service is the only client invoking this endpoint. It calls the `GetLastNBlocks` method on the `Blockchain Service` to retrieve the last N blocks from the blockchain.

### 2.5. Checking if a Block Exists in the Blockchain

![blockchain_check_exists.svg](img%2Fplantuml%2Fblockchain%2Fblockchain_check_exists.svg)

Explanation of the sequence:

1. **Client Request:**
    - The `Client` initiates a call to the `GetBlockExists` method on the `Blockchain Service`, providing the context and request (which includes the block hash).

2. **Retrieve Block Existence from Store:**
    - The `Blockchain Service` processes the request by first converting the provided hash in the request to a `chainhash.Hash` object.
    - It then queries the `Store` to check if the block exists, using the adjusted context (`ctx1`) and the block hash.

3. **Handle Store Response:**
    - If there's an error in checking the existence of the block, the service returns the error to the client.
    - If the existence check is successful, the service prepares the response.

4. **Prepare and Return Response:**
    - The `Blockchain Service` creates a `GetBlockExistsResponse`, indicating whether the block exists or not.
    - It then returns this response to the `Client`.

The `Block Validation` service is the only client invoking this endpoint. It calls the `GetBlockExists` method on the `Blockchain Service` to check if a block exists in the blockchain.

### 2.6. Getting the Best Block Header

![blockchain_get_best_block_header.svg](img%2Fplantuml%2Fblockchain%2Fblockchain_get_best_block_header.svg)

Explanation of the sequence:

1. **Client Request:**
    - The `Client` initiates a call to the `GetBestBlockHeader` method on the `Blockchain Service`, providing the context and an empty message.

2. **Retrieve Best Block Header from Store:**
    - The `Blockchain Service` processes the request by querying the `Store` for the best block header, using the adjusted context (`ctx1`).

3. **Handle Store Response:**
    - If there's an error in retrieving the best block header, the service returns the error to the client.
    - If the retrieval is successful, the service prepares the response.

4. **Prepare and Return Response:**
    - The `Blockchain Service` creates a `GetBlockHeaderResponse` with the block header, height, transaction count, size in bytes, and miner information.
    - It then returns this response to the `Client`.

Multiple services make use of this endpoint, including the `Block Assembly`, `P2P Server`, and `Asset Server` services, as well as the `UTXO Store`.

### 2.7. Getting the Block Headers

The methods `GetBlockHeader`, `GetBlockHeaders`, and `GetBlockHeaderIDs` in the `Blockchain` service provide different ways to retrieve information about blocks in the blockchain.

![blockchain_get_block_header.svg](img%2Fplantuml%2Fblockchain%2Fblockchain_get_block_header.svg)

1. **GetBlockHeader:**
    - **Purpose:** Retrieves a single block header.
    - **Process:**
        - It takes a `GetBlockHeaderRequest` containing the hash of the desired block.
        - Converts the hash into a `chainhash.Hash` object.
        - Calls `GetBlockHeader` on the store, providing the context and the hash, to fetch the block header and associated metadata.
        - If successful, it creates and returns a `GetBlockHeaderResponse` containing the block header's byte representation and metadata like height, transaction count, size, and miner.

2. **GetBlockHeaders:**
    - **Purpose:** Fetches multiple block headers starting from a given hash.
    - **Process:**
        - Accepts a `GetBlockHeadersRequest` with the starting block hash and the number of headers to retrieve.
        - Converts the starting hash into a `chainhash.Hash` object.
        - Calls `GetBlockHeaders` on the store to obtain a list of block headers and their heights.
        - Assembles the headers into a byte array and returns them in a `GetBlockHeadersResponse`.

3. **GetBlockHeaderIDs:**
    - **Purpose:** Retrieves the IDs (hashes) of a range of block headers.
    - **Process:**
        - Receives a `GetBlockHeadersRequest` similar to `GetBlockHeaders`.
        - Converts the start hash to a `chainhash.Hash` object.
        - Uses the store's `GetBlockHeaderIDs` method to fetch the IDs of the requested block headers.
        - Returns the IDs in a `GetBlockHeaderIDsResponse`.

Each of these methods serves a specific need:
- `GetBlockHeader` is for fetching detailed information about a single block.
- `GetBlockHeaders` is useful for getting information about a sequence of blocks.
- `GetBlockHeaderIDs` provides a lighter way to retrieve just the IDs of a range of block headers without the additional metadata.

Multiple services make use of these endpoints, including the `Block Assembly`, `Block Validation`, and `Asset Server` services.


### 2.8. Invalidating a Block

![blockchain_invalidate_block.svg](img%2Fplantuml%2Fblockchain%2Fblockchain_invalidate_block.svg)

1. The `Block Assembly` sends an `InvalidateBlock` request to the `Blockchain Service`.
2. The `Blockchain Service` processes the request and calls the `InvalidateBlock` method on the `Store`, passing the block hash.
3. The `Store` performs the invalidation operation and returns the result (success or error) back to the `Blockchain Service`.
4. Finally, the `Blockchain Service` returns a response to the `Client`, which is either an empty response (indicating success) or an error message.

### 2.9. Subscribing to Blockchain Events

The Blockchain service provides a subscription mechanism for clients to receive notifications about blockchain events.

![blockchain_subscribe.svg](img%2Fplantuml%2Fblockchain%2Fblockchain_subscribe.svg)

In this diagram, the sequence of operations is as follows:
1. The `Client` sends a `Subscribe` request to the `Blockchain Client`.
2. The `Blockchain Server` receives the subscription request and adds it to the `Subscription Store` (a map of subscriber channels).
3. The server then enters a loop where it waits for either the client's context to be done (indicating disconnection) or the subscription to end.
4. If the client's context is done, the server logs the disconnection and breaks out of the loop. If the subscription ends, the loop is also exited.
5. The server then sends back a response to the client, indicating that the subscription has been established or ended.
6. On the client side, after establishing the subscription, it manages the subscription by continuously receiving stream notifications from the server and processing them as they arrive.

Multiple services make use of the subscription service, including the `Block Assembly`, `P2P`, and `Asset Server` services, and `UTXO` store. To know more, check the documentation of those services.

### 2.10. Triggering a Subscription Notification

There are two distinct paths for sending notifications, notifications originating from the `Blockchain Server` and notifications originating from a `Blockchain Client` gRPC client.

![blockchain_send_notifications.svg](img%2Fplantuml%2Fblockchain%2Fblockchain_send_notifications.svg)

1. **Path 1: Notification Originating from Blockchain Server**
   - The `Blockchain Server` processes an `AddBlock` call.
   - Inside this method, it creates a notification of type `MiningOn` or `Block`.
   - The server then calls its own `SendNotification` method to disseminate this notification.
   - The `Subscription Store` is queried to send the notification to all relevant subscribers.

2. **Path 2: Notification from Block Assembly Through Blockchain Client**
   - The `Block Assembly` component requests the `Blockchain Client` to send a notification, of type `model.NotificationType_Subtree`.
   - The `Blockchain Client` then communicates with the `Blockchain Server` to invoke the `SendNotification` method.
   - Similar to Path 1, the server uses the `Subscription Store` to distribute the notification to all subscribers.

In both scenarios, the mechanism for reaching the subscribers through the `Subscription Store` remains consistent.



## 3. gRPC Protobuf Definitions

The Blockchain Service uses gRPC for communication between nodes. The protobuf definitions used for defining the service methods and message formats can be seen [here](protobuf_docs/blockchainProto.md).


## 4. Data Model

### 4.1. Blocks

Each block is an abstraction which is a container of a group of subtrees. A block contains a variable number of subtrees, a coinbase transaction, and a header, called a block header, which includes the block ID of the previous block, effectively creating a chain.

| Field       | Type                  | Description                                                 |
|-------------|-----------------------|-------------------------------------------------------------|
| Header      | *BlockHeader          | The Block Header                                            |
| CoinbaseTx  | *bt.Tx                | The coinbase transaction.                                   |
| Subtrees    | []*chainhash.Hash     | An array of hashes, representing the subtrees of the block. |

This table provides an overview of each field in the `Block` struct, including the data type and a brief description of its purpose or contents.

More information on the block structure and purpose can be found in the [Architecture Documentation](docs/architecture/architecture.md).

### 4.2. Block - Database Data Structure

The blockchain database stores the block header, coinbase TX, and block merkle root. The following is the structure of the `blocks` data:

| Field          | Type              | Constraints                           | Description                                          |
|----------------|-------------------|---------------------------------------|------------------------------------------------------|
| id             | BIGSERIAL         | PRIMARY KEY                           | Unique identifier for each block.                    |
| parent_id      | BIGSERIAL         | REFERENCES blocks(id)                 | Identifier of the parent block.                      |
| version        | INTEGER           | NOT NULL                              | Version of the block.                                |
| hash           | BYTEA             | NOT NULL                              | Hash of the block.                                   |
| previous_hash  | BYTEA             | NOT NULL                              | Hash of the previous block.                          |
| merkle_root    | BYTEA             | NOT NULL                              | Merkle root of the block.                            |
| block_time     | BIGINT            | NOT NULL                              | Timestamp of when the block was created.             |
| n_bits         | BYTEA             | NOT NULL                              | Compact form of the block's target difficulty.       |
| nonce          | BIGINT            | NOT NULL                              | Nonce used during the mining process.                |
| height         | BIGINT            | NOT NULL                              | Height of the block in the blockchain.               |
| chain_work     | BYTEA             | NOT NULL                              | Cumulative proof of work of the blockchain up to this block. |
| tx_count       | BIGINT            | NOT NULL                              | Number of transactions in the block.                 |
| size_in_bytes  | BIGINT            | NOT NULL                              | Size of the block in bytes.                          |
| subtree_count  | BIGINT            | NOT NULL                              | Number of subtrees in the block.                     |
| subtrees       | BYTEA             | NOT NULL                              | Serialized data of the subtrees.                     |
| coinbase_tx    | BYTEA             | NOT NULL                              | Serialized data of the coinbase transaction.         |
| invalid        | BOOLEAN           | NOT NULL DEFAULT FALSE                | Flag to mark the block as valid or invalid.          |
| peer_id        | VARCHAR(64)       | NOT NULL                              | Identifier of the peer that provided the block.      |
| inserted_at    | TIMESTAMPTZ       | NOT NULL DEFAULT CURRENT_TIMESTAMP   | Timestamp of when the block was inserted in the database. |

The table structure is designed to store comprehensive information about each block in the blockchain, including its relationships with other blocks, its contents, and metadata.


## 5. Technology


1. **PostgreSQL Database:**
  - The primary store technology for the blockchain service.
  - Used for persisting blockchain data such as blocks, block headers, and state information.
  - SQL scripts and functions (`/stores/blockchain/sql`) facilitate querying and manipulating blockchain data within the PostgreSQL database.

2. **Go Programming Language:**
  - The service is implemented in Go (Golang).

3. **gRPC and Protocol Buffers:**
  - The service uses gRPC for inter-service communication.
  - Protocol Buffers (`.proto` files) are used for defining the service API and data structures, ensuring efficient and strongly-typed data exchange.

4. **gocore Library:**
  - Utilized for managing application configurations and statistics gathering.

5. **Model Layer (in `/model`):**
  - Represents the data structures and business logic related to blockchain operations.
  - Contains definitions for blocks and other blockchain components.

6. **Prometheus for Metrics:**
  - Client in `metrics.go`.
  - Used for monitoring the performance and health of the service.


## 6. Directory Structure and Main Files

The Blockchain service is located in the `./services/blockchain` directory. The following is the directory structure of the service:

```
services/blockchain
├── Client.go
    - Implements the client-side logic for interacting with the Blockchain service.
├── Interface.go
    - Defines the interface for the Blockchain store, outlining required methods for implementation.
├── LocalClient.go
    - Provides a local client implementation for the Blockchain service, for internal or in-process use.
├── Server.go
    - Contains server-side logic for the Blockchain service, handling requests and processing blockchain operations.
├── blockchain_api
    │   ├── blockchain_api.pb.go
        - Auto-generated Go bindings from the `.proto` file, used for implementing the Blockchain service API.
    │   ├── blockchain_api.proto
        - The Protocol Buffers definition file for the Blockchain service API.
    │   ├── blockchain_api_extra.go
        - Supplemental code extending or enhancing the auto-generated API code.
    │   └── blockchain_api_grpc.pb.go
        - Auto-generated gRPC bindings from the `.proto` file, specifically for gRPC communication.
├── metrics.go
    - Manages and implements functionality related to operational metrics of the Blockchain service.
└── work
    ├── work.go
        - Used to compute the cumulative chain work.
    └── work_test.go
        - Contains unit tests for the `work.go` file.
```

Further to this, the store part of the service is kept under `stores/blockchain`. The following is the directory structure of the store:


```
/stores/blockchain
├── Interface.go
    - Defines the interface for blockchain storage, outlining the methods for blockchain data manipulation and retrieval.
├── new.go
    - Contains the constructor or factory methods for creating new instances of the blockchain store.
└── sql
    ├── GetBestBlockHeader.go
        - Implements the retrieval of the best block header (the latest valid block header) from the blockchain database.
    ├── GetBestBlockHeader_test.go
        - Contains tests for the `GetBestBlockHeader.go` functionality.
    ├── GetBlock.go
        - Provides functionality to retrieve a full block from the blockchain database using its hash.
    ├── GetBlockExists.go
        - Contains logic to check if a specific block exists in the blockchain database.
    ├── GetBlockHeader.go
        - Implements the retrieval of a specific block header from the blockchain database.
    ├── GetBlockHeaderIDs.go
        - Retrieves a list of block header IDs from the blockchain database.
    ├── GetBlockHeaders.go
        - Provides functionality to retrieve multiple block headers from the blockchain database.
    ├── GetBlockHeaders_test.go
        - Contains tests for the `GetBlockHeaders.go` functionality.
    ├── GetBlockHeight.go
        - Implements the retrieval of a block's height from the blockchain database.
    ├── GetBlockHeight_test.go
        - Contains tests for the `GetBlockHeight.go` functionality.
    ├── GetBlock_test.go
        - Contains tests for the `GetBlock.go` functionality.
    ├── GetHeader.go
        - Provides functionality to retrieve a block header from the blockchain database.
    ├── GetHeader_test.go
        - Contains tests for the `GetHeader.go` functionality.
    ├── GetLastNBlocks.go
        - Implements retrieval of the last N blocks from the blockchain database.
    ├── InvalidateBlock.go
        - Provides functionality to mark a block as invalid in the blockchain database.
    ├── State.go
        - Manages the state-related data in the blockchain database.
    ├── State_test.go
        - Contains tests for the `State.go` functionality.
    ├── StoreBlock.go
        - Implements storing a block in the blockchain database.
    ├── StoreBlock_test.go
        - Contains tests for the `StoreBlock.go` functionality.
    ├── sql.go
        - Contains common SQL-related operations or utilities used by other SQL files in this directory.
    └── sql_test.go
        - Contains tests for the `sql.go` file.
```



## 7. How to run

To run the Blockchain Service locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -Blockchain=1
```

Please refer to the [Locally Running Services Documentation](../locallyRunningServices.md) document for more information on running the Blockchain Service locally.


## 8. Configuration options (settings flags)

This service uses several `gocore` configuration settings. Here's a list of these settings:

### Blockchain Service Configuration
- **Blockchain Store URL (`blockchain_store`)**: The URL for connecting to the blockchain data store. Essential for the service's ability to access and store block data.
- **gRPC Listen Address (`blockchain_grpcListenAddress`)**: Specifies the address and port the blockchain service's gRPC server listens on, enabling RPC calls for blockchain operations.
- **Kafka Brokers URL (`block_kafkaBrokers`)**: Configuration for connecting to Kafka brokers, used for publishing blockchain events and notifications.
- **Difficulty Adjustment Window (`difficulty_adjustment_window`)**: Defines the number of blocks considered for calculating difficulty adjustments, impacting how the network responds to changes in block production rates.
- **Difficulty Adjustment Flag (`difficulty_adjustment`)**: Enables or disables dynamic difficulty adjustments, allowing for a static difficulty for networks that do not require frequent adjustments.
- **Proof of Work Limit (`difficulty_pow_limit`)**: Sets the upper limit for the proof of work calculations, ensuring that difficulty adjustments do not make the mining process infeasibly hard.
- **Initial Difficulty (`mining_n_bits`)**: The starting difficulty target for mining, relevant for network startups or when special difficulty rules apply.

### Operational Settings
- **Max Retries (`blockchain_maxRetries`)**: The maximum number of attempts to connect to the blockchain service, ensuring resilience against temporary connectivity issues.
- **Retry Sleep Duration (`blockchain_retrySleep`)**: The wait time between retry attempts for connecting to the blockchain service, providing a back-off mechanism to reduce load during outages.
- **Initial Blocks Count (`mine_initial_blocks_count`)**: Specifies the number of blocks that should be mined at the initial difficulty level, useful for network startups or testing environments.
- **Target Time per Block (`difficulty_target_time_per_block`)**: The desired time interval between blocks, guiding the difficulty adjustment process to maintain a steady block production rate.
