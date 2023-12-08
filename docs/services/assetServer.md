# 🗂️ Asset Server

## Index

1. [Description](#1-description)
2. [Architecture](#2-architecture)
3. [Data Model ](#3-data-model-)
- [3.1. Blocks](#31-blocks)
- [3.2. Block Headers](#32-block-headers)
- [3.3. Subtrees](#33-subtrees)
- [3.4. Txs](#34-txs)
- [3.5. UTXOs](#35-utxos)
- [3.6. TX Meta](#36-tx-meta)
4. [Use Cases](#4-use-cases)
- [4.1. gRPC](#41-grpc)
- [4.1.1. getBlock(), getBestBlockHeader(), getBlockHeaders() ](#411-getblock-getbestblockheader-getblockheaders-)
- [4.1.2. Subtree Get()](#412-subtree-get)
- [4.1.3. Subtree Set() and SetTTL()](#413-subtree-set-and-setttl)
- [4.1.5. Subscribe to notifications ](#415-subscribe-to-notifications-)
- [4.2. HTTP and Websockets    ](#42-http-and-websockets----)
- [4.2.1. getTransaction() and getTransactions()](#421-gettransaction-and-gettransactions)
- [4.2.2. GetTransactionMeta() ](#422-gettransactionmeta-)
- [4.2.3. GetSubtree() ](#423-getsubtree-)
- [4.2.4. GetBlockHeaders(), GetBlockHeader() and GetBestBlockHeader()](#424-getblockheaders-getblockheader-and-getbestblockheader)
- [4.2.5. GetBlock() and GetLastNBlocks()](#425-getblock-and-getlastnblocks)
- [4.2.6. GetUTXO() and GetUTXOsByTXID()](#426-getutxo-and-getutxosbytxid)
- [4.2.7. Websocket Subscriptions](#427-websocket-subscriptions)
5. [Technology ](#5-technology-)
6. [Directory Structure and Main Files](#6-directory-structure-and-main-files)
7. [How to run](#7-how-to-run)
- [7.1. How to run](#71-how-to-run)
- [7.2  Configuration options (settings flags)](#72--configuration-options-settings-flags)

## 1. Description

The Asset Service acts as an interface ("Front" or "Facade") to various data stores. It deals with several key data elements:

- **Transactions (TX)**.


- **SubTrees**.


- **Blocks and Block Headers**.


- **Unspent Transaction Outputs (UTXO)**.


- **Metadata for a Transaction (TXMeta)**.


The server uses both HTTP and gRPC as communication protocols:

- **HTTP**: A ubiquitous protocol that allows the server to be accessible from the web, enabling other nodes or clients to interact with the server using standard web requests.


- **gRPC**: Allowing for efficient communication between nodes, particularly suited for microservices communication in the UBSV distributed network.

The server being externally accessible implies that it is designed to communicate with other nodes and external clients across the network, to share blockchain data or synchronize states.

The various micro-services typically write directly to the data stores, but the asset service fronts them as a common interface.

Finally, the Asset Service also offers a WebSocket interface, allowing clients to receive real-time notifications when new subtrees and blocks are added to the blockchain.

## 2. Architecture

![Asset_Server_System_Context_Diagram.png](img%2FAsset_Server_System_Context_Diagram.png)

The Asset Server provides data to other Teranode components over gRPC. It also provides data to external clients over HTTP / Websockets, such as the UBSV UI Dashboard.

All data is retrieved from other Teranode services / stores.

Here we can see the Asset Server's relationship with other Teranode components in more detail:

![Asset_Server_System_Container_Diagram.png](img%2FAsset_Server_System_Container_Diagram.png)


The Asset Server is composed of the following components:

![Asset_Server_System_Component_Diagram.png](img%2FAsset_Server_System_Component_Diagram.png)


* **UTXO Store**: Provides UTXO data to the Asset Server.
* **TX Meta Store**: Provides TX Meta data to the Asset Server.
* **Blob Store**: Provides Subtree and Extended TX data to the Asset Server, referred here as Subtree Store and TX Store.
* **Blockchain Server**: Provides blockchain data (blocks and block headers) to the Asset Server.


## 3. Data Model

The following data types are provided by the Asset Server:

### 3.1. Blocks

Each block is an abstraction which is a container of a group of subtrees. A block contains a variable number of subtrees, a coinbase transaction, and a header, called a block header, which includes the block ID of the previous block, effectively creating a chain.

| Field       | Type                  | Description                                                 |
|-------------|-----------------------|-------------------------------------------------------------|
| Header      | *BlockHeader          | The Block Header                                            |
| CoinbaseTx  | *bt.Tx                | The coinbase transaction.                                   |
| Subtrees    | []*chainhash.Hash     | An array of hashes, representing the subtrees of the block. |

This table provides an overview of each field in the `Block` struct, including the data type and a brief description of its purpose or contents.

More information on the block structure and purpose can be found in the [Architecture Documentation](docs/architecture/architecture.md).

### 3.2. Block Headers


The block header is a data structure that contains metadata about a block. It is used to connect blocks together in a blockchain. The block header is a structure that is hashed as part of the proof-of-work algorithm for mining. It contains the following fields:

| Field           | Type               | Description                                                                                                                            |
|-----------------|--------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| Version         | uint32             | Version of the block, different from the protocol version. Represented as 4 bytes in little endian when built into block header bytes.
| HashPrevBlock   | *chainhash.Hash    | Reference to the hash of the previous block header in the blockchain.                                                                  |
| HashMerkleRoot  | *chainhash.Hash    | Reference to the Merkle tree hash of all subtrees in the block.                                                                        |
| Timestamp       | uint32             | The time when the block was created, in Unix time. Represented as 4 bytes in little endian when built into block header bytes.         |
| Bits            | NBit               | Difficulty target for the block. Represented as a target threshold in little endian, the format used in a Bitcoin block.               |
| Nonce           | uint32             | Nonce used in generating the block. Represented as 4 bytes in little endian when built into block header bytes.                        |



### 3.3. Subtrees

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

### 3.4. Txs

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


### 3.5. UTXOs

The UTXO are kept with the following structure.

| Field     | Description                                                   |
|-----------|---------------------------------------------------------------|
| hash      | The hash of the UTXO for identification purposes              |
| lock_time | The block number or timestamp at which this UTXO can be spent |
| tx_id     | The transaction ID where this UTXO was spent                  |


More information on the UTXO structure and purpose can be found in the [Architecture Documentation](docs/architecture/architecture.md) and the [UTXO Store Documentation](../stores/utxo.md).

### 3.6. TX Meta


| Field Name  | Description                                                     | Data Type             |
|-------------|-----------------------------------------------------------------|-----------------------|
| Hash        | Unique identifier for the transaction.                          | String/Hexadecimal    |
| Fee         | The fee associated with the transaction.                        | Decimal       |
| Size in Bytes | The size of the transaction in bytes.                        | Integer               |
| Parents     | List of hashes representing the parent transactions.            | Array of Strings/Hexadecimals |
| Blocks      | List of hashes of the blocks that include this transaction.     | Array of Strings/Hexadecimals |
| LockTime    | The earliest time or block number that this transaction can be included in the blockchain. | Integer/Timestamp or Block Number |


More information on the TX Meta structure and purpose can be found in the [Architecture Documentation](docs/architecture/architecture.md) and the [TX Meta Store Documentation](../stores/txmeta.md).


## 4. Use Cases

### 4.1. gRPC

The Asset Service exposes the following gRPC methods:

### 4.1.1. getBlock(), getBestBlockHeader(), getBlockHeaders()

![asset_server_grpc_get_block.svg](img%2Fplantuml%2Fassetserver%2Fasset_server_grpc_get_block.svg)

### 4.1.2. Subtree Get()

![asset_server_grpc_get_subtree.svg](img%2Fplantuml%2Fassetserver%2Fasset_server_grpc_get_subtree.svg)

### 4.1.3. Subtree Set() and SetTTL()

The Asset Server also permits to store subtrees and to update their retention TTL in the Subtree (Blob) Store.

![asset_server_grpc_set_subtree.svg](img%2Fplantuml%2Fassetserver%2Fasset_server_grpc_set_subtree.svg)

- The **New Subtree Scenario** shows the process of setting a new subtree in the Blob Subtree Store via the Block Assembly Server, Remote TTL, Asset Client, and Asset Server.

- The **Clear Subtree TTL Scenario** illustrates the sequence of operations involved in submitting a mining solution, which includes removing the TTL (Time To Live) for subtrees associated with a block. This process involves iterating over each subtree in the block and setting the TTL through the Remote TTL, Asset Client, Asset Server, and the Blob Subtree Store.

### 4.1.5. Subscribe to notifications

![asset_server_grpc_subscribe_notifications.svg](img%2Fplantuml%2Fassetserver%2Fasset_server_grpc_subscribe_notifications.svg)

1. The Coinbase service initiates a subscription through the Asset Client. We expect to receive **Block, MiningOn, and Subtree** notifications.
   * In all cases, a (block or subtree) hash is sent out. The Coinbase service can then request the full block or subtree from the Asset Server.
   * In the current implementation, there is no difference between Block and MiningOn messages, as they are both hashed blocks.
2. The Asset Server tracks the subscriber in the Subscriber Database.
3. The Asset Server subscribes to the Blockchain Client, which sends back notifications for Block, MiningOn, and Subtree messages.
4. Independently, the Blockchain Server adds a block and sends notifications (Block and MiningOn) to the Asset Server, which then forwards these to the Coinbase service.
5. Concurrently, the Block Assembly Server, upon initialization and adding a new subtree, sends a Subtree notification to the Asset Server (via the Blockchain subscription), which again forwards it to the Coinbase service.


### 4.2. HTTP and Websockets

The Asset Service exposes the following HTTP and Websocket methods:

### 4.2.1. getTransaction() and getTransactions()

![asset_server_http_get_transaction.svg](img%2Fplantuml%2Fassetserver%2Fasset_server_http_get_transaction.svg)

### 4.2.2. GetTransactionMeta()

![asset_server_http_get_transaction_meta.svg](img%2Fplantuml%2Fassetserver%2Fasset_server_http_get_transaction_meta.svg)

### 4.2.3. GetSubtree()

![asset_server_http_get_subtree.svg](img%2Fplantuml%2Fassetserver%2Fasset_server_http_get_subtree.svg)

### 4.2.4. GetBlockHeaders(), GetBlockHeader() and GetBestBlockHeader()

![asset_server_http_get_block_header.svg](img%2Fplantuml%2Fassetserver%2Fasset_server_http_get_block_header.svg)

### 4.2.5. GetBlock() and GetLastNBlocks()

![asset_server_http_get_block.svg](img%2Fplantuml%2Fassetserver%2Fasset_server_http_get_block.svg)

### 4.2.6. GetUTXO() and GetUTXOsByTXID()

![asset_server_http_get_utxo.svg](img%2Fplantuml%2Fassetserver%2Fasset_server_http_get_utxo.svg)

* For specific UTXO by hash requests (/utxo/:hash), the HTTP Server requests UTXO data from the UtxoStore using a hash.


* For getting UTXOs by a transaction ID (/utxos/:hash/json), the HTTP Server requests transaction meta data from the TxMetaStore using a transaction hash. Then for each output in the transaction, it queries the UtxoStore to get UTXO data for the corresponding output hash.

### 4.2.7. Websocket Subscriptions

The `HandleWebSocket(c)` (`services/asset/http_impl/HandleWebsocket.go`) function sets up a WebSocket server for handling real-time notifications. It involves managing client connections, broadcasting messages (including periodic pings), and handling client disconnections.

1. **Initialization**: The function initializes three channels: `newClientCh` for new client connections, `deadClientCh` for client disconnections, and `notificationCh` for receiving notifications.

2. **WebSocket Connection Handling**:
    - When a new WebSocket client connects, it's added to the `clientChannels` map for message broadcasting.
    - When a client disconnects, it is removed from the `clientChannels` map.

3. **Notification Broadcasting**:
    - The function listens for incoming notifications and pings on separate goroutines. Notice the noti
    - When a new notification is received, it is marshaled into JSON and sent to all connected WebSocket clients.
    - A ping message is sent to all clients every 30 seconds to maintain the WebSocket connection.

4. **WebSocket Data Transmission**:
    - The WebSocket server sends data to connected clients via the `clientChannels`.
    - If an error occurs during message sending (e.g., if a client disconnects), the client is marked as dead and removed from active client channels.

![asset_webserver_websocket.svg](img%2Fplantuml%2Fassetserver%2Fasset_webserver_websocket.svg)

Notice that the notifications are the same notifications sent by the gRPC subscriber (see [Subscribe to notifications](#415-subscribe-to-notifications-)) - i.e. `Subtree`, `Block`, `MiningOn`.

## 5. Technology

Key technologies involved:

1. **Go Programming Language (Golang)**:
    - A statically typed, compiled language known for its simplicity and efficiency, especially in concurrent operations and networked services.
    - The primary language used for implementing the service's logic.

2. **gRPC (Google Remote Procedure Call)**:
    - A high-performance, open-source framework developed by Google.
    - Used for inter-service communication, enabling the server to efficiently communicate with connected clients or nodes.
    - Supports features like streaming requests and responses and robust error handling.

3. **Protobuf (Protocol Buffers)**:
    - A language-neutral, platform-neutral, extensible mechanism for serializing structured data, developed by Google.
    - Used in conjunction with gRPC for defining service methods and message formats.

4. **HTTP/HTTPS Protocols**:
    - HTTP for transferring data over the web. HTTPS adds a layer of security with SSL/TLS encryption.
    - Used for communication between clients and the server, and for serving web pages or APIs.

5. **Echo Web Framework**:
    - A high-performance, extensible, minimalist Go web framework.
    - Used for handling HTTP requests and routing, including upgrading HTTP connections to WebSocket connections.
    - Library: github.com/labstack/echo

6. **WebSocket Protocol**:
    - A TCP-based protocol that provides full-duplex communication channels over a single connection.
    - Used for real-time data transfer between clients and the server, particularly useful for continuously updating the state of the network to connected clients.
    - Library: github.com/gorilla/websocket

8. **JSON (JavaScript Object Notation)**:
    - A lightweight data-interchange format, easy for humans to read and write, and easy for machines to parse and generate.
    - Used for structuring data sent to and from clients, especially in contexts where WebSocket or HTTP is used.


## 6. Directory Structure and Main Files

```
./services/asset
├── Client.go                  # gRPC Client functions for the Asset Service.
├── Interface.go               # Defines the interface for an asset peer.
├── Peer.go                    # Defines a Peer and manages peer-related functionalities.
├── Server.go                  # Server logic for the Asset Service.
├── asset_api                  # API definitions and generated files.
├── grpc_impl                  # Implementation of the service using gRPC, used by the Server.go.
├── http_impl                  # Implementation of the service using HTTP.
│   ├── GetBestBlockHeader.go  # Logic to retrieve the best block header.
│   ├── GetBlock.go            # Logic to retrieve a specific block.
│   ├── GetBlockHeader.go      # Logic to retrieve a block header.
│   ├── GetBlockHeaders.go     # Logic to retrieve multiple block headers.
│   ├── GetLastNBlocks.go      # Logic to retrieve the last N blocks.
│   ├── GetSubtree.go          # Logic to retrieve a subtree.
│   ├── GetTransaction.go      # Logic to retrieve a specific transaction.
│   ├── GetTransactionMeta.go  # Logic to retrieve transaction metadata.
│   ├── GetTransactions.go     # Logic to retrieve multiple transactions.
│   ├── GetUTXO.go             # Logic to retrieve UTXO data.
│   ├── GetUTXOsByTXID.go      # Logic to retrieve UTXOs by a transaction ID.
│   ├── HandleWebsocket.go     # Manages WebSocket connections.
│   ├── Readmode.go            # Manages read-only mode settings.
│   ├── blockHeaderResponse.go # Formats block header responses.
│   ├── http.go                # Core HTTP implementation.
│   ├── metrics.go             # HTTP-specific metrics.
│   └── sendError.go           # Utility for sending error responses.
└── repository                 # Repository layer managing data interactions.
    └── repository.go          # Core repository implementation.
```


## 7. How to run

### 7.1. How to run

To run the Asset Server locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -Asset=1
```

Please refer to the [Locally Running Services Documentation](../locallyRunningServices.md) document for more information on running the Bootstrap Service locally.


### 7.2  Configuration options (settings flags)

1. **General Port Configuration**
  - `ASSET_GRPC_PORT`: Defines the port for gRPC communication.
    - Example: `ASSET_GRPC_PORT=8091`
  - `ASSET_HTTP_PORT`: Specifies the port for HTTP communication.
    - Example: `ASSET_HTTP_PORT=8090`

2. **gRPC Configuration**
  - `asset_grpcListenAddress.${YOUR_USERNAME}`: Address for the Asset Service to listen for gRPC requests.
    - Example: `asset_grpcListenAddress.johndoe=:8091` (Listens on port 8091 for user "johndoe")

3. **TX Meta Store Configuration**
  - `txmeta_store_asset-service.${YOUR_USERNAME}`: Connection string for the TX Meta Store used by the Asset Service.
    - Example: `txmeta_store_asset-service.johndoe=aerospike://ubsv-store-eu-0.eu.eu-west-1.ubsv.internal:3000/ubsv-store?ConnectionQueueSize=5&LimitConnectionsToQueueSize=false&MinConnectionsPerNode=5&expiration=7200`

4. **Asset Service Startup Configuration**
  - `startAsset.${YOUR_USERNAME}`: Flag to start the Asset Service.
    - Example: `startAsset.johndoe=true`

5. **Asset Service Network Configuration**
  - `asset_grpcListenAddress.${YOUR_USERNAME}`: Configures the listening address for the Asset Service's gRPC server.
    - Example: `asset_grpcListenAddress.johndoe=localhost:8091`
  - `asset_grpcAddress.${YOUR_USERNAME}`: Address for accessing the Asset Service via gRPC.
    - Example: `asset_grpcAddress.johndoe=localhost:8091`
  - `asset_httpListenAddress.${YOUR_USERNAME}`: Configures the listening address for the Asset Service's HTTP server.
    - Example: `asset_httpListenAddress.johndoe=localhost:8090`
  - `asset_http_port.${YOUR_USERNAME}`: Standard HTTP port for the Asset Service.
    - Example: `asset_http_port.johndoe=80`
  - `asset_https_port.${YOUR_USERNAME}`: Standard HTTPS port for the Asset Service.
    - Example: `asset_https_port.johndoe=443`
  - `asset_httpAddress.${YOUR_USERNAME}`: Full HTTP address to access the Asset Service.
    - Example: `asset_httpAddress.johndoe=http://localhost:8090`

6. **Miscellaneous Settings**
  - `asset_clientName.${YOUR_USERNAME}`: Name of the client using the Asset Service.
    - Example: `asset_clientName.johndoe=Not specified`
  - `coinbase_assetGrpcAddress.${YOUR_USERNAME}`: gRPC address for the Coinbase service to connect to the Asset Service.
    - Example: `coinbase_assetGrpcAddress.johndoe=localhost:8091`
