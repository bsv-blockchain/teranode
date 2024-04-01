#  🔗 P2P Service

## Index

1. [Introduction](#1-introduction)
- [1.1. Summary](#11-summary)
- [1.2. Data Transformation for Compatibility](#12-data-transformation-for-compatibility)
- [1.3. Phased Migration Towards Teranode](#13-phased-migration-towards-teranode)
2. [Architecture](#2-architecture)
3. [Data Model](#3-data-model)
4. [Functionality](#4-functionality)
- [4.1. BSV to Teranode Communication](#41-bsv-to-teranode-communication)
   - [4.1.1. Receiving Inventory Notifications](#411-receiving-inventory-notifications)
   - [4.1.2. Processing New Blocks](#412-processing-new-blocks)
5. [Technology](#5-technology)
6. [Directory Structure and Main Files](#6-directory-structure-and-main-files)
7. [How to run](#7-how-to-run)

## 1. Introduction

### 1.1. Summary

The P2P Legacy Service bridges the gap between the traditional BSV nodes and the advanced Teranode-BSV nodes, ensuring seamless communication and data translation between the two. By facilitating this integration, the P2P Legacy Service enables both historical BSV nodes and modern Teranode nodes to operate concurrently, supporting a smooth and gradual transition to the Teranode infrastructure.

The core functionality of the P2P Legacy Service revolves around managing and translating communications between historical BSV nodes and Teranode-BSV nodes. This includes:

- **Receiving Blocks and Transactions:** The service accepts blocks and transactions from legacy BSV nodes, ensuring that these can be efficiently propagated to Teranode nodes within the network.


- **Disseminating Newly Mined Blocks:** It also sends newly mined blocks from Teranode nodes back to the legacy nodes, maintaining the continuity and integrity of the blockchain across different node versions.


### 1.2. Data Transformation for Compatibility

The P2P Legacy Service performs two major data transformation processes, designed to ensure full compatibility between the differing data structures of BSV and Teranode:

1. **Decorate Translations for Transaction Formats:** The service adeptly translates from / to the traditional TX format used in BSV and the Extended TX format that is prevalent in Teranode. This decoration process ensures that transactions can seamlessly transition between the two systems without loss of information or functionality.

2. **Encapsulation and Conversion of Blocks and Transactions:** Legacy blocks and their transactions are encapsulated into subtrees, aligning with the Teranode model's approach to data management. In a similar way, the service can transparently convert blocks, subtrees, and transactions from the Teranode format back into the conventional block and transaction format used by BSV. This bidirectional conversion capability allows to maintain operational continuity across the network.

### 1.3. Phased Migration Towards Teranode

The P2P Legacy Service allows for BSV and Teranode BSV nodes to operate side by side. This is intended as a temporary solution until all historical BSV nodes are phased out. As transaction volumes on the BSV network continue to grow, the demand for more scalable and feature-rich infrastructure will necessitate a complete migration to Teranode. The P2P Legacy Service is a critical enabler of this transition, ensuring that the migration can occur gradually and without disrupting the network's ongoing operations.


## 2. Architecture

The P2P Legacy Service acts as a BSV node, connecting to the BSV mainnet and receiving txs and blocks. It then processes these txs and blocks, converting them into the Teranode format and propagating them to the Teranode network.
The service also receives blocks from the Teranode network and converts them back into the BSV format for dissemination to the BSV mainnet.

![P2P_Legacy_Container_Diagram.png](img%2FP2P_Legacy_Container_Diagram.png)

As it can be seen in the diagram above, the service maintains its own block database (in historical format).

When the service receives a block message from the network, the service will propagate it to the Teranode Block Validator Service for validation and inclusion in the Teranode blockchain.

![P2P_Legacy_Component_Diagram.png](img%2FP2P_Legacy_Component_Diagram.png)

Both the Block Validation and the Subtree validation will query the P2P Legacy Service (using the same endpoints the Asset Server offers) for the block and subtree data, respectively. The P2P Legacy Service will respond with the requested data in the same format the Asset Server uses. The following endpoints are offered by the P2P Legacy Service:

```go
	e.GET("/block/:hash", tb.BlockHandler)
	e.GET("/subtree/:hash", tb.SubtreeHandler)
	e.GET("/tx/:hash", tb.TxHandler)
	e.POST("/txs", tb.TxBatchHandler())
```

In addition to the block database, the service maintains an in-memory peer database, tracking peers it receives / sends messages from /to.

## 3. Data Model

When we announce a teranode block, the format is:
* Blockheader
* Block meta data (size, tx count etc etc)
* Coinbase TX (the actual payload / bytes of the coinbase TX itself)
* A slice of subtrees.

* Each subtree has a list of txid's, and to calculate the merkle root, you replace the coinbase placeholder with the txid of the coinbase transaction and then do the merkle tree calculation as normal.  The result should match the merkle root in the block header.

## 4. Functionality

### 4.1. BSV to Teranode Communication

The overall cycle is:

![legacy_p2p_bsv_to_teranode.svg](img%2Fplantuml%2Flegacyp2p%2Flegacy_p2p_bsv_to_teranode.svg)

1) An inventory (inv) notification is received, indicating that a new block or transaction is available from a BSV node. The P2P Legacy Service processes this notification and requests the block or transaction data from the BSV node.

2) When a new block is received from the BSV node, it is transformed into a Teranode subtree and block propagation format. The block hash is then sent to the Teranode network for further processing.

3) The Teranode network processes the block hash and requests the full block data from the P2P Legacy Service.
   3.1) Upon receipt of the full block data, Teranode will notice that it contains subtrees it is not aware of, and request them from the P2P Legacy Service.
   3.2) Upon receipt of the full subtree data, the list of transactions will be known, and will be requested from the P2P Legacy Service.
   3.3) With all the information now available, the block is then added to the Teranode blockchain.

In the next sections, we will detail the steps involved.

#### 4.1.1. Receiving Inventory Notifications

In the Bitcoin network, an "inv" message is a component of the network's communication protocol, used to indicate inventory. The term "inv" stands for inventory, and these messages are utilized by nodes to inform other nodes about the existence of certain items, such as blocks and transactions, without transmitting the full data of those items. This mechanism is crucial for the efficient propagation of information across the network, helping to minimize bandwidth usage and improve the overall scalability of the system.

The primary purpose of an inv message is to advertise the availability of specific data objects, like new transactions or blocks, to peer nodes. It allows a node to broadcast to its peers that it has something of interest, which the peers might not yet have. The peers can then decide whether they need this data based on the inventory vectors provided in the inv message and request the full data using a "getdata" message if necessary.

An inv message consists of a list of inventory vectors, where each vector specifies the type and identifier of a particular item. The structure of an inventory vector is as follows:

- **Type:** This field indicates the type of item being advertised. It could represent a block, a transaction, or other data types defined in the protocol.
- **Identifier:** This is a hash that uniquely identifies the item, such as the hash of a block or a transaction.

There are several types of inventory that can be advertised using inv messages, including but not limited to:

- **Transaction:** Indicates the presence of a new transaction.
- **Block:** Signifies the availability of a new block.
- **Filtered Block:** Used in connection with Bloom filters to indicate a block that matches the filter criteria, allowing lightweight clients to download only a subset of transactions.

Inv messages are critical in the data dissemination process within the Bitcoin network.

1. **Broadcasting New Transactions:** When a node receives or creates a new transaction, it broadcasts an inv message containing the transaction's hash to its peers, signaling that this transaction is available.
2. **Announcing New Blocks:** Similarly, when a node learns about a new block (whether by mining it or receiving it from another node), it uses an inv message to announce this block to its peers.
3. **Data Request:** Upon receiving an inv message, a node can send a "getdata" message to request the full data for any items it does not already have but is interested in obtaining.

In the context of the P2P Legacy Service, the reception of inv messages is a crucial step in the communication process between BSV nodes and Teranode nodes. These messages serve as the initial trigger for data exchange, allowing the service to identify new blocks or transactions and initiate the necessary actions to retrieve and process this data.

![legacy_p2p_bsv_to_teranode_inv_message.svg](img%2Fplantuml%2Flegacyp2p%2Flegacy_p2p_bsv_to_teranode_inv_message.svg)


1. **Connection Establishment:** The P2P Legacy Overlay Service establishes a connection with the MainNet, setting the stage for data exchange.

2. **Block and Transaction Advertisement:**
    - The MainNet node sends an `OnInv` message to the Legacy Service, advertising available blocks or transactions.
    - The Legacy Service checks with its database (`db`) to determine if the advertised item (block or transaction) is already known.
    - If the item is new, the Legacy Service requests the full data using `OnGetData`.

3. **Receiving Full Data:**
    - For a new block, the MainNet responds with `OnBlock`, containing the block data. The Legacy Service processes this block and stores its details in the database.
    - For a new transaction, the MainNet responds with `OnTx`, containing the transaction data. Similarly, the Legacy Service processes and stores this information.

#### 4.1.2. Processing New Blocks

Once a new block is received by the Legacy P2P Service, it undergoes a series of transformations to align with the Teranode model. This process involves encapsulating the block into subtrees and converting the block data into a format that is compatible with the Teranode blockchain structure.

The sequence of steps involved in processing a new block is as follows:

![legacy_p2p_bsv_to_teranode_process_new_blocks.svg](img%2Fplantuml%2Flegacyp2p%2Flegacy_p2p_bsv_to_teranode_process_new_blocks.svg)

Process flow:

1. **Receiving a Block:**
    - The MainNet sends a block to the P2P Legacy Overlay Service using the `OnBlock` message, which includes the peer identifier, message metadata, and the block data in bytes.

2. **Block Processing:**
    - The Legacy Service constructs a block from the received bytes and initiates the `HandleBlock` function to process the block.

3. **Subtree Creation and Transaction Processing:**
    - A new, empty subtree is created, and a placeholder coinbase transaction is added to this subtree.
    - For each transaction in the block, the Legacy Service:
        - Converts the transaction into an Extended Transaction format.
        - Adds the Extended Transaction to the Subtree.
        - Caches the Extended Transaction in the "Tx Cache" (a short term Tx cache DB).

4. **Caching and Block Building:**
    - The entire subtree is cached in the "Subtree Cache" (a short term subtree cache DB).
    - A Teranode block is constructed using the processed block and the subtree, and this block is cached in the "Block Cache" (a short term block cache DB).

5. **Block Validation:**
    - The `BlockFound` function is called on the Block Validator (part of Teranode), passing the context, hash of the block, and a base URL for accessing the block and its components.
      - It must be noted that this base URL would typically represent a native Teranode Asset server. However, in this case, the P2P Legacy Service will handle the endpoints, providing responses in the same format as the Teranode Asset server, to allow Teranode to request data in the same way they would do it with a remote asset server.
      - Notice how the Teranode does not understand that the new blocks are being added by the P2P Legacy Service. As far as the Teranode is concerned, a request for a new block is received as if it came from an internal Teranode service. And when requesting data from the provided baseURL, it receives information as if it was a remote asset server. In all senses, the P2P Legacy Service impersonates a remote asset server for the Teranode.
    - The Block Validator requests the block using the provided URL, and the Legacy Service responds with the block data.
    - The Block Validator then instructs the Subtree Validator to check the subtree, providing the hash and base URL for the subtree.

6. **Subtree Validation:**
    - The Subtree Validator requests the subtree using the provided URL, and the Legacy Service responds with the subtree data.
    - For each transaction ID in the subtree, the Subtree Validator requests the corresponding transaction data using the base URL, and the Legacy Service provides the transaction data in response.

7. **Finalization:**
    - Upon successful validation, the Block Validator coordinates with other Teranode microservices and stores, and adds the block to the blockchain, completing the process.



## 5. Technology

The entire codebase is written in Go (Golang), a statically typed, compiled programming language designed for simplicity and efficiency.

Technology highlights:

- **Bitcoin SV (BSV):** An implementation of the Bitcoin protocol that follows the vision set out by Satoshi Nakamoto's original Bitcoin whitepaper. BSV focuses on scalability, aiming to provide high transaction throughput and enterprise-level capabilities.
- **Teranode:** A high-performance implementation of the Bitcoin protocol designed to handle a massive scale of transactions.

- **Echo Framework:** The code uses Echo, a high-performance, extensible web framework for Go, to create an HTTP server. This server exposes endpoints for block, transaction, and subtree data retrieval.

- **ExpiringMap:** An expiring map is used for caching transactions, subtrees, and blocks with a time-based eviction policy.
- **Chainhash:** A library for handling SHA-256 double hash values, commonly used as identifiers in blockchain (e.g., transaction IDs, block hashes).

- **Database Technologies** (ffldb, leveldb, sqlite): Used as block data stores. Each has its use cases:

  - **ffldb**: Optimized for blockchain data storage and retrieval. Default and recommended choice.
  - **leveldb**: A fast key-value storage library suitable for indexing blockchain data.
  - **sqlite**: An embedded SQL database engine for simpler deployment and structured data queries.

## 6. Directory Structure and Main Files


**** TODO  - clarify and wait for structure cleanup

## 7. How to run

To run the P2P Legacy Service locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -Legacy=1
```

Please refer to the [Locally Running Services Documentation](../locallyRunningServices.md) document for more information on running the P2P Legacy Service locally.
