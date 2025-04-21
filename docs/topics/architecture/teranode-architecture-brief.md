![BSVBA-Logo_FC.svg](img%2FBSVBA-Logo_FC.svg)

---

![teranode.png](img%2Fteranode.png)

---

# Teranode - Architecture Overview

## 1. Overview


The original design of the Bitcoin network imposed a constraint on block size to 1MB (1 Megabyte). This size limit inherently restricts the network to a throughput of approximately **3.3 to 7 transactions per second**. As adoption has increased, this constraint has led to bottlenecks in transaction processing, resulting in delays and increased transaction fees, highlighting the need for a scalability solution.

The Teranode Project, being developed by the BSV Association, addresses the challenges of vertical scaling by instead spreading the workload across multiple machines. This horizontal scaling approach, coupled with an unbound block size, enables network capacity to grow with increasing demand through the addition of cluster nodes, allowing for BSV scaling to be truly unbounded.

Teranode provides a robust node processing system for BSV that can consistently handle over **1 million transactions per second**, while strictly adhering to the Bitcoin whitepaper.
The node has been designed as a collection of services that work together to provide a decentralized, scalable, and secure blockchain network. The node is designed to be modular, allowing for easy integration of new services and features.

The diagram below shows the main microservices, together with their interactions, that make up Teranode.

&nbsp;
&nbsp;

![TERANODE_OVERVIEW.png](img/TERANODE_OVERVIEW.png)

&nbsp;
&nbsp;
&nbsp;
&nbsp;

## 2. Data Model and Propagation

Teranode strictly adheres to the Bitcoin Whitepaper, while introducing ground-breaking enhancements.  Let's examine the differences between the BTC data model and the Teranode data model.



### 2.1. Comparison of BTC, BSV, and Teranode BSV

| Feature | BTC | BSV (pre-Teranode) | Teranode BSV |
|---------|-----|-------------------|---------------|
| **Transactions** | Standard Bitcoin transaction model. | Standard Bitcoin transaction model with restored original op_codes. | Adopts an extended format with extra metadata, improving processing efficiency. |
| **SubTrees** | Not used. | Not used. Traditional block propagation. | A novel concept in Teranode, serving as an intermediary for holding transaction IDs and their Merkle roots. </br></br> Each subtree contains 1 million transactions. Subtrees are broadcast every second. </br></br>Broadcast frequently for faster and continuous data propagation. |
| **Blocks** | Transactions are grouped into blocks. Direct transaction data is stored in the block. Each block is linked to the previous one by a cryptographic hash, forming a secure, chronological chain. | Same as BTC, with increased block size capacity. | In the BSV blockchain, Bitcoin blocks are stored and propagated using an abstraction using subtrees of transaction IDs. This method significantly streamlines the validation process and synchronization among miners, optimizing the overall efficiency of the network. |
| **Block Size** | Originally capped at 1MB (1 Megabyte), restricting transactions per block. | Increased to 2GB, then to 4GB block size limit. | Current BSV expands to 4GB (4 Gigabytes), increasing transaction capacity. <br/><br/>Teranode removes the size limit, enabling limitless transactions per block. |
| **Processed Transactions per second** | 3.3 to 7 transactions per second. | Up to several thousand transactions per second. | Guaranteees a minimum of **1 million transactions per second** (100,000 x faster than BTC). |
| **Mempool** | Maintains a memory pool of unconfirmed transactions waiting to be included in blocks. Size limited by node memory. | Similar to BTC, but with larger capacity due to increased memory limits. | No traditional mempool. Transactions are immediately processed and organized into subtrees. Continuous validation and subtree creation replaces mempool functionality. |

&nbsp;

### 2.2. Advantages of the Teranode Model

Enhances validation speed and scalability. The continuous broadcasting of subtrees allows for more consistent and efficient data validation and network behavior.

### 2.3. Network Behavior

The Teranode network behaviour is characterized by its proactive approach, with nodes broadcasting and validating subtrees regularly, leading to expedited block validation and higher transaction throughput.

## 3. Node Workflow

- **Transaction Submission**: Managed via a dedicated Submission Service, ensuring efficient entry of transactions into the network.
- **Transaction Validation**: Conducted by the TX Validation Service, which rigorously checks transaction compliance with network rules.
- **Subtree Assembly**: Involves organizing validated transactions into subtrees, a critical step for efficient block assembly.
- **Block Assembly**: Focuses on compiling subtrees into block templates, which are then used for mining.
- **Block Validation**: Ensures each block adheres to the network's consensus rules, maintaining blockchain integrity.

## 4. Services

- **Transaction Propagation Service**: Handles the receipt and forwarding of transactions for validation and distribution to other nodes.
- **Transaction Validator**: Validates each transaction against network rules and updates their status in the system.
- **Block Assembly Service**: Responsible for creating subtrees and preparing block templates for mining.
- **Subtree and Block Validator**: Plays a pivotal role in confirming the integrity and validity of both subtrees and blocks.
- **Blockchain Service**: Manages the addition of new blocks to the blockchain and maintains the blockchain database.
- **Asset Service**: Serves as a gateway to various data elements, facilitating interactions with transactions, UTXOs, etc.
- **Bootstrap**: Assists new nodes in integrating into the Teranode network by discovering peers.
- **Legacy Service**: Ensures compatibility and communication between BSV nodes and Teranodes.
- **UTXO Store**: Focuses on tracking all spendable UTXOs, essential for validating new transactions.

The Teranode's architecture revolutionizes Bitcoin's scalability through a combination of unbounded block size, innovative data models (such as SubTrees), and a modular node system. These advancements facilitate high transaction throughput and efficient network operations, positioning Teranode as a scalable solution for future blockchain demands.

-----
© 2024 BSV Blockchain Org.
