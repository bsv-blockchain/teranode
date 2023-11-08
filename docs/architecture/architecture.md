# UBSV (Unbounded Bitcoin Satoshi Vision) Architecture Overview

## Index


- [Overview](#overview)
- [Data Model and Propagation](#data-model-and-propagation)
  - [Block Size](#block-size)
  - [Original Bitcoin Data Model](#original-bitcoin-data-model)
  - [UBSV Data Model](#ubsv-data-model)
  - [Advantages of the UBSV Model](#advantages-of-the-ubsv-model)
  - [Network Behavior](#network-behavior)
- [Node Workflow](#node-workflow)
- [Services](#services)
  - [Transaction Propagation Service](#transaction-propagation-service)
  - [Transaction Validator](#transaction-validator)
  - [Block Assembly Service](#block-assembly-service)
  - [Miner / Hasher](#miner--hasher)
  - [Subtree and Block Validator](#subtree-and-block-validator)
    - [Service Components and Dependencies:](#service-components-and-dependencies)
    - [SubTree Validation:](#subtree-validation)
    - [Block Validation:](#block-validation)
    - [Overall Block and SubTree Validation Process](#overall-block-and-subtree-validation-process)
  - [Blockchain Service](#blockchain-service)
  - [Blob Server](#blob-server)
  - [Coinbase](#coinbase)
  - [Bootstrap](#bootstrap)
  - [P2P](#p2p)
  - [UTXO Store](#utxo-store)
  - [Transaction Meta Store](#transaction-meta-store)



## Overview


The original design of the Bitcoin network imposed a constraint on block size to 1 megabyte, a measure put in place to prevent spam transactions at a time when the digital currency's use was nascent. This size limit inherently restricts the network to a throughput of approximately **3.3 to 7 transactions per second**. As adoption has increased, this constraint has led to bottlenecks in transaction processing, resulting in delays and increased transaction fees, highlighting the need for a scalability solution.

**UBSV** is BSV’s solution to the challenges of vertical scaling by instead spreading the workload across multiple machines. This horizontal scaling approach, coupled with an unbound block size, enables network capacity to grow with increasing demand through the addition of cluster nodes, allowing for BSV scaling to be truly unbounded.

UBSV provides a robust node processing system for BSV that can consistently handle over **1M transactions per second**, while strictly adhering to the Bitcoin whitepaper.
The node has been designed as a collection of services that work together to provide a decentralized, scalable, and secure blockchain network. The node is designed to be modular, allowing for easy integration of new services and features.

The diagram below shows all the different microservices, together with their interactions, that make up the UBSV node.

![UBSV_Overview.png](img%2FUBSV_Overview.png)

Various services and components are outlined to show their interactions and functions within the system:

1. **Legacy P2P Network Bridge**: This service handles the legacy peer-to-peer communications within the blockchain network. As older (legacy) nodes will not be able to directly communicate with the newer (UBSV) nodes, this service acts as a bridge between the two types of nodes.

2. **TX / TXO Lookup Service**: This is used for querying "in scope" transaction and transaction ouput data.

3. **Peer Service**: This service handles node discovery, managing connections with peer nodes in the network.

4. **Coinbase Overlay Node**: This service handles the Coinbase transactions, which are the first transactions in a block that create new coins and reward miners. ****CLARIFY - Creates them??? what does this do? ** END CLARIFY**

5. **UBSV Node Core Services**:
  - **Propagation Service**: Responsible for propagating transactions through the network.
  - **TX Validation Service**: Checks transactions for correctness and adherence to network rules.
  - **Block Assembly Service**: Assembles blocks for the blockchain.
  - **Blockchain Service**: Responsible for managing the block headers and list of subtrees in a block.
  - **Block Validation Service**: Validates new blocks before they are added to the blockchain.
  - **Public Endpoints Service**: Provides API endpoints for external entities to interact with the node. **CLARIFY**

6. **Ancillary Services**:
  - **TX Submission (ARC)**: Manages the submission of new transactions to the network. **CLARIFY**
  - **Banlist Service**: Maintains and checks a list of banned entities or nodes. **CLARIFY**
  - **UTXO Lookup Service**:
    - **UTXO Lookup Service**: Retrieves information about **unspent** transaction outputs, which are essential for validating new transactions.

7. **Other Components**:
  - **Message Broker**: A middleware that facilitates communication between different services.
  - **TX Status Store**: Keeps track of the status of transactions within the system.  **CLARIFY**
  - **Hashers**: Perform the computational work of hashing that is central to the mining process.

---

## Data Model and Propagation

The UBSV data model addresses scalability and efficiency issues found in the original Bitcoin by changing the way data is organized and propagated across the network. Here's a summary of the key points and how they represent improvements over the historical Bitcoin model:

### Block Size
- **Bitcoin**: Fixed at 1MB, limiting the number of transactions per block.
- **BSV**: Increased to 4GB, allowing significantly more transactions per block.
- **UBSV**: Unbounded block size, enabling potentially unlimited transactions per block, increasing throughput, and reducing transaction fees due to economies of scale.

We can then compare the data models of the original Bitcoin and UBSV to understand how the latter improves on the former.

### Original Bitcoin Data Model

In the original Bitcoin the data model is as follows:

##### Transactions


Transactions are broadcast and included in blocks as they are found.


_Current Transaction format:_

| Field           | Description                                          | Size                                             |
|-----------------|------------------------------------------------------|--------------------------------------------------|
| Version no      | currently 2                                          | 4 bytes                                          |
| In-counter      | positive integer VI = [[VarInt]]                     | 1 - 9 bytes                                      |
| list of inputs  | Transaction Input  Structure                         | <in-counter> qty with variable length per input  |
| Out-counter     | positive integer VI = [[VarInt]]                     | 1 - 9 bytes                                      |
| list of outputs | Transaction Output Structure                         | <out-counter> qty with variable length per output |
| nLocktime       | if non-zero and sequence numbers are < 0xFFFFFFFF: block height or timestamp when transaction is final | 4 bytes                                          |


##### Blocks:


Blocks contain all transaction data for the transactions included.


![Legacy_Bitcoin_Block.png](img%2FLegacy_Bitcoin_Block.png)

Note how the Bitcoin block contains all transactions (including ALL transaction data) for each transaction it contains. This can only scale up to a certain point before the block size becomes too large to be practical.


Additionally, this is very inefficient. The receiving nodes will be idle for periods of approx 10 mins, and then receive a large amount of data to be validated in one go. This is a poor use of resources, and cannot scale to the levels required for a global payment system.


Let's see next how the UBSV data model addresses these issues.

### UBSV Data Model

##### Transactions:

UBSV Transactions (referred to as "Extended Transactions") include additional metadata to facilitate processing, and are broadcast to nodes as they occur.

The Extended Format adds a marker to the transaction format:


| Field           | Description                                                                                            | Size                                              |
|-----------------|--------------------------------------------------------------------------------------------------------|---------------------------------------------------|
| Version no      | currently 2                                                                                            | 4 bytes                                           |
| **EF marker**   | **marker for extended format**                                                                         | **0000000000EF**                                  |
| In-counter      | positive integer VI = [[VarInt]]                                                                       | 1 - 9 bytes                                       |
| list of inputs  | **Extended Format** transaction Input Structure                                                        | <in-counter> qty with variable length per input   |
| Out-counter     | positive integer VI = [[VarInt]]                                                                       | 1 - 9 bytes                                       |
| list of outputs | Transaction Output Structure                                                                           | <out-counter> qty with variable length per output |
| nLocktime       | if non-zero and sequence numbers are < 0xFFFFFFFF: block height or timestamp when transaction is final | 4 bytes                                           |


The Extended Format marker allows a library that supports the format to recognize that it is dealing with a transaction in extended format, while a library that does not support extended format will read the transaction as having 0 inputs, 0 outputs and a future nLock time. This has been done to minimize the possible problems a legacy library will have when reading the extended format. It can in no way be recognized as a valid transaction.


The input structure is the only additional thing that is changed in the Extended Format. The original input structure looks like this:

| Field                     | Description                                                                                 | Size                          |
|---------------------------|---------------------------------------------------------------------------------------------|-------------------------------|
| Previous Transaction hash | TXID of the transaction the output was created in                                           | 32 bytes                      |
| Previous Txout-index      | Index of the output (Non negative integer)                                                  | 4 bytes                       |
| Txin-script length        | Non negative integer VI = VarInt                                                            | 1 - 9 bytes                   |
| Txin-script / scriptSig   | Script                                                                                      | <in-script length>-many bytes |
| Sequence_no               | Used to iterate inputs inside a payment channel. Input is final when nSequence = 0xFFFFFFFF | 4 bytes                       |

In the Extended Format, we extend the input structure to include the previous locking script and satoshi outputs:

| Field                          | Description                                                                                 | Size                            |
|--------------------------------|---------------------------------------------------------------------------------------------|---------------------------------|
| Previous Transaction hash      | TXID of the transaction the output was created in                                           | 32 bytes                        |
| Previous Txout-index           | Index of the output (Non negative integer)                                                  | 4 bytes                         |
| Txin-script length             | Non negative integer VI = VarInt                                                            | 1 - 9 bytes                     |
| Txin-script / scriptSig        | Script                                                                                      | <in-script length>-many bytes   |
| Sequence_no                    | Used to iterate inputs inside a payment channel. Input is final when nSequence = 0xFFFFFFFF | 4 bytes                         |
| **Previous TX satoshi output** | **Output value in satoshis of previous input**                                              | **8 bytes**                     |
| **Previous TX script length**  | **Non negative integer VI = VarInt**                                                        | **1 - 9 bytes**                 |
| **Previous TX locking script** | **Script**                                                                                  | **\<script length>-many bytes** |

The Extended Format is not backwards compatible, but has been designed in such a way that existing software should not read a transaction in Extend Format as a valid (partial) transaction. The Extended Format header (0000000000EF) will be read as an empty transaction with a future nLock time in a library that does not support the Extended Format.


To know more about the Extended Transaction Format, please refer to the [Bitcoin Improvement Proposal 239 (09 November 2022)](https://github.com/bitcoin-sv/arc/blob/b6296d1f775e7f3568f915e13d8f03bfe8fd3c32/doc/BIP-239.md).


##### SubTrees:

The SubTrees are an innovation aimed at improving scalability and real-time processing capabilities of the blockchain system.

**Unique to UBSV**: The concept of SubTrees is a distinct feature not found in the original Bitcoin protocol or other derivatives.

1. A Subtree acts as an intermediate data structure to hold batches of 1M transaction IDs (including metadata) and their corresponding Merkle root.
2. Each SubTree computes its own Merkle root, which is a single hash representing the entire set of transactions within that SubTree.

**Efficiency**: Subtrees are broadcast every second (assuming the target of 1M transactions per second is met), making data propagation more continuous rather than batched every 10 minutes.
1. By broadcasting these SubTrees at such a high frequency, receiving nodes can validate these batches quickly and continuously, having them "pre-approved" for inclusion in a block.
2. This contrasts with the original Bitcoin protocol, where a new block, and hence a new batch of transactions, is broadcast approximately every ten minutes after being confirmed by miners.

**Lightweight**: Subtrees only include transaction IDs, not the full transaction data, since all nodes already have the transactions, thus reducing the size of the data to propagate.
1. Since all nodes participating in the network are assumed to already have the full transaction data (which they receive and store as transactions are created and spread through the network), it's unnecessary to rebroadcast the full details with every SubTree.
2. The SubTree then allows nodes to confirm they have all the relevant transactions and to update their state accordingly without having to process vast amounts of data repeatedly.

![UBSV_SubTree.png](img%2FUBSV_SubTree.png)

##### Blocks:
  - **UBSV**: Blocks contain lists of subtree identifiers, not transactions. This is practical for nodes because they have been processing subtrees continuously, allowing for quick validation of blocks.

![UBSV_Block.png](img%2FUBSV_Block.png)

### Advantages of the UBSV Model
- **Continuous Data Flow**: Instead of nodes being idle between blocks, they are continuously receiving and processing subtrees.
- **Faster Validation**: Since nodes process subtrees continuously, validating a block is quicker because it involves validating the presence and correctness of subtree identifiers rather than individual transactions.
- **Scalability**: The model supports a much higher transaction throughput (1M transactions per second).


### Network Behavior
- **Transactions**: They are broadcast network-wide, and each node further propagates the transactions.
- **Subtrees**: Nodes broadcast subtrees to indicate prepared batches of transactions for block inclusion, allowing other nodes to perform preliminary validations.
- **Block Propagation**: When a block is found, its validation is expedited due to the continuous processing of subtrees. If a node encounters a subtree it is unaware of within a new block, it can request the details from the node that submitted the block.

This proactive approach with subtrees enables the network to handle a significantly higher volume of transactions while maintaining quick validation times. It also allows nodes to utilize their processing power more evenly over time, rather than experiencing idle times between blocks. This model ensures that the UBSV can scale effectively to meet high transaction demands without the bottlenecks experienced by the original Bitcoin.

---

## Node Workflow

At a high level, the UBSV node performs the following functions:

1. **Transaction Submission**: Transactions are submitted to the network via the Submission Service. When a node receives a transaction, it immediately broadcast them to all other nodes in the network.


2. **Transaction Validation**: Transactions are validated by the TX Validation Service. This service checks each received transaction against the network's rules, ensuring they are correctly formed and that their inputs are valid and unspent (verified by the UTXO Lookup Service).Once validated, the status of transactions are updated in the TX Status Store, indicating they have not been included in a block yet and are eligible for inclusion.


3. **Subtree Assembly**: The Block Assembly Service ingests transactions and organizes them into subtrees. "Subtrees" are a key component of the UBSV node, allowing for efficient processing of transactions and blocks. A subtree can contain up to 1M transaction. Once a subtree is created, it is broadcasted to all other nodes in the network.
   * Note - nodes are expected to arrive to similar or equal subtree compositions. All nodes should have the same transactions in their subtrees, but the order of the transactions may differ. As they build their subtrees, nodes will broadcast them these subtrees to each other.


4. **Subtree Validation**: The Block Validation Service validates the subtrees it receives. This involves checking the transactions within the subtree against the network's rules and ensuring they are correctly formed and that their inputs are valid and unspent (verified by the UTXO Lookup Service). Once validated, the status of the subtree is updated, marking it as eligible for inclusion.
   * Note - If a subtree is not valid, it is discarded and not included in the block. If a subtree is valid, it is stored for later use in block assembly.


5. **Block Assembly**: The Block Assembly Service compiles block templates consisting of subtrees. These templates are pre-blocks that the node miner service will use to create a full block. Once a hashing solution has been found, the block is broadcasted to all other nodes for validation.


6. **Block Validation**: Once a node finds a valid hashing solution (a successful proof-of-work), the found block is sent to the Block Validation Service. This service checks the new block against the network's consensus rules. If the block is valid, the node will append it to their version of the blockchain.


![ValidationSequenceDiagram.svg](img%2FValidationSequenceDiagram.svg)



---

## Services

The node has been designed as a collection of microservices, each handling specific functionalities of the BSV network.

### Transaction Propagation Service

-- **CLARIFY** ???
** _Propagation Service only propagates transactions but not subtrees or blocks? Those are handled directly by the block assembly?_
** _Who sends subtrees and blocks to others?_
-- **END CLARIFY** ???

The Propagation service is responsible for:

* Receiving transactions from other nodes and forwarding them to the Validator service.

* Propagating transactions to other nodes.


Here is a breakdown of the components as shown:

1. **TX Storage Service:** This is a datastore that holds transactions that are eligible for inclusion in upcoming blocks.

2. **Multicast Receiver Service (Multiple Instances):** These services are responsible for listening to transactions that are being broadcasted over the network. These services listen on IPV6 multicast network addresses reserved for Bitcoin transactions. There are multiple instances, each listening to a set of fixed IPV6 addresses, offering a horizontally scalable design that allows for handling more transactions by increasing the number of service instances. There can be an arbitrary number of these multicast receiver services operating, which is part of how the system achieves scalability.

3. **Message Broker:** This is the communication layer that the multicast receiver services use to forward the extended transactions to the Validation service. The use of a message broker introduces decoupling between the services, allowing for more scalable and maintainable systems.

4. **Sanity Checks**: Before sending transactions on to the Validation Service, the Multicast Receiver Services perform basic validations to ensure transactions are correct and adhere to network protocols.

Note:
- **Communication via gRPC**: While the main network might use multicast for propagation, the test network simplifies operations by using gRPC, a high-performance, open-source universal RPC framework, for direct communication between the Propagation and Validation Services, bypassing the need for IPV6 broadcasting.




![UBSV_Propagation_Overview.jpg](img%2FUBSV_Propagation_Overview.jpg)

--- NOTE - WHERE IS THE CODE FOR THE TRANSACTION STORE? IS IT A SERVICE?? **CLARIFY**

- Technology and specific Stores [TODO] **CLARIFY**
  - TX Storage...

**CLARIFY**  - in this diagram, we have an extended transaction, but it is not clear what the extended transaction is. Is it the transaction with the metadata? Why does it come from the Multicast Group? Who originally assembles the "Extended TX"???? **CLARIFY**


---

### Transaction Validator

The Transaction Validation Service is responsible for validating a transaction according to the rules of the Bitcoin network and then sending the approved transaction ID forwards to block assembly.

The transactions that have passed the TX Validation Service are immediately marked as spent in the UTXO store.

After the UTXOs have been marked as spent, the transaction metadata is stored in the TX Status store and sent onwards to the Block Assembly Service via a Message Broker.


![UBSV_TX_Validation_Service.png](img%2FUBSV_TX_Validation_Service.png)


Here is a breakdown of the components as shown:

1. **Message Broker:** Communication layers that facilitate message passing between the Propagation Service and the Validation Service, and between the Validation Service and the Block Assembly.

2. **Transaction Validation Service (Multiple Instances):** Multiple instances of the service can be initiated, allowing to validate transactions in parallel, which will help to increase throughput and scalability.

4. **TX ids:** After validation, transaction identifiers (ids) are passed to the Block Assembly service.

5. **UTXO Service:** Datastore of UTXOs (the outputs from transactions that have not been spent and can be used as inputs in new transactions).

6. **TX Status:** Datastore managing the statuses of transactions. If validated and not mined, they will be eligible for inclusion in a block.


** CLARIFY - What is the TX METADATA Store for? Include here in the details? ** CLARIFY


---


### Block Assembly Service

The service is responsible for creating subtree and block templates for Miner Services to hash against. The Block Assembly will broadcast any newly created subtrees and blocks to the network.

There are two distinct processes that the Block Assembly Service performs:

1. **Subtree Assembly**: The Block Assembly Service ingests transactions and organizes them into subtrees. "Subtrees" are a key component of the UBSV node, allowing for efficient processing of transactions and blocks. A subtree can contain up to 1M transaction. Once a subtree is created, it is broadcasted to all other nodes in the network.

2. **Block Assembly**: The Block Assembly Service compiles block templates consisting of subtrees and their transactions. Once a hashing solution has been found, the block is broadcasted to all other nodes for validation.


![UBSV_Block_Assembly_Service_Overview.png](img%2FUBSV_Block_Assembly_Service_Overview.png)


Here’s an explanation of the components and their interactions:

1. **Subtree Storage**: This stores blocks and their corresponding Merkle subtrees.

2. **Message Broker**: This is a middleware that allows for the decoupling of different parts of the blockchain system, facilitating asynchronous communication. Transaction IDs (Tx IDs) are sent from the Message Broker to the Block Assembly Controller.

3. **Block Assembly Controller**: This component orchestrates the process of creating new subtrees and new blocks. It receives transaction IDs from the Message Broker, performs necessary checks, and assembles subtrees and blocks.

4. **TX Status Database**: This stores the status of transactions. Before a transaction ID is included in a subtree or block template, the Block Assembly Service checks this database to ensure that the transaction has not been previously included in another subtree / block (i.e., it is not yet seen in a block).

5. **Subtrees**: Completed subtrees are announced on the network, so other nodes can incorporate them into their blocks.

6. **Hashers / Miners**: After the block template (including all eligible subtrees of transactions) is prepared, it is sent to entities called Miners, which are responsible for performing the computational work (hashing) needed to find a valid block.


---


### Miner / Hasher

The Miner service is responsible for mining blocks. It solves a hashing proof of work for the current set of transactions and subtrees on behalf of a Block Assembly Service, consistently with the Blockchain Whitepaper rules.

Upon successful mining of a block, the miner is rewarded with a block reward (newly minted coins) and the transaction fees from the transactions included in the block.

Challenges and Considerations:

- If two miners solve a block at roughly the same time, there could be a temporary fork in the blockchain. The network resolves this by choosing the chain with the most accumulated work (typically the longest chain).

- Miners must handle orphaned blocks and transactions: if another chain becomes longer, transactions from the orphaned blocks are tracked to be mined in future blocks.

---


### Subtree and Block Validator


The Validator service is a critical component designed to ensure the integrity and consistency of the blockchain. Its responsibilities can be broadly categorized into two main areas: SubTree validation and Block validation. In addition, the Validator service plays a key role in maintaining the Unspent Transaction Outputs (UTXO) set, which is essential for determining the ownership of coins.




![UBSV_Block_Validation_Overview.png](img%2FUBSV_Block_Validation_Overview.png)



#### Service Components and Dependencies:

1. **Subtree Storage:** Holds the Merkle subtrees, which are partial hashes of the complete block. These are crucial for quickly validating new blocks found by miners.

2. **TX Storage Service:** Maintains a record of all recently created transactions, including those that have not yet been confirmed and added to a block on the blockchain.

3. **Block Validation Controller:** Acts as the orchestrator for the block validation process. It reacts to new blocks being found by either this node or other nodes in the network.

4. **Block Validation Subtree Service:** This micro-service (1 or more present) is tasked with validating the individual subtrees of a block. It operates in parallel to handle the validation workload efficiently, as indicated by multiple instances (1 or more).

5. **Multicast Subtree Announcement Network:** This network's role is to broadcast Merkle subtree announcements to the network. Multicast allows for efficient distribution of information to nodes interested in receiving these announcements.

6. **TX Point to Point Request Network (IPv6):** This is utilized to request the full transaction data for transactions that are not "blessed."

7. **UTXO Service:** Manages the list of all unspent transaction outputs, which represent potential inputs for new transactions.

8. **TX Status:** Keeps track of the validation status of individual transactions.


#### SubTree Validation:

- **Real-Time Validation**: As SubTrees are received, which may occur as frequently as every second, the Validator service immediately checks their integrity. This involves verifying that each transaction ID listed within a SubTree is valid and that the corresponding transactions exist and are themselves valid within the network's consensus rules.


- **UTXO Validation**: Part of the validation process includes ensuring that the transactions within a SubTree correctly reference and spend UTXOs.


- **Efficiency and Scalability**: By validating SubTrees in real-time, the system can efficiently manage a high throughput of transactions, reducing bottlenecks that would otherwise arise during block validation.


- **Handling Unvalidated Transactions**: If a SubTree contains a transaction that hasn't been previously validated or "blessed," the Validator service must retrieve and validate that transaction. If the transaction is valid, it is added to the block assembly process. If it is invalid due to issues like missing inputs or double-spending attempts, the transaction and its SubTree are not accepted ("blessed").


#### Block Validation:

- **Top Merkle Tree Validation**: When a new block is announced, the Validator service doesn't need to validate all individual transactions, as the SubTrees have already been validated. Instead, it can quickly validate the block by checking the top part of the Merkle tree composed of the hashes of the SubTrees. This greatly reduces the amount of data that needs to be processed during block validation.


- **Merkle Subtree Store**: This is a dedicated storage component within the Validator service that holds the SubTrees. It retains the SubTrees until a new block is found and integrated into the blockchain, after which the SubTrees are no longer needed and can be discarded. This storage ensures that SubTrees are readily available for block validation and helps in maintaining the continuity of the validation process.


#### Overall Block and SubTree Validation Process

The validation process is continuous and iterative, designed to maintain the blockchain's integrity and support high transaction throughput:

1. **Transaction and SubTree Receipt**: Transactions are collected and grouped into SubTrees, each representing up to 1 million transaction IDs.


2. **SubTree Validation**: As SubTrees are broadcast, the Validator service validates them in real-time.


3. **Merkle Subtree Storage**: Validated SubTrees are stored in the Merkle Subtree Store.


4. **Block Announcement**: When a new block is discovered by a miner, it's announced to the network, including only the top-level Merkle hashes of the SubTrees.


5. **Block Validation by Validator Service**: The Validator service uses the stored SubTrees to validate the newly announced block quickly. This is done by deriving the announced top-level Merkle hashes from the hashes of the SubTrees in the Merkle Subtree Store.


6. **Transaction Retrieval and Validation**: If a SubTree contains a transaction that has not been previously validated, the Validator service retrieves the full transaction data and validates it.


7. **SubTree "Blessing"**: If the Validator service successfully validates a SubTree, it is "blessed," indicating approval. If a SubTree (or a transaction within the Subtree) fails validation, the subtree is rejected.


8. **Block Assembly and Propagation**: Once the Validator service validates a block, it propagates this block to the Blockchain service, ensuring that the blockchain remains up-to-date and consistent across all nodes.


---

### Blockchain Service

The service is reponsible for managing block updates and adding them to the blockchain maintained by the node. The blocks can be received from other nodes or mined by the node itself.

![UBSV_Blockchain_Service_Overview.png](img%2FUBSV_Blockchain_Service_Overview.png)


Here is an explanation of the process:

- This service manages block headers and lists of subtrees (segments of the blockchain) in each block.
- All blocks are recorded in the blockchain databases, including orphaned blocks (blocks that were not accepted into the main chain).
- Other services can request and store blocks with the service, for purposes like analyzing blockchain data or validating the chain's integrity.
- The service also provides information about the blockchain's current state, such as the best block header (the header of the most recently accepted block, which is critical for mining new blocks) and the current difficulty (a measure of how hard it is to find a new block, which adjusts to keep block discovery times consistent).


Here is an explanation of the components and the process:

1. **Block (header) storage DB**: This is a database that stores block headers. In blockchain systems, a block header is part of a block that includes metadata such as the previous block's hash, the timestamp, the nonce used for mining, and the Merkle tree root.

2. **Blockchain Server**: It acts as the central node that interfaces with the block storage database. It is responsible for processing new blocks (Block Found) and identifying the best block header to be used for further operations like mining or appending to the chain.

3. **Block Assembly Service**: This service takes transactions that are waiting to be included in a block and assembles them into new subtrees and blocks. Once a new block is created (Block Found), it sends this block to the blockchain server.

4. **Block Validation Service**: This component is responsible for validating new blocks. It checks if a block complies with the network's consensus rules. After the blockchain server processes a block (Block Found), it will interact with this service to ensure that the block is valid before finalizing it in the blockchain.

The system is designed to maintain the blockchain's integrity by ensuring that all blocks are properly assembled, validated, and stored. It enables other services and participants in the network to interact with the blockchain, request data, and understand the current state of the network for further actions like mining.


---


### Blob Server

The Blob Server service is responsible for storing and retrieving blobs. It is a key-value store that uses the blob ID as the key and the blob as the value. The Blob Server service is used by the Validator service to retrieve blobs when validating transactions.

*** CLARIFY - What is the Blob Server Service? Tie up with the model *** CLARIFY

- Business rationale / what it is for / process flows / diagrams [TODO]
- Architecture [TODO]
- Data model [TODO]
- Technology and specific Stores [TODO]
- Related modules [TODO]

---


### Coinbase

The Coinbase service is responsible for creating coinbase transactions. It is also responsible for receiving coinbase transactions from other nodes and forwarding them to the Validator service.

- Business rationale / what it is for / process flows / diagrams [TODO]
- Architecture [TODO]
- Data model [TODO]
- Technology and specific Stores [TODO]
- Related modules [TODO]


---


### Bootstrap

The Bootstrap service is responsible for bootstrapping the network. It is also responsible for receiving bootstrap nodes from other nodes and forwarding them to the Validator service.

This object is designed to manage a set of subscribers and broadcast notifications to them, likely to keep them updated about changes in the network or data of interest. Discovery....

This service is likely an integral part of a network's node discovery and communication layer, responsible for maintaining up-to-date information about network participants and facilitating their communication.
*** **CLARIFY** - What is the Bootstrap Service? Tie up with the model *** **CLARIFY**


- Business rationale / what it is for / process flows / diagrams [TODO]
- Architecture [TODO]
- Data model [TODO]
- Technology and specific Stores [TODO]
- Related modules [TODO]


---


### P2P

The P2P service is responsible for managing communications between legacy and UBSV nodes, effectively translating between the old (Legacy Bitcoin) and the new (UBSV) data model. This makes possible to run legacy and UBSV nodes side by side, allowing for a gradual rollout of UBSV.

This legacy P2P network is to be phased out as transaction volumes increase, forcing an eventual migration towards a more scalable and feature-rich system.


![Legacy_P2P_Overview.png](img%2FLegacy_P2P_Overview.png)


This diagram describes the architecture and workflow of the legacy Peer-to-Peer (P2P) service.

Here's the breakdown of the components and their functions:

1. **P2P Network (<IPv4>)**: This refers to the original Bitcoin peer-to-peer network using the IPv4 internet protocol.

2. **P2P Receiver Service**: These are the services (1 or more) that receive and send transactions from / to the P2P network.

3. **TX Lookup service**: This service is responsible for looking up previously stored transaction information. It is used to enrich transactions with additional information before they are broadcast to the network.

4. **Multicast Group Tx Receive**: This indicates a multicast setup where transactions are broadcast to multiple nodes simultaneously. This is efficient for disseminating information quickly to many nodes in the network.

---

### UTXO Store

The UTXO Store service is responsible for storing and retrieving UTXOs. It is a key-value store that uses the UTXO ID as the key and the UTXO as the value. The UTXO Store service is used by the Validator service to retrieve UTXOs when validating transactions.

- Business rationale / what it is for / process flows / diagrams [TODO]
- Architecture [TODO]
- Data model [TODO]
- Technology and specific Stores [TODO]
- Related modules [TODO]

---

### Transaction Meta Store

The Transaction Meta Store service is responsible for storing and retrieving transaction metadata. It is a key-value store that uses the transaction ID as the key and the transaction metadata as the value. The Transaction Meta Store service is used by the Validator service to retrieve transaction metadata when validating transactions.

- Business rationale / what it is for / process flows / diagrams [TODO]
- Architecture [TODO]
- Data model [TODO]
- Technology and specific Stores [TODO]
- Related modules [TODO]
