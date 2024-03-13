# 🔍 SubTree Assembly Service

## Index

1. [Description](#1-description)
2. [Functionality](#2-functionality)
- [2.1 Service Initialization](#21-service-initialization)
- [2.2 Receiving a new Block Notification](#22-receiving-a-new-block-notification)
- [2.3 Processing a Subtree](#23-processing-a-subtree)
3. [Data Model](#3-data-model)
4. [Technology](#4-technology)
5. [Directory Structure and Main Files](#5-directory-structure-and-main-files)
6. [How to run](#6-how-to-run)
7. [Configuration options (settings flags)](#7-configuration-options-settings-flags)


## 1. Description

The Subtree Assembly service functions as an overlay microservice, designed to post-process subtrees after their integration into blocks.

Whenever a new block is introduced to the blockchain, the Subtree Assembly service decorates (enriches) all transactions within the block's subtrees, ensuring the inclusion of transaction metadata (extended transaction format).

This service plays a key role within the Teranode network, guaranteeing that subtrees are accurately processed and stored with the essential metadata for necessary audit and traceability purposes.

It's important to note that the post-processed subtree data is not automatically utilized by any subsequent service. However, the decorated subtrees remain invaluable for future data analysis and inspection.

![Subtree_Assembly_Service_Container_Diagram.png](img%2FSubtree_Assembly_Service_Container_Diagram.png)

* The Subtree Assembly consumes notifications from the Blockchain service, and stores the decorated subtree txs in the subtree store.

![Subtree_Assembly_Service_Components_Diagram.png](img%2FSubtree_Assembly_Service_Components_Diagram.png)

* The Blockchain service relies on Kafka for its new block notifications, to which the Subtree Assembly service subscribes to.


## 2. Functionality

### 2.1 Service Initialization

![subtree_assembly_init.svg](img%2Fplantuml%2Fsubtreeassembly%2Fsubtree_assembly_init.svg)

- The service starts by initializing a connection to the subtree store and subscribing to the new block notifications from Kafka.
- Additionally, it subscribes to internally generated subtree notifications.

### 2.2 Receiving a new Block Notification

![subtree_assembly_receive_new_blocks.svg](img%2Fplantuml%2Fsubtreeassembly%2Fsubtree_assembly_receive_new_blocks.svg)

- The Blockchain service, after adding a new block, emits a Kafka notification which is received by the Subtree Assembly service.
- The Subtree Assembly service then extracts every subtree from the block and processes each one.
  - To do, each subtree is sent to a kafka topic, and then consumed by the same service.
  - The Subtree Assembly consumes and processes the subtree notifications in parallel, allowing for efficient processing of a large number of subtrees.

### 2.3 Processing a Subtree

The processing of a specific subtree (in the ProcessSubtree method) involves the following steps:

![subtree_assembly_process_subtree.svg](img%2Fplantuml%2Fsubtreeassembly%2Fsubtree_assembly_process_subtree.svg)

- The subtree data is obtained from the subtree store based on the hash.
- Iterating over each Tx in the Subtree, the service decorates all transactions (txMetaStore.MetaBatchDecorate).
- A new subtree blob is generated with the decorated transactions.
- The new subtree blob is saved with the decorated transactions in the subtree store, overriding the original subtree blob.


## 3. Data Model

The Subtree Assembly service data model is identical in scope to the Block Validation model. Please refer to the Block Validation documentation [here](blockValidation.md#4-data-model) for more information.

## 4. Technology

1. **Go (Golang):** The primary programming language used for developing the service.

2. **Bitcoin SV (BSV) Libraries:**
  - **Data Models and Utilities:** For handling BSV blockchain data structures and operations, including transaction and block processing.

3. **Apache Kafka:**
  - **Distributed Messaging:** Used for consuming block and subtree notifications and producing messages related to subtree processing.

4. **Storage Libraries:**
  - **Blob Store:** For storing and retrieving the subtree blobs.
  - **Transaction Metadata Store:** To access and store transaction metadata-

5. **Configuration and Logging:**
  - **Dynamic Configuration:** For managing service settings, including Kafka broker URLs and worker configurations.
  - **Logging:** For monitoring service operations, error handling, and debugging.


## 5. Directory Structure and Main Files

The Subtree Assembly service is located in the `services/subtreeassembly` directory. All logic can be found on the `Server.go` file.

## 6. How to run

To run the Subtree Assembly Service locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -SubtreeAssembly=1
```

Please refer to the [Locally Running Services Documentation](../locallyRunningServices.md) document for more information on running the Block Assembly Service locally.


## 7. Configuration options (settings flags)

1. **Kafka Brokers URLs for Blocks and Subtrees:**
  - **`block_kafkaBrokers`**: This URL setting is used to configure the Kafka brokers for listening to blockchain block notifications.
  - **`subtree_kafkaBrokers`**: Similar to the block Kafka brokers, this URL is for the Kafka brokers dedicated to subtree notifications. It's obtained using `gocore.Config().GetURL("subtree_kafkaBrokers")`. The service utilizes this URL to connect to the Kafka cluster for receiving and sending subtree-related messages.

2. **Worker Settings for Kafka Listeners:**
  - **`block_kafkaWorkers`**: This setting specifies the number of workers to use for the Kafka blocks listener.
  - **`subtree_kafkaWorkers`**: Similarly, this setting defines the number of workers for the Kafka subtrees listener.
