# 🔍 Block Persister Service

## Index

1. [Description](#1-description)
2. [Functionality](#2-functionality)
- [2.1 Service Initialization](#21-service-initialization)
- [2.2 Receiving and Processing a new Block Notification](#22-receiving-and-processing-a-new-block-notification)
3. [Data Model](#3-data-model)
4. [Technology](#4-technology)
5. [Directory Structure and Main Files](#5-directory-structure-and-main-files)
6. [How to run](#6-how-to-run)
7. [Configuration options (settings flags)](#7-configuration-options-settings-flags)
8. [Other Resources](#8-other-resources)

## 1. Description

The Block Persister service functions as an overlay microservice, designed to post-process subtrees after their integration into blocks and persisting them to a separate storage.

Whenever a new block is introduced to the blockchain, the Block Persister service decorates (enriches) all transactions within the block's subtrees, ensuring the inclusion of transaction metadata (UTXO meta data). Then, it will save a number of files into a file storage system (such as S3):

- A file for the block (`.block` extension).
- A file for each subtree in a block (`.subtree` extension), containing the decorated transactions.
- A file for the UTXO Additions (`.utxo-additions` extension) per block, containing the newly created UTXOs.
- A file for the UTXO Deletions ( `.utxo-deletions` extension) per block, containing the spent UTXOs.

This service plays a key role within the Teranode network, guaranteeing that txs are accurately post-processed and stored with the essential metadata for necessary audit and traceability purposes. The decorated subtrees remain invaluable for future data analysis and inspection.

The Block Persister files are optionally post-processed by the UTXO Persister, which maintains a UTXO Set in a similar disk format.

![Block_Persister_Service_Container_Diagram.png](img/Block_Persister_Service_Container_Diagram.png)

* The Block Persister consumes notifications from the Blockchain service, and stores the decorated block in a data store (such as S3).

![Block_Persister_Service_Component_Diagram.png](img/Block_Persister_Service_Component_Diagram.png)

* The Blockchain service relies on Kafka for its new block notifications, to which the Block Persister service subscribes to.

* However, note how the Blockchain client is directly accessed in order to wait for the node State Management to change to `RUNNING` state. For more information on this, please refer to the [State Management](../architecture/stateManagement.md)  documentation.

## 2. Functionality

### 2.1 Service Initialization

![block_persister_init.svg](img/plantuml/blockpersister/block_persister_init.svg)

The service initializes through the following sequence:

1. Loads configuration settings
2. Initializes state management
3. Establishes connection with blockchain client
4. Waits for FSM transition from IDLE state
5. Starts block processing loop

### 2.2 Receiving and Processing a new Block Notification

![block_persister_receive_new_blocks.svg](img/plantuml/blockpersister/block_persister_receive_new_blocks.svg)

The service processes blocks through a polling mechanism:

1. **Block Discovery**
  - Retrieves last persisted block height from the Blockchain
  - Determines if new blocks need processing based on `BlockPersisterPersistAge`. `BlockPersisterPersistAge` defines how many blocks behind the current tip the persister should stay. i.e. the service intentionally stays `BlockPersisterPersistAge` blocks behind the tip. This helps to avoid reorgs and ensures block finality.

2. **Processing Flow**
  - Retrieves the next block to process
  - Converts block to bytes
  - Persists block data to storage
  - Creates and stores the associated files:
    - Block file (.block)
    - A Subtree file for each subtree in the block (.subtree), including the number of transactions in the subtree, and the decorated transactions (as UTXO meta data).
    - UTXO additions (.utxo-additions), containing the UTXOs spent in the block. This represent a list of removed Txs (inputs)
    - UTXO deletions (.utxo-deletions), containing the UTXOs added to the block. This represents a list of added Txs (outputs), including the Coinbase Tx.
  - Updates the local state with the new block height

3. **Sleep Mechanisms**
  - On error: the services sleeps for a 1-minute period
  - If no new blocks: The service sleeps for a configurable period if time (`BlockPersisterPersistSleep`)


In more detail:

![block_persister_receive_new_blocks_subtrees.svg](img/plantuml/blockpersister/block_persister_receive_new_blocks_subtrees.svg)


## 3. Data Model

The Block Persister service data model is identical in scope to the Block Validation model. Please refer to the Block Validation documentation [here](blockValidation.md#4-data-model) for more information.

In addition to blocks and subtrees, utxo additions and deletions files are created for each block, containing the newly created and spent UTXOs, respectively.

- **.utxo-additions** file:

Content: A series of UTXO records, each containing:

- TxID (32 bytes)
- Output Index (4 bytes)
- Value (8 bytes)
- Block Height (4 bytes)
- Locking Script Length (4 bytes)
- Locking Script (variable length)
- Coinbase flag (1 bit, packed with block height)

Format: Binary encoded, as per the UTXO struct:

```go
type UTXO struct {
    TxID     *chainhash.Hash
    Index    uint32
    Value    uint64
    Height   uint32
    Script   []byte
    Coinbase bool
}
```

- **.utxo-deletions** file:

Content: A series of UTXO deletion records, each containing:
- TxID (32 bytes)
- Output Index (4 bytes)

Format: Binary encoded, as per the UTXODeletion struct:

```go
type UTXODeletion struct {
    TxID  *chainhash.Hash
    Index uint32
}
```



## 4. Technology

1. **Go (Golang):** The primary programming language used for developing the service.

2. **Bitcoin SV (BSV) Libraries:**
  - **Data Models and Utilities:** For handling BSV blockchain data structures and operations, including transaction and block processing.

3. **Storage Libraries:**
  - **Blob Store:** For retrieving the subtree blobs.
  - **UTXO Store:** To access and store transaction metadata.
  - **File Storage:** For saving the decorated block files.

4. **Configuration and Logging:**
  - **Dynamic Configuration:** For managing service settings, including Kafka broker URLs and worker configurations.
  - **Logging:** For monitoring service operations, error handling, and debugging.


## 5. Directory Structure and Main Files

The Block Persister service is located in the `services/blockpersister` directory.


```
services/blockpersister/
├── state/          # State management
├── server.go       # Main service implementation
└── metrics.go      # Prometheus metrics
```

## 6. How to run

To run the Block Persister Service locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -BlockPersister=1
```

Please refer to the [Locally Running Services Documentation](../locallyRunningServices.md) document for more information on running the Block Persister Service locally.


## 7. Configuration options (settings flags)

The Block Persister service uses the following configuration settings:

| Setting Name                              | Type     | Description                                                       | Default                                  |
|-------------------------------------------|----------|-------------------------------------------------------------------|------------------------------------------|
| `blockpersister_stateFile`                | string   | Path to state file for tracking persisted blocks                  | "file://./data/blockpersister_state.txt" |
| `blockpersister_httpListenAddress`        | string   | HTTP listener address for blob store server                       | ":8083"                                  |
| `blockpersister_persistAge`               | int      | Number of blocks to stay behind the tip (controls block finality) | 100                                      |
| `blockpersister_persistSleep`             | duration | Sleep duration between polling attempts when no blocks available  | 1 minute                                 |
| `blockpersister_concurrency`              | int      | Concurrency level for block processing                            | 8                                        |
| `blockpersister_batchMissingTransactions` | bool     | Whether to batch missing transaction requests                     | true                                     |
| `blockpersister_skipUTXODelete`           | bool     | Whether to skip UTXO deletion operations                          | false                                    |


The `BlockPersisterPersistAge` setting is particularly important as it determines how many blocks behind the tip the persister stays. For example:
- If set to 100 (default), only blocks that are at least 100 blocks deep will be persisted
- This helps avoid reorgs by ensuring blocks are sufficiently confirmed


## 8. Other Resources

[Block Persister Reference](../../references/services/blockpersister_reference.md)
