# 🔍 Block Persister Service

## Index

1. [Description](#1-description)
2. [Functionality](#2-functionality)
    - [2.1 Service Initialization](#21-service-initialization)
    - [2.2 Block Discovery and Processing](#22-block-discovery-and-processing)
    - [2.3 Subtree Processing Details](#23-subtree-processing-details)
3. [Data Model](#3-data-model)
4. [Technology](#4-technology)
5. [Directory Structure and Main Files](#5-directory-structure-and-main-files)
6. [How to run](#6-how-to-run)
7. [Configuration options (settings flags)](#7-configuration-options-settings-flags)
8. [Other Resources](#8-other-resources)

## 1. Description

The Block Persister service functions as an overlay microservice, designed to post-process subtrees after their integration into blocks and persisting them to a separate storage.

The Block Persister service operates as a **background processing service** that continuously polls the blockchain service for new blocks. Unlike other Teranode services, it does not expose a gRPC API and functions purely as a data persistence layer. When new blocks are detected through polling, the Block Persister service decorates (enriches) all transactions within the block's subtrees, ensuring the inclusion of transaction metadata (UTXO meta data). Then, it will save a number of files into a file storage system (such as S3):

- A file for the block (`.block` extension).
- A file for each subtree in a block (`.subtree` extension), containing the decorated transactions.
- A file for the UTXO Additions (`.utxo-additions` extension) per block, containing the newly created UTXOs.
- A file for the UTXO Deletions ( `.utxo-deletions` extension) per block, containing the spent UTXOs.

This service plays a key role within the Teranode network, guaranteeing that txs are accurately post-processed and stored with the essential metadata for necessary audit and traceability purposes. The decorated subtrees remain invaluable for future data analysis and inspection.

The Block Persister files are optionally post-processed by the UTXO Persister, which maintains a UTXO Set in a similar disk format.

> **Note**: For information about how the Block Persister service is initialized during daemon startup and how it interacts with other services, see the [Teranode Daemon Reference](../../references/teranodeDaemonReference.md#service-initialization-flow).

![Block_Persister_Service_Container_Diagram.png](img/Block_Persister_Service_Container_Diagram.png)

- The Block Persister polls the Blockchain service for new blocks and stores the decorated block in a data store (such as S3).

![Block_Persister_Service_Component_Diagram.png](img/Block_Persister_Service_Component_Diagram.png)

- The Block Persister service operates independently by polling the Blockchain service directly for new blocks, rather than relying on external notification systems.

- The Blockchain client is directly accessed to wait for the node State Management to change to `RUNNING` state before beginning block processing operations. For more information on this, please refer to the [State Management](../architecture/stateManagement.md) documentation.

### Detailed Component View

The following diagram provides a deeper level of detail into the Block Persister Service's internal components and their interactions:

![block_persister_detailed_component.svg](img/plantuml/blockpersister/block_persister_detailed_component.svg)

## 2. Functionality

### 2.1 Service Initialization

![block_persister_init.svg](img/plantuml/blockpersister/block_persister_init.svg)

The service initializes through the following sequence:

1. Loads configuration settings
2. Establishes connection with blockchain client
3. Waits for FSM transition from IDLE state
4. Starts block processing loop

### 2.2 Block Discovery and Processing

![block_persister_receive_new_blocks.svg](img/plantuml/blockpersister/block_persister_receive_new_blocks.svg)

The service processes blocks through a **continuous polling mechanism**:

1. **Block Discovery**
    - Queries the database for blocks where `persisted_at IS NULL` and `invalid = false`
    - Retrieves blocks in ascending height order to ensure sequential processing
    - Returns immediately if no unpersisted blocks are found

2. **Processing Flow**
    - Retrieves the next block to process from the Blockchain service
    - Converts block to bytes
    - Persists block data to storage
    - Creates and stores the associated files:

    - Block file (.block)
    - A Subtree file for each subtree in the block (.subtree), including the number of transactions in the subtree, and the decorated transactions (with UTXO meta data).
    - UTXO additions (.utxo-additions), containing the UTXOs created in the block. This represents a list of new transaction outputs, including the Coinbase transaction outputs.
    - UTXO deletions (.utxo-deletions), containing the UTXOs spent in the block. This represents a list of transaction inputs that reference and spend previous outputs.
    - Updates the block's `persisted_at` timestamp in the database

3. **Sleep Mechanisms**
    - On error: the service sleeps for a 1-minute period before retrying
    - If no new blocks: The service sleeps for a configurable period (`BlockPersisterPersistSleep`) before polling again

4. **Service Dependencies**
    - **FSM State Dependency**: The service waits for the blockchain FSM (Finite State Machine) to transition from the IDLE state to RUNNING before beginning block processing operations
    - **No gRPC API**: Unlike other Teranode services, the Block Persister does not expose a gRPC API and operates purely as a background processing service

### 2.3 Subtree Processing Details (Two-Phase Architecture)

![block_persister_receive_new_blocks_subtrees.svg](img/plantuml/blockpersister/block_persister_receive_new_blocks_subtrees.svg)

The Block Persister uses a **two-phase architecture** for efficient subtree processing:

#### Phase 1: Create Subtree Data Files (Parallel)

Multiple subtrees are processed concurrently with configurable concurrency:

1. **Subtree Retrieval**
    - Retrieves subtree structure from the subtree store
    - Checks if subtree data already exists (skips if present)

2. **Transaction Metadata Loading**
    - Loads transaction metadata in batches for efficiency
    - Decorates transactions with UTXO metadata from the store
    - Falls back to individual processing if batch fails

3. **Streaming File Creation**
    - Uses `SubtreeDataWriter` for ordered batch writes
    - Streams data directly to storage without buffering entire file
    - Stores decorated subtree files (.subtree extension)

4. **Error Recovery**
    - Uses success-flag pattern to ensure incomplete files are never finalized
    - On error, `Abort()` is called instead of `Close()` to remove temporary files
    - No incomplete subtree files are left in storage after failures

#### Phase 2: Process UTXO Changes (Sequential)

UTXO processing runs sequentially to maintain correct ordering:

1. **Read Subtree Data Files**
    - Opens each subtree data file for streaming read
    - Processes subtrees in order

2. **UTXO Diff Processing**
    - Processes each transaction through the UTXO diff tracker
    - Records additions (new outputs) and deletions (spent outputs)
    - Writes to `.utxo-additions` and `.utxo-deletions` files

This two-phase approach provides:

- **Maximum parallelism** for I/O-bound subtree file creation
- **Correct ordering** for UTXO state updates
- **Memory efficiency** through streaming reads/writes

## 3. Data Model

The Block Persister service data model is identical in scope to the Block Validation model. Please refer to the [Block Validation documentation](blockValidation.md#4-data-model) for more information.

In addition to blocks and subtrees, utxo additions and deletions files are created for each block, containing the newly created and spent UTXOs, respectively.

### UTXO Files

#### .utxo-additions file

**Content**: A series of UTXO records, each containing:

- TxID (32 bytes)
- Output Index (4 bytes)
- Value (8 bytes)
- Block Height (4 bytes)
- Locking Script Length (4 bytes)
- Locking Script (variable length)
- Coinbase flag (1 bit, packed with block height)

**Format**: Binary encoded, as per the UTXO struct:

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

#### .utxo-deletions file

**Content**: A series of UTXO deletion records, each containing:

- TxID (32 bytes)
- Output Index (4 bytes)

**Format**: Binary encoded, as per the UTXODeletion struct:

```go
type UTXODeletion struct {
    TxID  *chainhash.Hash
    Index uint32
}
```

### 3.1 Storage Architecture

The Block Persister service uses two distinct storage buckets:

#### Block Store

The block-store bucket contains all the persistent data needed for blockchain reconstruction:

- **Block files** (`.block`) - Complete serialized block data including all transactions
- **Detailed subtree files** (`.subtree`) - Complete transaction data for each subtree, containing full transaction contents
- **UTXO files** (`.utxo-additions`, `.utxo-deletions`, `.utxo-set`) - UTXO state changes and complete sets

#### Subtree Store

The subtree-store bucket contains lightweight subtree information:

- **Lightweight subtree files** (no extension) - Minimal transaction metadata (hash, fee, size)
- **Optional meta files** (`.meta`) - Additional transaction metadata created by block validation

This dual-storage approach serves different purposes:

1. The **subtree-store** acts as a shared, lightweight transaction reference used by multiple services (block validation, subtree validation, block assembly, and asset services)

2. The **block-store** contains comprehensive blockchain data with complete transaction details needed for audit, analysis and blockchain reconstruction

If you need all transaction information in a subtree, you should access the `.subtree` file in the block-store bucket. If you only need basic transaction references (hashes, fees), you can use the more efficient files in the subtree-store bucket.

## 4. Technology

1. **Go (Golang):** The primary programming language used for developing the service.

2. **Bitcoin SV (BSV) Libraries:**
    - **Data Models and Utilities:** For handling BSV blockchain data structures and operations, including transaction and block processing.

3. **Storage Libraries:**
    - **Blob Store:** For retrieving the subtree blobs.
    - **UTXO Store:** To access and store transaction metadata.
    - **File Storage:** For saving the decorated block files.

4. **Configuration and Logging:**
    - **Dynamic Configuration:** For managing service settings, including transaction metadata caching configurations and worker settings.
    - **Logging:** For monitoring service operations, error handling, and debugging.

## 5. Directory Structure and Main Files

The Block Persister service is located in the `services/blockpersister` directory.

```text
services/blockpersister/
├── Server.go                          # Main service implementation
├── Server_test.go                     # Service tests
├── persist_block.go                   # Block persistence logic
├── persist_block_test.go              # Block persistence tests
├── streaming_process_subtree.go       # Subtree processing with streaming writes
├── streaming_process_subtree_test.go  # Subtree processing tests
├── subtree_data_writer.go             # Ordered batch writer for subtree data
├── processTxMetaUsingStore.go         # Transaction metadata processing
└── metrics.go                         # Prometheus metrics
```

## 6. How to run

To run the Block Persister Service locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run . -blockpersister=1
```

Please refer to the [Locally Running Services Documentation](../../howto/locallyRunningServices.md) document for more information on running the Block Persister Service locally.

## 7. Configuration options (settings flags)

For comprehensive configuration documentation including all settings, defaults, and interactions, see the [block Persister Settings Reference](../../references/settings/services/blockpersister_settings.md).

## 8. Other Resources

[Block Persister Reference](../../references/services/blockpersister_reference.md)
