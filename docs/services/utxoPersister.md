# 🔍 UTXO Persister Service

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

## 1. Description

The UTXO Persister primary function is to create and maintain an up-to-date Unspent Transaction Output (UTXO) file set for each block in the blockchain.

To achieve its target, the UTXO Persister uses the output of the Block Persister service, and outputs an updated UTXO set file.

The UTXO set file can be exported and used as an input for initializing the UTXO store in a new Teranode instance.

1. Input Processing:
    - The UTXO Persister works with the output of the Block Persister, which includes:
        - `utxo-additions`: New UTXOs created in a block,
        - `utxo-deletions`: UTXOs spent in a block,
        - `.block`: The block data,
        - `.subtree`: Subtree information for the block.

2. UTXO Set Generation:
    - For each new block fileset detected, the UTXO Persister creates a 'utxo-set' file.
    - This file represents the complete set of unspent transaction outputs up to and including the current block.

3. File Monitoring and Processing:
    - The service continuously monitors the shared storage for new .block files and additional related files.
    - When new files are detected, it triggers the UTXO set creation process.

4. Progress Tracking:
    - The service maintains a 'lastProcessed.dat' file to keep track of the last block height processed.
    - This ensures continuity and allows the service to resume from the correct point after restarts or interruptions.

5. Efficient Data Handling:
    - The service uses optimized data structures and file formats to handle large volumes of UTXO data efficiently.
    - It implements binary encoding for UTXOs and UTXO deletions to minimize storage requirements and improve processing speed.

6. File Management:
    - The service interacts with the storage system to read and write necessary files.


![UTXO_Persister_Service_Container_Diagram.png](img/UTXO_Persister_Service_Container_Diagram.png)

The service interacts with the storage system to read and write necessary files (shared with the Block Persister service), and requests block information from the Blockchain service (or, optionally, directly from the Blockchain store)

![UTXO_Persister_Service_Component_Diagram.png](img/UTXO_Persister_Service_Component_Diagram.png)


## 2. Functionality

### 2.1 Service Initialization


![utxo_persister_initialization.svg](img/plantuml/utxopersister/utxo_persister_initialization.svg)

The Service initialization is simple. It does instantiate a connection (blob store) to the block storage, shared with the block persister. It then creates a connection to either the Blockchain service, or the Blockchain store

After that, the services waits on a loop, monitoring the shared storage for new block files. When a new block file is detected, the service triggers the UTXO set creation process.

### 2.2 Receiving and Processing a new UTXO Set

![utxo_persister_processing_blocks.svg](img/plantuml/utxopersister/utxo_persister_processing_blocks.svg)

1) **The Service detects a new block set of files**, and kicks off the process to create a new UTXO set.


2) **GetUTXODiff()**:

- The server creates a UTXODiff object for the new block.
- The UTXODiff retrieves UTXO additions and deletions from the BlockStore.
- These additions and deletions represent the changes in the UTXO set for this block.
- The UTXODiff processes these changes, preparing them for application to the UTXO set.


3) **CreateUTXOSet()**:

- The server calls this method on the UTXODiff to generate the new UTXO set.
- The UTXODiff retrieves the previous block's UTXO set from the BlockStore.
- It creates a new FileStorer to write the updated UTXO set.

- For each UTXO:
  - If it's in the previous set and not in the deletions, it's written to the new set.
  - If it's in the additions, it's written to the new set.

- The FileStorer is closed, which triggers the writing of the UTXO set file to the BlockStore.



4) **writeLastHeight()**:

- The server updates its record of the last processed height, by writing the new block height to the lastProcessed.dat file.


5) **Trigger next block processing**:

- The server initiates the processing of the next block, continuing the cycle.


## 3. Data Model


1. **Basic Structure:**
   The UTXO set is essentially a collection of all unspent transaction outputs in the blockchain up to a specific block height. Each UTXO represents a piece of cryptocurrency that can be spent in future transactions.


2. **UTXO Components:**
   Each UTXO contains the following information:

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

   - **TxID**: The transaction ID where this UTXO was created (32 bytes)
   - **Index**: The output index in the transaction (4 bytes)
   - **Value**: The amount of cryptocurrency in this UTXO (8 bytes)
   - **Height**: The block height where this UTXO was created (4 bytes)
   - **Script**: The locking script that must be satisfied to spend this UTXO (variable length)
   - **Coinbase**: A flag indicating whether this UTXO is from a coinbase transaction (1 bit, packed with Height)


3. **Binary Encoding:**
   The UTXO is encoded into a binary format for efficient storage and retrieval:
   - 32 bytes: TxID
   - 4 bytes: Index (little-endian)
   - 8 bytes: Value (little-endian)
   - 4 bytes: Height and Coinbase flag (Height << 1 | CoinbaseFlag)
   - 4 bytes: Script length (little-endian)
   - Variable bytes: Script


4. **UTXO Set File**:
   The UTXO set for each block is stored in a file with the extension `utxo-set`. This file contains a series of encoded UTXOs representing all unspent outputs up to that block.


5. **UTXO Diff**:
   The UTXO Persister uses a diff-based approach to update the UTXO set:
   - `utxo-additions`: New UTXOs created in a block
   - `utxo-deletions`: UTXOs spent in a block


6. **UTXO Deletion Model**:
   When a UTXO is spent, it's recorded in the `utxo-deletions` file. The deletion record contains:
   - TxID (32 bytes)
   - Index (4 bytes)


7. **Set Operations**:
   - Creating a new UTXO set involves:
      1. Starting with the previous block's UTXO set
      2. Removing UTXOs listed in the current block's `utxo-deletions`
      3. Adding UTXOs listed in the current block's `utxo-additions`


8. **Persistence:**
   The UTXO set is persisted using a _FileStorer_, which writes the data to a blob store.





## 4. Technology


1. **Programming Language:**
   - Go (Golang): The entire service is written in Go.


2. **Blockchain-specific Libraries:**
   - github.com/libsv/go-bt/v2: A Bitcoin SV library for Go, used for handling Bitcoin transactions and blocks.
   - github.com/bitcoin-sv/ubsv: Custom package for Bitcoin SV operations (likely an internal library).


3. **Storage:**
   - Blob Store: Used for reading block data, subtrees, and UTXO diff files, and for writing UTXO Set files.


4. **Configuration Management:**
   - github.com/ordishs/gocore: Used for configuration management (e.g., reading config values).



## 5. Directory Structure and Main Files

The Block Persister service is located in the `services/utxopersister` directory.

```
./services/utxopersister/
│
├── Server.go
│   Main implementation of the UTXO Persister server. It contains the core logic for
│   initializing the service, handling requests, and coordinating UTXO set updates.
│
├── UTXO.go
│   Defines the UTXO (Unspent Transaction Output) data structure and related methods.
│
├── UTXODeletion.go
│   Implements the logic for UTXO deletions, which occur when UTXOs are spent in a transaction.
│
├── UTXODiff.go
│   Implements the UTXODiff structure, which represents the difference in the UTXO set
│   between two consecutive blocks.
│
└── filestorer/
    │
    └── FileStorer.go
        Implements a custom file storage mechanism, optimized for the specific
        needs of storing and retrieving UTXO data efficiently.
```

## 6. How to run

To run the UTXO Persister Service locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -UTXOPersister=1
```

Please refer to the [Locally Running Services Documentation](../locallyRunningServices.md) document for more information on running the UTXO Persister Service locally.


## 7. Configuration options (settings flags)

The `utxopersister` service utilizes specific `gocore` settings for configuration, each serving a distinct purpose in the service's operation:

1. **blockstore**
- **Purpose:** Defines the URL for the persistence layer storing block, subtree and UTXO files.
- **Usage:** Initializes the blob store component for data storage during the server setup.
