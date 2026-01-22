# 🚨 Alert Service

## Index

1. [Description](#1-description)
2. [Functionality](#2-functionality)
    - [2.1. Initialization](#21-initialization)
    - [2.2. UTXO Freezing](#22-utxo-freezing)
    - [2.3. UTXO Unfreezing](#23-utxo-unfreezing)
    - [2.4. UTXO Reassignment](#24-utxo-reassignment)
    - [2.5. Block Invalidation](#25-block-invalidation)
3. [Technology](#3-technology)
4. [Directory Structure and Main Files](#4-directory-structure-and-main-files)
5. [How to run](#5-how-to-run)
6. [Configuration](#6-configuration)
7. [Other Resources](#7-other-resources)

## 1. Description

The Teranode Alert Service reintroduces the alert system functionality that was removed from Bitcoin in 2016. This service is designed to enhance control by allowing specific actions on UTXOs and peer management.

The Service features are:

### UTXO Freezing

- Ability to freeze a set of UTXOs at a specified block height.
- Frozen UTXOs are classified as such and attempts to spend them are rejected.

### UTXO Unfreezing

- Capability to unfreeze a set of UTXOs at a specified block height.

### UTXO Reassignment

- Ability to reassign UTXOs to another specified address at a given block height.

### Peer Management

- Ban a peer based on IP address (with optional netmask), cutting off communications.
- Unban a previously banned peer, re-enabling communications.

### Block Invalidation

- Manually invalidate a block based on its hash.
- Retrieve and re-validate all valid transactions from the invalidated block and subsequent blocks.
- Include valid transactions in the next block(s) to be built.
- Start building the new longest honest chain from the block height -1 of the invalidated block.

> **Note**: For information about how the Alert service is initialized during daemon startup and how it interacts with other services, see the [Teranode Daemon Reference](../../references/teranodeDaemonReference.md#service-initialization-flow).

The Alert Service uses the third party `github.com/bsv-blockchain/go-alert-system` library. This library provides the ability to subscribe to a private P2P network where other BSV nodes participate, and subscribes to topics where Alert related messages are received.
Based on the received messages, the Alert Service handles the UTXO freezing, unfreezing, reassignment, block invalidation and peer management operations.

![Alert_Service_Container_Diagram.png](img/Alert_Service_Container_Diagram.png)

The Alert Service interacts with several core components of Teranode:

- **Blockchain Service**: For block invalidation and chain management
- **UTXO Store**: For freezing, unfreezing, and reassigning UTXOs
- **Block Assembly**: For including re-validated transactions after block invalidation

![Alert_Service_Component_Diagram.png](img/Alert_Service_Component_Diagram.png)

Additionally, a P2P private network is used for peer management, allowing the Alert Service to ban and unban peers based on IP addresses.

The following diagram provides a deeper level of detail into the Alert Service's internal components and their interactions:

![alert_detailed_component.svg](img/plantuml/alert/alert_detailed_component.svg)

## 2. Functionality

### 2.1. Initialization

The Alert Service initializes the necessary components and services to start processing alerts.

![alert_init.svg](img/plantuml/alert/alert_init.svg)

1. The Teranode Main function creates a new Alert Service instance, passing necessary dependencies (logger, blockchain client, UTXO store, and block assembly client).

2. The Alert Service initializes the Prometheus metrics.

3. The Main function calls the `Init` method on the Alert Service:

    - The service loads its configuration.
    - It initializes the datastore (database connection). This is a dependency for the alert library, which uses the datastore to store alert data.
    > **Note**: For detailed information about the alert datastore structure, data models, and configuration options, see the [Alert Service Datastore Reference](../../references/services/alert_reference.md).
    - It creates and stores a genesis alert in the database.
    - If enabled, it verifies the RPC connection to the Bitcoin node.

4. After initialization, the Main function calls the `Start` method:

    - The Alert Service creates a new P2P Server instance.
    - It starts the P2P Server.

5. The Alert Service is now fully initialized and running.

### 2.2. UTXO Freezing

![alert_freeze_utxo.svg](img/plantuml/alert/alert_freeze_utxo.svg)

1. The P2P Alert library initiates the process by calling `AddToConsensusBlacklist` with a list of funds to freeze.

2. The Alert Service iterates through each fund:

    - It retrieves the transaction data from the UTXO Store.
    - Calculates the UTXO hash.
    - Calls the UTXO Store to freeze the UTXO.

3. The UTXO Store interacts with the database to mark the UTXO as frozen.

4. Depending on the success of the freeze operation, the Alert Service adds the result to either the processed or notProcessed list.

5. Finally, the Alert Service returns a BlacklistResponse to the P2P network.

### 2.3. UTXO Unfreezing

![alert_unfreeze_utxo.svg](img/plantuml/alert/alert_unfreeze_utxo.svg)

1. The P2P Alert library initiates the process by calling `AddToConsensusBlacklist` with a list of funds to potentially unfreeze.

2. The Alert Service iterates through each fund:

    - It retrieves the transaction data from the UTXO Store.
    - Calculates the UTXO hash.
    - Checks if the fund is eligible for unfreezing by comparing the EnforceAtHeight.Stop with the current block height.

3. If the fund is eligible for unfreezing:

    - The Alert Service calls the UTXO Store to unfreeze the UTXO.
    - The UTXO Store interacts with the database to mark the UTXO as unfrozen.
    - Depending on the success of the unfreeze operation, the Alert Service adds the result to either the processed or notProcessed list.

4. If the fund is not eligible for unfreezing:

    - The Alert Service adds it to the notProcessed list with a reason.

5. Finally, the Alert Service returns a BlacklistResponse to the P2P network.

### 2.4. UTXO Reassignment

![alert_reassign_utxo.svg](img/plantuml/alert/alert_reassign_utxo.svg)

1. The P2P Alert library initiates the process by calling `AddToConfiscationTransactionWhitelist` with a list of transactions.
2. The Alert Service iterates through each transaction:

    - It parses the transaction from the provided hex string.

3. For each input in the transaction:

    - The Alert Service retrieves the parent transaction data from the UTXO Store.
    - It calculates the old UTXO hash based on the parent transaction output.
    - It extracts the public key from the input's unlocking script.
    - It creates a new locking script using the extracted public key.
    - It calculates a new UTXO hash based on the new locking script.

4. The Alert Service calls the UTXO Store to reassign the UTXO:

    - The UTXO Store updates the database to reflect the new UTXO assignment.

5. Depending on the success of the reassignment operation:

    - The Alert Service adds the result to either the processed or notProcessed list.

6. After processing all inputs of all transactions, the Alert Service returns an AddToConfiscationTransactionWhitelistResponse to the P2P network.

### 2.5. Block Invalidation

![alert_block_invalidation.svg](img/plantuml/alert/alert_block_invalidation.svg)

1. The P2P Alert library initiates the process by calling `InvalidateBlock` with the hash of the block to be invalidated.

2. The Alert Service forwards this request to the Blockchain Client.

3. The Blockchain Client interacts with the Blockchain Store to:

    - Mark the specified block and all child blocks as invalid in the database.
    - Set the mined status to false for these blocks.
    - Return the list of invalidated block hashes.

4. The Blockchain Client confirms the invalidation process to the Alert Service.

5. Finally, the Alert Service returns the invalidation result to the P2P network.

## 3. Technology

1. **Go Programming Language:**
    - The Alert service is implemented in Go (Golang).

2. **gRPC and Protocol Buffers:**
    - Uses gRPC for inter-service communication.
    - Protocol Buffers (`.proto` files in `alert_api/`) define the service API and data structures.

3. **Database Technologies:**
    - Supports both SQLite and PostgreSQL:

        - SQLite for development and lightweight deployments.
        - PostgreSQL for production environments.

    - GORM ORM is used for database operations, with a custom logger (`gorm_logger.go`).

4. **gocore Library:**
    - Utilized for managing application configurations.

    - Handles statistics gathering and operational settings.

5. **P2P Networking:**
    - Implements peer-to-peer communication for alert distribution.
    - Uses libp2p library for P2P network stack.
    - Includes custom topic name and protocol ID for Bitcoin alert system.

6. **Prometheus for Metrics:**
    - Metrics collection and reporting implemented in `metrics.go`.
    - Used for monitoring the performance and health of the Alert service.

7. **Bitcoin-specific Libraries:**
    - Uses `github.com/bsv-blockchain/go-bt/v2` for Bitcoin transaction handling.
    - Integrates with `github.com/bsv-blockchain/go-alert-system` for core alert functionality.

## 4. Directory Structure and Main Files

```text
/services/alert/
├── alert_api/
│   ├── alert_api.pb.go
│   │   Description: Auto-generated Go code from the Protocol Buffers definition.
│   │   Purpose: Defines structures and interfaces for the Alert API.
│   │
│   ├── alert_api.proto
│   │   Description: Protocol Buffers definition file for the Alert API.
│   │   Purpose: Defines the service and message structures for the Alert system.
│   │
│   └── alert_api_grpc.pb.go
│       Description: Auto-generated gRPC Go code from the Protocol Buffers definition.
│       Purpose: Provides gRPC server and client implementations for the Alert API.
│
├── gorm_logger.go
│   Description: Custom logger implementation for GORM.
│   Purpose: Provides logging functionality specifically tailored for GORM database operations.
│
├── logger.go
│   Description: Custom logger implementation for the Alert service.
│   Purpose: Defines logging methods and interfaces used throughout the Alert service.
│
├── metrics.go
│   Description: Metrics collection and reporting for the Alert service.
│   Purpose: Initializes and manages Prometheus metrics for monitoring the Alert service.
│
├── node.go
│   Description: Implementation of the Node interface for the Alert system.
│   Purpose: Provides methods for interacting with the blockchain and managing alerts.
│
└── server.go
```

## 5. How to run

To run the Alert Service locally, you can execute the following command:

```shell
SETTINGS_CONTEXT=dev.[YOUR_CONTEXT] go run . -alert=1
```

Please refer to the [Locally Running Services Documentation](../../howto/locallyRunningServices.md) document for more information on running the Alert Service locally.

## 6. Configuration

For comprehensive configuration documentation including all settings, defaults, and interactions, see the [Alert Service Settings Reference](../../references/settings/services/alert_settings.md).

## 7. Other Resources

[Alert Reference](../../references/services/alert_reference.md)
