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
6. [Configuration options (settings flags)](#6-configuration-options-settings-flags)
    - [Alert Service Configuration](#alert-service-configuration)
    - [Network Configuration](#network-configuration)
    - [P2P Configuration](#p2p-configuration)
7. [Other Resources](#7-other-resources)


## 1. Description


The Teranode Alert Service reintroduces the alert system functionality that was removed from Bitcoin in 2016. This service is designed to enhance control by allowing specific actions on UTXOs and peer management.

The Service features are:


**UTXO Freezing**

* Ability to freeze a set of UTXOs at a specific block height + 1.
* Frozen UTXOs are classified as such and attempts to spend them are rejected.


**UTXO Unfreezing**

* Capability to unfreeze a set of UTXOs at a specified block height.


**UTXO Reassignment**

* Ability to reassign UTXOs to another specified address at a given block height.


**Peer Management**

* Ban a peer based on IP address (with optional netmask), cutting off communications.
* Unban a previously banned peer, re-enabling communications.


**Block Invalidation**

* Manually invalidate a block based on its hash.
* Retrieve and re-validate all valid transactions from the invalidated block and subsequent blocks.
* Include valid transactions in the next block(s) to be built.
* Start building the new longest honest chain from the block height -1 of the invalidated block.

> **Note**: For information about how the Alert service is initialized during daemon startup and how it interacts with other services, see the [Teranode Daemon Reference](../../references/teranodeDaemonReference.md#service-initialization-flow).

The Alert Service uses the third party `github.com/bitcoin-sv/alert-system` library. This library provides the ability to subscribe to a private P2P network where other BSV nodes participate, and subscribes to topics where Alert related messages are received.
Based on the received messages, the Alert Service handles the UTXO freezing, unfreezing, reassignment, block invalidation and peer management operations.

![Alert_Service_Container_Diagram.png](img/Alert_Service_Container_Diagram.png)

The Alert Service interacts with several core components of Teranode:

**Blockchain Service**: For block invalidation and chain management.
**UTXO Store**: For freezing, unfreezing, and reassigning UTXOs.
**Block Assembly**: For including re-validated transactions after block invalidation.

![Alert_Service_Component_Diagram.png](img/Alert_Service_Component_Diagram.png)

Additionally, a P2P private network is used for peer management, allowing the Alert Service to ban and unban peers based on IP addresses.

## 2. Functionality

### 2.1. Initialization

The Alert Service initializes the necessary components and services to start processing alerts.

![alert_init.svg](img/plantuml/alert/alert_init.svg)


1. The Teranode Main function creates a new Alert Service instance, passing necessary dependencies (logger, blockchain client, UTXO store, and block assembly client).

2. The Alert Service initializes the Prometheus metrics.

3. The Main function calls the `Init` method on the Alert Service:
    - The service loads its configuration.
    - It initializes the datastore (database connection). This is a dependency for the alert library, which uses the datastore to store alert data.
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
    - Mark the specified block as invalid.
    - Retrieve all transactions from the invalidated block.

4. For each transaction in the invalidated block:
    - The Blockchain Client re-validates the transaction.
    - If the transaction is still valid, it's added back to the Block Assembly service, for re-inclusion in the next mined block.

5. The Blockchain Client then:
    - Retrieves the block immediately preceding the invalidated block.
    - Sets the chain tip to this previous block, effectively removing the invalidated block from the main chain.

6. The Blockchain Client confirms the invalidation process to the Alert Service.

7. Finally, the Alert Service returns the invalidation result to the P2P network.


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
    - Uses `github.com/libsv/go-bt/v2` for Bitcoin transaction handling.
    - Integrates with `github.com/bitcoin-sv/alert-system` for core alert functionality.

## 4. Directory Structure and Main Files

```
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
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -Alert=1
```

Please refer to the [Locally Running Services Documentation](../../howto/locallyRunningServices.md) document for more information on running the Alert Service locally.


## 6. Configuration options (settings flags)

This service uses several `gocore` configuration settings. Here's a list of these settings:

### Alert Service Configuration
- **Alert Store URL (`alert_store`)**: The URL for connecting to the alert system's data store. Default is "sqlite:///alert". Can be set to a PostgreSQL URL for production use.
  Example: `alert_store = sqlite:///alert`


- **Alert Store Operator URL (`alert_store.operator`)**: The URL for the operator's database connection, typically a PostgreSQL database.
  Example: `alert_store.operator = postgres://teranode:teranode@server:5432/alert`


- **Genesis Keys (`alert_genesis_keys`)**: A pipe-separated list of public keys used for genesis alerts. These keys are crucial for the initial setup and security of the alert system.
  Example: `alert_genesis_keys = "02a1589f2c8e1a4e7cbf28d4d6b676aa2f30811277883211027950e82a83eb2768 | 03aec1d40f02ac7f6df701ef8f629515812f1bcd949b6aa6c7a8dd778b748b2433 | 03ddb2806f3cc48aa36bd4aea6b9f1c7ed3ffc8b9302b198ca963f15beff123678 | 036846e3e8f4f944af644b6a6c6243889dd90d7b6c3593abb9ccf2acb8c9e606e2 | 03e45c9dd2b34829c1d27c8b5d16917dd0dc2c88fa0d7bad7bffb9b542229a9304"`


- **P2P Private Key (`alert_p2p_private_key`)**: The private key used for P2P communications in the alert system. This should be kept secure and not shared.
  Example: `alert_p2p_private_key = "e76c77795b43d2aacd564648bffebde74a4c31540357dad4a3694a561b4c4f1fbb0ba060a3015f7f367742500ef8486707e58032af1b4dfdb1203c790bcf2526"`


- **P2P Topic Name (`alert_topic_name`)**: The topic name used for P2P communications. Default is "bitcoin_alert_system".
  Example: `alert_topic_name = "bitcoin_alert_system"`


- **P2P Protocol ID (`alert_protocol_id`)**: The protocol ID used for P2P communications.
  Example: `alert_protocol_id = "/bitcoin/alert-system/1.0.0"`


### Network Configuration
- **Network Type (`network`)**: Specifies the network type (e.g., "mainnet", "testnet"). This affects the P2P topic name for non-mainnet networks.
  Example: `network = "mainnet"`


### P2P Configuration
- **P2P Port (`ALERT_P2P_PORT`)**: The port used for P2P communications in the alert system. Default is 9908.
  Example: `ALERT_P2P_PORT = 9908`



## 7. Other Resources

[Alert Reference](../../references/services/alert_reference.md)
