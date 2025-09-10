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

7. [Other Resources](#7-other-resources)

## 1. Description

The Teranode Alert Service reintroduces the alert system functionality that was removed from Bitcoin in 2016. This service is designed to enhance control by allowing specific actions on UTXOs and peer management.

The Service features are:

### UTXO Freezing

- Ability to freeze a set of UTXOs at a specific block height + 1.
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

The Alert Service uses the third party `github.com/bitcoin-sv/alert-system` library. This library provides the ability to subscribe to a private P2P network where other BSV nodes participate, and subscribes to topics where Alert related messages are received.
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
    - Uses `github.com/bsv-blockchain/go-bt/v2` for Bitcoin transaction handling.
    - Integrates with `github.com/bitcoin-sv/alert-system` for core alert functionality.

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
SETTINGS_CONTEXT=dev.[YOUR_USERNAME] go run -Alert=1
```

Please refer to the [Locally Running Services Documentation](../../howto/locallyRunningServices.md) document for more information on running the Alert Service locally.

## 6. Configuration options (settings flags)

The Alert Service can be configured using various settings that control its behavior, storage options, and network connectivity. This section provides a comprehensive reference for all configuration options.

### 6.1 Core Alert Service Configuration

- **Alert Store URL (`alert_store`)**: The URL for connecting to the alert system's data store.
    - Type: `string` (converted to `*url.URL` internally)
    - Impact: Determines the database backend and connection parameters
    - Example: `alert_store = sqlite:///alert` or `alert_store = postgres://user:pass@host:5432/database?sslmode=disable`

- **Genesis Keys (`alert_genesis_keys`)**: A pipe-separated list of public keys used for genesis alerts.
    - Type: `[]string`
    - Impact: **Critical** - The service will not start without valid genesis keys
    - Purpose: These keys determine which alerts are valid; only alerts signed by these keys will be processed
    - Example: `alert_genesis_keys = "02a1589f2c8e1a4e7cbf28d4d6b676aa2f30811277883211027950e82a83eb2768 | 03aec1d40f02ac7f6df701ef8f629515812f1bcd949b6aa6c7a8dd778b748b2433"`

- **P2P Private Key (`alert_p2p_private_key`)**: Private key for P2P communication.
    - Type: `string`
    - Impact: Establishes node identity in the P2P network
    - Note: If not provided, a default key will be created in the data directory
    - Example: `alert_p2p_private_key = "08c7fec91e75046d0ac6a2b4edb2daaae34b1e4c3c25a48b1ebdffe5955e33bc"`

- **Protocol ID (`alert_protocol_id`)**: Protocol identifier for the P2P alert network.
    - Type: `string`
    - Default: "/bitcoin/alert-system/1.0.0"
    - Impact: Determines which P2P protocol group the service will join
    - Example: `alert_protocol_id = "/bsv/alert/1.0.0"`

- **Topic Name (`alert_topic_name`)**: P2P topic name for alert propagation.
    - Type: `string`
    - Default: "bitcoin_alert_system"
    - Note: Automatically prefixed with network name if not on mainnet
    - Example: `alert_topic_name = "bitcoin_alert_system"`

- **P2P Port (`alert_p2p_port`)**: Port number for P2P communication.
    - Type: `int`
    - Default: 9908
    - Impact: **Required** - Service will not start without a valid port
    - Example: `alert_p2p_port = 4001`

### 6.2 Data Storage Configuration

The Alert Service supports multiple database backends through the `alert_store` URL:

#### 6.2.1 SQLite

```text
sqlite:///database_name
```

The SQLite database will be stored in the `DataFolder` directory specified in the main Teranode configuration.

- **Parameters**: None
- **Connection Pool Settings (hardcoded)**:

    - Max Idle Connections: 1
    - Max Open Connections: 1
- **Table Prefix**: Uses the database name as prefix

#### 6.2.2 In-Memory SQLite

```text
sqlitememory:///database_name
```

- **Parameters**: None
- **Connection Pool Settings (hardcoded)**:

    - Max Idle Connections: 1
    - Max Open Connections: 1
- **Storage**: In-memory only
- **Table Prefix**: Uses the database name as prefix

#### 6.2.3 PostgreSQL

```text
postgres://username:password@host:port/database?param1=value1&param2=value2
```

- **Parameters**:

    - `sslmode`: SSL mode for the connection (default: `disable`)
- **Connection Pool Settings (hardcoded)**:

    - Max Idle Connections: 2
    - Max Open Connections: 5
    - Max Connection Idle Time: 20 seconds
    - Max Connection Time: 20 seconds
    - Transaction Timeout: 20 seconds
- **Table Prefix**: "alert_system"

#### 6.2.4 MySQL

```text
mysql://username:password@host:port/database?param1=value1&param2=value2
```

- **Parameters**:

    - `sslmode`: SSL mode for the connection (default: `disable`)
- **Connection Pool Settings (hardcoded)**: Same as PostgreSQL
- **Table Prefix**: "alert_system"

### 6.3 Internal Settings and Behaviors

The following settings are hardcoded in the service and cannot be configured externally:

**Core Processing Settings:**

- **Alert Processing Interval**: 5 minutes
    - Controls how frequently alerts are processed

- **Request Logging**: Enabled (true)
    - Controls HTTP request logging

- **Auto Migrate**: Enabled (true)
    - Automatically migrates the database schema on startup

- **Database Table Prefix**: "alert_system" for PostgreSQL/MySQL, database name for SQLite
    - Prefix used for database tables

- **DHT Mode**: "client"
    - Sets the node as a DHT client for peer discovery

- **Peer Discovery Interval**: Uses system default
    - Controls how frequently peers are discovered

**Database Settings:**

- **Table Prefix**: "alert_system" for PostgreSQL/MySQL, database name for SQLite
    - Prefix used for database tables

### 6.4 Service Dependencies

The Alert Service depends on several other Teranode services:

#### Required Dependencies

| Dependency | Purpose | Configuration Impact | Failure Impact |
|------------|---------|---------------------|----------------|
| **Blockchain Client** | Block invalidation, chain state queries | Requires blockchain service to be properly configured and running | `InvalidateBlock` functionality unavailable |
| **UTXO Store** | UTXO freezing, unfreezing, reassignment operations | Requires UTXO store to be initialized and accessible | Core alert functionality (fund management) unavailable |
| **Block Assembly Client** | Mining coordination when alerts affect blocks | Requires block assembly service configuration | Mining-related alert functions unavailable |
| **Peer Client (Legacy)** | Legacy peer management (ban/unban operations) | Requires legacy peer service configuration | Legacy peer banning functionality unavailable |
| **P2P Client** | Modern P2P alert distribution and peer management | Requires P2P service configuration | Alert distribution and modern peer management unavailable |

#### Configuration Interdependencies

1. **Network Configuration**: The Alert service inherits network configuration from global Teranode settings, affecting topic naming and protocol selection
2. **Logging Integration**: Uses the global Teranode logging configuration for consistent log formatting and output
3. **Metrics Integration**: Integrates with Teranode's Prometheus metrics system using global metrics configuration

### 6.5 Environment Variables

All settings can also be configured through environment variables using the following pattern:

```text
TERANODE_ALERT_<SETTING_NAME>
```

#### Standard Environment Variables

The following environment variables are used to configure the Alert Service:

- `TERANODE_ALERT_GENESIS_KEYS`: A pipe-separated list of genesis keys.
- `TERANODE_ALERT_P2P_PRIVATE_KEY`: The P2P private key.
- `TERANODE_ALERT_PROTOCOL_ID`: The P2P protocol identifier.
- `TERANODE_ALERT_STORE`: The database connection URL.
- `TERANODE_ALERT_TOPIC_NAME`: The P2P topic name.

#### Special Environment Variables

The following environment variable is used to configure the P2P port:

- `ALERT_P2P_PORT`: The P2P port (note: different naming pattern).

### 6.6 Security Considerations

#### 6.6.1 Genesis Keys Management

The genesis keys are critical for the security of the alert system. They should be carefully managed:

- Store private keys securely and offline
- Use multiple keys with a threshold signature scheme for increased security
- Rotate keys periodically according to your security policy

#### 6.6.2 Database Security

When using PostgreSQL or MySQL:

- Use strong passwords
- Enable SSL for database connections (`sslmode=require` or stronger)
- Restrict database access to authorized users only
- Consider using a dedicated database user with limited permissions

### 6.7 Configuration Examples

#### Complete Configuration Example

```text
# Alert Service Core Configuration
alert_store = postgres://alert_user:secure_password@db-server:5432/alert_db?sslmode=require
alert_genesis_keys = "02a1589f2c8e1a4e7cbf28d4d6b676aa2f30811277883211027950e82a83eb2768 | 03aec1d40f02ac7f6df701ef8f629515812f1bcd949b6aa6c7a8dd778b748b2433"
alert_p2p_private_key = "08c7fec91e75046d0ac6a2b4edb2daaae34b1e4c3c25a48b1ebdffe5955e33bc"
alert_protocol_id = "/bsv/alert/1.0.0"
alert_topic_name = "bitcoin_alert_system"
alert_p2p_port = 4001
```

#### Development Configuration Example

```text
alert_store = sqlite:///alert
alert_genesis_keys = "02a1589f2c8e1a4e7cbf28d4d6b676aa2f30811277883211027950e82a83eb2768"
alert_p2p_port = 4001
```

### 6.8 Error Handling and Troubleshooting

#### Common Configuration Errors

**Genesis Keys Errors:**

```text
Error: ErrNoGenesisKeys
Cause: No genesis keys provided in configuration
```

**P2P Configuration Errors:**

```text
Error: ErrNoP2PIP
Cause: P2P IP address validation failed (less than 5 characters)

Error: ErrNoP2PPort
Cause: P2P port validation failed (less than 2 characters when converted to string)
```

**Database Connection Errors:**

```text
Error: ErrDatastoreUnsupported
Cause: Unsupported database scheme in alert_store URL
Supported schemes: sqlite://, sqlitememory://, postgres://, mysql://
```

## 7. Other Resources

[Alert Reference](../../references/services/alert_reference.md)
