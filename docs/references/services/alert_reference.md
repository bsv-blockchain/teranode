# Alert Service Reference Documentation

## Types

### Server

```go
type Server struct {
    // UnimplementedAlertAPIServer is embedded for forward compatibility with the alert API
    alert_api.UnimplementedAlertAPIServer

    // logger handles all logging operations
    logger ulogger.Logger

    // settings contains the server configuration settings
    settings *settings.Settings

    // stats tracks server statistics
    stats *gocore.Stat

    // blockchainClient provides access to blockchain operations
    blockchainClient blockchain.ClientI

    // peerClient provides access to peer operations
    peerClient peer.ClientI

    // p2pClient provides access to p2p operations
    p2pClient p2pservice.ClientI

    // utxoStore manages UTXO operations
    utxoStore utxo.Store

    // blockassemblyClient handles block assembly operations
    blockassemblyClient blockassembly.ClientI

    // appConfig contains alert system specific configuration
    appConfig *config.Config

    // p2pServer manages peer-to-peer communication
    p2pServer *p2p.Server
}
```

The `Server` type is the main structure for the Alert Service. It implements the `UnimplementedAlertAPIServer` and contains components for managing alerts, blockchain interactions, and P2P communication.

### Node

```go
type Node struct {
    // logger handles logging operations
    logger ulogger.Logger

    // blockchainClient provides access to blockchain operations
    blockchainClient blockchain.ClientI

    // utxoStore manages UTXO operations
    utxoStore utxo.Store

    // blockassemblyClient handles block assembly operations
    blockassemblyClient blockassembly.ClientI

    // peerClient handles peer operations
    peerClient peer.ClientI

    // p2pClient handles p2p operations
    p2pClient p2p.ClientI

    // settings contains node configuration
    settings *settings.Settings
}
```

The `Node` type represents a node in the network and provides methods for interacting with the blockchain and managing UTXOs.

## Functions

### Server

#### New

```go
func New(logger ulogger.Logger, tSettings *settings.Settings, blockchainClient blockchain.ClientI, utxoStore utxo.Store, blockassemblyClient blockassembly.ClientI, peerClient peer.ClientI, p2pClient p2pservice.ClientI) *Server
```

Creates a new instance of the `Server` with the specified dependencies and initializes Prometheus metrics.

#### Health

```go
func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Performs comprehensive health checks on the Alert Service. If `checkLiveness` is true, only performs basic liveness checks. Otherwise, performs readiness checks on all dependencies:

- BlockchainClient
- FSM (Finite State Machine)
- BlockassemblyClient
- UTXOStore

Returns HTTP status code, details message, and any error encountered.

#### HealthGRPC

```go
func (s *Server) HealthGRPC(ctx context.Context, _ *emptypb.Empty) (*alert_api.HealthResponse, error)
```

Performs a gRPC health check on the Alert Service and returns a structured response with timestamp.

#### Init

```go
func (s *Server) Init(ctx context.Context) (err error)
```

Initializes the Alert Service by loading configuration. Returns a configuration error if initialization fails.

#### Start

```go
func (s *Server) Start(ctx context.Context, readyCh chan<- struct{}) (err error)
```

Starts the Alert Service by:

1. Creating genesis alert in database
2. Verifying RPC connection (unless disabled)
3. Creating and starting P2P server
4. Waiting for shutdown signal

#### Stop

```go
func (s *Server) Stop(ctx context.Context) error
```

Gracefully stops the Alert Service by closing all configurations and shutting down the P2P server.

### Node Methods

#### NewNodeConfig

```go
func NewNodeConfig(logger ulogger.Logger, blockchainClient blockchain.ClientI, utxoStore utxo.Store, blockassemblyClient blockassembly.ClientI, peerClient peer.ClientI, p2pClient p2p.ClientI, tSettings *settings.Settings) config.NodeInterface
```

Creates a new instance of the `Node` with the specified dependencies.

#### BestBlockHash

```go
func (n *Node) BestBlockHash(ctx context.Context) (string, error)
```

Retrieves the hash of the best block in the blockchain.

#### InvalidateBlock

```go
func (n *Node) InvalidateBlock(ctx context.Context, blockHashStr string) error
```

Invalidates a block in the blockchain using its hash.

#### RPC Interface Methods

```go
func (n *Node) GetRPCHost() string
func (n *Node) GetRPCPassword() string
func (n *Node) GetRPCUser() string
```

Interface methods for accessing RPC connection details. Currently return empty strings.

#### Peer Management

```go
func (n *Node) BanPeer(ctx context.Context, peer string) error
func (n *Node) UnbanPeer(ctx context.Context, peer string) error
```

Methods for managing peer bans. BanPeer adds the peer's IP address to the ban list for both p2p and legacy peers for a duration of 100 years. UnbanPeer removes the peer from the ban list.

#### Consensus Management

```go
func (n *Node) AddToConsensusBlacklist(ctx context.Context, funds []models.Fund) (*models.AddToConsensusBlacklistResponse, error)
```

Adds funds to the consensus blacklist, setting specified UTXOs as un-spendable. Supports both freezing and unfreezing based on enforcement height.

```go
func (n *Node) AddToConfiscationTransactionWhitelist(ctx context.Context, txs []models.ConfiscationTransactionDetails) (*models.AddToConfiscationTransactionWhitelistResponse, error)
```

Re-assigns UTXOs to confiscation transactions, allowing them to be spent.

#### Helper Methods

```go
func (n *Node) getAddToConsensusBlacklistResponse(fund models.Fund, err error) models.AddToConsensusBlacklistNotProcessed
```

Creates a standardized response for failed blacklist operations. Formats consistent response objects for UTXOs that could not be blacklisted, including the original fund details and the specific error that prevented processing.

```go
func (n *Node) getAddToConfiscationTransactionWhitelistResponse(txID string, err error) models.AddToConfiscationTransactionWhitelistNotProcessed
```

Creates a standardized response for failed whitelist operations. Formats consistent response objects for confiscation transactions that could not be whitelisted, including the transaction ID and the specific error that prevented processing.

```go
func extractPublicKey(scriptSig []byte) ([]byte, error)
```

Extracts the public key from a P2PKH scriptSig. Parses the unlocking script from a transaction input to extract the public key used for signing. Handles the standard P2PKH script format: [signature] [public key].

## Configuration

The Alert Service uses a configuration structure (`config.Config`) that includes:

### Core Settings

- Alert processing interval (default: 5 minutes)
- Request logging
- Genesis keys (required)

### Datastore Configuration

- Auto-migration settings
- Support for:

    - SQLite (including in-memory)
    - PostgreSQL
    - MySQL

- Connection pooling options
- Debug settings

### P2P Configuration

- IP and port settings
- DHT mode
- Protocol ID (defaults to system default if not specified)
- Topic name (network-dependent: "bitcoin_alert_system_[network]" for non-mainnet)
- Private key management
- Peer discovery interval

### Error Types

Common configuration errors include:

- `ErrNoGenesisKeys`: No genesis keys provided
- `ErrNoP2PIP`: Invalid P2P IP configuration
- `ErrNoP2PPort`: Invalid P2P port configuration
- `ErrDatastoreUnsupported`: Unsupported datastore type

### Health Checks

The service implements comprehensive health checks:

- Liveness check: Basic service health
- Readiness check: Dependency health including:

    - Blockchain client
    - FSM status
    - Blockassembly client
    - UTXO store

The health checks return appropriate HTTP status codes:

- `200 OK`: Service is healthy
- `503 Service Unavailable`: Service or dependencies are unhealthy

## Alert Datastore

### Overview

The Alert Service uses a persistent datastore to manage alert-related data, consensus state, and operational history. The datastore integrates with the `github.com/bsv-blockchain/go-alert-system` library to provide alert message processing, validation, and P2P network communication.

### Supported Database Backends

The alert datastore supports multiple database backends:

- **SQLite** (including in-memory mode)
- **PostgreSQL**
- **MySQL**

Database selection is configured via the `StoreURL` setting in the Alert service configuration.

### Alert Data Models

#### 1. Genesis Alert

**Purpose**: Bootstrap alert created during service initialization

- **Function**: `models.CreateGenesisAlert()` creates this foundational alert
- **Storage**: Stored in the database during startup to establish the alert system baseline
- **Role**: Provides the initial state for the alert consensus system

#### 2. Alert Messages

Core alert data structure containing:

- **Alert ID**: Unique identifier for each alert
- **Alert Type**: Type of alert operation
    - UTXO freeze
    - UTXO unfreeze
    - UTXO reassignment
    - Peer ban/unban
    - Block invalidation
- **Timestamp**: When the alert was created/received
- **Status**: Alert processing status (pending, processed, failed)
- **Payload**: Alert-specific data payload
- **Signatures**: Cryptographic signatures for alert validation

#### 3. UTXO-Related Alert Data

For UTXO freeze, unfreeze, and reassignment operations:

- **UTXO Identifiers**:

    - **Transaction hashes (txid)**
    - **Output indices (vout)**
    - **Block Height**: Target block height for UTXO operations
    - **Operation Type**: Freeze, unfreeze, or reassign
    - **New Address**: For UTXO reassignment operations (destination address)
    - **Execution Status**: Whether the UTXO operation has been applied

#### 4. Peer Management Alert Data

For peer banning and unbanning operations:

- **Peer Addresses**: IP addresses with optional netmasks
- **Ban Duration**: How long peers should remain banned (typically 100 years for permanent bans)
- **Ban Reason**: Textual description of why the peer was banned
- **Ban Status**: Active, expired, or lifted
- **Target Networks**: Both P2P and Legacy peer networks

#### 5. Block Invalidation Alert Data

For block invalidation operations:

- **Block Hash**: Hash of the block to be invalidated
- **Invalidation Reason**: Why the block should be invalidated
- **Recovery Actions**: Instructions for handling transactions from invalidated blocks
- **Re-validation Status**: Status of transaction re-validation process

#### 6. Alert Consensus Data

For alert validation and consensus:

- **Consensus State**: Current consensus status of alerts
- **Validation Results**: Results of alert validation processes
- **Participant Signatures**: Signatures from network participants
- **Consensus Threshold**: Required consensus level for alert execution

#### 7. P2P Network Alert Data

For P2P network communication:

- **Topic Subscriptions**: P2P topics the alert system subscribes to
- **Network Participants**: Other nodes participating in the alert network
- **Message History**: Historical alert messages received from the P2P network
- **Network Status**: Connection status to the private alert P2P network

#### 8. Configuration and State Data

Operational configuration and state:

- **Alert Processing Interval**: How frequently alerts are processed (default: 5 minutes)
- **Network Configuration**: Network-specific settings (mainnet vs testnet)
- **Service State**: Current operational state of the alert service
- **Last Processed**: Timestamps of last processed alerts by type

### Database Configuration

#### Connection Settings

The datastore connection is configured via the `StoreURL` setting:

```
# SQLite example
StoreURL: sqlite://path/to/alert.db

# PostgreSQL example
StoreURL: postgres://user:password@host:port/database?sslmode=disable

# MySQL example
StoreURL: mysql://user:password@host:port/database
```

#### Auto-Migration

The Alert service supports automatic database schema migration:

- **Enabled**: When `Datastore.AutoMigrate` is true
- **Models**: Uses models from the `github.com/bsv-blockchain/go-alert-system` library
- **Safety**: Migrations are applied during service startup

#### SSL Configuration

For PostgreSQL and MySQL connections:

- **SSL Mode**: Configurable via query parameter `sslmode`
- **Default**: `disable` for development, `require` recommended for production
- **Certificates**: Standard SSL certificate configuration supported

### Data Flow and Lifecycle

#### Alert Reception and Storage

1. **Alert Reception**: Alerts received from private P2P network
2. **Validation**: Alert signatures and consensus validation
3. **Storage**: Alert data stored in datastore for persistence
4. **Processing**: Alerts processed according to their type and timing
5. **Execution**: Alert actions executed (UTXO operations, peer bans, etc.)
6. **State Updates**: Alert processing state updated in datastore

#### Data Retention

- **Alert History**: All alert messages are retained for audit purposes
- **Consensus Data**: Consensus validation data is preserved
- **Cleanup**: No automatic cleanup - manual maintenance may be required
