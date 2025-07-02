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
    blockassemblyClient *blockassembly.Client

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
    blockassemblyClient *blockassembly.Client

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
func New(logger ulogger.Logger, tSettings *settings.Settings, blockchainClient blockchain.ClientI, utxoStore utxo.Store, blockassemblyClient *blockassembly.Client, peerClient peer.ClientI, p2pClient p2pservice.ClientI) *Server
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
func NewNodeConfig(logger ulogger.Logger, blockchainClient blockchain.ClientI, utxoStore utxo.Store, blockassemblyClient *blockassembly.Client, peerClient peer.ClientI, p2pClient p2p.ClientI, tSettings *settings.Settings) config.NodeInterface
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
func (n *Node) AddToConsensusBlacklist(ctx context.Context, funds []models.Fund) (*models.BlacklistResponse, error)
```

Adds funds to the consensus blacklist, setting specified UTXOs as un-spendable. Supports both freezing and unfreezing based on enforcement height.

```go
func (n *Node) AddToConfiscationTransactionWhitelist(ctx context.Context, txs []models.ConfiscationTransactionDetails) (*models.AddToConfiscationTransactionWhitelistResponse, error)
```

Re-assigns UTXOs to confiscation transactions, allowing them to be spent.

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
