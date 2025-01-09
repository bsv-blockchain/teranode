# Legacy Server Reference Documentation

## Overview

The Legacy Server provides backward compatibility with legacy Bitcoin peer-to-peer networking protocols. It serves as a bridge between modern Teranode services and older network nodes, enabling seamless communication across different protocol versions. The server integrates with multiple core services including blockchain management, validation, and storage systems to ensure consistent network operation.

## Core Components

### Server Structure

The Server implements the peer service interface and manages connections with legacy network nodes. Its main structure includes:

```go
type Server struct {
    peer_api.UnimplementedPeerServiceServer
    logger               ulogger.Logger
    settings            *settings.Settings
    stats               *gocore.Stat
    server              *server
    lastHash            *chainhash.Hash
    height              uint32
    blockchainClient    blockchain.ClientI
    validationClient    validator.Interface
    subtreeStore        blob.Store
    tempStore           blob.Store
    utxoStore           utxo.Store
    subtreeValidation   subtreevalidation.Interface
    blockValidation     blockvalidation.Interface
    blockAssemblyClient *blockassembly.Client
}
```

Each component serves a specific purpose:
- The logger provides structured logging capabilities
- The settings manager controls server configuration
- The stats collector monitors performance metrics
- The server component handles peer connections
- Various clients interface with other Teranode services
- Store interfaces manage different types of blockchain data

## Server Operations

### Initialization

The server initializes through the New function:

```go
func New(logger ulogger.Logger,
    tSettings *settings.Settings,
    blockchainClient blockchain.ClientI,
    validationClient validator.Interface,
    subtreeStore blob.Store,
    tempStore blob.Store,
    utxoStore utxo.Store,
    subtreeValidation subtreevalidation.Interface,
    blockValidation blockvalidation.Interface,
    blockAssemblyClient *blockassembly.Client,
) *Server
```

This function creates a new server instance with all required dependencies and initializes Prometheus metrics for monitoring.

### Health Management

The server implements comprehensive health checking through two main methods:

```go
func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

This method performs health checks at two levels:

For liveness checks (when checkLiveness is true):
- Verifies basic server operation
- Returns quickly without checking dependencies
- Used for basic operational status

For readiness checks (when checkLiveness is false):
- Verifies BlockchainClient status and FSM state
- Checks ValidationClient functionality
- Confirms SubtreeStore availability
- Validates UTXOStore operation
- Verifies SubtreeValidation service
- Checks BlockValidation service
- Confirms BlockAssembly client status

### Server Lifecycle Management

The server lifecycle is managed through several key methods:

```go
func (s *Server) Init(ctx context.Context) error
```

The Init method performs essential setup:
- Sets network wire protocol limits (4000000000)
- Determines the server's public IP address
- Configures listen addresses (default: public IP:8333)
- Establishes required HTTP endpoints
- Initializes internal server components
- Sets up initial peer connections

```go
func (s *Server) Start(ctx context.Context) error
```

The Start method initializes server operation:
- Signals FSM to transition to LegacySync state
- Starts the internal server for peer connections
- Initializes the gRPC server for the peer service
- Begins processing network messages

```go
func (s *Server) Stop(_ context.Context) error
```

The Stop method ensures graceful shutdown by stopping the internal server.

### Peer Management

```go
func (s *Server) GetPeers(ctx context.Context, _ *emptypb.Empty) (*peer_api.GetPeersResponse, error)
```

This method provides detailed information about connected peers including:
- Peer identification (ID, addresses)
- Connection statistics (bytes sent/received)
- Timing information (last send/receive times)
- Protocol details (version, services)
- Node status (starting height, current height)
- Connection quality metrics (ban score, whitelist status)

## Configuration

The Legacy Server uses various configuration values from `gocore.Config()`, including:

- `network`: The Bitcoin network to connect to (default: "mainnet")
- `legacy_listen_addresses`: Addresses to listen on for incoming connections
- `asset_httpAddress`: HTTP address for the asset service
- `legacy_connect_peers`: Peers to connect to on startup
- `fsm_state_restore`: Whether to restore the FSM state on startup

## Dependencies

The Legacy Server depends on several components:

- `blockchain.ClientI`: Interface for blockchain operations
- `validator.Interface`: Interface for validation operations
- `blob.Store`: Store for subtree data
- `utxo.Store`: Store for UTXO data
- `subtreevalidation.Interface`: Interface for subtree validation
- `blockvalidation.Interface`: Interface for block validation
- `blockassembly.Client`: Client for block assembly

These dependencies are injected into the `Server` struct during initialization
