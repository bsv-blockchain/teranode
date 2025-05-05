# Legacy Service Reference Documentation

## Overview

The Legacy Service provides backward compatibility with legacy Bitcoin peer-to-peer networking protocols. It serves as a bridge between modern Teranode services and older network nodes, enabling seamless communication across different protocol versions. The service integrates with multiple core services including blockchain management, validation, and storage systems to ensure consistent network operation.

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

The server implements comprehensive health checking through the Health method:

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
- Checks peer connections to ensure at least one peer is connected
- Verifies recent peer activity within the last 2 minutes

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
func (s *Server) Start(ctx context.Context, readyCh chan<- struct{}) error
```

The Start method initializes server operation:
- Accepts a ready channel that signals when the server is ready
- Waits for FSM transition from IDLE state
- Starts the internal server for peer connections
- Initiates periodic peer statistics logging
- Initializes the gRPC server for the peer service
- Signals readiness by closing the readyCh channel

```go
func (s *Server) Stop(_ context.Context) error
```

The Stop method ensures graceful shutdown by stopping the internal server.

### Peer Management

```go
func (s *Server) GetPeerCount(ctx context.Context, _ *emptypb.Empty) (*peer_api.GetPeerCountResponse, error)
```

Returns the count of currently connected peers.

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

### Ban Management

```go
func (s *Server) IsBanned(ctx context.Context, peer *peer_api.IsBannedRequest) (*peer_api.IsBannedResponse, error)
```

Checks if a specific peer is currently banned.

```go
func (s *Server) ListBanned(ctx context.Context, _ *emptypb.Empty) (*peer_api.ListBannedResponse, error)
```

Returns a list of all currently banned peers.

```go
func (s *Server) ClearBanned(ctx context.Context, _ *emptypb.Empty) (*peer_api.ClearBannedResponse, error)
```

Removes all entries from the ban list.

```go
func (s *Server) BanPeer(ctx context.Context, peer *peer_api.BanPeerRequest) (*peer_api.BanPeerResponse, error)
```

Bans a specific peer for a specified duration.

```go
func (s *Server) UnbanPeer(ctx context.Context, peer *peer_api.UnbanPeerRequest) (*peer_api.UnbanPeerResponse, error)
```

Removes a specific peer from the ban list.

## Configuration

The Legacy Service uses various configuration values from `gocore.Config()`, including:

- `settings.Legacy.ListenAddresses`: Addresses to listen on for incoming connections
- `settings.Legacy.GRPCListenAddress`: Address for the gRPC service
- `settings.Legacy.ConnectPeers`: Peer addresses to connect to on startup
- `settings.Asset.HTTPAddress`: HTTP address for the asset service

## Dependencies

The Legacy Service depends on several components:

- `blockchain.ClientI`: Interface for blockchain operations
- `validator.Interface`: Interface for validation operations
- `blob.Store`: Store for subtree data
- `utxo.Store`: Store for UTXO data
- `subtreevalidation.Interface`: Interface for subtree validation
- `blockvalidation.Interface`: Interface for block validation
- `blockassembly.Client`: Client for block assembly

These dependencies are injected into the `Server` struct during initialization
