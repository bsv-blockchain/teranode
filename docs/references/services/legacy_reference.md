# Legacy Service Reference Documentation

## Overview

The Legacy Service provides backward compatibility with legacy Bitcoin peer-to-peer networking protocols. It serves as a bridge between modern Teranode services and older network nodes, enabling seamless communication across different protocol versions. The service integrates with multiple core services including blockchain management, validation, and storage systems to ensure consistent network operation.

## Core Components

### Server Structure

The Server implements the peer service interface and manages connections with legacy network nodes. Its main structure includes:

```go
type Server struct {
    // UnimplementedPeerServiceServer is embedded to satisfy the gRPC interface
    peer_api.UnimplementedPeerServiceServer

    // logger provides logging functionality
    logger ulogger.Logger

    // settings contains the configuration settings for the server
    settings *settings.Settings

    // stats tracks server statistics
    stats *gocore.Stat

    // server is the internal server implementation
    server *server

    // lastHash stores the most recent block hash
    lastHash *chainhash.Hash

    // height represents the current blockchain height
    height uint32

    // blockchainClient handles blockchain operations
    // Used for querying blockchain state and submitting new blocks
    blockchainClient blockchain.ClientI

    // validationClient handles transaction validation
    // Used to validate incoming transactions before relay
    validationClient validator.Interface

    // subtreeStore provides storage for merkle subtrees
    // Used in block validation and merkle proof verification
    subtreeStore blob.Store

    // tempStore provides temporary storage
    // Used for ephemeral data storage during processing
    tempStore blob.Store

    // utxoStore manages the UTXO set
    // Used for transaction validation and UTXO queries
    utxoStore utxo.Store

    // subtreeValidation handles merkle subtree validation
    // Used to verify merkle proofs and validate block structure
    subtreeValidation subtreevalidation.Interface

    // blockValidation handles block validation
    // Used to validate incoming blocks before acceptance
    blockValidation blockvalidation.Interface

    // blockAssemblyClient handles block assembly operations
    // Used for mining and block template generation
    blockAssemblyClient *blockassembly.Client
}
```

Each component serves a specific purpose:

- **logger**: Provides structured logging capabilities
- **settings**: Contains configuration settings for the server and its components
- **stats**: Tracks server statistics and performance metrics
- **server**: Handles the internal peer-to-peer connection management
- **lastHash**: Stores the most recent block hash
- **height**: Represents the current blockchain height
- **blockchainClient**: Interface to the blockchain service for operations like querying state and submitting blocks
- **validationClient**: Interface to the transaction validation service
- **subtreeStore**: Blob storage interface for merkle subtree data
- **tempStore**: Temporary blob storage for ephemeral data
- **utxoStore**: Interface to the UTXO (Unspent Transaction Output) database
- **subtreeValidation**: Interface to the subtree validation service
- **blockValidation**: Interface to the block validation service
- **blockAssemblyClient**: Client for the block assembly service (used for mining)

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

This function creates a new server instance with all required dependencies and initializes Prometheus metrics for monitoring. It is the recommended constructor for the legacy server and sets up all necessary internal state required for proper operation. It does not start any network operations or begin listening for connections until the Start method is called.

### Health Management

The server implements comprehensive health checking through the Health method:

```go
func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

This method performs health checks at two levels:

For liveness checks (when checkLiveness is true):

- Verifies basic server operation
- Returns quickly without checking dependencies
- Used to determine if the service should be restarted

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

- Sets up message size limits for the wire protocol
- Determines the server's public IP address for listening
- Creates and configures the internal server implementation
- Sets up initial peer connections from configuration

Init does not start accepting connections or begin network operations. That happens when Start is called.

```go
func (s *Server) Start(ctx context.Context, readyCh chan<- struct{}) error
```

The Start method initializes and starts all components of the legacy service:

- Waits for the blockchain FSM to transition from IDLE state
- Starts the internal peer server to handle P2P connections
- Begins periodic peer statistics logging
- Sets up API key authentication for protected methods
- Launches the gRPC service to handle API requests
- Signals readiness by closing the readyCh channel

Concurrency notes:

- Start launches multiple goroutines for different server components
- Each component runs independently but coordinates via the server state
- The gRPC service runs in the current goroutine and blocks until completion

```go
func (s *Server) Stop(_ context.Context) error
```

The Stop method performs a clean shutdown of all server components:

- Closes all peer connections
- Stops all network listeners
- Shuts down the internal server state
- Releases any allocated resources

### Peer Management

```go
func (s *Server) GetPeerCount(ctx context.Context, _ *emptypb.Empty) (*peer_api.GetPeerCountResponse, error)
```

Returns the total number of connected peers. This method is part of the peer_api.PeerServiceServer gRPC interface.

```go
func (s *Server) GetPeers(ctx context.Context, _ *emptypb.Empty) (*peer_api.GetPeersResponse, error)
```

This method provides detailed information about all connected peers including:

- Peer identification and addressing information
- Connection statistics (bytes sent/received, timing)
- Protocol and version information
- Blockchain synchronization status
- Ban score and whitelisting status

This method is used by monitoring and control systems to observe the state of the peer network.

### Ban Management

```go
func (s *Server) IsBanned(ctx context.Context, peer *peer_api.IsBannedRequest) (*peer_api.IsBannedResponse, error)
```

Checks if a specific IP address or subnet is currently banned.

```go
func (s *Server) ListBanned(ctx context.Context, _ *emptypb.Empty) (*peer_api.ListBannedResponse, error)
```

Returns a list of all currently banned IP addresses and subnets.

```go
func (s *Server) ClearBanned(ctx context.Context, _ *emptypb.Empty) (*peer_api.ClearBannedResponse, error)
```

Removes all entries from the ban list, allowing previously banned addresses to reconnect.

```go
func (s *Server) BanPeer(ctx context.Context, peer *peer_api.BanPeerRequest) (*peer_api.BanPeerResponse, error)
```

Bans a peer from connecting for a specified duration. The peer will be disconnected if currently connected and prevented from reconnecting until the ban expires.

```go
func (s *Server) UnbanPeer(ctx context.Context, peer *peer_api.UnbanPeerRequest) (*peer_api.UnbanPeerResponse, error)
```

Removes a ban on a specific peer, allowing it to reconnect immediately. The request is processed asynchronously through a channel to the internal server component.

### Internal Methods

```go
func (s *Server) banPeer(peerAddr string, until int64) error
```

Internal method that handles the actual banning of a peer by address. This method searches through connected peers to find the matching peer and applies the ban with the specified expiration time.

```go
func (s *Server) logPeerStats(ctx context.Context)
```

Internal method that runs in a background goroutine to periodically log peer statistics and connection information. This provides operational visibility into the peer network status.

## Authentication

The Legacy Service implements an authentication system for its gRPC API:

- Uses the `GRPCAdminAPIKey` setting for protected methods
- Automatically generates a secure 32-byte random API key if none is provided
- Restricts access to sensitive methods (BanPeer, UnbanPeer) through API key authentication
- Protected methods require the API key to be provided in the gRPC metadata

## Configuration

The Legacy Service uses various configuration values from settings, including:

- `settings.Legacy.ListenAddresses`: Addresses to listen on for incoming connections
- `settings.Legacy.GRPCListenAddress`: Address for the gRPC service
- `settings.Legacy.ConnectPeers`: Peer addresses to connect to on startup
- `settings.GRPCAdminAPIKey`: API key for securing administrative gRPC methods
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

These dependencies are injected into the `Server` struct during initialization.

## Related Documents

- [Legacy Topic Guide](../../topics/services/legacy.md)
- [Legacy Settings](../settings/services/legacy_settings.md)
- [Legacy Peer Protobuf Reference](../protobuf_docs/legacyPeerProto.md)
- [Prometheus Metrics](../prometheusMetrics.md)
