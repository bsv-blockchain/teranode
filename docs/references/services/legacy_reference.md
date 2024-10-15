# Legacy Server Reference Documentation

## Overview

The Legacy Server is a component of a Bitcoin SV implementation that provides backward compatibility with legacy peer-to-peer networking protocols. It integrates with various services such as blockchain, validation, and storage to facilitate communication with older network nodes.

## Types

### Server

```go
type Server struct {
peer_api.UnimplementedPeerServiceServer
logger            ulogger.Logger
stats             *gocore.Stat
server            *server
lastHash          *chainhash.Hash
height            uint32
blockchainClient  blockchain.ClientI
validationClient  validator.Interface
subtreeStore      blob.Store
utxoStore         utxo.Store
subtreeValidation subtreevalidation.Interface
blockValidation   blockvalidation.Interface
}
```

The `Server` struct is the main type for the Legacy Server. It implements the `UnimplementedPeerServiceServer` interface and contains various clients and stores for interacting with the Bitcoin SV network.

## Functions

### New

```go
func New(logger ulogger.Logger,
blockchainClient blockchain.ClientI,
validationClient validator.Interface,
subtreeStore blob.Store,
utxoStore utxo.Store,
subtreeValidation subtreevalidation.Interface,
blockValidation blockvalidation.Interface,
) *Server
```

Creates a new instance of the Legacy Server with the provided dependencies.

## Methods

### Health

```go
func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Performs health checks on the server and its dependencies. It returns an HTTP status code, a description, and an error if any.

### Init

```go
func (s *Server) Init(ctx context.Context) error
```

Initializes the Legacy Server, setting up network parameters and creating the internal server instance.

### GetPeers

```go
func (s *Server) GetPeers(ctx context.Context, _ *emptypb.Empty) (*peer_api.GetPeersResponse, error)
```

Retrieves information about connected peers and returns it in a `GetPeersResponse` struct.

### Start

```go
func (s *Server) Start(ctx context.Context) error
```

Starts the Legacy Server, including FSM state transitions, internal server startup, and gRPC server initialization.

### Stop

```go
func (s *Server) Stop(_ context.Context) error
```

Stops the internal server.

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

These dependencies are injected into the `Server` struct during initialization
