# RPC Server Reference Documentation

## Overview

The RPC Server provides a JSON-RPC interface for interacting with the Bitcoin SV node. It handles various Bitcoin-related commands and manages client connections.

## Types

### RPCServer

```go
type RPCServer struct {
started                int32
shutdown               int32
authsha                [sha256.Size]byte
limitauthsha           [sha256.Size]byte
numClients             int32
statusLines            map[int]string
statusLock             sync.RWMutex
wg                     sync.WaitGroup
requestProcessShutdown chan struct{}
quit                   chan int
logger                 ulogger.Logger
rpcMaxClients          int
rpcQuirks              bool
listeners              []net.Listener
blockchainClient       blockchain.ClientI
blockAssemblyClient    *blockassembly.Client
peerClient             peer.ClientI
chainParams            *chaincfg.Params
}
```

## Functions

### NewServer

```go
func NewServer(logger ulogger.Logger, blockchainClient blockchain.ClientI) (*RPCServer, error)
```

Creates a new instance of the RPC Server.

## Methods

### Start

```go
func (s *RPCServer) Start(ctx context.Context) error
```

Starts the RPC server and begins listening for client connections.

### Stop

```go
func (s *RPCServer) Stop(ctx context.Context) error
```

Stops the RPC server and closes all client connections.

### Init

```go
func (s *RPCServer) Init(ctx context.Context) (err error)
```

Initializes the RPC server, setting up necessary clients and configurations.

### Health

```go
func (s *RPCServer) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Performs health checks on the RPC server and its dependencies.

### checkAuth

```go
func (s *RPCServer) checkAuth(r *http.Request, require bool) (bool, bool, error)
```

Checks the HTTP Basic authentication supplied by a wallet or RPC client.

### jsonRPCRead

```go
func (s *RPCServer) jsonRPCRead(w http.ResponseWriter, r *http.Request, isAdmin bool)
```

Handles reading and responding to RPC messages.

## RPC Handlers

The RPC Server implements various handlers for Bitcoin-related commands. Some key handlers include:

- `handleGetBlock`: Retrieves block information
- `handleGetBlockHash`: Gets the hash of a block at a specific height
- `handleGetBestBlockHash`: Retrieves the hash of the best (most recent) block
- `handleCreateRawTransaction`: Creates a raw transaction
- `handleSendRawTransaction`: Broadcasts a raw transaction to the network
- `handleGetMiningCandidate`: Retrieves a candidate block for mining
- `handleSubmitMiningSolution`: Submits a solved block to the network

## Configuration

The RPC Server uses various configuration values, including:

- `rpc_user` and `rpc_pass`: Credentials for RPC authentication
- `rpc_limit_user` and `rpc_limit_pass`: Credentials for limited RPC access
- `rpc_max_clients`: Maximum number of concurrent RPC clients
- `rpc_quirks`: Enables compatibility quirks for legacy clients
- `rpc_listener_url`: URL for the RPC listener

## Authentication

The server supports two levels of authentication:

1. Admin-level access with full permissions
2. Limited access with restricted permissions

Authentication is performed using HTTP Basic Auth.

## Error Handling

Errors are wrapped in `bsvjson.RPCError` structures, providing standardized error codes and messages as per the Bitcoin Core RPC specification.

## Concurrency

The server uses goroutines to handle multiple client connections concurrently. It also employs various synchronization primitives (mutexes, atomic operations) to ensure thread-safety.

## Extensibility

The command handling system is designed to be extensible. New RPC commands can be added by implementing new handler functions and registering them in the `rpcHandlers` map.

## Limitations

- The server does not implement wallet functionality. Wallet-related commands are delegated to a separate wallet service.
- Some commands are marked as unimplemented and will return an error if called.

## Security

- The server enforces a maximum number of concurrent clients to prevent resource exhaustion.
- It supports TLS for secure communications (when configured).
- Authentication is required for most operations, with a distinction between admin and limited-access users.
