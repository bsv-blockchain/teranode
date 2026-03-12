# RPC Service Reference Documentation

## Index

- [Overview](#overview)
- [Types](#types)
    - [RPCServer](#rpcserver)
- [Functions](#functions)
    - [NewServer](#newserver)
- [Methods](#methods)
    - [Start](#start)
    - [Stop](#stop)
    - [Init](#init)
    - [Health](#health)
    - [checkAuth](#checkauth)
    - [jsonRPCRead](#jsonrpcread)
- [RPC Handlers](#rpc-handlers)
- [Configuration](#configuration)
- [Authentication](#authentication)
- [General Format](#general-format)
- [Supported RPC Commands](#supported-rpc-commands)
    - [createrawtransaction](#createrawtransaction) - Creates a raw transaction without signing it
    - [generate](#generate) - Mine blocks (for regression testing)
    - [generatetoaddress](#generatetoaddress) - Mine blocks to a specified address
    - [getbestblockhash](#getbestblockhash) - Returns the hash of the best (most recent) block in the longest blockchain
    - [getblock](#getblock) - Returns information about a block
    - [getblockbyheight](#getblockbyheight) - Returns information about a block at the specified height
    - [getblockhash](#getblockhash) - Returns the hash of a block at the specified height
    - [getblockheader](#getblockheader) - Returns information about a block header
    - [getblockchaininfo](#getblockchaininfo) - Returns blockchain state information
    - [getdifficulty](#getdifficulty) - Returns the proof-of-work difficulty
    - [getinfo](#getinfo) - Returns general information about the node
    - [getmininginfo](#getmininginfo) - Returns mining-related information
    - [getpeerinfo](#getpeerinfo) - Returns data about each connected network node
    - [getrawmempool](#getrawmempool) - Returns transaction IDs being processed for block assembly
    - [getrawtransaction](#getrawtransaction) - Returns raw transaction data
    - [help](#help) - Returns help text for RPC commands
    - [getminingcandidate](#getminingcandidate) - Returns mining candidate information for generating a new block
    - [invalidateblock](#invalidateblock) - Permanently marks a block as invalid
    - [isbanned](#isbanned) - Checks if an IP/subnet is banned
    - [listbanned](#listbanned) - Lists all banned IPs/subnets
    - [clearbanned](#clearbanned) - Clears all banned IPs
    - [reconsiderblock](#reconsiderblock) - Removes invalidity status from a block
    - [sendrawtransaction](#sendrawtransaction) - Submits a raw transaction to the network
    - [setban](#setban) - Attempts to add or remove an IP/subnet from the banned list
    - [stop](#stop) - Stops the server
    - [submitminingsolution](#submitminingsolution) - Submits a mining solution to the network
    - [version](#version) - Returns the server version information
    - [freeze](#freeze) - Freezes specified UTXOs or OUTPUTs
    - [unfreeze](#unfreeze) - Unfreezes specified UTXOs or OUTPUTs
    - [reassign](#reassign) - Reassigns specified frozen UTXOs to a new address
    - [getchaintips](#getchaintips) - Returns information about all known chain tips
- [Unimplemented RPC Commands](#unimplemented-rpc-commands)
- [Error Handling](#error-handling)
- [Connection Limiting](#connection-limiting)
- [Version Compatibility](#version-compatibility)
- [Concurrency](#concurrency)
- [Extensibility](#extensibility)
- [Limitations](#limitations)
- [Security](#security)

## Overview

The RPC Service provides a JSON-RPC interface for interacting with the Bitcoin SV node. It handles various Bitcoin-related commands and manages client connections. The service implements a standard Bitcoin protocol interface while integrating with Teranode's modular architecture to provide high-performance access to blockchain data and network operations.

## Types

### RPCServer

```go
type RPCServer struct {
    // settings contains the configuration parameters for the RPC server including
    // authentication credentials, network binding options, and service parameters
    settings *settings.Settings

    // started indicates whether the server has been started (1) or not (0)
    // This uses atomic operations for thread-safe access
    started int32

    // shutdown indicates whether the server is in the process of shutting down (1) or not (0)
    // This uses atomic operations for thread-safe access
    shutdown int32

    // authsha contains the SHA256 hash of the HTTP basic auth string for admin-level access
    // This is used for authenticating clients with full administrative privileges
    authsha [sha256.Size]byte

    // limitauthsha contains the SHA256 hash of the HTTP basic auth string for limited-level access
    // This is used for authenticating clients with restricted access (read-only operations)
    limitauthsha [sha256.Size]byte

    // numClients tracks the number of connected RPC clients for connection limiting
    // This uses atomic operations for thread-safe access
    numClients int32

    // statusLines maps HTTP status codes to their corresponding status text lines
    // Used for proper HTTP response generation
    statusLines map[int]string

    // statusLock protects concurrent access to the status lines map
    statusLock sync.RWMutex

    // wg is used to wait for all goroutines to exit during shutdown
    wg sync.WaitGroup

    // requestProcessShutdown is closed when an authorized RPC client requests a shutdown
    // This channel is used to notify the main process that a shutdown has been requested
    requestProcessShutdown chan struct{}

    // quit is used to signal the server to shut down
    // All long-running goroutines should monitor this channel for termination signals
    quit chan int

    // logger provides structured logging capabilities for operational and debugging messages
    logger ulogger.Logger

    // rpcMaxClients is the maximum number of concurrent RPC clients allowed
    // This setting helps prevent resource exhaustion from too many simultaneous connections
    rpcMaxClients int

    // rpcQuirks enables backwards-compatible quirks in the RPC server when true
    // This improves compatibility with clients expecting legacy Bitcoin Core behavior
    rpcQuirks bool

    // listeners contains the network listeners for the RPC server
    // Multiple listeners may be active for different IP addresses and ports
    listeners []net.Listener

    // blockchainClient provides access to blockchain data and operations
    // Used for retrieving block information, chain state, and blockchain operations
    blockchainClient blockchain.ClientI

    // blockValidationClient provides access to block validation services
    // Used for validating blocks and triggering revalidation of invalid blocks
    blockValidationClient blockvalidation.Interface

    // blockAssemblyClient provides access to block assembly and mining services
    // Used for mining-related RPC commands like getminingcandidate and generate
    blockAssemblyClient blockassembly.ClientI

    // legacyP2PClient provides access to legacy peer network services
    // Used for peer management and information retrieval
    legacyP2PClient peer.ClientI

    // p2pClient provides access to the P2P network services
    // Used for modern peer management and network operations
    p2pClient p2p.ClientI

    // assetHTTPURL is the URL where assets (e.g., for HTTP UI) are served from
    assetHTTPURL *url.URL

    // helpCacher caches help text for RPC commands to improve performance
    // Prevents regenerating help text for each request
    helpCacher *helpCacher

    // utxoStore provides access to the UTXO (Unspent Transaction Output) database
    // Used for transaction validation and UTXO queries
    utxoStore utxo.Store

    // txStore provides access to the transaction blob store for persisting transactions
    // Used for storing raw transaction data before validation
    txStore blob.Store

    // validatorClient provides access to the transaction validator service
    // Used for synchronous transaction validation in sendrawtransaction RPC
    validatorClient validator.Interface
}
```

The RPCServer provides a concurrent-safe JSON-RPC server implementation for the Bitcoin protocol. It handles client authentication, request processing, response generation, and maintains connections to other Teranode services to fulfill RPC requests.

The server implements a two-tier authentication system that separates administrative capabilities from limited-user operations, providing security through proper authorization. It supports standard Bitcoin Core RPC methods and Bitcoin SV extensions for compatibility with existing tools while enhancing functionality.

The RPCServer is designed for concurrent operation, employing synchronization mechanisms to handle multiple simultaneous client connections without race conditions or resource conflicts. It implements proper connection management, graceful shutdown, and health monitoring.

## Functions

### NewServer

```go
func NewServer(logger ulogger.Logger, tSettings *settings.Settings, blockchainClient blockchain.ClientI, blockValidationClient blockvalidation.Interface, utxoStore utxo.Store, blockAssemblyClient blockassembly.ClientI, legacyPeerClient peer.ClientI, p2pClient p2p.ClientI, txStore blob.Store, validatorClient validator.Interface) (*RPCServer, error)
```

Creates a new instance of the RPC Service with the necessary dependencies including logger, settings, blockchain client, block validation client, UTXO store, transaction store, validator client, and service clients.

This factory function creates a fully configured RPCServer instance, setting up:

- Authentication credentials from settings
- Connection limits and parameters
- Command handlers and help text
- Client connections to required services

**Parameters:**

- `logger`: Structured logger for operational and debug messages
- `tSettings`: Configuration settings for the RPC server and related features
- `blockchainClient`: Interface to the blockchain service for block and chain operations
- `blockValidationClient`: Interface to the block validation service for block validation operations
- `utxoStore`: Interface to the UTXO database for transaction validation
- `blockAssemblyClient`: Interface to the block assembly service for mining operations
- `legacyPeerClient`: Interface to the legacy peer network services
- `p2pClient`: Interface to the P2P network services

The RPC server requires connections to several other Teranode services to function properly, as it primarily serves as an API gateway to underlying node functionality. These dependencies are injected through this constructor to maintain proper service separation and testability.

## Methods

### Start

```go
func (s *RPCServer) Start(ctx context.Context, readyCh chan<- struct{}) error
```

!!! info "Initialization Tasks"
    Starts the RPC server, begins listening for client connections, and signals readiness by closing the readyCh channel once initialization is complete.

    This method performs several critical initialization tasks:

    1. **Validates the server** has not already been started (using atomic operations)
    2. **Initializes network listeners** on all configured interfaces and ports
    3. **Launches goroutines** to accept and process incoming connections
    4. **Sets up proper handling** for clean shutdown
    5. **Signals readiness** through the provided channel

The server supports binding to multiple addresses simultaneously, allowing both IPv4 and IPv6 connections, as well as restricting access to localhost-only if configured for development or testing environments.

### Stop

```go
func (s *RPCServer) Stop(ctx context.Context) error
```

Gracefully stops the RPC server by:

1. **Setting shutdown flag** to prevent new connections
2. **Closing all active listeners** to stop accepting new requests
3. **Waiting for active connections** to complete their current operations
4. **Cleaning up resources** and releasing network ports

This method implements a thread-safe shutdown mechanism using atomic operations to prevent multiple concurrent shutdown attempts. When called, it closes the quit channel to signal all goroutines to terminate, then waits for them to exit using the wait group before returning.

### Init

```go
func (s *RPCServer) Init(ctx context.Context) (err error)
```

Performs second-stage initialization of the RPC server by establishing connections to dependent services that weren't available during initial construction.

!!! note "Initialization Steps"
    This method completes the RPC server initialization by:

    1. **Connecting to the Block Assembly service** for mining-related operations
    2. **Connecting to the P2P service** for network peer management
    3. **Connecting to the Legacy service** for compatibility with older protocols
    4. **Refreshing the help cache** with complete command information

The initialization is designed to be idempotent and can be safely called multiple times, though typically it's only called once after NewServer and before Start.

### Health

```go
func (s *RPCServer) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Reports the operational status of the RPC service for monitoring and health checking.

This method implements the standard Teranode health checking interface used across all services for consistent monitoring, alerting, and orchestration.

!!! success "Health Check Types"
    It provides both readiness and liveness checking capabilities to support different operational scenarios:

    - **Readiness**: Indicates whether the service is ready to accept requests (listeners are bound and core dependencies are available)
    - **Liveness**: Indicates whether the service is functioning correctly (listeners are still working and not in a hung state)

**Health Check Components:**

- **Verifying network listeners** are active
- **Checking connections** to dependent services
- **Validating internal state** consistency

### checkAuth

```go
func (s *RPCServer) checkAuth(r *http.Request, require bool) (bool, bool, error)
```

Implements the two-tier HTTP Basic authentication system for RPC clients. It validates credentials supplied in the HTTP request against configured admin and limited-access username/password combinations.

!!! abstract "Authentication Flow"
    The method implements a secure authentication flow that:

    1. **Extracts the Authorization header** from the HTTP request
    2. **Validates the credentials** against both admin and limited-user authentication strings
    3. **Uses time-constant comparison** operations to prevent timing attacks
    4. **Distinguishes between admin users** (who can perform state-changing operations) and limited users (who can only perform read-only operations)

!!! warning "Security Considerations"

- **Uses SHA256** for credential hashing
- **Implements constant-time comparison** to prevent timing attacks
- **Properly handles missing** or malformed authentication headers
- **Can be configured** to require or not require authentication based on settings

**Returns:**

- `bool`: Authentication success (true if successful)
- `bool`: Authorization level (true for admin access, false for limited access). The value specifies whether the user can change the state of the node.
- `error`: Authentication error if any occurred, nil on success

### jsonRPCRead

```go
func (s *RPCServer) jsonRPCRead(w http.ResponseWriter, r *http.Request, isAdmin bool)
```

Handles reading and responding to RPC messages. This method is the core request processing function.

!!! info "Request Processing Steps"
    1. **Parses incoming JSON-RPC requests** from HTTP request bodies
    2. **Validates request format** and structure
    3. **Routes requests** to appropriate command handlers
    4. **Formats and sends responses** back to clients
    5. **Implements proper error handling** and serialization
    6. **Ensures thread-safety** for concurrent request handling

The method supports batch requests, proper HTTP status codes, and includes safeguards against oversized or malformed requests. It also handles authorization checking to ensure admin-only commands cannot be executed by limited-access users.

## RPC Handlers

The RPC Service implements various handlers for Bitcoin-related commands. All handlers follow a consistent function signature:

```go
func handleCommand(ctx context.Context, s *RPCServer, cmd interface{}, closeChan <-chan struct{}) (interface{}, error)
```

Each handler receives a context for cancellation and tracing, the RPC server instance, parsed command parameters, and a close notification channel. They return a properly formatted response object or an error.

Some key handlers include:

- `handleGetBlock`: Retrieves block information at different verbosity levels
- `handleGetBlockHash`: Gets the hash of a block at a specific height
- `handleGetBestBlockHash`: Retrieves the hash of the best (most recent) block
- `handleCreateRawTransaction`: Creates a raw transaction
- `handleSendRawTransaction`: Broadcasts a raw transaction to the network
- `handleGetMiningCandidate`: Retrieves a candidate block for mining
- `handleSubmitMiningSolution`: Submits a solved block to the network
- `handleFreeze`: Freezes specified UTXOs to prevent spending
- `handleUnfreeze`: Unfreezes previously frozen UTXOs
- `handleReassign`: Reassigns specified frozen UTXOs to a new address
- `handleSetBan`: Adds or removes IP addresses/subnets from the node's ban list
- `handleClearBanned`: Removes all bans from the node
- `handleListBanned`: Lists all currently banned IP addresses and subnets

## Configuration

!!! example "RPC Configuration Settings"
    The RPC Service uses various configuration values:

    **Authentication Settings:**

    - **`rpc_user` and `rpc_pass`**: Credentials for RPC authentication with full admin privileges
    - **`rpc_limit_user` and `rpc_limit_pass`**: Credentials for limited RPC access (read-only operations)

    **Connection Settings:**

    - **`rpc_max_clients`**: Maximum number of concurrent RPC clients (default: 1)
    - **`rpc_listener_url`**: URL for the RPC listener

    **Timeout Settings:**

    - **`rpc_timeout`**: Maximum time allowed for an RPC call to complete (default: 30s)
        - Prevents resource exhaustion from long-running operations
        - Returns error code -30 (ErrRPCTimeout) on timeout
    - **`rpc_client_call_timeout`**: Timeout for internal client calls to other services (default: 5s)
        - Used when RPC handlers call P2P, Legacy peer, or other internal services
        - Prevents hanging when dependent services are unresponsive

    **Performance Settings:**

    - **`rpc_cache_enabled`**: Enables RPC response caching (default: true)

    **Compatibility Settings:**

    - **`rpc_quirks`**: Enables compatibility quirks for legacy clients (default: true)

Configuration values can be provided through the configuration file, environment variables, or command-line flags, with precedence in that order.

## Authentication

!!! info "Authentication Levels"
    The server supports two levels of authentication:

    1. **Admin-level access** with full permissions (configured via `rpc_user` / `rpc_pass`)
    2. **Limited access** with restricted permissions (configured via `rpc_limit_user` / `rpc_limit_pass`)

Authentication is performed using HTTP Basic Auth.

**Credential Provision Methods:**

- **Username and password** in the HTTP header
- **Cookie-based authentication**
- **Configuration file settings**

### Admin vs Limited Access

Admin users can execute all RPC commands. Limited users can execute the following commands:

**Read-only commands:**
createrawtransaction, decoderawtransaction, decodescript, estimatefee, getbestblock, getbestblockhash,
getblock, getblockcount, getblockhash, getblockheader, getcfilter, getcfilterheader, getcurrentnet,
getdifficulty, getheaders, getinfo, getnettotals, getnetworkhashps, getrawmempool, getrawtransaction,
gettxout, gettxoutproof, help, searchrawtransactions, uptime, validateaddress, verifymessage,
verifytxoutproof, version

**State-changing commands (also available to limited users):**
sendrawtransaction, submitblock, getminingcandidate, submitminingsolution, freeze, unfreeze, reassign

**Admin-only commands (not available to limited users):**
generate, generatetoaddress, getblockbyheight, getblockchaininfo, getchaintips, getmininginfo,
getpeerinfo, invalidateblock, reconsiderblock, setban, isbanned, listbanned, clearbanned, stop

### GRPC API Key Authentication

For GRPC services, certain administrative operations require additional API key authentication:

- **Protected Methods**: `BanPeer` and `UnbanPeer` operations in both P2P and Legacy GRPC services require API key authentication
- **Configuration**: Set via `grpc_admin_api_key` setting in the configuration file
- **Auto-generation**: If no API key is configured, the system generates a random 32-byte key at startup
- **Usage**: API key must be included in GRPC requests as metadata with the key `x-api-key`

!!! warning "Security Note"
    The API key provides administrative access to ban/unban operations. Ensure it is properly secured and not exposed in logs or configuration files in production environments.

## General Format

!!! info "Request Requirements"
    All requests should be POST requests with Content-Type: application/json.

**Request format:**

```json
{
    "jsonrpc": "1.0",
    "id": "id",
    "method": "method_name",
    "params": []
}
```

## Supported RPC Commands

The following RPC commands are fully implemented and supported in the current version of the node.

### createrawtransaction

Creates a raw Bitcoin transaction without signing it.

**Parameters:**

1. `inputs` (array, required):

    ```json
    [
      {
        "txid": "hex_string",       // The transaction id
        "vout": n                   // The output number
      }
    ]
    ```

2. `outputs` (object, required):

   ```json
   {
     "address": x.xxx,            // Bitcoin address:amount pairs
     "data": "hex"               // Optional data output
   }
   ```

**Returns:**

- `string` - hex-encoded raw transaction

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "createrawtransaction",
    "params": [
        [{"txid":"1234abcd...", "vout":0}],
        {"1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa": 0.01}
    ]
}
```

**Example Response:**

```json
{
    "result": "0200000001abcd1234...00000000",
    "error": null,
    "id": "curltest"
}
```

### generate

Mines blocks immediately (for testing only).

**Parameters:**

1. `nblocks` (numeric, required) - Number of blocks to generate
2. `maxtries` (numeric, optional) - Maximum number of iterations to try

**Returns:**

- `array` - hashes of blocks generated

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "generate",
    "params": [1, 1000000]
}
```

**Example Response:**

```json
{
    "result": [
        "36252b5852a5921bdfca8701f936b39edeb1f8c39fffe73b0d8437921401f9af"
    ],
    "error": null,
    "id": "curltest"
}
```

### getbestblockhash

Returns the hash of the best (tip) block in the longest blockchain.

**Parameters:** none

**Returns:**

- `string` - The block hash

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "getbestblockhash",
    "params": []
}
```

**Example Response:**

```json
{
    "result": "000000000000000004a1b6d6fdfa0d0a0e52a7a2c8a35ee5b5a7518a846387bc",
    "error": null,
    "id": "curltest"
}
```

### getblock

Returns information about a block.

**Parameters:**

1. `blockhash` (string, required) - The block hash
2. `verbosity` (numeric, optional, default=1) - 0 for hex-encoded data, 1 for a json object, 2 for json object with transaction data

**Returns:**

- If verbosity is 0: `string` - hex-encoded block data
- If verbosity is 1 or 2: `object` - JSON object with block information

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "getblock",
    "params": [
        "000000000000000004a1b6d6fdfa0d0a0e52a7a2c8a35ee5b5a7518a846387bc",
        1
    ]
}
```

**Example Response:**

```json
{
    "result": {
        "hash": "000000000000000004a1b6d6fdfa0d0a0e52a7a2c8a35ee5b5a7518a846387bc",
        "confirmations": 1,
        "size": 1000,
        "height": 1000,
        "version": 1,
        "versionHex": "00000001",
        "merkleroot": "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
        "tx": ["4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"],
        "time": 1231006505,
        "mediantime": 1231006505,
        "nonce": 2083236893,
        "bits": "1d00ffff",
        "difficulty": 1,
        "chainwork": "0000000000000000000000000000000000000000000000000000000100010001",
        "previousblockhash": "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048"
    },
    "error": null,
    "id": "curltest"
}
```

### getblockbyheight

Returns information about a block at the specified height in the blockchain. This is similar to `getblock` but uses a height parameter instead of a hash.

**Parameters:**

1. `height` (numeric, required) - The height of the block
2. `verbosity` (numeric, optional, default=1) - 0 for hex-encoded data, 1 for a json object, 2 for json object with transaction data

**Returns:**

- If verbosity is 0: `string` - hex-encoded block data
- If verbosity is 1 or 2: `object` - JSON object with block information

**Example Request:**

```json
{
   "jsonrpc": "1.0",
   "id": "curltest",
   "method": "getblockbyheight",
   "params": [2]
}
```

**Example Response:**

```json
{
   "result":
    {
       "hash": "000000000000000004a1b6d6fdfa0d0a0e52a7a2c8a35ee5b5a7518a846387bc",
       "height": 2,
       "version": 1,
       "versionHex": "00000001",
       "merkleroot": "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
       "time": 1231006505,
       "nonce": 2083236893,
       "bits": "1d00ffff",
       "difficulty": 1,
       "previousblockhash": "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048",
       "tx": ["4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"],
       "size": 285,
       "confirmations": 750000
    },
   "error": null,
   "id": "curltest"
}
```

### getblockhash

Returns the hash of block at the specified height in the blockchain.

**Parameters:**

1. `height` (numeric, required) - The height of the block

**Returns:**

- `string` - The block hash

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "getblockhash",
    "params": [2]
}
```

**Example Response:**

```json
{
    "result": "000000000000000004a1b6d6fdfa0d0a0e52a7a2c8a35ee5b5a7518a846387bc",
    "error": null,
    "id": "curltest"
}
```

### getblockheader

Returns information about a block header.

**Parameters:**

1. `hash` (string, required) - The block hash
2. `verbose` (boolean, optional, default=true) - true for a json object, false for the hex-encoded data

**Returns:**

- If verbose=false: `string` - hex-encoded block header
- If verbose=true: `object` - JSON object with header information

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "getblockheader",
    "params": [
        "000000000000000004a1b6d6fdfa0d0a0e52a7a2c8a35ee5b5a7518a846387bc",
        true
    ]
}
```

**Example Response:**

```json
{
    "result": {
        "hash": "000000000000000004a1b6d6fdfa0d0a0e52a7a2c8a35ee5b5a7518a846387bc",
        "version": 1,
        "versionHex": "00000001",
        "previousblockhash": "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048",
        "merkleroot": "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
        "time": 1231006505,
        "mediantime": 1231006505,
        "nonce": 2083236893,
        "bits": "1d00ffff",
        "difficulty": 1,
        "chainwork": "0000000000000000000000000000000000000000000000000000000100010001",
        "height": 1000,
        "size": 285,
        "num_tx": 1,
        "status": "active"
    },
    "error": null,
    "id": "curltest"
}
```

### getdifficulty

Returns the proof-of-work difficulty as a multiple of the minimum difficulty.

**Parameters:** none

**Returns:**

- `number` - The current difficulty

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "getdifficulty",
    "params": []
}
```

**Example Response:**

```json
{
    "result": 21448277761059.71,
    "error": null,
    "id": "curltest"
}
```

### getmininginfo

Returns a json object containing mining-related information.

**Parameters:** none

**Returns:**

```json
{
    "blocks": number,           // The current block count
    "currentblocksize": number, // The last block size
    "currentblocktx": number,   // The last block transaction count
    "difficulty": number,       // The current difficulty
    "errors": "string",        // Current errors
    "networkhashps": number,   // The estimated network hashes per second
    "chain": "string"          // Current network name (main, test, regtest)
}
```

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "getmininginfo",
    "params": []
}
```

**Example Response:**

```json
{
    "result": {
        "blocks": 750000,
        "currentblocksize": 1000000,
        "currentblocktx": 2000,
        "difficulty": 21448277761059.71,
        "errors": "",
        "networkhashps": 7.088e+17,
        "chain": "main"
    },
    "error": null,
    "id": "curltest"
}
```

### sendrawtransaction

Submits a raw transaction to the network.

**Parameters:**

1. `hexstring` (string, required) - The hex string of the raw transaction
2. `allowhighfees` (boolean, optional, default=false) - Allow high fees

**Returns:**

- `string` - The transaction hash in hex

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "sendrawtransaction",
    "params": ["0200000001abcd1234...00000000"]
}
```

**Example Response:**

```json
{
    "result": "a1b2c3d4e5f6...",
    "error": null,
    "id": "curltest"
}
```

### getminingcandidate

Returns information for creating a new block.

**Parameters:**

1. `parameters` (object, optional):

   - `provideCoinbaseTx` (boolean, optional, default=false): If true, includes the coinbase transaction in the response
   - `verbosity` (numeric, optional, default=0): 0 for standard response, 1 to include subtree hashes

**Returns:**

```json
{
    "id": "string",                // Mining candidate ID
    "prevhash": "string",          // Previous block hash
    "coinbaseValue": number,       // Coinbase value in satoshis
    "version": number,             // Block version
    "nBits": "string",             // Compressed difficulty target
    "time": number,                // Current timestamp
    "height": number,              // Block height
    "num_tx": number,              // Number of transactions
    "sizeWithoutCoinbase": number, // Block size excluding coinbase
    "merkleProof": ["string"],     // Merkle proof
    "coinbase": "string"           // Coinbase transaction (only if provideCoinbaseTx=true)
}
```

When `verbosity=1`, the response also includes:

- `subtreeHashes` (array of strings): Hashes of subtrees in the block assembly

**Example Request (standard):**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "getminingcandidate",
    "params": []
}
```

**Example Request (with coinbase transaction and subtree hashes):**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "getminingcandidate",
    "params": [{"provideCoinbaseTx": true, "verbosity": 1}]
}
```

### submitminingsolution

Submits a solved block to the network.

**Parameters:**

1. `solution` (object, required):

   ```json
   {
       "id": "string",          // Mining candidate ID
       "nonce": number,         // Block nonce
       "coinbase": "string",    // Coinbase transaction
       "time": number,          // Block time
       "version": number        // Block version
   }
   ```

**Returns:**

- `boolean` - True if accepted, false if rejected

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "submitminingsolution",
    "params": [{
        "id": "100000",
        "nonce": 1234567890,
        "coinbase": "01000000...",
        "time": 1631234567,
        "version": 1
    }]
}
```

### getblockchaininfo

Returns state information about blockchain processing.

**Parameters:** none

**Returns:**

```json
{
    "chain": "string",              // Current network name (main, test, regtest)
    "blocks": number,               // Current number of blocks processed
    "headers": number,              // Current number of headers processed
    "bestblockhash": "string",      // Hash of the currently best block
    "difficulty": number,           // Current difficulty
    "mediantime": number,          // Median time for the current best block
    "verificationprogress": number, // Estimate of verification progress [0..1]
    "chainwork": "string",         // Total amount of work in active chain, in hexadecimal
    "pruned": boolean,             // If the blocks are subject to pruning
    "softforks": [                 // Status of softforks
        {
            "id": "string",        // Name of the softfork
            "version": number,     // Block version that signals this softfork
            "enforce": {           // Progress toward enforcing the softfork rules
                "status": boolean, // True if threshold reached
                "found": number,   // Number of blocks with the new version found
                "required": number,// Number of blocks required
                "window": number   // Maximum size of examined window of recent blocks
            },
            "reject": {           // Progress toward rejecting pre-softfork blocks
                "status": boolean, // True if threshold reached
                "found": number,   // Number of blocks with the new version found
                "required": number,// Number of blocks required
                "window": number   // Maximum size of examined window of recent blocks
            }
        }
    ]
}
```

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "getblockchaininfo",
    "params": []
}
```

**Example Response:**

```json
{
    "result": {
        "chain": "main",
        "blocks": 750000,
        "headers": 750000,
        "bestblockhash": "000000000000000004a1b6d6fdfa0d0a0e52a7a2c8a35ee5b5a7518a846387bc",
        "difficulty": 21448277761059.71,
        "mediantime": 1657123456,
        "verificationprogress": 0.9999987,
        "chainwork": "000000000000000000000000000000000000000000a0ff23f0182b0000000000",
        "pruned": false,
        "softforks": []
    },
    "error": null,
    "id": "curltest"
}
```

### getinfo

Returns general information about the node and blockchain.

**Parameters:** none

**Returns:**

```json
{
    "version": number,          // Server version
    "protocolversion": number,  // Protocol version
    "blocks": number,           // Current number of blocks
    "timeoffset": number,       // Time offset in seconds
    "connections": number,      // Number of connections
    "proxy": "string",         // Proxy used
    "difficulty": number,      // Current difficulty
    "testnet": boolean,       // If using testnet
    "relayfee": number,       // Minimum relay fee
    "errors": "string"        // Current errors
}
```

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "getinfo",
    "params": []
}
```

**Example Response:**

```json
{
    "result": {
        "version": 1000000,
        "protocolversion": 70015,
        "blocks": 750000,
        "timeoffset": 0,
        "connections": 8,
        "proxy": "",
        "difficulty": 21448277761059.71,
        "testnet": false,
        "relayfee": 0.00001000,
        "errors": ""
    },
    "error": null,
    "id": "curltest"
}
```

### getpeerinfo

Returns data about each connected network node.

**Parameters:** none

**Returns:**

- Array of Objects, one per peer:

```json
[
    {
        "id": number,               // Peer index
        "addr": "string",           // IP address and port
        "addrlocal": "string",      // Local address
        "services": "string",       // Services offered (hex)
        "lastsend": number,         // Time in seconds since last send
        "lastrecv": number,         // Time in seconds since last receive
        "bytessent": number,        // Total bytes sent
        "bytesrecv": number,        // Total bytes received
        "conntime": number,         // Connection time in seconds
        "pingtime": number,         // Ping time in seconds
        "version": number,          // Peer version
        "subver": "string",         // Peer subversion string
        "inbound": boolean,         // Inbound (true) or Outbound (false)
        "startingheight": number,   // Starting height when peer connected
        "banscore": number,         // Ban score
        "synced_headers": number,   // Last header we have in common with this peer
        "synced_blocks": number     // Last block we have in common with this peer
    }
]
```

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "getpeerinfo",
    "params": []
}
```

**Example Response:**

```json
{
    "result": [
        {
            "id": 1,
            "addr": "192.168.1.123:8333",
            "addrlocal": "192.168.1.100:8333",
            "services": "000000000000040d",
            "lastsend": 1657123456,
            "lastrecv": 1657123455,
            "bytessent": 123456,
            "bytesrecv": 234567,
            "conntime": 1657120000,
            "pingtime": 0.001,
            "version": 70015,
            "subver": "/Bitcoin SV:1.0.0/",
            "inbound": false,
            "startingheight": 750000,
            "banscore": 0,
            "synced_headers": 750000,
            "synced_blocks": 750000
        }
    ],
    "error": null,
    "id": "curltest"
}
```

### invalidateblock

Permanently marks a block as invalid, as if it violated a consensus rule.

**Parameters:**

1. `blockhash` (string, required) - The hash of the block to mark as invalid

**Returns:**

- `null` on success

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "invalidateblock",
    "params": ["000000000000000004a1b6d6fdfa0d0a0e52a7a2c8a35ee5b5a7518a846387bc"]
}
```

**Example Response:**

```json
{
    "result": null,
    "error": null,
    "id": "curltest"
}
```

### reconsiderblock

Removes invalidity status of a block and its descendants, reconsidering them for activation.

**Important:** This command performs **full block validation** to ensure the block meets all consensus rules before removing the invalid status. The block must pass complete validation including all transaction validations, merkle root checks, and consensus rules.

**Parameters:**

1. `blockhash` (string, required) - The hash of the block to reconsider

**Returns:**

- `null` on success (block passed full validation)
- Error if the block fails validation

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "reconsiderblock",
    "params": ["000000000000000004a1b6d6fdfa0d0a0e52a7a2c8a35ee5b5a7518a846387bc"]
}
```

**Example Response:**

```json
{
    "result": null,
    "error": null,
    "id": "curltest"
}
```

### setban

Attempts to add or remove an IP/Subnet from the banned list.

**Parameters:**

1. `subnet` (string, required) - The IP/Subnet with an optional netmask (default is /32 = single IP)
2. `command` (string, required) - 'add' to add a ban, 'remove' to remove a ban
3. `bantime` (numeric, optional) - Time in seconds how long the ban is in effect, 0 or empty means using the default time of 24h
4. `absolute` (boolean, optional) - If set, the bantime must be an absolute timestamp in seconds since epoch

**Returns:**

- `null` on success

**Note:** This command internally uses GRPC `BanPeer` and `UnbanPeer` methods which require API key authentication. The RPC command handles this authentication automatically.

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "setban",
    "params": ["192.168.0.6", "add", 86400]
}
```

**Example Response:**

```json
{
    "result": null,
    "error": null,
    "id": "curltest"
}
```

### listbanned

Returns list of all banned IP addresses/subnets.

**Parameters:** none

**Returns:**

- Array of objects with banned addresses and details

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "listbanned",
    "params": []
}
```

**Example Response:**

```json
{
    "result": [
        {
            "address": "192.168.0.6/32",
            "ban_created": 1621500000,
            "ban_reason": "manually added",
            "banned_until": 1621586400
        }
    ],
    "error": null,
    "id": "curltest"
}
```

### clearbanned

Removes all IP address bans.

**Parameters:** none

**Returns:**

- `null` on success

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "clearbanned",
    "params": []
}
```

**Example Response:**

```json
{
    "result": null,
    "error": null,
    "id": "curltest"
}
```

### stop

Safely shuts down the node.

**Parameters:** none

**Returns:**

- `string` - A message indicating the node is stopping

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "stop",
    "params": []
}
```

**Example Response:**

```json
{
    "result": "bsvd stopping.",
    "error": null,
    "id": "curltest"
}
```

### version

Returns the server version information.

**Parameters:** none

**Returns:**

```json
{
    "version": "string",      // Server version string
    "subversion": "string",   // User agent string
    "protocolversion": number // Protocol version number
}
```


**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "version",
    "params": []
}
```

**Example Response:**

```json
{
    "result": {
        "version": "1.0.0",
        "subversion": "/Bitcoin SV:1.0.0/",
        "protocolversion": 70015
    },
    "error": null,
    "id": "curltest"
}
```

### isbanned

Checks if a network address is currently banned.

**Parameters:**

1. `address` (string, required) - The network address to check, e.g. "192.168.0.1/24"

**Returns:**

- `boolean` - True if the address is banned, false otherwise

**Note:** This command accesses GRPC ban status methods which require API key authentication when accessed directly. The RPC command handles this authentication automatically.

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "isbanned",
    "params": ["192.168.0.6"]
}
```

**Example Response:**

```json
{
    "result": false,
    "error": null,
    "id": "curltest"
}
```

### freeze

Freezes a specific UTXO, preventing it from being spent.

**Parameters:**

1. `txid` (string, required) - Transaction ID of the output to freeze
2. `vout` (numeric, required) - Output index to freeze

**Returns:**

- `boolean` - True if the UTXO was successfully frozen

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "freeze",
    "params": [
        "a08e6907dbbd3d809776dbfc5d82e371b764ed838b5655e72f463568df1aadf0",
        1
    ]
}
```

**Example Response:**

```json
{
    "result": true,
    "error": null,
    "id": "curltest"
}
```

### unfreeze

Unfreezes a previously frozen UTXO, allowing it to be spent.

**Parameters:**

1. `txid` (string, required) - Transaction ID of the frozen output
2. `vout` (numeric, required) - Output index to unfreeze

**Returns:**

- `boolean` - True if the UTXO was successfully unfrozen

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "unfreeze",
    "params": [
        "a08e6907dbbd3d809776dbfc5d82e371b764ed838b5655e72f463568df1aadf0",
        1
    ]
}
```

**Example Response:**

```json
{
    "result": true,
    "error": null,
    "id": "curltest"
}
```

### reassign

Reassigns ownership of a specific UTXO to a new Bitcoin address.

**Parameters:**

1. `txid` (string, required) - Transaction ID of the output to reassign
2. `vout` (numeric, required) - Output index to reassign
3. `destination` (string, required) - Bitcoin address to reassign the UTXO to

**Returns:**

- `boolean` - True if the UTXO was successfully reassigned

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "reassign",
    "params": [
        "a08e6907dbbd3d809776dbfc5d82e371b764ed838b5655e72f463568df1aadf0",
        1,
        "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"
    ]
}
```

**Example Response:**

```json
{
    "result": true,
    "error": null,
    "id": "curltest"
}
```

### generatetoaddress

Mines blocks immediately to a specified address (for testing only).

**Parameters:**

1. `nblocks` (numeric, required) - Number of blocks to generate
2. `address` (string, required) - The address to send the newly generated bitcoin to
3. `maxtries` (numeric, optional) - Maximum number of iterations to try

**Returns:**

- `array` - hashes of blocks generated

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "generatetoaddress",
    "params": [1, "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1000000]
}
```

**Example Response:**

```json
{
    "result": [
        "36252b5852a5921bdfca8701f936b39edeb1f8c39fffe73b0d8437921401f9af"
    ],
    "error": null,
    "id": "curltest"
}
```

### help

**Note:** This command is registered but not yet implemented. It currently returns a null result. A future release will provide help text for RPC commands.

**Parameters:**

1. `command` (string, optional) - The command to get help for.

**Returns:**

- `null` - The handler is not yet functional.

### getrawmempool

Returns transaction IDs currently being processed for block assembly in Teranode's subtree-based architecture.

**Note**: Unlike traditional Bitcoin nodes, Teranode doesn't use a mempool. This command returns transactions from the subtree-based block assembly system that are being prepared for inclusion in the next block. The method name is kept for Bitcoin Core compatibility.

**Parameters:**

1. `verbose` (boolean, optional, default=false) - If true, returns detailed information about pending transactions

**Returns:**

- If verbose=false: `array` - Array of transaction IDs being processed for block assembly
- If verbose=true: `object` - Detailed information about transactions in the block assembly process

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "getrawmempool",
    "params": [false]
}
```

**Example Response:**

```json
{
    "result": [
        "a08e6907dbbd3d809776dbfc5d82e371b764ed838b5655e72f463568df1aadf0",
        "b19f7805dbcd4e809887ecfd6e93f482c875fe849c6766f83f574679ef2bbef1"
    ],
    "error": null,
    "id": "curltest"
}
```

### getrawtransaction

Returns raw transaction data for a specific transaction.

**Parameters:**

1. `txid` (string, required) - The transaction id
2. `verbose` (boolean, optional, default=false) - If false, returns a string that is serialized, hex-encoded data for the transaction. If true, returns a JSON object with transaction information.

**Returns:**

- If verbose=false: `string` - Serialized, hex-encoded data for the transaction
- If verbose=true: `object` - A JSON object with transaction information

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "getrawtransaction",
    "params": [
        "a08e6907dbbd3d809776dbfc5d82e371b764ed838b5655e72f463568df1aadf0",
        true
    ]
}
```

**Example Response:**

```json
{
    "result": {
        "hex": "0200000001abcd1234...00000000",
        "txid": "a08e6907dbbd3d809776dbfc5d82e371b764ed838b5655e72f463568df1aadf0",
        "hash": "a08e6907dbbd3d809776dbfc5d82e371b764ed838b5655e72f463568df1aadf0",
        "size": 225,
        "version": 2,
        "locktime": 0,
        "vin": [
            {
                "txid": "efgh5678...",
                "vout": 0,
                "scriptSig": {
                    "asm": "...",
                    "hex": "..."
                },
                "sequence": 4294967295
            }
        ],
        "vout": [
            {
                "value": 0.01000000,
                "n": 0,
                "scriptPubKey": {
                    "asm": "OP_DUP OP_HASH160 hash OP_EQUALVERIFY OP_CHECKSIG",
                    "hex": "76a914hash88ac",
                    "reqSigs": 1,
                    "type": "pubkeyhash",
                    "addresses": [
                        "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"
                    ]
                }
            }
        ],
        "blockhash": "0000000000000000000b9d2ec5a352ecba0592946514a92f14319dc2cf8127f0",
        "confirmations": 1024,
        "time": 1570747519,
        "blocktime": 1570747519
    },
    "error": null,
    "id": "curltest"
}
```

### getchaintips

Returns information about all known chain tips in the block tree, including the main chain as well as orphaned branches.

**Parameters:** none

**Returns:**

- `array` - Array of chain tip objects, each containing:

    - `height` (number) - Height of the chain tip
    - `hash` (string) - Block hash of the chain tip
    - `branchlen` (number) - Zero for main chain, otherwise length of branch connecting the tip to the main chain
    - `status` (string) - Status of the chain tip ("active" for main chain, "valid-fork", "valid-headers", "headers-only", "invalid")

**Example Request:**

```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "getchaintips",
    "params": []
}
```

**Example Response:**

```json
{
    "result": [
        {
            "height": 700001,
            "hash": "000000000000000004a1b6d6fdfa0d0a...",
            "branchlen": 0,
            "status": "active"
        },
        {
            "height": 700000,
            "hash": "000000000000000003f2c4e5b8d9a1b2...",
            "branchlen": 1,
            "status": "valid-fork"
        }
    ],
    "error": null,
    "id": "curltest"
}
```

## Unimplemented RPC Commands

Teranode implements a subset of Bitcoin Core's RPC methods. Commands that are recognized but not implemented will return an `ErrRPCUnimplemented` error. Wallet-related commands are not supported as Teranode does not include wallet functionality.

## Error Handling

The RPC service uses a standardized error handling system based on the Bitcoin Core JSON-RPC error codes. All errors returned to clients follow the JSON-RPC 2.0 specification format:

```json
{
  "jsonrpc": "2.0",
  "id": "request-id",
  "error": {
    "code": -32000,
    "message": "Error message"
  }
}
```

Standard error codes include:

- -1: General error during command handling
- -3: Wallet error (wallet functionality not implemented)
- -5: Invalid address or key
- -8: Out of memory or other resource exhaustion
- -20: Database inconsistency or corruption
- -22: Error parsing or validating a block or transaction
- -25: Transaction or block validation error
- -26: Transaction rejected by policy rules
- -27: Transaction already in chain
- -30: RPC timeout - request exceeded configured timeout limit
- -32600: Invalid JSON-RPC request
- -32601: Method not found
- -32602: Invalid parameters
- -32603: Internal JSON-RPC error
- -32700: Parse error (invalid JSON)

Each handler implements specific error checks relevant to its operation and returns appropriately formatted error responses.

Errors are wrapped in `bsvjson.RPCError` structures, providing standardized error codes and messages as per the Bitcoin Core RPC specification.

RPC calls return errors in the following format:

```json
{
  "result": null,
  "error": {
    "code": -32601,
    "message": "Method not found"
  },
  "id": "id"
}
```

Common error codes that may be returned:

| Code    | Message                  | Meaning                                          |
|---------|--------------------------|--------------------------------------------------|
| -1      | RPC_MISC_ERROR          | Standard "catch-all" error                       |
| -3      | RPC_TYPE_ERROR          | Unexpected type was passed as parameter          |
| -5      | RPC_INVALID_ADDRESS     | Invalid address or key                           |
| -8      | RPC_INVALID_PARAMETER   | Invalid, missing or duplicate parameter          |
| -32600  | RPC_INVALID_REQUEST     | JSON request format error                        |
| -32601  | RPC_METHOD_NOT_FOUND    | Method not found                                 |
| -32602  | RPC_INVALID_PARAMS      | Invalid method parameters                        |
| -32603  | RPC_INTERNAL_ERROR      | Internal RPC error                              |
| -32700  | RPC_PARSE_ERROR         | Error parsing JSON request                       |

## Connection Limiting

The RPC interface implements connection limiting to prevent resource exhaustion. Default limits:

- Maximum concurrent connections: 1 (configurable via `rpc_max_clients`)

## Version Compatibility

These RPC commands are compatible with Bitcoin SV Teranode version 1.0.0 and later.

## Concurrency

The RPC Service is designed for high-concurrency operation with multiple simultaneous client connections. Key concurrency features include:

- Thread-safe request processing with proper synchronization
- Atomic operations for tracking client connections
- Connection limits to prevent resource exhaustion
- Wait groups for coordinated shutdown with in-progress requests
- Context-based cancellation for long-running operations
- Non-blocking request handler dispatch
- Proper mutex usage for shared data structures
- Per-request goroutines for parallel processing
- Response caching to reduce contention on frequently accessed data

These mechanisms ensure the RPC service can safely handle many concurrent connections without race conditions or deadlocks, even under high load scenarios.

The server uses goroutines to handle multiple client connections concurrently. It also employs various synchronization primitives (mutexes, atomic operations) to ensure thread-safety.

## Extensibility

The command handling system is designed to be extensible. New RPC commands can be added by implementing new handler functions and registering them in the `rpcHandlers` map.

## Limitations

- The server does not implement wallet functionality. Wallet-related commands are delegated to a separate wallet service.
- Several commands are marked as unimplemented and will return an error if called.
- The UTXO management commands (freeze, unfreeze, reassign) require the node to be properly configured with the necessary capabilities.
- Memory and connection limits are enforced to prevent resource exhaustion.

## Security

The RPC Service implements several security features:

- HTTP Basic authentication with SHA256 credential validation
- Two-tier authentication system separating admin from limited-access operations
- Connection limiting to prevent denial-of-service attacks
- Configurable binding to specific network interfaces
- Proper HTTP request/response handling with appropriate headers
- Input validation on all parameters
- Authorization checking for privileged operations
- Ban management capabilities for malicious IP addresses
- Context timeouts to prevent resource exhaustion
- Secure credential handling to prevent information leakage
- TLS support for encrypted communications (when configured)

## Related Documents

- [RPC Service Topic Documentation](../../topics/services/rpc.md)
- [RPC Settings Reference](../settings/services/rpc_settings.md)
- [Blockchain Reference](blockchain_reference.md)
- [Block Assembly Reference](blockassembly_reference.md)
- [Legacy Service Reference](legacy_reference.md)
- [P2P Service Reference](p2p_reference.md)
