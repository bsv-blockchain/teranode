# RPC Server Reference Documentation

## Overview

The RPC Server provides a JSON-RPC interface for interacting with the Bitcoin SV node. It handles various Bitcoin-related commands and manages client connections.

## Types

### RPCServer

```go
type RPCServer struct {
   settings               *settings.Settings
   started                int32
   shutdown               int32
   authsha                [sha256.Size]byte
   limitauthsha          [sha256.Size]byte
   numClients            int32
   statusLines           map[int]string
   statusLock            sync.RWMutex
   wg                    sync.WaitGroup
   requestProcessShutdown chan struct{}
   quit                  chan int
   logger                ulogger.Logger
   rpcMaxClients         int
   rpcQuirks             bool
   listeners             []net.Listener
   blockchainClient      blockchain.ClientI
   blockAssemblyClient   *blockassembly.Client
   peerClient            peer.ClientI
   p2pClient             p2p.ClientI
   assetHTTPURL          *url.URL
   helpCacher            *helpCacher
}
```

## Functions

### NewServer

```go
func NewServer(logger ulogger.Logger, tSettings *settings.Settings, blockchainClient blockchain.ClientI) (*RPCServer, error)
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

Provide credentials using:
- Username and password in the HTTP header
- Cookie-based authentication
- Configuration file settings


## General Format

All requests should be POST requests with Content-Type: application/json.

Request format:
```json
{
    "jsonrpc": "1.0",
    "id": "id",
    "method": "method_name",
    "params": []
}
```

## Supported RPC Commands

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

Returns data needed to construct a block to work on.

**Parameters:** none

**Returns:**
```json
{
    "id": "string",              // Mining candidate ID
    "prevhash": "string",        // Previous block hash
    "coinbase": "string",        // Coinbase transaction
    "coinbaseValue": number,     // Value of the coinbase transaction in satoshis
    "version": number,           // Block version
    "nBits": "string",          // Compressed difficulty target
    "time": number,             // Current timestamp
    "height": number,           // Block height
    "num_tx": number,           // Number of transactions
    "merkleProof": ["string"],  // Merkle proof
    "merkleRoot": "string"      // Merkle root
}
```

**Example Request:**
```json
{
    "jsonrpc": "1.0",
    "id": "curltest",
    "method": "getminingcandidate",
    "params": []
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
    "result": "Bitcoin server stopping",
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


## Error Handling

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



## Rate Limiting

The RPC interface implements rate limiting to prevent abuse. Default limits:
- Maximum concurrent connections: 16
- Maximum requests per minute: 60

## Version Compatibility

These RPC commands are compatible with Bitcoin SV Teranode version 1.0.0 and later.

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
