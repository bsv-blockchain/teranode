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
    - [getrawtransaction](#getrawtransaction) - Returns raw transaction data
    - [getminingcandidate](#getminingcandidate) - Returns mining candidate information for generating a new block
    - [help](#help) - Lists all available commands or gets help on a specific command
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
- [Unimplemented RPC Commands](#unimplemented-rpc-commands)
- [Error Handling](#error-handling)
- [Rate Limiting](#rate-limiting)
- [Version Compatibility](#version-compatibility)
- [Concurrency](#concurrency)
- [Extensibility](#extensibility)
- [Limitations](#limitations)
- [Security](#security)


## Overview

The RPC Service provides a JSON-RPC interface for interacting with the Bitcoin SV node. It handles various Bitcoin-related commands and manages client connections.

## Types

### RPCServer

```go
type RPCServer struct {
   settings               *settings.Settings
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
   p2pClient              p2p.ClientI
   assetHTTPURL           *url.URL
   helpCacher             *helpCacher
   utxoStore              utxo.Store
}
```

## Functions

### NewServer

```go
func NewServer(logger ulogger.Logger, tSettings *settings.Settings, blockchainClient blockchain.ClientI, utxoStore utxo.Store) (*RPCServer, error)
```

Creates a new instance of the RPC Service with the necessary dependencies including logger, settings, blockchain client, and UTXO store.

## Methods

### Start

```go
func (s *RPCServer) Start(ctx context.Context, readyCh chan<- struct{}) error
```

Starts the RPC server, begins listening for client connections, and signals readiness by closing the readyCh channel once initialization is complete.

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

The RPC Service implements various handlers for Bitcoin-related commands. Some key handlers include:

- `handleGetBlock`: Retrieves block information
- `handleGetBlockHash`: Gets the hash of a block at a specific height
- `handleGetBestBlockHash`: Retrieves the hash of the best (most recent) block
- `handleCreateRawTransaction`: Creates a raw transaction
- `handleSendRawTransaction`: Broadcasts a raw transaction to the network
- `handleGetMiningCandidate`: Retrieves a candidate block for mining
- `handleSubmitMiningSolution`: Submits a solved block to the network

## Configuration

The RPC Service uses various configuration values, including:

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
        "previoushash": "00000000839a8e6886ab5951d76f411475428afc90947ee320161bbf18eb6048",
        "merkleroot": "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
        "time": 1231006505,
        "nonce": 2083236893,
        "bits": "1d00ffff",
        "difficulty": 1,
        "height": 1000
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

**Parameters:**
1. `blockhash` (string, required) - The hash of the block to reconsider

**Returns:**
- `null` on success

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

### isbanned

Checks if a network address is currently banned.

**Parameters:**
1. `address` (string, required) - The network address to check, e.g. "192.168.0.1/24"

**Returns:**
- `boolean` - True if the address is banned, false otherwise

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

## Unimplemented RPC Commands

The following commands are recognized by the RPC server but are not currently implemented (they would return an ErrRPCUnimplemented error):

- `addnode` - Adds a node to the peer list
- `debuglevel` - Changes the debug level on the fly
- `decoderawtransaction` - Decodes a raw transaction hexadecimal string
- `decodescript` - Decodes a hex-encoded script
- `estimatefee` - Estimates the fee per kilobyte
- `getaddednodeinfo` - Returns information about added nodes
- `getbestblock` - Returns information about best block
- `getblockcount` - Returns the current block count
- `getblocktemplate` - Returns template for block generation
- `getcfilter` - Returns the committed filter for a block
- `getcfilterheader` - Returns the filter header for a filter
- `getconnectioncount` - Returns connection count
- `getcurrentnet` - Returns the current network ID
- `getgenerate` - Returns if the server is generating coins
- `gethashespersec` - Returns hashes per second
- `getheaders` - Returns header information
- `getmempoolinfo` - Returns mempool information (Not in scope for Teranode)
- `getnettotals` - Returns network statistics
- `getnetworkhashps` - Returns estimated network hashes per second
- `getrawmempool` - Returns raw mempool contents (Not in scope for Teranode)
- `gettxout` - Returns unspent transaction output
- `gettxoutproof` - Returns proof that transaction was included in a block
- `node` - Attempts to add or remove a node
- `ping` - Pings the server
- `searchrawtransactions` - Searches for raw transactions
- `setgenerate` - Sets if server will generate coins
- `submitblock` - Submits a block to the network
- `uptime` - Returns how long the server has been running
- `validateaddress` - Validates an address
- `verifychain` - Verifies the blockchain
- `verifymessage` - Verifies a signed message
- `verifytxoutproof` - Verifies proof that transaction was included in a block

### Wallet-Related Commands

The following wallet-related commands are recognized but explicitly not supported in this implementation (they would return an ErrRPCNoWallet error). These commands should be directed to a connected Bitcoin SV wallet service:

- `addmultisigaddress` - Add a multisignature address to the wallet
- `backupwallet` - Safely copies wallet.dat to the specified file
- `createencryptedwallet` - Creates a new encrypted wallet
- `createmultisig` - Creates a multi-signature address and returns the redeem script
- `dumpprivkey` - Reveals the private key corresponding to an address
- `dumpwallet` - Dumps all wallet keys in a human-readable format
- `encryptwallet` - Encrypts the wallet
- `getaccount` - Returns the account associated with an address
- `getaccountaddress` - Returns an address for the given account
- `getaddressesbyaccount` - Returns all addresses for the given account
- `getbalance` - Returns total balance available
- `getnewaddress` - Returns a new Bitcoin address for receiving payments
- `getrawchangeaddress` - Returns a new address for receiving change
- `getreceivedbyaccount` - Returns total amount received for the account
- `getreceivedbyaddress` - Returns amount received by an address
- `gettransaction` - Returns transaction details
- `gettxoutsetinfo` - Returns statistics about the unspent transaction output set
- `getunconfirmedbalance` - Returns the server's unconfirmed balance
- `getwalletinfo` - Returns wallet state information
- `importprivkey` - Adds a private key to the wallet
- `importwallet` - Imports keys from a wallet dump file
- `keypoolrefill` - Fills the keypool
- `listaccounts` - Returns all account names and their balances
- `listaddressgroupings` - Lists groups of addresses which have had their common ownership made public
- `listlockunspent` - Returns list of temporarily unspendable outputs
- `listreceivedbyaccount` - Lists balances by account
- `listreceivedbyaddress` - Lists balances by address
- `listsinceblock` - Returns transactions since the specified block
- `listtransactions` - Returns the most recent transactions
- `listunspent` - Returns an array of unspent transaction outputs
- `lockunspent` - Locks or unlocks specified transaction outputs
- `move` - Moves funds between accounts
- `sendfrom` - Sends an amount from an account to an address
- `sendmany` - Sends multiple amounts to multiple addresses
- `sendtoaddress` - Sends an amount to an address
- `setaccount` - Sets the account associated with an address
- `settxfee` - Sets the transaction fee per kB
- `signmessage` - Signs a message with the private key of an address
- `signrawtransaction` - Creates a raw transaction
- `walletlock` - Removes the encryption key from memory, locking the wallet
- `walletpassphrase` - Stores the wallet decryption key in memory
- `walletpassphrasechange` - Changes the wallet passphrase

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


# Related Documents

- [RPC API Docs](https://bitcoin-sv.github.io/teranode/references/wallet-toolbox/open-rpc/)
