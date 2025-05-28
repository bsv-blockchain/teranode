# How to Interact with the Asset Server

Last Modified: 4-May-2025

There are 2 primary ways to interact with the node, using the RPC Server, and using the Asset Server. This document will focus on the Asset Server. The Asset Server provides an HTTP API for interacting with the node. Below is a list of implemented endpoints with their parameters and return values.

## Teranode Asset Server HTTP API

The Teranode Asset Server provides the following HTTP endpoints organized by category. Unless otherwise specified, all endpoints are GET requests.

Base URL: `/api/v1` (configurable)

Port: `8090` (configurable)

### Response Formats

Many endpoints support multiple response formats, indicated by the URL path or an optional format parameter:

- **Default/Binary**: Returns raw binary data (Content-Type: `application/octet-stream`)
- **Hex**: Returns hexadecimal string (Content-Type: `text/plain`)
- **JSON**: Returns structured JSON (Content-Type: `application/json`)

### Health and Status Endpoints

- GET `/alive`
  - Description: Returns the service status and uptime
  - Parameters: None
  - Returns: Text message with uptime information
  - Status Code: 200 on success

- GET `/health`
  - Description: Performs a health check on the service and its dependencies
  - Parameters: None
  - Returns: Status information of service and dependencies
  - Status Code: 200 on success, 503 on failure

### Transaction Endpoints

- GET `/api/v1/tx/:hash`
  - Description: Retrieves a transaction in binary stream format
  - Parameters:
    - `hash`: Transaction hash (hex string)
  - Returns: Transaction data in binary format

- GET `/api/v1/tx/:hash/hex`
  - Description: Retrieves a transaction in hexadecimal format
  - Parameters:
    - `hash`: Transaction hash (hex string)
  - Returns: Transaction data as hex string

- GET `/api/v1/tx/:hash/json`
  - Description: Retrieves a transaction in JSON format
  - Parameters:
    - `hash`: Transaction hash (hex string)
  - Returns: Transaction data in structured JSON format

- POST `/api/v1/subtree/:hash/txs`
  - Description: Batch retrieves multiple transactions
  - Request Body: Concatenated series of 32-byte transaction hashes without separators
    - Format: `[32-byte hash][32-byte hash][32-byte hash]...`
  - Returns: Concatenated transactions in binary format

- GET `/api/v1/txmeta/:hash/json`
  - Description: Retrieves transaction metadata
  - Parameters:
    - `hash`: Transaction hash (hex string)
  - Returns: Transaction metadata in JSON format

- GET `/api/v1/txmeta_raw/:hash`
  - Description: Retrieves raw transaction metadata
  - Parameters:
    - `hash`: Transaction hash (hex string)
  - Returns: Raw transaction metadata (binary)

- GET `/api/v1/txmeta_raw/:hash/hex`
  - Description: Retrieves raw transaction metadata in hex format
  - Parameters:
    - `hash`: Transaction hash (hex string)
  - Returns: Raw transaction metadata as hex string

- GET `/api/v1/txmeta_raw/:hash/json`
  - Description: Retrieves raw transaction metadata in JSON format
  - Parameters:
    - `hash`: Transaction hash (hex string)
  - Returns: Raw transaction metadata in JSON format

### Block Endpoints

- GET `/api/v1/block/:hash`
  - Description: Retrieves a block by hash in binary format
  - Parameters:
    - `hash`: Block hash (hex string)
  - Returns: Block data in binary format

- GET `/api/v1/block/:hash/hex`
  - Description: Retrieves a block by hash in hexadecimal format
  - Parameters:
    - `hash`: Block hash (hex string)
  - Returns: Block data as hex string

- GET `/api/v1/block/:hash/json`
  - Description: Retrieves a block by hash in JSON format
  - Parameters:
    - `hash`: Block hash (hex string)
  - Returns: Block data in structured JSON format

- GET `/api/v1/block/:hash/forks`
  - Description: Retrieves fork information for a block
  - Parameters:
    - `hash`: Block hash (hex string)
  - Returns: JSON object with fork data

- GET `/api/v1/blocks`
  - Description: Retrieves a paginated list of blocks
  - Parameters:
    - `offset` (optional): Page offset
    - `limit` (optional): Page size limit
  - Returns: JSON array of block data

- GET `/api/v1/blocks/:hash`
  - Description: Retrieves multiple blocks starting with the specified hash
  - Parameters:
    - `hash`: Starting block hash (hex string)
    - `n` (optional): Number of blocks to retrieve
  - Returns: Block data in binary format

- GET `/api/v1/blocks/:hash/hex`
  - Description: Same as above but in hexadecimal format

- GET `/api/v1/blocks/:hash/json`
  - Description: Same as above but in JSON format

- GET `/api/v1/lastblocks`
  - Description: Retrieves the most recent blocks in the blockchain
  - Parameters:
    - `n` (optional): Number of blocks to retrieve (default: 10)
    - `includeorphans` (optional): Whether to include orphaned blocks (default: false)
    - `height` (optional): Start retrieval from this height
  - Returns: JSON array of recent block information

- GET `/api/v1/blockstats`
  - Description: Retrieves statistical information about the blockchain
  - Parameters: None
  - Returns: JSON object with block statistics

- GET `/api/v1/blockgraphdata/:period`
  - Description: Retrieves time-series data for graphing purposes
  - Parameters:
    - `period`: Period in milliseconds for data aggregation
  - Returns: JSON object with time-series block data

- GET `/rest/block/:hash.bin`
  - Description: Legacy endpoint for retrieving a block in binary format
  - Parameters:
    - `hash`: Block hash (hex string)
  - Returns: Block data in binary format

- GET `/api/v1/block_legacy/:hash`
  - Description: Alternative legacy endpoint for retrieving a block in binary format
  - Parameters:
    - `hash`: Block hash (hex string)
  - Returns: Block data in binary format

### Block Header Endpoints

- GET `/api/v1/header/:hash`
  - Description: Retrieves a block header by hash in binary format
  - Parameters:
    - `hash`: Block hash (hex string)
  - Returns: Block header in binary format

- GET `/api/v1/header/:hash/hex`
  - Description: Retrieves a block header by hash in hexadecimal format
  - Parameters:
    - `hash`: Block hash (hex string)
  - Returns: Block header as hex string

- GET `/api/v1/header/:hash/json`
  - Description: Retrieves a block header by hash in JSON format
  - Parameters:
    - `hash`: Block hash (hex string)
  - Returns: Block header in structured JSON format

- GET `/api/v1/headers/:hash`
  - Description: Retrieves multiple headers starting from a hash
  - Parameters:
    - `hash`: Starting block hash (hex string)
    - `n` (optional): Number of headers to retrieve
  - Returns: Headers in binary format

- GET `/api/v1/headers/:hash/hex`
  - Description: Same as above but in hexadecimal format

- GET `/api/v1/headers/:hash/json`
  - Description: Same as above but in JSON format

- GET `/api/v1/headers_to_common_ancestor/:hash`
  - Description: Retrieves headers from specified hash to common ancestor
  - Parameters:
    - `hash`: Target block hash (hex string)
    - `locator` (required): Comma-separated list of block hashes for locator
  - Returns: Headers in binary format

- GET `/api/v1/headers_to_common_ancestor/:hash/hex`
  - Description: Same as above but in hexadecimal format

- GET `/api/v1/headers_to_common_ancestor/:hash/json`
  - Description: Same as above but in JSON format

- GET `/api/v1/bestblockheader`
  - Description: Retrieves the current best block header
  - Parameters: None
  - Returns: Best block header in binary format

- GET `/api/v1/bestblockheader/hex`
  - Description: Same as above but in hexadecimal format

- GET `/api/v1/bestblockheader/json`
  - Description: Same as above but in JSON format

### UTXO Endpoints

- GET `/api/v1/utxo/:hash`
  - Description: Retrieves UTXO information in binary format
  - Parameters:
    - `hash`: UTXO transaction hash (hex string)
    - `vout` (required): Output index
  - Returns: UTXO data in binary format

- GET `/api/v1/utxo/:hash/hex`
  - Description: Retrieves UTXO information in hexadecimal format
  - Parameters:
    - `hash`: UTXO transaction hash (hex string)
    - `vout` (required): Output index
  - Returns: UTXO data as hex string

- GET `/api/v1/utxo/:hash/json`
  - Description: Retrieves UTXO information in JSON format
  - Parameters:
    - `hash`: UTXO transaction hash (hex string)
    - `vout` (required): Output index
  - Returns: UTXO data in structured JSON format

- GET `/api/v1/utxos/:hash/json`
  - Description: Retrieves all UTXOs for a given transaction
  - Parameters:
    - `hash`: Transaction hash (hex string)
  - Returns: Array of UTXO data in JSON format

### Subtree Endpoints

- GET `/api/v1/subtree/:hash`
  - Description: Retrieves a subtree in binary format
  - Parameters:
    - `hash`: Subtree hash (hex string)
  - Returns: Subtree data in binary format

- GET `/api/v1/subtree/:hash/hex`
  - Description: Retrieves a subtree in hexadecimal format
  - Parameters:
    - `hash`: Subtree hash (hex string)
  - Returns: Subtree data as hex string

- GET `/api/v1/subtree/:hash/json`
  - Description: Retrieves a subtree in JSON format
  - Parameters:
    - `hash`: Subtree hash (hex string)
  - Returns: Subtree data in structured JSON format

- GET `/api/v1/subtree/:hash/txs/json`
  - Description: Retrieves transactions within a subtree
  - Parameters:
    - `hash`: Subtree hash (hex string)
  - Returns: Array of transaction data in JSON format

- GET `/api/v1/block/:hash/subtrees/json`
  - Description: Retrieves all subtrees for a block
  - Parameters:
    - `hash`: Block hash (hex string)
  - Returns: Array of subtree data in JSON format

### Search Endpoints

- GET `/api/v1/search`
  - Description: Searches for blockchain entities by hash or height
  - Parameters:
    - `query` (required): Search query (hash or block height)
  - Returns: JSON object with search results and entity type

### Finite State Machine (FSM) Endpoints

- GET `/api/v1/fsm/state`
  - Description: Returns current blockchain FSM state
  - Parameters: None
  - Returns: JSON object with current state information

- POST `/api/v1/fsm/state`
  - Description: Sends an event to the blockchain FSM
  - Parameters: JSON object with event details
  - Returns: JSON object with updated state information

- GET `/api/v1/fsm/events`
  - Description: Lists all possible FSM events
  - Parameters: None
  - Returns: JSON array of available events

- GET `/api/v1/fsm/states`
  - Description: Lists all possible FSM states
  - Parameters: None
  - Returns: JSON array of available states

## Error Handling

All endpoints return appropriate HTTP status codes to indicate success or failure:

- 200 OK: Request successful
- 400 Bad Request: Invalid input parameters
- 404 Not Found: Resource not found
- 500 Internal Server Error: Server-side error

Error responses include a JSON object with an error message:

```json
{
  "error": "Error message description"
}
```

## Examples

### Retrieving a Transaction

```
GET /api/v1/tx/abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789/json
```

Response:
```json
{
  "txid": "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
  "version": 1,
  "locktime": 0,
  "vin": [...],
  "vout": [...]
}
```

### Batch Retrieving Transactions

Request:
```
POST /api/v1/subtree/:hash/txs
Content-Type: application/octet-stream

<binary data: concatenated 32-byte transaction hashes>
```

Response: Binary data containing the concatenated transactions

### Retrieving Block Information

```
GET /api/v1/block/000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f/json
```

Response:
```json
{
  "hash": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f",
  "confirmations": 123456,
  "size": 285,
  "height": 0,
  "version": 1,
  "merkleroot": "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b",
  "tx": [...],
  "time": 1231006505,
  "nonce": 2083236893,
  "bits": "1d00ffff",
  "difficulty": 1.0
}
```
    - No separators between hashes
    - The body can contain multiple hashes
    - The server reads until it reaches the end of the body (EOF)

- GET `/txmeta/:hash/json`
    - Retrieves transaction metadata in JSON format

- GET `/txmeta_raw/:hash`
    - Retrieves raw transaction metadata in binary stream format
- GET `/txmeta_raw/:hash/hex`
    - Retrieves raw transaction metadata in hexadecimal format
- GET `/txmeta_raw/:hash/json`
    - Retrieves raw transaction metadata in JSON format



**Subtree Endpoints**

- GET `/subtree/:hash`
    - Retrieves a subtree in binary stream format
- GET `/subtree/:hash/hex`
    - Retrieves a subtree in hexadecimal format
- GET `/subtree/:hash/json`
    - Retrieves a subtree in JSON format
- GET `/subtree/:hash/txs`
    - Retrieves transactions in a subtree in BINARY format



**Block and Header Endpoints**

- GET `/headers/:hash`
    - Retrieves block headers in binary stream format
- GET `/headers/:hash/hex`
    - Retrieves block headers in hexadecimal format
- GET `/headers/:hash/json`
    - Retrieves block headers in JSON format
- GET `/header/:hash`
    - Retrieves a single block header in binary stream format
- GET `/header/:hash/hex`
    - Retrieves a single block header in hexadecimal format
- GET `/header/:hash/json`
    - Retrieves a single block header in JSON format
- GET `/blocks`
    - Retrieves multiple blocks
- GET `/blocks/:hash`
    - Retrieves N blocks starting from a specific hash in binary stream format
- GET `/blocks/:hash/hex`
    - Retrieves N blocks starting from a specific hash in hexadecimal format
- GET `/blocks/:hash/json`
    - Retrieves N blocks starting from a specific hash in JSON format
- GET `/block_legacy/:hash`
    - Retrieves a block in legacy format (binary stream)
- GET `/block/:hash`
    - Retrieves a block by hash in binary stream format
- GET `/rest/block/:hash.bin`
    - (Deprecated). Retrieves a block by hash in binary stream format.

- GET `/block/:hash/hex`
    - Retrieves a block by hash in hexadecimal format
- GET `/block/:hash/json`
    - Retrieves a block by hash in JSON format
- GET `/block/:hash/forks`
    - Retrieves fork information for a specific block
- GET `/block/:hash/subtrees/json`
    - Retrieves subtrees of a block in JSON format
- GET `/lastblocks`
    - Retrieves the last N blocks
- GET `/bestblockheader`
    - Retrieves the best block header in binary stream format
- GET `/bestblockheader/hex`
    - Retrieves the best block header in hexadecimal format
- GET `/bestblockheader/json`
    - Retrieves the best block header in JSON format



**UTXO and Balance Endpoints**

- GET `/utxo/:hash`
    - Retrieves a UTXO in binary stream format
- GET `/utxo/:hash/hex`
    - Retrieves a UTXO in hexadecimal format
- GET `/utxo/:hash/json`
    - Retrieves a UTXO in JSON format

- GET `/utxos/:hash/json`
    - Retrieves UTXOs by transaction ID in JSON format

- GET `/balance`
    - Retrieves balance information



**Miscellaneous Endpoints**

- GET `/search?q=:hash`
    - Performs a search

- GET `/blockstats`
    - Retrieves block statistics

- GET `/blockgraphdata/:period`
    - Retrieves block graph data for a specified period


For more information on the Asset Server, see the [Asset Server Reference](../../references/services/asset_reference.md).
