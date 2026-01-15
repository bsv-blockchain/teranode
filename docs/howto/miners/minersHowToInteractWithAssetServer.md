# How to Interact with the Asset Server

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

- POST `/api/v1/txs`
    - Description: Batch retrieves multiple transactions
    - Request Body:

        Concatenated series of 32-byte transaction hashes without separators
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

- GET `/api/v1/block/:hash/subtrees/json`
    - Description: Retrieves subtree information for a specific block
    - Parameters:

        - `hash`: Block hash (hex string)

    - Returns: JSON object with block subtree information including subtree hashes and metadata

- GET `/api/v1/blocks`
    - Description: Retrieves a paginated list of blocks
    - Parameters:

        - `offset` (optional): Number of blocks to skip from the tip (default: 0)
        - `limit` (optional): Maximum number of blocks to return (default: 20, max: 100)
        - `includeOrphans` (optional): Whether to include orphaned blocks (default: false)

    - Returns: JSON array of block data

- GET `/api/v1/blocks/:hash`
    - Description: Retrieves multiple consecutive blocks starting with the specified hash, traversing backward through the chain
    - Parameters:

        - `hash`: Starting block hash (hex string)
        - `n` (optional): Number of blocks to retrieve (default: 100, max: 1000)

    - Returns: Concatenated block data in binary format (application/octet-stream)
    - **Important:** All blocks are concatenated without delimiters. Clients must parse blocks sequentially:
        1. Read 80-byte block header
        2. Read VarInt transaction count
        3. Read VarInt size in bytes
        4. Read VarInt subtree count, then subtree hashes (32 bytes each)
        5. Read coinbase transaction
        6. Read VarInt block height
        7. Repeat for next block
    - **Note:** When downloading in a browser, the file may appear to contain only one block. The full data is present but requires sequential parsing. Use `/json` endpoint for easier inspection.

- GET `/api/v1/blocks/:hash/hex`
    - Description: Same as above but returns hexadecimal-encoded string
    - Parameters:

        - `hash`: Starting block hash (hex string)
        - `n` (optional): Number of blocks to retrieve (default: 100, max: 1000)

    - Returns: Hex string of concatenated block bytes (text/plain)

- GET `/api/v1/blocks/:hash/json`
    - Description: Same as above but returns structured JSON array of blocks
    - Parameters:

        - `hash`: Starting block hash (hex string)
        - `n` (optional): Number of blocks to retrieve (default: 100, max: 1000)

    - Returns: JSON array containing parsed block objects with all fields
    - **Recommended:** Use this endpoint for easier parsing and inspection of multiple blocks

- GET `/api/v1/lastblocks`
    - Description: Retrieves the most recent blocks in the blockchain
    - Parameters:

        - `n` (optional): Number of blocks to retrieve (default: 10)
        - `fromHeight` (optional): Starting block height for retrieval (default: 0)
        - `includeOrphans` (optional): Whether to include orphaned blocks (default: false)

    - Response Format:

        JSON array of recent block information including:

        - `hash`: Block hash
        - `height`: Block height
        - `time`: Block timestamp
        - `txCount`: Number of transactions in the block
        - `size`: Block size in bytes
        - `orphan`: Boolean indicating if the block is an orphan

    - Returns: JSON array of recent block information

- GET `/api/v1/blockstats`
    - Description: Retrieves statistical information about the blockchain
    - Parameters:
        None

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

- GET `/api/v1/headers_from_common_ancestor/:hash`
    - Description: Retrieves headers from common ancestor to specified hash
    - Parameters:

        - `hash`: Target block hash (hex string)
        - `locator` (required): Comma-separated list of block hashes for locator

    - Returns: Headers in binary format

- GET `/api/v1/headers_from_common_ancestor/:hash/hex`
    - Description: Same as above but in hexadecimal format

- GET `/api/v1/headers_from_common_ancestor/:hash/json`
    - Description: Same as above but in JSON format

- GET `/api/v1/block_locator`
    - Description: Retrieves block locator information for blockchain synchronization
    - Parameters:
        None

    - Returns: JSON object with block locator data

- GET `/api/v1/bestblockheader`
    - Description: Retrieves the current best block header
    - Parameters:
        None

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
        - `vout` (required): Output index (query parameter)

    - Returns: UTXO data in binary format

- GET `/api/v1/utxo/:hash/hex`
    - Description: Retrieves UTXO information in hexadecimal format
    - Parameters:

        - `hash`: UTXO transaction hash (hex string)
        - `vout` (required): Output index (query parameter)

    - Returns: UTXO data as hex string

- GET `/api/v1/utxo/:hash/json`
    - Description: Retrieves UTXO information in JSON format
    - Parameters:

        - `hash`: UTXO transaction hash (hex string)
        - `vout` (required): Output index (query parameter)

    - Returns: UTXO data in structured JSON format

- GET `/api/v1/utxos/:hash/json`
    - Description: Retrieves all UTXOs for a given transaction
    - Parameters:

        - `hash`: Transaction hash (hex string)

    - Returns: Array of UTXO data in JSON format

### Merkle Proof Endpoints

- GET `/api/v1/merkle_proof/:hash`
    - Description: Retrieves merkle proof for a transaction in binary format
    - Parameters:

        - `hash`: Transaction hash (hex string)

    - Returns: Merkle proof data in binary format

- GET `/api/v1/merkle_proof/:hash/hex`
    - Description: Retrieves merkle proof for a transaction in hexadecimal format
    - Parameters:

        - `hash`: Transaction hash (hex string)

    - Returns: Merkle proof data as hex string

- GET `/api/v1/merkle_proof/:hash/json`
    - Description: Retrieves merkle proof for a transaction in JSON format
    - Parameters:

        - `hash`: Transaction hash (hex string)

    - Returns: Merkle proof data in BSV Unified Merkle Path (BUMP) format as structured JSON

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

- GET `/api/v1/subtree_data/:hash`
    - Description: Retrieves subtree metadata and information
    - Parameters:

        - `hash`: Subtree hash (hex string)

    - Returns: Subtree metadata in JSON format

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

        - `q` (required): Search query (hash or block height)

    - Returns: JSON object with search results and entity type

### Block Management Endpoints

- POST `/api/v1/block/invalidate`
    - Description: Marks a block as invalid, forcing a chain reorganization
    - Parameters:

        - Request body: JSON object with block hash information

          ```json
          {
            "hash": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
          }
          ```

    - Returns: JSON object with status of the invalidation operation

- POST `/api/v1/block/revalidate`
    - Description: Reconsiders a previously invalidated block
    - Parameters:

        - Request body: JSON object with block hash information

          ```json
          {
            "hash": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
          }
          ```

    - Returns: JSON object with status of the revalidation operation

- GET `/api/v1/blocks/invalid`
    - Description: Retrieves a list of currently invalidated blocks
    - Parameters:

        - `limit` (optional): Maximum number of blocks to retrieve

    - Returns: Array of invalid block information

### Finite State Machine (FSM) Endpoints

- GET `/api/v1/fsm/state`
    - Description: Returns current blockchain FSM state
    - Parameters:
        None

    - Returns: JSON object with current state information including:

        - `state`: Current state name
        - `metadata`: Additional state information
        - `allowedTransitions`: Events that can be triggered from this state

    - Example response:

          ```json
          {
            "state": "Running",
            "metadata": {
              "syncedHeight": 700001,
              "bestHeight": 700001,
              "isSynchronized": true
            },
            "allowedTransitions": ["stop", "pause"]
          }
          ```

- POST `/api/v1/fsm/state`
    - Description: Sends an event to the blockchain FSM to trigger a state transition
    - Parameters:

        JSON object with event details

        - `event` (string, required): The event name to trigger
        - `data` (object, optional): Additional data for the event

    - Example request:

          ```json
          {
            "event": "pause",
            "data": {
              "reason": "maintenance"
            }
          }
          ```

    - Returns: JSON object with updated state information and transition result

- GET `/api/v1/fsm/events`
    - Description: Lists all possible FSM events
    - Parameters:
        None

    - Returns: JSON array of available events with descriptions

    - Example response:

          ```json
          [
            {
              "name": "start",
              "description": "Start the blockchain service"
            },
            {
              "name": "stop",
              "description": "Stop the blockchain service"
            },
            {
              "name": "pause",
              "description": "Temporarily pause operations"
            }
          ]
          ```

- GET `/api/v1/fsm/states`
    - Description: Lists all possible FSM states
    - Parameters:
        None

    - Returns: JSON array of available states with descriptions

    - Example response:

          ```json
          [
            {
              "name": "Idle",
              "description": "Service is idle and not processing blocks"
            },
            {
              "name": "Running",
              "description": "Service is active and processing blocks"
            },
            {
              "name": "Paused",
              "description": "Service is temporarily paused"
            }
          ]
          ```

### Peer Management Endpoints

- GET `/api/v1/peers`
    - Description: Retrieves information about connected peers
    - Parameters:
        None

    - Returns: JSON array of peer information

### Synchronization Endpoints

- GET `/api/v1/catchup/status`
    - Description: Returns current blockchain catchup/synchronization status
    - Parameters:
        None

    - Returns: JSON object with sync progress information

- GET `/api/v1/service/heights`
    - Description: Returns block heights across different services
    - Parameters:
        None

    - Returns: JSON object with service height information

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

```http
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

```http
POST /api/v1/txs
Content-Type: application/octet-stream

<binary data: concatenated 32-byte transaction hashes>
```

Response: Binary data containing the concatenated transactions

### Retrieving Block Information

```http
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

For more information on the Asset Server, see the [Asset Server Reference](../../references/services/asset_reference.md).
