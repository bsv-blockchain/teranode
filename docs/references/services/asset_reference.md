# Asset Service Reference Documentation

## Types

### Server

```go
type Server struct {
    logger              ulogger.Logger
    settings            *settings.Settings
    utxoStore           utxo.Store
    txStore             blob.Store
    subtreeStore        blob.Store
    blockPersisterStore blob.Store
    httpAddr            string
    httpServer          *httpimpl.HTTP
    centrifugeAddr      string
    centrifugeServer    *centrifuge_impl.Centrifuge
    blockchainClient    blockchain.ClientI
}
```

The `Server` type is the main structure for the Asset Service. It coordinates between different storage backends and provides both HTTP and Centrifuge interfaces for accessing blockchain data.

### Repository

```go
type Repository struct {
    logger              ulogger.Logger
    settings            *settings.Settings
    UtxoStore           utxo.Store
    TxStore             blob.Store
    SubtreeStore        blob.Store
    BlockPersisterStore blob.Store
    BlockchainClient    blockchain.ClientI
}
```

The `Repository` type provides access to blockchain data storage and retrieval operations. It implements the necessary interfaces to interact with various data stores and blockchain clients.

### HTTP

```go
type HTTP struct {
logger     ulogger.Logger
settings   *settings.Settings
repository repository.Interface
e          *echo.Echo
startTime  time.Time
privKey    crypto.PrivKey
}
```

The `HTTP` type represents the HTTP server for the Asset Service.

## Functions

### Server

#### NewServer

```go
func NewServer(logger ulogger.Logger, tSettings *settings.Settings, utxoStore utxo.Store, txStore blob.Store, subtreeStore blob.Store, blockPersisterStore blob.Store, blockchainClient blockchain.ClientI) *Server
```

Creates a new instance of the `Server` with the provided dependencies. It initializes the server with necessary stores and clients for handling blockchain data.

#### Health

```go
func (v *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Performs health checks on the server and its dependencies. It supports both liveness and readiness checks based on the checkLiveness parameter.

#### Init

```go
func (v *Server) Init(ctx context.Context) (err error)
```

Initializes the server by setting up HTTP and Centrifuge endpoints. It configures the necessary components based on the provided configuration.

#### Start

```go
func (v *Server) Start(ctx context.Context, readyCh chan<- struct{}) error
```

Starts the Asset Service, launching HTTP and Centrifuge servers if configured. It also handles FSM state restoration if required. The readyCh channel is closed when the service is ready to receive requests.

#### Stop

```go
func (v *Server) Stop(ctx context.Context) error
```

Gracefully shuts down the server and its components, including HTTP and Centrifuge servers.

### Repository

#### NewRepository

```go
func NewRepository(logger ulogger.Logger, tSettings *settings.Settings, utxoStore utxo.Store, txStore blob.Store, blockchainClient blockchain.ClientI, subtreeStore blob.Store, blockPersisterStore blob.Store) (*Repository, error)
```

Creates a new instance of the `Repository` with the provided dependencies. It initializes connections to various data stores for blockchain data access.

#### GetTransaction

```go
func (repo *Repository) GetTransaction(ctx context.Context, hash *chainhash.Hash) ([]byte, error)
```

Retrieves a transaction by its hash.

#### GetBlockStats

```go
func (repo *Repository) GetBlockStats(ctx context.Context) (*model.BlockStats, error)
```

Retrieves block statistics.

#### GetBlockGraphData

```go
func (repo *Repository) GetBlockGraphData(ctx context.Context, periodMillis uint64) (*model.BlockDataPoints, error)
```

Retrieves block graph data for a specified period.

#### GetBlockByHash

```go
func (repo *Repository) GetBlockByHash(ctx context.Context, hash *chainhash.Hash) (*model.Block, error)
```

Retrieves a block by its hash.

#### GetBlockByHeight

```go
func (repo *Repository) GetBlockByHeight(ctx context.Context, height uint32) (*model.Block, error)
```

Retrieves a block by its height.

#### GetBlockHeader

```go
func (repo *Repository) GetBlockHeader(ctx context.Context, hash *chainhash.Hash) (*model.BlockHeader, *model.BlockHeaderMeta, error)
```

Retrieves a block header by its hash.

#### GetLastNBlocks

```go
func (repo *Repository) GetLastNBlocks(ctx context.Context, n int64, includeOrphans bool, fromHeight uint32) ([]*model.BlockInfo, error)
```

Retrieves the last N blocks.

#### GetBlockHeaders

```go
func (repo *Repository) GetBlockHeaders(ctx context.Context, hash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
```

Retrieves a sequence of block headers starting from a specific hash.

#### GetBlockHeadersToCommonAncestor

```go
func (repo *Repository) GetBlockHeadersToCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
```

Retrieves block headers from the target hash back to the common ancestor with the provided block locator.

#### GetBlockHeadersFromHeight

```go
func (repo *Repository) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
```

Retrieves block headers starting from a specific height up to the specified limit.

#### GetSubtree

```go
func (repo *Repository) GetSubtree(ctx context.Context, hash *chainhash.Hash) (*util.Subtree, error)
```

Retrieves a subtree by its hash.

#### GetSubtreeBytes

```go
func (repo *Repository) GetSubtreeBytes(ctx context.Context, hash *chainhash.Hash) ([]byte, error)
```

Retrieves the raw bytes of a subtree.

#### GetSubtreeReader

```go
func (repo *Repository) GetSubtreeReader(ctx context.Context, hash *chainhash.Hash) (io.ReadCloser, error)
```

Provides a reader interface for accessing subtree data.

#### GetSubtreeDataReader

```go
func (repo *Repository) GetSubtreeDataReader(ctx context.Context, hash *chainhash.Hash) (io.ReadCloser, error)
```

Provides a reader interface for accessing subtree data from the block persister.

#### GetSubtreeExists

```go
func (repo *Repository) GetSubtreeExists(ctx context.Context, hash *chainhash.Hash) (bool, error)
```

Checks if a subtree with the given hash exists in the store.

#### GetSubtreeHead

```go
func (repo *Repository) GetSubtreeHead(ctx context.Context, hash *chainhash.Hash) (*util.Subtree, int, error)
```

Retrieves only the head portion of a subtree, containing fees and size information.

#### GetUtxo

```go
func (repo *Repository) GetUtxo(ctx context.Context, spend *utxo.Spend) (*utxo.SpendResponse, error)
```

Retrieves UTXO information.

#### GetBestBlockHeader

```go
func (repo *Repository) GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error)
```

Retrieves the best (most recent) block header.

#### GetBlockLocator

```go
func (repo *Repository) GetBlockLocator(ctx context.Context, blockHeaderHash *chainhash.Hash, height uint32) ([]*chainhash.Hash, error)
```

Retrieves a sequence of block hashes at exponentially increasing distances back from the provided block hash or the best block if no hash is specified.

#### GetBalance

```go
func (repo *Repository) GetBalance(ctx context.Context) (uint64, uint64, error)
```

Retrieves the balance information.

#### GetBlockForks

```go
func (repo *Repository) GetBlockForks(ctx context.Context, hash *chainhash.Hash) (*model.ForkInfo, error)
```

Retrieves information about forks related to the specified block.

#### GetBlockSubtrees

```go
func (repo *Repository) GetBlockSubtrees(ctx context.Context, hash *chainhash.Hash) ([]*util.Subtree, error)
```

Retrieves all subtrees included in the specified block.

### HTTP

#### New

```go
func New(logger ulogger.Logger, tSettings *settings.Settings, repo *repository.Repository) (*HTTP, error)
```

Creates a new instance of the HTTP server.

#### Init

```go
func (h *HTTP) Init(_ context.Context) error
```

Initializes the HTTP server.

#### Start

```go
func (h *HTTP) Start(ctx context.Context, addr string) error
```

Starts the HTTP server.

#### Stop

```go
func (h *HTTP) Stop(ctx context.Context) error
```

Stops the HTTP server.

#### AddHTTPHandler

```go
func (h *HTTP) AddHTTPHandler(pattern string, handler http.Handler) error
```

Adds a new HTTP handler to the server.

#### Sign

```go
func (h *HTTP) Sign(resp *echo.Response, hash []byte) error
```

Signs the HTTP response.

## Configuration

The Asset Service uses configuration values from the `gocore.Config()` function, including:

### Network and API Configuration
- `asset_httpListenAddress`: HTTP listen address (default: ":8090")
- `asset_apiPrefix`: API prefix (default: "/api/v1")
- `securityLevelHTTP`: Security level for HTTP (0 for HTTP, non-zero for HTTPS)
- `server_certFile`: Certificate file for HTTPS
- `server_keyFile`: Key file for HTTPS

### Centrifuge Configuration (Real-time Updates)
- `asset_centrifuge_disable`: Whether to disable Centrifuge server (default: false)
- `asset_centrifugeListenAddress`: Centrifuge listen address (default: ":8000")

### Security
- `http_sign_response`: Whether to sign HTTP responses (default: false)
- `p2p_private_key`: Private key for signing responses

### Dashboard Configuration
- `dashboard.enabled`: Whether to enable the web dashboard (default: false)
- `dashboard.auth.enabled`: Whether to enable authentication for the dashboard (default: true)
- `dashboard.auth.username`: Dashboard admin username
- `dashboard.auth.password`: Dashboard admin password (stored as hash)

### Debug Configuration
- `asset_echoDebug`: Enable debug logging for HTTP server (default: false)
- `statsPrefix`: Prefix for stats endpoints (default: "/debug/")

The Asset Service dashboard provides a visual interface for monitoring blockchain status, viewing blocks and transactions, and managing the node. When enabled, it provides:

1. Real-time blockchain statistics
2. Block explorer functionality
3. Transaction viewer
4. FSM state visualization and control
5. Node management capabilities including block invalidation/revalidation

The dashboard is particularly useful for monitoring the unique subtree-based transaction management system used in Teranode, which replaces the traditional mempool architecture found in other Bitcoin implementations.

## Dependencies

The Asset Service depends on several other components and services:

- UTXO Store
- Transaction Store
- Subtree Store
- Block Persister Store
- Blockchain Client
- Coinbase API Client (optional)

These dependencies are injected into the `Server` and `Repository` structures during initialization.


## API Endpoints

The Asset Service provides various API endpoints for interacting with blockchain data. These endpoints are organized by category and support different response formats for flexibility.

All API endpoints are prefixed with `/api/v1` unless otherwise specified.

### Response Formats

Most endpoints support multiple response formats:

- **BINARY_STREAM**: Raw binary data (`application/octet-stream`)
- **HEX**: Hexadecimal string representation (`text/plain`)
- **JSON**: Structured JSON data (`application/json`)

The format can be selected by appending `/hex` or `/json` to the endpoint, or by setting the appropriate `Accept` header. If not specified, the binary format is used as the default.

### Error Handling

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

### Health and Status Endpoints

- **GET `/alive`**
    - Purpose: Service liveness check
  - Returns: Text message with uptime information
  - Status Code: 200 on success

- **GET `/health`**
    - Purpose: Service health check with dependency status
  - Returns: Status information and dependency health
  - Status Code: 200 on success, 503 on failure

### Transaction Endpoints

- **GET `/api/v1/tx/:hash`**
    - Purpose: Get transaction in binary format
  - Parameters: `hash` - Transaction ID hash (hex string)
  - Returns: Transaction data (binary)

- **GET `/api/v1/tx/:hash/hex`**
    - Purpose: Get transaction in hex format
  - Parameters: `hash` - Transaction ID hash (hex string)
  - Returns: Transaction data (hex string)

- **GET `/api/v1/tx/:hash/json`**
    - Purpose: Get transaction in JSON format
  - Parameters: `hash` - Transaction ID hash (hex string)
  - Returns: Transaction data (JSON)

- **POST `/api/v1/subtree/:hash/txs`**
    - Purpose: Batch retrieve multiple transactions
  - Request Body: Concatenated 32-byte transaction hashes
  - Returns: Concatenated transactions (binary)

- **GET `/api/v1/txmeta/:hash/json`**
    - Purpose: Get transaction metadata
  - Parameters: `hash` - Transaction ID hash (hex string)
  - Returns: Transaction metadata (JSON)

- **GET `/api/v1/txmeta_raw/:hash`**
    - Purpose: Get raw transaction metadata (binary)
  - Parameters: `hash` - Transaction ID hash (hex string)
  - Returns: Raw transaction metadata

- **GET `/api/v1/txmeta_raw/:hash/hex`**
    - Purpose: Get raw transaction metadata (hex)
  - Parameters: `hash` - Transaction ID hash (hex string)
  - Returns: Raw transaction metadata (hex string)

- **GET `/api/v1/txmeta_raw/:hash/json`**
    - Purpose: Get raw transaction metadata (JSON)
  - Parameters: `hash` - Transaction ID hash (hex string)
  - Returns: Raw transaction metadata (JSON)

### Block Endpoints

- **GET `/api/v1/block/:hash`**
    - Purpose: Get block by hash (binary)
  - Parameters: `hash` - Block hash (hex string)
  - Returns: Block data (binary)

- **GET `/api/v1/block/:hash/hex`**
    - Purpose: Get block by hash (hex)
  - Parameters: `hash` - Block hash (hex string)
  - Returns: Block data (hex string)

- **GET `/api/v1/block/:hash/json`**
    - Purpose: Get block by hash (JSON)
  - Parameters: `hash` - Block hash (hex string)
  - Returns: Block data (JSON)

- **GET `/api/v1/block/:hash/forks`**
    - Purpose: Get fork information for a block
  - Parameters: `hash` - Block hash (hex string)
  - Returns: Fork data (JSON)

- **GET `/api/v1/blocks`**
    - Purpose: Get paginated blocks list
  - Parameters: `offset`, `limit` (optional)
  - Returns: Blocks list (JSON)

- **GET `/api/v1/blocks/:hash`**
    - Purpose: Get multiple blocks starting with specified hash (binary)
  - Parameters: `hash` - Starting block hash, `n` - Number of blocks (optional)
  - Returns: Block data (binary)

- **GET `/api/v1/lastblocks`**
    - Purpose: Get most recent blocks
  - Parameters: `n` (optional) - Number of blocks, `includeorphans` (optional), `height` (optional)
  - Returns: Recent blocks data (JSON)

- **GET `/api/v1/blockstats`**
    - Purpose: Get block statistics
  - Returns: Block statistics (JSON)

- **GET `/api/v1/blockgraphdata/:period`**
    - Purpose: Get time-series block data for graphing
  - Parameters: `period` - Time period in milliseconds
  - Returns: Time-series data (JSON)

- **GET `/rest/block/:hash.bin`**
    - Purpose: Legacy endpoint for block retrieval
  - Parameters: `hash` - Block hash (hex string)
  - Returns: Block data (binary)

- **GET `/api/v1/block_legacy/:hash`**
    - Purpose: Alternative legacy block retrieval
  - Parameters: `hash` - Block hash (hex string)
  - Returns: Block data (binary)

### Block Header Endpoints

- **GET `/api/v1/header/:hash`**
    - Purpose: Get single block header (binary)
  - Parameters: `hash` - Block hash (hex string)
  - Returns: Block header (binary)

- **GET `/api/v1/headers/:hash`**
    - Purpose: Get multiple headers starting from hash (binary)
  - Parameters: `hash` - Starting block hash, `n` - Number of headers (optional)
  - Returns: Block headers (binary)

- **GET `/api/v1/headers_to_common_ancestor/:hash`**
    - Purpose: Get headers to common ancestor (binary)
  - Parameters: `hash` - Target hash, `locator` - Comma-separated block hashes
  - Returns: Block headers (binary)

- **GET `/api/v1/bestblockheader`**
    - Purpose: Get best block header (binary)
  - Returns: Best block header (binary)

### Block Management Endpoints

- **POST `/api/v1/block/invalidate`**
    - Purpose: Mark a block as invalid, forcing a chain reorganization
  - Parameters: JSON object in request body with block hash information
    ```json
    {
      "hash": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
    }
    ```
  - Returns: JSON object with status of the invalidation operation
  - Security: This is an administrative operation that can affect blockchain consensus

- **POST `/api/v1/block/revalidate`**
    - Purpose: Reconsider a previously invalidated block
  - Parameters: JSON object in request body with block hash information
    ```json
    {
      "hash": "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
    }
    ```
  - Returns: JSON object with status of the revalidation operation
  - Security: This is an administrative operation that can affect blockchain consensus

- **GET `/api/v1/blocks/invalid`**
    - Purpose: Retrieve a list of currently invalidated blocks
  - Parameters:

    - `limit` (optional): Maximum number of blocks to retrieve
  - Returns: Array of invalid block information

### Finite State Machine (FSM) Endpoints

- **GET `/api/v1/fsm/state`**
    - Purpose: Get current blockchain FSM state
  - Returns: JSON object with current state information including state name, metadata, and allowed transitions
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

- **POST `/api/v1/fsm/state`**
    - Purpose: Send an event to the blockchain FSM to trigger a state transition
  - Parameters: JSON object in request body with event details
    ```json
    {
      "event": "pause",
      "data": {
        "reason": "maintenance"
      }
    }
    ```
  - Returns: JSON object with updated state information and transition result
  - Security: This is an administrative operation that can affect blockchain operation

- **GET `/api/v1/fsm/events`**
    - Purpose: List all possible FSM events
  - Returns: JSON array of available events with descriptions

- **GET `/api/v1/fsm/states`**
    - Purpose: List all possible FSM states
  - Returns: JSON array of available states with descriptions

### UTXO Endpoints

- **GET `/api/v1/utxo/:hash`**
    - Purpose: Get UTXO information (binary)
  - Parameters: `hash` - Transaction hash, `vout` - Output index
  - Returns: UTXO data (binary)

- **GET `/api/v1/utxos/:hash/json`**
    - Purpose: Get all UTXOs for a transaction
  - Parameters: `hash` - Transaction hash
  - Returns: UTXO data array (JSON)

### Subtree Endpoints

- **GET `/api/v1/subtree/:hash`**
    - Purpose: Get subtree data (binary)
  - Parameters: `hash` - Subtree hash
  - Returns: Subtree data (binary)

- **GET `/api/v1/subtree/:hash/txs/json`**
    - Purpose: Get transactions in a subtree
  - Parameters: `hash` - Subtree hash
  - Returns: Transaction data array (JSON)

- **GET `/api/v1/block/:hash/subtrees/json`**
    - Purpose: Get all subtrees for a block
  - Parameters: `hash` - Block hash
  - Returns: Subtree data array (JSON)

### Search Endpoints

- **GET `/api/v1/search`**
    - Purpose: Search for blockchain entities
  - Parameters: `query` - Search term (hash or height)
  - Returns: Search results (JSON)

### FSM (Finite State Machine) Endpoints

- **GET `/api/v1/fsm/state`**
    - Purpose: Get current blockchain FSM state
  - Returns: Current state information (JSON)

- **POST `/api/v1/fsm/state`**
    - Purpose: Send event to blockchain FSM
  - Request Body: Event details (JSON)
  - Returns: Updated state information (JSON)

- **GET `/api/v1/fsm/events`**
    - Purpose: List all possible FSM events
  - Returns: Array of available events (JSON)

- **GET `/api/v1/fsm/states`**
    - Purpose: List all possible FSM states
  - Returns: Array of available states (JSON)

### Authentication

The service supports response signing. When enabled, responses include an `X-Signature` header containing an Ed25519 signature of the response data.

### Common Headers

#### Request Headers
- `Content-Type`: Specifies the format of request data
- `Accept`: Specifies the desired response format

#### Response Headers
- `Content-Type`: Indicates the format of response data
- `X-Signature`: Ed25519 signature (when response signing is enabled)

### Pagination

Endpoints that return lists support pagination through query parameters:

- `offset`: Number of items to skip (default: 0)
- `limit`: Maximum number of items to return (default: 20, max: 100)

Paginated responses include metadata:
```json
{
  "data": [...],
  "pagination": {
    "offset": 0,
    "limit": 20,
    "totalRecords": 100
  }
}
```

### Transaction Endpoints

#### Get Transaction
Retrieves a transaction by its hash.

```
GET /tx/{hash}
GET /tx/{hash}/hex
GET /tx/{hash}/json
```

**Parameters:**
- `hash`: Transaction hash (64 characters hexadecimal)

**Response Formats:**
- `BINARY_STREAM`: Raw transaction data
- `HEX`: Hexadecimal string of transaction data
- `JSON`: Structured transaction data

#### Get Multiple Transactions
Retrieves multiple transactions in a single request.

```
POST /subtree/:hash/txs
```

**Request Body:** Concatenated 32-byte transaction hashes

**Response:** Concatenated transaction data in binary format

#### Get Transaction Metadata
Retrieves metadata about a transaction.

```
GET /txmeta/{hash}/json
GET /txmeta_raw/{hash}
GET /txmeta_raw/{hash}/hex
GET /txmeta_raw/{hash}/json
```

**Parameters:**
- `hash`: Transaction hash (64 characters hexadecimal)

### Block Endpoints

#### Get Block
Retrieves a block by its hash.

```
GET /block/{hash}
GET /block/{hash}/hex
GET /block/{hash}/json
```

**Parameters:**
- `hash`: Block hash (64 characters hexadecimal)

#### Get Blocks List
Retrieves a paginated list of blocks.

```
GET /blocks
```

**Query Parameters:**
- `offset`: Number of blocks to skip (default: 0)
- `limit`: Maximum number of blocks to return (default: 20, max: 100)
- `includeOrphans`: Include orphaned blocks (default: false)

#### Get Block Fork Information
Retrieves fork information for a block.

```
GET /block/{hash}/forks
```

**Parameters:**
- `hash`: Block hash (64 characters hexadecimal)

#### Get Block Statistics
Retrieves statistical information about the blockchain.

```
GET /blockstats
```

#### Get Block Graph Data
Retrieves time-series data about blocks.

```
GET /blockgraphdata/{period}
```

**Parameters:**
- `period`: Time period ("2h", "6h", "12h", "24h", "1w", "1m", "3m")



### Block Header Endpoints

#### Get Block Headers
Retrieves multiple consecutive block headers starting from a specific hash.

```
GET /headers/{hash}
GET /headers/{hash}/hex
GET /headers/{hash}/json
```

**Parameters:**
- `hash`: Starting block hash (64 characters hexadecimal)
- `n`: Number of headers to retrieve (query parameter, default: 100, max: 1000)

#### Get Single Block Header
Retrieves a specific block header.

```
GET /header/{hash}
GET /header/{hash}/hex
GET /header/{hash}/json
```

**Parameters:**
- `hash`: Block hash (64 characters hexadecimal)

#### Get Best Block Header
Retrieves the most recent block header.

```
GET /bestblockheader
GET /bestblockheader/hex
GET /bestblockheader/json
```

### Subtree Endpoints

#### Get Subtree
Retrieves a subtree by its hash.

```
GET /subtree/{hash}
GET /subtree/{hash}/hex
GET /subtree/{hash}/json
```

**Parameters:**
- `hash`: Subtree hash (64 characters hexadecimal)

#### Get Subtree Transactions
Retrieves transactions contained in a subtree.

```
GET /subtree/{hash}/txs/json
```

**Parameters:**
- `hash`: Subtree hash (64 characters hexadecimal)
- `offset`: Number of transactions to skip (query parameter)
- `limit`: Maximum number of transactions to return (query parameter)

#### Get Block Subtrees
Retrieves all subtrees for a specific block.

```
GET /block/{hash}/subtrees/json
```

**Parameters:**
- `hash`: Block hash (64 characters hexadecimal)
- `offset`: Number of subtrees to skip (query parameter)
- `limit`: Maximum number of subtrees to return (query parameter)

### UTXO Endpoints

#### Get UTXO
Retrieves information about an unspent transaction output.

```
GET /utxo/{hash}
GET /utxo/{hash}/hex
GET /utxo/{hash}/json
```

**Parameters:**
- `hash`: UTXO hash (64 characters hexadecimal)

#### Get UTXOs by Transaction
Retrieves all UTXOs associated with a transaction.

```
GET /utxos/{hash}/json
```

**Parameters:**
- `hash`: Transaction hash (64 characters hexadecimal)

#### Get Balance
Retrieves the current UTXO set balance information.

```
GET /balance
```

### Search Endpoint

#### Search Blockchain Entities
Searches for blocks, transactions, subtrees, or UTXOs.

```
GET /search
```

**Query Parameters:**
- `q`: Search query (64-character hex hash or numeric block height)

**Search Types:**
- Block hash
- Transaction hash
- Subtree hash
- UTXO hash
- Block height (numeric value)

### Health and Status Endpoints

#### Liveness Check
Verifies service is running.

```
GET /alive
```

#### Health Check
Performs comprehensive health check of service and dependencies.

```
GET /health
```

### Error Handling

#### Error Response Format
```json
{
    "status": <HTTP status code>,
    "code": <application error code>,
    "error": "error message"
}
```

#### Common Error Codes

| HTTP Status | Error Code | Description |
|------------|------------|-------------|
| 400 | 1 | Missing query parameter |
| 400 | 2 | Invalid hash format |
| 400 | 3 | Block search error |
| 400 | 4 | Subtree search error |
| 400 | 5 | Transaction search error |
| 400 | 6 | UTXO search error |
| 400 | 7 | Invalid query format |
| 404 | - | Resource not found |
| 500 | - | Internal server error |

#### Rate Limiting
The service implements rate limiting with the following defaults:

- Maximum concurrent connections: 16
- Maximum requests per minute: 60

### Legacy Support

#### Get Legacy Block Format
Retrieves a block in legacy Bitcoin protocol format.

```
GET /rest/block/{hash}.bin
GET /block_legacy/{hash}
```

**Parameters:**
- `hash`: Block hash (64 characters hexadecimal)

### Monitoring

The service provides Prometheus metrics for monitoring:

- Transaction processing counts
- Block retrieval statistics
- UTXO operations
- Response times
- Error rates

Metric endpoints:
```
GET /stats      # Current statistics
GET /reset      # Reset statistics counters
```

### Security Considerations

1. **Response Signing**
    - Responses can be cryptographically signed
    - Signature provided in X-Signature header
    - Uses Ed25519 signing algorithm

2. **TLS Support**
    - HTTPS available when configured
    - Requires valid certificate and key files
    - Controlled via securityLevelHTTP setting

3. **CORS**
    - Cross-Origin Resource Sharing enabled
    - Allows GET methods from any origin
    - Configurable through middleware

4. **Compression**
    - Gzip compression supported
    - Automatically applied when appropriate
    - Reduces bandwidth usage
