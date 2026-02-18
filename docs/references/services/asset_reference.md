# Asset Service Reference Documentation

## Types

### Server Structure

```go
type Server struct {
    logger                ulogger.Logger
    settings              *settings.Settings
    utxoStore             utxo.Store
    txStore               blob.Store
    subtreeStore          blob.Store
    blockPersisterStore   blob.Store
    httpAddr              string
    httpServer            *httpimpl.HTTP
    centrifugeAddr        string
    centrifugeServer      *centrifuge_impl.Centrifuge
    blockchainClient      blockchain.ClientI
    blockvalidationClient blockvalidation.Interface
    p2pClient             p2p.ClientI
}
```

The `Server` type is the main structure for the Asset Service. It coordinates between different storage backends and provides both HTTP and Centrifuge interfaces for accessing blockchain data.

### Repository Structure

```go
type Repository struct {
    logger                ulogger.Logger
    settings              *settings.Settings
    UtxoStore             utxo.Store
    TxStore               blob.Store
    SubtreeStore          blob.Store
    BlockPersisterStore   blob.Store
    BlockchainClient      blockchain.ClientI
    BlockvalidationClient blockvalidation.Interface
    P2PClient             p2p.ClientI
}
```

The `Repository` type provides access to blockchain data storage and retrieval operations. It implements the necessary interfaces to interact with various data stores and blockchain clients.

### HTTP Structure

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

### Server Functions

#### NewServer

```go
func NewServer(logger ulogger.Logger, tSettings *settings.Settings, utxoStore utxo.Store, txStore blob.Store, subtreeStore blob.Store, blockPersisterStore blob.Store, blockchainClient blockchain.ClientI, blockvalidationClient blockvalidation.Interface, p2pClient p2p.ClientI) *Server
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

### Repository Functions

#### NewRepository

```go
func NewRepository(logger ulogger.Logger, tSettings *settings.Settings, utxoStore utxo.Store, txStore blob.Store, blockchainClient blockchain.ClientI, blockvalidationClient blockvalidation.Interface, subtreeStore blob.Store, blockPersisterStore blob.Store, p2pClient p2p.ClientI) (*Repository, error)
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
func (repo *Repository) GetBlockHeadersToCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []*chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
```

Retrieves block headers from the target hash back to the common ancestor with the provided block locator.

#### GetBlockHeadersFromHeight

```go
func (repo *Repository) GetBlockHeadersFromHeight(ctx context.Context, height, limit uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
```

Retrieves block headers starting from a specific height up to the specified limit.

#### GetSubtree

```go
func (repo *Repository) GetSubtree(ctx context.Context, hash *chainhash.Hash) (*subtree.Subtree, error)
```

Retrieves a subtree by its hash.

#### GetSubtreeBytes

```go
func (repo *Repository) GetSubtreeBytes(ctx context.Context, hash *chainhash.Hash) ([]byte, error)
```

Retrieves the raw bytes of a subtree.

#### GetSubtreeData

```go
func (repo *Repository) GetSubtreeData(ctx context.Context, hash *chainhash.Hash) (*subtree.Data, error)
```

Retrieves the parsed data contents of a subtree.

#### GetSubtreeTxIDsReader

```go
func (repo *Repository) GetSubtreeTxIDsReader(ctx context.Context, hash *chainhash.Hash) (io.ReadCloser, error)
```

Provides a reader interface for streaming subtree transaction IDs.

#### GetSubtreeDataReader

```go
func (repo *Repository) GetSubtreeDataReader(ctx context.Context, subtreeHash *chainhash.Hash) (io.ReadCloser, error)
```

Provides a reader interface for accessing subtree data from the subtree store.

#### GetSubtreeDataReaderFromBlockPersister

```go
func (repo *Repository) GetSubtreeDataReaderFromBlockPersister(ctx context.Context, hash *chainhash.Hash) (io.ReadCloser, error)
```

Provides a reader interface for accessing subtree data from the block persister store.

#### GetSubtreeTransactions

```go
func (repo *Repository) GetSubtreeTransactions(ctx context.Context, hash *chainhash.Hash) (map[chainhash.Hash]*bt.Tx, error)
```

Retrieves all transactions contained in a subtree, returned as a map keyed by transaction hash.

#### GetSubtreeExists

```go
func (repo *Repository) GetSubtreeExists(ctx context.Context, hash *chainhash.Hash) (bool, error)
```

Checks if a subtree with the given hash exists in the store.

#### GetSubtreeHead

```go
func (repo *Repository) GetSubtreeHead(ctx context.Context, hash *chainhash.Hash) (*subtree.Subtree, int, error)
```

Retrieves only the head portion of a subtree, containing fees and size information.

#### FindBlocksContainingSubtree

```go
func (repo *Repository) FindBlocksContainingSubtree(ctx context.Context, subtreeHash *chainhash.Hash) ([]uint32, []uint32, []int, error)
```

Finds all blocks that contain the specified subtree, returning block heights, block IDs, and subtree indices.

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

#### GetTxMeta

```go
func (repo *Repository) GetTxMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error)
```

Retrieves raw transaction metadata from the UTXO store.

#### GetTransactionMeta

```go
func (repo *Repository) GetTransactionMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error)
```

Retrieves transaction metadata including block height and mined status.

#### GetBlocks

```go
func (repo *Repository) GetBlocks(ctx context.Context, hash *chainhash.Hash, n uint32) ([]*model.Block, error)
```

Retrieves N consecutive blocks starting from the specified hash.

#### GetBlocksByHeight

```go
func (repo *Repository) GetBlocksByHeight(ctx context.Context, startHeight, endHeight uint32) ([]*model.Block, error)
```

Retrieves blocks within a height range.

#### GetBlockHeadersFromCommonAncestor

```go
func (repo *Repository) GetBlockHeadersFromCommonAncestor(ctx context.Context, hashTarget *chainhash.Hash, blockLocatorHashes []chainhash.Hash, maxHeaders uint32) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
```

Retrieves block headers from the common ancestor forward to the target hash, using the provided block locator to find the fork point.

#### GetLegacyBlockReader

```go
func (repo *Repository) GetLegacyBlockReader(ctx context.Context, hash *chainhash.Hash, wireBlock ...bool) (*io.PipeReader, error)
```

Returns a streaming reader for a block in legacy wire format, suitable for serving to legacy Bitcoin nodes.

#### GetBlockByID

```go
func (repo *Repository) GetBlockByID(ctx context.Context, id uint64) (*model.Block, error)
```

Retrieves a block by its internal database ID.

#### GetBlockchainClient

```go
func (repo *Repository) GetBlockchainClient() blockchain.ClientI
```

Returns the blockchain client used by the repository.

#### GetBlockvalidationClient

```go
func (repo *Repository) GetBlockvalidationClient() blockvalidation.Interface
```

Returns the block validation client used by the repository.

#### GetP2PClient

```go
func (repo *Repository) GetP2PClient() p2p.ClientI
```

Returns the P2P client used by the repository.

### HTTP Functions

#### New

```go
func New(logger ulogger.Logger, tSettings *settings.Settings, repo *repository.Repository) (*HTTP, error)
```

Creates a new instance of the HTTP server.

#### HTTP Init

```go
func (h *HTTP) Init(_ context.Context) error
```

Initializes the HTTP server.

#### HTTP Start

```go
func (h *HTTP) Start(ctx context.Context, addr string) error
```

Starts the HTTP server.

#### HTTP Stop

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

The Asset Service uses the following configuration values from the settings system:

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

- **POST `/api/v1/txs`**
    - Purpose: Batch retrieve multiple transactions (legacy endpoint)
    - Request Body: Concatenated 32-byte transaction hashes
    - Returns: Concatenated transactions (binary)

- **POST `/api/v1/subtree/:hash/txs`**
    - Purpose: Batch retrieve multiple transactions for a subtree
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
    - Purpose: Get fork information and tree structure for a block
    - URL Parameters: `hash` - Block hash (hex string)
    - Query Parameters:

        - `limit` (integer, optional, default: 20, max: 100) - Maximum blocks to include in tree
    - Returns: Fork data (JSON) with parent-child block relationships

- **GET `/api/v1/blocks`**
    - Purpose: Get paginated blocks list
    - Query Parameters:

        - `offset` (integer, optional, default: 0) - Number of blocks to skip from tip
        - `limit` (integer, optional, default: 20, max: 100) - Maximum blocks to return
        - `includeOrphans` (boolean, optional, default: false) - Include orphaned blocks
    - Returns: Blocks list (JSON) with pagination metadata

- **GET `/api/v1/blocks/:hash`**
    - Purpose: Get N consecutive blocks starting from specified hash
    - URL Parameters: `hash` - Starting block hash (hex string)
    - Query Parameters:

        - `n` (integer, optional, default: 100, max: 1000) - Number of blocks to retrieve
    - Returns: Block data (binary)
    - Also available: `/api/v1/blocks/:hash/hex` (hex), `/api/v1/blocks/:hash/json` (JSON)

- **GET `/api/v1/lastblocks`**
    - Purpose: Get most recent blocks
    - Query Parameters:

        - `n` (integer, optional, default: 10) - Number of blocks to retrieve
        - `fromHeight` (unsigned integer, optional, default: 0) - Starting block height
        - `includeOrphans` (boolean, optional, default: false) - Include orphaned blocks
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

- **GET `/api/v1/block_locator`**
    - Purpose: Get block locator hashes for blockchain synchronization
    - Query Parameters:

        - `hash` (string, optional) - Block hash to start from (default: best block)
        - `height` (unsigned integer, optional) - Block height (ignored if hash provided)
    - Returns: JSON array of block hashes at exponentially increasing distances
    - Response Format: `{ "block_locator": ["<hash1>", "<hash2>", ...] }`
    - Notes: Uses exponential backoff algorithm; always includes genesis block

### Block Header Endpoints

- **GET `/api/v1/header/:hash`**
    - Purpose: Get single block header (binary)
    - Parameters: `hash` - Block hash (hex string)
    - Returns: Block header (binary)

- **GET `/api/v1/headers/:hash`**
    - Purpose: Get N consecutive block headers starting from specified hash
    - URL Parameters: `hash` - Starting block hash (hex string)
    - Query Parameters:

        - `n` (integer, optional, default: 100, max: 1000) - Number of headers to retrieve
    - Returns: Block headers (binary, 80 bytes per header)
    - Also available: `/api/v1/headers/:hash/hex` (hex), `/api/v1/headers/:hash/json` (JSON)

- **GET `/api/v1/headers_to_common_ancestor/:hash`**
    - Purpose: Get headers from target back to common ancestor (binary)
    - Parameters: `hash` - Target hash, `locator` - Comma-separated block hashes
    - Returns: Block headers (binary)
    - Also available: `/api/v1/headers_to_common_ancestor/:hash/hex` (hex), `/api/v1/headers_to_common_ancestor/:hash/json` (JSON)

- **GET `/api/v1/headers_from_common_ancestor/:hash`**
    - Purpose: Get headers from common ancestor forward to target (binary)
    - Parameters: `hash` - Target hash, `locator` - Comma-separated block locator hashes
    - Returns: Block headers (binary)
    - Also available: `/api/v1/headers_from_common_ancestor/:hash/hex` (hex), `/api/v1/headers_from_common_ancestor/:hash/json` (JSON)

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
    - Also available: `/api/v1/subtree/:hash/hex` (hex), `/api/v1/subtree/:hash/json` (JSON)

- **GET `/api/v1/subtree_data/:hash`**
    - Purpose: Get subtree data content as a streaming download
    - Parameters: `hash` - Subtree hash (hex string)
    - Returns: Subtree data (binary stream)

- **GET `/api/v1/subtree/:hash/txs/json`**
    - Purpose: Get transactions in a subtree
    - Parameters: `hash` - Subtree hash
    - Returns: Transaction data array (JSON)

- **GET `/api/v1/block/:hash/subtrees/json`**
    - Purpose: Get paginated list of subtrees for a block
    - URL Parameters: `hash` - Block hash (hex string)
    - Query Parameters:

        - `offset` (integer, optional, default: 0) - Number of subtrees to skip
        - `limit` (integer, optional, default: 20, max: 100) - Maximum subtrees to return
    - Returns: Subtree data array (JSON) with pagination metadata

### Merkle Proof Endpoints

- **GET `/api/v1/merkle_proof/:hash`**
    - Purpose: Get merkle proof for a transaction (binary)
    - Parameters: `hash` - Transaction hash (hex string)
    - Returns: Merkle proof data in BRC-74 BUMP format (binary)
    - Also available: `/api/v1/merkle_proof/:hash/hex` (hex), `/api/v1/merkle_proof/:hash/json` (JSON)

### Chain Information Endpoints

- **GET `/api/v1/chainparams`**
    - Purpose: Get chain configuration parameters
    - Returns: Chain parameters (JSON) including network name and configuration

- **GET `/api/v1/service/heights`**
    - Purpose: Get current heights reported by each service
    - Returns: JSON object with per-service height information

### P2P and Network Endpoints

- **GET `/api/p2p/peers`**
    - Purpose: Get peer registry information from P2P service
    - Returns: Peer list (JSON) with reputation metrics, connection status, and catchup statistics
    - Response includes: peer ID, client name, blockchain height, reputation score, catchup metrics, ban status

- **POST `/api/p2p/reset-reputation`**
    - Purpose: Reset all peer reputation scores to neutral
    - Returns: Status of the reset operation
    - Security: Requires dashboard authentication

- **GET `/api/catchup/status`**
    - Purpose: Get current blockchain synchronization status
    - Returns: Catchup status (JSON) with progress metrics and peer information
    - Response includes: current/target heights, blocks fetched/validated, peer details, fork depth, previous attempt data

- **GET `/api/v1/peers`**
    - Purpose: Get peer registry (alternative endpoint under API prefix)
    - Returns: Same as `/api/p2p/peers`

- **GET `/api/v1/catchup/status`**
    - Purpose: Get catchup status (alternative endpoint under API prefix)
    - Returns: Same as `/api/catchup/status`

### Settings Endpoints

These endpoints require authentication.

- **GET `/api/v1/settings`**
    - Purpose: Get all current node settings
    - Returns: JSON object with all settings grouped by section
    - Security: Requires authentication

- **GET `/api/v1/settings/categories`**
    - Purpose: Get available settings categories
    - Returns: JSON array of settings category names
    - Security: Requires authentication

### Search Endpoints

- **GET `/api/v1/search`**
    - Purpose: Search for blockchain entities by hash or height
    - Query Parameters:

        - `q` (string, required) - Search query (64-character hex string or numeric block height)
    - Returns: Search results (JSON)
    - Response Format: `{ "type": "block|tx|subtree", "hash": "<hash>" }`
    - Search Priority: Block hash → Transaction hash → Subtree hash → Block height (if numeric)
    - Status Codes: 200 OK, 400 Bad Request (missing or invalid query), 404 Not Found

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

## Related Documents

- [Asset Server Topic Guide](../../topics/services/assetServer.md)
- [Asset Settings](../settings/services/asset_settings.md)
- [Asset Protobuf Reference](../protobuf_docs/assetProto.md)
- [Prometheus Metrics](../prometheusMetrics.md)
