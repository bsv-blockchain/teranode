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
repository *repository.Repository
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

#### GetUtxoBytes

```go
func (repo *Repository) GetUtxoBytes(ctx context.Context, spend *utxo.Spend) ([]byte, error)
```

Retrieves the raw bytes of spending transaction ID for a specific UTXO.

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

- `asset_httpListenAddress`: HTTP listen address
- `asset_centrifuge_disable`: Whether to disable Centrifuge server
- `asset_centrifugeListenAddress`: Centrifuge listen address
- `http_sign_response`: Whether to sign HTTP responses
- `p2p_private_key`: Private key for signing responses
- `asset_apiPrefix`: API prefix (default: "/api/v1")
- `securityLevelHTTP`: Security level for HTTP (0 for HTTP, non-zero for HTTPS)
- `server_certFile`: Certificate file for HTTPS
- `server_keyFile`: Key file for HTTPS

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

The Asset Service provides various API endpoints for interacting with blockchain data:

- `/alive`: Check if the service is alive
- `/health`: Perform a health check
- `/rest/block/:hash.bin`: Get a legacy block (binary stream)
- `/api/v1/tx/:hash`: Get a transaction (various formats)
- `/api/v1/txmeta/:hash/json`: Get transaction metadata
- `/api/v1/subtree/:hash`: Get a subtree (various formats)
- `/api/v1/headers/:hash`: Get block headers (various formats)
- `/api/v1/blocks`: Get blocks
- `/api/v1/block/:hash`: Get a block by hash (various formats)
- `/api/v1/search`: Search functionality
- `/api/v1/blockstats`: Get block statistics
- `/api/v1/blockgraphdata/:period`: Get block graph data
- `/api/v1/lastblocks`: Get the last N blocks
- `/api/v1/utxo/:hash`: Get UTXO information (various formats)
- `/api/v1/balance`: Get balance information
- `/api/v1/bestblockheader`: Get the best block header (various formats)


All API endpoints are prefixed with `/api/v1` unless otherwise specified.

### Response Formats

Most endpoints support multiple response formats:

- **BINARY_STREAM**: Raw binary data (`application/octet-stream`)
- **HEX**: Hexadecimal string representation (`text/plain`)
- **JSON**: Structured JSON data (`application/json`)

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
POST /txs
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
