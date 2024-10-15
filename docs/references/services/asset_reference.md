# Asset Service Reference Documentation

## Types

### Server

```go
type Server struct {
logger              ulogger.Logger
utxoStore           utxo.Store
txStore             blob.Store
subtreeStore        blob.Store
blockPersisterStore blob.Store
httpAddr            string
httpServer          *http_impl.HTTP
centrifugeAddr      string
centrifugeServer    *centrifuge_impl.Centrifuge
blockchainClient    blockchain.ClientI
}
```

The `Server` type is the main structure for the Asset Service. It contains various components for managing stores, HTTP and Centrifuge servers, and blockchain interactions.

### Repository

```go
type Repository struct {
logger              ulogger.Logger
UtxoStore           utxo.Store
TxStore             blob.Store
SubtreeStore        blob.Store
BlockPersisterStore blob.Store
BlockchainClient    blockchain.ClientI
CoinbaseProvider    coinbase_api.CoinbaseAPIClient
}
```

The `Repository` type provides methods for interacting with various data stores and services.

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
func NewServer(logger ulogger.Logger, utxoStore utxo.Store, txStore blob.Store, subtreeStore blob.Store, blockPersisterStore blob.Store, blockchainClient blockchain.ClientI) *Server
```

Creates a new instance of the `Server`.

#### Health

```go
func (v *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Performs health checks on the Asset Service.

#### Init

```go
func (v *Server) Init(ctx context.Context) (err error)
```

Initializes the Asset Service.

#### Start

```go
func (v *Server) Start(ctx context.Context) error
```

Starts the Asset Service.

#### Stop

```go
func (v *Server) Stop(ctx context.Context) error
```

Stops the Asset Service.

### Repository

#### NewRepository

```go
func NewRepository(logger ulogger.Logger, utxoStore utxo.Store, txStore blob.Store, blockchainClient blockchain.ClientI, subtreeStore blob.Store, blockPersisterStore blob.Store) (*Repository, error)
```

Creates a new instance of the `Repository`.

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

#### GetSubtree

```go
func (repo *Repository) GetSubtree(ctx context.Context, hash *chainhash.Hash) (*util.Subtree, error)
```

Retrieves a subtree by its hash.

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

#### GetBalance

```go
func (repo *Repository) GetBalance(ctx context.Context) (uint64, uint64, error)
```

Retrieves the balance information.

### HTTP

#### New

```go
func New(logger ulogger.Logger, repo *repository.Repository) (*HTTP, error)
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
