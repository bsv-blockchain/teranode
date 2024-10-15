# Alert Service Reference Documentation

## Types

### Server

```go
type Server struct {
alert_api.UnimplementedAlertAPIServer
logger              ulogger.Logger
stats               *gocore.Stat
blockchainClient    blockchain.ClientI
utxoStore           utxo.Store
blockassemblyClient *blockassembly.Client
appConfig           *config.Config
p2pServer           *p2p.Server
webServer           *webserver.Server
}
```

The `Server` type is the main structure for the Alert Service. It implements the `UnimplementedAlertAPIServer` and contains various components for managing alerts, blockchain interactions, and P2P communication.

### Node

```go
type Node struct {
logger              ulogger.Logger
blockchainClient    blockchain.ClientI
utxoStore           utxo.Store
blockassemblyClient *blockassembly.Client
}
```

The `Node` type represents a node in the network and provides methods for interacting with the blockchain and managing UTXOs.

## Functions

### Server

#### New

```go
func New(logger ulogger.Logger, blockchainClient blockchain.ClientI, utxoStore utxo.Store, blockassemblyClient *blockassembly.Client) *Server
```

Creates a new instance of the `Server`.

#### Health

```go
func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Performs health checks on the Alert Service.

#### HealthGRPC

```go
func (s *Server) HealthGRPC(ctx context.Context, _ *emptypb.Empty) (*alert_api.HealthResponse, error)
```

Performs a gRPC health check on the Alert Service.

#### Init

```go
func (s *Server) Init(ctx context.Context) (err error)
```

Initializes the Alert Service.

#### Start

```go
func (s *Server) Start(ctx context.Context) (err error)
```

Starts the Alert Service.

#### Stop

```go
func (s *Server) Stop(ctx context.Context) error
```

Stops the Alert Service.

#### loadConfig

```go
func (s *Server) loadConfig(ctx context.Context, models []interface{}, isTesting bool) (err error)
```

Loads the configuration for the Alert Service.

#### requireP2P

```go
func (s *Server) requireP2P() error
```

Ensures the P2P configuration is valid.

#### createPrivateKeyDirectory

```go
func (s *Server) createPrivateKeyDirectory() error
```

Creates the private key directory for P2P communication.

#### loadDatastore

```go
func (s *Server) loadDatastore(ctx context.Context, models []interface{}, dbURL *url.URL) error
```

Loads an instance of Datastore into the dependencies.

### Node

#### NewNodeConfig

```go
func NewNodeConfig(logger ulogger.Logger, blockchainClient blockchain.ClientI, utxoStore utxo.Store, blockassemblyClient *blockassembly.Client) config.NodeInterface
```

Creates a new instance of the `Node`.

#### BestBlockHash

```go
func (n *Node) BestBlockHash(ctx context.Context) (string, error)
```

Retrieves the hash of the best block in the blockchain.

#### InvalidateBlock

```go
func (n *Node) InvalidateBlock(ctx context.Context, blockHashStr string) error
```

Invalidates a block in the blockchain.

#### BanPeer

```go
func (n *Node) BanPeer(ctx context.Context, peer string) error
```

Adds the peer's IP address to the ban list.

#### UnbanPeer

```go
func (n *Node) UnbanPeer(ctx context.Context, peer string) error
```

Removes the peer's IP address from the ban list.

#### AddToConsensusBlacklist

```go
func (n *Node) AddToConsensusBlacklist(ctx context.Context, funds []models.Fund) (*models.BlacklistResponse, error)
```

Adds funds to the consensus blacklist, setting specified UTXOs as un-spendable.

#### AddToConfiscationTransactionWhitelist

```go
func (n *Node) AddToConfiscationTransactionWhitelist(ctx context.Context, txs []models.ConfiscationTransactionDetails) (*models.AddToConfiscationTransactionWhitelistResponse, error)
```

Re-assigns UTXOs to confiscation transactions, allowing them to be spent.

#### extractPublicKey

```go
func extractPublicKey(scriptSig []byte) ([]byte, error)
```

Extracts the public key from a P2PKH scriptSig.

## Configuration

The Alert Service uses a configuration structure (`config.Config`) that includes settings for:

- Alert processing interval
- Request logging
- Datastore configuration
- P2P configuration
- RPC connections
- Genesis keys

The configuration is loaded from the Teranode settings file and environment variables.

## P2P Communication

The Alert Service uses a P2P server for communication. The P2P configuration includes:

- IP and port
- DHT mode
- Protocol ID
- Topic name
- Private key

## Datastore

The Alert Service supports multiple database backends:

- SQLite (including in-memory)
- PostgreSQL
- MySQL

The datastore is configured with options for debugging, connection pooling, and auto-migration of models.

## Consensus Blacklist and Confiscation Transactions

The Alert Service provides functionality to manage a consensus blacklist and confiscation transactions:

- `AddToConsensusBlacklist`: Freezes or unfreezes specified UTXOs.
- `AddToConfiscationTransactionWhitelist`: Re-assigns UTXOs to allow spending by confiscation transactions.

These functions interact with the UTXO store to manage the state of specific outputs in the blockchain.
