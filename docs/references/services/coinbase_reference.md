# Coinbase Service Reference Documentation

## Types

### Server

```go
type Server struct {
coinbase_api.UnimplementedCoinbaseAPIServer
blockchainClient bc.ClientI
coinbase         *Coinbase
logger           ulogger.Logger
stats            *gocore.Stat
}
```

The `Server` type is the main structure for the Coinbase Service. It implements the `UnimplementedCoinbaseAPIServer` and contains components for managing coinbase transactions and interactions with the blockchain.

### Coinbase

```go
type Coinbase struct {
db               *usql.DB
engine           util.SQLEngine
blockchainClient bc.ClientI
store            blockchain.Store
distributor      *distributor.Distributor
privateKey       *bec.PrivateKey
running          bool
blockFoundCh     chan processBlockFound
catchupCh        chan processBlockCatchup
logger           ulogger.Logger
address          string
dbTimeout        time.Duration
peerSync         *p2p.PeerHeight
waitForPeers     bool
g                *errgroup.Group
gCtx             context.Context
stats            *gocore.Stat
minConfirmations uint64
}
```

The `Coinbase` type handles the core logic for managing coinbase transactions, UTXO management, and interactions with the blockchain.

## Functions

### Server

#### New

```go
func New(logger ulogger.Logger, blockchainClient bc.ClientI) *Server
```

Creates a new instance of the `Server`.

#### Health

```go
func (s *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Performs health checks on the Coinbase Service.

#### HealthGRPC

```go
func (s *Server) HealthGRPC(ctx context.Context, _ *emptypb.Empty) (*coinbase_api.HealthResponse, error)
```

Performs a gRPC health check on the Coinbase Service.

#### Init

```go
func (s *Server) Init(ctx context.Context) error
```

Initializes the Coinbase Service.

#### Start

```go
func (s *Server) Start(ctx context.Context) error
```

Starts the Coinbase Service.

#### Stop

```go
func (s *Server) Stop(ctx context.Context) error
```

Stops the Coinbase Service.

#### RequestFunds

```go
func (s *Server) RequestFunds(ctx context.Context, req *coinbase_api.RequestFundsRequest) (*coinbase_api.RequestFundsResponse, error)
```

Handles requests for funds from the coinbase.

#### DistributeTransaction

```go
func (s *Server) DistributeTransaction(ctx context.Context, req *coinbase_api.DistributeTransactionRequest) (*coinbase_api.DistributeTransactionResponse, error)
```

Distributes a transaction across the network.

#### GetBalance

```go
func (s *Server) GetBalance(ctx context.Context, _ *emptypb.Empty) (*coinbase_api.GetBalanceResponse, error)
```

Retrieves the current balance of the coinbase.

### Coinbase

#### NewCoinbase

```go
func NewCoinbase(logger ulogger.Logger, blockchainClient bc.ClientI, store blockchain.Store) (*Coinbase, error)
```

Creates a new instance of `Coinbase`.

#### Init

```go
func (c *Coinbase) Init(ctx context.Context) (err error)
```

Initializes the Coinbase instance.

#### processCoinbase

```go
func (c *Coinbase) processCoinbase(ctx context.Context, blockId uint64, blockHash *chainhash.Hash, coinbaseTx *bt.Tx) error
```

Processes a coinbase transaction.

#### splitUtxo

```go
func (c *Coinbase) splitUtxo(ctx context.Context, utxo *bt.UTXO) error
```

Splits a UTXO into smaller outputs.

#### RequestFunds

```go
func (c *Coinbase) RequestFunds(ctx context.Context, address string, disableDistribute bool) (*bt.Tx, error)
```

Handles requests for funds from the coinbase.

#### DistributeTransaction

```go
func (c *Coinbase) DistributeTransaction(ctx context.Context, tx *bt.Tx) ([]*distributor.ResponseWrapper, error)
```

Distributes a transaction across the network.

#### insertCoinbaseUTXOs

```go
func (c *Coinbase) insertCoinbaseUTXOs(ctx context.Context, blockId uint64, tx *bt.Tx) error
```

Inserts coinbase UTXOs into the database.

#### insertSpendableUTXOs

```go
func (c *Coinbase) insertSpendableUTXOs(ctx context.Context, tx *bt.Tx) error
```

Inserts spendable UTXOs into the database.

#### getBalance

```go
func (c *Coinbase) getBalance(ctx context.Context) (*coinbase_api.GetBalanceResponse, error)
```

Retrieves the current balance of the coinbase.

## Key Processes

### Coinbase Transaction Processing

1. When a new block is found, `processBlock` is called.
2. The coinbase transaction in the block is processed using `processCoinbase`.
3. Coinbase UTXOs are inserted into the database using `insertCoinbaseUTXOs`.
4. After a certain number of confirmations, UTXOs become spendable and are processed by `createSpendingUtxos`.

### UTXO Management

1. Spendable UTXOs are split into smaller outputs using `splitUtxo`.
2. New spendable UTXOs are inserted into the database using `insertSpendableUTXOs`.
3. The balance of spendable UTXOs is tracked and can be retrieved using `getBalance`.

### Fund Distribution

1. `RequestFunds` handles requests for funds from the coinbase.
2. It selects a spendable UTXO and creates a transaction to split it into multiple outputs.
3. The transaction is distributed across the network using `DistributeTransaction`.

## Configuration

The Coinbase Service uses various configuration values, including:

- `coinbase_wallet_private_key`: Private key for the coinbase wallet
- `distributor_backoff_duration`: Backoff duration for the transaction distributor
- `distributor_max_retries`: Maximum number of retries for transaction distribution
- `distributor_failure_tolerance`: Failure tolerance for transaction distribution
- `blockchain_store_dbTimeoutMillis`: Timeout for database operations
- `coinbase_wait_for_peers`: Whether to wait for peers before processing
- `blockvalidation_maxPreviousBlockHeadersToCheck`: Number of previous block headers to check for confirmations

## Dependencies

The Coinbase Service depends on several other components and services:

- Blockchain Client
- Blockchain Store
- Transaction Distributor
- Peer Sync Service
- SQL Database (PostgreSQL or SQLite)

These dependencies are injected into the `Server` and `Coinbase` structures during initialization.

## Database Schema

The service uses two main tables:

1. `coinbase_utxos`: Stores coinbase UTXOs
2. `spendable_utxos`: Stores UTXOs that are available for spending

Additional tables and functions are created for PostgreSQL to optimize balance tracking and UTXO management.

## Error Handling

The service implements robust error handling, particularly in database operations and transaction processing. Errors are wrapped with custom error types to provide context and aid in debugging.

## Concurrency

- The service uses goroutines and error groups to process blocks and UTXOs concurrently.
- A lock-free queue is used for processing new blocks and catching up with the blockchain.

## Metrics

The service initializes Prometheus metrics for monitoring various aspects of its operation, including:

- Health check responses
- Fund requests
- Transaction distribution
- Balance retrieval

These metrics can be used to monitor the performance and health of the Coinbase Service.

## Security

- The service uses a private key to sign transactions.
- Only P2PKH (Pay to Public Key Hash) coinbase outputs are supported.
- The service implements a mechanism to wait for peer synchronization before processing blocks, enhancing security in a distributed environment.
