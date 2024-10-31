# Block Persister Service Reference Documentation

## Types

### Server

```go
type Server struct {
ctx                            context.Context
logger                         ulogger.Logger
blockStore                     blob.Store
subtreeStore                   blob.Store
utxoStore                      utxo.Store
stats                          *gocore.Stat
blockchainClient               blockchain.ClientI
blocksFinalKafkaConsumerClient *kafka.KafkaConsumerGroup
kafkaHealthURL                 *url.URL
}
```

The `Server` type is the main structure for the Block Persister Service. It contains various components for managing stores, Kafka consumers, and blockchain interactions.

## Functions

### Server

#### New

```go
func New(ctx context.Context, logger ulogger.Logger, blockStore blob.Store, subtreeStore blob.Store, utxoStore utxo.Store, blockchainClient blockchain.ClientI) *Server
```

Creates a new instance of the `Server`.

#### Health

```go
func (u *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Performs health checks on the Block Persister Service.

#### Init

```go
func (u *Server) Init(ctx context.Context) (err error)
```

Initializes the Block Persister Service.

#### Start

```go
func (u *Server) Start(ctx context.Context) error
```

Starts the Block Persister Service.

#### Stop

```go
func (u *Server) Stop(_ context.Context) error
```

Stops the Block Persister Service.

#### blocksFinalHandler

```go
func (u *Server) blocksFinalHandler(msg *kafka.KafkaMessage) error
```

Handles incoming Kafka messages for finalized blocks.

#### persistBlock

```go
func (u *Server) persistBlock(ctx context.Context, hash *chainhash.Hash, blockBytes []byte) error
```

Persists a block to storage.

#### ProcessSubtree

```go
func (u *Server) ProcessSubtree(pCtx context.Context, subtreeHash chainhash.Hash, coinbaseTx *bt.Tx, utxoDiff *utxopersister.UTXOSet) error
```

Processes a subtree, validating and storing its transactions.

### Utility Functions

#### WriteTxs

```go
func WriteTxs(ctx context.Context, logger ulogger.Logger, writer *filestorer.FileStorer, txMetaSlice []*meta.Data, utxoDiff *utxopersister.UTXOSet) error
```

Writes transactions to storage and processes them for UTXO updates.

## Key Processes

### Block Persistence

1. The service listens for finalized block messages from Kafka.
2. When a message is received, it's handled by `blocksFinalHandler`.
3. The handler attempts to acquire a lock for the block to prevent duplicate processing.
4. If the lock is acquired, `persistBlock` is called to process the block.
5. `persistBlock` creates a new UTXO diff, processes all subtrees in the block, and writes the block to disk.

### Subtree Processing

1. `ProcessSubtree` is called for each subtree in a block.
2. It retrieves the subtree data from storage and deserializes it.
3. Transaction metadata for each transaction in the subtree is loaded.
4. Transactions are written to storage using `WriteTxs`.
5. The UTXO set is updated for each transaction.

### UTXO Management

- The service maintains a UTXO set that's updated as blocks are processed.
- UTXO diffs are created for each block and applied to the main UTXO set.

## Configuration

The Block Persister Service uses configuration values from the `gocore.Config()` function, including:

- `kafka_blocksFinalConfig`: Kafka configuration for finalized block messages
- `blockPersister_httpListenAddress`: HTTP listen address for the block store server
- `blockstore`: URL for the block store
- `fsm_state_restore`: Whether to restore the FSM state on startup

## Dependencies

The Block Persister Service depends on several other components and services:

- Block Store (blob.Store)
- Subtree Store (blob.Store)
- UTXO Store (utxo.Store)
- Blockchain Client (blockchain.ClientI)
- Kafka Consumer

These dependencies are injected into the `Server` structure during initialization.

## Error Handling

The service implements robust error handling, particularly in the Kafka message processing:

- Recoverable errors (e.g., storage errors, service errors) cause the message to be reprocessed.
- Unrecoverable errors (e.g., processing errors, invalid arguments) are logged, and the message is marked as completed to prevent infinite retry loops.

## Concurrency

- The service uses goroutines and error groups to process subtrees concurrently.
- A quorum-based locking mechanism is used to prevent duplicate processing of blocks.

## Metrics

The service initializes Prometheus metrics for monitoring various aspects of its operation, including:

- Block persistence duration
- Subtree validation duration
- UTXO set updates

These metrics can be used to monitor the performance and health of the Block Persister Service.
