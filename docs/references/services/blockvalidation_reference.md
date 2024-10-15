# Block Validation Service Reference Documentation

## Types

### Server

```go
type Server struct {
blockvalidation_api.UnimplementedBlockValidationAPIServer
logger              ulogger.Logger
blockchainClient    blockchain.ClientI
subtreeStore        blob.Store
txStore             blob.Store
utxoStore           utxo.Store
validatorClient     validator.Interface
blockFoundCh        chan processBlockFound
catchupCh           chan processBlockCatchup
blockValidation     *BlockValidation
SetTxMetaQ          *util.LockFreeQ[[][]byte]
kafkaConsumerClient *kafka.KafkaConsumerGroup
kafkaHealthURL      *url.URL
processSubtreeNotify *ttlcache.Cache[chainhash.Hash, bool]
stats               *gocore.Stat
}
```

The `Server` type is the main structure for the Block Validation Service. It implements the `UnimplementedBlockValidationAPIServer` and contains various components for managing block validation, storage, and communication with other services.

### BlockValidation

```go
type BlockValidation struct {
logger                             ulogger.Logger
blockchainClient                   blockchain.ClientI
subtreeStore                       blob.Store
subtreeTTL                         time.Duration
txStore                            blob.Store
utxoStore                          utxo.Store
recentBlocksBloomFilters           []*model.BlockBloomFilter
recentBlocksBloomFiltersMu         sync.Mutex
recentBlocksBloomFiltersExpiration time.Duration
validatorClient                    validator.Interface
subtreeValidationClient            subtreevalidation.Interface
subtreeDeDuplicator                *deduplicator.DeDuplicator
optimisticMining                   bool
lastValidatedBlocks                *expiringmap.ExpiringMap[chainhash.Hash, *model.Block]
blockExists                        *expiringmap.ExpiringMap[chainhash.Hash, bool]
subtreeExists                      *expiringmap.ExpiringMap[chainhash.Hash, bool]
subtreeCount                       atomic.Int32
blockHashesCurrentlyValidated      *util.SwissMap
blockBloomFiltersBeingCreated      *util.SwissMap
bloomFilterStats                   *model.BloomStats
setMinedChan                       chan *chainhash.Hash
revalidateBlockChan                chan revalidateBlockData
stats                              *gocore.Stat
excessiveBlockSize                 int
lastUsedBaseURL                    string
maxPreviousBlockHeadersToCheck     uint64
}
```

The `BlockValidation` type handles the core logic for validating blocks and managing related data structures.

## Functions

### Server

#### New

```go
func New(logger ulogger.Logger, subtreeStore blob.Store, txStore blob.Store, utxoStore utxo.Store, validatorClient validator.Interface, blockchainClient blockchain.ClientI) *Server
```

Creates a new instance of the `Server`.

#### Health

```go
func (u *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Performs health checks on the Block Validation Service.

#### HealthGRPC

```go
func (u *Server) HealthGRPC(ctx context.Context, _ *blockvalidation_api.EmptyMessage) (*blockvalidation_api.HealthResponse, error)
```

Performs a gRPC health check on the Block Validation Service.

#### Init

```go
func (u *Server) Init(ctx context.Context) (err error)
```

Initializes the Block Validation Service.

#### Start

```go
func (u *Server) Start(ctx context.Context) error
```

Starts the Block Validation Service.

#### Stop

```go
func (u *Server) Stop(_ context.Context) error
```

Stops the Block Validation Service.

#### BlockFound

```go
func (u *Server) BlockFound(ctx context.Context, req *blockvalidation_api.BlockFoundRequest) (*blockvalidation_api.EmptyMessage, error)
```

Handles the notification of a new block being found.

#### ProcessBlock

```go
func (u *Server) ProcessBlock(ctx context.Context, request *blockvalidation_api.ProcessBlockRequest) (*blockvalidation_api.EmptyMessage, error)
```

Processes a block, validating its contents.

#### SubtreeFound

```go
func (u *Server) SubtreeFound(_ context.Context, req *blockvalidation_api.SubtreeFoundRequest) (*blockvalidation_api.EmptyMessage, error)
```

Handles the notification of a new subtree being found.

### BlockValidation

#### NewBlockValidation

```go
func NewBlockValidation(ctx context.Context, logger ulogger.Logger, blockchainClient blockchain.ClientI, subtreeStore blob.Store, txStore blob.Store, txMetaStore utxo.Store, validatorClient validator.Interface, subtreeValidationClient subtreevalidation.Interface, bloomExpiration time.Duration) *BlockValidation
```

Creates a new instance of `BlockValidation`.

#### ValidateBlock

```go
func (u *BlockValidation) ValidateBlock(ctx context.Context, block *model.Block, baseURL string, bloomStats *model.BloomStats, disableOptimisticMining ...bool) error
```

Validates a block, including its subtrees and transactions.

#### SetTxMetaCache

```go
func (u *BlockValidation) SetTxMetaCache(ctx context.Context, hash *chainhash.Hash, txMeta *meta.Data) error
```

Sets transaction metadata in the cache.

#### SetTxMetaCacheMinedMulti

```go
func (u *BlockValidation) SetTxMetaCacheMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error
```

Sets multiple transactions as mined in the cache.

#### createAppendBloomFilter

```go
func (u *BlockValidation) createAppendBloomFilter(ctx context.Context, block *model.Block)
```

Creates and appends a Bloom filter for a block.

#### updateSubtreesTTL

```go
func (u *BlockValidation) updateSubtreesTTL(ctx context.Context, block *model.Block) (err error)
```

Updates the TTL (Time To Live) for subtrees in a block.

## Key Processes

### Block Validation

1. When a new block is found, `BlockFound` is called.
2. The block is added to the `blockFoundCh` channel for processing.
3. `processBlockFound` handles the block, which includes:
- Checking if the block already exists
- Waiting for the parent block to be mined
- Validating the block's subtrees
- Validating the block itself
4. If optimistic mining is enabled, the block is added to the blockchain before full validation.
5. After validation, the block's bloom filter is created and added to `recentBlocksBloomFilters`.

### Subtree Validation

1. `validateBlockSubtrees` is called for each block.
2. It checks if each subtree exists and validates it if necessary.
3. The `subtreeValidationClient` is used to check and validate subtrees.

### Transaction Metadata Handling

1. `SetTxMetaCache` and `SetTxMetaCacheMinedMulti` are used to manage transaction metadata.
2. The `SetTxMetaQ` queue is used to batch updates to transaction metadata.

### Bloom Filter Management

1. `createAppendBloomFilter` creates a new Bloom filter for each validated block.
2. Bloom filters are used to optimize transaction lookups in recent blocks.
3. Old Bloom filters are pruned based on the `recentBlocksBloomFiltersExpiration` duration.

## Configuration

The Block Validation Service uses various configuration values, including:

- `blockvalidation_subtreeTTL`: TTL for subtrees
- `optimisticMining`: Whether to use optimistic mining
- `excessiveblocksize`: Maximum allowed block size
- `blockvalidation_maxPreviousBlockHeadersToCheck`: Number of previous block headers to check
- `blockvalidation_subtreeTTLConcurrency`: Concurrency for updating subtree TTLs
- `blockvalidation_validateBlockSubtreesConcurrency`: Concurrency for validating block subtrees

## Dependencies

The Block Validation Service depends on several other components and services:

- Blockchain Client
- Subtree Store
- Transaction Store
- UTXO Store
- Validator Client
- Subtree Validation Client
- Kafka Consumer

These dependencies are injected into the `Server` and `BlockValidation` structures during initialization.

## Error Handling

The service implements robust error handling, particularly in block validation:

- Invalid blocks are handled by potentially invalidating them in the blockchain.
- Recoverable errors (e.g., storage errors, service errors) may trigger re-validation.
- Unrecoverable errors are logged and may result in block invalidation.

## Concurrency

- The service uses goroutines and error groups to process blocks and subtrees concurrently.
- Various concurrency limits are configurable (e.g., `blockvalidation_subtreeTTLConcurrency`).

## Metrics

The service initializes Prometheus metrics for monitoring various aspects of its operation, including:

- Block validation duration
- Subtree validation counts
- Transaction metadata cache operations
- Bloom filter creation and usage

These metrics can be used to monitor the performance and health of the Block Validation Service.
