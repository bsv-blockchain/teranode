# Block Validation Service Reference Documentation

## Types

### Server

```go
type Server struct {
    // UnimplementedBlockValidationAPIServer provides default implementations of gRPC methods
    blockvalidation_api.UnimplementedBlockValidationAPIServer

    // logger provides structured logging with contextual information
    logger                ulogger.Logger

    // settings contains operational parameters for block validation,
    // including timeouts, concurrency limits, and feature flags
    settings             *settings.Settings

    // blockchainClient provides access to the blockchain state and operations
    // like block storage, header retrieval, and chain reorganization
    blockchainClient     blockchain.ClientI

    // subtreeStore provides persistent storage for block subtrees,
    // enabling efficient block organization and validation
    subtreeStore         blob.Store

    // txStore handles permanent storage of individual transactions,
    // allowing retrieval of historical transaction data
    txStore              blob.Store

    // utxoStore manages the Unspent Transaction Output (UTXO) set,
    // tracking available outputs for transaction validation
    utxoStore            utxo.Store

    // validatorClient handles transaction validation operations,
    // ensuring each transaction follows network rules
    validatorClient      validator.Interface

    // blockFoundCh receives notifications of newly discovered blocks
    // that need validation. This channel buffers requests when high load occurs.
    blockFoundCh         chan processBlockFound

    // catchupCh handles blocks that need processing during chain catchup operations.
    // This channel is used when the node falls behind the chain tip.
    catchupCh            chan processBlockCatchup

    // blockValidation contains the core validation logic and state
    blockValidation      *BlockValidation

    // kafkaConsumerClient handles subscription to and consumption of
    // Kafka messages for distributed coordination
    kafkaConsumerClient  kafka.KafkaConsumerGroupI

    // processSubtreeNotify caches subtree processing state to prevent duplicate
    // processing of the same subtree from multiple miners
    processSubtreeNotify *ttlcache.Cache[chainhash.Hash, bool]

    // stats tracks operational metrics for monitoring and troubleshooting
    stats                *gocore.Stat
}
```

The `Server` type is the main structure for the Block Validation Service. It implements the `UnimplementedBlockValidationAPIServer` and contains components for managing block validation, storage, and communication with other services.

### BlockValidation

```go
type BlockValidation struct {
    // logger provides structured logging capabilities
    logger                        ulogger.Logger

    // settings contains operational parameters and feature flags
    settings                      *settings.Settings

    // blockchainClient interfaces with the blockchain for operations
    blockchainClient              blockchain.ClientI

    // subtreeStore provides persistent storage for block subtrees
    subtreeStore                  blob.Store

    // subtreeBlockHeightRetention specifies how long subtrees should be retained
    subtreeBlockHeightRetention   uint32

    // txStore handles permanent storage of transactions
    txStore                       blob.Store

    // utxoStore manages the UTXO set for transaction validation
    utxoStore                     utxo.Store

    // recentBlocksBloomFilters maintains bloom filters for recent blocks
    recentBlocksBloomFilters      *util.SyncedMap[chainhash.Hash, *model.BlockBloomFilter]

    // bloomFilterRetentionSize defines the number of blocks to keep the bloom filter for
    bloomFilterRetentionSize      uint32

    // validatorClient handles transaction validation operations
    validatorClient               validator.Interface

    // subtreeValidationClient manages subtree validation processes
    subtreeValidationClient       subtreevalidation.Interface

    // subtreeDeDuplicator prevents duplicate processing of subtrees
    subtreeDeDuplicator           *DeDuplicator

    // lastValidatedBlocks caches recently validated blocks for 2 minutes
    lastValidatedBlocks           *expiringmap.ExpiringMap[chainhash.Hash, *model.Block]

    // blockExists tracks validated block hashes for 2 hours
    blockExists                   *expiringmap.ExpiringMap[chainhash.Hash, bool]

    // subtreeExists tracks validated subtree hashes for 10 minutes
    subtreeExists                 *expiringmap.ExpiringMap[chainhash.Hash, bool]

    // subtreeCount tracks the number of subtrees being processed
    subtreeCount                  atomic.Int32

    // blockHashesCurrentlyValidated tracks blocks in validation process
    blockHashesCurrentlyValidated *util.SwissMap

    // blockBloomFiltersBeingCreated tracks bloom filters being generated
    blockBloomFiltersBeingCreated *util.SwissMap

    // bloomFilterStats collects statistics about bloom filter operations
    bloomFilterStats              *model.BloomStats

    // setMinedChan receives block hashes that need to be marked as mined
    setMinedChan                  chan *chainhash.Hash

    // revalidateBlockChan receives blocks that need revalidation
    revalidateBlockChan           chan revalidateBlockData

    // stats tracks operational metrics for monitoring
    stats                         *gocore.Stat

    // lastUsedBaseURL stores the most recent source URL used for retrievals
    lastUsedBaseURL               string
}
```

The `BlockValidation` type handles the core logic for validating blocks and managing related data structures.

## Functions

### Server

#### New
```go
func New(logger ulogger.Logger, tSettings *settings.Settings, subtreeStore blob.Store, txStore blob.Store, utxoStore utxo.Store, validatorClient validator.Interface, blockchainClient blockchain.ClientI, kafkaConsumerClient kafka.KafkaConsumerGroupI) *Server
```

Creates a new instance of the `Server` with:
- Initialization of Prometheus metrics
- Setup of background processors
- Configuration of validation components

#### Health
```go
func (u *Server) Health(ctx context.Context, checkLiveness bool) (int, string, error)
```

Performs comprehensive health checks:
- Liveness checks when checkLiveness is true
- Dependency checks including:
    - Kafka broker status
    - Blockchain client
    - FSM state
    - Subtree store
    - Transaction store
    - UTXO store

#### Init
```go
func (u *Server) Init(ctx context.Context) (err error)
```

Initializes the Block Validation Service:
- Creates subtree validation client
- Configures UTXO store and expiration settings
- Initializes the BlockValidation instance
- Starts background processors for transaction metadata
- Sets up communication channels

#### Start
```go
func (u *Server) Start(ctx context.Context, readyCh chan<- struct{}) error
```

Starts the Block Validation Service:
- Waits for blockchain FSM to transition from IDLE state
- Starts Kafka consumer for blocks
- Initializes and starts the gRPC server
- Signals readiness through readyCh channel

#### Stop
```go
func (u *Server) Stop(_ context.Context) error
```

Gracefully shuts down the Block Validation Service:
- Stops the subtree notification cache
- Closes the Kafka consumer

#### HealthGRPC
```go
func (u *Server) HealthGRPC(ctx context.Context, _ *blockvalidation_api.EmptyMessage) (*blockvalidation_api.HealthResponse, error)
```

Performs a gRPC health check on the service, including:
- Full dependency verification
- Detailed status reporting
- Timestamp information

#### Init
```go
func (u *Server) Init(ctx context.Context) (err error)
```

Initializes the service:
- Sets up subtree validation client
- Configures background processors
- Initializes Kafka consumer
- Sets up metadata processing queue

#### Start
```go
func (u *Server) Start(ctx context.Context) error
```

Starts all service components:
- Kafka consumer
- HTTP server (if configured)
- gRPC server
- Background processors

#### Stop
```go
func (u *Server) Stop(_ context.Context) error
```

Gracefully stops the service:
- Closes TTL cache
- Shuts down Kafka consumer
- Cleans up resources

#### BlockFound
```go
func (u *Server) BlockFound(ctx context.Context, req *blockvalidation_api.BlockFoundRequest) (*blockvalidation_api.EmptyMessage, error)
```

Handles notification of new blocks:
- Verifies block doesn't already exist
- Queues block for validation
- Optionally waits for validation completion

#### ProcessBlock
```go
func (u *Server) ProcessBlock(ctx context.Context, request *blockvalidation_api.ProcessBlockRequest) (*blockvalidation_api.EmptyMessage, error)
```

Processes complete blocks:
- Validates block structure
- Handles height calculation
- Integrates with blockchain state

#### SubtreeFound
```go
func (u *Server) SubtreeFound(_ context.Context, req *blockvalidation_api.SubtreeFoundRequest) (*blockvalidation_api.EmptyMessage, error)
```

Handles notification of new subtrees.

#### Get
```go
func (u *Server) Get(ctx context.Context, request *blockvalidation_api.GetSubtreeRequest) (*blockvalidation_api.GetSubtreeResponse, error)
```

Retrieves subtree data from storage.

#### Exists
```go
func (u *Server) Exists(ctx context.Context, request *blockvalidation_api.ExistsSubtreeRequest) (*blockvalidation_api.ExistsSubtreeResponse, error)
```

Verifies subtree existence in storage.

#### SetTxMeta
```go
func (u *Server) SetTxMeta(ctx context.Context, request *blockvalidation_api.SetTxMetaRequest) (*blockvalidation_api.SetTxMetaResponse, error)
```

Queues transaction metadata updates.

#### DelTxMeta
```go
func (u *Server) DelTxMeta(ctx context.Context, request *blockvalidation_api.DelTxMetaRequest) (*blockvalidation_api.DelTxMetaResponse, error)
```

Removes transaction metadata.

#### SetMinedMulti
```go
func (u *Server) SetMinedMulti(ctx context.Context, request *blockvalidation_api.SetMinedMultiRequest) (*blockvalidation_api.SetMinedMultiResponse, error)
```

Marks multiple transactions as mined in a block.

### BlockValidation

#### ValidateBlock
```go
func (u *BlockValidation) ValidateBlock(ctx context.Context, block *model.Block, baseURL string, bloomStats *model.BloomStats, disableOptimisticMining ...bool) error
```

Performs comprehensive block validation:
- Size verification
- Parent block validation
- Subtree validation
- Optional optimistic mining

#### GetBlockExists
```go
func (u *BlockValidation) GetBlockExists(ctx context.Context, hash *chainhash.Hash) (bool, error)
```

Checks block existence in validation system.


## Core Features

### Chain Catchup Process
The Chain Catchup system automatically handles situations when the node falls behind the main blockchain. When activated, it:

The system detects missing blocks by comparing the current node's block height with the network height. When it identifies a gap, it initiates the catchup process. The service retrieves blocks in batches of up to 100 blocks at a time to optimize network usage. During catchup, multiple blocks are validated simultaneously using configurable concurrency settings, typically defaulting to 32 concurrent validations. The service manages state transitions through the FSM (Finite State Machine), moving from normal operation to catchup mode and back once synchronization is complete.

### Optimistic Mining Support
Optimistic Mining is a performance optimization technique that allows faster block processing. In this mode:

The system accepts new blocks before completing full validation, provisionally adding them to the chain. While the block is being added, validation continues in the background without blocking the main processing pipeline. This behavior can be configured through settings, allowing operators to balance between performance and validation thoroughness based on their requirements.

### Bloom Filters

The Block Validation Service maintains bloom filters for recent blocks to optimize validation. This mechanism provides a probabilistic way to quickly determine if a transaction has been seen in a previous block without needing to perform extensive searches.

Creates filters dynamically as new blocks are validated, maintaining them in memory for rapid access. The service uses a configurable retention size parameter (default set in settings) to determine how many recent blocks should maintain bloom filters. The service uses a thread-safe SyncedMap to ensure concurrent access to filters is handled properly.

## Service Configuration

The service configuration is managed through several key areas:

### Validation Settings
- Maximum block size limits
- Block validation timeouts
- Transaction verification parameters
- Signature checking requirements

### Performance Controls
- Maximum concurrent validations
- Batch processing sizes
- Queue buffer sizes
- Cache retention periods

### Network Configuration
- Kafka broker addresses and topics
- HTTP server binding and ports
- Connection timeout values
- TLS/SSL settings

### Chain Catchup Parameters
- Maximum blocks per batch
- Catchup trigger thresholds
- Validation concurrency limits
- State transition timeouts

## Error Management

The service implements a structured error handling system:

### Error Categories
Recoverable errors include temporary network issues or resource constraints that can be resolved through retries. Unrecoverable errors indicate fundamental problems like invalid block structures or consensus violations.

### Error Processing
All errors are wrapped with appropriate context for the gRPC interface, maintaining error type information while adding relevant metadata. The system implements exponential backoff for retryable operations, with configurable retry limits and delays.

## Performance Monitoring

The service provides comprehensive performance monitoring through Prometheus metrics:

### Block Processing Metrics
- Time to validate blocks (histogram)
- Number of blocks in processing queue
- Block acceptance/rejection rates
- Chain catchup progress

### Resource Usage Metrics
- Transaction queue lengths
- Memory cache utilization
- Store operation latencies
- Background worker statistics
